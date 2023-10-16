/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.accumulo.manager.tableOps.merge;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.function.Predicate;
import java.util.stream.Stream;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.RowIterator;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.TabletHostingGoal;
import org.apache.accumulo.core.clientImpl.TabletHostingGoalUtil;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.NamespaceId;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.fate.Repo;
import org.apache.accumulo.core.gc.ReferenceFile;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.metadata.RootTable;
import org.apache.accumulo.core.metadata.StoredTabletFile;
import org.apache.accumulo.core.metadata.schema.Ample;
import org.apache.accumulo.core.metadata.schema.DataFileValue;
import org.apache.accumulo.core.metadata.schema.MetadataSchema;
import org.apache.accumulo.core.metadata.schema.MetadataTime;
import org.apache.accumulo.core.metadata.schema.TabletMetadata;
import org.apache.accumulo.core.metadata.schema.TabletsMetadata;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.util.Pair;
import org.apache.accumulo.manager.Manager;
import org.apache.accumulo.manager.tableOps.ManagerRepo;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.gc.AllVolumesDirectory;
import org.apache.accumulo.server.manager.state.MergeInfo;
import org.apache.accumulo.server.manager.state.MergeState;
import org.apache.accumulo.server.util.MetadataTableUtil;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;

public class DeleteRows extends ManagerRepo {

  private static final long serialVersionUID = 1L;

  private static final Logger log = LoggerFactory.getLogger(DeleteRows.class);

  private final NamespaceId namespaceId;
  private final TableId tableId;

  public DeleteRows(NamespaceId namespaceId, TableId tableId) {
    this.namespaceId = namespaceId;
    this.tableId = tableId;
  }

  @Override
  public Repo<Manager> call(long tid, Manager manager) throws Exception {
    MergeInfo mergeInfo = manager.getMergeInfo(tableId);
    Preconditions.checkState(mergeInfo.getState() == MergeState.MERGING);
    Preconditions.checkState(mergeInfo.isDelete());

    deleteTablets(manager, mergeInfo);

    manager.setMergeState(mergeInfo, MergeState.COMPLETE);

    // TODO namespace id
    return new FinishTableRangeOp(namespaceId, tableId);
  }

  private void deleteTablets(Manager manager, MergeInfo info) throws AccumuloException {
    // Before updated metadata and get the first and last tablets which
    // are fenced if necessary
    final Pair<KeyExtent,KeyExtent> firstAndLastTablets =
        updateMetadataRecordsForDelete(manager, info);

    // Find the deletion start row (exclusive) for tablets that need to be actually deleted
    // This will be null if deleting everything up until the end row or it will be
    // the endRow of the first tablet as the first tablet should be kept and will have
    // already been fenced if necessary
    final Text deletionStartRow = getDeletionStartRow(firstAndLastTablets.getFirst());

    // Find the deletion end row (inclusive) for tablets that need to be actually deleted
    // This will be null if deleting everything after the starting row or it will be
    // the prevEndRow of the last tablet as the last tablet should be kept and will have
    // already been fenced if necessary
    Text deletionEndRow = getDeletionEndRow(firstAndLastTablets.getSecond());

    // check if there are any tablets to delete and if not return
    if (!hasTabletsToDelete(firstAndLastTablets.getFirst(), firstAndLastTablets.getSecond())) {
      log.trace("No tablets to delete for range {}, returning", info.getExtent());
      return;
    }

    // Build an extent for the actual deletion range
    final KeyExtent extent =
        new KeyExtent(info.getExtent().tableId(), deletionEndRow, deletionStartRow);
    log.debug("Tablet deletion range is {}", extent);
    String targetSystemTable = extent.isMeta() ? RootTable.NAME : MetadataTable.NAME;
    log.debug("Deleting tablets for {}", extent);
    MetadataTime metadataTime = null;
    KeyExtent followingTablet = null;
    Set<TabletHostingGoal> goals = new HashSet<>();
    if (extent.endRow() != null) {
      Key nextExtent = new Key(extent.endRow()).followingKey(PartialKey.ROW);
      followingTablet = getHighTablet(manager,
          new KeyExtent(extent.tableId(), nextExtent.getRow(), extent.endRow()));
      log.debug("Found following tablet {}", followingTablet);
    }
    try {
      AccumuloClient client = manager.getContext();
      ServerContext context = manager.getContext();
      Ample ample = context.getAmple();
      Text start = extent.prevEndRow();
      if (start == null) {
        start = new Text();
      }
      log.debug("Making file deletion entries for {}", extent);
      Range deleteRange =
          new Range(MetadataSchema.TabletsSection.encodeRow(extent.tableId(), start), false,
              MetadataSchema.TabletsSection.encodeRow(extent.tableId(), extent.endRow()), true);
      Scanner scanner = client.createScanner(targetSystemTable, Authorizations.EMPTY);
      scanner.setRange(deleteRange);
      MetadataSchema.TabletsSection.ServerColumnFamily.DIRECTORY_COLUMN.fetch(scanner);
      MetadataSchema.TabletsSection.ServerColumnFamily.TIME_COLUMN.fetch(scanner);
      MetadataSchema.TabletsSection.HostingColumnFamily.GOAL_COLUMN.fetch(scanner);
      scanner.fetchColumnFamily(MetadataSchema.TabletsSection.DataFileColumnFamily.NAME);
      scanner.fetchColumnFamily(MetadataSchema.TabletsSection.CurrentLocationColumnFamily.NAME);
      Set<ReferenceFile> datafilesAndDirs = new TreeSet<>();
      for (Map.Entry<Key,Value> entry : scanner) {
        Key key = entry.getKey();
        if (key.compareColumnFamily(MetadataSchema.TabletsSection.DataFileColumnFamily.NAME) == 0) {
          var stf = new StoredTabletFile(key.getColumnQualifierData().toString());
          datafilesAndDirs.add(new ReferenceFile(stf.getTableId(), stf));
          if (datafilesAndDirs.size() > 1000) {
            ample.putGcFileAndDirCandidates(extent.tableId(), datafilesAndDirs);
            datafilesAndDirs.clear();
          }
        } else if (MetadataSchema.TabletsSection.ServerColumnFamily.TIME_COLUMN.hasColumns(key)) {
          metadataTime = MetadataTime.parse(entry.getValue().toString());
        } else if (key.compareColumnFamily(
            MetadataSchema.TabletsSection.CurrentLocationColumnFamily.NAME) == 0) {
          throw new IllegalStateException(
              "Tablet " + key.getRow() + " is assigned during a merge!");
        } else if (MetadataSchema.TabletsSection.ServerColumnFamily.DIRECTORY_COLUMN
            .hasColumns(key)) {
          var allVolumesDirectory =
              new AllVolumesDirectory(extent.tableId(), entry.getValue().toString());
          datafilesAndDirs.add(allVolumesDirectory);
          if (datafilesAndDirs.size() > 1000) {
            ample.putGcFileAndDirCandidates(extent.tableId(), datafilesAndDirs);
            datafilesAndDirs.clear();
          }
        } else if (MetadataSchema.TabletsSection.HostingColumnFamily.GOAL_COLUMN.hasColumns(key)) {
          TabletHostingGoal thisGoal = TabletHostingGoalUtil.fromValue(entry.getValue());
          goals.add(thisGoal);
        }
      }
      ample.putGcFileAndDirCandidates(extent.tableId(), datafilesAndDirs);
      BatchWriter bw = client.createBatchWriter(targetSystemTable);
      try {
        deleteTablets(info, deleteRange, bw, client);
      } finally {
        bw.close();
      }

      if (followingTablet != null) {
        log.debug("Updating prevRow of {} to {}", followingTablet, extent.prevEndRow());
        bw = client.createBatchWriter(targetSystemTable);
        try {
          Mutation m = new Mutation(followingTablet.toMetaRow());
          MetadataSchema.TabletsSection.TabletColumnFamily.PREV_ROW_COLUMN.put(m,
              MetadataSchema.TabletsSection.TabletColumnFamily
                  .encodePrevEndRow(extent.prevEndRow()));
          bw.addMutation(m);
          bw.flush();
        } finally {
          bw.close();
        }
      } else {
        // Recreate the default tablet to hold the end of the table
        MetadataTableUtil.addTablet(new KeyExtent(extent.tableId(), null, extent.prevEndRow()),
            MetadataSchema.TabletsSection.ServerColumnFamily.DEFAULT_TABLET_DIR_NAME,
            manager.getContext(), metadataTime.getType(), manager.getManagerLock(),
            getMergeHostingGoal(extent, goals));
      }
    } catch (RuntimeException | TableNotFoundException ex) {
      throw new AccumuloException(ex);
    }
  }

  private Pair<KeyExtent,KeyExtent> updateMetadataRecordsForDelete(Manager manager, MergeInfo info)
      throws AccumuloException {
    final KeyExtent range = info.getExtent();

    String targetSystemTable = MetadataTable.NAME;
    if (range.isMeta()) {
      targetSystemTable = RootTable.NAME;
    }
    final Pair<KeyExtent,KeyExtent> startAndEndTablets;

    final AccumuloClient client = manager.getContext();

    try (BatchWriter bw = client.createBatchWriter(targetSystemTable)) {
      final Text startRow = range.prevEndRow();
      final Text endRow = range.endRow() != null
          ? new Key(range.endRow()).followingKey(PartialKey.ROW).getRow() : null;

      // Find the tablets that overlap the start and end row of the deletion range
      // If the startRow is null then there will be an empty startTablet we don't need
      // to fence a starting tablet as we are deleting everything up to the end tablet
      // Likewise, if the endRow is null there will be an empty endTablet as we are deleting
      // all tablets after the starting tablet
      final Optional<TabletMetadata> startTablet =
          Optional.ofNullable(startRow).flatMap(row -> loadTabletMetadata(manager, range.tableId(),
              row, TabletMetadata.ColumnType.PREV_ROW, TabletMetadata.ColumnType.FILES));
      final Optional<TabletMetadata> endTablet =
          Optional.ofNullable(endRow).flatMap(row -> loadTabletMetadata(manager, range.tableId(),
              row, TabletMetadata.ColumnType.PREV_ROW, TabletMetadata.ColumnType.FILES));

      // Store the tablets in a Map if present so that if we have the same Tablet we
      // only need to process the same tablet once when fencing
      final SortedMap<KeyExtent,TabletMetadata> tabletMetadatas = new TreeMap<>();
      startTablet.ifPresent(ft -> tabletMetadatas.put(ft.getExtent(), ft));
      endTablet.ifPresent(lt -> tabletMetadatas.putIfAbsent(lt.getExtent(), lt));

      // Capture the tablets to return them or null if not loaded
      startAndEndTablets = new Pair<>(startTablet.map(TabletMetadata::getExtent).orElse(null),
          endTablet.map(TabletMetadata::getExtent).orElse(null));

      for (TabletMetadata tabletMetadata : tabletMetadatas.values()) {
        final KeyExtent keyExtent = tabletMetadata.getExtent();

        // Check if this tablet needs to have its files fenced for the deletion
        if (needsFencingForDeletion(info, keyExtent)) {
          log.debug("Found overlapping keyExtent {} for delete, fencing files.", keyExtent);

          // Create the ranges for fencing the files, this takes the place of
          // chop compactions and splits
          final List<Range> ranges = createRangesForDeletion(tabletMetadata, range);
          Preconditions.checkState(!ranges.isEmpty(),
              "No ranges found that overlap deletion range.");

          // Go through and fence each of the files that are part of the tablet
          for (Map.Entry<StoredTabletFile,DataFileValue> entry : tabletMetadata.getFilesMap()
              .entrySet()) {
            final StoredTabletFile existing = entry.getKey();
            final DataFileValue value = entry.getValue();

            final Mutation m = new Mutation(keyExtent.toMetaRow());

            // Go through each range that was created and modify the metadata for the file
            // The end row should be inclusive for the current tablet and the previous end row
            // should be exclusive for the start row.
            final Set<StoredTabletFile> newFiles = new HashSet<>();
            final Set<StoredTabletFile> existingFile = Set.of(existing);

            for (Range fenced : ranges) {
              // Clip range with the tablet range if the range already exists
              fenced = existing.hasRange() ? existing.getRange().clip(fenced, true) : fenced;

              // If null the range is disjoint which can happen if there are existing fenced files
              // If the existing file is disjoint then later we will delete if the file is not part
              // of the newFiles set which means it is disjoint with all ranges
              if (fenced != null) {
                final StoredTabletFile newFile = StoredTabletFile.of(existing.getPath(), fenced);
                log.trace("Adding new file {} with range {}", newFile.getMetadataPath(),
                    newFile.getRange());

                // Add the new file to the newFiles set, it will be added later if it doesn't match
                // the existing file already. We still need to add to the set to be checked later
                // even if it matches the existing file as later the deletion logic will check to
                // see if the existing file is part of this set before deleting. This is done to
                // make sure the existing file isn't deleted unless it is not needed/disjoint
                // with all ranges.
                newFiles.add(newFile);
              } else {
                log.trace("Found a disjoint file {} with  range {} on delete",
                    existing.getMetadataPath(), existing.getRange());
              }
            }

            // If the existingFile is not contained in the newFiles set then we can delete it
            Sets.difference(existingFile, newFiles).forEach(
                delete -> m.putDelete(MetadataSchema.TabletsSection.DataFileColumnFamily.NAME,
                    existing.getMetadataText()));

            // Add any new files that don't match the existingFile
            // As of now we will only have at most 2 files as up to 2 ranges are created
            final List<StoredTabletFile> filesToAdd =
                new ArrayList<>(Sets.difference(newFiles, existingFile));
            Preconditions.checkArgument(filesToAdd.size() <= 2,
                "There should only be at most 2 StoredTabletFiles after computing new ranges.");

            // If more than 1 new file then re-calculate the num entries and size
            if (filesToAdd.size() == 2) {
              // This splits up the values in half and makes sure they total the original
              // values
              final Pair<DataFileValue,DataFileValue> newDfvs = computeNewDfv(value);
              m.put(MetadataSchema.TabletsSection.DataFileColumnFamily.NAME,
                  filesToAdd.get(0).getMetadataText(), newDfvs.getFirst().encodeAsValue());
              m.put(MetadataSchema.TabletsSection.DataFileColumnFamily.NAME,
                  filesToAdd.get(1).getMetadataText(), newDfvs.getSecond().encodeAsValue());
            } else {
              // Will be 0 or 1 files
              filesToAdd
                  .forEach(newFile -> m.put(MetadataSchema.TabletsSection.DataFileColumnFamily.NAME,
                      newFile.getMetadataText(), value.encodeAsValue()));
            }

            if (!m.getUpdates().isEmpty()) {
              bw.addMutation(m);
            }
          }
        } else {
          log.debug(
              "Skipping metadata update on file for keyExtent {} for delete as not overlapping on rows.",
              keyExtent);
        }
      }

      bw.flush();

      return startAndEndTablets;
    } catch (Exception ex) {
      throw new AccumuloException(ex);
    }
  }

  // This method finds returns the deletion starting row (exclusive) for tablets that
  // need to be actually deleted. If the startTablet is null then
  // the deletion start row will just be null as all tablets are being deleted
  // up to the end. Otherwise, this returns the endRow of the first tablet
  // as the first tablet should be kept and will have been previously
  // fenced if necessary
  private Text getDeletionStartRow(final KeyExtent startTablet) {
    if (startTablet == null) {
      log.debug("First tablet for delete range is null");
      return null;
    }

    final Text deletionStartRow = startTablet.endRow();
    log.debug("Start row is {} for deletion", deletionStartRow);

    return deletionStartRow;
  }

  // This method finds returns the deletion ending row (inclusive) for tablets that
  // need to be actually deleted. If the endTablet is null then
  // the deletion end row will just be null as all tablets are being deleted
  // after the start row. Otherwise, this returns the prevEndRow of the last tablet
  // as the last tablet should be kept and will have been previously
  // fenced if necessary
  private Text getDeletionEndRow(final KeyExtent endTablet) {
    if (endTablet == null) {
      log.debug("Last tablet for delete range is null");
      return null;
    }

    Text deletionEndRow = endTablet.prevEndRow();
    log.debug("Deletion end row is {}", deletionEndRow);

    return deletionEndRow;
  }

  static TabletHostingGoal getMergeHostingGoal(KeyExtent range, Set<TabletHostingGoal> goals) {
    TabletHostingGoal mergeHostingGoal = TabletHostingGoal.ONDEMAND;
    if (range.isMeta() || goals.contains(TabletHostingGoal.ALWAYS)) {
      mergeHostingGoal = TabletHostingGoal.ALWAYS;
    } else if (goals.contains(TabletHostingGoal.NEVER)) {
      mergeHostingGoal = TabletHostingGoal.NEVER;
    }
    return mergeHostingGoal;
  }

  // Divide each new DFV in half and make sure the sum equals the original
  @VisibleForTesting
  static Pair<DataFileValue,DataFileValue> computeNewDfv(DataFileValue value) {
    final DataFileValue file1Value = new DataFileValue(Math.max(1, value.getSize() / 2),
        Math.max(1, value.getNumEntries() / 2), value.getTime());

    final DataFileValue file2Value =
        new DataFileValue(Math.max(1, value.getSize() - file1Value.getSize()),
            Math.max(1, value.getNumEntries() - file1Value.getNumEntries()), value.getTime());

    return new Pair<>(file1Value, file2Value);
  }

  private Optional<TabletMetadata> loadTabletMetadata(Manager manager, TableId tabletId,
      final Text row, TabletMetadata.ColumnType... columns) {
    try (TabletsMetadata tabletsMetadata = manager.getContext().getAmple().readTablets()
        .forTable(tabletId).overlapping(row, true, row).fetch(columns).build()) {
      return tabletsMetadata.stream().findFirst();
    }
  }

  // This method is used to detect if a tablet needs to be split/chopped for a delete
  // Instead of performing a split or chop compaction, the tablet will have its files fenced.
  private boolean needsFencingForDeletion(MergeInfo info, KeyExtent keyExtent) {
    // Does this extent cover the end points of the delete?
    final Predicate<Text> isWithin = r -> r != null && keyExtent.contains(r);
    final Predicate<Text> isNotBoundary =
        r -> !r.equals(keyExtent.endRow()) && !r.equals(keyExtent.prevEndRow());
    final KeyExtent deleteRange = info.getExtent();

    return (keyExtent.overlaps(deleteRange) && Stream
        .of(deleteRange.prevEndRow(), deleteRange.endRow()).anyMatch(isWithin.and(isNotBoundary)))
        || info.needsToBeChopped(keyExtent);
  }

  // Instead of splitting or chopping tablets for a delete we instead create ranges
  // to exclude the portion of the tablet that should be deleted
  private Text followingRow(Text row) {
    if (row == null) {
      return null;
    }
    return new Key(row).followingKey(PartialKey.ROW).getRow();
  }

  // Instead of splitting or chopping tablets for a delete we instead create ranges
  // to exclude the portion of the tablet that should be deleted
  private List<Range> createRangesForDeletion(TabletMetadata tabletMetadata,
      final KeyExtent deleteRange) {
    final KeyExtent tabletExtent = tabletMetadata.getExtent();

    // If the delete range wholly contains the tablet being deleted then there is no range to clip
    // files to because the files should be completely dropped.
    Preconditions.checkArgument(!deleteRange.contains(tabletExtent), "delete range:%s tablet:%s",
        deleteRange, tabletExtent);

    final List<Range> ranges = new ArrayList<>();

    if (deleteRange.overlaps(tabletExtent)) {
      if (deleteRange.prevEndRow() != null
          && tabletExtent.contains(followingRow(deleteRange.prevEndRow()))) {
        log.trace("Fencing tablet {} files to ({},{}]", tabletExtent, tabletExtent.prevEndRow(),
            deleteRange.prevEndRow());
        ranges.add(new Range(tabletExtent.prevEndRow(), false, deleteRange.prevEndRow(), true));
      }

      // This covers the case of when a deletion range overlaps the last tablet. We need to create a
      // range that excludes the deletion.
      if (deleteRange.endRow() != null
          && tabletMetadata.getExtent().contains(deleteRange.endRow())) {
        log.trace("Fencing tablet {} files to ({},{}]", tabletExtent, deleteRange.endRow(),
            tabletExtent.endRow());
        ranges.add(new Range(deleteRange.endRow(), false, tabletExtent.endRow(), true));
      }
    } else {
      log.trace("Fencing tablet {} files to itself because it does not overlap delete range",
          tabletExtent);
      ranges.add(tabletExtent.toDataRange());
    }

    return ranges;
  }

  private static boolean isFirstTabletInTable(KeyExtent tablet) {
    return tablet != null && tablet.prevEndRow() == null;
  }

  private static boolean isLastTabletInTable(KeyExtent tablet) {
    return tablet != null && tablet.endRow() == null;
  }

  private static boolean areContiguousTablets(KeyExtent firstTablet, KeyExtent lastTablet) {
    return firstTablet != null && lastTablet != null
        && Objects.equals(firstTablet.endRow(), lastTablet.prevEndRow());
  }

  private boolean hasTabletsToDelete(final KeyExtent firstTabletInRange,
      final KeyExtent lastTableInRange) {
    // If the tablets are equal (and not null) then the deletion range is just part of 1 tablet
    // which will be fenced so there are no tablets to delete. The null check is because if both
    // are null then we are just deleting everything, so we do have tablets to delete
    if (Objects.equals(firstTabletInRange, lastTableInRange) && firstTabletInRange != null) {
      log.trace(
          "No tablets to delete, firstTablet {} equals lastTablet {} in deletion range and was fenced.",
          firstTabletInRange, lastTableInRange);
      return false;
      // If the lastTablet of the deletion range is the first tablet of the table it has been fenced
      // already so nothing to actually delete before it
    } else if (isFirstTabletInTable(lastTableInRange)) {
      log.trace(
          "No tablets to delete, lastTablet {} in deletion range is the first tablet of the table and was fenced.",
          lastTableInRange);
      return false;
      // If the firstTablet of the deletion range is the last tablet of the table it has been fenced
      // already so nothing to actually delete after it
    } else if (isLastTabletInTable(firstTabletInRange)) {
      log.trace(
          "No tablets to delete, firstTablet {} in deletion range is the last tablet of the table and was fenced.",
          firstTabletInRange);
      return false;
      // If the firstTablet and lastTablet are contiguous tablets then there is nothing to delete as
      // each will be fenced and nothing between
    } else if (areContiguousTablets(firstTabletInRange, lastTableInRange)) {
      log.trace(
          "No tablets to delete, firstTablet {} and lastTablet {} in deletion range are contiguous and were fenced.",
          firstTabletInRange, lastTableInRange);
      return false;
    }

    return true;
  }

  static void deleteTablets(MergeInfo info, Range scanRange, BatchWriter bw, AccumuloClient client)
      throws TableNotFoundException, MutationsRejectedException {
    Scanner scanner;
    Mutation m;
    // Delete everything in the other tablets
    // group all deletes into tablet into one mutation, this makes tablets
    // either disappear entirely or not all.. this is important for the case
    // where the process terminates in the loop below...
    scanner = client.createScanner(info.getExtent().isMeta() ? RootTable.NAME : MetadataTable.NAME,
        Authorizations.EMPTY);
    log.debug("Deleting range {}", scanRange);
    scanner.setRange(scanRange);
    RowIterator rowIter = new RowIterator(scanner);
    while (rowIter.hasNext()) {
      Iterator<Map.Entry<Key,Value>> row = rowIter.next();
      m = null;
      while (row.hasNext()) {
        Map.Entry<Key,Value> entry = row.next();
        Key key = entry.getKey();

        if (m == null) {
          m = new Mutation(key.getRow());
        }

        m.putDelete(key.getColumnFamily(), key.getColumnQualifier());
        log.debug("deleting entry {}", key);
      }
      bw.addMutation(m);
    }

    bw.flush();
  }

  static KeyExtent getHighTablet(Manager manager, KeyExtent range) throws AccumuloException {
    try {
      AccumuloClient client = manager.getContext();
      Scanner scanner = client.createScanner(range.isMeta() ? RootTable.NAME : MetadataTable.NAME,
          Authorizations.EMPTY);
      MetadataSchema.TabletsSection.TabletColumnFamily.PREV_ROW_COLUMN.fetch(scanner);
      KeyExtent start = new KeyExtent(range.tableId(), range.endRow(), null);
      scanner.setRange(new Range(start.toMetaRow(), null));
      Iterator<Map.Entry<Key,Value>> iterator = scanner.iterator();
      if (!iterator.hasNext()) {
        throw new AccumuloException("No last tablet for a merge " + range);
      }
      Map.Entry<Key,Value> entry = iterator.next();
      KeyExtent highTablet = KeyExtent.fromMetaPrevRow(entry);
      if (!highTablet.tableId().equals(range.tableId())) {
        throw new AccumuloException("No last tablet for merge " + range + " " + highTablet);
      }
      return highTablet;
    } catch (Exception ex) {
      throw new AccumuloException("Unexpected failure finding the last tablet for a merge " + range,
          ex);
    }
  }
}

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

import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.FILES;
import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.LOCATION;
import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.LOGS;
import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.OPID;
import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.PREV_ROW;
import static org.apache.accumulo.manager.tableOps.merge.MergeTablets.validateTablet;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import org.apache.accumulo.core.client.admin.TabletAvailability;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.fate.FateId;
import org.apache.accumulo.core.fate.Repo;
import org.apache.accumulo.core.metadata.StoredTabletFile;
import org.apache.accumulo.core.metadata.schema.Ample;
import org.apache.accumulo.core.metadata.schema.Ample.ConditionalResult.Status;
import org.apache.accumulo.core.metadata.schema.DataFileValue;
import org.apache.accumulo.core.metadata.schema.TabletMetadata;
import org.apache.accumulo.core.metadata.schema.TabletOperationId;
import org.apache.accumulo.core.metadata.schema.TabletOperationType;
import org.apache.accumulo.core.util.Pair;
import org.apache.accumulo.manager.Manager;
import org.apache.accumulo.manager.tableOps.ManagerRepo;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;

public class DeleteRows extends ManagerRepo {

  private static final long serialVersionUID = 1L;

  private static final Logger log = LoggerFactory.getLogger(DeleteRows.class);

  private final MergeInfo data;

  public DeleteRows(MergeInfo data) {
    Preconditions.checkArgument(data.op == MergeInfo.Operation.DELETE);
    this.data = data;
  }

  @Override
  public Repo<Manager> call(FateId fateId, Manager manager) throws Exception {
    // delete or fence files within the deletion range
    var mergeRange = deleteTabletFiles(manager, fateId);

    // merge away empty tablets in the deletion range
    return new MergeTablets(mergeRange.map(mre -> data.useMergeRange(mre)).orElse(data));
  }

  private Optional<KeyExtent> deleteTabletFiles(Manager manager, FateId fateId) {
    // Only delete data within the original extent specified by the user
    KeyExtent range = data.getOriginalExtent();
    log.debug("{} deleting tablet files in range {}", fateId, range);
    var opid = TabletOperationId.from(TabletOperationType.MERGING, fateId);

    try (
        var tabletsMetadata = manager.getContext().getAmple().readTablets()
            .forTable(range.tableId()).overlapping(range.prevEndRow(), range.endRow())
            .fetch(OPID, LOCATION, FILES, PREV_ROW, LOGS).checkConsistency().build();
        var tabletsMutator = manager.getContext().getAmple().conditionallyMutateTablets()) {

      KeyExtent firstCompleteContained = null;
      KeyExtent lastCompletelyContained = null;

      for (var tabletMetadata : tabletsMetadata) {
        validateTablet(tabletMetadata, fateId, opid, data.tableId);
        var tabletMutator = tabletsMutator.mutateTablet(tabletMetadata.getExtent())
            .requireOperation(opid).requireAbsentLocation();

        Set<StoredTabletFile> filesToDelete = new HashSet<>();
        Map<StoredTabletFile,DataFileValue> filesToAddMap = new HashMap<>();

        if (range.contains(tabletMetadata.getExtent())) {
          if (firstCompleteContained == null) {
            firstCompleteContained = tabletMetadata.getExtent();
          }
          lastCompletelyContained = tabletMetadata.getExtent();
          // delete range completely contains tablet, so want to delete all the tablets files
          filesToDelete.addAll(tabletMetadata.getFiles());
        } else {
          Preconditions.checkState(range.overlaps(tabletMetadata.getExtent()),
              "%s tablet %s does not overlap delete range %s", fateId, tabletMetadata.getExtent(),
              range);

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
                log.trace("{} Adding new file {} with range {}", fateId, newFile.getMetadataPath(),
                    newFile.getRange());

                // Add the new file to the newFiles set, it will be added later if it doesn't match
                // the existing file already. We still need to add to the set to be checked later
                // even if it matches the existing file as later the deletion logic will check to
                // see if the existing file is part of this set before deleting. This is done to
                // make sure the existing file isn't deleted unless it is not needed/disjoint
                // with all ranges.
                newFiles.add(newFile);
              } else {
                log.trace("{} Found a disjoint file {} with  range {} on delete", fateId,
                    existing.getMetadataPath(), existing.getRange());
              }
            }

            // If the existingFile is not contained in the newFiles set then we can delete it
            filesToDelete.addAll(Sets.difference(existingFile, newFiles));

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
              filesToAddMap.put(filesToAdd.get(0), newDfvs.getFirst());
              filesToAddMap.put(filesToAdd.get(1), newDfvs.getSecond());
            } else {
              // Will be 0 or 1 files
              filesToAdd.forEach(newFile -> filesToAddMap.put(newFile, value));
            }
          }
        }

        filesToDelete.forEach(file -> log.debug("{} deleting file {} for {}", fateId, file,
            tabletMetadata.getExtent()));
        filesToAddMap.forEach((file, dfv) -> log.debug("{} adding file {} {} for {}", fateId, file,
            dfv, tabletMetadata.getExtent()));

        filesToDelete.forEach(tabletMutator::deleteFile);
        filesToAddMap.forEach(tabletMutator::putFile);

        tabletMutator.submit(tm -> tm.getFiles().containsAll(filesToAddMap.keySet())
            && Collections.disjoint(tm.getFiles(), filesToDelete));
      }

      var results = tabletsMutator.process();
      verifyAccepted(results, fateId);

      return computeMergeRange(range, firstCompleteContained, lastCompletelyContained);
    }
  }

  /**
   * Tablets that are completely contained in the delete range can be merged away. Use the first and
   * last tablet were completely contained in the delete range to create a merge range.
   */
  private Optional<KeyExtent> computeMergeRange(KeyExtent deleteRange,
      KeyExtent firstCompleteContained, KeyExtent lastCompletelyContained) {
    if (deleteRange.prevEndRow() == null && deleteRange.endRow() == null) {
      return Optional.empty();
    }

    if (firstCompleteContained == null) {
      return Optional.empty();
    }

    // Extend the merge range past the end of the last fully contained extent to merge that tablet
    // away.
    Text end = lastCompletelyContained.endRow();
    if (end != null) {
      end = new Key(end).followingKey(PartialKey.ROW).getRow();
    }

    return Optional
        .of(new KeyExtent(deleteRange.tableId(), end, firstCompleteContained.prevEndRow()));
  }

  static void verifyAccepted(Map<KeyExtent,Ample.ConditionalResult> results, FateId fateId) {
    if (results.values().stream()
        .anyMatch(conditionalResult -> conditionalResult.getStatus() != Status.ACCEPTED)) {
      results.forEach(((extent, conditionalResult) -> {
        if (conditionalResult.getStatus() != Status.ACCEPTED) {
          log.error("{} failed to update {}", fateId, extent);
        }
      }));

      throw new IllegalStateException(fateId + " failed to update tablet files");
    }
  }

  static TabletAvailability getMergeTabletAvailability(KeyExtent range,
      Set<TabletAvailability> tabletAvailabilities) {
    TabletAvailability mergeTabletAvailability = TabletAvailability.ONDEMAND;
    if (range.isMeta() || tabletAvailabilities.contains(TabletAvailability.HOSTED)) {
      mergeTabletAvailability = TabletAvailability.HOSTED;
    } else if (tabletAvailabilities.contains(TabletAvailability.UNHOSTED)) {
      mergeTabletAvailability = TabletAvailability.UNHOSTED;
    }
    return mergeTabletAvailability;
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
}

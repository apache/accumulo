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

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.admin.TabletHostingGoal;
import org.apache.accumulo.core.clientImpl.TabletHostingGoalUtil;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.NamespaceId;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.fate.Repo;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.metadata.RootTable;
import org.apache.accumulo.core.metadata.StoredTabletFile;
import org.apache.accumulo.core.metadata.schema.MetadataSchema;
import org.apache.accumulo.core.metadata.schema.MetadataTime;
import org.apache.accumulo.core.metadata.schema.TabletMetadata;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.manager.Manager;
import org.apache.accumulo.manager.tableOps.ManagerRepo;
import org.apache.accumulo.server.gc.AllVolumesDirectory;
import org.apache.accumulo.server.manager.state.MergeInfo;
import org.apache.accumulo.server.manager.state.MergeState;
import org.apache.accumulo.server.tablets.TabletTime;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

public class MergeTablets extends ManagerRepo {

  private static final long serialVersionUID = 1L;

  private static final Logger log = LoggerFactory.getLogger(MergeTablets.class);

  private final NamespaceId namespaceId;
  private final TableId tableId;

  public MergeTablets(NamespaceId namespaceId, TableId tableId) {
    this.namespaceId = namespaceId;
    this.tableId = tableId;
  }

  @Override
  public Repo<Manager> call(long tid, Manager manager) throws Exception {
    MergeInfo mergeInfo = manager.getMergeInfo(tableId);
    Preconditions.checkState(mergeInfo.getState() == MergeState.MERGING);
    Preconditions.checkState(!mergeInfo.isDelete());

    var extent = mergeInfo.getExtent();
    long tabletCount;

    try (var tabletMeta = manager.getContext().getAmple().readTablets().forTable(extent.tableId())
        .overlapping(extent.prevEndRow(), extent.endRow()).fetch(TabletMetadata.ColumnType.PREV_ROW)
        .checkConsistency().build()) {
      tabletCount = tabletMeta.stream().count();
    }

    if (tabletCount > 1) {
      mergeMetadataRecords(manager, mergeInfo);
    }

    return new FinishTableRangeOp(namespaceId, tableId);
  }

  private void mergeMetadataRecords(Manager manager, MergeInfo info) throws AccumuloException {
    KeyExtent range = info.getExtent();
    log.debug("Merging metadata for {}", range);
    KeyExtent stop = DeleteRows.getHighTablet(manager, range);
    log.debug("Highest tablet is {}", stop);
    Value firstPrevRowValue = null;
    Text stopRow = stop.toMetaRow();
    Text start = range.prevEndRow();
    if (start == null) {
      start = new Text();
    }
    Range scanRange = new Range(MetadataSchema.TabletsSection.encodeRow(range.tableId(), start),
        false, stopRow, false);
    String targetSystemTable = MetadataTable.NAME;
    if (range.isMeta()) {
      targetSystemTable = RootTable.NAME;
    }
    Set<TabletHostingGoal> goals = new HashSet<>();

    AccumuloClient client = manager.getContext();

    KeyExtent stopExtent = KeyExtent.fromMetaRow(stop.toMetaRow());
    KeyExtent previousKeyExtent = null;
    KeyExtent lastExtent = null;

    try (BatchWriter bw = client.createBatchWriter(targetSystemTable)) {
      long fileCount = 0;
      // Make file entries in highest tablet
      Scanner scanner = client.createScanner(targetSystemTable, Authorizations.EMPTY);
      // Update to set the range to include the highest tablet
      scanner.setRange(new Range(MetadataSchema.TabletsSection.encodeRow(range.tableId(), start),
          false, stopRow, true));
      MetadataSchema.TabletsSection.TabletColumnFamily.PREV_ROW_COLUMN.fetch(scanner);
      MetadataSchema.TabletsSection.ServerColumnFamily.TIME_COLUMN.fetch(scanner);
      MetadataSchema.TabletsSection.ServerColumnFamily.DIRECTORY_COLUMN.fetch(scanner);
      MetadataSchema.TabletsSection.HostingColumnFamily.GOAL_COLUMN.fetch(scanner);
      scanner.fetchColumnFamily(MetadataSchema.TabletsSection.DataFileColumnFamily.NAME);
      Mutation m = new Mutation(stopRow);
      MetadataTime maxLogicalTime = null;
      for (Map.Entry<Key,Value> entry : scanner) {
        Key key = entry.getKey();
        Value value = entry.getValue();

        final KeyExtent keyExtent = KeyExtent.fromMetaRow(key.getRow());

        // Keep track of the last Key Extent seen so we can use it to fence
        // of RFiles when merging the metadata
        if (lastExtent != null && !keyExtent.equals(lastExtent)) {
          previousKeyExtent = lastExtent;
        }

        // Special case to handle the highest/stop tablet, which is where files are
        // merged to. The existing merge code won't delete files from this tablet
        // so we need to handle the deletes in this tablet when fencing files.
        // We may be able to make this simpler in the future.
        if (keyExtent.equals(stopExtent)) {
          if (previousKeyExtent != null && key.getColumnFamily()
              .equals(MetadataSchema.TabletsSection.DataFileColumnFamily.NAME)) {

            // Fence off existing files by the end row of the previous tablet and current tablet
            final StoredTabletFile existing = StoredTabletFile.of(key.getColumnQualifier());
            // The end row should be inclusive for the current tablet and the previous end row
            // should be exclusive for the start row
            Range fenced = new Range(previousKeyExtent.endRow(), false, keyExtent.endRow(), true);

            // Clip range if exists
            fenced = existing.hasRange() ? existing.getRange().clip(fenced) : fenced;

            final StoredTabletFile newFile = StoredTabletFile.of(existing.getPath(), fenced);
            // If the existing metadata does not match then we need to delete the old
            // and replace with a new range
            if (!existing.equals(newFile)) {
              m.putDelete(MetadataSchema.TabletsSection.DataFileColumnFamily.NAME,
                  existing.getMetadataText());
              m.put(key.getColumnFamily(), newFile.getMetadataText(), value);
            }

            fileCount++;
          }
          // For the highest tablet we only care about the DataFileColumnFamily
          continue;
        }

        // Handle metadata updates for all other tablets except the highest tablet
        // Ranges are created for the files and then added to the highest tablet in
        // the merge range. Deletes are handled later for the old files when the tablets
        // are removed.
        if (key.getColumnFamily().equals(MetadataSchema.TabletsSection.DataFileColumnFamily.NAME)) {
          final StoredTabletFile existing = StoredTabletFile.of(key.getColumnQualifier());

          // Fence off files by the previous tablet and current tablet that is being merged
          // The end row should be inclusive for the current tablet and the previous end row should
          // be exclusive for the start row.
          Range fenced = new Range(previousKeyExtent != null ? previousKeyExtent.endRow() : null,
              false, keyExtent.endRow(), true);

          // Clip range with the tablet range if the range already exists
          fenced = existing.hasRange() ? existing.getRange().clip(fenced) : fenced;

          // Move the file and range to the last tablet
          StoredTabletFile newFile = StoredTabletFile.of(existing.getPath(), fenced);
          m.put(key.getColumnFamily(), newFile.getMetadataText(), value);

          fileCount++;
        } else if (MetadataSchema.TabletsSection.TabletColumnFamily.PREV_ROW_COLUMN.hasColumns(key)
            && firstPrevRowValue == null) {
          log.debug("prevRow entry for lowest tablet is {}", value);
          firstPrevRowValue = new Value(value);
        } else if (MetadataSchema.TabletsSection.ServerColumnFamily.TIME_COLUMN.hasColumns(key)) {
          maxLogicalTime =
              TabletTime.maxMetadataTime(maxLogicalTime, MetadataTime.parse(value.toString()));
        } else if (MetadataSchema.TabletsSection.ServerColumnFamily.DIRECTORY_COLUMN
            .hasColumns(key)) {
          var allVolumesDir = new AllVolumesDirectory(range.tableId(), value.toString());
          bw.addMutation(manager.getContext().getAmple().createDeleteMutation(allVolumesDir));
        } else if (MetadataSchema.TabletsSection.HostingColumnFamily.GOAL_COLUMN.hasColumns(key)) {
          TabletHostingGoal thisGoal = TabletHostingGoalUtil.fromValue(value);
          goals.add(thisGoal);
        }

        lastExtent = keyExtent;
      }

      // read the logical time from the last tablet in the merge range, it is not included in
      // the loop above
      scanner = client.createScanner(targetSystemTable, Authorizations.EMPTY);
      scanner.setRange(new Range(stopRow));
      MetadataSchema.TabletsSection.ServerColumnFamily.TIME_COLUMN.fetch(scanner);
      MetadataSchema.TabletsSection.HostingColumnFamily.GOAL_COLUMN.fetch(scanner);
      scanner.fetchColumnFamily(MetadataSchema.TabletsSection.ExternalCompactionColumnFamily.NAME);
      Set<String> extCompIds = new HashSet<>();
      for (Map.Entry<Key,Value> entry : scanner) {
        if (MetadataSchema.TabletsSection.ServerColumnFamily.TIME_COLUMN
            .hasColumns(entry.getKey())) {
          maxLogicalTime = TabletTime.maxMetadataTime(maxLogicalTime,
              MetadataTime.parse(entry.getValue().toString()));
        } else if (MetadataSchema.TabletsSection.ExternalCompactionColumnFamily.NAME
            .equals(entry.getKey().getColumnFamily())) {
          extCompIds.add(entry.getKey().getColumnQualifierData().toString());
        } else if (MetadataSchema.TabletsSection.HostingColumnFamily.GOAL_COLUMN
            .hasColumns(entry.getKey())) {
          TabletHostingGoal thisGoal = TabletHostingGoalUtil.fromValue(entry.getValue());
          goals.add(thisGoal);
        }
      }

      if (maxLogicalTime != null) {
        MetadataSchema.TabletsSection.ServerColumnFamily.TIME_COLUMN.put(m,
            new Value(maxLogicalTime.encode()));
      }

      // delete any entries for external compactions
      extCompIds.forEach(ecid -> m
          .putDelete(MetadataSchema.TabletsSection.ExternalCompactionColumnFamily.STR_NAME, ecid));

      // Set the TabletHostingGoal for this tablet based on the goals of the other tablets in
      // the merge range. Always takes priority over never.
      TabletHostingGoal mergeHostingGoal = DeleteRows.getMergeHostingGoal(range, goals);
      MetadataSchema.TabletsSection.HostingColumnFamily.GOAL_COLUMN.put(m,
          TabletHostingGoalUtil.toValue(mergeHostingGoal));

      if (!m.getUpdates().isEmpty()) {
        bw.addMutation(m);
      }

      bw.flush();

      log.debug("Moved {} files to {}", fileCount, stop);

      if (firstPrevRowValue == null) {
        log.debug("tablet already merged");
        return;
      }

      stop = new KeyExtent(stop.tableId(), stop.endRow(),
          MetadataSchema.TabletsSection.TabletColumnFamily.decodePrevEndRow(firstPrevRowValue));
      Mutation updatePrevRow =
          MetadataSchema.TabletsSection.TabletColumnFamily.createPrevRowMutation(stop);
      log.debug("Setting the prevRow for last tablet: {}", stop);
      bw.addMutation(updatePrevRow);
      bw.flush();

      DeleteRows.deleteTablets(info, scanRange, bw, client);

    } catch (Exception ex) {
      throw new AccumuloException(ex);
    }
  }
}

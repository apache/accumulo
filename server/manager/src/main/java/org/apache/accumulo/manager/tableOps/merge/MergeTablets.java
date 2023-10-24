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

import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.DIR;
import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.ECOMP;
import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.FILES;
import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.HOSTING_GOAL;
import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.LOCATION;
import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.OPID;
import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.PREV_ROW;
import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.TIME;
import static org.apache.accumulo.manager.tableOps.merge.DeleteRows.verifyAccepted;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.admin.TabletHostingGoal;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.fate.FateTxId;
import org.apache.accumulo.core.fate.Repo;
import org.apache.accumulo.core.gc.ReferenceFile;
import org.apache.accumulo.core.metadata.StoredTabletFile;
import org.apache.accumulo.core.metadata.schema.Ample;
import org.apache.accumulo.core.metadata.schema.DataFileValue;
import org.apache.accumulo.core.metadata.schema.MetadataTime;
import org.apache.accumulo.core.metadata.schema.TabletMetadata;
import org.apache.accumulo.core.metadata.schema.TabletOperationId;
import org.apache.accumulo.core.metadata.schema.TabletOperationType;
import org.apache.accumulo.manager.Manager;
import org.apache.accumulo.manager.tableOps.ManagerRepo;
import org.apache.accumulo.server.gc.AllVolumesDirectory;
import org.apache.accumulo.server.tablets.TabletTime;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

public class MergeTablets extends ManagerRepo {

  private static final long serialVersionUID = 1L;

  private static final Logger log = LoggerFactory.getLogger(MergeTablets.class);

  private final MergeInfo data;

  public MergeTablets(MergeInfo data) {
    this.data = data;
  }

  @Override
  public Repo<Manager> call(long tid, Manager manager) throws Exception {
    mergeMetadataRecords(manager, tid);
    return new FinishTableRangeOp(data);
  }

  private void mergeMetadataRecords(Manager manager, long tid) throws AccumuloException {
    var fateStr = FateTxId.formatTid(tid);
    KeyExtent range = data.getMergeExtent();
    log.debug("{} Merging metadata for {}", fateStr, range);

    var opid = TabletOperationId.from(TabletOperationType.MERGING, tid);
    Set<TabletHostingGoal> goals = new HashSet<>();
    MetadataTime maxLogicalTime = null;
    List<ReferenceFile> dirs = new ArrayList<>();
    Map<StoredTabletFile,DataFileValue> newFiles = new HashMap<>();
    TabletMetadata firstTabletMeta = null;
    TabletMetadata lastTabletMeta = null;

    try (var tabletsMetadata = manager.getContext().getAmple().readTablets()
        .forTable(range.tableId()).overlapping(range.prevEndRow(), range.endRow())
        .fetch(OPID, LOCATION, HOSTING_GOAL, FILES, TIME, DIR, ECOMP, PREV_ROW).build()) {

      int tabletsSeen = 0;

      for (var tabletMeta : tabletsMetadata) {
        Preconditions.checkState(lastTabletMeta == null,
            "%s unexpectedly saw multiple last tablets %s %s", fateStr, tabletMeta.getExtent(),
            range);
        validateTablet(tabletMeta, fateStr, opid, data.tableId);

        if (firstTabletMeta == null) {
          firstTabletMeta = Objects.requireNonNull(tabletMeta);
        }

        tabletsSeen++;

        // want to gather the following for all tablets, including the last tablet
        maxLogicalTime = TabletTime.maxMetadataTime(maxLogicalTime, tabletMeta.getTime());
        goals.add(tabletMeta.getHostingGoal());

        // determine if this is the last tablet in the merge range
        boolean isLastTablet = (range.endRow() == null && tabletMeta.getExtent().endRow() == null)
            || (range.endRow() != null && tabletMeta.getExtent().contains(range.endRow()));
        if (isLastTablet) {
          lastTabletMeta = tabletMeta;
        } else {
          // files for the last tablet need to specially handled, so only add other tablets files
          // here
          tabletMeta.getFilesMap().forEach((file, dfv) -> {
            newFiles.put(fenceFile(tabletMeta.getExtent(), file), dfv);
          });

          // queue all tablets dirs except the last tablets to be added as GC candidates
          dirs.add(new AllVolumesDirectory(range.tableId(), tabletMeta.getDirName()));
        }
      }

      if (tabletsSeen == 1) {
        // The merge range overlaps a single tablet, so there is nothing to do. This could be
        // because there was only a single tablet before merge started or this operation completed
        // but the process died and now its running a 2nd time.
        return;
      }

      Preconditions.checkState(lastTabletMeta != null, "%s no tablets seen in range %s", opid,
          lastTabletMeta);
    }

    log.info("{} merge low tablet {}", fateStr, firstTabletMeta.getExtent());
    log.info("{} merge high tablet {}", fateStr, lastTabletMeta.getExtent());

    // Check if the last tablet was already updated, this could happen if a process died and this
    // code is running a 2nd time. If running a 2nd time it possible the last tablet was updated and
    // only a subset of the other tablets were deleted. If the last tablet was never updated, then
    // its prev row should be the greatest.
    Comparator<Text> prevRowComparator = Comparator.nullsFirst(Text::compareTo);
    if (prevRowComparator.compare(firstTabletMeta.getPrevEndRow(), lastTabletMeta.getPrevEndRow())
        < 0) {
      // update the last tablet
      try (var tabletsMutator = manager.getContext().getAmple().conditionallyMutateTablets()) {
        var lastExtent = lastTabletMeta.getExtent();
        var tabletMutator =
            tabletsMutator.mutateTablet(lastExtent).requireOperation(opid).requireAbsentLocation();

        // fence the files in the last tablet if needed
        lastTabletMeta.getFilesMap().forEach((file, dfv) -> {
          var fencedFile = fenceFile(lastExtent, file);
          // If the existing metadata does not match then we need to delete the old
          // and replace with a new range
          if (!fencedFile.equals(file)) {
            tabletMutator.deleteFile(file);
            tabletMutator.putFile(fencedFile, dfv);
          }
        });

        newFiles.forEach(tabletMutator::putFile);
        tabletMutator.putTime(maxLogicalTime);
        lastTabletMeta.getExternalCompactions().keySet()
            .forEach(tabletMutator::deleteExternalCompaction);
        tabletMutator.putHostingGoal(DeleteRows.getMergeHostingGoal(range, goals));
        tabletMutator.putPrevEndRow(firstTabletMeta.getPrevEndRow());

        // if the tablet no longer exists (because changed prev end row, then the update was
        // successful.
        tabletMutator.submit(Ample.RejectionHandler.acceptAbsentTablet());

        verifyAccepted(tabletsMutator.process(), fateStr);
      }
    }

    // add gc candidates for the tablet dirs that being merged away, once these dirs are empty the
    // Accumulo GC will delete the dir
    manager.getContext().getAmple().putGcFileAndDirCandidates(range.tableId(), dirs);

    // delete tablets
    try (
        var tabletsMetadata =
            manager.getContext().getAmple().readTablets().forTable(range.tableId())
                .overlapping(range.prevEndRow(), range.endRow()).saveKeyValues().build();
        var tabletsMutator = manager.getContext().getAmple().conditionallyMutateTablets()) {

      for (var tabletMeta : tabletsMetadata) {
        validateTablet(tabletMeta, fateStr, opid, data.tableId);

        // do not delete the last tablet
        if (Objects.equals(tabletMeta.getExtent().endRow(), lastTabletMeta.getExtent().endRow())) {
          break;
        }

        var tabletMutator = tabletsMutator.mutateTablet(tabletMeta.getExtent())
            .requireOperation(opid).requireAbsentLocation();

        tabletMeta.getKeyValues().keySet().forEach(key -> {
          log.debug("{} deleting {}", fateStr, key);
        });

        tabletMutator.deleteAll(tabletMeta.getKeyValues().keySet());
        // if the tablet no longer exists, then it was successful
        tabletMutator.submit(Ample.RejectionHandler.acceptAbsentTablet());
      }

      verifyAccepted(tabletsMutator.process(), fateStr);
    }
  }

  static void validateTablet(TabletMetadata tabletMeta, String fateStr, TabletOperationId opid,
      TableId expectedTableId) {
    // its expected at this point that tablets have our operation id and no location, so lets
    // check that
    Preconditions.checkState(tabletMeta.getLocation() == null,
        "%s merging tablet %s had location %s", fateStr, tabletMeta.getExtent(),
        tabletMeta.getLocation());
    Preconditions.checkState(opid.equals(tabletMeta.getOperationId()),
        "%s merging tablet %s had unexpected opid %s", fateStr, tabletMeta.getExtent(),
        tabletMeta.getOperationId());
    Preconditions.checkState(expectedTableId.equals(tabletMeta.getTableId()),
        "%s tablet %s has unexpected table id %s expected %s", fateStr, tabletMeta.getExtent(),
        tabletMeta.getTableId(), expectedTableId);
  }

  /**
   * Fence a file to a tablets data range.
   */
  private static StoredTabletFile fenceFile(KeyExtent extent, StoredTabletFile file) {
    Range fenced = extent.toDataRange();
    // Clip range if exists
    fenced = file.hasRange() ? file.getRange().clip(fenced) : fenced;
    return StoredTabletFile.of(file.getPath(), fenced);
  }
}

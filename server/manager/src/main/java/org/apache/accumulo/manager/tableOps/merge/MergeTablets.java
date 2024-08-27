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

import static org.apache.accumulo.manager.tableOps.merge.DeleteRows.verifyAccepted;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import org.apache.accumulo.core.client.admin.TabletAvailability;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.fate.FateId;
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
  public Repo<Manager> call(FateId fateId, Manager manager) throws Exception {
    KeyExtent range = data.getMergeExtent();
    log.debug("{} Merging metadata for {}", fateId, range);

    var opid = TabletOperationId.from(TabletOperationType.MERGING, fateId);
    Set<TabletAvailability> tabletAvailabilities = new HashSet<>();
    MetadataTime maxLogicalTime = null;
    List<ReferenceFile> dirs = new ArrayList<>();
    Map<StoredTabletFile,DataFileValue> newFiles = new HashMap<>();
    TabletMetadata firstTabletMeta = null;
    TabletMetadata lastTabletMeta = null;

    try (var tabletsMetadata = manager.getContext().getAmple().readTablets()
        .forTable(range.tableId()).overlapping(range.prevEndRow(), range.endRow()).build()) {

      int tabletsSeen = 0;

      KeyExtent prevExtent = null;

      for (var tabletMeta : tabletsMetadata) {
        Preconditions.checkState(lastTabletMeta == null,
            "%s unexpectedly saw multiple last tablets %s %s", fateId, tabletMeta.getExtent(),
            range);
        validateTablet(tabletMeta, fateId, opid, data.tableId);

        if (firstTabletMeta == null) {
          firstTabletMeta = Objects.requireNonNull(tabletMeta);
        }

        // determine if this is the last tablet in the merge range
        boolean isLastTablet = (range.endRow() == null && tabletMeta.getExtent().endRow() == null)
            || (range.endRow() != null && tabletMeta.getExtent().contains(range.endRow()));

        if (prevExtent == null) {
          prevExtent = tabletMeta.getExtent();
        } else {
          boolean pointsToPrevious =
              Objects.equals(prevExtent.endRow(), tabletMeta.getExtent().prevEndRow());
          boolean isAlreadyMerged = isLastTablet && tabletMeta.hasMerged()
              && Objects.equals(firstTabletMeta.getPrevEndRow(), tabletMeta.getPrevEndRow());

          // Need to ensure the tablets being merged form a proper linked list. In the case where
          // this operation is running a second time the last tablet will not form a linked list and
          // that is ok.
          Preconditions.checkState(pointsToPrevious || isAlreadyMerged,
              "%s unexpectedly saw a hole in the metadata table %s %s", fateId, prevExtent,
              tabletMeta.getExtent());
          prevExtent = tabletMeta.getExtent();
        }

        tabletsSeen++;

        // want to gather the following for all tablets, including the last tablet
        maxLogicalTime = TabletTime.maxMetadataTime(maxLogicalTime, tabletMeta.getTime());
        tabletAvailabilities.add(tabletMeta.getTabletAvailability());

        if (isLastTablet) {
          lastTabletMeta = tabletMeta;
        } else {
          // only expect to see the merged marker in the last tablet
          Preconditions.checkState(!tabletMeta.hasMerged(),
              "%s tablet %s has unexpected merge marker", fateId, tabletMeta.getExtent());

          // files for the last tablet need to specially handled, so only add other tablets files
          // here
          tabletMeta.getFilesMap().forEach((file, dfv) -> {
            newFiles.put(fenceFile(tabletMeta.getExtent(), file), dfv);
          });

          // queue all tablets dirs except the last tablets to be added as GC candidates
          dirs.add(new AllVolumesDirectory(range.tableId(), tabletMeta.getDirName()));
          if (dirs.size() > 1000) {
            Preconditions.checkState(tabletsSeen > 1);
            manager.getContext().getAmple().putGcFileAndDirCandidates(range.tableId(), dirs);
            dirs.clear();
          }
        }
      }

      if (tabletsSeen == 1) {
        // The merge range overlaps a single tablet, so there is nothing to do. This could be
        // because there was only a single tablet before merge started or this operation completed
        // but the process died and now its running a 2nd time.
        return new FinishTableRangeOp(data);
      }

      Preconditions.checkState(lastTabletMeta != null, "%s no tablets seen in range %s", opid,
          lastTabletMeta);
    }

    log.info("{} merge low tablet {}", fateId, firstTabletMeta.getExtent());
    log.info("{} merge high tablet {}", fateId, lastTabletMeta.getExtent());

    // Check if the last tablet was already updated, this could happen if a process died and this
    // code is running a 2nd time. If running a 2nd time it possible the last tablet was updated and
    // only a subset of the other tablets were deleted. If the last tablet was never updated, then
    // the merged marker should not exist
    if (!lastTabletMeta.hasMerged()) {
      // update the last tablet
      try (var tabletsMutator = manager.getContext().getAmple().conditionallyMutateTablets()) {
        var lastExtent = lastTabletMeta.getExtent();
        var tabletMutator = tabletsMutator.mutateTablet(lastExtent).requireOperation(opid)
            .requireAbsentLocation().requireAbsentLogs();

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
        tabletMutator.putTabletAvailability(
            DeleteRows.getMergeTabletAvailability(range, tabletAvailabilities));
        tabletMutator.putPrevEndRow(firstTabletMeta.getPrevEndRow());

        // scan entries are related to a hosted tablet, this tablet is not hosted so can safely
        // delete these
        lastTabletMeta.getScans().forEach(tabletMutator::deleteScan);

        if (lastTabletMeta.getHostingRequested()) {
          // The range of the tablet is changing, so let's delete the hosting requested column in
          // case this tablet does not actually need to be hosted.
          tabletMutator.deleteHostingRequested();
        }

        if (lastTabletMeta.getSuspend() != null) {
          // This no longer the exact tablet that was suspended, so let's delete the suspend marker.
          tabletMutator.deleteSuspension();
        }

        if (lastTabletMeta.getLast() != null) {
          // This is no longer the same tablet, so let's delete the last location.
          tabletMutator.deleteLocation(lastTabletMeta.getLast());
        }

        if (lastTabletMeta.getUnSplittable() != null) {
          // This is no longer the same tablet, so let's delete the unsplittable marker
          tabletMutator.deleteUnSplittable();
        }

        // Set merged marker on the last tablet when we are finished
        // so we know that we already updated metadata if the process restarts
        tabletMutator.setMerged();

        // if the tablet no longer exists (because changed prev end row, then the update was
        // successful.
        tabletMutator.submit(Ample.RejectionHandler.acceptAbsentTablet());

        verifyAccepted(tabletsMutator.process(), fateId);
      }
    }

    // add gc candidates for the tablet dirs that being merged away, once these dirs are empty the
    // Accumulo GC will delete the dir
    manager.getContext().getAmple().putGcFileAndDirCandidates(range.tableId(), dirs);

    return new DeleteTablets(data, lastTabletMeta.getEndRow());
  }

  static void validateTablet(TabletMetadata tabletMeta, FateId fateId, TabletOperationId opid,
      TableId expectedTableId) {
    // its expected at this point that tablets have our operation id and no location, so lets
    // check that
    Preconditions.checkState(tabletMeta.getLocation() == null,
        "%s merging tablet %s had location %s", fateId, tabletMeta.getExtent(),
        tabletMeta.getLocation());
    Preconditions.checkState(opid.equals(tabletMeta.getOperationId()),
        "%s merging tablet %s had unexpected opid %s", fateId, tabletMeta.getExtent(),
        tabletMeta.getOperationId());
    Preconditions.checkState(expectedTableId.equals(tabletMeta.getTableId()),
        "%s tablet %s has unexpected table id %s expected %s", fateId, tabletMeta.getExtent(),
        tabletMeta.getTableId(), expectedTableId);
    Preconditions.checkState(tabletMeta.getLogs().isEmpty(),
        "%s merging tablet %s has unexpected walogs %s", fateId, tabletMeta.getExtent(),
        tabletMeta.getLogs().size());
    // The table lock that merge gets should prevent concurrent table compactions and bulk imports
    // from running. Therefore metadata columns related to these operations are unexpected and not
    // currently handled by the merge code. Since the code does not handle them, throw an exception
    // if they are seen.
    Preconditions.checkState(tabletMeta.getCompacted().isEmpty(),
        "%s tablet %s has unexpected compacted columns", fateId, tabletMeta.getExtent());
    Preconditions.checkState(tabletMeta.getSelectedFiles() == null,
        "%s tablet %s has unexpected selected file column", fateId, tabletMeta.getExtent());
    Preconditions.checkState(tabletMeta.getUserCompactionsRequested().isEmpty(),
        "%s tablet %s has unexpected use compaction requested column", fateId,
        tabletMeta.getExtent());
    Preconditions.checkState(tabletMeta.getLoaded().isEmpty(),
        "%s tablet %s has unexpected loaded column", fateId, tabletMeta.getExtent());
    Preconditions.checkState(tabletMeta.getCloned() == null,
        "%s tablet %s has unexpected cloned column", fateId, tabletMeta.getExtent());
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

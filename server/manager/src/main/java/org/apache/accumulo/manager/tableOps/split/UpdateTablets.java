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
package org.apache.accumulo.manager.tableOps.split;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.accumulo.core.client.admin.TabletMergeability;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.fate.FateId;
import org.apache.accumulo.core.fate.Repo;
import org.apache.accumulo.core.file.FileSKVIterator;
import org.apache.accumulo.core.metadata.StoredTabletFile;
import org.apache.accumulo.core.metadata.schema.Ample;
import org.apache.accumulo.core.metadata.schema.Ample.ConditionalResult.Status;
import org.apache.accumulo.core.metadata.schema.DataFileValue;
import org.apache.accumulo.core.metadata.schema.TabletMergeabilityMetadata;
import org.apache.accumulo.core.metadata.schema.TabletMetadata;
import org.apache.accumulo.core.metadata.schema.TabletOperationId;
import org.apache.accumulo.core.metadata.schema.TabletOperationType;
import org.apache.accumulo.core.util.RowRangeUtil;
import org.apache.accumulo.manager.Manager;
import org.apache.accumulo.manager.tableOps.ManagerRepo;
import org.apache.accumulo.server.ServerContext;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;

public class UpdateTablets extends ManagerRepo {
  private static final Logger log = LoggerFactory.getLogger(UpdateTablets.class);
  private static final long serialVersionUID = 1L;
  private final SplitInfo splitInfo;
  private final List<String> dirNames;

  public UpdateTablets(SplitInfo splitInfo, List<String> dirNames) {
    this.splitInfo = splitInfo;
    this.dirNames = dirNames;
  }

  @Override
  public Repo<Manager> call(FateId fateId, Manager manager) throws Exception {
    TabletMetadata tabletMetadata =
        manager.getContext().getAmple().readTablet(splitInfo.getOriginal());

    var opid = TabletOperationId.from(TabletOperationType.SPLITTING, fateId);

    if (tabletMetadata == null) {
      // check to see if this operation has already succeeded.
      TabletMetadata newTabletMetadata = manager.getContext().getAmple()
          .readTablet(splitInfo.getTablets().navigableKeySet().last());

      if (newTabletMetadata != null && opid.equals(newTabletMetadata.getOperationId())) {
        // have already created the new tablet and failed before we could return the next step, so
        // lets go ahead and return the next step.
        log.trace(
            "{} creating new tablet was rejected because it existed, operation probably failed before.",
            fateId);
        return new DeleteOperationIds(splitInfo);
      } else {
        throw new IllegalStateException("Tablet is in an unexpected condition "
            + splitInfo.getOriginal() + " " + (newTabletMetadata == null) + " "
            + (newTabletMetadata == null ? null : newTabletMetadata.getOperationId()));
      }
    }

    Preconditions.checkState(opid.equals(tabletMetadata.getOperationId()),
        "Tablet %s does not have expected operation id %s it has %s", splitInfo.getOriginal(), opid,
        tabletMetadata.getOperationId());

    Preconditions.checkState(tabletMetadata.getLocation() == null,
        "Tablet %s unexpectedly has a location %s", splitInfo.getOriginal(),
        tabletMetadata.getLocation());

    Preconditions.checkState(tabletMetadata.getLogs().isEmpty(),
        "Tablet unexpectedly had walogs %s %s %s", fateId, tabletMetadata.getLogs(),
        tabletMetadata.getExtent());

    Preconditions.checkState(!tabletMetadata.hasMerged(),
        "Tablet unexpectedly has a merged marker %s %s", fateId, tabletMetadata.getExtent());

    Preconditions.checkState(tabletMetadata.getCloned() == null,
        "Tablet unexpectedly has a cloned marker %s %s %s", fateId, tabletMetadata.getCloned(),
        tabletMetadata.getExtent());

    var newTablets = splitInfo.getTablets();

    var newTabletsFiles = getNewTabletFiles(fateId, newTablets, tabletMetadata,
        file -> manager.getSplitter().getCachedFileInfo(splitInfo.getOriginal().tableId(), file));

    addNewTablets(fateId, manager, tabletMetadata, opid, newTablets, newTabletsFiles);

    // Only update the original tablet after successfully creating the new tablets, this is
    // important for failure cases where this operation partially runs a then runs again.
    updateExistingTablet(fateId, manager.getContext(), tabletMetadata, opid, newTablets,
        newTabletsFiles);

    return new DeleteOperationIds(splitInfo);
  }

  /**
   * Determine which files from the original tablet go to each new tablet being created by the
   * split.
   */
  static Map<KeyExtent,Map<StoredTabletFile,DataFileValue>> getNewTabletFiles(FateId fateId,
      SortedMap<KeyExtent,TabletMergeability> newTablets, TabletMetadata tabletMetadata,
      Function<StoredTabletFile,FileSKVIterator.FileRange> fileInfoProvider) {

    Map<KeyExtent,Map<StoredTabletFile,DataFileValue>> tabletsFiles = new TreeMap<>();

    newTablets.keySet().forEach(extent -> tabletsFiles.put(extent, new HashMap<>()));

    // determine which files overlap which tablets and their estimated sizes
    tabletMetadata.getFilesMap().forEach((file, dataFileValue) -> {
      FileSKVIterator.FileRange fileRange = fileInfoProvider.apply(file);

      // This predicate is used to determine if a given tablet range overlaps the data in this file.
      Predicate<Range> overlapPredicate;
      if (fileRange == null) {
        // The range is not known, so assume all tablets overlap the file
        overlapPredicate = r -> true;
      } else if (fileRange.empty) {
        // The file is empty so not tablets can overlap it
        overlapPredicate = r -> false;
      } else {
        // check if the tablet range overlaps the file range
        overlapPredicate = range -> range.clip(fileRange.rowRange, true) != null;

        if (!file.getRange().isInfiniteStartKey() || !file.getRange().isInfiniteStopKey()) {
          // Its expected that if a file has a range that the first row and last row will be clipped
          // to be within that range. For that reason this code does not check file.getRange() when
          // making decisions about whether a file should go to a tablet, because its assumed that
          // fileRange will cover that case. Since file.getRange() is not being checked directly
          // this code validates the assumption that fileRange is within file.getRange()

          Preconditions.checkState(
              file.getRange().clip(new Range(fileRange.rowRange.getStartKey().getRow()), false)
                  != null,
              "First row %s computed for file %s did not fall in its range",
              fileRange.rowRange.getStartKey().getRow(), file);

          var lastRow = RowRangeUtil.stripZeroTail(fileRange.rowRange.getEndKey().getRowData());
          Preconditions.checkState(
              file.getRange().clip(new Range(new Text(lastRow.toArray())), false) != null,
              "Last row %s computed for file %s did not fall in its range", lastRow, file);
        }
      }

      // count how many of the new tablets the file will overlap
      double numOverlapping =
          newTablets.keySet().stream().map(KeyExtent::toDataRange).filter(overlapPredicate).count();

      if (numOverlapping == 0) {
        log.debug("{} File {} with range {} that does not overlap tablet {}", fateId, file,
            fileRange, tabletMetadata.getExtent());
      } else {
        // evenly split the tablets estimates between the number of tablets it actually overlaps
        double sizePerTablet = dataFileValue.getSize() / numOverlapping;
        double entriesPerTablet = dataFileValue.getNumEntries() / numOverlapping;

        // add the file to the tablets it overlaps
        newTablets.keySet().forEach(newTablet -> {
          if (overlapPredicate.apply(newTablet.toDataRange())) {
            DataFileValue ndfv = new DataFileValue((long) sizePerTablet, (long) entriesPerTablet,
                dataFileValue.getTime());
            tabletsFiles.get(newTablet).put(file, ndfv);
          }
        });
      }
    });

    if (log.isTraceEnabled()) {
      tabletMetadata.getFilesMap().forEach((f, v) -> {
        log.trace("{} {} original file {} {} {} {}", fateId, tabletMetadata.getExtent(),
            f.getFileName(), f.getRange(), v.getSize(), v.getNumEntries());
      });

      tabletsFiles.forEach((extent, files) -> {
        files.forEach((f, v) -> {
          log.trace("{} {} split file {} {} {} {}", fateId, extent, f.getFileName(), f.getRange(),
              v.getSize(), v.getNumEntries());
        });
      });
    }

    return tabletsFiles;
  }

  private void addNewTablets(FateId fateId, Manager manager, TabletMetadata tabletMetadata,
      TabletOperationId opid, NavigableMap<KeyExtent,TabletMergeability> newTablets,
      Map<KeyExtent,Map<StoredTabletFile,DataFileValue>> newTabletsFiles) {
    Iterator<String> dirNameIter = dirNames.iterator();

    try (var tabletsMutator = manager.getContext().getAmple().conditionallyMutateTablets()) {
      for (var entry : newTablets.entrySet()) {
        var newExtent = entry.getKey();
        var mergeability = entry.getValue();

        if (newExtent.equals(newTablets.navigableKeySet().last())) {
          // Skip the last tablet, its done after successfully adding all new tablets
          continue;
        }

        var mutator = tabletsMutator.mutateTablet(newExtent).requireAbsentTablet();

        mutator.putOperation(opid);
        mutator.putDirName(dirNameIter.next());
        mutator.putTime(tabletMetadata.getTime());
        tabletMetadata.getFlushId().ifPresent(mutator::putFlushId);
        mutator.putPrevEndRow(newExtent.prevEndRow());

        tabletMetadata.getCompacted().forEach(mutator::putCompacted);

        tabletMetadata.getCompacted().forEach(compactedFateId -> log
            .debug("{} copying compacted marker to new child tablet {}", fateId, compactedFateId));

        mutator.putTabletAvailability(tabletMetadata.getTabletAvailability());

        // Null is only expected for the last tablet which is skipped
        Preconditions.checkState(mergeability != null,
            "Null TabletMergeability for extent %s is unexpected", newExtent);
        mutator.putTabletMergeability(
            TabletMergeabilityMetadata.toMetadata(mergeability, manager.getSteadyTime()));
        tabletMetadata.getLoaded().forEach((k, v) -> mutator.putBulkFile(k.getTabletFile(), v));

        newTabletsFiles.get(newExtent).forEach(mutator::putFile);

        mutator.submit(afterMeta -> opid.equals(afterMeta.getOperationId()));
      }

      var results = tabletsMutator.process();
      results.values().forEach(result -> {
        var status = result.getStatus();

        Preconditions.checkState(status == Status.ACCEPTED, "Failed to add new tablet %s %s %s",
            status, splitInfo.getOriginal(), result.getExtent());
      });
    }
  }

  private void updateExistingTablet(FateId fateId, ServerContext ctx, TabletMetadata tabletMetadata,
      TabletOperationId opid, NavigableMap<KeyExtent,TabletMergeability> newTablets,
      Map<KeyExtent,Map<StoredTabletFile,DataFileValue>> newTabletsFiles) {
    try (var tabletsMutator = ctx.getAmple().conditionallyMutateTablets()) {
      var newExtent = newTablets.navigableKeySet().last();

      var mutator = tabletsMutator.mutateTablet(splitInfo.getOriginal()).requireOperation(opid)
          .requireAbsentLocation().requireAbsentLogs();

      Preconditions
          .checkArgument(Objects.equals(tabletMetadata.getExtent().endRow(), newExtent.endRow()));

      mutator.putPrevEndRow(newExtent.prevEndRow());

      newTabletsFiles.get(newExtent).forEach(mutator::putFile);

      // remove the files from the original tablet that did not end up in the tablet
      tabletMetadata.getFiles().forEach(existingFile -> {
        if (!newTabletsFiles.get(newExtent).containsKey(existingFile)) {
          mutator.deleteFile(existingFile);
        }
      });

      // remove any external compaction entries that are present
      tabletMetadata.getExternalCompactions().keySet().forEach(mutator::deleteExternalCompaction);

      tabletMetadata.getExternalCompactions().keySet().forEach(
          ecid -> log.debug("{} deleting external compaction entry for split {}", fateId, ecid));

      // remove any selected file entries that are present, the compaction operation will need to
      // reselect files
      if (tabletMetadata.getSelectedFiles() != null) {
        mutator.deleteSelectedFiles();
        log.debug("{} deleting selected files {} because of split", fateId,
            tabletMetadata.getSelectedFiles().getFateId());
      }

      // Remove any user compaction requested markers as the tablet may fall outside the compaction
      // range. The markers will be recreated if needed.
      tabletMetadata.getUserCompactionsRequested().forEach(mutator::deleteUserCompactionRequested);

      // scan entries are related to a hosted tablet, this tablet is not hosted so can safely delete
      // these
      tabletMetadata.getScans().forEach(mutator::deleteScan);

      if (tabletMetadata.getHostingRequested()) {
        // The range of the tablet is changing, so lets delete the hosting requested column in case
        // this tablet does not actually need to be hosted.
        mutator.deleteHostingRequested();
      }

      if (tabletMetadata.getSuspend() != null) {
        // This no longer the exact tablet that was suspended. For consistency should either delete
        // the suspension marker OR add it to the new tablets. Choosing to delete it.
        mutator.deleteSuspension();
      }

      if (tabletMetadata.getLast() != null) {
        // This is no longer the same tablet so lets delete the last location.
        mutator.deleteLocation(tabletMetadata.getLast());
      }

      // Clean up any previous unsplittable marker
      if (tabletMetadata.getUnSplittable() != null) {
        mutator.deleteUnSplittable();
        log.debug("{} deleting unsplittable metadata from {} because of split", fateId, newExtent);
      }

      var migration = tabletMetadata.getMigration();
      if (migration != null) {
        // This is no longer the same tablet, so delete the migration
        mutator.deleteMigration();
        log.debug("{} deleting migration {} metadata from {} because of split", fateId, migration,
            newExtent);
      }

      // if the tablet no longer exists (because changed prev end row, then the update was
      // successful.
      mutator.submit(Ample.RejectionHandler.acceptAbsentTablet());

      var result = tabletsMutator.process().get(splitInfo.getOriginal());

      Preconditions.checkState(result.getStatus() == Status.ACCEPTED,
          "Failed to update existing tablet in split %s %s %s", fateId, splitInfo.getOriginal(),
          result.getExtent());
    }
  }
}

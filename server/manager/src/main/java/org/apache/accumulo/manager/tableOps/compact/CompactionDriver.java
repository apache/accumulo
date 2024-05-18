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
package org.apache.accumulo.manager.tableOps.compact;

import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.COMPACTED;
import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.ECOMP;
import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.FILES;
import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.OPID;
import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.PREV_ROW;
import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.SELECTED;
import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.USER_COMPACTION_REQUESTED;

import java.time.Duration;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.apache.accumulo.core.client.admin.CompactionConfig;
import org.apache.accumulo.core.clientImpl.AcceptableThriftTableOperationException;
import org.apache.accumulo.core.clientImpl.TableOperationsImpl;
import org.apache.accumulo.core.clientImpl.thrift.TableOperation;
import org.apache.accumulo.core.clientImpl.thrift.TableOperationExceptionType;
import org.apache.accumulo.core.data.NamespaceId;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.fate.FateId;
import org.apache.accumulo.core.fate.Repo;
import org.apache.accumulo.core.fate.zookeeper.ZooReaderWriter;
import org.apache.accumulo.core.logging.TabletLogger;
import org.apache.accumulo.core.metadata.AbstractTabletFile;
import org.apache.accumulo.core.metadata.AccumuloTable;
import org.apache.accumulo.core.metadata.StoredTabletFile;
import org.apache.accumulo.core.metadata.schema.Ample;
import org.apache.accumulo.core.metadata.schema.Ample.ConditionalResult.Status;
import org.apache.accumulo.core.metadata.schema.SelectedFiles;
import org.apache.accumulo.core.metadata.schema.TabletMetadata;
import org.apache.accumulo.core.util.Retry;
import org.apache.accumulo.manager.Manager;
import org.apache.accumulo.manager.tableOps.ManagerRepo;
import org.apache.accumulo.manager.tableOps.bulkVer2.TabletRefresher;
import org.apache.accumulo.manager.tableOps.delete.PreDeleteTable;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.compaction.CompactionConfigStorage;
import org.apache.accumulo.server.compaction.CompactionPluginUtils;
import org.apache.hadoop.io.Text;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

class CompactionDriver extends ManagerRepo {

  private static final Logger log = LoggerFactory.getLogger(CompactionDriver.class);

  private static final long serialVersionUID = 1L;

  private final TableId tableId;
  private final NamespaceId namespaceId;
  private byte[] startRow;
  private byte[] endRow;

  public CompactionDriver(NamespaceId namespaceId, TableId tableId, byte[] startRow,
      byte[] endRow) {
    this.tableId = tableId;
    this.namespaceId = namespaceId;
    this.startRow = startRow;
    this.endRow = endRow;
  }

  @Override
  public long isReady(FateId fateId, Manager manager) throws Exception {

    if (tableId.equals(AccumuloTable.ROOT.tableId())) {
      // this codes not properly handle the root table. See #798
      return 0;
    }

    ZooReaderWriter zoo = manager.getContext().getZooReaderWriter();

    if (isCancelled(fateId, manager.getContext())) {
      // compaction was canceled
      throw new AcceptableThriftTableOperationException(tableId.canonical(), null,
          TableOperation.COMPACT, TableOperationExceptionType.OTHER,
          TableOperationsImpl.COMPACTION_CANCELED_MSG);
    }

    String deleteMarkerPath =
        PreDeleteTable.createDeleteMarkerPath(manager.getInstanceID(), tableId);
    if (zoo.exists(deleteMarkerPath)) {
      // table is being deleted
      throw new AcceptableThriftTableOperationException(tableId.canonical(), null,
          TableOperation.COMPACT, TableOperationExceptionType.OTHER,
          TableOperationsImpl.TABLE_DELETED_MSG);
    }

    long t1 = System.currentTimeMillis();

    int tabletsToWaitFor = updateAndCheckTablets(manager, fateId);

    long scanTime = System.currentTimeMillis() - t1;

    if (tabletsToWaitFor == 0) {
      return 0;
    }

    long sleepTime = 500;

    sleepTime = Math.max(2 * scanTime, sleepTime);

    sleepTime = Math.min(sleepTime, 30000);

    return sleepTime;
  }

  // visible for testing
  protected boolean isCancelled(FateId fateId, ServerContext context)
      throws InterruptedException, KeeperException {
    return CompactionConfigStorage.getConfig(context, fateId) == null;
  }

  public int updateAndCheckTablets(Manager manager, FateId fateId)
      throws AcceptableThriftTableOperationException {

    var ample = manager.getContext().getAmple();

    // This map tracks tablets that had a conditional mutation submitted to select files. If the
    // conditional mutation is successful then want to log a message. Use a concurrent map as the
    // result consumer may run in another thread.
    ConcurrentHashMap<KeyExtent,Set<StoredTabletFile>> selectionsSubmitted =
        new ConcurrentHashMap<>();

    Consumer<Ample.ConditionalResult> resultConsumer = result -> {
      if (result.getStatus() == Status.REJECTED) {
        log.debug("{} update for {} was rejected ", fateId, result.getExtent());
      }

      // always remove extents from the map even if not successful in order to avoid placing too
      // many in memory
      var selected = selectionsSubmitted.remove(result.getExtent());
      if (selected != null && result.getStatus() == Status.ACCEPTED) {
        // successfully selected files so log this
        TabletLogger.selected(fateId, result.getExtent(), selected);
      }
    };

    long t1 = System.currentTimeMillis();

    int complete = 0;
    int total = 0;
    int opidsSeen = 0;
    int noFiles = 0;
    int noneSelected = 0;
    int alreadySelected = 0;
    int otherSelected = 0;
    int userCompactionRequested = 0;
    int userCompactionWaiting = 0;
    int selected = 0;

    KeyExtent minSelected = null;
    KeyExtent maxSelected = null;

    try (
        var tablets = ample.readTablets().forTable(tableId).overlapping(startRow, endRow)
            .fetch(PREV_ROW, COMPACTED, FILES, SELECTED, ECOMP, OPID, USER_COMPACTION_REQUESTED)
            .checkConsistency().build();
        var tabletsMutator = ample.conditionallyMutateTablets(resultConsumer)) {

      CompactionConfig config = CompactionConfigStorage.getConfig(manager.getContext(), fateId);

      for (TabletMetadata tablet : tablets) {

        total++;

        if (tablet.getCompacted().contains(fateId)) {
          // this tablet is already considered done
          log.trace("{} compaction for {} is complete", fateId, tablet.getExtent());
          complete++;
        } else if (tablet.getOperationId() != null) {
          log.trace("{} ignoring tablet {} with active operation {} ", fateId, tablet.getExtent(),
              tablet.getOperationId());
          opidsSeen++;
        } else if (tablet.getFiles().isEmpty()) {
          log.trace("{} tablet {} has no files, attempting to mark as compacted ", fateId,
              tablet.getExtent());
          // this tablet has no files try to mark it as done
          tabletsMutator.mutateTablet(tablet.getExtent()).requireAbsentOperation()
              .requireSame(tablet, FILES, COMPACTED).putCompacted(fateId)
              .submit(tabletMetadata -> tabletMetadata.getCompacted().contains(fateId));
          noFiles++;
        } else if (tablet.getSelectedFiles() == null && tablet.getExternalCompactions().isEmpty()) {
          // there are no selected files
          log.trace("{} selecting {} files compaction for {}", fateId, tablet.getFiles().size(),
              tablet.getExtent());

          Set<StoredTabletFile> filesToCompact;
          try {
            filesToCompact = CompactionPluginUtils.selectFiles(manager.getContext(),
                tablet.getExtent(), config, tablet.getFilesMap());
          } catch (Exception e) {
            log.warn("{} failed to select files for {} using {}", fateId, tablet.getExtent(),
                config.getSelector(), e);
            throw new AcceptableThriftTableOperationException(tableId.canonical(), null,
                TableOperation.COMPACT, TableOperationExceptionType.OTHER,
                "Failed to select files");
          }

          if (log.isTraceEnabled()) {
            log.trace("{} selected {} of {} files for {}", fateId,
                filesToCompact.stream().map(AbstractTabletFile::getFileName)
                    .collect(Collectors.toList()),
                tablet.getFiles().stream().map(AbstractTabletFile::getFileName)
                    .collect(Collectors.toList()),
                tablet.getExtent());
          }
          if (filesToCompact.isEmpty()) {
            // no files were selected so mark the tablet as compacted
            tabletsMutator.mutateTablet(tablet.getExtent()).requireAbsentOperation()
                .requireSame(tablet, FILES, SELECTED, ECOMP, COMPACTED).putCompacted(fateId)
                .submit(tabletMetadata -> tabletMetadata.getCompacted().contains(fateId));

            noneSelected++;
          } else {
            var mutator = tabletsMutator.mutateTablet(tablet.getExtent()).requireAbsentOperation()
                .requireSame(tablet, FILES, SELECTED, ECOMP, COMPACTED, USER_COMPACTION_REQUESTED);
            var selectedFiles = new SelectedFiles(filesToCompact,
                tablet.getFiles().equals(filesToCompact), fateId, manager.getSteadyTime());

            mutator.putSelectedFiles(selectedFiles);

            // We no longer need to include this marker if files are selected
            if (tablet.getUserCompactionsRequested().contains(fateId)) {
              mutator.deleteUserCompactionRequested(fateId);
            }

            selectionsSubmitted.put(tablet.getExtent(), filesToCompact);

            mutator.submit(tabletMetadata -> tabletMetadata.getSelectedFiles() != null
                && tabletMetadata.getSelectedFiles().getFateId().equals(fateId)
                || tabletMetadata.getCompacted().contains(fateId));

            if (minSelected == null || tablet.getExtent().compareTo(minSelected) < 0) {
              minSelected = tablet.getExtent();
            }

            if (maxSelected == null || tablet.getExtent().compareTo(maxSelected) > 0) {
              maxSelected = tablet.getExtent();
            }

            selected++;
          }

        } else if (tablet.getSelectedFiles() != null) {
          if (tablet.getSelectedFiles().getFateId().equals(fateId)) {
            log.trace(
                "{} tablet {} already has {} selected files for this compaction, waiting for them be processed",
                fateId, tablet.getExtent(), tablet.getSelectedFiles().getFiles().size());
            alreadySelected++;
          } else {
            log.trace(
                "{} tablet {} already has {} selected files by another compaction {}, waiting for them be processed",
                fateId, tablet.getExtent(), tablet.getSelectedFiles().getFiles().size(),
                tablet.getSelectedFiles().getFateId());
            otherSelected++;
          }
        } else if (!tablet.getExternalCompactions().isEmpty()) {
          // If there are compactions preventing selection of files, then add
          // selecting marker that prevents new compactions from starting
          if (!tablet.getUserCompactionsRequested().contains(fateId)) {
            log.trace(
                "Another compaction exists for {}, Marking {} as needing a user requested compaction",
                tablet.getExtent(), fateId);
            var mutator = tabletsMutator.mutateTablet(tablet.getExtent()).requireAbsentOperation()
                .requireSame(tablet, ECOMP, USER_COMPACTION_REQUESTED)
                .putUserCompactionRequested(fateId);
            mutator.submit(tm -> tm.getUserCompactionsRequested().contains(fateId));
            userCompactionRequested++;
          } else {
            // Marker was already added and we are waiting
            log.trace("Waiting on {} for previously marked user requested compaction {} to run",
                tablet.getExtent(), fateId);
            userCompactionWaiting++;
          }
        }
      }
    } catch (InterruptedException | KeeperException e) {
      throw new RuntimeException(e);
    }

    long t2 = System.currentTimeMillis();

    // The Fate operation gets a table lock that prevents the table from being deleted while this is
    // running, so seeing zero tablets in the metadata table is unexpected.
    Preconditions.checkState(total > 0,
        "No tablets were seen for table %s in the compaction range %s %s", tableId,
        startRow == null ? new Text() : new Text(startRow),
        endRow == null ? new Text() : new Text(endRow));

    log.debug(
        "{} tablet stats, total:{} complete:{} selected_now:{} selected_prev:{} selected_by_other:{} "
            + "no_files:{} none_selected:{} user_compaction_requested:{} user_compaction_waiting:{} "
            + "opids:{} scan_update_time:{}ms",
        fateId, total, complete, selected, alreadySelected, otherSelected, noFiles, noneSelected,
        userCompactionRequested, userCompactionWaiting, opidsSeen, t2 - t1);

    if (selected > 0) {
      manager.getEventCoordinator().event(
          new KeyExtent(tableId, maxSelected.endRow(), minSelected.prevEndRow()),
          "%s selected files for compaction for %d tablets", fateId, selected);
    }

    return total - complete;
  }

  @Override
  public Repo<Manager> call(FateId fateId, Manager env) throws Exception {
    return new RefreshTablets(tableId, namespaceId, startRow, endRow);
  }

  @Override
  public void undo(FateId fateId, Manager env) throws Exception {
    cleanupTabletMetadata(fateId, env);

    // For any compactions that may have happened before this operation failed, attempt to refresh
    // tablets.
    TabletRefresher.refresh(env, fateId, tableId, startRow, endRow, tabletMetadata -> true);
  }

  /**
   * Cleans up any tablet metadata that may have been added as part of this compaction operation.
   */
  private void cleanupTabletMetadata(FateId fateId, Manager manager) throws Exception {
    var ample = manager.getContext().getAmple();

    boolean allCleanedUp = false;

    Retry retry = Retry.builder().infiniteRetries().retryAfter(Duration.ofMillis(100))
        .incrementBy(Duration.ofMillis(100)).maxWait(Duration.ofSeconds(1)).backOffFactor(1.5)
        .logInterval(Duration.ofMinutes(3)).createRetry();

    while (!allCleanedUp) {

      AtomicLong rejectedCount = new AtomicLong(0);
      Consumer<Ample.ConditionalResult> resultConsumer = result -> {
        if (result.getStatus() == Status.REJECTED) {
          log.debug("{} update for {} was rejected ", fateId, result.getExtent());
          rejectedCount.incrementAndGet();
        }
      };

      try (var tablets = ample.readTablets().forTable(tableId).overlapping(startRow, endRow)
          .fetch(PREV_ROW, COMPACTED, SELECTED, USER_COMPACTION_REQUESTED).checkConsistency()
          .build(); var tabletsMutator = ample.conditionallyMutateTablets(resultConsumer)) {
        Predicate<TabletMetadata> needsUpdate =
            tabletMetadata -> (tabletMetadata.getSelectedFiles() != null
                && tabletMetadata.getSelectedFiles().getFateId().equals(fateId))
                || tabletMetadata.getCompacted().contains(fateId)
                || tabletMetadata.getUserCompactionsRequested().contains(fateId);
        Predicate<TabletMetadata> needsNoUpdate = needsUpdate.negate();

        for (TabletMetadata tablet : tablets) {

          if (needsUpdate.test(tablet)) {
            var mutator = tabletsMutator.mutateTablet(tablet.getExtent()).requireAbsentOperation()
                .requireSame(tablet, COMPACTED, SELECTED);
            if (tablet.getSelectedFiles() != null
                && tablet.getSelectedFiles().getFateId().equals(fateId)) {
              mutator.deleteSelectedFiles();
            }

            if (tablet.getCompacted().contains(fateId)) {
              mutator.deleteCompacted(fateId);
            }
            if (tablet.getUserCompactionsRequested().contains(fateId)) {
              mutator.deleteUserCompactionRequested(fateId);
            }

            mutator.submit(needsNoUpdate::test);
          }
        }
      }

      allCleanedUp = rejectedCount.get() == 0;

      if (!allCleanedUp) {
        retry.waitForNextAttempt(log, "Cleanup metadata for failed compaction " + fateId);
      }
    }
  }

}

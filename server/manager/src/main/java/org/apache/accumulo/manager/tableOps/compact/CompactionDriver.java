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

import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.COMPACT_ID;
import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.ECOMP;
import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.FILES;
import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.OPID;
import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.PREV_ROW;
import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.SELECTED;

import java.util.Set;
import java.util.stream.Collectors;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.admin.CompactionConfig;
import org.apache.accumulo.core.clientImpl.AcceptableThriftTableOperationException;
import org.apache.accumulo.core.clientImpl.TableOperationsImpl;
import org.apache.accumulo.core.clientImpl.thrift.TableOperation;
import org.apache.accumulo.core.clientImpl.thrift.TableOperationExceptionType;
import org.apache.accumulo.core.data.InstanceId;
import org.apache.accumulo.core.data.NamespaceId;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.fate.FateTxId;
import org.apache.accumulo.core.fate.Repo;
import org.apache.accumulo.core.fate.zookeeper.ZooReaderWriter;
import org.apache.accumulo.core.metadata.AbstractTabletFile;
import org.apache.accumulo.core.metadata.RootTable;
import org.apache.accumulo.core.metadata.StoredTabletFile;
import org.apache.accumulo.core.metadata.schema.Ample.ConditionalResult.Status;
import org.apache.accumulo.core.metadata.schema.SelectedFiles;
import org.apache.accumulo.core.metadata.schema.TabletMetadata;
import org.apache.accumulo.core.util.Pair;
import org.apache.accumulo.manager.Manager;
import org.apache.accumulo.manager.tableOps.ManagerRepo;
import org.apache.accumulo.manager.tableOps.delete.PreDeleteTable;
import org.apache.accumulo.server.compaction.CompactionConfigStorage;
import org.apache.accumulo.server.compaction.CompactionPluginUtils;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class CompactionDriver extends ManagerRepo {

  private static final Logger log = LoggerFactory.getLogger(CompactionDriver.class);

  public static String createCompactionCancellationPath(InstanceId instanceId, TableId tableId) {
    return Constants.ZROOT + "/" + instanceId + Constants.ZTABLES + "/" + tableId.canonical()
        + Constants.ZTABLE_COMPACT_CANCEL_ID;
  }

  private static final long serialVersionUID = 1L;

  private long compactId;
  private final TableId tableId;
  private final NamespaceId namespaceId;
  private byte[] startRow;
  private byte[] endRow;

  public CompactionDriver(long compactId, NamespaceId namespaceId, TableId tableId, byte[] startRow,
      byte[] endRow) {
    this.compactId = compactId;
    this.tableId = tableId;
    this.namespaceId = namespaceId;
    this.startRow = startRow;
    this.endRow = endRow;
  }

  @Override
  public long isReady(long tid, Manager manager) throws Exception {

    if (tableId.equals(RootTable.ID)) {
      // this codes not properly handle the root table. See #798
      return 0;
    }

    String zCancelID = createCompactionCancellationPath(manager.getInstanceID(), tableId);
    ZooReaderWriter zoo = manager.getContext().getZooReaderWriter();

    if (Long.parseLong(new String(zoo.getData(zCancelID))) >= compactId) {
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

    int tabletsToWaitFor = updateAndCheckTablets(manager, tid, compactId);

    long scanTime = System.currentTimeMillis() - t1;

    if (tabletsToWaitFor == 0) {
      return 0;
    }

    long sleepTime = 500;

    sleepTime = Math.max(2 * scanTime, sleepTime);

    sleepTime = Math.min(sleepTime, 30000);

    return sleepTime;
  }

  public int updateAndCheckTablets(Manager manager, long tid, long compactId) {

    var ample = manager.getContext().getAmple();

    // ELASTICITY_TODO use existing compaction logging

    try (
        var tablets = ample.readTablets().forTable(tableId).overlapping(startRow, endRow)
            .fetch(PREV_ROW, COMPACT_ID, FILES, SELECTED, ECOMP, OPID).build();
        var tabletsMutator = ample.conditionallyMutateTablets()) {

      int complete = 0;
      int total = 0;

      int selected = 0;

      for (TabletMetadata tablet : tablets) {

        total++;

        // TODO change all logging to trace

        if (tablet.getCompactId().orElse(-1) >= compactId) {
          // this tablet is already considered done
          log.debug("{} compaction for {} is complete", FateTxId.formatTid(tid),
              tablet.getExtent());
          complete++;
        } else if (tablet.getOperationId() != null) {
          log.debug("{} ignoring tablet {} with active operation {} ", FateTxId.formatTid(tid),
              tablet.getExtent(), tablet.getOperationId());
        } else if (tablet.getFiles().isEmpty()) {
          log.debug("{} tablet {} has no files, attempting to mark as compacted ",
              FateTxId.formatTid(tid), tablet.getExtent());
          // this tablet has no files try to mark it as done
          tabletsMutator.mutateTablet(tablet.getExtent()).requireAbsentOperation()
              .requireSame(tablet, PREV_ROW, FILES, COMPACT_ID).putCompactionId(compactId)
              .submit(tabletMetadata -> tabletMetadata.getCompactId().orElse(-1) >= compactId);
        } else if (tablet.getSelectedFiles() == null && tablet.getExternalCompactions().isEmpty()) {
          // there are no selected files
          log.debug("{} selecting {} files compaction for {}", FateTxId.formatTid(tid),
              tablet.getFiles().size(), tablet.getExtent());

          // ELASTICITY_TODO this is inefficient, going to zookeeper for each tablet... Having per
          // fate
          // transaction config lends itself to caching very well because the config related to the
          // fate txid is fixed and is not changing
          Pair<Long,CompactionConfig> comactionConfig = null;
          try {
            comactionConfig =
                CompactionConfigStorage.getCompactionID(manager.getContext(), tablet.getExtent());
          } catch (KeeperException.NoNodeException e) {
            throw new RuntimeException(e);
          }

          Set<StoredTabletFile> filesToCompact =
              CompactionPluginUtils.selectFiles(manager.getContext(), tablet.getExtent(),
                  comactionConfig.getSecond(), tablet.getFilesMap());

          // TODO expensive logging
          log.debug("{} selected {} of {} files for {}", FateTxId.formatTid(tid),
              filesToCompact.stream().map(AbstractTabletFile::getFileName)
                  .collect(Collectors.toList()),
              tablet.getFiles().stream().map(AbstractTabletFile::getFileName)
                  .collect(Collectors.toList()),
              tablet.getExtent());

          if (filesToCompact.isEmpty()) {
            // no files were selected so mark the tablet as compacted
            tabletsMutator.mutateTablet(tablet.getExtent()).requireAbsentOperation()
                .requireSame(tablet, PREV_ROW, FILES, COMPACT_ID).putCompactionId(compactId)
                .submit(tabletMetadata -> tabletMetadata.getCompactId().orElse(-1) >= compactId);
          } else {
            var mutator = tabletsMutator.mutateTablet(tablet.getExtent()).requireAbsentOperation()
                .requireSame(tablet, PREV_ROW, FILES, SELECTED, ECOMP, COMPACT_ID);
            var selectedFiles =
                new SelectedFiles(filesToCompact, tablet.getFiles().equals(filesToCompact), tid);

            mutator.putSelectedFiles(selectedFiles);

            mutator.submit(tabletMetadata -> tabletMetadata.getSelectedFiles() != null
                && tabletMetadata.getSelectedFiles().getMetadataValue()
                    .equals(selectedFiles.getMetadataValue()));

            selected++;
          }

        } else if (tablet.getSelectedFiles() != null
            && tablet.getSelectedFiles().getFateTxId() == tid) {
          log.debug(
              "{} tablet {} already has {} selected files for this compaction, waiting for them be processed",
              FateTxId.formatTid(tid), tablet.getExtent(),
              tablet.getSelectedFiles().getFiles().size());
        } else {
          // ELASTICITY_TODO if there are compactions preventing selection of files, then add
          // selecting marker that prevents new compactions from starting
        }
      }

      tabletsMutator.process().values().stream()
          .filter(result -> result.getStatus() == Status.REJECTED)
          .forEach(result -> log.debug("{} update for {} was rejected ", FateTxId.formatTid(tid),
              result.getExtent()));

      if (selected > 0) {
        // selected files for some tablets, send a notification to get the tablet group watcher to
        // scan for tablets to compact
        manager.getEventCoordinator().event("%s selected files for compaction for %d tablets",
            FateTxId.formatTid(tid), selected);
      }

      return total - complete;
    }

    // ELASTICITIY_TODO need to handle seeing zero tablets
  }

  @Override
  public Repo<Manager> call(long tid, Manager env) throws Exception {
    return new RefreshTablets(tableId, namespaceId, startRow, endRow);
  }

  @Override
  public void undo(long tid, Manager environment) {

  }

}

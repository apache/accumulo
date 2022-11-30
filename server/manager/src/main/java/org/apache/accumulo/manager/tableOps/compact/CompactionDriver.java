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
import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.LOCATION;
import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.PREV_ROW;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.clientImpl.AcceptableThriftTableOperationException;
import org.apache.accumulo.core.clientImpl.TableOperationsImpl;
import org.apache.accumulo.core.clientImpl.thrift.TableOperation;
import org.apache.accumulo.core.clientImpl.thrift.TableOperationExceptionType;
import org.apache.accumulo.core.data.InstanceId;
import org.apache.accumulo.core.data.NamespaceId;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.fate.Repo;
import org.apache.accumulo.core.fate.zookeeper.ZooReaderWriter;
import org.apache.accumulo.core.manager.state.tables.TableState;
import org.apache.accumulo.core.metadata.RootTable;
import org.apache.accumulo.core.metadata.TServerInstance;
import org.apache.accumulo.core.metadata.schema.TabletMetadata;
import org.apache.accumulo.core.metadata.schema.TabletsMetadata;
import org.apache.accumulo.core.util.MapCounter;
import org.apache.accumulo.manager.Manager;
import org.apache.accumulo.manager.tableOps.ManagerRepo;
import org.apache.accumulo.manager.tableOps.Utils;
import org.apache.accumulo.manager.tableOps.delete.PreDeleteTable;
import org.apache.accumulo.server.manager.LiveTServerSet.TServerConnection;
import org.apache.thrift.TException;
import org.slf4j.LoggerFactory;

class CompactionDriver extends ManagerRepo {

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

    MapCounter<TServerInstance> serversToFlush = new MapCounter<>();
    long t1 = System.currentTimeMillis();

    int tabletsToWaitFor = 0;
    int tabletCount = 0;

    TabletsMetadata tablets = TabletsMetadata.builder(manager.getContext()).forTable(tableId)
        .overlapping(startRow, endRow).fetch(LOCATION, PREV_ROW, COMPACT_ID).build();

    for (TabletMetadata tablet : tablets) {
      if (tablet.getCompactId().orElse(-1) < compactId) {
        tabletsToWaitFor++;
        if (tablet.hasCurrent()) {
          serversToFlush.increment(tablet.getLocation(), 1);
        }
      }

      tabletCount++;
    }

    long scanTime = System.currentTimeMillis() - t1;

    manager.getContext().clearTableListCache();
    if (tabletCount == 0 && !manager.getContext().tableNodeExists(tableId)) {
      throw new AcceptableThriftTableOperationException(tableId.canonical(), null,
          TableOperation.COMPACT, TableOperationExceptionType.NOTFOUND, null);
    }

    if (serversToFlush.size() == 0
        && manager.getContext().getTableState(tableId) == TableState.OFFLINE) {
      throw new AcceptableThriftTableOperationException(tableId.canonical(), null,
          TableOperation.COMPACT, TableOperationExceptionType.OFFLINE, null);
    }

    if (tabletsToWaitFor == 0) {
      return 0;
    }

    for (TServerInstance tsi : serversToFlush.keySet()) {
      try {
        final TServerConnection server = manager.getConnection(tsi);
        if (server != null) {
          server.compact(manager.getManagerLock(), tableId.canonical(), startRow, endRow);
        }
      } catch (TException ex) {
        LoggerFactory.getLogger(CompactionDriver.class).error(ex.toString());
      }
    }

    long sleepTime = 500;

    // make wait time depend on the server with the most to compact
    if (serversToFlush.size() > 0) {
      sleepTime = serversToFlush.max() * sleepTime;
    }

    sleepTime = Math.max(2 * scanTime, sleepTime);

    sleepTime = Math.min(sleepTime, 30000);

    return sleepTime;
  }

  @Override
  public Repo<Manager> call(long tid, Manager env) throws Exception {
    CompactRange.removeIterators(env, tid, tableId);
    Utils.getReadLock(env, tableId, tid).unlock();
    Utils.getReadLock(env, namespaceId, tid).unlock();
    return null;
  }

  @Override
  public void undo(long tid, Manager environment) {

  }

}

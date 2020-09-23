/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.accumulo.master.tableOps.compact;

import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.COMPACT_ID;
import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.LOCATION;
import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.PREV_ROW;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.clientImpl.AcceptableThriftTableOperationException;
import org.apache.accumulo.core.clientImpl.Tables;
import org.apache.accumulo.core.clientImpl.thrift.TableOperation;
import org.apache.accumulo.core.clientImpl.thrift.TableOperationExceptionType;
import org.apache.accumulo.core.data.NamespaceId;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.master.state.tables.TableState;
import org.apache.accumulo.core.metadata.RootTable;
import org.apache.accumulo.core.metadata.schema.TabletMetadata;
import org.apache.accumulo.core.metadata.schema.TabletsMetadata;
import org.apache.accumulo.core.util.MapCounter;
import org.apache.accumulo.fate.Repo;
import org.apache.accumulo.fate.zookeeper.ZooReaderWriter;
import org.apache.accumulo.master.Master;
import org.apache.accumulo.master.tableOps.MasterRepo;
import org.apache.accumulo.master.tableOps.Utils;
import org.apache.accumulo.server.master.LiveTServerSet.TServerConnection;
import org.apache.accumulo.server.master.state.TServerInstance;
import org.apache.thrift.TException;
import org.slf4j.LoggerFactory;

class CompactionDriver extends MasterRepo {

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
  public long isReady(long tid, Master master) throws Exception {

    if (tableId.equals(RootTable.ID)) {
      // this codes not properly handle the root table. See #798
      return 0;
    }

    String zCancelID = Constants.ZROOT + "/" + master.getInstanceID() + Constants.ZTABLES + "/"
        + tableId + Constants.ZTABLE_COMPACT_CANCEL_ID;

    ZooReaderWriter zoo = master.getContext().getZooReaderWriter();

    if (Long.parseLong(new String(zoo.getData(zCancelID, null))) >= compactId) {
      // compaction was canceled
      throw new AcceptableThriftTableOperationException(tableId.canonical(), null,
          TableOperation.COMPACT, TableOperationExceptionType.OTHER, "Compaction canceled");
    }

    MapCounter<TServerInstance> serversToFlush = new MapCounter<>();
    long t1 = System.currentTimeMillis();

    int tabletsToWaitFor = 0;
    int tabletCount = 0;

    TabletsMetadata tablets =
        TabletsMetadata.builder().forTable(tableId).overlapping(startRow, endRow)
            .fetch(LOCATION, PREV_ROW, COMPACT_ID).build(master.getContext());

    for (TabletMetadata tablet : tablets) {
      if (tablet.getCompactId().orElse(-1) < compactId) {
        tabletsToWaitFor++;
        if (tablet.hasCurrent()) {
          serversToFlush.increment(new TServerInstance(tablet.getLocation()), 1);
        }
      }

      tabletCount++;
    }

    long scanTime = System.currentTimeMillis() - t1;

    Tables.clearCache(master.getContext());
    if (tabletCount == 0 && !Tables.exists(master.getContext(), tableId))
      throw new AcceptableThriftTableOperationException(tableId.canonical(), null,
          TableOperation.COMPACT, TableOperationExceptionType.NOTFOUND, null);

    if (serversToFlush.size() == 0
        && Tables.getTableState(master.getContext(), tableId) == TableState.OFFLINE)
      throw new AcceptableThriftTableOperationException(tableId.canonical(), null,
          TableOperation.COMPACT, TableOperationExceptionType.OFFLINE, null);

    if (tabletsToWaitFor == 0)
      return 0;

    for (TServerInstance tsi : serversToFlush.keySet()) {
      try {
        final TServerConnection server = master.getConnection(tsi);
        if (server != null)
          server.compact(master.getMasterLock(), tableId.canonical(), startRow, endRow);
      } catch (TException ex) {
        LoggerFactory.getLogger(CompactionDriver.class).error(ex.toString());
      }
    }

    long sleepTime = 500;

    // make wait time depend on the server with the most to compact
    if (serversToFlush.size() > 0)
      sleepTime = serversToFlush.max() * sleepTime;

    sleepTime = Math.max(2 * scanTime, sleepTime);

    sleepTime = Math.min(sleepTime, 30000);

    return sleepTime;
  }

  @Override
  public Repo<Master> call(long tid, Master env) throws Exception {
    CompactRange.removeIterators(env, tid, tableId);
    Utils.getReadLock(env, tableId, tid).unlock();
    Utils.getReadLock(env, namespaceId, tid).unlock();
    return null;
  }

  @Override
  public void undo(long tid, Master environment) {

  }

}

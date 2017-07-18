/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.accumulo.master.tableOps;

import java.util.Collections;
import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.IsolatedScanner;
import org.apache.accumulo.core.client.RowIterator;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.impl.AcceptableThriftTableOperationException;
import org.apache.accumulo.core.client.impl.Namespace;
import org.apache.accumulo.core.client.impl.Table;
import org.apache.accumulo.core.client.impl.Tables;
import org.apache.accumulo.core.client.impl.thrift.TableOperation;
import org.apache.accumulo.core.client.impl.thrift.TableOperationExceptionType;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.data.impl.KeyExtent;
import org.apache.accumulo.core.master.state.tables.TableState;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.metadata.RootTable;
import org.apache.accumulo.core.metadata.schema.MetadataSchema;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.util.MapCounter;
import org.apache.accumulo.fate.Repo;
import org.apache.accumulo.fate.zookeeper.IZooReaderWriter;
import org.apache.accumulo.master.Master;
import org.apache.accumulo.server.master.LiveTServerSet.TServerConnection;
import org.apache.accumulo.server.master.state.TServerInstance;
import org.apache.accumulo.server.zookeeper.ZooReaderWriter;
import org.apache.hadoop.io.Text;
import org.apache.thrift.TException;
import org.slf4j.LoggerFactory;

class CompactionDriver extends MasterRepo {

  private static final long serialVersionUID = 1L;

  private long compactId;
  private final Table.ID tableId;
  private final Namespace.ID namespaceId;
  private byte[] startRow;
  private byte[] endRow;

  public CompactionDriver(long compactId, Namespace.ID namespaceId, Table.ID tableId, byte[] startRow, byte[] endRow) {
    this.compactId = compactId;
    this.tableId = tableId;
    this.namespaceId = namespaceId;
    this.startRow = startRow;
    this.endRow = endRow;
  }

  @Override
  public long isReady(long tid, Master master) throws Exception {

    String zCancelID = Constants.ZROOT + "/" + master.getInstance().getInstanceID() + Constants.ZTABLES + "/" + tableId + Constants.ZTABLE_COMPACT_CANCEL_ID;

    IZooReaderWriter zoo = ZooReaderWriter.getInstance();

    if (Long.parseLong(new String(zoo.getData(zCancelID, null))) >= compactId) {
      // compaction was canceled
      throw new AcceptableThriftTableOperationException(tableId.canonicalID(), null, TableOperation.COMPACT, TableOperationExceptionType.OTHER,
          "Compaction canceled");
    }

    MapCounter<TServerInstance> serversToFlush = new MapCounter<>();
    Connector conn = master.getConnector();

    Scanner scanner;

    if (tableId.equals(MetadataTable.ID)) {
      scanner = new IsolatedScanner(conn.createScanner(RootTable.NAME, Authorizations.EMPTY));
      scanner.setRange(MetadataSchema.TabletsSection.getRange());
    } else {
      scanner = new IsolatedScanner(conn.createScanner(MetadataTable.NAME, Authorizations.EMPTY));
      Range range = new KeyExtent(tableId, null, startRow == null ? null : new Text(startRow)).toMetadataRange();
      scanner.setRange(range);
    }

    TabletsSection.ServerColumnFamily.COMPACT_COLUMN.fetch(scanner);
    TabletsSection.ServerColumnFamily.DIRECTORY_COLUMN.fetch(scanner);
    scanner.fetchColumnFamily(TabletsSection.CurrentLocationColumnFamily.NAME);

    long t1 = System.currentTimeMillis();
    RowIterator ri = new RowIterator(scanner);

    int tabletsToWaitFor = 0;
    int tabletCount = 0;

    while (ri.hasNext()) {
      Iterator<Entry<Key,Value>> row = ri.next();
      long tabletCompactID = -1;

      TServerInstance server = null;

      Entry<Key,Value> entry = null;
      while (row.hasNext()) {
        entry = row.next();
        Key key = entry.getKey();

        if (TabletsSection.ServerColumnFamily.COMPACT_COLUMN.equals(key.getColumnFamily(), key.getColumnQualifier()))
          tabletCompactID = Long.parseLong(entry.getValue().toString());

        if (TabletsSection.CurrentLocationColumnFamily.NAME.equals(key.getColumnFamily()))
          server = new TServerInstance(entry.getValue(), key.getColumnQualifier());
      }

      if (tabletCompactID < compactId) {
        tabletsToWaitFor++;
        if (server != null)
          serversToFlush.increment(server, 1);
      }

      tabletCount++;

      Text tabletEndRow = new KeyExtent(entry.getKey().getRow(), (Text) null).getEndRow();
      if (tabletEndRow == null || (endRow != null && tabletEndRow.compareTo(new Text(endRow)) >= 0))
        break;
    }

    long scanTime = System.currentTimeMillis() - t1;

    Instance instance = master.getInstance();
    Tables.clearCache(instance);
    if (tabletCount == 0 && !Tables.exists(instance, tableId))
      throw new AcceptableThriftTableOperationException(tableId.canonicalID(), null, TableOperation.COMPACT, TableOperationExceptionType.NOTFOUND, null);

    if (serversToFlush.size() == 0 && Tables.getTableState(instance, tableId) == TableState.OFFLINE)
      throw new AcceptableThriftTableOperationException(tableId.canonicalID(), null, TableOperation.COMPACT, TableOperationExceptionType.OFFLINE, null);

    if (tabletsToWaitFor == 0)
      return 0;

    for (TServerInstance tsi : serversToFlush.keySet()) {
      try {
        final TServerConnection server = master.getConnection(tsi);
        if (server != null)
          server.compact(master.getMasterLock(), tableId.canonicalID(), startRow, endRow);
      } catch (TException ex) {
        LoggerFactory.getLogger(CompactionDriver.class).error(ex.toString());
      }
    }

    long sleepTime = 500;

    if (serversToFlush.size() > 0)
      sleepTime = Collections.max(serversToFlush.values()) * sleepTime; // make wait time depend on the server with the most to
                                                                        // compact

    sleepTime = Math.max(2 * scanTime, sleepTime);

    sleepTime = Math.min(sleepTime, 30000);

    return sleepTime;
  }

  @Override
  public Repo<Master> call(long tid, Master env) throws Exception {
    CompactRange.removeIterators(env, tid, tableId);
    Utils.getReadLock(tableId, tid).unlock();
    Utils.getReadLock(namespaceId, tid).unlock();
    return null;
  }

  @Override
  public void undo(long tid, Master environment) throws Exception {

  }

}

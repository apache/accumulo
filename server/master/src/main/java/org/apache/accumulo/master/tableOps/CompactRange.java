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

import static java.nio.charset.StandardCharsets.UTF_8;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.IsolatedScanner;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.RowIterator;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.admin.CompactionStrategyConfig;
import org.apache.accumulo.core.client.impl.CompactionStrategyConfigUtil;
import org.apache.accumulo.core.client.impl.Tables;
import org.apache.accumulo.core.client.impl.thrift.TableOperation;
import org.apache.accumulo.core.client.impl.thrift.TableOperationExceptionType;
import org.apache.accumulo.core.client.impl.thrift.ThriftTableOperationException;
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
import org.apache.accumulo.fate.zookeeper.IZooReaderWriter.Mutator;
import org.apache.accumulo.master.Master;
import org.apache.accumulo.server.master.LiveTServerSet.TServerConnection;
import org.apache.accumulo.server.master.state.TServerInstance;
import org.apache.accumulo.server.master.tableOps.UserCompactionConfig;
import org.apache.accumulo.server.zookeeper.ZooReaderWriter;
import org.apache.commons.codec.binary.Hex;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableUtils;
import org.apache.thrift.TException;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

class CompactionDriver extends MasterRepo {

  private static final long serialVersionUID = 1L;

  private long compactId;
  private final String tableId;
  private byte[] startRow;
  private byte[] endRow;

  public CompactionDriver(long compactId, String tableId, byte[] startRow, byte[] endRow) {

    this.compactId = compactId;
    this.tableId = tableId;
    this.startRow = startRow;
    this.endRow = endRow;
  }

  @Override
  public long isReady(long tid, Master master) throws Exception {

    String zCancelID = Constants.ZROOT + "/" + master.getInstance().getInstanceID() + Constants.ZTABLES + "/" + tableId + Constants.ZTABLE_COMPACT_CANCEL_ID;

    IZooReaderWriter zoo = ZooReaderWriter.getInstance();

    if (Long.parseLong(new String(zoo.getData(zCancelID, null))) >= compactId) {
      // compaction was canceled
      throw new ThriftTableOperationException(tableId, null, TableOperation.COMPACT, TableOperationExceptionType.OTHER, "Compaction canceled");
    }

    MapCounter<TServerInstance> serversToFlush = new MapCounter<TServerInstance>();
    Connector conn = master.getConnector();

    Scanner scanner;

    if (tableId.equals(MetadataTable.ID)) {
      scanner = new IsolatedScanner(conn.createScanner(RootTable.NAME, Authorizations.EMPTY));
      scanner.setRange(MetadataSchema.TabletsSection.getRange());
    } else {
      scanner = new IsolatedScanner(conn.createScanner(MetadataTable.NAME, Authorizations.EMPTY));
      Range range = new KeyExtent(new Text(tableId), null, startRow == null ? null : new Text(startRow)).toMetadataRange();
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
      throw new ThriftTableOperationException(tableId, null, TableOperation.COMPACT, TableOperationExceptionType.NOTFOUND, null);

    if (serversToFlush.size() == 0 && Tables.getTableState(instance, tableId) == TableState.OFFLINE)
      throw new ThriftTableOperationException(tableId, null, TableOperation.COMPACT, TableOperationExceptionType.OFFLINE, null);

    if (tabletsToWaitFor == 0)
      return 0;

    for (TServerInstance tsi : serversToFlush.keySet()) {
      try {
        final TServerConnection server = master.getConnection(tsi);
        if (server != null)
          server.compact(master.getMasterLock(), tableId, startRow, endRow);
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
  public Repo<Master> call(long tid, Master environment) throws Exception {
    String namespaceId = Tables.getNamespaceId(environment.getInstance(), tableId);
    CompactRange.removeIterators(environment, tid, tableId);
    Utils.getReadLock(tableId, tid).unlock();
    Utils.getReadLock(namespaceId, tid).unlock();
    return null;
  }

  @Override
  public void undo(long tid, Master environment) throws Exception {

  }

}

public class CompactRange extends MasterRepo {

  private static final long serialVersionUID = 1L;
  private final String tableId;
  private byte[] startRow;
  private byte[] endRow;
  private byte[] config;

  public CompactRange(String tableId, byte[] startRow, byte[] endRow, List<IteratorSetting> iterators, CompactionStrategyConfig compactionStrategy)
      throws ThriftTableOperationException {

    Preconditions.checkNotNull(tableId, "Invalid argument: null tableId");
    Preconditions.checkNotNull(iterators, "Invalid argument: null iterator list");
    Preconditions.checkNotNull(compactionStrategy, "Invalid argument: null compactionStrategy");

    this.tableId = tableId;
    this.startRow = startRow.length == 0 ? null : startRow;
    this.endRow = endRow.length == 0 ? null : endRow;

    if (iterators.size() > 0 || !compactionStrategy.equals(CompactionStrategyConfigUtil.DEFAULT_STRATEGY)) {
      this.config = WritableUtils.toByteArray(new UserCompactionConfig(this.startRow, this.endRow, iterators, compactionStrategy));
    } else {
      log.info("No iterators or compaction strategy");
    }

    if (this.startRow != null && this.endRow != null && new Text(startRow).compareTo(new Text(endRow)) >= 0)
      throw new ThriftTableOperationException(tableId, null, TableOperation.COMPACT, TableOperationExceptionType.BAD_RANGE,
          "start row must be less than end row");
  }

  @Override
  public long isReady(long tid, Master environment) throws Exception {
    String namespaceId = Tables.getNamespaceId(environment.getInstance(), tableId);
    return Utils.reserveNamespace(namespaceId, tid, false, true, TableOperation.COMPACT)
        + Utils.reserveTable(tableId, tid, false, true, TableOperation.COMPACT);
  }

  @Override
  public Repo<Master> call(final long tid, Master environment) throws Exception {
    String zTablePath = Constants.ZROOT + "/" + environment.getInstance().getInstanceID() + Constants.ZTABLES + "/" + tableId + Constants.ZTABLE_COMPACT_ID;

    IZooReaderWriter zoo = ZooReaderWriter.getInstance();
    byte[] cid;
    try {
      cid = zoo.mutate(zTablePath, null, null, new Mutator() {
        @Override
        public byte[] mutate(byte[] currentValue) throws Exception {
          String cvs = new String(currentValue, UTF_8);
          String[] tokens = cvs.split(",");
          long flushID = Long.parseLong(tokens[0]);
          flushID++;

          String txidString = String.format("%016x", tid);

          for (int i = 1; i < tokens.length; i++) {
            if (tokens[i].startsWith(txidString))
              continue; // skip self

            log.debug("txidString : {}", txidString);
            log.debug("tokens[{}] : {}", i, tokens[i]);

            throw new ThriftTableOperationException(tableId, null, TableOperation.COMPACT, TableOperationExceptionType.OTHER,
                "Another compaction with iterators and/or a compaction strategy is running");
          }

          StringBuilder encodedIterators = new StringBuilder();

          if (config != null) {
            Hex hex = new Hex();
            encodedIterators.append(",");
            encodedIterators.append(txidString);
            encodedIterators.append("=");
            encodedIterators.append(new String(hex.encode(config), UTF_8));
          }

          return (Long.toString(flushID) + encodedIterators).getBytes(UTF_8);
        }
      });

      return new CompactionDriver(Long.parseLong(new String(cid, UTF_8).split(",")[0]), tableId, startRow, endRow);
    } catch (NoNodeException nne) {
      throw new ThriftTableOperationException(tableId, null, TableOperation.COMPACT, TableOperationExceptionType.NOTFOUND, null);
    }

  }

  static void removeIterators(Master environment, final long txid, String tableId) throws Exception {
    String zTablePath = Constants.ZROOT + "/" + environment.getInstance().getInstanceID() + Constants.ZTABLES + "/" + tableId + Constants.ZTABLE_COMPACT_ID;

    IZooReaderWriter zoo = ZooReaderWriter.getInstance();

    zoo.mutate(zTablePath, null, null, new Mutator() {
      @Override
      public byte[] mutate(byte[] currentValue) throws Exception {
        String cvs = new String(currentValue, UTF_8);
        String[] tokens = cvs.split(",");
        long flushID = Long.parseLong(tokens[0]);

        String txidString = String.format("%016x", txid);

        StringBuilder encodedIterators = new StringBuilder();
        for (int i = 1; i < tokens.length; i++) {
          if (tokens[i].startsWith(txidString))
            continue;
          encodedIterators.append(",");
          encodedIterators.append(tokens[i]);
        }

        return (Long.toString(flushID) + encodedIterators).getBytes(UTF_8);
      }
    });

  }

  @Override
  public void undo(long tid, Master environment) throws Exception {
    String namespaceId = Tables.getNamespaceId(environment.getInstance(), tableId);
    try {
      removeIterators(environment, tid, tableId);
    } finally {
      Utils.unreserveNamespace(namespaceId, tid, false);
      Utils.unreserveTable(tableId, tid, false);
    }
  }

}

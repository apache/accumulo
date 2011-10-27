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
package org.apache.accumulo.server.master.tableOps;

import java.util.Collections;
import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.IsolatedScanner;
import org.apache.accumulo.core.client.RowIterator;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.impl.Tables;
import org.apache.accumulo.core.client.impl.thrift.TableOperation;
import org.apache.accumulo.core.client.impl.thrift.TableOperationExceptionType;
import org.apache.accumulo.core.client.impl.thrift.ThriftTableOperationException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.KeyExtent;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.master.state.tables.TableState;
import org.apache.accumulo.core.util.ColumnFQ;
import org.apache.accumulo.server.client.HdfsZooInstance;
import org.apache.accumulo.server.fate.Repo;
import org.apache.accumulo.server.master.LiveTServerSet.TServerConnection;
import org.apache.accumulo.server.master.Master;
import org.apache.accumulo.server.master.state.TServerInstance;
import org.apache.accumulo.server.security.SecurityConstants;
import org.apache.accumulo.server.util.MapCounter;
import org.apache.accumulo.server.zookeeper.IZooReaderWriter;
import org.apache.accumulo.server.zookeeper.ZooReaderWriter;
import org.apache.accumulo.server.zookeeper.ZooReaderWriter.Mutator;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;
import org.apache.thrift.TException;
import org.apache.zookeeper.KeeperException.NoNodeException;

class CompactionDriver extends MasterRepo {
  
  private static final long serialVersionUID = 1L;
  
  private long compactId;
  private String tableId;
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
    
    MapCounter<TServerInstance> serversToFlush = new MapCounter<TServerInstance>();
    Instance instance = HdfsZooInstance.getInstance();
    Connector conn = instance.getConnector(SecurityConstants.getSystemCredentials());
    Scanner scanner = new IsolatedScanner(conn.createScanner(Constants.METADATA_TABLE_NAME, Constants.NO_AUTHS));
    
    Range range = new KeyExtent(new Text(tableId), null, startRow == null ? null : new Text(startRow)).toMetadataRange();
    
    if (tableId.equals(Constants.METADATA_TABLE_ID))
      range = range.clip(new Range(Constants.ROOT_TABLET_EXTENT.getMetadataEntry(), false, null, true));
    
    scanner.setRange(range);
    ColumnFQ.fetch(scanner, Constants.METADATA_COMPACT_COLUMN);
    ColumnFQ.fetch(scanner, Constants.METADATA_DIRECTORY_COLUMN);
    scanner.fetchColumnFamily(Constants.METADATA_CURRENT_LOCATION_COLUMN_FAMILY);
    
    // TODO since not using tablet iterator, are there any issues w/ splits merges?
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
        
        if (Constants.METADATA_COMPACT_COLUMN.equals(key.getColumnFamily(), key.getColumnQualifier()))
          tabletCompactID = Long.parseLong(entry.getValue().toString());
        
        if (Constants.METADATA_CURRENT_LOCATION_COLUMN_FAMILY.equals(key.getColumnFamily()))
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
        Logger.getLogger(CompactionDriver.class).error(ex.toString());
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
    Utils.getReadLock(tableId, tid).unlock();
    return null;
  }
  
  @Override
  public void undo(long tid, Master environment) throws Exception {
    
  }
  
}

public class CompactRange extends MasterRepo {
  
  private static final long serialVersionUID = 1L;
  private String tableId;
  private byte[] startRow;
  private byte[] endRow;
  
  public CompactRange(String tableId, byte[] startRow, byte[] endRow) throws ThriftTableOperationException {
    this.tableId = tableId;
    this.startRow = startRow.length == 0 ? null : startRow;
    this.endRow = endRow.length == 0 ? null : endRow;
    
    if (this.startRow != null && this.endRow != null && new Text(startRow).compareTo(new Text(endRow)) >= 0)
      throw new ThriftTableOperationException(tableId, null, TableOperation.COMPACT, TableOperationExceptionType.BAD_RANGE,
          "start row must be less than end row");
  }
  
  @Override
  public long isReady(long tid, Master environment) throws Exception {
    return Utils.reserveTable(tableId, tid, false, true, TableOperation.COMPACT);
  }
  
  @Override
  public Repo<Master> call(long tid, Master environment) throws Exception {
    String zTablePath = Constants.ZROOT + "/" + HdfsZooInstance.getInstance().getInstanceID() + Constants.ZTABLES + "/" + tableId + Constants.ZTABLE_COMPACT_ID;
    
    IZooReaderWriter zoo = ZooReaderWriter.getRetryingInstance();
    byte[] cid;
    try {
      cid = zoo.mutate(zTablePath, null, null, new Mutator() {
        @Override
        public byte[] mutate(byte[] currentValue) throws Exception {
          long flushID = Long.parseLong(new String(currentValue));
          flushID++;
          return ("" + flushID).getBytes();
        }
      });
      
      return new CompactionDriver(Long.parseLong(new String(cid)), tableId, startRow, endRow);
    } catch (NoNodeException nne) {
      throw new ThriftTableOperationException(tableId, null, TableOperation.COMPACT, TableOperationExceptionType.NOTFOUND, null);
    }
    
  }
  
  @Override
  public void undo(long tid, Master environment) throws Exception {
    Utils.unreserveTable(tableId, tid, false);
  }
  
}

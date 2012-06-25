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
package org.apache.accumulo.server.test.performance.thrift;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.accumulo.cloudtrace.thrift.TInfo;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.impl.Tables;
import org.apache.accumulo.core.data.KeyExtent;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.thrift.InitialMultiScan;
import org.apache.accumulo.core.data.thrift.InitialScan;
import org.apache.accumulo.core.data.thrift.IterInfo;
import org.apache.accumulo.core.data.thrift.MapFileInfo;
import org.apache.accumulo.core.data.thrift.MultiScanResult;
import org.apache.accumulo.core.data.thrift.ScanResult;
import org.apache.accumulo.core.data.thrift.TColumn;
import org.apache.accumulo.core.data.thrift.TConstraintViolationSummary;
import org.apache.accumulo.core.data.thrift.TKeyExtent;
import org.apache.accumulo.core.data.thrift.TMutation;
import org.apache.accumulo.core.data.thrift.TRange;
import org.apache.accumulo.core.data.thrift.UpdateErrors;
import org.apache.accumulo.core.master.thrift.TabletServerStatus;
import org.apache.accumulo.core.security.thrift.AuthInfo;
import org.apache.accumulo.core.security.thrift.ThriftSecurityException;
import org.apache.accumulo.core.tabletserver.thrift.ActiveScan;
import org.apache.accumulo.core.tabletserver.thrift.TabletClientService;
import org.apache.accumulo.core.tabletserver.thrift.TabletStats;
import org.apache.accumulo.core.util.UtilWaitThread;
import org.apache.accumulo.server.client.ClientServiceHandler;
import org.apache.accumulo.server.client.HdfsZooInstance;
import org.apache.accumulo.server.master.state.Assignment;
import org.apache.accumulo.server.master.state.MetaDataStateStore;
import org.apache.accumulo.server.master.state.MetaDataTableScanner;
import org.apache.accumulo.server.master.state.TServerInstance;
import org.apache.accumulo.server.master.state.TabletLocationState;
import org.apache.accumulo.server.security.SecurityConstants;
import org.apache.accumulo.server.util.TServerUtils;
import org.apache.accumulo.server.zookeeper.TransactionWatcher;
import org.apache.hadoop.io.Text;
import org.apache.thrift.TException;


/**
 * The purpose of this class is to server as fake tserver that is a data sink like /dev/null. NullTserver modifies the !METADATA location entries for a table to
 * point to it. This allows thrift performance to be measured by running any client code that writes to a table.
 * 
 */

public class NullTserver {
  
  public static class ThriftClientHandler extends ClientServiceHandler implements TabletClientService.Iface {
    
    private long updateSession = 1;
    
    public ThriftClientHandler(Instance instance, TransactionWatcher watcher) {
      super(instance, watcher);
    }
    
    @Override
    public long startUpdate(TInfo tinfo, AuthInfo credentials) {
      return updateSession++;
    }
    
    @Override
    public void applyUpdates(TInfo tinfo, long updateID, TKeyExtent keyExtent, List<TMutation> mutation) {}
    
    @Override
    public UpdateErrors closeUpdate(TInfo tinfo, long updateID) {
      return new UpdateErrors(new HashMap<TKeyExtent,Long>(), new ArrayList<TConstraintViolationSummary>(), new ArrayList<TKeyExtent>());
    }
    
    @Override
    public List<TKeyExtent> bulkImport(TInfo tinfo, AuthInfo credentials, long tid, Map<TKeyExtent,Map<String,MapFileInfo>> files, boolean setTime) {
      return null;
    }
    
    @Override
    public void closeMultiScan(TInfo tinfo, long scanID) {}
    
    @Override
    public void closeScan(TInfo tinfo, long scanID) {}
    
    @Override
    public MultiScanResult continueMultiScan(TInfo tinfo, long scanID) {
      return null;
    }
    
    @Override
    public ScanResult continueScan(TInfo tinfo, long scanID) {
      return null;
    }
    
    @Override
    public void splitTablet(TInfo tinfo, AuthInfo credentials, TKeyExtent extent, ByteBuffer splitPoint) {
      
    }
    
    @Override
    public InitialMultiScan startMultiScan(TInfo tinfo, AuthInfo credentials, Map<TKeyExtent,List<TRange>> batch, List<TColumn> columns,
        List<IterInfo> ssiList, Map<String,Map<String,String>> ssio, List<ByteBuffer> authorizations, boolean waitForWrites) {
      return null;
    }
    
    @Override
    public InitialScan startScan(TInfo tinfo, AuthInfo credentials, TKeyExtent extent, TRange range, List<TColumn> columns, int batchSize,
        List<IterInfo> ssiList, Map<String,Map<String,String>> ssio, List<ByteBuffer> authorizations, boolean waitForWrites, boolean isolated) {
      return null;
    }
    
    @Override
    public void update(TInfo tinfo, AuthInfo credentials, TKeyExtent keyExtent, TMutation mutation) {
      
    }
    
    @Override
    public TabletServerStatus getTabletServerStatus(TInfo tinfo, AuthInfo credentials) throws ThriftSecurityException, TException {
      return null;
    }
    
    @Override
    public List<TabletStats> getTabletStats(TInfo tinfo, AuthInfo credentials, String tableId) throws ThriftSecurityException, TException {
      return null;
    }
    
    @Override
    public TabletStats getHistoricalStats(TInfo tinfo, AuthInfo credentials) throws ThriftSecurityException, TException {
      return null;
    }
    
    @Override
    public void halt(TInfo tinfo, AuthInfo credentials, String lock) throws ThriftSecurityException, TException {}
    
    @Override
    public void fastHalt(TInfo tinfo, AuthInfo credentials, String lock) {}
    
    @Override
    public void loadTablet(TInfo tinfo, AuthInfo credentials, String lock, TKeyExtent extent) throws TException {}
    
    @Override
    public void unloadTablet(TInfo tinfo, AuthInfo credentials, String lock, TKeyExtent extent, boolean save) throws TException {}
    
    @Override
    public List<ActiveScan> getActiveScans(TInfo tinfo, AuthInfo credentials) throws ThriftSecurityException, TException {
      return new ArrayList<ActiveScan>();
    }
    
    @Override
    public void chop(TInfo tinfo, AuthInfo credentials, String lock, TKeyExtent extent) throws TException {}
    
    @Override
    public void flushTablet(TInfo tinfo, AuthInfo credentials, String lock, TKeyExtent extent) throws TException {
      
    }
    
    @Override
    public void compact(TInfo tinfo, AuthInfo credentials, String lock, String tableId, ByteBuffer startRow, ByteBuffer endRow) throws TException {
      
    }
    
    @Override
    public void flush(TInfo tinfo, AuthInfo credentials, String lock, String tableId, ByteBuffer startRow, ByteBuffer endRow) throws TException {
      
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see org.apache.accumulo.core.tabletserver.thrift.TabletClientService.Iface#removeLogs(org.apache.accumulo.cloudtrace.thrift.TInfo,
     * org.apache.accumulo.core.security.thrift.AuthInfo, java.util.List)
     */
    @Override
    public void removeLogs(TInfo tinfo, AuthInfo credentials, List<String> filenames) throws TException {
    }
  }
  
  public static void main(String[] args) throws Exception {
    
    String iname = args[0];
    String keepers = args[1];
    String tableName = args[2];
    int port = Integer.parseInt(args[3]);
    
    TransactionWatcher watcher = new TransactionWatcher();
    ThriftClientHandler tch = new ThriftClientHandler(HdfsZooInstance.getInstance(), watcher);
    TabletClientService.Processor processor = new TabletClientService.Processor(tch);
    TServerUtils.startTServer(port, processor, "NullTServer", "null tserver", 2, 1000);
    
    InetSocketAddress addr = new InetSocketAddress(InetAddress.getLocalHost(), port);
    
    // modify !METADATA
    ZooKeeperInstance zki = new ZooKeeperInstance(iname, keepers);
    String tableId = Tables.getTableId(zki, tableName);
    
    // read the locations for the table
    Range tableRange = new KeyExtent(new Text(tableId), null, null).toMetadataRange();
    MetaDataTableScanner s = new MetaDataTableScanner(zki, SecurityConstants.getSystemCredentials(), tableRange);
    long randomSessionID = port;
    TServerInstance instance = new TServerInstance(addr, randomSessionID);
    List<Assignment> assignments = new ArrayList<Assignment>();
    while (s.hasNext()) {
      TabletLocationState next = s.next();
      assignments.add(new Assignment(next.extent, instance));
    }
    s.close();
    // point them to this server
    MetaDataStateStore store = new MetaDataStateStore();
    store.setLocations(assignments);
    
    while (true) {
      UtilWaitThread.sleep(10000);
    }
  }
}

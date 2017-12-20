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
package org.apache.accumulo.test.performance.thrift;

import static org.apache.accumulo.fate.util.UtilWaitThread.sleepUninterruptibly;

import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.cli.Help;
import org.apache.accumulo.core.client.ClientConfiguration;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.impl.Table;
import org.apache.accumulo.core.client.impl.Tables;
import org.apache.accumulo.core.client.impl.thrift.SecurityErrorCode;
import org.apache.accumulo.core.client.impl.thrift.ThriftSecurityException;
import org.apache.accumulo.core.client.impl.thrift.ThriftTableOperationException;
import org.apache.accumulo.core.conf.DefaultConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.impl.KeyExtent;
import org.apache.accumulo.core.data.thrift.InitialMultiScan;
import org.apache.accumulo.core.data.thrift.InitialScan;
import org.apache.accumulo.core.data.thrift.IterInfo;
import org.apache.accumulo.core.data.thrift.MapFileInfo;
import org.apache.accumulo.core.data.thrift.MultiScanResult;
import org.apache.accumulo.core.data.thrift.ScanResult;
import org.apache.accumulo.core.data.thrift.TCMResult;
import org.apache.accumulo.core.data.thrift.TColumn;
import org.apache.accumulo.core.data.thrift.TConditionalMutation;
import org.apache.accumulo.core.data.thrift.TConditionalSession;
import org.apache.accumulo.core.data.thrift.TConstraintViolationSummary;
import org.apache.accumulo.core.data.thrift.TKeyExtent;
import org.apache.accumulo.core.data.thrift.TMutation;
import org.apache.accumulo.core.data.thrift.TRange;
import org.apache.accumulo.core.data.thrift.TRowRange;
import org.apache.accumulo.core.data.thrift.TSummaries;
import org.apache.accumulo.core.data.thrift.TSummaryRequest;
import org.apache.accumulo.core.data.thrift.UpdateErrors;
import org.apache.accumulo.core.master.thrift.TabletServerStatus;
import org.apache.accumulo.core.security.thrift.TCredentials;
import org.apache.accumulo.core.tabletserver.thrift.ActiveCompaction;
import org.apache.accumulo.core.tabletserver.thrift.ActiveScan;
import org.apache.accumulo.core.tabletserver.thrift.NoSuchScanIDException;
import org.apache.accumulo.core.tabletserver.thrift.TDurability;
import org.apache.accumulo.core.tabletserver.thrift.TSamplerConfiguration;
import org.apache.accumulo.core.tabletserver.thrift.TUnloadTabletGoal;
import org.apache.accumulo.core.tabletserver.thrift.TabletClientService;
import org.apache.accumulo.core.tabletserver.thrift.TabletClientService.Iface;
import org.apache.accumulo.core.tabletserver.thrift.TabletClientService.Processor;
import org.apache.accumulo.core.tabletserver.thrift.TabletStats;
import org.apache.accumulo.core.trace.thrift.TInfo;
import org.apache.accumulo.core.util.HostAndPort;
import org.apache.accumulo.server.AccumuloServerContext;
import org.apache.accumulo.server.client.ClientServiceHandler;
import org.apache.accumulo.server.client.HdfsZooInstance;
import org.apache.accumulo.server.conf.ServerConfigurationFactory;
import org.apache.accumulo.server.master.state.Assignment;
import org.apache.accumulo.server.master.state.MetaDataStateStore;
import org.apache.accumulo.server.master.state.MetaDataTableScanner;
import org.apache.accumulo.server.master.state.TServerInstance;
import org.apache.accumulo.server.master.state.TabletLocationState;
import org.apache.accumulo.server.rpc.TServerUtils;
import org.apache.accumulo.server.rpc.ThriftServerType;
import org.apache.accumulo.server.zookeeper.TransactionWatcher;
import org.apache.thrift.TException;

import com.beust.jcommander.Parameter;

/**
 * The purpose of this class is to server as fake tserver that is a data sink like /dev/null. NullTserver modifies the metadata location entries for a table to
 * point to it. This allows thrift performance to be measured by running any client code that writes to a table.
 *
 */

public class NullTserver {

  public static class ThriftClientHandler extends ClientServiceHandler implements TabletClientService.Iface {

    private long updateSession = 1;

    public ThriftClientHandler(AccumuloServerContext context, TransactionWatcher watcher) {
      super(context, watcher, null);
    }

    @Override
    public long startUpdate(TInfo tinfo, TCredentials credentials, TDurability durability) {
      return updateSession++;
    }

    @Override
    public void applyUpdates(TInfo tinfo, long updateID, TKeyExtent keyExtent, List<TMutation> mutation) {}

    @Override
    public UpdateErrors closeUpdate(TInfo tinfo, long updateID) {
      return new UpdateErrors(new HashMap<TKeyExtent,Long>(), new ArrayList<TConstraintViolationSummary>(), new HashMap<TKeyExtent,SecurityErrorCode>());
    }

    @Override
    public List<TKeyExtent> bulkImport(TInfo tinfo, TCredentials credentials, long tid, Map<TKeyExtent,Map<String,MapFileInfo>> files, boolean setTime) {
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
    public void splitTablet(TInfo tinfo, TCredentials credentials, TKeyExtent extent, ByteBuffer splitPoint) {

    }

    @Override
    public InitialMultiScan startMultiScan(TInfo tinfo, TCredentials credentials, Map<TKeyExtent,List<TRange>> batch, List<TColumn> columns,
        List<IterInfo> ssiList, Map<String,Map<String,String>> ssio, List<ByteBuffer> authorizations, boolean waitForWrites, TSamplerConfiguration tsc,
        long batchTimeOut, String context) {
      return null;
    }

    @Override
    public InitialScan startScan(TInfo tinfo, TCredentials credentials, TKeyExtent extent, TRange range, List<TColumn> columns, int batchSize,
        List<IterInfo> ssiList, Map<String,Map<String,String>> ssio, List<ByteBuffer> authorizations, boolean waitForWrites, boolean isolated,
        long readaheadThreshold, TSamplerConfiguration tsc, long batchTimeOut, String classLoaderContext) {
      return null;
    }

    @Override
    public void update(TInfo tinfo, TCredentials credentials, TKeyExtent keyExtent, TMutation mutation, TDurability durability) {

    }

    @Override
    public TabletServerStatus getTabletServerStatus(TInfo tinfo, TCredentials credentials) throws ThriftSecurityException, TException {
      return null;
    }

    @Override
    public List<TabletStats> getTabletStats(TInfo tinfo, TCredentials credentials, String tableId) throws ThriftSecurityException, TException {
      return null;
    }

    @Override
    public TabletStats getHistoricalStats(TInfo tinfo, TCredentials credentials) throws ThriftSecurityException, TException {
      return null;
    }

    @Override
    public void halt(TInfo tinfo, TCredentials credentials, String lock) throws ThriftSecurityException, TException {}

    @Override
    public void fastHalt(TInfo tinfo, TCredentials credentials, String lock) {}

    @Override
    public void loadTablet(TInfo tinfo, TCredentials credentials, String lock, TKeyExtent extent) throws TException {}

    @Override
    public void unloadTablet(TInfo tinfo, TCredentials credentials, String lock, TKeyExtent extent, TUnloadTabletGoal goal, long requestTime)
        throws TException {}

    @Override
    public List<ActiveScan> getActiveScans(TInfo tinfo, TCredentials credentials) throws ThriftSecurityException, TException {
      return new ArrayList<>();
    }

    @Override
    public void chop(TInfo tinfo, TCredentials credentials, String lock, TKeyExtent extent) throws TException {}

    @Override
    public void flushTablet(TInfo tinfo, TCredentials credentials, String lock, TKeyExtent extent) throws TException {

    }

    @Override
    public void compact(TInfo tinfo, TCredentials credentials, String lock, String tableId, ByteBuffer startRow, ByteBuffer endRow) throws TException {

    }

    @Override
    public void flush(TInfo tinfo, TCredentials credentials, String lock, String tableId, ByteBuffer startRow, ByteBuffer endRow) throws TException {

    }

    @Override
    public List<ActiveCompaction> getActiveCompactions(TInfo tinfo, TCredentials credentials) throws ThriftSecurityException, TException {
      return new ArrayList<>();
    }

    @Override
    public TConditionalSession startConditionalUpdate(TInfo tinfo, TCredentials credentials, List<ByteBuffer> authorizations, String tableID,
        TDurability durability, String classLoaderContext) throws ThriftSecurityException, TException {
      return null;
    }

    @Override
    public List<TCMResult> conditionalUpdate(TInfo tinfo, long sessID, Map<TKeyExtent,List<TConditionalMutation>> mutations, List<String> symbols)
        throws NoSuchScanIDException, TException {
      return null;
    }

    @Override
    public void invalidateConditionalUpdate(TInfo tinfo, long sessID) throws TException {}

    @Override
    public void closeConditionalUpdate(TInfo tinfo, long sessID) throws TException {}

    @Override
    public List<String> getActiveLogs(TInfo tinfo, TCredentials credentials) throws TException {
      return null;
    }

    @Override
    public void removeLogs(TInfo tinfo, TCredentials credentials, List<String> filenames) throws TException {}

    @Override
    public TSummaries startGetSummaries(TInfo tinfo, TCredentials credentials, TSummaryRequest request)
        throws ThriftSecurityException, ThriftTableOperationException, NoSuchScanIDException, TException {
      return null;
    }

    @Override
    public TSummaries startGetSummariesForPartition(TInfo tinfo, TCredentials credentials, TSummaryRequest request, int modulus, int remainder)
        throws ThriftSecurityException, NoSuchScanIDException, TException {
      return null;
    }

    @Override
    public TSummaries startGetSummariesFromFiles(TInfo tinfo, TCredentials credentials, TSummaryRequest request, Map<String,List<TRowRange>> files)
        throws ThriftSecurityException, NoSuchScanIDException, TException {
      return null;
    }

    @Override
    public TSummaries contiuneGetSummaries(TInfo tinfo, long sessionId) throws NoSuchScanIDException, TException {
      return null;
    }
  }

  static class Opts extends Help {
    @Parameter(names = {"-i", "--instance"}, description = "instance name", required = true)
    String iname = null;
    @Parameter(names = {"-z", "--keepers"}, description = "comma-separated list of zookeeper host:ports", required = true)
    String keepers = null;
    @Parameter(names = "--table", description = "table to adopt", required = true)
    String tableName = null;
    @Parameter(names = "--port", description = "port number to use")
    int port = DefaultConfiguration.getInstance().getPort(Property.TSERV_CLIENTPORT)[0];
  }

  public static void main(String[] args) throws Exception {
    Opts opts = new Opts();
    opts.parseArgs(NullTserver.class.getName(), args);

    // modify metadata
    ZooKeeperInstance zki = new ZooKeeperInstance(new ClientConfiguration().withInstance(opts.iname).withZkHosts(opts.keepers));
    Instance inst = HdfsZooInstance.getInstance();
    AccumuloServerContext context = new AccumuloServerContext(inst, new ServerConfigurationFactory(zki));

    TransactionWatcher watcher = new TransactionWatcher();
    ThriftClientHandler tch = new ThriftClientHandler(new AccumuloServerContext(inst, new ServerConfigurationFactory(inst)), watcher);
    Processor<Iface> processor = new Processor<>(tch);
    TServerUtils.startTServer(context.getConfiguration(), ThriftServerType.CUSTOM_HS_HA, processor, "NullTServer", "null tserver", 2, 1, 1000, 10 * 1024 * 1024,
        null, null, -1, HostAndPort.fromParts("0.0.0.0", opts.port));

    HostAndPort addr = HostAndPort.fromParts(InetAddress.getLocalHost().getHostName(), opts.port);

    Table.ID tableId = Tables.getTableId(zki, opts.tableName);

    // read the locations for the table
    Range tableRange = new KeyExtent(tableId, null, null).toMetadataRange();
    List<Assignment> assignments = new ArrayList<>();
    try (MetaDataTableScanner s = new MetaDataTableScanner(context, tableRange)) {
      long randomSessionID = opts.port;
      TServerInstance instance = new TServerInstance(addr, randomSessionID);

      while (s.hasNext()) {
        TabletLocationState next = s.next();
        assignments.add(new Assignment(next.extent, instance));
      }
    }
    // point them to this server
    MetaDataStateStore store = new MetaDataStateStore(context);
    store.setLocations(assignments);

    while (true) {
      sleepUninterruptibly(10, TimeUnit.SECONDS);
    }
  }
}

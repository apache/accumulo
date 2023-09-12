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
package org.apache.accumulo.test.performance;

import static org.apache.accumulo.core.util.UtilWaitThread.sleepUninterruptibly;

import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.cli.Help;
import org.apache.accumulo.core.clientImpl.thrift.ClientService;
import org.apache.accumulo.core.clientImpl.thrift.ThriftSecurityException;
import org.apache.accumulo.core.conf.DefaultConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.conf.SiteConfiguration;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.dataImpl.thrift.InitialMultiScan;
import org.apache.accumulo.core.dataImpl.thrift.InitialScan;
import org.apache.accumulo.core.dataImpl.thrift.IterInfo;
import org.apache.accumulo.core.dataImpl.thrift.MapFileInfo;
import org.apache.accumulo.core.dataImpl.thrift.MultiScanResult;
import org.apache.accumulo.core.dataImpl.thrift.ScanResult;
import org.apache.accumulo.core.dataImpl.thrift.TCMResult;
import org.apache.accumulo.core.dataImpl.thrift.TColumn;
import org.apache.accumulo.core.dataImpl.thrift.TConditionalMutation;
import org.apache.accumulo.core.dataImpl.thrift.TConditionalSession;
import org.apache.accumulo.core.dataImpl.thrift.TKeyExtent;
import org.apache.accumulo.core.dataImpl.thrift.TMutation;
import org.apache.accumulo.core.dataImpl.thrift.TRange;
import org.apache.accumulo.core.dataImpl.thrift.TRowRange;
import org.apache.accumulo.core.dataImpl.thrift.TSummaries;
import org.apache.accumulo.core.dataImpl.thrift.TSummaryRequest;
import org.apache.accumulo.core.dataImpl.thrift.UpdateErrors;
import org.apache.accumulo.core.master.thrift.TabletServerStatus;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.metadata.TServerInstance;
import org.apache.accumulo.core.metadata.TabletLocationState;
import org.apache.accumulo.core.metadata.schema.Ample.DataLevel;
import org.apache.accumulo.core.rpc.clients.ThriftClientTypes;
import org.apache.accumulo.core.securityImpl.thrift.TCredentials;
import org.apache.accumulo.core.tabletserver.thrift.ActiveCompaction;
import org.apache.accumulo.core.tabletserver.thrift.ActiveScan;
import org.apache.accumulo.core.tabletserver.thrift.TCompactionQueueSummary;
import org.apache.accumulo.core.tabletserver.thrift.TDurability;
import org.apache.accumulo.core.tabletserver.thrift.TExternalCompactionJob;
import org.apache.accumulo.core.tabletserver.thrift.TSamplerConfiguration;
import org.apache.accumulo.core.tabletserver.thrift.TUnloadTabletGoal;
import org.apache.accumulo.core.tabletserver.thrift.TabletClientService;
import org.apache.accumulo.core.tabletserver.thrift.TabletScanClientService;
import org.apache.accumulo.core.tabletserver.thrift.TabletStats;
import org.apache.accumulo.core.trace.thrift.TInfo;
import org.apache.accumulo.core.util.HostAndPort;
import org.apache.accumulo.core.util.threads.ThreadPools;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.client.ClientServiceHandler;
import org.apache.accumulo.server.manager.state.Assignment;
import org.apache.accumulo.server.manager.state.MetaDataTableScanner;
import org.apache.accumulo.server.manager.state.TabletStateStore;
import org.apache.accumulo.server.rpc.TServerUtils;
import org.apache.accumulo.server.rpc.ThriftProcessorTypes;
import org.apache.accumulo.server.rpc.ThriftServerType;
import org.apache.accumulo.server.zookeeper.TransactionWatcher;
import org.apache.thrift.TException;
import org.apache.thrift.TMultiplexedProcessor;

import com.beust.jcommander.Parameter;

/**
 * The purpose of this class is to server as fake tserver that is a data sink like /dev/null.
 * NullTserver modifies the metadata location entries for a table to point to it. This allows thrift
 * performance to be measured by running any client code that writes to a table.
 */
public class NullTserver {

  public static class NullTServerTabletClientHandler
      implements TabletClientService.Iface, TabletScanClientService.Iface {

    private long updateSession = 1;

    @Override
    public long startUpdate(TInfo tinfo, TCredentials credentials, TDurability durability) {
      return updateSession++;
    }

    @Override
    public void applyUpdates(TInfo tinfo, long updateID, TKeyExtent keyExtent,
        List<TMutation> mutation) {}

    @Override
    public UpdateErrors closeUpdate(TInfo tinfo, long updateID) {
      return new UpdateErrors(new HashMap<>(), new ArrayList<>(), new HashMap<>());
    }

    @Override
    public List<TKeyExtent> bulkImport(TInfo tinfo, TCredentials credentials, long tid,
        Map<TKeyExtent,Map<String,MapFileInfo>> files, boolean setTime) {
      return null;
    }

    @Override
    public void loadFiles(TInfo tinfo, TCredentials credentials, long tid, String dir,
        Map<TKeyExtent,Map<String,MapFileInfo>> fileMap, boolean setTime) {}

    @Override
    public void closeMultiScan(TInfo tinfo, long scanID) {}

    @Override
    public void closeScan(TInfo tinfo, long scanID) {}

    @Override
    public MultiScanResult continueMultiScan(TInfo tinfo, long scanID, long busyTimeout) {
      return null;
    }

    @Override
    public ScanResult continueScan(TInfo tinfo, long scanID, long busyTimeout) {
      return null;
    }

    @Override
    public void splitTablet(TInfo tinfo, TCredentials credentials, TKeyExtent extent,
        ByteBuffer splitPoint) {

    }

    @Override
    public InitialMultiScan startMultiScan(TInfo tinfo, TCredentials credentials,
        Map<TKeyExtent,List<TRange>> batch, List<TColumn> columns, List<IterInfo> ssiList,
        Map<String,Map<String,String>> ssio, List<ByteBuffer> authorizations, boolean waitForWrites,
        TSamplerConfiguration tsc, long batchTimeOut, String context,
        Map<String,String> executionHints, long busyTimeout) {
      return null;
    }

    @Override
    public InitialScan startScan(TInfo tinfo, TCredentials credentials, TKeyExtent extent,
        TRange range, List<TColumn> columns, int batchSize, List<IterInfo> ssiList,
        Map<String,Map<String,String>> ssio, List<ByteBuffer> authorizations, boolean waitForWrites,
        boolean isolated, long readaheadThreshold, TSamplerConfiguration tsc, long batchTimeOut,
        String classLoaderContext, Map<String,String> executionHints, long busyTimeout) {
      return null;
    }

    @Override
    public void update(TInfo tinfo, TCredentials credentials, TKeyExtent keyExtent,
        TMutation mutation, TDurability durability) {

    }

    @Override
    public TabletServerStatus getTabletServerStatus(TInfo tinfo, TCredentials credentials) {
      return null;
    }

    @Override
    public List<TabletStats> getTabletStats(TInfo tinfo, TCredentials credentials, String tableId) {
      return null;
    }

    @Override
    public TabletStats getHistoricalStats(TInfo tinfo, TCredentials credentials) {
      return null;
    }

    @Override
    public void halt(TInfo tinfo, TCredentials credentials, String lock) {}

    @Override
    public void fastHalt(TInfo tinfo, TCredentials credentials, String lock) {}

    @Override
    public void loadTablet(TInfo tinfo, TCredentials credentials, String lock, TKeyExtent extent) {}

    @Override
    public void unloadTablet(TInfo tinfo, TCredentials credentials, String lock, TKeyExtent extent,
        TUnloadTabletGoal goal, long requestTime) {}

    @Override
    public List<ActiveScan> getActiveScans(TInfo tinfo, TCredentials credentials) {
      return new ArrayList<>();
    }

    @Override
    public void chop(TInfo tinfo, TCredentials credentials, String lock, TKeyExtent extent) {}

    @Override
    public void flushTablet(TInfo tinfo, TCredentials credentials, String lock, TKeyExtent extent) {

    }

    @Override
    public void compact(TInfo tinfo, TCredentials credentials, String lock, String tableId,
        ByteBuffer startRow, ByteBuffer endRow) {

    }

    @Override
    public void flush(TInfo tinfo, TCredentials credentials, String lock, String tableId,
        ByteBuffer startRow, ByteBuffer endRow) {

    }

    @Override
    public List<ActiveCompaction> getActiveCompactions(TInfo tinfo, TCredentials credentials) {
      return new ArrayList<>();
    }

    @Override
    public TConditionalSession startConditionalUpdate(TInfo tinfo, TCredentials credentials,
        List<ByteBuffer> authorizations, String tableID, TDurability durability,
        String classLoaderContext) {
      return null;
    }

    @Override
    public List<TCMResult> conditionalUpdate(TInfo tinfo, long sessID,
        Map<TKeyExtent,List<TConditionalMutation>> mutations, List<String> symbols) {
      return null;
    }

    @Override
    public void invalidateConditionalUpdate(TInfo tinfo, long sessID) {}

    @Override
    public void closeConditionalUpdate(TInfo tinfo, long sessID) {}

    @Override
    public List<String> getActiveLogs(TInfo tinfo, TCredentials credentials) {
      return null;
    }

    @Override
    public void removeLogs(TInfo tinfo, TCredentials credentials, List<String> filenames) {}

    @Override
    public TSummaries startGetSummaries(TInfo tinfo, TCredentials credentials,
        TSummaryRequest request) {
      return null;
    }

    @Override
    public TSummaries startGetSummariesForPartition(TInfo tinfo, TCredentials credentials,
        TSummaryRequest request, int modulus, int remainder) {
      return null;
    }

    @Override
    public TSummaries startGetSummariesFromFiles(TInfo tinfo, TCredentials credentials,
        TSummaryRequest request, Map<String,List<TRowRange>> files) {
      return null;
    }

    @Override
    public TSummaries contiuneGetSummaries(TInfo tinfo, long sessionId) {
      return null;
    }

    @Override
    public List<TCompactionQueueSummary> getCompactionQueueInfo(TInfo tinfo,
        TCredentials credentials) throws ThriftSecurityException, TException {
      return null;
    }

    @Override
    public TExternalCompactionJob reserveCompactionJob(TInfo tinfo, TCredentials credentials,
        String queueName, long priority, String compactor, String externalCompactionId)
        throws ThriftSecurityException, TException {
      return null;
    }

    @Override
    public void compactionJobFinished(TInfo tinfo, TCredentials credentials,
        String externalCompactionId, TKeyExtent extent, long fileSize, long entries)
        throws ThriftSecurityException, TException {}

    @Override
    public void compactionJobFailed(TInfo tinfo, TCredentials credentials,
        String externalCompactionId, TKeyExtent extent) throws TException {}

  }

  static class Opts extends Help {
    @Parameter(names = {"-i", "--instance"}, description = "instance name", required = true)
    String iname = null;
    @Parameter(names = {"-z", "--keepers"},
        description = "comma-separated list of zookeeper host:ports", required = true)
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
    int zkTimeOut =
        (int) DefaultConfiguration.getInstance().getTimeInMillis(Property.INSTANCE_ZK_TIMEOUT);
    var siteConfig = SiteConfiguration.auto();
    ServerContext context = ServerContext.override(siteConfig, opts.iname, opts.keepers, zkTimeOut);
    ClientServiceHandler csh = new ClientServiceHandler(context, new TransactionWatcher(context));
    NullTServerTabletClientHandler tch = new NullTServerTabletClientHandler();

    TMultiplexedProcessor muxProcessor = new TMultiplexedProcessor();
    muxProcessor.registerProcessor(ThriftClientTypes.CLIENT.getServiceName(),
        ThriftProcessorTypes.CLIENT.getTProcessor(ClientService.Processor.class,
            ClientService.Iface.class, csh, context));
    muxProcessor.registerProcessor(ThriftClientTypes.TABLET_SERVER.getServiceName(),
        ThriftProcessorTypes.TABLET_SERVER.getTProcessor(TabletClientService.Processor.class,
            TabletClientService.Iface.class, tch, context));
    muxProcessor.registerProcessor(ThriftProcessorTypes.TABLET_SERVER_SCAN.getServiceName(),
        ThriftProcessorTypes.TABLET_SERVER_SCAN.getTProcessor(
            TabletScanClientService.Processor.class, TabletScanClientService.Iface.class, tch,
            context));

    TServerUtils.startTServer(context.getConfiguration(), ThriftServerType.CUSTOM_HS_HA,
        muxProcessor, "NullTServer", "null tserver", 2, ThreadPools.DEFAULT_TIMEOUT_MILLISECS, 1000,
        10 * 1024 * 1024, null, null, -1, context.getConfiguration().getCount(Property.RPC_BACKLOG),
        HostAndPort.fromParts("0.0.0.0", opts.port));

    HostAndPort addr = HostAndPort.fromParts(InetAddress.getLocalHost().getHostName(), opts.port);

    TableId tableId = context.getTableId(opts.tableName);

    // read the locations for the table
    Range tableRange = new KeyExtent(tableId, null, null).toMetaRange();
    List<Assignment> assignments = new ArrayList<>();
    try (var s = new MetaDataTableScanner(context, tableRange, MetadataTable.NAME)) {
      long randomSessionID = opts.port;
      TServerInstance instance = new TServerInstance(addr, randomSessionID);

      while (s.hasNext()) {
        TabletLocationState next = s.next();
        assignments.add(new Assignment(next.extent, instance, next.last));
      }
    }
    // point them to this server
    TabletStateStore store = TabletStateStore.getStoreForLevel(DataLevel.USER, context);
    store.setLocations(assignments);

    while (true) {
      sleepUninterruptibly(10, TimeUnit.SECONDS);
    }
  }
}

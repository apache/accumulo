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

import static java.util.concurrent.TimeUnit.SECONDS;

import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.cli.Help;
import org.apache.accumulo.core.clientImpl.thrift.ClientService;
import org.apache.accumulo.core.clientImpl.thrift.TInfo;
import org.apache.accumulo.core.conf.DefaultConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.conf.SiteConfiguration;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.dataImpl.thrift.InitialMultiScan;
import org.apache.accumulo.core.dataImpl.thrift.InitialScan;
import org.apache.accumulo.core.dataImpl.thrift.IterInfo;
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
import org.apache.accumulo.core.fate.zookeeper.ZooUtil;
import org.apache.accumulo.core.lock.ServiceLock;
import org.apache.accumulo.core.lock.ServiceLock.AccumuloLockWatcher;
import org.apache.accumulo.core.lock.ServiceLock.LockLossReason;
import org.apache.accumulo.core.lock.ServiceLock.ServiceLockPath;
import org.apache.accumulo.core.lock.ServiceLockData;
import org.apache.accumulo.core.lock.ServiceLockData.ThriftService;
import org.apache.accumulo.core.manager.thrift.TabletServerStatus;
import org.apache.accumulo.core.metadata.TServerInstance;
import org.apache.accumulo.core.metadata.schema.Ample.DataLevel;
import org.apache.accumulo.core.metadata.schema.TabletMetadata;
import org.apache.accumulo.core.rpc.clients.ThriftClientTypes;
import org.apache.accumulo.core.securityImpl.thrift.TCredentials;
import org.apache.accumulo.core.tablet.thrift.TUnloadTabletGoal;
import org.apache.accumulo.core.tablet.thrift.TabletManagementClientService;
import org.apache.accumulo.core.tabletingest.thrift.TDurability;
import org.apache.accumulo.core.tabletingest.thrift.TabletIngestClientService;
import org.apache.accumulo.core.tabletscan.thrift.ActiveScan;
import org.apache.accumulo.core.tabletscan.thrift.TSamplerConfiguration;
import org.apache.accumulo.core.tabletscan.thrift.TabletScanClientService;
import org.apache.accumulo.core.tabletserver.thrift.ActiveCompaction;
import org.apache.accumulo.core.tabletserver.thrift.TabletServerClientService;
import org.apache.accumulo.core.tabletserver.thrift.TabletStats;
import org.apache.accumulo.core.util.threads.ThreadPools;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.client.ClientServiceHandler;
import org.apache.accumulo.server.manager.state.Assignment;
import org.apache.accumulo.server.manager.state.TabletStateStore;
import org.apache.accumulo.server.rpc.TServerUtils;
import org.apache.accumulo.server.rpc.ThriftProcessorTypes;
import org.apache.accumulo.server.rpc.ThriftServerType;
import org.apache.thrift.TException;
import org.apache.thrift.TMultiplexedProcessor;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.beust.jcommander.Parameter;
import com.google.common.net.HostAndPort;

/**
 * The purpose of this class is to server as fake tserver that is a data sink like /dev/null.
 * NullTserver modifies the metadata location entries for a table to point to it. This allows thrift
 * performance to be measured by running any client code that writes to a table.
 */
public class NullTserver {

  private static final Logger LOG = LoggerFactory.getLogger(NullTserver.class);

  public static class NullTServerTabletClientHandler
      implements TabletServerClientService.Iface, TabletScanClientService.Iface,
      TabletIngestClientService.Iface, TabletManagementClientService.Iface {

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
    public boolean cancelUpdate(TInfo tinfo, long updateID) throws TException {
      return true;
    }

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
    public void flushTablet(TInfo tinfo, TCredentials credentials, String lock, TKeyExtent extent) {

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
    public List<TKeyExtent> refreshTablets(TInfo tinfo, TCredentials credentials,
        List<TKeyExtent> refreshes) throws TException {
      return List.of();
    }

    @Override
    public Map<TKeyExtent,Long> allocateTimestamps(TInfo tinfo, TCredentials credentials,
        List<TKeyExtent> tablets, int numStamps) throws TException {
      return Map.of();
    }
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
    ClientServiceHandler csh = new ClientServiceHandler(context);
    NullTServerTabletClientHandler tch = new NullTServerTabletClientHandler();

    TMultiplexedProcessor muxProcessor = new TMultiplexedProcessor();
    muxProcessor.registerProcessor(ThriftClientTypes.CLIENT.getServiceName(),
        ThriftProcessorTypes.CLIENT.getTProcessor(ClientService.Processor.class,
            ClientService.Iface.class, csh, context));
    muxProcessor.registerProcessor(ThriftClientTypes.TABLET_SERVER.getServiceName(),
        ThriftProcessorTypes.TABLET_SERVER.getTProcessor(TabletServerClientService.Processor.class,
            TabletServerClientService.Iface.class, tch, context));
    muxProcessor.registerProcessor(ThriftProcessorTypes.TABLET_SCAN.getServiceName(),
        ThriftProcessorTypes.TABLET_SCAN.getTProcessor(TabletScanClientService.Processor.class,
            TabletScanClientService.Iface.class, tch, context));
    muxProcessor.registerProcessor(ThriftClientTypes.TABLET_INGEST.getServiceName(),
        ThriftProcessorTypes.TABLET_INGEST.getTProcessor(TabletIngestClientService.Processor.class,
            TabletIngestClientService.Iface.class, tch, context));
    muxProcessor.registerProcessor(ThriftProcessorTypes.TABLET_MGMT.getServiceName(),
        ThriftProcessorTypes.TABLET_MGMT.getTProcessor(
            TabletManagementClientService.Processor.class,
            TabletManagementClientService.Iface.class, tch, context));

    TServerUtils.startTServer(context.getConfiguration(), ThriftServerType.CUSTOM_HS_HA,
        muxProcessor, "NullTServer", "null tserver", 2, ThreadPools.DEFAULT_TIMEOUT_MILLISECS, 1000,
        10 * 1024 * 1024, null, null, -1, context.getConfiguration().getCount(Property.RPC_BACKLOG),
        context.getMetricsInfo(), HostAndPort.fromParts("0.0.0.0", opts.port));

    AccumuloLockWatcher miniLockWatcher = new AccumuloLockWatcher() {

      @Override
      public void lostLock(LockLossReason reason) {
        LOG.warn("Lost lock: " + reason.toString());
      }

      @Override
      public void unableToMonitorLockNode(Exception e) {
        LOG.warn("Unable to monitor lock: " + e.getMessage());
      }

      @Override
      public void acquiredLock() {
        LOG.debug("Acquired ZooKeeper lock for NullTserver");
      }

      @Override
      public void failedToAcquireLock(Exception e) {
        LOG.warn("Failed to acquire ZK lock for NullTserver, msg: " + e.getMessage());
      }
    };

    ServiceLock miniLock = null;
    try {
      ZooKeeper zk = context.getZooReaderWriter().getZooKeeper();
      UUID nullTServerUUID = UUID.randomUUID();
      String miniZDirPath = context.getZooKeeperRoot() + "/mini";
      String miniZInstancePath = miniZDirPath + "/" + nullTServerUUID.toString();
      try {
        context.getZooReaderWriter().putPersistentData(miniZDirPath, new byte[0],
            ZooUtil.NodeExistsPolicy.SKIP);
        context.getZooReaderWriter().putPersistentData(miniZInstancePath, new byte[0],
            ZooUtil.NodeExistsPolicy.SKIP);
      } catch (KeeperException | InterruptedException e) {
        throw new IllegalStateException("Error creating path in ZooKeeper", e);
      }
      ServiceLockPath path = ServiceLock.path(miniZInstancePath);
      ServiceLockData sld = new ServiceLockData(nullTServerUUID, "localhost", ThriftService.TSERV,
          Constants.DEFAULT_RESOURCE_GROUP_NAME);
      miniLock = new ServiceLock(zk, path, UUID.randomUUID());
      miniLock.lock(miniLockWatcher, sld);
      context.setServiceLock(miniLock);
      HostAndPort addr = HostAndPort.fromParts(InetAddress.getLocalHost().getHostName(), opts.port);

      TableId tableId = context.getTableId(opts.tableName);

      // read the locations for the table
      Range tableRange = new KeyExtent(tableId, null, null).toMetaRange();
      List<Assignment> assignments = new ArrayList<>();
      try (var tablets = context.getAmple().readTablets().forLevel(DataLevel.USER).build()) {
        long randomSessionID = opts.port;
        TServerInstance instance = new TServerInstance(addr, randomSessionID);
        var s = tablets.iterator();

        while (s.hasNext()) {
          TabletMetadata next = s.next();
          assignments.add(new Assignment(next.getExtent(), instance, next.getLast()));
        }
      }
      // point them to this server
      final ServiceLock lock = miniLock;
      TabletStateStore store = TabletStateStore.getStoreForLevel(DataLevel.USER, context);
      store.setLocations(assignments);

      while (true) {
        Thread.sleep(SECONDS.toMillis(10));
      }

    } finally {
      if (miniLock != null) {
        miniLock.unlock();
      }
    }
  }
}

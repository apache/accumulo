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
package org.apache.accumulo.tserver;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.accumulo.fate.util.UtilWaitThread.sleepUninterruptibly;

import java.io.IOException;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.clientImpl.ScanServerDiscovery;
import org.apache.accumulo.core.clientImpl.thrift.ConfigurationType;
import org.apache.accumulo.core.clientImpl.thrift.TDiskUsage;
import org.apache.accumulo.core.clientImpl.thrift.ThriftSecurityException;
import org.apache.accumulo.core.clientImpl.thrift.ThriftTableOperationException;
import org.apache.accumulo.core.conf.Property;
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
import org.apache.accumulo.core.metadata.schema.TabletMetadata;
import org.apache.accumulo.core.metrics.MetricsUtil;
import org.apache.accumulo.core.securityImpl.thrift.TCredentials;
import org.apache.accumulo.core.tabletserver.thrift.ActiveCompaction;
import org.apache.accumulo.core.tabletserver.thrift.ActiveScan;
import org.apache.accumulo.core.tabletserver.thrift.ConstraintViolationException;
import org.apache.accumulo.core.tabletserver.thrift.NoSuchScanIDException;
import org.apache.accumulo.core.tabletserver.thrift.NotServingTabletException;
import org.apache.accumulo.core.tabletserver.thrift.TCompactionQueueSummary;
import org.apache.accumulo.core.tabletserver.thrift.TDurability;
import org.apache.accumulo.core.tabletserver.thrift.TExternalCompactionJob;
import org.apache.accumulo.core.tabletserver.thrift.TSampleNotPresentException;
import org.apache.accumulo.core.tabletserver.thrift.TSamplerConfiguration;
import org.apache.accumulo.core.tabletserver.thrift.TUnloadTabletGoal;
import org.apache.accumulo.core.tabletserver.thrift.TabletClientService;
import org.apache.accumulo.core.tabletserver.thrift.TabletClientService.Iface;
import org.apache.accumulo.core.tabletserver.thrift.TabletStats;
import org.apache.accumulo.core.tabletserver.thrift.TooManyFilesException;
import org.apache.accumulo.core.trace.TraceUtil;
import org.apache.accumulo.core.trace.thrift.TInfo;
import org.apache.accumulo.core.util.Halt;
import org.apache.accumulo.core.util.ServerServices;
import org.apache.accumulo.core.util.ServerServices.Service;
import org.apache.accumulo.fate.util.UtilWaitThread;
import org.apache.accumulo.fate.zookeeper.ServiceLock;
import org.apache.accumulo.fate.zookeeper.ServiceLock.LockLossReason;
import org.apache.accumulo.fate.zookeeper.ServiceLock.LockWatcher;
import org.apache.accumulo.fate.zookeeper.ZooReaderWriter;
import org.apache.accumulo.fate.zookeeper.ZooUtil.NodeExistsPolicy;
import org.apache.accumulo.server.ServerOpts;
import org.apache.accumulo.server.fs.VolumeManager;
import org.apache.accumulo.server.rpc.ServerAddress;
import org.apache.accumulo.server.rpc.TCredentialsUpdatingWrapper;
import org.apache.accumulo.server.rpc.TServerUtils;
import org.apache.accumulo.server.rpc.ThriftServerType;
import org.apache.accumulo.server.security.SecurityUtil;
import org.apache.accumulo.tserver.TabletServerResourceManager.TabletResourceManager;
import org.apache.accumulo.tserver.metrics.TabletServerScanMetrics;
import org.apache.accumulo.tserver.tablet.Tablet;
import org.apache.accumulo.tserver.tablet.TabletData;
import org.apache.thrift.TException;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ScanServer extends TabletServer implements TabletClientService.Iface {

  protected static class CurrentScan {
    protected KeyExtent extent;
    protected Tablet tablet;
    protected Long scanId;
  }

  private static final Logger LOG = LoggerFactory.getLogger(ScanServer.class);

  protected ThriftClientHandler handler;
  protected CurrentScan currentScan = new CurrentScan();

  protected ScanServer(ServerOpts opts, String[] args) {
    super(opts, args, true);
    handler = getHandler();
  }

  protected ThriftClientHandler getHandler() {
    return new ThriftClientHandler(this);
  }

  private void cleanupTimedOutSession() {
    synchronized (currentScan) {
      if (currentScan.scanId != null && !sessionManager.exists(currentScan.scanId)) {
        endScan();
      }
    }
  }

  /**
   * Start the thrift service to handle incoming client requests
   *
   * @return address of this client service
   * @throws UnknownHostException
   *           host unknown
   */
  protected ServerAddress startScanServerClientService() throws UnknownHostException {
    Iface rpcProxy = TraceUtil.wrapService(this);
    if (getContext().getThriftServerType() == ThriftServerType.SASL) {
      rpcProxy = TCredentialsUpdatingWrapper.service(rpcProxy, getClass(), getConfiguration());
    }
    final TabletClientService.Processor<Iface> processor =
        new TabletClientService.Processor<>(rpcProxy);

    Property maxMessageSizeProperty =
        (getConfiguration().get(Property.SSERV_MAX_MESSAGE_SIZE) != null
            ? Property.SSERV_MAX_MESSAGE_SIZE : Property.GENERAL_MAX_MESSAGE_SIZE);
    ServerAddress sp = TServerUtils.startServer(getContext(), getHostname(),
        Property.SSERV_CLIENTPORT, processor, this.getClass().getSimpleName(),
        "Thrift Client Server", Property.SSERV_PORTSEARCH, Property.SSERV_MINTHREADS,
        Property.SSERV_MINTHREADS_TIMEOUT, Property.SSERV_THREADCHECK, maxMessageSizeProperty);

    LOG.info("address = {}", sp.address);
    return sp;
  }

  /**
   * Set up nodes and locks in ZooKeeper for this Compactor
   */
  private ServiceLock announceExistence() {
    ZooReaderWriter zoo = getContext().getZooReaderWriter();
    try {

      var zLockPath = ServiceLock.path(
          getContext().getZooKeeperRoot() + Constants.ZSSERVERS + "/" + getClientAddressString());

      try {
        zoo.putPersistentData(zLockPath.toString(), new byte[] {}, NodeExistsPolicy.SKIP);
      } catch (KeeperException e) {
        if (e.code() == KeeperException.Code.NOAUTH) {
          LOG.error("Failed to write to ZooKeeper. Ensure that"
              + " accumulo.properties, specifically instance.secret, is consistent.");
        }
        throw e;
      }

      ServiceLock lock = new ServiceLock(zoo.getZooKeeper(), zLockPath, UUID.randomUUID());

      LockWatcher lw = new LockWatcher() {

        @Override
        public void lostLock(final LockLossReason reason) {
          Halt.halt(serverStopRequested ? 0 : 1, () -> {
            if (!serverStopRequested) {
              LOG.error("Lost tablet server lock (reason = {}), exiting.", reason);
            }
            gcLogger.logGCInfo(getConfiguration());
          });
        }

        @Override
        public void unableToMonitorLockNode(final Exception e) {
          Halt.halt(1, () -> LOG.error("Lost ability to monitor scan server lock, exiting.", e));
        }
      };

      byte[] lockContent = new ServerServices(getClientAddressString(), Service.SSERV_CLIENT)
          .toString().getBytes(UTF_8);
      for (int i = 0; i < 120 / 5; i++) {
        zoo.putPersistentData(zLockPath.toString(), new byte[0], NodeExistsPolicy.SKIP);

        if (lock.tryLock(lw, lockContent)) {
          LOG.debug("Obtained scan server lock {}", lock.getLockPath());
          return lock;
        }
        LOG.info("Waiting for scan server lock");
        sleepUninterruptibly(5, TimeUnit.SECONDS);
      }
      String msg = "Too many retries, exiting.";
      LOG.info(msg);
      throw new RuntimeException(msg);
    } catch (Exception e) {
      LOG.info("Could not obtain scan server lock, exiting.", e);
      throw new RuntimeException(e);
    }
  }

  @Override
  public void run() {
    SecurityUtil.serverLogin(getConfiguration());

    ServerAddress address = null;
    try {
      address = startScanServerClientService();
      clientAddress = address.getAddress();
    } catch (UnknownHostException e1) {
      throw new RuntimeException("Failed to start the compactor client service", e1);
    }

    ServiceLock lock = announceExistence();

    try {
      MetricsUtil.initializeMetrics(getContext().getConfiguration(), this.applicationName,
          clientAddress);
    } catch (Exception e1) {
      LOG.error("Error initializing metrics, metrics will not be emitted.", e1);
    }
    scanMetrics = new TabletServerScanMetrics();
    MetricsUtil.initializeProducers(scanMetrics);

    this.getContext().getScheduledExecutor().scheduleWithFixedDelay(() -> cleanupTimedOutSession(),
        1, 1, TimeUnit.SECONDS);

    try {
      try {
        ScanServerDiscovery.unreserve(this.getContext().getZooKeeperRoot(),
            getContext().getZooReaderWriter(), getClientAddressString());
      } catch (Exception e2) {
        throw new RuntimeException("Error setting initial unreserved state in ZooKeeper", e2);
      }

      while (!serverStopRequested) {
        UtilWaitThread.sleep(1000);
      }
    } finally {
      LOG.info("Stopping Thrift Servers");
      TServerUtils.stopTServer(address.getServer());

      try {
        LOG.debug("Closing filesystems");
        VolumeManager mgr = getContext().getVolumeManager();
        if (null != mgr) {
          mgr.close();
        }
      } catch (IOException e) {
        LOG.warn("Failed to close filesystem : {}", e.getMessage(), e);
      }

      gcLogger.logGCInfo(getConfiguration());
      LOG.info("stop requested. exiting ... ");
      try {
        if (null != lock) {
          lock.unlock();
        }
      } catch (Exception e) {
        LOG.warn("Failed to release scan server lock", e);
      }

    }
  }

  private synchronized void checkInUse() {
    synchronized (currentScan) {
      if (currentScan.extent != null || currentScan.tablet != null) {
        throw new RuntimeException("Scan server in use for another query");
      }
    }
  }

  protected synchronized boolean loadTablet(TKeyExtent textent)
      throws IllegalArgumentException, IOException, AccumuloException {
    synchronized (currentScan) {
      KeyExtent extent = KeyExtent.fromThrift(textent);
      TabletMetadata tabletMetadata = getContext().getAmple().readTablet(extent);
      boolean canLoad =
          AssignmentHandler.checkTabletMetadata(extent, getTabletSession(), tabletMetadata);
      if (canLoad) {
        TabletResourceManager trm =
            resourceManager.createTabletResourceManager(extent, getTableConfiguration(extent));
        TabletData data = new TabletData(tabletMetadata);
        currentScan.tablet = new Tablet(this, currentScan.extent, trm, data);
        currentScan.extent = extent;
        onlineTablets.put(currentScan.extent, currentScan.tablet);
        return true;
      } else {
        return false;
      }
    }
  }

  protected synchronized void endScan() {
    synchronized (currentScan) {
      try {
        if (currentScan.tablet != null) {
          try {
            currentScan.tablet.close(false);
          } catch (IOException e1) {
            throw new RuntimeException("Error closing tablet", e1);
          }
        }
        if (currentScan.extent != null) {
          onlineTablets.remove(currentScan.extent);
        }
      } finally {
        currentScan.extent = null;
        currentScan.tablet = null;
        currentScan.scanId = null;
        try {
          ScanServerDiscovery.unreserve(this.getContext().getZooKeeperRoot(),
              getContext().getZooReaderWriter(), getClientAddressString());
        } catch (Exception e2) {
          throw new RuntimeException("Error setting initial unreserved state in ZooKeeper", e2);
        }
      }
    }
  }

  @Override
  public InitialScan startScan(TInfo tinfo, TCredentials credentials, TKeyExtent textent,
      TRange range, List<TColumn> columns, int batchSize, List<IterInfo> ssiList,
      Map<String,Map<String,String>> ssio, List<ByteBuffer> authorizations, boolean waitForWrites,
      boolean isolated, long readaheadThreshold, TSamplerConfiguration samplerConfig,
      long batchTimeOut, String classLoaderContext, Map<String,String> executionHints)
      throws ThriftSecurityException, NotServingTabletException, TooManyFilesException,
      TSampleNotPresentException, TException {

    checkInUse();
    try {
      if (loadTablet(textent)) {
        synchronized (currentScan) {
          InitialScan is = handler.startScan(tinfo, credentials, textent, range, columns, batchSize,
              ssiList, ssio, authorizations, waitForWrites, isolated, readaheadThreshold,
              samplerConfig, batchTimeOut, classLoaderContext, executionHints);
          currentScan.scanId = is.getScanID();
          return is;
        }
      } else {
        throw new RuntimeException("Unable to load tablet for scan");
      }
    } catch (Exception e) {
      endScan();
      throw new RuntimeException("Error creating tablet for scan", e);
    }
  }

  @Override
  public ScanResult continueScan(TInfo tinfo, long scanID) throws NoSuchScanIDException,
      NotServingTabletException, TooManyFilesException, TSampleNotPresentException, TException {
    try {
      return handler.continueScan(tinfo, scanID);
    } catch (Exception e) {
      endScan();
      throw e;
    }
  }

  @Override
  public void closeScan(TInfo tinfo, long scanID) throws TException {
    try {
      handler.closeScan(tinfo, scanID);
    } finally {
      endScan();
    }
  }

  @Override
  public InitialMultiScan startMultiScan(TInfo tinfo, TCredentials credentials,
      Map<TKeyExtent,List<TRange>> tbatch, List<TColumn> tcolumns, List<IterInfo> ssiList,
      Map<String,Map<String,String>> ssio, List<ByteBuffer> authorizations, boolean waitForWrites,
      TSamplerConfiguration tSamplerConfig, long batchTimeOut, String contextArg,
      Map<String,String> executionHints)
      throws ThriftSecurityException, TSampleNotPresentException, TException {

    checkInUse();

    if (tbatch.size() != 1) {
      throw new RuntimeException("Scan Server expects scans for one tablet only");
    }

    try {
      if (loadTablet(tbatch.keySet().iterator().next())) {
        synchronized (currentScan) {
          InitialMultiScan ims = handler.startMultiScan(tinfo, credentials, tbatch, tcolumns,
              ssiList, ssio, authorizations, waitForWrites, tSamplerConfig, batchTimeOut,
              contextArg, executionHints);
          currentScan.scanId = ims.getScanID();
          return ims;
        }
      } else {
        throw new RuntimeException("Unable to load tablet for scan");
      }
    } catch (Exception e) {
      endScan();
      throw new RuntimeException("Error creating tablet for scan", e);
    }
  }

  @Override
  public MultiScanResult continueMultiScan(TInfo tinfo, long scanID)
      throws NoSuchScanIDException, TSampleNotPresentException, TException {
    try {
      return handler.continueMultiScan(tinfo, scanID);
    } catch (Exception e) {
      endScan();
      throw e;
    }
  }

  @Override
  public void closeMultiScan(TInfo tinfo, long scanID) throws NoSuchScanIDException, TException {
    try {
      handler.closeMultiScan(tinfo, scanID);
    } finally {
      endScan();
    }
  }

  public static void main(String[] args) throws Exception {
    try (ScanServer tserver = new ScanServer(new ServerOpts(), args)) {
      tserver.runServer();
    }
  }

  @Override
  public String getRootTabletLocation() throws TException {
    return null;
  }

  @Override
  public String getInstanceId() throws TException {
    return null;
  }

  @Override
  public String getZooKeepers() throws TException {
    return null;
  }

  @Override
  public List<String> bulkImportFiles(TInfo tinfo, TCredentials credentials, long tid,
      String tableId, List<String> files, String errorDir, boolean setTime)
      throws ThriftSecurityException, ThriftTableOperationException, TException {
    return null;
  }

  @Override
  public boolean isActive(TInfo tinfo, long tid) throws TException {
    return false;
  }

  @Override
  public void ping(TCredentials credentials) throws ThriftSecurityException, TException {}

  @Override
  public List<TDiskUsage> getDiskUsage(Set<String> tables, TCredentials credentials)
      throws ThriftSecurityException, ThriftTableOperationException, TException {
    return null;
  }

  @Override
  public Set<String> listLocalUsers(TInfo tinfo, TCredentials credentials)
      throws ThriftSecurityException, TException {
    return null;
  }

  @Override
  public void createLocalUser(TInfo tinfo, TCredentials credentials, String principal,
      ByteBuffer password) throws ThriftSecurityException, TException {}

  @Override
  public void dropLocalUser(TInfo tinfo, TCredentials credentials, String principal)
      throws ThriftSecurityException, TException {}

  @Override
  public void changeLocalUserPassword(TInfo tinfo, TCredentials credentials, String principal,
      ByteBuffer password) throws ThriftSecurityException, TException {}

  @Override
  public boolean authenticate(TInfo tinfo, TCredentials credentials)
      throws ThriftSecurityException, TException {
    return false;
  }

  @Override
  public boolean authenticateUser(TInfo tinfo, TCredentials credentials, TCredentials toAuth)
      throws ThriftSecurityException, TException {
    return false;
  }

  @Override
  public void changeAuthorizations(TInfo tinfo, TCredentials credentials, String principal,
      List<ByteBuffer> authorizations) throws ThriftSecurityException, TException {}

  @Override
  public List<ByteBuffer> getUserAuthorizations(TInfo tinfo, TCredentials credentials,
      String principal) throws ThriftSecurityException, TException {
    return null;
  }

  @Override
  public boolean hasSystemPermission(TInfo tinfo, TCredentials credentials, String principal,
      byte sysPerm) throws ThriftSecurityException, TException {
    return false;
  }

  @Override
  public boolean hasTablePermission(TInfo tinfo, TCredentials credentials, String principal,
      String tableName, byte tblPerm)
      throws ThriftSecurityException, ThriftTableOperationException, TException {
    return false;
  }

  @Override
  public boolean hasNamespacePermission(TInfo tinfo, TCredentials credentials, String principal,
      String ns, byte tblNspcPerm)
      throws ThriftSecurityException, ThriftTableOperationException, TException {
    return false;
  }

  @Override
  public void grantSystemPermission(TInfo tinfo, TCredentials credentials, String principal,
      byte permission) throws ThriftSecurityException, TException {}

  @Override
  public void revokeSystemPermission(TInfo tinfo, TCredentials credentials, String principal,
      byte permission) throws ThriftSecurityException, TException {}

  @Override
  public void grantTablePermission(TInfo tinfo, TCredentials credentials, String principal,
      String tableName, byte permission)
      throws ThriftSecurityException, ThriftTableOperationException, TException {}

  @Override
  public void revokeTablePermission(TInfo tinfo, TCredentials credentials, String principal,
      String tableName, byte permission)
      throws ThriftSecurityException, ThriftTableOperationException, TException {}

  @Override
  public void grantNamespacePermission(TInfo tinfo, TCredentials credentials, String principal,
      String ns, byte permission)
      throws ThriftSecurityException, ThriftTableOperationException, TException {}

  @Override
  public void revokeNamespacePermission(TInfo tinfo, TCredentials credentials, String principal,
      String ns, byte permission)
      throws ThriftSecurityException, ThriftTableOperationException, TException {}

  @Override
  public Map<String,String> getConfiguration(TInfo tinfo, TCredentials credentials,
      ConfigurationType type) throws TException {
    return null;
  }

  @Override
  public Map<String,String> getTableConfiguration(TInfo tinfo, TCredentials credentials,
      String tableName) throws ThriftTableOperationException, TException {
    return null;
  }

  @Override
  public Map<String,String> getNamespaceConfiguration(TInfo tinfo, TCredentials credentials,
      String ns) throws ThriftTableOperationException, TException {
    return null;
  }

  @Override
  public boolean checkClass(TInfo tinfo, TCredentials credentials, String className,
      String interfaceMatch) throws TException {
    return false;
  }

  @Override
  public boolean checkTableClass(TInfo tinfo, TCredentials credentials, String tableId,
      String className, String interfaceMatch)
      throws ThriftSecurityException, ThriftTableOperationException, TException {
    return false;
  }

  @Override
  public boolean checkNamespaceClass(TInfo tinfo, TCredentials credentials, String namespaceId,
      String className, String interfaceMatch)
      throws ThriftSecurityException, ThriftTableOperationException, TException {
    return false;
  }

  @Override
  public long startUpdate(TInfo tinfo, TCredentials credentials, TDurability durability)
      throws ThriftSecurityException, TException {
    return 0;
  }

  @Override
  public void applyUpdates(TInfo tinfo, long updateID, TKeyExtent keyExtent,
      List<TMutation> mutations) throws TException {}

  @Override
  public UpdateErrors closeUpdate(TInfo tinfo, long updateID)
      throws NoSuchScanIDException, TException {
    return null;
  }

  @Override
  public void update(TInfo tinfo, TCredentials credentials, TKeyExtent keyExtent,
      TMutation mutation, TDurability durability) throws ThriftSecurityException,
      NotServingTabletException, ConstraintViolationException, TException {}

  @Override
  public TConditionalSession startConditionalUpdate(TInfo tinfo, TCredentials credentials,
      List<ByteBuffer> authorizations, String tableID, TDurability durability,
      String classLoaderContext) throws ThriftSecurityException, TException {
    return null;
  }

  @Override
  public List<TCMResult> conditionalUpdate(TInfo tinfo, long sessID,
      Map<TKeyExtent,List<TConditionalMutation>> mutations, List<String> symbols)
      throws NoSuchScanIDException, TException {
    return null;
  }

  @Override
  public void invalidateConditionalUpdate(TInfo tinfo, long sessID) throws TException {}

  @Override
  public void closeConditionalUpdate(TInfo tinfo, long sessID) throws TException {}

  @Override
  public List<TKeyExtent> bulkImport(TInfo tinfo, TCredentials credentials, long tid,
      Map<TKeyExtent,Map<String,MapFileInfo>> files, boolean setTime)
      throws ThriftSecurityException, TException {
    return null;
  }

  @Override
  public void loadFiles(TInfo tinfo, TCredentials credentials, long tid, String dir,
      Map<TKeyExtent,Map<String,MapFileInfo>> files, boolean setTime) throws TException {}

  @Override
  public void splitTablet(TInfo tinfo, TCredentials credentials, TKeyExtent extent,
      ByteBuffer splitPoint)
      throws ThriftSecurityException, NotServingTabletException, TException {}

  @Override
  public void loadTablet(TInfo tinfo, TCredentials credentials, String lock, TKeyExtent extent)
      throws TException {}

  @Override
  public void unloadTablet(TInfo tinfo, TCredentials credentials, String lock, TKeyExtent extent,
      TUnloadTabletGoal goal, long requestTime) throws TException {}

  @Override
  public void flush(TInfo tinfo, TCredentials credentials, String lock, String tableId,
      ByteBuffer startRow, ByteBuffer endRow) throws TException {}

  @Override
  public void flushTablet(TInfo tinfo, TCredentials credentials, String lock, TKeyExtent extent)
      throws TException {}

  @Override
  public void chop(TInfo tinfo, TCredentials credentials, String lock, TKeyExtent extent)
      throws TException {}

  @Override
  public void compact(TInfo tinfo, TCredentials credentials, String lock, String tableId,
      ByteBuffer startRow, ByteBuffer endRow) throws TException {}

  @Override
  public TabletServerStatus getTabletServerStatus(TInfo tinfo, TCredentials credentials)
      throws ThriftSecurityException, TException {
    return null;
  }

  @Override
  public List<TabletStats> getTabletStats(TInfo tinfo, TCredentials credentials, String tableId)
      throws ThriftSecurityException, TException {
    return null;
  }

  @Override
  public TabletStats getHistoricalStats(TInfo tinfo, TCredentials credentials)
      throws ThriftSecurityException, TException {
    return null;
  }

  @Override
  public void halt(TInfo tinfo, TCredentials credentials, String lock)
      throws ThriftSecurityException, TException {}

  @Override
  public void fastHalt(TInfo tinfo, TCredentials credentials, String lock) throws TException {}

  @Override
  public List<ActiveScan> getActiveScans(TInfo tinfo, TCredentials credentials)
      throws ThriftSecurityException, TException {
    return null;
  }

  @Override
  public List<ActiveCompaction> getActiveCompactions(TInfo tinfo, TCredentials credentials)
      throws ThriftSecurityException, TException {
    return null;
  }

  @Override
  public void removeLogs(TInfo tinfo, TCredentials credentials, List<String> filenames)
      throws TException {}

  @Override
  public List<String> getActiveLogs(TInfo tinfo, TCredentials credentials) throws TException {
    return null;
  }

  @Override
  public TSummaries startGetSummaries(TInfo tinfo, TCredentials credentials,
      TSummaryRequest request)
      throws ThriftSecurityException, ThriftTableOperationException, TException {
    return null;
  }

  @Override
  public TSummaries startGetSummariesForPartition(TInfo tinfo, TCredentials credentials,
      TSummaryRequest request, int modulus, int remainder)
      throws ThriftSecurityException, TException {
    return null;
  }

  @Override
  public TSummaries startGetSummariesFromFiles(TInfo tinfo, TCredentials credentials,
      TSummaryRequest request, Map<String,List<TRowRange>> files)
      throws ThriftSecurityException, TException {
    return null;
  }

  @Override
  public TSummaries contiuneGetSummaries(TInfo tinfo, long sessionId)
      throws NoSuchScanIDException, TException {
    return null;
  }

  @Override
  public List<TCompactionQueueSummary> getCompactionQueueInfo(TInfo tinfo, TCredentials credentials)
      throws ThriftSecurityException, TException {
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
      throws TException {}

  @Override
  public void compactionJobFailed(TInfo tinfo, TCredentials credentials,
      String externalCompactionId, TKeyExtent extent) throws TException {}

}

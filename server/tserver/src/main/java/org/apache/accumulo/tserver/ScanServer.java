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
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.AccumuloException;
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
import org.apache.accumulo.core.metadata.schema.ExternalCompactionId;
import org.apache.accumulo.core.metadata.schema.TabletMetadata;
import org.apache.accumulo.core.metrics.MetricsUtil;
import org.apache.accumulo.core.securityImpl.thrift.TCredentials;
import org.apache.accumulo.core.spi.compaction.CompactionExecutorId;
import org.apache.accumulo.core.spi.compaction.CompactionServiceId;
import org.apache.accumulo.core.spi.compaction.CompactionServices;
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
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.ServerOpts;
import org.apache.accumulo.server.fs.VolumeManager;
import org.apache.accumulo.server.rpc.ServerAddress;
import org.apache.accumulo.server.rpc.TCredentialsUpdatingWrapper;
import org.apache.accumulo.server.rpc.TServerUtils;
import org.apache.accumulo.server.rpc.ThriftServerType;
import org.apache.accumulo.server.security.SecurityUtil;
import org.apache.accumulo.tserver.TabletServerResourceManager.TabletResourceManager;
import org.apache.accumulo.tserver.compactions.Compactable;
import org.apache.accumulo.tserver.compactions.CompactionManager;
import org.apache.accumulo.tserver.compactions.ExternalCompactionJob;
import org.apache.accumulo.tserver.metrics.CompactionExecutorsMetrics;
import org.apache.accumulo.tserver.metrics.TabletServerScanMetrics;
import org.apache.accumulo.tserver.session.ScanSession;
import org.apache.accumulo.tserver.tablet.Tablet;
import org.apache.accumulo.tserver.tablet.TabletData;
import org.apache.commons.lang3.tuple.MutableTriple;
import org.apache.thrift.TException;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.github.benmanes.caffeine.cache.Scheduler;

public class ScanServer extends TabletServer implements TabletClientService.Iface {

  static class ScanInformation extends MutableTriple<Long,KeyExtent,Tablet> {
    private static final long serialVersionUID = 1L;

    public Long getScanId() {
      return getLeft();
    }

    public void setScanId(Long scanId) {
      setLeft(scanId);
    }

    public KeyExtent getExtent() {
      return getMiddle();
    }

    public void setExtent(KeyExtent extent) {
      setMiddle(extent);
    }

    public Tablet getTablet() {
      return getRight();
    }

    public void setTablet(Tablet tablet) {
      setRight(tablet);
    }
  }

  /**
   * A compaction manager that does nothing
   */
  private static class ScanServerCompactionManager extends CompactionManager {

    public ScanServerCompactionManager(ServerContext context,
        CompactionExecutorsMetrics ceMetrics) {
      super(new ArrayList<>(), context, ceMetrics);
    }

    @Override
    public void compactableChanged(Compactable compactable) {}

    @Override
    public void start() {}

    @Override
    public CompactionServices getServices() {
      return null;
    }

    @Override
    public boolean isCompactionQueued(KeyExtent extent, Set<CompactionServiceId> servicesUsed) {
      return false;
    }

    @Override
    public int getCompactionsRunning() {
      return 0;
    }

    @Override
    public int getCompactionsQueued() {
      return 0;
    }

    @Override
    public ExternalCompactionJob reserveExternalCompaction(String queueName, long priority,
        String compactorId, ExternalCompactionId externalCompactionId) {
      return null;
    }

    @Override
    public void registerExternalCompaction(ExternalCompactionId ecid, KeyExtent extent,
        CompactionExecutorId ceid) {}

    @Override
    public void commitExternalCompaction(ExternalCompactionId extCompactionId,
        KeyExtent extentCompacted, Map<KeyExtent,Tablet> currentTablets, long fileSize,
        long entries) {}

    @Override
    public void externalCompactionFailed(ExternalCompactionId ecid, KeyExtent extentCompacted,
        Map<KeyExtent,Tablet> currentTablets) {}

    @Override
    public List<TCompactionQueueSummary> getCompactionQueueSummaries() {
      return null;
    }

    @Override
    public Collection<ExtCompMetric> getExternalMetrics() {
      return null;
    }

    @Override
    public void compactableClosed(KeyExtent extent, Set<CompactionServiceId> servicesUsed,
        Set<ExternalCompactionId> ecids) {}

  }

  private static final Logger LOG = LoggerFactory.getLogger(ScanServer.class);

  protected Map<Long,ScanInformation> scans;
  protected ThriftClientHandler handler;
  protected int maxConcurrentScans;
  private final LoadingCache<KeyExtent,TabletMetadata> tabletMetadataCache;

  public ScanServer(ServerOpts opts, String[] args) {
    super(opts, args, true);
    // Sanity check some configuration settings
    maxConcurrentScans = getConfiguration().getCount(Property.SSERV_CONCURRENT_SCANS);
    int tserverScanThreads =
        getConfiguration().getCount(Property.TSERV_SCAN_EXECUTORS_DEFAULT_THREADS);
    if (maxConcurrentScans > tserverScanThreads) {
      throw new RuntimeException(
          Property.SSERV_CONCURRENT_SCANS.getKey() + " must be less than or equal to "
              + Property.TSERV_SCAN_EXECUTORS_DEFAULT_THREADS.getKey());
    }
    int thriftServerThreads = getConfiguration().getCount(Property.SSERV_MINTHREADS);
    if (maxConcurrentScans > thriftServerThreads) {
      LOG.warn(
          "Thrift server threads (property {}) value {} is lower than configured concurrent scans (property {}) value {}",
          Property.SSERV_MINTHREADS.getKey(), thriftServerThreads,
          Property.SSERV_CONCURRENT_SCANS.getKey(), maxConcurrentScans);
    }
    scans = new ConcurrentHashMap<>(maxConcurrentScans, 1.0f, maxConcurrentScans);
    long cacheExpiration =
        getConfiguration().getTimeInMillis(Property.SSERV_CACHED_TABLET_METADATA_EXPIRATION);
    if (cacheExpiration == 0L) {
      LOG.warn("Tablet metadata caching disabled, may cause excessive scans on metadata table.");
      tabletMetadataCache = null;
    } else {
      if (cacheExpiration < 60000) {
        LOG.warn(
            "Tablet metadata caching less than one minute, may cause excessive scans on metadata table.");
      }
      tabletMetadataCache =
          Caffeine.newBuilder().expireAfterWrite(cacheExpiration, TimeUnit.MILLISECONDS)
              .scheduler(Scheduler.systemScheduler())
              .build(key -> getContext().getAmple().readTablet(key));
    }
    handler = getHandler();
  }

  protected ThriftClientHandler getHandler() {
    return new ThriftClientHandler(this);
  }

  private void cleanupTimedOutSession() {
    scans.forEach((k, v) -> {
      if (v.getScanId() != null && !sessionManager.exists(v.getScanId())) {
        LOG.info("{} is no longer active, ending scan", v.getScanId());
        endScan(v);
      }
    });
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

      tabletServerLock = new ServiceLock(zoo.getZooKeeper(), zLockPath, UUID.randomUUID());

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

        if (tabletServerLock.tryLock(lw, lockContent)) {
          LOG.debug("Obtained scan server lock {}", tabletServerLock.getLockPath());
          return tabletServerLock;
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

    try {
      MetricsUtil.initializeMetrics(getContext().getConfiguration(), this.applicationName,
          clientAddress);
    } catch (Exception e1) {
      LOG.error("Error initializing metrics, metrics will not be emitted.", e1);
    }
    scanMetrics = new TabletServerScanMetrics();
    MetricsUtil.initializeProducers(scanMetrics);

    // We need to set the compaction manager so that we don't get an NPE in CompactableImpl.close
    ceMetrics = new CompactionExecutorsMetrics();
    this.compactionManager = new ScanServerCompactionManager(getContext(), ceMetrics);

    // SessionManager times out sessions when sessions have been idle longer than
    // TSERV_SESSION_MAXIDLE. Create a background thread that looks for sessions that
    // have timed out and end the scan.
    long maxIdle =
        this.getContext().getConfiguration().getTimeInMillis(Property.TSERV_SESSION_MAXIDLE);
    LOG.debug("Looking for timed out scan sessions every {}ms", Math.max(maxIdle / 2, 1000));
    this.getContext().getScheduledExecutor().scheduleWithFixedDelay(() -> cleanupTimedOutSession(),
        Math.max(maxIdle / 2, 1000), Math.max(maxIdle / 2, 1000), TimeUnit.MILLISECONDS);

    ServiceLock lock = announceExistence();

    try {
      while (!serverStopRequested) {
        UtilWaitThread.sleep(1000);
      }
    } finally {
      LOG.info("Stopping Thrift Servers");
      TServerUtils.stopTServer(address.server);

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

  private TabletMetadata getTabletMetadata(KeyExtent extent) {
    if (tabletMetadataCache == null) {
      return getContext().getAmple().readTablet(extent);
    } else {
      return tabletMetadataCache.get(extent);
    }
  }

  protected ScanInformation loadTablet(KeyExtent extent)
      throws IllegalArgumentException, IOException, AccumuloException {

    // Need to call ScanServerKeyExtent.toKeyExtent so that the cache key
    // is the unique per tablet, not unique per scan.
    TabletMetadata tabletMetadata = getTabletMetadata(extent);

    // Need to call ScanServerKeyExtent.toKeyExtent for the equivalence checks that
    // happen inside AssignmentHandler.
    boolean canLoad =
        AssignmentHandler.checkTabletMetadata(extent, getTabletSession(), tabletMetadata, true);
    if (canLoad) {
      ScanInformation si = new ScanInformation();
      si.setExtent(extent);
      TabletResourceManager trm =
          resourceManager.createTabletResourceManager(extent, getTableConfiguration(extent));
      TabletData data = new TabletData(tabletMetadata);
      si.setTablet(new Tablet(this, extent, trm, data));
      LOG.debug("loaded tablet: {}", si.getExtent());
      return si;
    } else {
      return null;
    }
  }

  protected void logOnlineTablets() {
    StringBuilder buf = new StringBuilder();
    onlineTablets.snapshot().forEach((k, v) -> {
      buf.append("\n" + k + " -> " + v.getDatafiles());
    });
    LOG.debug("onlineTablets: {}", buf.toString());
  }

  protected void endScan(ScanInformation si) {
    LOG.debug("ending scan: {}", si.getScanId());
    try {
      if (si.getExtent() != null && si.getTablet() != null) {
        onlineTablets.remove(si.getExtent());
        try {
          si.getTablet().close(false);
        } catch (IOException e1) {
          throw new RuntimeException("Error closing tablet", e1);
        }
        LOG.debug("Closed Tablet for extent: " + si.getExtent());
        logOnlineTablets();
      }
    } finally {
      scans.remove(si.getScanId(), si);
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

    if (scans.size() == maxConcurrentScans) {
      throw new TException("ScanServer is busy");
    }
    KeyExtent extent = KeyExtent.fromThrift(textent);
    try {
      ScanInformation si = null;
      try {
        si = loadTablet(extent);
        if (si == null) {
          throw new NotServingTabletException();
        }
      } catch (Exception e) {
        LOG.error("Error loading tablet for extent: " + textent, e);
        if (si != null) {
          endScan(si);
        }
        throw new NotServingTabletException();
      }

      var fsi = si;
      ScanSession.TabletResolver tabletResolver = ke -> {
        if (ke.equals(fsi.getExtent())) {
          return fsi.getTablet();
        } else {
          return null;
        }
      };

      InitialScan is = handler.startScan(tinfo, credentials, extent, range, columns, batchSize,
          ssiList, ssio, authorizations, waitForWrites, isolated, readaheadThreshold, samplerConfig,
          batchTimeOut, classLoaderContext, executionHints, tabletResolver);
      si.setScanId(is.getScanID());
      if (scans.size() == maxConcurrentScans) {
        endScan(si);
        throw new TException("ScanServer is busy");
      }
      scans.put(si.getScanId(), si);
      LOG.debug("started scan {} for extent {}", si.getScanId(), si.getExtent());
      logOnlineTablets();
      return is;
    } catch (TException e) {
      throw e;
    }
  }

  @Override
  public ScanResult continueScan(TInfo tinfo, long scanID) throws NoSuchScanIDException,
      NotServingTabletException, TooManyFilesException, TSampleNotPresentException, TException {
    ScanInformation si = scans.get(scanID);
    if (si == null) {
      throw new NoSuchScanIDException();
    }
    LOG.debug("continue scan: {}", scanID);
    try {
      return handler.continueScan(tinfo, scanID);
    } catch (Exception e) {
      endScan(si);
      throw e;
    }
  }

  @Override
  public void closeScan(TInfo tinfo, long scanID) throws TException {
    ScanInformation si = scans.get(scanID);
    if (si == null) {
      throw new NoSuchScanIDException();
    }
    LOG.debug("close scan: {}", scanID);
    try {
      handler.closeScan(tinfo, scanID);
    } finally {
      endScan(si);
    }
  }

  @Override
  public InitialMultiScan startMultiScan(TInfo tinfo, TCredentials credentials,
      Map<TKeyExtent,List<TRange>> tbatch, List<TColumn> tcolumns, List<IterInfo> ssiList,
      Map<String,Map<String,String>> ssio, List<ByteBuffer> authorizations, boolean waitForWrites,
      TSamplerConfiguration tSamplerConfig, long batchTimeOut, String contextArg,
      Map<String,String> executionHints)
      throws ThriftSecurityException, TSampleNotPresentException, TException {

    if (tbatch.size() != 1) {
      // TODO support multiple tablets
      throw new TException("Scan Server expects scans for one tablet only");
    }
    Entry<TKeyExtent,List<TRange>> entry = tbatch.entrySet().iterator().next();
    TKeyExtent textent = entry.getKey();
    if (textent == null) {
      throw new TException("TKeyExtent is missing!");
    }

    if (scans.size() == maxConcurrentScans) {
      throw new TException("ScanServer is busy");
    }
    KeyExtent extent = KeyExtent.fromThrift(textent);
    try {
      ScanInformation si = null;
      try {
        si = loadTablet(extent);
        if (si == null) {
          throw new NotServingTabletException();
        }
      } catch (Exception e) {
        LOG.error("Error loading tablet for extent: " + textent, e);
        if (si != null) {
          endScan(si);
        }
        throw new NotServingTabletException();
      }
      Map<KeyExtent,List<TRange>> newBatch = new HashMap<>();
      newBatch.put(extent, entry.getValue());

      var fsi = si;
      ScanSession.TabletResolver tabletResolver = ke -> {
        if (ke.equals(fsi.getExtent())) {
          return fsi.getTablet();
        } else {
          return null;
        }
      };

      InitialMultiScan ims = handler.startMultiScan(tinfo, credentials, tcolumns, ssiList, newBatch,
          ssio, authorizations, waitForWrites, tSamplerConfig, batchTimeOut, contextArg,
          executionHints, tabletResolver);
      si.setScanId(ims.getScanID());
      if (scans.size() == maxConcurrentScans) {
        endScan(si);
        throw new TException("ScanServer is busy");
      }
      scans.put(si.getScanId(), si);
      LOG.debug("started scan: {}", si.getScanId());
      logOnlineTablets();
      return ims;
    } catch (TException e) {
      throw e;
    }
  }

  @Override
  public MultiScanResult continueMultiScan(TInfo tinfo, long scanID)
      throws NoSuchScanIDException, TSampleNotPresentException, TException {
    ScanInformation si = scans.get(scanID);
    if (si == null) {
      throw new NoSuchScanIDException();
    }
    LOG.debug("continue multi scan: {}", scanID);
    try {
      return handler.continueMultiScan(tinfo, scanID);
    } catch (Exception e) {
      endScan(si);
      throw e;
    }
  }

  @Override
  public void closeMultiScan(TInfo tinfo, long scanID) throws NoSuchScanIDException, TException {
    ScanInformation si = scans.get(scanID);
    if (si == null) {
      throw new NoSuchScanIDException();
    }
    LOG.debug("close multi scan: {}", scanID);
    try {
      handler.closeMultiScan(tinfo, scanID);
    } finally {
      endScan(si);
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
  public void fastHalt(TInfo tinfo, TCredentials credentials, String lock) throws TException {
    handler.fastHalt(tinfo, credentials, lock);
  }

  @Override
  public List<ActiveScan> getActiveScans(TInfo tinfo, TCredentials credentials)
      throws ThriftSecurityException, TException {
    return handler.getActiveScans(tinfo, credentials);
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

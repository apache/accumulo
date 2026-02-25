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
package org.apache.accumulo.tserver;

import static com.google.common.util.concurrent.Uninterruptibles.sleepUninterruptibly;
import static org.apache.accumulo.core.util.threads.ThreadPoolNames.SCAN_SERVER_TABLET_METADATA_CACHE_POOL;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.cli.ServerOpts;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.admin.servers.ServerId;
import org.apache.accumulo.core.client.admin.servers.ServerId.Type;
import org.apache.accumulo.core.clientImpl.thrift.TInfo;
import org.apache.accumulo.core.clientImpl.thrift.ThriftSecurityException;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.conf.SiteConfiguration;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.dataImpl.thrift.InitialMultiScan;
import org.apache.accumulo.core.dataImpl.thrift.InitialScan;
import org.apache.accumulo.core.dataImpl.thrift.IterInfo;
import org.apache.accumulo.core.dataImpl.thrift.MultiScanResult;
import org.apache.accumulo.core.dataImpl.thrift.ScanResult;
import org.apache.accumulo.core.dataImpl.thrift.TColumn;
import org.apache.accumulo.core.dataImpl.thrift.TKeyExtent;
import org.apache.accumulo.core.dataImpl.thrift.TRange;
import org.apache.accumulo.core.fate.zookeeper.ZooReaderWriter;
import org.apache.accumulo.core.fate.zookeeper.ZooUtil.NodeExistsPolicy;
import org.apache.accumulo.core.file.blockfile.cache.impl.BlockCacheConfiguration;
import org.apache.accumulo.core.lock.ServiceLock;
import org.apache.accumulo.core.lock.ServiceLock.LockWatcher;
import org.apache.accumulo.core.lock.ServiceLockData;
import org.apache.accumulo.core.lock.ServiceLockData.ServiceDescriptor;
import org.apache.accumulo.core.lock.ServiceLockData.ServiceDescriptors;
import org.apache.accumulo.core.lock.ServiceLockData.ThriftService;
import org.apache.accumulo.core.lock.ServiceLockPaths.ServiceLockPath;
import org.apache.accumulo.core.lock.ServiceLockSupport;
import org.apache.accumulo.core.lock.ServiceLockSupport.ServiceLockWatcher;
import org.apache.accumulo.core.metadata.ScanServerRefTabletFile;
import org.apache.accumulo.core.metadata.StoredTabletFile;
import org.apache.accumulo.core.metadata.schema.Ample;
import org.apache.accumulo.core.metadata.schema.TabletMetadata;
import org.apache.accumulo.core.metadata.schema.TabletsMetadata;
import org.apache.accumulo.core.metrics.MetricsInfo;
import org.apache.accumulo.core.securityImpl.thrift.TCredentials;
import org.apache.accumulo.core.tabletscan.thrift.ActiveScan;
import org.apache.accumulo.core.tabletscan.thrift.ScanServerBusyException;
import org.apache.accumulo.core.tabletscan.thrift.TSampleNotPresentException;
import org.apache.accumulo.core.tabletscan.thrift.TSamplerConfiguration;
import org.apache.accumulo.core.tabletscan.thrift.TabletScanClientService;
import org.apache.accumulo.core.tabletscan.thrift.TooManyFilesException;
import org.apache.accumulo.core.tabletserver.thrift.NoSuchScanIDException;
import org.apache.accumulo.core.tabletserver.thrift.NotServingTabletException;
import org.apache.accumulo.core.util.Retry;
import org.apache.accumulo.core.util.Timer;
import org.apache.accumulo.core.util.UtilWaitThread;
import org.apache.accumulo.core.util.cache.Caches.CacheName;
import org.apache.accumulo.core.util.threads.ThreadPools;
import org.apache.accumulo.server.AbstractServer;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.client.ClientServiceHandler;
import org.apache.accumulo.server.compaction.PausedCompactionMetrics;
import org.apache.accumulo.server.conf.TableConfiguration;
import org.apache.accumulo.server.fs.VolumeManager;
import org.apache.accumulo.server.rpc.TServerUtils;
import org.apache.accumulo.server.rpc.ThriftProcessorTypes;
import org.apache.accumulo.server.security.SecurityUtil;
import org.apache.accumulo.tserver.TabletServerResourceManager.TabletResourceManager;
import org.apache.accumulo.tserver.log.LogSorter;
import org.apache.accumulo.tserver.metrics.TabletServerScanMetrics;
import org.apache.accumulo.tserver.session.MultiScanSession;
import org.apache.accumulo.tserver.session.ScanSession;
import org.apache.accumulo.tserver.session.ScanSession.TabletResolver;
import org.apache.accumulo.tserver.session.Session;
import org.apache.accumulo.tserver.session.SessionManager;
import org.apache.accumulo.tserver.session.SingleScanSession;
import org.apache.accumulo.tserver.tablet.SnapshotTablet;
import org.apache.accumulo.tserver.tablet.Tablet;
import org.apache.accumulo.tserver.tablet.TabletBase;
import org.apache.thrift.TApplicationException;
import org.apache.thrift.TException;
import org.apache.thrift.TProcessor;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.benmanes.caffeine.cache.CacheLoader;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.github.benmanes.caffeine.cache.Scheduler;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

public class ScanServer extends AbstractServer
    implements TabletScanClientService.Iface, TabletHostingServer {

  private static final Logger LOG = LoggerFactory.getLogger(ScanServer.class);

  private static class TabletMetadataLoader implements CacheLoader<KeyExtent,TabletMetadata> {

    private final Ample ample;

    private TabletMetadataLoader(Ample ample) {
      this.ample = ample;
    }

    @Override
    public @Nullable TabletMetadata load(KeyExtent keyExtent) {
      long t1 = System.currentTimeMillis();
      var tm = ample.readTablet(keyExtent);
      long t2 = System.currentTimeMillis();
      LOG.trace("Read metadata for 1 tablet in {} ms", t2 - t1);
      return tm;
    }

    @Override
    public Map<? extends KeyExtent,? extends TabletMetadata>
        loadAll(Set<? extends KeyExtent> keys) {
      Map<KeyExtent,TabletMetadata> tms;
      long t1 = System.currentTimeMillis();
      @SuppressWarnings("unchecked")
      Collection<KeyExtent> extents = (Collection<KeyExtent>) keys;
      try (TabletsMetadata tabletsMetadata =
          ample.readTablets().forTablets(extents, Optional.empty()).build()) {
        tms =
            tabletsMetadata.stream().collect(Collectors.toMap(TabletMetadata::getExtent, tm -> tm));
      }
      long t2 = System.currentTimeMillis();
      LOG.trace("Read metadata for {} tablets in {} ms", keys.size(), t2 - t1);
      return tms;
    }
  }

  protected ThriftScanClientHandler delegate;
  private UUID serverLockUUID;
  private final TabletMetadataLoader tabletMetadataLoader;
  private final LoadingCache<KeyExtent,TabletMetadata> tabletMetadataCache;
  private final ThreadPoolExecutor tmCacheExecutor;
  // tracks file reservations that are in the process of being added or removed from the metadata
  // table
  private final Set<StoredTabletFile> influxFiles = new HashSet<>();
  // a read lock that ensures files are not removed from reservedFiles while its held
  private final ReentrantReadWriteLock.ReadLock reservationsReadLock;
  // a write lock that must be held when mutating influxFiles or when removing entries from
  // reservedFiles
  private final ReentrantReadWriteLock.WriteLock reservationsWriteLock;
  // this condition is used to signal changes to influxFiles
  private final Condition reservationCondition;
  // the key is the set of files that have reservations in the metadata table, the value contains
  // information about which scans are currently using the file
  private final Map<StoredTabletFile,ReservedFile> reservedFiles = new ConcurrentHashMap<>();
  private final AtomicLong nextScanReservationId = new AtomicLong();

  private final ServerContext context;
  private final SessionManager sessionManager;
  private final TabletServerResourceManager resourceManager;

  private ServiceLock scanServerLock;
  protected TabletServerScanMetrics scanMetrics;
  private ScanServerMetrics scanServerMetrics;
  private BlockCacheMetrics blockCacheMetrics;

  private final ConcurrentHashMap<TableId,Boolean> allowedTables = new ConcurrentHashMap<>();
  private volatile String currentAllowedTableRegex;

  public ScanServer(ServerOpts opts, String[] args) {
    super(ServerId.Type.SCAN_SERVER, opts, ServerContext::new, args);
    context = super.getContext();
    LOG.info("Version " + Constants.VERSION);
    LOG.info("Instance " + getContext().getInstanceID());
    this.sessionManager = new SessionManager(context);

    this.resourceManager = new TabletServerResourceManager(context, this);

    var readWriteLock = new ReentrantReadWriteLock();
    reservationsReadLock = readWriteLock.readLock();
    reservationsWriteLock = readWriteLock.writeLock();
    reservationCondition = readWriteLock.writeLock().newCondition();

    // Note: The way to control the number of concurrent scans that a ScanServer will
    // perform is by using Property.SSERV_SCAN_EXECUTORS_DEFAULT_THREADS or the number
    // of threads in Property.SSERV_SCAN_EXECUTORS_PREFIX.

    long cacheExpiration =
        getConfiguration().getTimeInMillis(Property.SSERV_CACHED_TABLET_METADATA_EXPIRATION);

    long scanServerReservationExpiration =
        getConfiguration().getTimeInMillis(Property.SSERV_SCAN_REFERENCE_EXPIRATION_TIME);

    tabletMetadataLoader = new TabletMetadataLoader(getContext().getAmple());

    if (cacheExpiration == 0L) {
      LOG.warn("Tablet metadata caching disabled, may cause excessive scans on metadata table.");
      tabletMetadataCache = null;
      tmCacheExecutor = null;
    } else {
      if (cacheExpiration < 60000) {
        LOG.warn(
            "Tablet metadata caching less than one minute, may cause excessive scans on metadata table.");
      }

      // Get the cache refresh percentage property
      // Value must be less than 100% as 100 or over would effectively disable it
      double cacheRefreshPercentage =
          getConfiguration().getFraction(Property.SSERV_CACHED_TABLET_METADATA_REFRESH_PERCENT);
      Preconditions.checkArgument(cacheRefreshPercentage < cacheExpiration,
          "Tablet metadata cache refresh percentage is '%s' but must be less than 1",
          cacheRefreshPercentage);

      tmCacheExecutor = context.threadPools().getPoolBuilder(SCAN_SERVER_TABLET_METADATA_CACHE_POOL)
          .numCoreThreads(8).enableThreadPoolMetrics().build();
      var builder =
          context.getCaches().createNewBuilder(CacheName.SCAN_SERVER_TABLET_METADATA, true)
              .expireAfterWrite(cacheExpiration, TimeUnit.MILLISECONDS)
              .scheduler(Scheduler.systemScheduler()).executor(tmCacheExecutor).recordStats();
      if (cacheRefreshPercentage > 0) {
        // Compute the refresh time as a percentage of the expiration time
        // Cache hits after this time, but before expiration, will trigger a background
        // non-blocking refresh of the entry so future cache hits get an updated entry
        // without having to block for a refresh
        long cacheRefresh = (long) (cacheExpiration * cacheRefreshPercentage);
        LOG.debug("Tablet metadata refresh percentage set to {}, refresh time set to {} ms",
            cacheRefreshPercentage, cacheRefresh);
        builder.refreshAfterWrite(cacheRefresh, TimeUnit.MILLISECONDS);
      } else {
        LOG.warn("Tablet metadata cache refresh disabled, may cause blocking on cache expiration.");
      }
      tabletMetadataCache = builder.build(tabletMetadataLoader);
    }

    delegate = newThriftScanClientHandler(new WriteTracker());

    ThreadPools.watchCriticalScheduledTask(getContext().getScheduledExecutor()
        .scheduleWithFixedDelay(() -> cleanUpReservedFiles(scanServerReservationExpiration),
            scanServerReservationExpiration, scanServerReservationExpiration,
            TimeUnit.MILLISECONDS));

  }

  @Override
  protected String getResourceGroupPropertyValue(SiteConfiguration conf) {
    return conf.get(Property.SSERV_GROUP_NAME);
  }

  @VisibleForTesting
  protected ThriftScanClientHandler newThriftScanClientHandler(WriteTracker writeTracker) {
    return new ThriftScanClientHandler(this, writeTracker);
  }

  /**
   * Start the thrift service to handle incoming client requests
   *
   * @throws UnknownHostException host unknown
   */
  protected void startScanServerClientService() throws UnknownHostException {

    // This class implements TabletClientService.Iface and then delegates calls. Be sure
    // to set up the ThriftProcessor using this class, not the delegate.
    ClientServiceHandler clientHandler = new ClientServiceHandler(context);
    TProcessor processor =
        ThriftProcessorTypes.getScanServerTProcessor(this, clientHandler, this, getContext());

    updateThriftServer(() -> {
      return TServerUtils.createThriftServer(getContext(), getBindAddress(),
          Property.SSERV_CLIENTPORT, processor, this.getClass().getSimpleName(),
          Property.SSERV_PORTSEARCH, Property.SSERV_MINTHREADS, Property.SSERV_MINTHREADS_TIMEOUT,
          Property.SSERV_THREADCHECK);
    }, true);
  }

  /**
   * Set up nodes and locks in ZooKeeper for this ScanServer
   */
  private ServiceLock announceExistence() {
    final ZooReaderWriter zoo = getContext().getZooSession().asReaderWriter();
    try {

      final ServiceLockPath zLockPath =
          context.getServerPaths().createScanServerPath(getResourceGroup(), getAdvertiseAddress());
      ServiceLockSupport.createNonHaServiceLockPath(Type.SCAN_SERVER, zoo, zLockPath);
      serverLockUUID = UUID.randomUUID();
      scanServerLock = new ServiceLock(getContext().getZooSession(), zLockPath, serverLockUUID);
      LockWatcher lw = new ServiceLockWatcher(Type.SCAN_SERVER, () -> getShutdownComplete().get(),
          (type) -> context.getLowMemoryDetector().logGCInfo(getConfiguration()));

      for (int i = 0; i < 120 / 5; i++) {
        zoo.putPersistentData(zLockPath.toString(), new byte[0], NodeExistsPolicy.SKIP);

        ServiceDescriptors descriptors = new ServiceDescriptors();
        for (ThriftService svc : new ThriftService[] {ThriftService.CLIENT,
            ThriftService.TABLET_SCAN}) {
          descriptors.addService(new ServiceDescriptor(serverLockUUID, svc,
              getAdvertiseAddress().toString(), this.getResourceGroup()));
        }

        if (scanServerLock.tryLock(lw, new ServiceLockData(descriptors))) {
          LOG.debug("Obtained scan server lock {}", scanServerLock.getLockPath());
          return scanServerLock;
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
  @SuppressFBWarnings(value = "DM_EXIT", justification = "main class can call System.exit")
  public void run() {

    try {
      waitForUpgrade();
    } catch (InterruptedException e) {
      LOG.error("Interrupted while waiting for upgrade to complete, exiting...");
      System.exit(1);
    }

    SecurityUtil.serverLogin(getConfiguration());
    updateAllowedTables(false);

    try {
      startScanServerClientService();
    } catch (UnknownHostException e1) {
      throw new RuntimeException("Failed to start the scan server client service", e1);
    }

    MetricsInfo metricsInfo = getContext().getMetricsInfo();

    scanMetrics = new TabletServerScanMetrics(resourceManager::getOpenFiles);
    sessionManager.setZombieCountConsumer(scanMetrics::setZombieScanThreads);
    scanServerMetrics = new ScanServerMetrics(tabletMetadataCache);
    blockCacheMetrics = new BlockCacheMetrics(resourceManager.getIndexCache(),
        resourceManager.getDataCache(), resourceManager.getSummaryCache());

    metricsInfo.addMetricsProducers(this, scanMetrics, scanServerMetrics, blockCacheMetrics);

    ServiceLock lock = announceExistence();
    this.getContext().setServiceLock(lock);

    int threadPoolSize = getConfiguration().getCount(Property.SSERV_WAL_SORT_MAX_CONCURRENT);
    if (threadPoolSize > 0) {
      final LogSorter logSorter = new LogSorter(this);
      metricsInfo.addMetricsProducers(logSorter);
      try {
        // Attempt to process all existing log sorting work and start a background
        // thread to look for log sorting work in the future
        logSorter.startWatchingForRecoveryLogs(threadPoolSize);
      } catch (Exception ex) {
        LOG.error("Error starting LogSorter");
        throw new RuntimeException(ex);
      }
    } else {
      LOG.info(
          "Log sorting for tablet recovery is disabled, SSERV_WAL_SORT_MAX_CONCURRENT is less than 1.");
    }

    metricsInfo.init(MetricsInfo.serviceTags(getContext().getInstanceName(), getApplicationName(),
        getAdvertiseAddress(), getResourceGroup()));

    while (!isShutdownRequested()) {
      if (Thread.currentThread().isInterrupted()) {
        LOG.info("Server process thread has been interrupted, shutting down");
        break;
      }
      try {
        Thread.sleep(1000);
        updateIdleStatus(
            sessionManager.getActiveScans().isEmpty() && tabletMetadataCache.estimatedSize() == 0);
        updateAllowedTables(false);
      } catch (InterruptedException e) {
        LOG.info("Interrupt Exception received, shutting down");
        gracefulShutdown(getContext().rpcCreds());
      }
    }

    // Wait for scans to get to zero
    while (!sessionManager.getActiveScans().isEmpty()) {
      LOG.debug("Waiting on {} active scans to complete.", sessionManager.getActiveScans().size());
      UtilWaitThread.sleep(1000);
    }

    LOG.debug("Stopping Thrift Servers");
    getThriftServer().stop();

    try {
      LOG.info("Removing server scan references");
      this.getContext().getAmple().scanServerRefs().delete(getAdvertiseAddress().toString(),
          serverLockUUID);
    } catch (Exception e) {
      LOG.warn("Failed to remove scan server refs from metadata location", e);
    }

    try {
      LOG.debug("Closing filesystems");
      VolumeManager mgr = getContext().getVolumeManager();
      if (null != mgr) {
        mgr.close();
      }
    } catch (IOException e) {
      LOG.warn("Failed to close filesystem : {}", e.getMessage(), e);
    }

    if (tmCacheExecutor != null) {
      LOG.debug("Shutting down TabletMetadataCache executor");
      tmCacheExecutor.shutdownNow();
    }
  }

  // Visible for testing
  protected boolean isAllowed(TCredentials credentials, TableId tid)
      throws ThriftSecurityException {
    Boolean result = allowedTables.get(tid);
    if (result == null) {

      final Retry retry = Retry.builder().maxRetries(10).retryAfter(Duration.ofSeconds(1))
          .incrementBy(Duration.ZERO).maxWait(Duration.ofSeconds(2)).backOffFactor(1.0)
          .logInterval(Duration.ofSeconds(3)).createRetry();

      while (result == null && retry.canRetry()) {
        try {
          retry.waitForNextAttempt(LOG,
              "Allowed tables mapping does not contain an entry for table: " + tid
                  + ", refreshing table...");
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          LOG.error("Interrupted while waiting for next retry", e);
          break;
        }
        // Clear the cache and try again, maybe there
        // is a race condition in table creation and scan
        updateAllowedTables(true);
        // validate that the table exists, else throw
        delegate.getNamespaceId(credentials, tid);
        result = allowedTables.get(tid);
        retry.useRetry();
      }

      if (result == null) {
        // Ran out of retries
        throw new IllegalStateException(
            "Unable to get allowed table mapping for table: " + tid + " within 10s");
      }
    }
    return result;
  }

  private synchronized void updateAllowedTables(boolean clearCache) {

    LOG.trace("Updating allowed tables for ScanServer");
    if (clearCache) {
      context.clearTableListCache();
    }

    // Remove tables that no longer exist
    allowedTables.keySet().forEach(tid -> {
      if (!getContext().tableNodeExists(tid)) {
        LOG.trace("Removing table {} from allowed table map as it no longer exists", tid);
        allowedTables.remove(tid);
      }
    });

    @SuppressWarnings("deprecation")
    final String oldPropName = Property.SSERV_SCAN_ALLOWED_TABLES_DEPRECATED.getKey()
        + this.getResourceGroup().canonical();
    final String oldPropVal = getConfiguration().get(oldPropName);

    final String propName = Property.SSERV_SCAN_ALLOWED_TABLES.getKey();
    final String propVal = getConfiguration().get(propName);

    String allowedTableRegex = null;
    if (propVal != null && oldPropVal != null) {
      LOG.warn(
          "Property {} is deprecated, using value from replacement property {}. Remove old property from config.",
          oldPropName, propName);
      allowedTableRegex = propVal;
    } else if (propVal == null && oldPropVal != null) {
      LOG.warn("Property {} is deprecated, please use the newer replacement property {}",
          oldPropName, propName);
      allowedTableRegex = oldPropVal;
    } else if (propVal != null && oldPropVal == null) {
      allowedTableRegex = propVal;
    }

    if (allowedTableRegex == null) {
      allowedTableRegex = Property.SSERV_SCAN_ALLOWED_TABLES.getDefaultValue();
    }

    if (currentAllowedTableRegex == null) {
      LOG.trace("Property {} initial value: {}", propName, allowedTableRegex);
    } else if (currentAllowedTableRegex.equals(allowedTableRegex)) {
      // Property value has not changed, do nothing
    } else {
      LOG.info("Property {} has changed. Old value: {}, new value: {}", propName,
          currentAllowedTableRegex, allowedTableRegex);
    }

    Pattern allowedTablePattern;
    try {
      allowedTablePattern = Pattern.compile(allowedTableRegex);
      // Regex is valid, store it
      currentAllowedTableRegex = allowedTableRegex;
    } catch (PatternSyntaxException e) {
      LOG.error(
          "Property {} contains an invalid regular expression. Property value: {}. Disabling all tables.",
          propName, allowedTableRegex);
      allowedTablePattern = null;
    }

    Pattern p = allowedTablePattern;
    context.createTableIdToQualifiedNameMap().entrySet().forEach(te -> {
      String tname = te.getValue();
      TableId tid = te.getKey();
      LOG.info("Table Mapping: {} -> {}", tid, tname);
      if (p == null) {
        allowedTables.put(tid, Boolean.FALSE);
      } else {
        Matcher m = p.matcher(tname);
        if (m.matches()) {
          LOG.trace("Table {} can now be scanned via this ScanServer", tname);
          allowedTables.put(tid, Boolean.TRUE);
        } else {
          LOG.trace("Table {} cannot be scanned via this ScanServer", tname);
          allowedTables.put(tid, Boolean.FALSE);
        }
      }
    });
  }

  @SuppressWarnings("unchecked")
  private Map<KeyExtent,TabletMetadata> getTabletMetadata(Collection<KeyExtent> extents) {
    if (tabletMetadataCache == null) {
      return (Map<KeyExtent,TabletMetadata>) tabletMetadataLoader
          .loadAll((Set<? extends KeyExtent>) extents);
    } else {
      return tabletMetadataCache.getAll(extents);
    }
  }

  static class ReservedFile {
    final Set<Long> activeReservations = new ConcurrentSkipListSet<>();
    final AtomicLong lastUseTime = new AtomicLong(0);

    boolean shouldDelete(long expireTimeMs) {
      return activeReservations.isEmpty()
          && System.currentTimeMillis() - lastUseTime.get() > expireTimeMs;
    }
  }

  class ScanReservation implements AutoCloseable {

    private final Collection<StoredTabletFile> files;
    private final long myReservationId;
    private final Map<KeyExtent,TabletMetadata> tabletsMetadata;
    private final Map<TKeyExtent,List<TRange>> failures;

    /* This constructor is called when starting a scan */
    ScanReservation(Map<KeyExtent,TabletMetadata> tabletsMetadata, long myReservationId,
        Map<TKeyExtent,List<TRange>> failures) {
      this.tabletsMetadata = tabletsMetadata;
      this.failures = failures;
      this.files = tabletsMetadata.values().stream().flatMap(tm -> tm.getFiles().stream())
          .collect(Collectors.toUnmodifiableSet());
      this.myReservationId = myReservationId;
    }

    /* This constructor is called when continuing a scan */
    ScanReservation(Collection<StoredTabletFile> files, long myReservationId) {
      this.tabletsMetadata = null;
      this.failures = Map.of();
      this.files = files;
      this.myReservationId = myReservationId;
    }

    public TabletMetadata getTabletMetadata(KeyExtent extent) {
      return tabletsMetadata.get(extent);
    }

    public Set<KeyExtent> getTabletMetadataExtents() {
      return tabletsMetadata.keySet();
    }

    public Map<TKeyExtent,List<TRange>> getFailures() {
      return this.failures;
    }

    SnapshotTablet newTablet(ScanServer server, KeyExtent extent) throws IOException {
      var tabletMetadata = getTabletMetadata(extent);
      TabletResourceManager trm =
          resourceManager.createTabletResourceManager(tabletMetadata.getExtent(),
              context.getTableConfiguration(tabletMetadata.getExtent().tableId()));
      return new SnapshotTablet(server, tabletMetadata, trm);
    }

    @Override
    public void close() {
      // There is no need to get a lock for removing our reservations. The reservation was added
      // with a lock held and once its added that prevents file from being removed.
      for (StoredTabletFile file : files) {
        var reservedFile = reservedFiles.get(file);

        if (!reservedFile.activeReservations.remove(myReservationId)) {
          throw new IllegalStateException("reservation id was not in set as expected");
        }

        LOG.trace("RFFS {} unreserved reference for file {}", myReservationId, file);

        reservedFile.lastUseTime.set(System.currentTimeMillis());
      }
    }
  }

  /*
   * All extents passed in should end up in either the returned map or the failures set, but no
   * extent should be in both.
   */
  private Map<KeyExtent,TabletMetadata> reserveFilesInner(Collection<KeyExtent> extents,
      long myReservationId, Set<KeyExtent> failures) throws AccumuloException {
    // RFS is an acronym for Reference files for scan
    LOG.debug("RFFS {} ensuring files are referenced for scan of extents {}", myReservationId,
        extents);

    Map<KeyExtent,TabletMetadata> tabletsMetadata = getTabletMetadata(extents);
    if (!(tabletsMetadata instanceof HashMap)) {
      // the map returned by getTabletMetadata may not be mutable
      tabletsMetadata = new HashMap<>(tabletsMetadata);
    }

    for (KeyExtent extent : extents) {
      var tabletMetadata = tabletsMetadata.get(extent);
      if (tabletMetadata == null) {
        LOG.info("RFFS {} extent not found in metadata table {}", myReservationId, extent);
        failures.add(extent);
      }

      if (!AssignmentHandler.checkTabletMetadata(extent, null, tabletMetadata, true)) {
        LOG.info("RFFS {} extent unable to load {} as AssignmentHandler returned false",
            myReservationId, extent);
        failures.add(extent);
        tabletsMetadata.remove(extent);
      }
    }

    Map<StoredTabletFile,KeyExtent> allFiles = new HashMap<>();

    tabletsMetadata.forEach((extent, tm) -> {
      tm.getFiles().forEach(file -> allFiles.put(file, extent));
    });

    // The read lock prevents anything from being removed from reservedFiles while adding
    // reservations to the values of reservedFiles. Using a read lock avoids scans from having to
    // wait on each other their files are already reserved.
    reservationsReadLock.lock();
    try {
      if (reservedFiles.keySet().containsAll(allFiles.keySet())) {
        // all files already have reservations in the metadata table, so we can add ourself
        for (StoredTabletFile file : allFiles.keySet()) {
          if (!reservedFiles.get(file).activeReservations.add(myReservationId)) {
            throw new IllegalStateException("reservation id unexpectedly already in set");
          }
        }

        return tabletsMetadata;
      }
    } finally {
      reservationsReadLock.unlock();
    }

    // reservations do not exist in the metadata table, so we will attempt to add them
    reservationsWriteLock.lock();
    try {
      // wait if another thread is working on the files we are interested in
      while (!Collections.disjoint(influxFiles, allFiles.keySet())) {
        reservationCondition.await();
      }

      // add files to reserve to influxFiles so that no other thread tries to add or remove these
      // file from the metadata table or the reservedFiles map
      influxFiles.addAll(allFiles.keySet());
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    } finally {
      reservationsWriteLock.unlock();
    }

    // do not add any code here that could cause an exception which could lead to files not being
    // removed from influxFiles

    try {
      Set<StoredTabletFile> filesToReserve = new HashSet<>();
      List<ScanServerRefTabletFile> refs = new ArrayList<>();
      Set<KeyExtent> tabletsToCheck = new HashSet<>();

      String serverAddress = getAdvertiseAddress().toString();

      for (StoredTabletFile file : allFiles.keySet()) {
        if (!reservedFiles.containsKey(file)) {
          refs.add(new ScanServerRefTabletFile(serverLockUUID, serverAddress,
              file.getNormalizedPathStr()));
          filesToReserve.add(file);
          tabletsToCheck.add(Objects.requireNonNull(allFiles.get(file)));
          LOG.trace("RFFS {} need to add scan ref for file {}", myReservationId, file);
        }
      }

      if (!filesToReserve.isEmpty()) {
        scanServerMetrics.recordWriteOutReservationTime(
            () -> getContext().getAmple().scanServerRefs().put(refs));

        // After we insert the scan server refs we need to check and see if the tablet is still
        // using the file. As long as the tablet is still using the files then the Accumulo GC
        // should not have deleted the files. This assumes the Accumulo GC reads scan server refs
        // after tablet refs from the metadata table.

        if (tabletMetadataCache != null) {
          // lets clear the cache so we get the latest
          tabletMetadataCache.invalidateAll(tabletsToCheck);
        }

        var tabletsToCheckMetadata = getTabletMetadata(tabletsToCheck);

        for (KeyExtent extent : tabletsToCheck) {
          TabletMetadata metadataAfter = tabletsToCheckMetadata.get(extent);
          if (metadataAfter == null) {
            LOG.info("RFFS {} extent unable to load {} as metadata no longer referencing files",
                myReservationId, extent);
            failures.add(extent);
            tabletsMetadata.remove(extent);
          } else {
            // remove files that are still referenced
            filesToReserve.removeAll(metadataAfter.getFiles());
          }
        }

        // if this is not empty it means some files that we reserved are no longer referenced by
        // tablets. This means there could have been a time gap where nothing referenced a file
        // meaning it could have been GCed.
        if (!filesToReserve.isEmpty()) {
          LOG.info("RFFS {} tablet files changed while attempting to reference files {}",
              myReservationId, filesToReserve);
          getContext().getAmple().scanServerRefs().delete(refs);
          scanServerMetrics.incrementReservationConflictCount();
          return null;
        }
      }

      // we do not hold a lock but the files we are adding are in influxFiles so its ok to add to
      // reservedFiles
      for (StoredTabletFile file : allFiles.keySet()) {
        if (!reservedFiles.computeIfAbsent(file, k -> new ReservedFile()).activeReservations
            .add(myReservationId)) {
          throw new IllegalStateException("reservation id unexpectedly already in set");
        }

        LOG.trace("RFFS {} reserved reference for startScan {}", myReservationId, file);
      }

      return tabletsMetadata;
    } finally {
      reservationsWriteLock.lock();
      try {
        allFiles.keySet().forEach(file -> Preconditions.checkState(influxFiles.remove(file)));
        reservationCondition.signal();
      } finally {
        reservationsWriteLock.unlock();
      }
    }
  }

  @VisibleForTesting
  ScanReservation reserveFilesInstrumented(Map<KeyExtent,List<TRange>> extents)
      throws AccumuloException {
    Timer start = Timer.startNew();
    try {
      return reserveFiles(extents);
    } finally {
      scanServerMetrics.recordTotalReservationTime(start.elapsed());
    }

  }

  protected ScanReservation reserveFiles(Map<KeyExtent,List<TRange>> extents)
      throws AccumuloException {

    long myReservationId = nextScanReservationId.incrementAndGet();

    Set<KeyExtent> failedReservations = new HashSet<>();
    Map<KeyExtent,TabletMetadata> tabletsMetadata =
        reserveFilesInner(extents.keySet(), myReservationId, failedReservations);
    while (tabletsMetadata == null) {
      failedReservations.clear();
      tabletsMetadata = reserveFilesInner(extents.keySet(), myReservationId, failedReservations);
    }

    // validate that the tablet metadata set and failure set are disjoint and that the
    // tablet metadata set and failure set contain all of the extents
    if (!Collections.disjoint(tabletsMetadata.keySet(), failedReservations)
        || !extents.keySet().equals(Sets.union(tabletsMetadata.keySet(), failedReservations))) {
      throw new IllegalStateException("bug in reserverFilesInner " + extents.keySet() + ","
          + tabletsMetadata.keySet() + "," + failedReservations);
    }

    // Convert failures
    Map<TKeyExtent,List<TRange>> failures = new HashMap<>();
    failedReservations.forEach(extent -> {
      failures.put(extent.toThrift(), extents.get(extent));
    });

    return new ScanReservation(tabletsMetadata, myReservationId, failures);
  }

  @VisibleForTesting
  ScanReservation reserveFilesInstrumented(long scanId) throws NoSuchScanIDException {
    Timer start = Timer.startNew();
    try {
      return reserveFiles(scanId);
    } finally {
      scanServerMetrics.recordTotalReservationTime(start.elapsed());
    }
  }

  protected ScanReservation reserveFiles(long scanId) throws NoSuchScanIDException {
    var session = (ScanSession<?>) sessionManager.getSession(scanId);
    if (session == null) {
      throw new NoSuchScanIDException();
    }

    Set<StoredTabletFile> scanSessionFiles = getScanSessionFiles(session);

    long myReservationId = nextScanReservationId.incrementAndGet();
    // we are only reserving if the files already exists in reservedFiles, so only need the read
    // lock which prevents deletions from reservedFiles while we mutate the values of reservedFiles
    reservationsReadLock.lock();
    try {
      if (!reservedFiles.keySet().containsAll(scanSessionFiles)) {
        // the files are no longer reserved in the metadata table, so lets pretend there is no scan
        // session
        if (LOG.isTraceEnabled()) {
          LOG.trace("RFFS {} files are no longer referenced on continue scan {} {}",
              myReservationId, scanId, Sets.difference(scanSessionFiles, reservedFiles.keySet()));
        }
        throw new NoSuchScanIDException();
      }

      for (StoredTabletFile file : scanSessionFiles) {
        if (!reservedFiles.get(file).activeReservations.add(myReservationId)) {
          throw new IllegalStateException("reservation id unexpectedly already in set");
        }

        LOG.trace("RFFS {} reserved reference for continue scan {} {}", myReservationId, scanId,
            file);
      }
    } finally {
      reservationsReadLock.unlock();
    }

    return new ScanReservation(scanSessionFiles, myReservationId);
  }

  private static Set<StoredTabletFile> getScanSessionFiles(ScanSession<?> session) {
    if (session instanceof SingleScanSession sss) {
      return Set.copyOf(session.getTabletResolver().getTablet(sss.extent).getDatafiles().keySet());
    } else if (session instanceof MultiScanSession mss) {
      return mss.exents.stream().flatMap(e -> {
        var tablet = mss.getTabletResolver().getTablet(e);
        if (tablet == null) {
          // not all tablets passed to a multiscan are present in the metadata table
          return Stream.empty();
        } else {
          return tablet.getDatafiles().keySet().stream();
        }
      }).collect(Collectors.toUnmodifiableSet());
    } else {
      throw new IllegalArgumentException("Unknown session type " + session.getClass().getName());
    }
  }

  private void cleanUpReservedFiles(long expireTimeMs) {

    // Do a quick check to see if there is any potential work. This check is done to avoid acquiring
    // the write lock unless its needed since the write lock can be disruptive for the read lock.
    if (reservedFiles.values().stream().anyMatch(rf -> rf.shouldDelete(expireTimeMs))) {

      List<ScanServerRefTabletFile> refsToDelete = new ArrayList<>();
      List<StoredTabletFile> confirmed = new ArrayList<>();
      String serverAddress = getAdvertiseAddress().toString();

      reservationsWriteLock.lock();
      try {
        var reservedIter = reservedFiles.entrySet().iterator();

        while (reservedIter.hasNext()) {
          var entry = reservedIter.next();
          var file = entry.getKey();
          if (entry.getValue().shouldDelete(expireTimeMs) && !influxFiles.contains(file)) {
            // if some other thread decides to add the file back again while we try to delete it
            // then adding the file to influxFiles will make it wait until we finish
            influxFiles.add(file);
            confirmed.add(file);
            refsToDelete.add(new ScanServerRefTabletFile(file.getNormalizedPathStr(), serverAddress,
                serverLockUUID));

            // remove the entry from the map while holding the write lock ensuring no new
            // reservations are added to the map values while the metadata operation to delete is
            // running
            reservedIter.remove();
          }
        }
      } finally {
        reservationsWriteLock.unlock();
      }

      if (!confirmed.isEmpty()) {
        try {
          // Do this metadata operation is done w/o holding the lock
          getContext().getAmple().scanServerRefs().delete(refsToDelete);
          if (LOG.isTraceEnabled()) {
            confirmed.forEach(refToDelete -> LOG.trace(
                "RFFS referenced files has not been used recently, removing reference {}",
                refToDelete));
          }
        } finally {
          reservationsWriteLock.lock();
          try {
            confirmed.forEach(file -> Preconditions.checkState(influxFiles.remove(file)));
            reservationCondition.signal();
          } finally {
            reservationsWriteLock.unlock();
          }
        }
      }

    }

    List<StoredTabletFile> candidates = new ArrayList<>();

    reservedFiles.forEach((file, reservationInfo) -> {
      if (reservationInfo.shouldDelete(expireTimeMs)) {
        candidates.add(file);
      }
    });
  }

  /*
   * This simple method exists to be overridden in tests
   */
  protected KeyExtent getKeyExtent(TKeyExtent textent) {
    return KeyExtent.fromThrift(textent);
  }

  protected TabletResolver getScanTabletResolver(final TabletBase tablet) {
    return new TabletResolver() {
      final TabletBase t = tablet;

      @Override
      public TabletBase getTablet(KeyExtent extent) {
        if (extent.equals(t.getExtent())) {
          return t;
        } else {
          LOG.warn("TabletResolver passed the wrong tablet. Known extent: {}, requested extent: {}",
              t.getExtent(), extent);
          return null;
        }
      }

      @Override
      public void close() {
        try {
          t.close(false);
        } catch (IOException e) {
          throw new UncheckedIOException("Error closing tablet", e);
        }
      }
    };
  }

  protected TabletResolver getBatchScanTabletResolver(final HashMap<KeyExtent,TabletBase> tablets) {
    return new TabletResolver() {
      @Override
      public TabletBase getTablet(KeyExtent extent) {
        return tablets.get(extent);
      }

      @Override
      public void close() {
        tablets.forEach((e, t) -> {
          try {
            t.close(false);
          } catch (IOException ex) {
            throw new UncheckedIOException("Error closing tablet: " + e.toString(), ex);
          }
        });
      }
    };
  }

  @Override
  public InitialScan startScan(TInfo tinfo, TCredentials credentials, TKeyExtent textent,
      TRange range, List<TColumn> columns, int batchSize, List<IterInfo> ssiList,
      Map<String,Map<String,String>> ssio, List<ByteBuffer> authorizations, boolean waitForWrites,
      boolean isolated, long readaheadThreshold, TSamplerConfiguration samplerConfig,
      long batchTimeOut, String classLoaderContext, Map<String,String> executionHints,
      long busyTimeout) throws ThriftSecurityException, NotServingTabletException,
      TooManyFilesException, TSampleNotPresentException, TException {

    if (isShutdownRequested()) {
      // Prevent scans from starting if shutting down
      throw new ScanServerBusyException();
    }

    KeyExtent extent = getKeyExtent(textent);

    if (!isAllowed(credentials, extent.tableId())) {
      throw new TApplicationException(TApplicationException.INTERNAL_ERROR,
          "Scan of table " + extent.tableId() + " disallowed by property: "
              + Property.SSERV_SCAN_ALLOWED_TABLES.getKey());
    }

    try (ScanReservation reservation =
        reserveFilesInstrumented(Map.of(extent, Collections.singletonList(range)))) {

      if (reservation.getFailures().containsKey(textent)) {
        throw new NotServingTabletException(extent.toThrift());
      }

      TabletBase tablet = reservation.newTablet(this, extent);

      InitialScan is = delegate.startScan(tinfo, credentials, extent, range, columns, batchSize,
          ssiList, ssio, authorizations, waitForWrites, isolated, readaheadThreshold, samplerConfig,
          batchTimeOut, classLoaderContext, executionHints, getScanTabletResolver(tablet),
          busyTimeout);

      LOG.trace("started scan: {}", is.getScanID());
      return is;
    } catch (ScanServerBusyException be) {
      scanServerMetrics.incrementBusy();
      throw be;
    } catch (AccumuloException | IOException e) {
      LOG.error("Error starting scan", e);
      throw new RuntimeException(e);
    }
  }

  @Override
  public ScanResult continueScan(TInfo tinfo, long scanID, long busyTimeout)
      throws NoSuchScanIDException, NotServingTabletException, TooManyFilesException,
      TSampleNotPresentException, TException {
    LOG.trace("continue scan: {}", scanID);

    try (ScanReservation reservation = reserveFilesInstrumented(scanID)) {
      Preconditions.checkState(reservation.getFailures().isEmpty());
      return delegate.continueScan(tinfo, scanID, busyTimeout);
    } catch (ScanServerBusyException be) {
      scanServerMetrics.incrementBusy();
      throw be;
    }
  }

  @Override
  public void closeScan(TInfo tinfo, long scanID) throws TException {
    LOG.trace("close scan: {}", scanID);
    delegate.closeScan(tinfo, scanID);
  }

  @Override
  public InitialMultiScan startMultiScan(TInfo tinfo, TCredentials credentials,
      Map<TKeyExtent,List<TRange>> tbatch, List<TColumn> tcolumns, List<IterInfo> ssiList,
      Map<String,Map<String,String>> ssio, List<ByteBuffer> authorizations, boolean waitForWrites,
      TSamplerConfiguration tSamplerConfig, long batchTimeOut, String contextArg,
      Map<String,String> executionHints, long busyTimeout)
      throws ThriftSecurityException, TSampleNotPresentException, TException {

    if (isShutdownRequested()) {
      // Prevent scans from starting if shutting down
      throw new ScanServerBusyException();
    }

    if (tbatch.size() == 0) {
      throw new TException("Scan Server batch must include at least one extent");
    }

    final Map<KeyExtent,List<TRange>> batch = new HashMap<>();

    for (Entry<TKeyExtent,List<TRange>> entry : tbatch.entrySet()) {
      KeyExtent extent = getKeyExtent(entry.getKey());

      if (!isAllowed(credentials, extent.tableId())) {
        throw new TApplicationException(TApplicationException.INTERNAL_ERROR,
            "Scan of table " + extent.tableId() + " disallowed by property: "
                + Property.SSERV_SCAN_ALLOWED_TABLES.getKey());
      }

      batch.put(extent, entry.getValue());
    }

    try (ScanReservation reservation = reserveFilesInstrumented(batch)) {

      HashMap<KeyExtent,TabletBase> tablets = new HashMap<>();
      reservation.getTabletMetadataExtents().forEach(extent -> {
        try {
          tablets.put(extent, reservation.newTablet(this, extent));
        } catch (IOException e) {
          throw new UncheckedIOException(e);
        }
      });

      InitialMultiScan ims = delegate.startMultiScan(tinfo, credentials, tcolumns, ssiList, batch,
          ssio, authorizations, waitForWrites, tSamplerConfig, batchTimeOut, contextArg,
          executionHints, getBatchScanTabletResolver(tablets), busyTimeout);

      LOG.trace("started multi scan: {}", ims.getScanID());
      return ims;
    } catch (ScanServerBusyException be) {
      scanServerMetrics.incrementBusy();
      throw be;
    } catch (TException e) {
      LOG.error("Error starting multi scan", e);
      throw e;
    } catch (AccumuloException e) {
      LOG.error("Error starting multi scan", e);
      throw new RuntimeException(e);
    }
  }

  @Override
  public MultiScanResult continueMultiScan(TInfo tinfo, long scanID, long busyTimeout)
      throws NoSuchScanIDException, TSampleNotPresentException, TException {
    LOG.trace("continue multi scan: {}", scanID);

    try (ScanReservation reservation = reserveFilesInstrumented(scanID)) {
      Preconditions.checkState(reservation.getFailures().isEmpty());
      return delegate.continueMultiScan(tinfo, scanID, busyTimeout);
    } catch (ScanServerBusyException be) {
      scanServerMetrics.incrementBusy();
      throw be;
    }
  }

  @Override
  public void closeMultiScan(TInfo tinfo, long scanID) throws NoSuchScanIDException, TException {
    LOG.trace("close multi scan: {}", scanID);
    delegate.closeMultiScan(tinfo, scanID);
  }

  @Override
  public List<ActiveScan> getActiveScans(TInfo tinfo, TCredentials credentials)
      throws ThriftSecurityException, TException {
    return delegate.getActiveScans(tinfo, credentials);
  }

  @Override
  public Tablet getOnlineTablet(KeyExtent extent) {
    throw new UnsupportedOperationException();
  }

  @Override
  public SessionManager getSessionManager() {
    return sessionManager;
  }

  @Override
  public TabletServerResourceManager getResourceManager() {
    return resourceManager;
  }

  @Override
  public TabletServerScanMetrics getScanMetrics() {
    return scanMetrics;
  }

  @Override
  public PausedCompactionMetrics getPausedCompactionMetrics() {
    // ScanServer does not perform compactions
    return null;
  }

  @Override
  public Session getSession(long scanID) {
    return sessionManager.getSession(scanID);
  }

  @Override
  public TableConfiguration getTableConfiguration(KeyExtent extent) {
    return getContext().getTableConfiguration(extent.tableId());
  }

  @Override
  public ServiceLock getLock() {
    return scanServerLock;
  }

  @Override
  public BlockCacheConfiguration getBlockCacheConfiguration(AccumuloConfiguration acuConf) {
    return BlockCacheConfiguration.forScanServer(acuConf);
  }

  public static void main(String[] args) throws Exception {
    AbstractServer.startServer(new ScanServer(new ServerOpts(), args), LOG);
  }

}

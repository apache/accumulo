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

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.accumulo.core.util.UtilWaitThread.sleepUninterruptibly;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.clientImpl.thrift.ThriftSecurityException;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.dataImpl.thrift.InitialMultiScan;
import org.apache.accumulo.core.dataImpl.thrift.InitialScan;
import org.apache.accumulo.core.dataImpl.thrift.IterInfo;
import org.apache.accumulo.core.dataImpl.thrift.MultiScanResult;
import org.apache.accumulo.core.dataImpl.thrift.ScanResult;
import org.apache.accumulo.core.dataImpl.thrift.TColumn;
import org.apache.accumulo.core.dataImpl.thrift.TKeyExtent;
import org.apache.accumulo.core.dataImpl.thrift.TRange;
import org.apache.accumulo.core.fate.zookeeper.ServiceLock;
import org.apache.accumulo.core.fate.zookeeper.ServiceLock.LockLossReason;
import org.apache.accumulo.core.fate.zookeeper.ServiceLock.LockWatcher;
import org.apache.accumulo.core.fate.zookeeper.ZooCache;
import org.apache.accumulo.core.fate.zookeeper.ZooReaderWriter;
import org.apache.accumulo.core.fate.zookeeper.ZooUtil.NodeExistsPolicy;
import org.apache.accumulo.core.file.blockfile.cache.impl.BlockCacheConfiguration;
import org.apache.accumulo.core.metadata.ScanServerRefTabletFile;
import org.apache.accumulo.core.metadata.StoredTabletFile;
import org.apache.accumulo.core.metadata.schema.Ample;
import org.apache.accumulo.core.metadata.schema.TabletMetadata;
import org.apache.accumulo.core.metrics.MetricsInfo;
import org.apache.accumulo.core.securityImpl.thrift.TCredentials;
import org.apache.accumulo.core.spi.scan.ScanServerSelector;
import org.apache.accumulo.core.tabletserver.thrift.ActiveScan;
import org.apache.accumulo.core.tabletserver.thrift.NoSuchScanIDException;
import org.apache.accumulo.core.tabletserver.thrift.NotServingTabletException;
import org.apache.accumulo.core.tabletserver.thrift.ScanServerBusyException;
import org.apache.accumulo.core.tabletserver.thrift.TSampleNotPresentException;
import org.apache.accumulo.core.tabletserver.thrift.TSamplerConfiguration;
import org.apache.accumulo.core.tabletserver.thrift.TabletScanClientService;
import org.apache.accumulo.core.tabletserver.thrift.TooManyFilesException;
import org.apache.accumulo.core.trace.thrift.TInfo;
import org.apache.accumulo.core.util.Halt;
import org.apache.accumulo.core.util.HostAndPort;
import org.apache.accumulo.core.util.UtilWaitThread;
import org.apache.accumulo.core.util.threads.ThreadPools;
import org.apache.accumulo.server.AbstractServer;
import org.apache.accumulo.server.GarbageCollectionLogger;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.ServerOpts;
import org.apache.accumulo.server.conf.TableConfiguration;
import org.apache.accumulo.server.fs.VolumeManager;
import org.apache.accumulo.server.rpc.ServerAddress;
import org.apache.accumulo.server.rpc.TServerUtils;
import org.apache.accumulo.server.rpc.ThriftProcessorTypes;
import org.apache.accumulo.server.security.SecurityUtil;
import org.apache.accumulo.tserver.TabletServerResourceManager.TabletResourceManager;
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
import org.apache.thrift.TException;
import org.apache.thrift.TProcessor;
import org.apache.zookeeper.KeeperException;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.beust.jcommander.Parameter;
import com.github.benmanes.caffeine.cache.CacheLoader;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.github.benmanes.caffeine.cache.Scheduler;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;

import io.micrometer.core.instrument.Tag;

public class ScanServer extends AbstractServer
    implements TabletScanClientService.Iface, TabletHostingServer {

  public static class ScanServerOpts extends ServerOpts {
    @Parameter(required = false, names = {"-g", "--group"},
        description = "Optional group name that will be made available to the ScanServerSelector client plugin.  If not specified will be set to '"
            + ScanServerSelector.DEFAULT_SCAN_SERVER_GROUP_NAME
            + "'.  Groups support at least two use cases : dedicating resources to scans and/or using different hardware for scans.")
    private String groupName = ScanServerSelector.DEFAULT_SCAN_SERVER_GROUP_NAME;

    public String getGroupName() {
      return groupName;
    }
  }

  private static final Logger log = LoggerFactory.getLogger(ScanServer.class);

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
      long t1 = System.currentTimeMillis();
      @SuppressWarnings("unchecked")
      var tms = ample.readTablets().forTablets((Collection<KeyExtent>) keys).build().stream()
          .collect(Collectors.toMap(tm -> tm.getExtent(), tm -> tm));
      long t2 = System.currentTimeMillis();
      LOG.trace("Read metadata for {} tablets in {} ms", keys.size(), t2 - t1);
      return tms;
    }
  }

  private static final Logger LOG = LoggerFactory.getLogger(ScanServer.class);

  protected ThriftScanClientHandler delegate;
  private UUID serverLockUUID;
  private final TabletMetadataLoader tabletMetadataLoader;
  private final LoadingCache<KeyExtent,TabletMetadata> tabletMetadataCache;
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
  HostAndPort clientAddress;
  private final GarbageCollectionLogger gcLogger = new GarbageCollectionLogger();

  private volatile boolean serverStopRequested = false;
  private ServiceLock scanServerLock;
  protected TabletServerScanMetrics scanMetrics;
  private ScanServerMetrics scanServerMetrics;
  private BlockCacheMetrics blockCacheMetrics;

  private ZooCache managerLockCache;

  private final String groupName;

  public ScanServer(ScanServerOpts opts, String[] args) {
    super("sserver", opts, args);

    context = super.getContext();
    log.info("Version " + Constants.VERSION);
    log.info("Instance " + getContext().getInstanceID());
    this.sessionManager = new SessionManager(context);

    this.resourceManager = new TabletServerResourceManager(context, this);

    this.managerLockCache = new ZooCache(context.getZooReader(), null);

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
        getConfiguration().getTimeInMillis(Property.SSERVER_SCAN_REFERENCE_EXPIRATION_TIME);

    tabletMetadataLoader = new TabletMetadataLoader(getContext().getAmple());

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
              .scheduler(Scheduler.systemScheduler()).recordStats().build(tabletMetadataLoader);
    }

    delegate = newThriftScanClientHandler(new WriteTracker());

    this.groupName = Objects.requireNonNull(opts.getGroupName());

    ThreadPools.watchCriticalScheduledTask(getContext().getScheduledExecutor()
        .scheduleWithFixedDelay(() -> cleanUpReservedFiles(scanServerReservationExpiration),
            scanServerReservationExpiration, scanServerReservationExpiration,
            TimeUnit.MILLISECONDS));

  }

  @VisibleForTesting
  protected ThriftScanClientHandler newThriftScanClientHandler(WriteTracker writeTracker) {
    return new ThriftScanClientHandler(this, writeTracker);
  }

  /**
   * Start the thrift service to handle incoming client requests
   *
   * @return address of this client service
   * @throws UnknownHostException host unknown
   */
  protected ServerAddress startScanServerClientService() throws UnknownHostException {

    // This class implements TabletClientService.Iface and then delegates calls. Be sure
    // to set up the ThriftProcessor using this class, not the delegate.
    TProcessor processor = ThriftProcessorTypes.getScanServerTProcessor(this, getContext());

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

  public String getClientAddressString() {
    if (clientAddress == null) {
      return null;
    }
    return clientAddress.getHost() + ":" + clientAddress.getPort();
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
        // Old zk nodes can be cleaned up by ZooZap
        zoo.putPersistentData(zLockPath.toString(), new byte[] {}, NodeExistsPolicy.SKIP);
      } catch (KeeperException e) {
        if (e.code() == KeeperException.Code.NOAUTH) {
          LOG.error("Failed to write to ZooKeeper. Ensure that"
              + " accumulo.properties, specifically instance.secret, is consistent.");
        }
        throw e;
      }

      serverLockUUID = UUID.randomUUID();
      scanServerLock = new ServiceLock(zoo.getZooKeeper(), zLockPath, serverLockUUID);

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

      // Don't use the normal ServerServices lock content, instead put the server UUID here.
      byte[] lockContent = (serverLockUUID.toString() + "," + groupName).getBytes(UTF_8);

      // wait for 120 seconds with 5 second delay
      for (int i = 0; i < 120 / 5; i++) {
        zoo.putPersistentData(zLockPath.toString(), new byte[0], NodeExistsPolicy.SKIP);

        if (scanServerLock.tryLock(lw, lockContent)) {
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
  public void run() {
    SecurityUtil.serverLogin(getConfiguration());

    ServerAddress address = null;
    try {
      address = startScanServerClientService();
      clientAddress = address.getAddress();
    } catch (UnknownHostException e1) {
      throw new RuntimeException("Failed to start the compactor client service", e1);
    }

    MetricsInfo metricsInfo = getContext().getMetricsInfo();
    metricsInfo.addServiceTags(getApplicationName(), clientAddress);
    metricsInfo.addCommonTags(List.of(Tag.of("resource.group", groupName)));

    scanMetrics = new TabletServerScanMetrics();
    scanServerMetrics = new ScanServerMetrics(tabletMetadataCache);
    blockCacheMetrics = new BlockCacheMetrics(resourceManager.getIndexCache(),
        resourceManager.getDataCache(), resourceManager.getSummaryCache());

    metricsInfo.addMetricsProducers(scanMetrics, scanServerMetrics, blockCacheMetrics);
    metricsInfo.init();
    // We need to set the compaction manager so that we don't get an NPE in CompactableImpl.close

    ServiceLock lock = announceExistence();

    try {
      while (!serverStopRequested) {
        UtilWaitThread.sleep(1000);
      }
    } finally {
      LOG.info("Stopping Thrift Servers");
      address.server.stop();

      LOG.info("Removing server scan references");
      this.getContext().getAmple().deleteScanServerFileReferences(clientAddress.toString(),
          serverLockUUID);

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

      String serverAddress = clientAddress.toString();

      for (StoredTabletFile file : allFiles.keySet()) {
        if (!reservedFiles.containsKey(file)) {
          refs.add(new ScanServerRefTabletFile(file.getPathStr(), serverAddress, serverLockUUID));
          filesToReserve.add(file);
          tabletsToCheck.add(Objects.requireNonNull(allFiles.get(file)));
          LOG.trace("RFFS {} need to add scan ref for file {}", myReservationId, file);
        }
      }

      if (!filesToReserve.isEmpty()) {
        getContext().getAmple().putScanServerFileReferences(refs);

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
          getContext().getAmple().deleteScanServerFileReferences(refs);
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
    long start = System.nanoTime();
    try {
      return reserveFiles(extents);
    } finally {
      scanServerMetrics.getReservationTimer().record(System.nanoTime() - start,
          TimeUnit.NANOSECONDS);
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
    long start = System.nanoTime();
    try {
      return reserveFiles(scanId);
    } finally {
      scanServerMetrics.getReservationTimer().record(System.nanoTime() - start,
          TimeUnit.NANOSECONDS);
    }
  }

  protected ScanReservation reserveFiles(long scanId) throws NoSuchScanIDException {
    var session = (ScanSession) sessionManager.getSession(scanId);
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

  private static Set<StoredTabletFile> getScanSessionFiles(ScanSession session) {
    if (session instanceof SingleScanSession) {
      var sss = (SingleScanSession) session;
      return Set.copyOf(session.getTabletResolver().getTablet(sss.extent).getDatafiles().keySet());
    } else if (session instanceof MultiScanSession) {
      var mss = (MultiScanSession) session;
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
      String serverAddress = clientAddress.toString();

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
            refsToDelete
                .add(new ScanServerRefTabletFile(file.getPathStr(), serverAddress, serverLockUUID));

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
          getContext().getAmple().deleteScanServerFileReferences(refsToDelete);
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

  /* Exposed for testing */
  protected boolean isSystemUser(TCredentials creds) {
    return context.getSecurityOperation().isSystemUser(creds);
  }

  @Override
  public InitialScan startScan(TInfo tinfo, TCredentials credentials, TKeyExtent textent,
      TRange range, List<TColumn> columns, int batchSize, List<IterInfo> ssiList,
      Map<String,Map<String,String>> ssio, List<ByteBuffer> authorizations, boolean waitForWrites,
      boolean isolated, long readaheadThreshold, TSamplerConfiguration samplerConfig,
      long batchTimeOut, String classLoaderContext, Map<String,String> executionHints,
      long busyTimeout) throws ThriftSecurityException, NotServingTabletException,
      TooManyFilesException, TSampleNotPresentException, TException {

    KeyExtent extent = getKeyExtent(textent);

    if (extent.isMeta() && !isSystemUser(credentials)) {
      throw new TException(
          "Only the system user can perform eventual consistency scans on the root and metadata tables");
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

    if (tbatch.size() == 0) {
      throw new TException("Scan Server batch must include at least one extent");
    }

    final Map<KeyExtent,List<TRange>> batch = new HashMap<>();

    for (Entry<TKeyExtent,List<TRange>> entry : tbatch.entrySet()) {
      KeyExtent extent = getKeyExtent(entry.getKey());

      if (extent.isMeta() && !context.getSecurityOperation().isSystemUser(credentials)) {
        throw new TException(
            "Only the system user can perform eventual consistency scans on the root and metadata tables");
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

      LOG.trace("started scan: {}", ims.getScanID());
      return ims;
    } catch (ScanServerBusyException be) {
      scanServerMetrics.incrementBusy();
      throw be;
    } catch (TException e) {
      LOG.error("Error starting scan", e);
      throw e;
    } catch (AccumuloException e) {
      LOG.error("Error starting scan", e);
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
  public ZooCache getManagerLockCache() {
    return managerLockCache;
  }

  @Override
  public GarbageCollectionLogger getGcLogger() {
    return gcLogger;
  }

  @Override
  public BlockCacheConfiguration getBlockCacheConfiguration(AccumuloConfiguration acuConf) {
    return BlockCacheConfiguration.forScanServer(acuConf);
  }

  public static void main(String[] args) throws Exception {
    try (ScanServer tserver = new ScanServer(new ScanServerOpts(), args)) {
      tserver.runServer();
    }
  }

}

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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.clientImpl.thrift.ConfigurationType;
import org.apache.accumulo.core.clientImpl.thrift.TDiskUsage;
import org.apache.accumulo.core.clientImpl.thrift.ThriftSecurityException;
import org.apache.accumulo.core.clientImpl.thrift.ThriftTableOperationException;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
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
import org.apache.accumulo.core.file.blockfile.cache.impl.BlockCacheConfiguration;
import org.apache.accumulo.core.master.thrift.TabletServerStatus;
import org.apache.accumulo.core.metadata.ScanServerRefTabletFile;
import org.apache.accumulo.core.metadata.StoredTabletFile;
import org.apache.accumulo.core.metadata.schema.Ample;
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
import org.apache.accumulo.core.util.threads.ThreadPools;
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
import org.apache.accumulo.tserver.session.MultiScanSession;
import org.apache.accumulo.tserver.session.ScanSession;
import org.apache.accumulo.tserver.session.ScanSession.TabletResolver;
import org.apache.accumulo.tserver.session.SingleScanSession;
import org.apache.accumulo.tserver.tablet.Tablet;
import org.apache.accumulo.tserver.tablet.TabletData;
import org.apache.commons.lang3.tuple.MutableTriple;
import org.apache.thrift.TException;
import org.apache.zookeeper.KeeperException;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.benmanes.caffeine.cache.CacheLoader;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.github.benmanes.caffeine.cache.Scheduler;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Sets;

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
      return Collections.emptyList();
    }

    @Override
    public void compactableClosed(KeyExtent extent, Set<CompactionServiceId> servicesUsed,
        Set<ExternalCompactionId> ecids) {}

  }

  public static class ScanServerCompactionExecutorMetrics extends CompactionExecutorsMetrics {

    @Override
    protected void startUpdateThread() {}

  }

  private static final Logger LOG = LoggerFactory.getLogger(ScanServer.class);

  protected ThriftClientHandler handler;
  private UUID serverLockUUID;
  private final TabletMetadataLoader tabletMetadataLoader;
  private final LoadingCache<KeyExtent,TabletMetadata> tabletMetadataCache;
  protected Set<StoredTabletFile> lockedFiles = new HashSet<>();
  protected Map<StoredTabletFile,ReservedFile> reservedFiles = new ConcurrentHashMap<>();
  protected AtomicLong nextScanReservationId = new AtomicLong();

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
    @SuppressWarnings("unchecked")
    public Map<? extends KeyExtent,? extends TabletMetadata>
        loadAll(Set<? extends KeyExtent> keys) {
      long t1 = System.currentTimeMillis();
      var tms = ample.readTablets().forTablets((Collection<KeyExtent>) keys).build().stream()
          .collect(Collectors.toMap(tm -> tm.getExtent(), tm -> tm));
      long t2 = System.currentTimeMillis();
      LOG.trace("Read metadata for {} tablets in {} ms", keys.size(), t2 - t1);
      return tms;
    }
  }

  public ScanServer(ServerOpts opts, String[] args) {
    super(opts, args, true);

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
              .scheduler(Scheduler.systemScheduler()).build(tabletMetadataLoader);
    }
    handler = getHandler();

    ThreadPools.watchCriticalScheduledTask(getContext().getScheduledExecutor()
        .scheduleWithFixedDelay(() -> cleanUpReservedFiles(scanServerReservationExpiration),
            scanServerReservationExpiration, scanServerReservationExpiration,
            TimeUnit.MILLISECONDS));

  }

  @VisibleForTesting
  protected ThriftClientHandler getHandler() {
    return new ThriftClientHandler(this);
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
      tabletServerLock = new ServiceLock(zoo.getZooKeeper(), zLockPath, serverLockUUID);

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
      byte[] lockContent = serverLockUUID.toString().getBytes(UTF_8);

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
    ceMetrics = new ScanServerCompactionExecutorMetrics();
    this.compactionManager = new ScanServerCompactionManager(getContext(), ceMetrics);

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
    Set<Long> activeReservations = new ConcurrentSkipListSet<>();
    volatile long lastUseTime;

    boolean shouldDelete(long expireTimeMs) {
      return activeReservations.isEmpty()
          && System.currentTimeMillis() - lastUseTime > expireTimeMs;
    }
  }

  private class FilesLock implements AutoCloseable {

    private final Collection<StoredTabletFile> files;
    private final AtomicBoolean closed = new AtomicBoolean(false);

    public FilesLock(Collection<StoredTabletFile> files) {
      this.files = files;
    }

    Collection<StoredTabletFile> getLockedFiles() {
      return files;
    }

    @Override
    public void close() {
      // only allow close to be called once
      if (!closed.compareAndSet(false, true)) {
        return;
      }

      synchronized (lockedFiles) {
        for (StoredTabletFile file : files) {
          if (!lockedFiles.remove(file)) {
            throw new IllegalStateException("tried to unlock file that was not locked");
          }
        }

        lockedFiles.notifyAll();
      }
    }
  }

  private FilesLock lockFiles(Collection<StoredTabletFile> files) {

    // lets ensure we lock and unlock that same set of files even if the passed in files changes
    var filesCopy = Set.copyOf(files);

    synchronized (lockedFiles) {

      while (!Collections.disjoint(filesCopy, lockedFiles)) {
        try {
          lockedFiles.wait();
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        }
      }

      for (StoredTabletFile file : filesCopy) {
        if (!lockedFiles.add(file)) {
          throw new IllegalStateException("file unexpectedly not added");
        }
      }
    }

    return new FilesLock(filesCopy);
  }

  class ScanReservation implements AutoCloseable {

    private final Collection<StoredTabletFile> files;
    private final long myReservationId;
    private final Map<KeyExtent,TabletMetadata> tabletsMetadata;

    ScanReservation(Map<KeyExtent,TabletMetadata> tabletsMetadata, long myReservationId) {
      this.tabletsMetadata = tabletsMetadata;
      this.files = tabletsMetadata.values().stream().flatMap(tm -> tm.getFiles().stream())
          .collect(Collectors.toUnmodifiableSet());
      this.myReservationId = myReservationId;
    }

    ScanReservation(Collection<StoredTabletFile> files, long myReservationId) {
      this.tabletsMetadata = null;
      this.files = files;
      this.myReservationId = myReservationId;
    }

    public TabletMetadata getTabletMetadata(KeyExtent extent) {
      return tabletsMetadata.get(extent);
    }

    Tablet newTablet(KeyExtent extent) throws IOException {
      var tabletMetadata = getTabletMetadata(extent);
      TabletData data = new TabletData(tabletMetadata);
      TabletResourceManager trm = resourceManager.createTabletResourceManager(
          tabletMetadata.getExtent(), getTableConfiguration(tabletMetadata.getExtent()));
      return new Tablet(ScanServer.this, tabletMetadata.getExtent(), trm, data, true);
    }

    @Override
    public void close() {
      try (FilesLock flock = lockFiles(files)) {
        for (StoredTabletFile file : flock.getLockedFiles()) {
          var reservedFile = reservedFiles.get(file);

          if (!reservedFile.activeReservations.remove(myReservationId)) {
            throw new IllegalStateException("reservation id was not in set as expected");
          }

          LOG.trace("RFFS {} unreserved reference for file {}", myReservationId, file);

          // TODO maybe use nano time
          reservedFile.lastUseTime = System.currentTimeMillis();
        }
      }
    }
  }

  private Map<KeyExtent,TabletMetadata> reserveFilesInner(Collection<KeyExtent> extents,
      long myReservationId) throws NotServingTabletException, AccumuloException {
    // RFS is an acronym for Reference files for scan
    LOG.trace("RFFS {} ensuring files are referenced for scan of extents {}", myReservationId,
        extents);

    Map<KeyExtent,TabletMetadata> tabletsMetadata = getTabletMetadata(extents);

    for (KeyExtent extent : extents) {
      var tabletMetadata = tabletsMetadata.get(extent);
      if (tabletMetadata == null) {
        LOG.trace("RFFS {} extent not found in metadata table {}", myReservationId, extent);
        throw new NotServingTabletException(extent.toThrift());
      }

      if (!AssignmentHandler.checkTabletMetadata(extent, getTabletSession(), tabletMetadata,
          true)) {
        LOG.trace("RFFS {} extent unable to load {} as AssignmentHandler returned false",
            myReservationId, extent);
        throw new NotServingTabletException(extent.toThrift());
      }
    }

    Map<StoredTabletFile,KeyExtent> allFiles = new HashMap<>();

    tabletsMetadata.forEach((extent, tm) -> {
      tm.getFiles().forEach(file -> allFiles.put(file, extent));
    });

    try (FilesLock flock = lockFiles(allFiles.keySet())) {
      Set<StoredTabletFile> filesToReserve = new HashSet<>();
      List<ScanServerRefTabletFile> refs = new ArrayList<>();
      Set<KeyExtent> tabletsToCheck = new HashSet<>();

      String serverAddress = clientAddress.toString();

      for (StoredTabletFile file : flock.getLockedFiles()) {
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
            getContext().getAmple().deleteScanServerFileReferences(refs);
            throw new NotServingTabletException(extent.toThrift());
          }

          // remove files that are still referenced
          filesToReserve.removeAll(metadataAfter.getFiles());
        }

        // if this is not empty it means some files that we reserved are no longer referenced by
        // tablets. This means there could have been a time gap where nothing referenced a file
        // meaning it could have been GCed.
        if (!filesToReserve.isEmpty()) {
          LOG.trace("RFFS {} tablet files changed while attempting to reference files {}",
              myReservationId, filesToReserve);
          getContext().getAmple().deleteScanServerFileReferences(refs);
          return null;
        }
      }

      for (StoredTabletFile file : flock.getLockedFiles()) {
        if (!reservedFiles.computeIfAbsent(file, k -> new ReservedFile()).activeReservations
            .add(myReservationId)) {
          throw new IllegalStateException("reservation id unexpectedly already in set");
        }

        LOG.trace("RFFS {} reserved reference for startScan {}", myReservationId, file);
      }

    }

    return tabletsMetadata;
  }

  protected ScanReservation reserveFiles(Collection<KeyExtent> extents)
      throws NotServingTabletException, AccumuloException {

    long myReservationId = nextScanReservationId.incrementAndGet();

    Map<KeyExtent,TabletMetadata> tabletsMetadata = reserveFilesInner(extents, myReservationId);
    while (tabletsMetadata == null) {
      tabletsMetadata = reserveFilesInner(extents, myReservationId);
    }

    return new ScanReservation(tabletsMetadata, myReservationId);
  }

  protected ScanReservation reserveFiles(long scanId) throws NoSuchScanIDException {
    var session = (ScanSession) sessionManager.getSession(scanId);
    if (session == null) {
      throw new NoSuchScanIDException();
    }

    Set<StoredTabletFile> scanSessionFiles;

    if (session instanceof SingleScanSession) {
      var sss = (SingleScanSession) session;
      scanSessionFiles =
          Set.copyOf(session.getTabletResolver().getTablet(sss.extent).getDatafiles().keySet());
    } else if (session instanceof MultiScanSession) {
      var mss = (MultiScanSession) session;
      scanSessionFiles = mss.exents.stream()
          .flatMap(e -> mss.getTabletResolver().getTablet(e).getDatafiles().keySet().stream())
          .collect(Collectors.toUnmodifiableSet());
    } else {
      throw new IllegalArgumentException("Unknown session type " + session.getClass().getName());
    }

    long myReservationId = nextScanReservationId.incrementAndGet();

    try (FilesLock flock = lockFiles(scanSessionFiles)) {
      if (!reservedFiles.keySet().containsAll(scanSessionFiles)) {
        // the files are no longer reserved in the metadata table, so lets pretend there is no scan
        // session
        if (LOG.isTraceEnabled()) {
          LOG.trace("RFFS {} files are no longer referenced on continue scan {} {}",
              myReservationId, scanId, Sets.difference(scanSessionFiles, reservedFiles.keySet()));
        }
        throw new NoSuchScanIDException();
      }

      for (StoredTabletFile file : flock.getLockedFiles()) {
        if (!reservedFiles.get(file).activeReservations.add(myReservationId)) {
          throw new IllegalStateException("reservation id unexpectedly already in set");
        }

        LOG.trace("RFFS {} reserved reference for continue scan {} {}", myReservationId, scanId,
            file);
      }
    }

    return new ScanReservation(scanSessionFiles, myReservationId);
  }

  private void cleanUpReservedFiles(long expireTimeMs) {
    List<StoredTabletFile> candidates = new ArrayList<>();

    reservedFiles.forEach((file, reservationInfo) -> {
      if (reservationInfo.shouldDelete(expireTimeMs)) {
        candidates.add(file);
      }
    });

    if (!candidates.isEmpty()) {
      // gain exclusive access to files to avoid multiple threads adding/deleting file reservations
      // at same time
      try (FilesLock flock = lockFiles(candidates)) {
        List<ScanServerRefTabletFile> refsToDelete = new ArrayList<>();
        List<StoredTabletFile> confirmed = new ArrayList<>();

        String serverAddress = clientAddress.toString();

        // check that is still a candidate now that files are locked and no other thread should be
        // modifying them
        for (StoredTabletFile candidate : flock.getLockedFiles()) {
          var reservation = reservedFiles.get(candidate);
          if (reservation != null && reservation.shouldDelete(expireTimeMs)) {
            refsToDelete.add(
                new ScanServerRefTabletFile(candidate.getPathStr(), serverAddress, serverLockUUID));
            confirmed.add(candidate);
            LOG.trace("RFFS referenced files has not been used recently, removing reference {}",
                candidate);
          }
        }

        getContext().getAmple().deleteScanServerFileReferences(refsToDelete);

        // those refs were successfully removed from metadata table, so remove them from the map
        reservedFiles.keySet().removeAll(confirmed);

      }
    }
  }

  /*
   * This simple method exists to be overridden in tests
   */
  protected KeyExtent getKeyExtent(TKeyExtent textent) {
    return KeyExtent.fromThrift(textent);
  }

  protected TabletResolver getScanTabletResolver(final Tablet tablet) {
    return new TabletResolver() {
      @Override
      public Tablet getTablet(KeyExtent extent) {
        if (extent.equals(tablet.getExtent())) {
          return tablet;
        } else {
          return null;
        }
      }
    };
  }

  protected TabletResolver getBatchScanTabletResolver(final HashMap<KeyExtent,Tablet> tablets) {
    return tablets::get;
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

    try (ScanReservation reservation = reserveFiles(Collections.singleton(extent))) {

      Tablet tablet = reservation.newTablet(extent);

      InitialScan is = handler.startScan(tinfo, credentials, extent, range, columns, batchSize,
          ssiList, ssio, authorizations, waitForWrites, isolated, readaheadThreshold, samplerConfig,
          batchTimeOut, classLoaderContext, executionHints, getScanTabletResolver(tablet),
          busyTimeout);

      return is;

    } catch (AccumuloException | IOException e) {
      LOG.error("Error starting scan", e);
      throw new RuntimeException(e);
    }
  }

  @Override
  public ScanResult continueScan(TInfo tinfo, long scanID, long busyTimeout)
      throws NoSuchScanIDException, NotServingTabletException, TooManyFilesException,
      TSampleNotPresentException, TException {
    LOG.debug("continue scan: {}", scanID);

    try (ScanReservation reservation = reserveFiles(scanID)) {
      return handler.continueScan(tinfo, scanID, busyTimeout);
    }
  }

  @Override
  public void closeScan(TInfo tinfo, long scanID) throws TException {
    LOG.debug("close scan: {}", scanID);
    handler.closeScan(tinfo, scanID);
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
      batch.put(extent, entry.getValue());
    }

    try (ScanReservation reservation = reserveFiles(batch.keySet())) {

      HashMap<KeyExtent,Tablet> tablets = new HashMap<>();
      batch.keySet().forEach(extent -> {
        try {
          tablets.put(extent, reservation.newTablet(extent));
        } catch (IOException e) {
          throw new UncheckedIOException(e);
        }
      });

      InitialMultiScan ims = handler.startMultiScan(tinfo, credentials, tcolumns, ssiList, batch,
          ssio, authorizations, waitForWrites, tSamplerConfig, batchTimeOut, contextArg,
          executionHints, getBatchScanTabletResolver(tablets), busyTimeout);

      LOG.debug("started scan: {}", ims.getScanID());
      return ims;
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
    LOG.debug("continue multi scan: {}", scanID);

    try (ScanReservation reservation = reserveFiles(scanID)) {
      return handler.continueMultiScan(tinfo, scanID, busyTimeout);
    }
  }

  @Override
  public void closeMultiScan(TInfo tinfo, long scanID) throws NoSuchScanIDException, TException {
    LOG.debug("close multi scan: {}", scanID);
    handler.closeMultiScan(tinfo, scanID);
  }

  @Override
  public BlockCacheConfiguration getBlockCacheConfiguration(AccumuloConfiguration acuConf) {
    return new BlockCacheConfiguration(acuConf, Property.SSERV_PREFIX,
        Property.SSERV_INDEXCACHE_SIZE, Property.SSERV_DATACACHE_SIZE,
        Property.SSERV_SUMMARYCACHE_SIZE, Property.SSERV_DEFAULT_BLOCKSIZE);
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

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
package org.apache.accumulo.manager.compaction.coordinator;

import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.toList;
import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.COMPACTED;
import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.ECOMP;
import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.FILES;
import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.LOCATION;
import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.OPID;
import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.SELECTED;
import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.USER_COMPACTION_REQUESTED;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.ref.SoftReference;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.TableDeletedException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.CompactionConfig;
import org.apache.accumulo.core.client.admin.compaction.CompactableFile;
import org.apache.accumulo.core.client.admin.servers.ServerId;
import org.apache.accumulo.core.clientImpl.thrift.SecurityErrorCode;
import org.apache.accumulo.core.clientImpl.thrift.TInfo;
import org.apache.accumulo.core.clientImpl.thrift.TableOperation;
import org.apache.accumulo.core.clientImpl.thrift.TableOperationExceptionType;
import org.apache.accumulo.core.clientImpl.thrift.ThriftSecurityException;
import org.apache.accumulo.core.clientImpl.thrift.ThriftTableOperationException;
import org.apache.accumulo.core.compaction.thrift.CompactionCoordinatorService;
import org.apache.accumulo.core.compaction.thrift.TCompactionState;
import org.apache.accumulo.core.compaction.thrift.TCompactionStatusUpdate;
import org.apache.accumulo.core.compaction.thrift.TExternalCompaction;
import org.apache.accumulo.core.compaction.thrift.TExternalCompactionList;
import org.apache.accumulo.core.compaction.thrift.TExternalCompactionMap;
import org.apache.accumulo.core.compaction.thrift.TNextCompactionJob;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.NamespaceId;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.dataImpl.thrift.TKeyExtent;
import org.apache.accumulo.core.fate.Fate;
import org.apache.accumulo.core.fate.FateId;
import org.apache.accumulo.core.fate.FateInstanceType;
import org.apache.accumulo.core.fate.FateKey;
import org.apache.accumulo.core.fate.zookeeper.ZooReaderWriter;
import org.apache.accumulo.core.iteratorsImpl.system.SystemIteratorUtil;
import org.apache.accumulo.core.logging.TabletLogger;
import org.apache.accumulo.core.manager.state.tables.TableState;
import org.apache.accumulo.core.metadata.CompactableFileImpl;
import org.apache.accumulo.core.metadata.ReferencedTabletFile;
import org.apache.accumulo.core.metadata.StoredTabletFile;
import org.apache.accumulo.core.metadata.schema.Ample;
import org.apache.accumulo.core.metadata.schema.Ample.DataLevel;
import org.apache.accumulo.core.metadata.schema.Ample.RejectionHandler;
import org.apache.accumulo.core.metadata.schema.CompactionMetadata;
import org.apache.accumulo.core.metadata.schema.ExternalCompactionId;
import org.apache.accumulo.core.metadata.schema.TabletMetadata;
import org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType;
import org.apache.accumulo.core.metadata.schema.TabletsMetadata;
import org.apache.accumulo.core.metadata.schema.filters.HasExternalCompactionsFilter;
import org.apache.accumulo.core.metrics.MetricsProducer;
import org.apache.accumulo.core.securityImpl.thrift.TCredentials;
import org.apache.accumulo.core.spi.compaction.CompactionJob;
import org.apache.accumulo.core.spi.compaction.CompactionKind;
import org.apache.accumulo.core.spi.compaction.CompactionPlanner;
import org.apache.accumulo.core.spi.compaction.CompactionServiceId;
import org.apache.accumulo.core.spi.compaction.CompactorGroupId;
import org.apache.accumulo.core.tabletserver.thrift.InputFile;
import org.apache.accumulo.core.tabletserver.thrift.IteratorConfig;
import org.apache.accumulo.core.tabletserver.thrift.TCompactionKind;
import org.apache.accumulo.core.tabletserver.thrift.TCompactionStats;
import org.apache.accumulo.core.tabletserver.thrift.TExternalCompactionJob;
import org.apache.accumulo.core.util.Retry;
import org.apache.accumulo.core.util.UtilWaitThread;
import org.apache.accumulo.core.util.cache.Caches.CacheName;
import org.apache.accumulo.core.util.compaction.CompactionPlannerInitParams;
import org.apache.accumulo.core.util.compaction.CompactionServicesConfig;
import org.apache.accumulo.core.util.compaction.ExternalCompactionUtil;
import org.apache.accumulo.core.util.compaction.RunningCompaction;
import org.apache.accumulo.core.util.threads.ThreadPools;
import org.apache.accumulo.core.util.threads.Threads;
import org.apache.accumulo.core.util.time.SteadyTime;
import org.apache.accumulo.core.volume.Volume;
import org.apache.accumulo.manager.Manager;
import org.apache.accumulo.manager.compaction.coordinator.commit.CommitCompaction;
import org.apache.accumulo.manager.compaction.coordinator.commit.CompactionCommitData;
import org.apache.accumulo.manager.compaction.coordinator.commit.RenameCompactionFile;
import org.apache.accumulo.manager.compaction.queue.CompactionJobPriorityQueue;
import org.apache.accumulo.manager.compaction.queue.CompactionJobQueues;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.ServiceEnvironmentImpl;
import org.apache.accumulo.server.compaction.CompactionConfigStorage;
import org.apache.accumulo.server.compaction.CompactionPluginUtils;
import org.apache.accumulo.server.security.SecurityOperation;
import org.apache.accumulo.server.tablets.TabletNameGenerator;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.thrift.TException;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.CacheLoader;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.github.benmanes.caffeine.cache.Weigher;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import com.google.common.net.HostAndPort;

import io.micrometer.core.instrument.MeterRegistry;

public class CompactionCoordinator
    implements CompactionCoordinatorService.Iface, Runnable, MetricsProducer {

  // Object that serves as a TopN view of the RunningCompactions, ordered by
  // RunningCompaction start time. The first entry in this Set should be the
  // oldest RunningCompaction.
  public static class TimeOrderedRunningCompactionSet {

    private static final int UPPER_LIMIT = 50;

    Comparator<RunningCompaction> oldestFirstComparator =
        Comparator.comparingLong(RunningCompaction::getStartTime)
            .thenComparing(rc -> rc.getJob().getExternalCompactionId());
    private final ConcurrentSkipListSet<RunningCompaction> compactions =
        new ConcurrentSkipListSet<>(oldestFirstComparator);

    // Tracking size here as ConcurrentSkipListSet.size() is not constant time
    private final AtomicInteger size = new AtomicInteger(0);

    public int size() {
      return size.get();
    }

    public boolean add(RunningCompaction e) {
      boolean added = compactions.add(e);
      if (added) {
        if (size.incrementAndGet() > UPPER_LIMIT) {
          this.remove(compactions.last());
        }
      }
      return added;
    }

    public boolean remove(Object o) {
      boolean removed = compactions.remove(o);
      if (removed) {
        size.decrementAndGet();
      }
      return removed;
    }

    public Iterator<RunningCompaction> iterator() {
      return compactions.iterator();
    }

    public Stream<RunningCompaction> stream() {
      return compactions.stream();
    }

  }

  private static final Logger LOG = LoggerFactory.getLogger(CompactionCoordinator.class);

  public static final String RESTART_UPDATE_MSG =
      "Coordinator restarted, compaction found in progress";

  /*
   * Map of compactionId to RunningCompactions. This is an informational cache of what external
   * compactions may be running. Its possible it may contain external compactions that are not
   * actually running. It may not contain compactions that are actually running. The metadata table
   * is the most authoritative source of what external compactions are currently running, but it
   * does not have the stats that this map has.
   */
  protected final Map<ExternalCompactionId,RunningCompaction> RUNNING_CACHE =
      new ConcurrentHashMap<>();

  protected final Map<String,TimeOrderedRunningCompactionSet> LONG_RUNNING_COMPACTIONS_BY_RG =
      new ConcurrentHashMap<>();

  /* Map of group name to last time compactor called to get a compaction job */
  private final Map<CompactorGroupId,Long> TIME_COMPACTOR_LAST_CHECKED = new ConcurrentHashMap<>();

  private final ServerContext ctx;
  private final SecurityOperation security;
  private final CompactionJobQueues jobQueues;
  private final AtomicReference<Map<FateInstanceType,Fate<Manager>>> fateInstances;
  // Exposed for tests
  protected final CountDownLatch shutdown = new CountDownLatch(1);

  private final ScheduledThreadPoolExecutor schedExecutor;

  private final Cache<ExternalCompactionId,RunningCompaction> completed;
  private final LoadingCache<FateId,CompactionConfig> compactionConfigCache;
  private final Cache<Path,Integer> tabletDirCache;
  private final DeadCompactionDetector deadCompactionDetector;

  private final QueueMetrics queueMetrics;
  private final Manager manager;

  private final LoadingCache<String,Integer> compactorCounts;
  private final int jobQueueInitialSize;

  private volatile long coordinatorStartTime;

  private final Map<DataLevel,ThreadPoolExecutor> reservationPools;
  private final Set<String> activeCompactorReservationRequest = ConcurrentHashMap.newKeySet();

  public CompactionCoordinator(ServerContext ctx, SecurityOperation security,
      AtomicReference<Map<FateInstanceType,Fate<Manager>>> fateInstances, Manager manager) {
    this.ctx = ctx;
    this.schedExecutor = this.ctx.getScheduledExecutor();
    this.security = security;
    this.manager = Objects.requireNonNull(manager);

    this.jobQueueInitialSize = ctx.getConfiguration()
        .getCount(Property.MANAGER_COMPACTION_SERVICE_PRIORITY_QUEUE_INITIAL_SIZE);

    this.jobQueues = new CompactionJobQueues(jobQueueInitialSize);

    this.queueMetrics = new QueueMetrics(jobQueues);

    this.fateInstances = fateInstances;

    completed = ctx.getCaches().createNewBuilder(CacheName.COMPACTIONS_COMPLETED, true)
        .maximumSize(200).expireAfterWrite(10, TimeUnit.MINUTES).build();

    CacheLoader<FateId,CompactionConfig> loader =
        fateId -> CompactionConfigStorage.getConfig(ctx, fateId);

    // Keep a small short lived cache of compaction config. Compaction config never changes, however
    // when a compaction is canceled it is deleted which is why there is a time limit. It does not
    // hurt to let a job that was canceled start, it will be canceled later. Caching this immutable
    // config will help avoid reading the same data over and over.
    compactionConfigCache = ctx.getCaches().createNewBuilder(CacheName.COMPACTION_CONFIGS, true)
        .expireAfterWrite(30, SECONDS).maximumSize(100).build(loader);

    Weigher<Path,Integer> weigher = (path, count) -> {
      return path.toUri().toString().length();
    };

    tabletDirCache = ctx.getCaches().createNewBuilder(CacheName.COMPACTION_DIR_CACHE, true)
        .maximumWeight(10485760L).weigher(weigher).build();

    deadCompactionDetector =
        new DeadCompactionDetector(this.ctx, this, schedExecutor, fateInstances);

    var rootReservationPool = ThreadPools.getServerThreadPools().createExecutorService(
        ctx.getConfiguration(), Property.COMPACTION_COORDINATOR_RESERVATION_THREADS_ROOT, true);

    var metaReservationPool = ThreadPools.getServerThreadPools().createExecutorService(
        ctx.getConfiguration(), Property.COMPACTION_COORDINATOR_RESERVATION_THREADS_META, true);

    var userReservationPool = ThreadPools.getServerThreadPools().createExecutorService(
        ctx.getConfiguration(), Property.COMPACTION_COORDINATOR_RESERVATION_THREADS_USER, true);

    reservationPools = Map.of(Ample.DataLevel.ROOT, rootReservationPool, Ample.DataLevel.METADATA,
        metaReservationPool, Ample.DataLevel.USER, userReservationPool);

    compactorCounts = ctx.getCaches().createNewBuilder(CacheName.COMPACTOR_COUNTS, false)
        .expireAfterWrite(30, TimeUnit.SECONDS).build(this::countCompactors);
    // At this point the manager does not have its lock so no actions should be taken yet
  }

  protected int countCompactors(String groupName) {
    return ExternalCompactionUtil.countCompactors(groupName, ctx);
  }

  private volatile Thread serviceThread = null;

  public void start() {
    serviceThread = Threads.createThread("CompactionCoordinator Thread", this);
    serviceThread.start();
  }

  public void shutdown() {
    shutdown.countDown();

    reservationPools.values().forEach(ExecutorService::shutdownNow);

    var localThread = serviceThread;
    if (localThread != null) {
      try {
        localThread.join();
      } catch (InterruptedException e) {
        LOG.error("Exception stopping compaction coordinator thread", e);
      }
    }
  }

  protected void startCompactorZKCleaner(ScheduledThreadPoolExecutor schedExecutor) {
    ScheduledFuture<?> future = schedExecutor
        .scheduleWithFixedDelay(this::cleanUpEmptyCompactorPathInZK, 0, 5, TimeUnit.MINUTES);
    ThreadPools.watchNonCriticalScheduledTask(future);
  }

  protected void startInternalStateCleaner(ScheduledThreadPoolExecutor schedExecutor) {
    ScheduledFuture<?> future =
        schedExecutor.scheduleWithFixedDelay(this::cleanUpInternalState, 0, 5, TimeUnit.MINUTES);
    ThreadPools.watchNonCriticalScheduledTask(future);
  }

  @Override
  public void run() {

    this.coordinatorStartTime = System.currentTimeMillis();
    startCompactorZKCleaner(schedExecutor);

    // On a re-start of the coordinator it's possible that external compactions are in-progress.
    // Attempt to get the running compactions on the compactors and then resolve which tserver
    // the external compaction came from to re-populate the RUNNING collection.
    LOG.info("Checking for running external compactions");
    // On re-start contact the running Compactors to try and seed the list of running compactions
    List<RunningCompaction> running = getCompactionsRunningOnCompactors();
    if (running.isEmpty()) {
      LOG.info("No running external compactions found");
    } else {
      LOG.info("Found {} running external compactions", running.size());
      running.forEach(rc -> {
        TCompactionStatusUpdate update = new TCompactionStatusUpdate();
        update.setState(TCompactionState.IN_PROGRESS);
        update.setMessage(RESTART_UPDATE_MSG);
        rc.addUpdate(System.currentTimeMillis(), update);
        rc.setStartTime(this.coordinatorStartTime);
        RUNNING_CACHE.put(ExternalCompactionId.of(rc.getJob().getExternalCompactionId()), rc);
        LONG_RUNNING_COMPACTIONS_BY_RG
            .computeIfAbsent(rc.getGroupName(), k -> new TimeOrderedRunningCompactionSet()).add(rc);
      });
    }

    startDeadCompactionDetector();
    startInternalStateCleaner(schedExecutor);

    try {
      shutdown.await();
    } catch (InterruptedException e) {
      LOG.warn("Interrupted waiting for shutdown latch.", e);
    }

    LOG.info("Shutting down");
  }

  private Map<String,Set<HostAndPort>> getIdleCompactors(Set<ServerId> runningCompactors) {

    final Map<String,Set<HostAndPort>> allCompactors = new HashMap<>();
    runningCompactors.forEach(
        (csi) -> allCompactors.computeIfAbsent(csi.getResourceGroup(), (k) -> new HashSet<>())
            .add(HostAndPort.fromParts(csi.getHost(), csi.getPort())));

    final Set<String> emptyQueues = new HashSet<>();

    // Remove all of the compactors that are running a compaction
    RUNNING_CACHE.values().forEach(rc -> {
      Set<HostAndPort> busyCompactors = allCompactors.get(rc.getGroupName());
      if (busyCompactors != null
          && busyCompactors.remove(HostAndPort.fromString(rc.getCompactorAddress()))) {
        if (busyCompactors.isEmpty()) {
          emptyQueues.add(rc.getGroupName());
        }
      }
    });
    // Remove entries with empty queues
    emptyQueues.forEach(e -> allCompactors.remove(e));
    return allCompactors;
  }

  protected void startDeadCompactionDetector() {
    deadCompactionDetector.start();
  }

  protected long getMissingCompactorWarningTime() {
    return this.ctx.getConfiguration().getTimeInMillis(Property.COMPACTOR_MAX_JOB_WAIT_TIME) * 3;
  }

  public long getNumRunningCompactions() {
    return RUNNING_CACHE.size();
  }

  /**
   * Return the next compaction job from the queue to a Compactor
   *
   * @param groupName group
   * @param compactorAddress compactor address
   * @throws ThriftSecurityException when permission error
   * @return compaction job
   */
  @Override
  public TNextCompactionJob getCompactionJob(TInfo tinfo, TCredentials credentials,
      String groupName, String compactorAddress, String externalCompactionId)
      throws ThriftSecurityException {

    // do not expect users to call this directly, expect compactors to call this method
    if (!security.canPerformSystemActions(credentials)) {
      throw new AccumuloSecurityException(credentials.getPrincipal(),
          SecurityErrorCode.PERMISSION_DENIED).asThriftException();
    }
    CompactorGroupId groupId = CompactorGroupId.of(groupName);
    LOG.trace("getCompactionJob called for group {} by compactor {}", groupId, compactorAddress);
    TIME_COMPACTOR_LAST_CHECKED.put(groupId, System.currentTimeMillis());

    TExternalCompactionJob result = null;

    CompactionJobQueues.MetaJob metaJob = jobQueues.poll(groupId);

    while (metaJob != null) {

      Optional<CompactionConfig> compactionConfig = getCompactionConfig(metaJob);

      // this method may reread the metadata, do not use the metadata in metaJob for anything after
      // this method
      CompactionMetadata ecm = null;

      var kind = metaJob.getJob().getKind();

      // Only reserve user compactions when the config is present. When compactions are canceled the
      // config is deleted.
      var cid = ExternalCompactionId.from(externalCompactionId);
      if (kind == CompactionKind.SYSTEM
          || (kind == CompactionKind.USER && compactionConfig.isPresent())) {
        ecm = reserveCompaction(metaJob, compactorAddress, cid);
      }

      if (ecm != null) {
        result = createThriftJob(externalCompactionId, ecm, metaJob, compactionConfig);
        // It is possible that by the time this added that the the compactor that made this request
        // is dead. In this cases the compaction is not actually running.
        RUNNING_CACHE.put(ExternalCompactionId.of(result.getExternalCompactionId()),
            new RunningCompaction(result, compactorAddress, groupName));
        TabletLogger.compacting(metaJob.getTabletMetadata(), cid, compactorAddress,
            metaJob.getJob());
        break;
      } else {
        LOG.debug(
            "Unable to reserve compaction job for {}, pulling another off the queue for group {}",
            metaJob.getTabletMetadata().getExtent(), groupName);
        metaJob = jobQueues.poll(CompactorGroupId.of(groupName));
      }
    }

    if (metaJob == null) {
      LOG.trace("No jobs found in group {} ", groupName);
    }

    if (result == null) {
      LOG.trace("No jobs found for group {}, returning empty job to compactor {}", groupName,
          compactorAddress);
      result = new TExternalCompactionJob();
    }

    return new TNextCompactionJob(result, compactorCounts.get(groupName));
  }

  @VisibleForTesting
  public static boolean canReserveCompaction(TabletMetadata tablet, CompactionKind kind,
      Set<StoredTabletFile> jobFiles, ServerContext ctx, SteadyTime steadyTime) {

    if (tablet == null) {
      // the tablet no longer exist
      return false;
    }

    if (tablet.getOperationId() != null) {
      return false;
    }

    if (ctx.getTableState(tablet.getTableId()) != TableState.ONLINE) {
      return false;
    }

    if (!tablet.getFiles().containsAll(jobFiles)) {
      return false;
    }

    var currentlyCompactingFiles = tablet.getExternalCompactions().values().stream()
        .flatMap(ecm -> ecm.getJobFiles().stream()).collect(Collectors.toSet());

    if (!Collections.disjoint(jobFiles, currentlyCompactingFiles)) {
      return false;
    }

    switch (kind) {
      case SYSTEM:
        var userRequestedCompactions = tablet.getUserCompactionsRequested().size();
        if (userRequestedCompactions > 0) {
          LOG.debug(
              "Unable to reserve {} for system compaction, tablet has {} pending requested user compactions",
              tablet.getExtent(), userRequestedCompactions);
          return false;
        } else if (!Collections.disjoint(jobFiles,
            getFilesReservedBySelection(tablet, steadyTime, ctx))) {
          return false;
        }
        break;
      case USER:
        if (tablet.getSelectedFiles() == null
            || !tablet.getSelectedFiles().getFiles().containsAll(jobFiles)) {
          return false;
        }
        break;
      default:
        throw new UnsupportedOperationException("Not currently handling " + kind);
    }

    return true;
  }

  private void checkTabletDir(KeyExtent extent, Path path) {
    try {
      if (tabletDirCache.getIfPresent(path) == null) {
        FileStatus[] files = null;
        try {
          files = ctx.getVolumeManager().listStatus(path);
        } catch (FileNotFoundException ex) {
          // ignored
        }

        if (files == null) {
          LOG.debug("Tablet {} had no dir, creating {}", extent, path);

          ctx.getVolumeManager().mkdirs(path);
        }
        tabletDirCache.put(path, 1);
      }
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  protected CompactionMetadata createExternalCompactionMetadata(CompactionJob job,
      Set<StoredTabletFile> jobFiles, TabletMetadata tablet, String compactorAddress,
      ExternalCompactionId externalCompactionId) {
    boolean propDels;

    FateId fateId = null;

    switch (job.getKind()) {
      case SYSTEM: {
        boolean compactingAll = tablet.getFiles().equals(jobFiles);
        propDels = !compactingAll;
      }
        break;
      case USER: {
        boolean compactingAll = tablet.getSelectedFiles().initiallySelectedAll()
            && tablet.getSelectedFiles().getFiles().equals(jobFiles);
        propDels = !compactingAll;
        fateId = tablet.getSelectedFiles().getFateId();
      }
        break;
      default:
        throw new IllegalArgumentException();
    }

    Consumer<String> directoryCreator = dir -> checkTabletDir(tablet.getExtent(), new Path(dir));
    ReferencedTabletFile newFile = TabletNameGenerator.getNextDataFilenameForMajc(propDels, ctx,
        tablet, directoryCreator, externalCompactionId);

    return new CompactionMetadata(jobFiles, newFile, compactorAddress, job.getKind(),
        job.getPriority(), job.getGroup(), propDels, fateId);

  }

  private class ReserveCompactionTask implements Supplier<CompactionMetadata> {

    // Use a soft reference for this in case free memory gets low while this is sitting in the queue
    // waiting to process. This object can contain the tablets list of files and if there are lots
    // of tablet with lots of files then that could start to cause memory problems. This hack could
    // be removed if #5188 were implemented.
    private final SoftReference<CompactionJobQueues.MetaJob> metaJobRef;
    private final String compactorAddress;
    private final ExternalCompactionId externalCompactionId;

    private ReserveCompactionTask(CompactionJobQueues.MetaJob metaJob, String compactorAddress,
        ExternalCompactionId externalCompactionId) {
      Preconditions.checkArgument(metaJob.getJob().getKind() == CompactionKind.SYSTEM
          || metaJob.getJob().getKind() == CompactionKind.USER);
      this.metaJobRef = new SoftReference<>(Objects.requireNonNull(metaJob));
      this.compactorAddress = Objects.requireNonNull(compactorAddress);
      this.externalCompactionId = Objects.requireNonNull(externalCompactionId);
      Preconditions.checkState(activeCompactorReservationRequest.add(compactorAddress),
          "compactor %s already on has a reservation in flight, cannot process %s",
          compactorAddress, externalCompactionId);
    }

    @Override
    public CompactionMetadata get() {
      try {
        var metaJob = metaJobRef.get();
        if (metaJob == null) {
          LOG.warn("Compaction reservation request for {} {} was garbage collected.",
              compactorAddress, externalCompactionId);
          return null;
        }

        var tabletMetadata = metaJob.getTabletMetadata();

        var jobFiles = metaJob.getJob().getFiles().stream()
            .map(CompactableFileImpl::toStoredTabletFile).collect(Collectors.toSet());

        Retry retry = Retry.builder().maxRetries(5).retryAfter(Duration.ofMillis(100))
            .incrementBy(Duration.ofMillis(100)).maxWait(Duration.ofSeconds(10)).backOffFactor(1.5)
            .logInterval(Duration.ofMinutes(3)).createRetry();

        while (retry.canRetry()) {
          try (var tabletsMutator = ctx.getAmple().conditionallyMutateTablets()) {
            var extent = metaJob.getTabletMetadata().getExtent();

            if (!canReserveCompaction(tabletMetadata, metaJob.getJob().getKind(), jobFiles, ctx,
                manager.getSteadyTime())) {
              return null;
            }

            var ecm = createExternalCompactionMetadata(metaJob.getJob(), jobFiles, tabletMetadata,
                compactorAddress, externalCompactionId);

            // any data that is read from the tablet to make a decision about if it can compact or
            // not
            // must be checked for changes in the conditional mutation.
            var tabletMutator = tabletsMutator.mutateTablet(extent).requireAbsentOperation()
                .requireFiles(jobFiles).requireNotCompacting(jobFiles);
            if (metaJob.getJob().getKind() == CompactionKind.SYSTEM) {
              // For system compactions the user compaction requested column is examined when
              // deciding
              // if a compaction can start so need to check for changes to this column.
              tabletMutator.requireSame(tabletMetadata, SELECTED, USER_COMPACTION_REQUESTED);
            } else {
              tabletMutator.requireSame(tabletMetadata, SELECTED);
            }

            if (metaJob.getJob().getKind() == CompactionKind.SYSTEM) {
              var selectedFiles = tabletMetadata.getSelectedFiles();
              var reserved =
                  getFilesReservedBySelection(tabletMetadata, manager.getSteadyTime(), ctx);

              // If there is a selectedFiles column, and the reserved set is empty this means that
              // either no user jobs were completed yet or the selection expiration time has passed
              // so the column is eligible to be deleted so a system job can run instead
              if (selectedFiles != null && reserved.isEmpty()
                  && !Collections.disjoint(jobFiles, selectedFiles.getFiles())) {
                LOG.debug("Deleting user compaction selected files for {} {}", extent,
                    externalCompactionId);
                tabletMutator.deleteSelectedFiles();
              }
            }

            tabletMutator.putExternalCompaction(externalCompactionId, ecm);
            tabletMutator
                .submit(tm -> tm.getExternalCompactions().containsKey(externalCompactionId));

            var result = tabletsMutator.process().get(extent);

            if (result.getStatus() == Ample.ConditionalResult.Status.ACCEPTED) {
              return ecm;
            } else {
              tabletMetadata = result.readMetadata();
            }
          }

          retry.useRetry();
          try {
            retry.waitForNextAttempt(LOG,
                "Reserved compaction for " + metaJob.getTabletMetadata().getExtent());
          } catch (InterruptedException e) {
            throw new RuntimeException(e);
          }
        }

        return null;
      } finally {
        Preconditions.checkState(activeCompactorReservationRequest.remove(compactorAddress),
            "compactorAddress:%s", compactorAddress);
      }
    }
  }

  protected CompactionMetadata reserveCompaction(CompactionJobQueues.MetaJob metaJob,
      String compactorAddress, ExternalCompactionId externalCompactionId) {

    if (activeCompactorReservationRequest.contains(compactorAddress)) {
      // In this case the compactor has a previously submitted reservation request that is still
      // processing. Do not want to let it queue up another reservation request. One possible cause
      // of this is that compactor timed out waiting for its last request to process and is now
      // making another request. The previously submitted request can not be used because the
      // compactor generates a new uuid for each request it makes. So the best thing to do is to
      // return null and wait for this situation to resolve. This will likely happen when some part
      // of the distributed system is not working well, so at this point want to avoid making
      // problems worse instead of trying to reserve a job.
      LOG.warn(
          "Ignoring request from {} to reserve compaction job because it has a reservation request in progress.",
          compactorAddress);
      return null;
    }

    var dataLevel = DataLevel.of(metaJob.getTabletMetadata().getTableId());
    var future = CompletableFuture.supplyAsync(
        new ReserveCompactionTask(metaJob, compactorAddress, externalCompactionId),
        reservationPools.get(dataLevel));
    return future.join();
  }

  protected TExternalCompactionJob createThriftJob(String externalCompactionId,
      CompactionMetadata ecm, CompactionJobQueues.MetaJob metaJob,
      Optional<CompactionConfig> compactionConfig) {

    Set<CompactableFile> selectedFiles;
    if (metaJob.getJob().getKind() == CompactionKind.SYSTEM) {
      selectedFiles = Set.of();
    } else {
      selectedFiles = metaJob.getTabletMetadata().getSelectedFiles().getFiles().stream()
          .map(file -> new CompactableFileImpl(file,
              metaJob.getTabletMetadata().getFilesMap().get(file)))
          .collect(Collectors.toUnmodifiableSet());
    }

    Map<String,String> overrides = CompactionPluginUtils.computeOverrides(compactionConfig, ctx,
        metaJob.getTabletMetadata().getExtent(), metaJob.getJob().getFiles(), selectedFiles);

    IteratorConfig iteratorSettings = SystemIteratorUtil
        .toIteratorConfig(compactionConfig.map(CompactionConfig::getIterators).orElse(List.of()));

    var files = ecm.getJobFiles().stream().map(storedTabletFile -> {
      var dfv = metaJob.getTabletMetadata().getFilesMap().get(storedTabletFile);
      return new InputFile(storedTabletFile.getMetadata(), dfv.getSize(), dfv.getNumEntries(),
          dfv.getTime());
    }).collect(toList());

    // The fateId here corresponds to the Fate transaction that is driving a user initiated
    // compaction. A system initiated compaction has no Fate transaction driving it so its ok to set
    // it to null. If anything tries to use the id for a system compaction and triggers a NPE it's
    // probably a bug that needs to be fixed.
    FateId fateId = null;
    if (metaJob.getJob().getKind() == CompactionKind.USER) {
      fateId = metaJob.getTabletMetadata().getSelectedFiles().getFateId();
    }

    return new TExternalCompactionJob(externalCompactionId,
        metaJob.getTabletMetadata().getExtent().toThrift(), files, iteratorSettings,
        ecm.getCompactTmpName().getNormalizedPathStr(), ecm.getPropagateDeletes(),
        TCompactionKind.valueOf(ecm.getKind().name()), fateId == null ? null : fateId.toThrift(),
        overrides);
  }

  @Override
  public void registerMetrics(MeterRegistry registry) {
    queueMetrics.registerMetrics(registry);
  }

  public void addJobs(TabletMetadata tabletMetadata, Collection<CompactionJob> jobs) {
    jobQueues.add(tabletMetadata, jobs);
  }

  public CompactionCoordinatorService.Iface getThriftService() {
    return this;
  }

  private Optional<CompactionConfig> getCompactionConfig(CompactionJobQueues.MetaJob metaJob) {
    if (metaJob.getJob().getKind() == CompactionKind.USER
        && metaJob.getTabletMetadata().getSelectedFiles() != null) {
      var cconf =
          compactionConfigCache.get(metaJob.getTabletMetadata().getSelectedFiles().getFateId());
      return Optional.ofNullable(cconf);
    }
    return Optional.empty();
  }

  /**
   * Compactors calls this method when they have finished a compaction. This method does the
   * following.
   *
   * <ol>
   * <li>Reads the tablets metadata and determines if the compaction can commit. Its possible that
   * things changed while the compaction was running and it can no longer commit.</li>
   * <li>Commit the compaction using a conditional mutation. If the tablets files or location
   * changed since reading the tablets metadata, then conditional mutation will fail. When this
   * happens it will reread the metadata and go back to step 1 conceptually. When committing a
   * compaction the compacted files are removed and scan entries are added to the tablet in case the
   * files are in use, this prevents GC from deleting the files between updating tablet metadata and
   * refreshing the tablet. The scan entries are only added when a tablet has a location.</li>
   * <li>After successful commit a refresh request is sent to the tablet if it has a location. This
   * will cause the tablet to start using the newly compacted files for future scans. Also the
   * tablet can delete the scan entries if there are no active scans using them.</li>
   * </ol>
   *
   * <p>
   * User compactions will be refreshed as part of the fate operation. The user compaction fate
   * operation will see the compaction was committed after this code updates the tablet metadata,
   * however if it were to rely on this code to do the refresh it would not be able to know when the
   * refresh was actually done. Therefore, user compactions will refresh as part of the fate
   * operation so that it's known to be done before the fate operation returns. Since the fate
   * operation will do it, there is no need to do it here for user compactions.
   *
   * @param tinfo trace info
   * @param credentials tcredentials object
   * @param externalCompactionId compaction id
   * @param textent tablet extent
   * @param stats compaction stats
   * @throws ThriftSecurityException when permission error
   */
  @Override
  public void compactionCompleted(TInfo tinfo, TCredentials credentials,
      String externalCompactionId, TKeyExtent textent, TCompactionStats stats)
      throws ThriftSecurityException {
    // do not expect users to call this directly, expect other tservers to call this method
    if (!security.canPerformSystemActions(credentials)) {
      throw new AccumuloSecurityException(credentials.getPrincipal(),
          SecurityErrorCode.PERMISSION_DENIED).asThriftException();
    }

    // maybe fate has not started yet
    var localFates = fateInstances.get();
    while (localFates == null) {
      UtilWaitThread.sleep(100);
      if (shutdown.getCount() == 0) {
        return;
      }
      localFates = fateInstances.get();
    }

    var extent = KeyExtent.fromThrift(textent);
    var localFate = localFates.get(FateInstanceType.fromTableId(extent.tableId()));

    LOG.info("Compaction completed, id: {}, stats: {}, extent: {}", externalCompactionId, stats,
        extent);
    final var ecid = ExternalCompactionId.of(externalCompactionId);

    var tabletMeta =
        ctx.getAmple().readTablet(extent, ECOMP, SELECTED, LOCATION, FILES, COMPACTED, OPID);

    var tableState = manager.getContext().getTableState(extent.tableId());
    if (tableState != TableState.ONLINE) {
      // Its important this check is done after the compaction id is set in the metadata table to
      // avoid race conditions with the client code that waits for tables to go offline. That code
      // looks for compaction ids in the metadata table after setting the table state. When that
      // client code sees nothing for a tablet its important that nothing will changes the tablets
      // files after that point in time which this check ensure.
      LOG.debug("Not committing compaction {} for {} because of table state {}", ecid, extent,
          tableState);
      // cleanup metadata table and files related to the compaction
      compactionsFailed(Map.of(ecid, extent));
      return;
    }

    if (!CommitCompaction.canCommitCompaction(ecid, tabletMeta)) {
      return;
    }

    // Start a fate transaction to commit the compaction.
    CompactionMetadata ecm = tabletMeta.getExternalCompactions().get(ecid);
    var renameOp = new RenameCompactionFile(new CompactionCommitData(ecid, extent, ecm, stats));
    localFate.seedTransaction("COMMIT_COMPACTION", FateKey.forCompactionCommit(ecid), renameOp,
        true);
  }

  @Override
  public void compactionFailed(TInfo tinfo, TCredentials credentials, String externalCompactionId,
      TKeyExtent extent) throws ThriftSecurityException {
    // do not expect users to call this directly, expect other tservers to call this method
    if (!security.canPerformSystemActions(credentials)) {
      throw new AccumuloSecurityException(credentials.getPrincipal(),
          SecurityErrorCode.PERMISSION_DENIED).asThriftException();
    }
    KeyExtent fromThriftExtent = KeyExtent.fromThrift(extent);
    LOG.info("Compaction failed, id: {}, extent: {}", externalCompactionId, fromThriftExtent);
    final var ecid = ExternalCompactionId.of(externalCompactionId);
    compactionsFailed(Map.of(ecid, KeyExtent.fromThrift(extent)));
  }

  void compactionsFailed(Map<ExternalCompactionId,KeyExtent> compactions) {
    // Need to process each level by itself because the conditional tablet mutator does not support
    // mutating multiple data levels at the same time. Also the conditional tablet mutator does not
    // support submitting multiple mutations for a single tablet, so need to group by extent.

    Map<DataLevel,Map<KeyExtent,Set<ExternalCompactionId>>> groupedCompactions =
        new EnumMap<>(DataLevel.class);

    compactions.forEach((ecid, extent) -> {
      groupedCompactions.computeIfAbsent(DataLevel.of(extent.tableId()), dl -> new HashMap<>())
          .computeIfAbsent(extent, e -> new HashSet<>()).add(ecid);
    });

    groupedCompactions
        .forEach((dataLevel, levelCompactions) -> compactionFailedForLevel(levelCompactions));
  }

  void compactionFailedForLevel(Map<KeyExtent,Set<ExternalCompactionId>> compactions) {

    try (var tabletsMutator = ctx.getAmple().conditionallyMutateTablets()) {
      compactions.forEach((extent, ecids) -> {
        try {
          ctx.requireNotDeleted(extent.tableId());
          var mutator = tabletsMutator.mutateTablet(extent).requireAbsentOperation();
          ecids.forEach(mutator::requireCompaction);
          ecids.forEach(mutator::deleteExternalCompaction);
          mutator.submit(new RejectionHandler() {
            @Override
            public boolean callWhenTabletDoesNotExists() {
              return true;
            }

            @Override
            public boolean test(TabletMetadata tabletMetadata) {
              return tabletMetadata == null
                  || Collections.disjoint(tabletMetadata.getExternalCompactions().keySet(), ecids);
            }

          });
        } catch (TableDeletedException e) {
          LOG.warn("Table {} was deleted, unable to update metadata for compaction failure.",
              extent.tableId());
        }
      });

      final List<ExternalCompactionId> ecidsForTablet = new ArrayList<>();
      tabletsMutator.process().forEach((extent, result) -> {
        if (result.getStatus() != Ample.ConditionalResult.Status.ACCEPTED) {

          // this should try again later when the dead compaction detector runs, lets log it in case
          // its a persistent problem
          if (LOG.isDebugEnabled()) {
            LOG.debug("Unable to remove failed compaction {} {}", extent, compactions.get(extent));
          }
        } else {
          // compactionFailed is called from the Compactor when either a compaction fails or
          // is cancelled and it's called from the DeadCompactionDetector. This block is
          // entered when the conditional mutator above successfully deletes an ecid from
          // the tablet metadata. Remove compaction tmp files from the tablet directory
          // that have a corresponding ecid in the name.

          ecidsForTablet.clear();
          ecidsForTablet.addAll(compactions.get(extent));

          if (!ecidsForTablet.isEmpty()) {
            final TabletMetadata tm = ctx.getAmple().readTablet(extent, ColumnType.DIR);
            if (tm != null) {
              final Collection<Volume> vols = ctx.getVolumeManager().getVolumes();
              for (Volume vol : vols) {
                try {
                  final String volPath =
                      vol.getBasePath() + Constants.HDFS_TABLES_DIR + Path.SEPARATOR
                          + extent.tableId().canonical() + Path.SEPARATOR + tm.getDirName();
                  final FileSystem fs = vol.getFileSystem();
                  for (ExternalCompactionId ecid : ecidsForTablet) {
                    final String fileSuffix = "_tmp_" + ecid.canonical();
                    FileStatus[] files = null;
                    try {
                      files = fs.listStatus(new Path(volPath),
                          (path) -> path.getName().endsWith(fileSuffix));
                    } catch (FileNotFoundException e) {
                      LOG.trace("Failed to list tablet dir {}", volPath, e);
                    }
                    if (files != null) {
                      for (FileStatus file : files) {
                        if (!fs.delete(file.getPath(), false)) {
                          LOG.warn("Unable to delete ecid tmp file: {}: ", file.getPath());
                        } else {
                          LOG.debug("Deleted ecid tmp file: {}", file.getPath());
                        }
                      }
                    }
                  }
                } catch (IOException e) {
                  LOG.error("Exception deleting compaction tmp files for tablet: {}", extent, e);
                }
              }
            } else {
              // TabletMetadata does not exist for the extent. This could be due to a merge or
              // split operation. Use the utility to find tmp files at the table level
              deadCompactionDetector.addTableId(extent.tableId());
            }
          }
        }
      });
    }

    compactions.values().forEach(ecids -> ecids.forEach(this::recordCompletion));
  }

  /**
   * Compactor calls to update the status of the assigned compaction
   *
   * @param tinfo trace info
   * @param credentials tcredentials object
   * @param externalCompactionId compaction id
   * @param update compaction status update
   * @param timestamp timestamp of the message
   * @throws ThriftSecurityException when permission error
   */
  @Override
  public void updateCompactionStatus(TInfo tinfo, TCredentials credentials,
      String externalCompactionId, TCompactionStatusUpdate update, long timestamp)
      throws ThriftSecurityException {
    // do not expect users to call this directly, expect other tservers to call this method
    if (!security.canPerformSystemActions(credentials)) {
      throw new AccumuloSecurityException(credentials.getPrincipal(),
          SecurityErrorCode.PERMISSION_DENIED).asThriftException();
    }
    LOG.debug("Compaction status update, id: {}, timestamp: {}, update: {}", externalCompactionId,
        timestamp, update);
    final RunningCompaction rc = RUNNING_CACHE.get(ExternalCompactionId.of(externalCompactionId));
    if (null != rc) {
      rc.addUpdate(timestamp, update);
      switch (update.state) {
        case STARTED:
          LONG_RUNNING_COMPACTIONS_BY_RG
              .computeIfAbsent(rc.getGroupName(), k -> new TimeOrderedRunningCompactionSet())
              .add(rc);
          break;
        case CANCELLED:
        case FAILED:
        case SUCCEEDED:
          var compactionSet = LONG_RUNNING_COMPACTIONS_BY_RG.get(rc.getGroupName());
          if (compactionSet != null) {
            compactionSet.remove(rc);
          }
          break;
        case ASSIGNED:
        case IN_PROGRESS:
        default:
          // do nothing
          break;

      }
    }
  }

  public void recordCompletion(ExternalCompactionId ecid) {
    var rc = RUNNING_CACHE.remove(ecid);
    if (rc != null) {
      completed.put(ecid, rc);
      var compactionSet = LONG_RUNNING_COMPACTIONS_BY_RG.get(rc.getGroupName());
      if (compactionSet != null) {
        compactionSet.remove(rc);
      }
    }
  }

  protected Set<ExternalCompactionId> readExternalCompactionIds() {
    try (TabletsMetadata tabletsMetadata =
        this.ctx.getAmple().readTablets().forLevel(Ample.DataLevel.USER)
            .filter(new HasExternalCompactionsFilter()).fetch(ECOMP).build()) {
      return tabletsMetadata.stream().flatMap(tm -> tm.getExternalCompactions().keySet().stream())
          .collect(Collectors.toSet());
    }
  }

  /**
   * Return information about running compactions
   *
   * @param tinfo trace info
   * @param credentials tcredentials object
   * @return map of ECID to TExternalCompaction objects
   * @throws ThriftSecurityException permission error
   */
  @Override
  public TExternalCompactionMap getRunningCompactions(TInfo tinfo, TCredentials credentials)
      throws ThriftSecurityException {
    // do not expect users to call this directly, expect other tservers to call this method
    if (!security.canPerformSystemActions(credentials)) {
      throw new AccumuloSecurityException(credentials.getPrincipal(),
          SecurityErrorCode.PERMISSION_DENIED).asThriftException();
    }

    final TExternalCompactionMap result = new TExternalCompactionMap();
    RUNNING_CACHE.forEach((ecid, rc) -> {
      TExternalCompaction trc = new TExternalCompaction();
      trc.setGroupName(rc.getGroupName());
      trc.setCompactor(rc.getCompactorAddress());
      trc.setUpdates(rc.getUpdates());
      trc.setJob(rc.getJob());
      result.putToCompactions(ecid.canonical(), trc);
    });
    return result;
  }

  /**
   * Return top 50 longest running compactions for each resource group
   *
   * @param tinfo trace info
   * @param credentials tcredentials object
   * @return map of group name to list of up to 50 compactions in sorted order, oldest compaction
   *         first.
   * @throws ThriftSecurityException permission error
   */
  @Override
  public Map<String,TExternalCompactionList> getLongRunningCompactions(TInfo tinfo,
      TCredentials credentials) throws ThriftSecurityException {
    // do not expect users to call this directly, expect other tservers to call this method
    if (!security.canPerformSystemActions(credentials)) {
      throw new AccumuloSecurityException(credentials.getPrincipal(),
          SecurityErrorCode.PERMISSION_DENIED).asThriftException();
    }

    final Map<String,TExternalCompactionList> result = new HashMap<>();

    for (Entry<String,TimeOrderedRunningCompactionSet> e : LONG_RUNNING_COMPACTIONS_BY_RG
        .entrySet()) {
      final TExternalCompactionList compactions = new TExternalCompactionList();
      Iterator<RunningCompaction> iter = e.getValue().iterator();
      while (iter.hasNext()) {
        RunningCompaction rc = iter.next();
        TExternalCompaction trc = new TExternalCompaction();
        trc.setGroupName(rc.getGroupName());
        trc.setCompactor(rc.getCompactorAddress());
        trc.setUpdates(rc.getUpdates());
        trc.setJob(rc.getJob());
        compactions.addToCompactions(trc);
      }
      result.put(e.getKey(), compactions);
    }
    return result;
  }

  /**
   * Return information about recently completed compactions
   *
   * @param tinfo trace info
   * @param credentials tcredentials object
   * @return map of ECID to TExternalCompaction objects
   * @throws ThriftSecurityException permission error
   */
  @Override
  public TExternalCompactionMap getCompletedCompactions(TInfo tinfo, TCredentials credentials)
      throws ThriftSecurityException {
    // do not expect users to call this directly, expect other tservers to call this method
    if (!security.canPerformSystemActions(credentials)) {
      throw new AccumuloSecurityException(credentials.getPrincipal(),
          SecurityErrorCode.PERMISSION_DENIED).asThriftException();
    }
    final TExternalCompactionMap result = new TExternalCompactionMap();
    completed.asMap().forEach((ecid, rc) -> {
      TExternalCompaction trc = new TExternalCompaction();
      trc.setGroupName(rc.getGroupName());
      trc.setCompactor(rc.getCompactorAddress());
      trc.setJob(rc.getJob());
      trc.setUpdates(rc.getUpdates());
      result.putToCompactions(ecid.canonical(), trc);
    });
    return result;
  }

  @Override
  public void cancel(TInfo tinfo, TCredentials credentials, String externalCompactionId)
      throws TException {
    var runningCompaction = RUNNING_CACHE.get(ExternalCompactionId.of(externalCompactionId));
    var extent = KeyExtent.fromThrift(runningCompaction.getJob().getExtent());
    try {
      NamespaceId nsId = this.ctx.getNamespaceId(extent.tableId());
      if (!security.canCompact(credentials, extent.tableId(), nsId)) {
        throw new AccumuloSecurityException(credentials.getPrincipal(),
            SecurityErrorCode.PERMISSION_DENIED).asThriftException();
      }
    } catch (TableNotFoundException e) {
      throw new ThriftTableOperationException(extent.tableId().canonical(), null,
          TableOperation.COMPACT_CANCEL, TableOperationExceptionType.NOTFOUND, e.getMessage());
    }

    cancelCompactionOnCompactor(runningCompaction.getCompactorAddress(), externalCompactionId);
  }

  /* Method exists to be called from test */
  public CompactionJobQueues getJobQueues() {
    return jobQueues;
  }

  /* Method exists to be overridden in test to hide static method */
  protected List<RunningCompaction> getCompactionsRunningOnCompactors() {
    return ExternalCompactionUtil.getCompactionsRunningOnCompactors(this.ctx);
  }

  /* Method exists to be overridden in test to hide static method */
  protected Set<ServerId> getRunningCompactors() {
    return ctx.instanceOperations().getServers(ServerId.Type.COMPACTOR);
  }

  /* Method exists to be overridden in test to hide static method */
  protected void cancelCompactionOnCompactor(String address, String externalCompactionId) {
    HostAndPort hostPort = HostAndPort.fromString(address);
    ExternalCompactionUtil.cancelCompaction(this.ctx, hostPort, externalCompactionId);
  }

  private void deleteEmpty(ZooReaderWriter zoorw, String path)
      throws KeeperException, InterruptedException {
    try {
      LOG.debug("Deleting empty ZK node {}", path);
      zoorw.delete(path);
    } catch (KeeperException.NotEmptyException e) {
      LOG.debug("Failed to delete {} its not empty, likely an expected race condition.", path);
    }
  }

  private void cleanUpEmptyCompactorPathInZK() {
    final String compactorQueuesPath = this.ctx.getZooKeeperRoot() + Constants.ZCOMPACTORS;

    final var zoorw = this.ctx.getZooReaderWriter();
    final double queueSizeFactor = ctx.getConfiguration()
        .getFraction(Property.MANAGER_COMPACTION_SERVICE_PRIORITY_QUEUE_SIZE_FACTOR);

    try {
      var groups = zoorw.getChildren(compactorQueuesPath);

      for (String group : groups) {
        final String qpath = compactorQueuesPath + "/" + group;
        final CompactorGroupId cgid = CompactorGroupId.of(group);
        final var compactors = zoorw.getChildren(qpath);

        if (compactors.isEmpty()) {
          deleteEmpty(zoorw, qpath);
          // Group has no compactors, we can clear its
          // associated priority queue of jobs
          CompactionJobPriorityQueue queue = getJobQueues().getQueue(cgid);
          if (queue != null) {
            queue.clearIfInactive(Duration.ofMinutes(10));
            queue.setMaxSize(this.jobQueueInitialSize);
          }
        } else {
          int aliveCompactorsForGroup = 0;
          for (String compactor : compactors) {
            String cpath = compactorQueuesPath + "/" + group + "/" + compactor;
            var lockNodes = zoorw.getChildren(compactorQueuesPath + "/" + group + "/" + compactor);
            if (lockNodes.isEmpty()) {
              deleteEmpty(zoorw, cpath);
            } else {
              aliveCompactorsForGroup++;
            }
          }
          CompactionJobPriorityQueue queue = getJobQueues().getQueue(cgid);
          if (queue != null) {
            queue.setMaxSize(Math.min(
                Math.max(1, (int) (aliveCompactorsForGroup * queueSizeFactor)), Integer.MAX_VALUE));
          }

        }

      }

    } catch (KeeperException | RuntimeException e) {
      LOG.warn("Failed to clean up compactors", e);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException(e);
    }
  }

  private Set<CompactorGroupId> getCompactionServicesConfigurationGroups()
      throws ReflectiveOperationException, IllegalArgumentException, SecurityException {

    Set<CompactorGroupId> groups = new HashSet<>();
    AccumuloConfiguration config = ctx.getConfiguration();
    CompactionServicesConfig servicesConfig = new CompactionServicesConfig(config);

    for (var entry : servicesConfig.getPlanners().entrySet()) {
      String serviceId = entry.getKey();
      String plannerClassName = entry.getValue();

      Class<? extends CompactionPlanner> plannerClass =
          Class.forName(plannerClassName).asSubclass(CompactionPlanner.class);
      CompactionPlanner planner = plannerClass.getDeclaredConstructor().newInstance();

      var initParams = new CompactionPlannerInitParams(CompactionServiceId.of(serviceId),
          servicesConfig.getPlannerPrefix(serviceId), servicesConfig.getOptions().get(serviceId),
          new ServiceEnvironmentImpl(ctx));

      planner.init(initParams);

      groups.addAll(initParams.getRequestedGroups());
    }
    return groups;
  }

  public void cleanUpInternalState() {

    // This method does the following:
    //
    // 1. Removes entries from RUNNING_CACHE and LONG_RUNNING_COMPACTIONS_BY_RG that are not really
    // running
    // 2. Cancels running compactions for groups that are not in the current configuration
    // 3. Remove groups not in configuration from TIME_COMPACTOR_LAST_CHECKED
    // 4. Log groups with no compactors
    // 5. Log compactors with no groups
    // 6. Log groups with compactors and queued jos that have not checked in

    var config = ctx.getConfiguration();
    ThreadPools.resizePool(reservationPools.get(DataLevel.ROOT), config,
        Property.COMPACTION_COORDINATOR_RESERVATION_THREADS_ROOT);
    ThreadPools.resizePool(reservationPools.get(DataLevel.METADATA), config,
        Property.COMPACTION_COORDINATOR_RESERVATION_THREADS_META);
    ThreadPools.resizePool(reservationPools.get(DataLevel.USER), config,
        Property.COMPACTION_COORDINATOR_RESERVATION_THREADS_USER);

    // grab a snapshot of the ids in the set before reading the metadata table. This is done to
    // avoid removing things that are added while reading the metadata.
    final Set<ExternalCompactionId> idsSnapshot = Set.copyOf(Sets.union(RUNNING_CACHE.keySet(),
        LONG_RUNNING_COMPACTIONS_BY_RG.values().stream()
            .flatMap(TimeOrderedRunningCompactionSet::stream)
            .map(rc -> rc.getJob().getExternalCompactionId()).map(ExternalCompactionId::of)
            .collect(Collectors.toSet())));

    // grab the ids that are listed as running in the metadata table. It important that this is done
    // after getting the snapshot.
    final Set<ExternalCompactionId> idsInMetadata = readExternalCompactionIds();

    final Set<ExternalCompactionId> idsToRemove = Sets.difference(idsSnapshot, idsInMetadata);

    // remove ids that are in the running set but not in the metadata table
    idsToRemove.forEach(this::recordCompletion);
    if (idsToRemove.size() > 0) {
      LOG.debug("Removed stale entries from RUNNING_CACHE : {}", idsToRemove);
    }

    // Get the set of groups being referenced in the current configuration
    Set<CompactorGroupId> groupsInConfiguration = null;
    try {
      groupsInConfiguration = getCompactionServicesConfigurationGroups();
    } catch (RuntimeException | ReflectiveOperationException e) {
      LOG.error(
          "Error getting groups from the compaction services configuration. Unable to clean up internal state.",
          e);
      return;
    }

    // Compaction jobs are created in the TabletGroupWatcher and added to the Coordinator
    // via the addJobs method which adds the job to the CompactionJobQueues object.
    final Set<CompactorGroupId> groupsWithJobs = jobQueues.getQueueIds();

    final Set<CompactorGroupId> jobGroupsNotInConfiguration =
        Sets.difference(groupsWithJobs, groupsInConfiguration);

    if (jobGroupsNotInConfiguration != null && !jobGroupsNotInConfiguration.isEmpty()) {
      RUNNING_CACHE.values().forEach(rc -> {
        if (jobGroupsNotInConfiguration.contains(CompactorGroupId.of(rc.getGroupName()))) {
          LOG.warn(
              "External compaction {} running in group {} on compactor {},"
                  + " but group not found in current configuration. Failing compaction...",
              rc.getJob().getExternalCompactionId(), rc.getGroupName(), rc.getCompactorAddress());
          cancelCompactionOnCompactor(rc.getCompactorAddress(),
              rc.getJob().getExternalCompactionId());
        }
      });

      final Set<CompactorGroupId> trackedGroups = Set.copyOf(TIME_COMPACTOR_LAST_CHECKED.keySet());
      TIME_COMPACTOR_LAST_CHECKED.keySet().retainAll(groupsInConfiguration);
      LOG.debug("No longer tracking compactor check-in times for groups: {}",
          Sets.difference(trackedGroups, TIME_COMPACTOR_LAST_CHECKED.keySet()));
    }

    final Set<ServerId> runningCompactors = getRunningCompactors();

    final Set<CompactorGroupId> runningCompactorGroups = new HashSet<>();
    runningCompactors
        .forEach(c -> runningCompactorGroups.add(CompactorGroupId.of(c.getResourceGroup())));

    final Set<CompactorGroupId> groupsWithNoCompactors =
        Sets.difference(groupsInConfiguration, runningCompactorGroups);
    if (groupsWithNoCompactors != null && !groupsWithNoCompactors.isEmpty()) {
      for (CompactorGroupId group : groupsWithNoCompactors) {
        long queuedJobCount = jobQueues.getQueuedJobs(group);
        if (queuedJobCount > 0) {
          LOG.warn("Compactor group {} has {} queued compactions but no running compactors", group,
              queuedJobCount);
        }
      }
    }

    final Set<CompactorGroupId> compactorsWithNoGroups =
        Sets.difference(runningCompactorGroups, groupsInConfiguration);
    if (compactorsWithNoGroups != null && !compactorsWithNoGroups.isEmpty()) {
      LOG.warn(
          "The following groups have running compactors, but are not in the current configuration: {}",
          compactorsWithNoGroups);
    }

    final long now = System.currentTimeMillis();
    final long warningTime = getMissingCompactorWarningTime();
    Map<String,Set<HostAndPort>> idleCompactors = getIdleCompactors(runningCompactors);
    for (CompactorGroupId groupName : groupsInConfiguration) {
      long lastCheckTime =
          TIME_COMPACTOR_LAST_CHECKED.getOrDefault(groupName, coordinatorStartTime);
      if ((now - lastCheckTime) > warningTime && jobQueues.getQueuedJobs(groupName) > 0
          && idleCompactors.containsKey(groupName.canonical())) {
        LOG.warn(
            "The group {} has queued jobs and {} idle compactors, however none have checked in "
                + "with coordinator for {}ms",
            groupName, idleCompactors.get(groupName.canonical()).size(), warningTime);
      }
    }
  }

  private static Set<StoredTabletFile> getFilesReservedBySelection(TabletMetadata tabletMetadata,
      SteadyTime steadyTime, ServerContext ctx) {
    if (tabletMetadata.getSelectedFiles() == null) {
      return Set.of();
    }

    if (tabletMetadata.getSelectedFiles().getCompletedJobs() > 0) {
      return tabletMetadata.getSelectedFiles().getFiles();
    }

    long selectedExpirationDuration = ctx.getTableConfiguration(tabletMetadata.getTableId())
        .getTimeInMillis(Property.TABLE_COMPACTION_SELECTION_EXPIRATION);

    if (steadyTime.minus(tabletMetadata.getSelectedFiles().getSelectedTime()).toMillis()
        < selectedExpirationDuration) {
      return tabletMetadata.getSelectedFiles().getFiles();
    }

    return Set.of();
  }
}

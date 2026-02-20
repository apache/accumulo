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
import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.PREV_ROW;
import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.SELECTED;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.UncheckedIOException;
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
import java.util.function.Consumer;
import java.util.function.Function;
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
import org.apache.accumulo.core.data.ResourceGroupId;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.dataImpl.thrift.TKeyExtent;
import org.apache.accumulo.core.fate.Fate;
import org.apache.accumulo.core.fate.FateClient;
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
import org.apache.accumulo.core.metadata.schema.DataFileValue;
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
import org.apache.accumulo.core.tabletserver.thrift.InputFile;
import org.apache.accumulo.core.tabletserver.thrift.IteratorConfig;
import org.apache.accumulo.core.tabletserver.thrift.TCompactionKind;
import org.apache.accumulo.core.tabletserver.thrift.TCompactionStats;
import org.apache.accumulo.core.tabletserver.thrift.TExternalCompactionJob;
import org.apache.accumulo.core.util.cache.Caches.CacheName;
import org.apache.accumulo.core.util.compaction.CompactionPlannerInitParams;
import org.apache.accumulo.core.util.compaction.CompactionServicesConfig;
import org.apache.accumulo.core.util.compaction.ExternalCompactionUtil;
import org.apache.accumulo.core.util.compaction.RunningCompaction;
import org.apache.accumulo.core.util.threads.ThreadPools;
import org.apache.accumulo.core.util.threads.Threads;
import org.apache.accumulo.core.volume.Volume;
import org.apache.accumulo.manager.Manager;
import org.apache.accumulo.manager.compaction.coordinator.commit.CommitCompaction;
import org.apache.accumulo.manager.compaction.coordinator.commit.CompactionCommitData;
import org.apache.accumulo.manager.compaction.coordinator.commit.RenameCompactionFile;
import org.apache.accumulo.manager.compaction.queue.CompactionJobPriorityQueue;
import org.apache.accumulo.manager.compaction.queue.CompactionJobQueues;
import org.apache.accumulo.manager.compaction.queue.ResolvedCompactionJob;
import org.apache.accumulo.manager.tableOps.FateEnv;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.ServiceEnvironmentImpl;
import org.apache.accumulo.server.compaction.CompactionConfigStorage;
import org.apache.accumulo.server.compaction.CompactionPluginUtils;
import org.apache.accumulo.server.security.AuditedSecurityOperation;
import org.apache.accumulo.server.tablets.TabletNameGenerator;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.thrift.TException;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.event.Level;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.CacheLoader;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.github.benmanes.caffeine.cache.Weigher;
import com.google.common.base.Preconditions;
import com.google.common.base.Suppliers;
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

  static class FailureCounts {
    long failures;
    long successes;

    FailureCounts(long failures, long successes) {
      this.failures = failures;
      this.successes = successes;
    }

    static FailureCounts incrementFailure(Object key, FailureCounts counts) {
      if (counts == null) {
        return new FailureCounts(1, 0);
      }
      counts.failures++;
      return counts;
    }

    static FailureCounts incrementSuccess(Object key, FailureCounts counts) {
      if (counts == null) {
        return new FailureCounts(0, 1);
      }
      counts.successes++;
      return counts;
    }
  }

  private final ConcurrentHashMap<ResourceGroupId,FailureCounts> failingQueues =
      new ConcurrentHashMap<>();
  private final ConcurrentHashMap<String,FailureCounts> failingCompactors =
      new ConcurrentHashMap<>();
  private final ConcurrentHashMap<TableId,FailureCounts> failingTables = new ConcurrentHashMap<>();

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
  private final Map<ResourceGroupId,Long> TIME_COMPACTOR_LAST_CHECKED = new ConcurrentHashMap<>();

  private final ServerContext ctx;
  private final AuditedSecurityOperation security;
  private final CompactionJobQueues jobQueues;
  private final Function<FateInstanceType,FateClient<FateEnv>> fateClients;
  // Exposed for tests
  protected final CountDownLatch shutdown = new CountDownLatch(1);

  private final Cache<ExternalCompactionId,RunningCompaction> completed;
  private final LoadingCache<FateId,CompactionConfig> compactionConfigCache;
  private final Cache<Path,Integer> tabletDirCache;
  private final DeadCompactionDetector deadCompactionDetector;

  private final QueueMetrics queueMetrics;
  private final Manager manager;

  private final LoadingCache<ResourceGroupId,Integer> compactorCounts;

  private volatile long coordinatorStartTime;

  private final Map<DataLevel,ThreadPoolExecutor> reservationPools;
  private final Set<String> activeCompactorReservationRequest = ConcurrentHashMap.newKeySet();

  public CompactionCoordinator(Manager manager,
      Function<FateInstanceType,FateClient<FateEnv>> fateClients) {
    this.ctx = manager.getContext();
    this.security = ctx.getSecurityOperation();
    this.manager = Objects.requireNonNull(manager);

    long jobQueueMaxSize =
        ctx.getConfiguration().getAsBytes(Property.MANAGER_COMPACTION_SERVICE_PRIORITY_QUEUE_SIZE);

    this.jobQueues = new CompactionJobQueues(jobQueueMaxSize);

    this.queueMetrics = new QueueMetrics(jobQueues);

    this.fateClients = fateClients;

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
        new DeadCompactionDetector(this.ctx, this, ctx.getScheduledExecutor(), fateClients);

    var rootReservationPool = ThreadPools.getServerThreadPools().createExecutorService(
        ctx.getConfiguration(), Property.COMPACTION_COORDINATOR_RESERVATION_THREADS_ROOT, true);

    var metaReservationPool = ThreadPools.getServerThreadPools().createExecutorService(
        ctx.getConfiguration(), Property.COMPACTION_COORDINATOR_RESERVATION_THREADS_META, true);

    var userReservationPool = ThreadPools.getServerThreadPools().createExecutorService(
        ctx.getConfiguration(), Property.COMPACTION_COORDINATOR_RESERVATION_THREADS_USER, true);

    reservationPools = Map.of(Ample.DataLevel.ROOT, rootReservationPool, Ample.DataLevel.METADATA,
        metaReservationPool, Ample.DataLevel.USER, userReservationPool);

    compactorCounts = ctx.getCaches().createNewBuilder(CacheName.COMPACTOR_COUNTS, false)
        .expireAfterWrite(2, TimeUnit.MINUTES).build(this::countCompactors);
    // At this point the manager does not have its lock so no actions should be taken yet
  }

  protected int countCompactors(ResourceGroupId groupName) {
    return ExternalCompactionUtil.countCompactors(groupName, ctx);
  }

  private volatile Thread serviceThread = null;

  public void start() {
    serviceThread = Threads.createCriticalThread("CompactionCoordinator Thread", this);
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

  protected void startConfigMonitor(ScheduledThreadPoolExecutor schedExecutor) {
    ScheduledFuture<?> future =
        schedExecutor.scheduleWithFixedDelay(this::checkForConfigChanges, 0, 1, TimeUnit.MINUTES);
    ThreadPools.watchNonCriticalScheduledTask(future);
  }

  private void checkForConfigChanges() {
    long jobQueueMaxSize =
        ctx.getConfiguration().getAsBytes(Property.MANAGER_COMPACTION_SERVICE_PRIORITY_QUEUE_SIZE);
    jobQueues.resetMaxSize(jobQueueMaxSize);
  }

  @Override
  public void run() {

    this.coordinatorStartTime = System.currentTimeMillis();
    startConfigMonitor(ctx.getScheduledExecutor());
    startCompactorZKCleaner(ctx.getScheduledExecutor());

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
            .computeIfAbsent(rc.getGroup().canonical(), k -> new TimeOrderedRunningCompactionSet())
            .add(rc);
      });
    }

    startDeadCompactionDetector();
    startQueueRunningSummaryLogging();
    startFailureSummaryLogging();
    startInternalStateCleaner(ctx.getScheduledExecutor());

    try {
      shutdown.await();
    } catch (InterruptedException e) {
      LOG.warn("Interrupted waiting for shutdown latch.", e);
    }

    LOG.info("Shutting down");
  }

  private Map<String,Set<HostAndPort>> getIdleCompactors(Set<ServerId> runningCompactors) {

    final Map<String,Set<HostAndPort>> allCompactors = new HashMap<>();
    runningCompactors.forEach((csi) -> allCompactors
        .computeIfAbsent(csi.getResourceGroup().canonical(), (k) -> new HashSet<>())
        .add(HostAndPort.fromParts(csi.getHost(), csi.getPort())));

    final Set<String> emptyQueues = new HashSet<>();

    // Remove all of the compactors that are running a compaction
    RUNNING_CACHE.values().forEach(rc -> {
      Set<HostAndPort> busyCompactors = allCompactors.get(rc.getGroup().canonical());
      if (busyCompactors != null
          && busyCompactors.remove(HostAndPort.fromString(rc.getCompactorAddress()))) {
        if (busyCompactors.isEmpty()) {
          emptyQueues.add(rc.getGroup().canonical());
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
    ResourceGroupId groupId = ResourceGroupId.of(groupName);
    LOG.trace("getCompactionJob called for group {} by compactor {}", groupId, compactorAddress);
    TIME_COMPACTOR_LAST_CHECKED.put(groupId, System.currentTimeMillis());

    TExternalCompactionJob result = null;

    ResolvedCompactionJob rcJob = (ResolvedCompactionJob) jobQueues.poll(groupId);

    while (rcJob != null) {

      Optional<CompactionConfig> compactionConfig = getCompactionConfig(rcJob);

      // this method may reread the metadata, do not use the metadata in rcJob for anything after
      // this method
      CompactionMetadata ecm = null;

      var kind = rcJob.getKind();

      // Only reserve user compactions when the config is present. When compactions are canceled the
      // config is deleted.
      var cid = ExternalCompactionId.from(externalCompactionId);
      if (kind == CompactionKind.SYSTEM
          || (kind == CompactionKind.USER && compactionConfig.isPresent())) {
        ecm = reserveCompaction(rcJob, compactorAddress, cid);
      }

      if (ecm != null) {
        result = createThriftJob(externalCompactionId, ecm, rcJob, compactionConfig);
        // It is possible that by the time this added that the the compactor that made this request
        // is dead. In this cases the compaction is not actually running.
        RUNNING_CACHE.put(ExternalCompactionId.of(result.getExternalCompactionId()),
            new RunningCompaction(result, compactorAddress, groupId));
        TabletLogger.compacting(rcJob.getExtent(), rcJob.getSelectedFateId(), cid, compactorAddress,
            rcJob, ecm.getCompactTmpName());
        break;
      } else {
        LOG.debug(
            "Unable to reserve compaction job for {}, pulling another off the queue for group {}",
            rcJob.getExtent(), groupName);
        rcJob = (ResolvedCompactionJob) jobQueues.poll(ResourceGroupId.of(groupName));
      }
    }

    if (rcJob == null) {
      LOG.trace("No jobs found in group {} ", groupName);
    }

    if (result == null) {
      LOG.trace("No jobs found for group {}, returning empty job to compactor {}", groupName,
          compactorAddress);
      result = new TExternalCompactionJob();
    }

    return new TNextCompactionJob(result, compactorCounts.get(groupId));
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

  protected CompactionMetadata createExternalCompactionMetadata(ResolvedCompactionJob job,
      String compactorAddress, ExternalCompactionId externalCompactionId) {
    boolean propDels = !job.isCompactingAll();
    FateId fateId = job.getSelectedFateId();

    Consumer<String> directoryCreator = dir -> checkTabletDir(job.getExtent(), new Path(dir));
    ReferencedTabletFile newFile = TabletNameGenerator.getNextDataFilenameForMajc(propDels, ctx,
        job.getExtent(), job.getTabletDir(), directoryCreator, externalCompactionId);

    return new CompactionMetadata(job.getJobFiles(), newFile, compactorAddress, job.getKind(),
        job.getPriority(), job.getGroup(), propDels, fateId);

  }

  private class ReserveCompactionTask implements Supplier<CompactionMetadata> {
    private final ResolvedCompactionJob rcJob;
    private final String compactorAddress;
    private final ExternalCompactionId externalCompactionId;

    private ReserveCompactionTask(ResolvedCompactionJob rcJob, String compactorAddress,
        ExternalCompactionId externalCompactionId) {
      Preconditions.checkArgument(
          rcJob.getKind() == CompactionKind.SYSTEM || rcJob.getKind() == CompactionKind.USER);
      this.rcJob = Objects.requireNonNull(rcJob);
      this.compactorAddress = Objects.requireNonNull(compactorAddress);
      this.externalCompactionId = Objects.requireNonNull(externalCompactionId);
      Preconditions.checkState(activeCompactorReservationRequest.add(compactorAddress),
          "compactor %s already on has a reservation in flight, cannot process %s",
          compactorAddress, externalCompactionId);
    }

    @Override
    public CompactionMetadata get() {
      if (ctx.getTableState(rcJob.getExtent().tableId()) != TableState.ONLINE) {
        return null;
      }

      try {
        try (var tabletsMutator = ctx.getAmple().conditionallyMutateTablets()) {
          var extent = rcJob.getExtent();
          var jobFiles = rcJob.getJobFiles();
          long selectedExpirationDuration = ctx.getTableConfiguration(extent.tableId())
              .getTimeInMillis(Property.TABLE_COMPACTION_SELECTION_EXPIRATION);
          var reservationCheck = new CompactionReservationCheck(rcJob.getKind(), jobFiles,
              rcJob.getSelectedFateId(), rcJob.isOverlapsSelectedFiles(), manager.getSteadyTime(),
              selectedExpirationDuration);
          var tabletMutator = tabletsMutator.mutateTablet(extent).requireAbsentOperation()
              .requireCheckSuccess(reservationCheck);

          var ecm = createExternalCompactionMetadata(rcJob, compactorAddress, externalCompactionId);

          if (rcJob.isOverlapsSelectedFiles()) {
            // There is corresponding code in CompactionReservationCheck that ensures this delete is
            // safe to do.
            tabletMutator.deleteSelectedFiles();
          }
          tabletMutator.putExternalCompaction(externalCompactionId, ecm);

          tabletMutator.submit(tm -> tm.getExternalCompactions().containsKey(externalCompactionId),
              () -> "compaction reservation");

          var result = tabletsMutator.process().get(extent);

          if (result.getStatus() == Ample.ConditionalResult.Status.ACCEPTED) {
            return ecm;
          } else {
            return null;
          }
        }
      } finally {
        Preconditions.checkState(activeCompactorReservationRequest.remove(compactorAddress),
            "compactorAddress:%s", compactorAddress);
      }
    }
  }

  protected CompactionMetadata reserveCompaction(ResolvedCompactionJob rcJob,
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

    var dataLevel = DataLevel.of(rcJob.getExtent().tableId());
    var future = CompletableFuture.supplyAsync(
        new ReserveCompactionTask(rcJob, compactorAddress, externalCompactionId),
        reservationPools.get(dataLevel));
    return future.join();
  }

  protected TExternalCompactionJob createThriftJob(String externalCompactionId,
      CompactionMetadata ecm, ResolvedCompactionJob rcJob,
      Optional<CompactionConfig> compactionConfig) {

    // Only reach out to metadata table and get these if requested, usually not needed unless
    // plugiun requests it.
    Supplier<Set<CompactableFile>> selectedFiles = Suppliers.memoize(() -> {
      if (rcJob.getKind() == CompactionKind.SYSTEM) {
        return Set.of();
      } else {
        var tabletMetadata =
            ctx.getAmple().readTablet(rcJob.getExtent(), SELECTED, FILES, PREV_ROW);
        Preconditions.checkState(
            tabletMetadata.getSelectedFiles().getFateId().equals(rcJob.getSelectedFateId()));
        return tabletMetadata.getSelectedFiles().getFiles().stream()
            .map(file -> new CompactableFileImpl(file, tabletMetadata.getFilesMap().get(file)))
            .collect(Collectors.toUnmodifiableSet());
      }
    });

    Map<String,String> overrides = CompactionPluginUtils.computeOverrides(compactionConfig, ctx,
        rcJob.getExtent(), rcJob.getFiles(), selectedFiles, ecm.getCompactTmpName());

    IteratorConfig iteratorSettings = SystemIteratorUtil
        .toIteratorConfig(compactionConfig.map(CompactionConfig::getIterators).orElse(List.of()));

    var files = rcJob.getJobFilesMap().entrySet().stream().map(e -> {
      StoredTabletFile file = e.getKey();
      DataFileValue dfv = e.getValue();
      return new InputFile(file.getMetadata(), dfv.getSize(), dfv.getNumEntries(), dfv.getTime());
    }).collect(toList());

    // The fateId here corresponds to the Fate transaction that is driving a user initiated
    // compaction. A system initiated compaction has no Fate transaction driving it so its ok to set
    // it to null. If anything tries to use the id for a system compaction and triggers a NPE it's
    // probably a bug that needs to be fixed.
    FateId fateId = null;
    if (rcJob.getKind() == CompactionKind.USER) {
      fateId = rcJob.getSelectedFateId();
    }

    return new TExternalCompactionJob(externalCompactionId, rcJob.getExtent().toThrift(), files,
        iteratorSettings, ecm.getCompactTmpName().getNormalizedPathStr(), ecm.getPropagateDeletes(),
        TCompactionKind.valueOf(ecm.getKind().name()), fateId == null ? null : fateId.toThrift(),
        overrides);
  }

  @Override
  public void registerMetrics(MeterRegistry registry) {
    queueMetrics.registerMetrics(registry);
  }

  public void addJobs(TabletMetadata tabletMetadata, Collection<CompactionJob> jobs) {
    ArrayList<CompactionJob> resolvedJobs = new ArrayList<>(jobs.size());
    for (var job : jobs) {
      resolvedJobs.add(new ResolvedCompactionJob(job, tabletMetadata));
    }

    jobQueues.add(tabletMetadata.getExtent(), resolvedJobs);
  }

  public CompactionCoordinatorService.Iface getThriftService() {
    return this;
  }

  private Optional<CompactionConfig> getCompactionConfig(ResolvedCompactionJob rcJob) {
    if (rcJob.getKind() == CompactionKind.USER) {
      var cconf = compactionConfigCache.get(rcJob.getSelectedFateId());
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
    var extent = KeyExtent.fromThrift(textent);
    var fateType = FateInstanceType.fromTableId(extent.tableId());
    var localFate = fateClients.apply(fateType);

    LOG.info("Compaction completed, id: {}, stats: {}, extent: {}", externalCompactionId, stats,
        extent);
    final var ecid = ExternalCompactionId.of(externalCompactionId);
    captureSuccess(ecid, extent);
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
    localFate.seedTransaction(Fate.FateOperation.COMMIT_COMPACTION,
        FateKey.forCompactionCommit(ecid), renameOp, true);
  }

  @Override
  public void compactionFailed(TInfo tinfo, TCredentials credentials, String externalCompactionId,
      TKeyExtent extent, String exceptionMessage, TCompactionState failureState)
      throws ThriftSecurityException {
    // do not expect users to call this directly, expect other tservers to call this method
    if (!security.canPerformSystemActions(credentials)) {
      throw new AccumuloSecurityException(credentials.getPrincipal(),
          SecurityErrorCode.PERMISSION_DENIED).asThriftException();
    }
    if (failureState != TCompactionState.CANCELLED || failureState != TCompactionState.FAILED) {
      LOG.error("Unexpected failure state sent to compactionFailed: {}. This is likely a bug.",
          failureState);
    }
    KeyExtent fromThriftExtent = KeyExtent.fromThrift(extent);
    LOG.info("Compaction {}: id: {}, extent: {}, compactor exception:{}", failureState,
        externalCompactionId, fromThriftExtent, exceptionMessage);
    final var ecid = ExternalCompactionId.of(externalCompactionId);
    if (failureState == TCompactionState.FAILED) {
      captureFailure(ecid, fromThriftExtent);
    }
    compactionsFailed(Map.of(ecid, KeyExtent.fromThrift(extent)));
  }

  private void captureFailure(ExternalCompactionId ecid, KeyExtent extent) {
    var rc = RUNNING_CACHE.get(ecid);
    if (rc != null) {
      failingQueues.compute(rc.getGroup(), FailureCounts::incrementFailure);
      final String compactor = rc.getCompactorAddress();
      failingCompactors.compute(compactor, FailureCounts::incrementFailure);
    }
    failingTables.compute(extent.tableId(), FailureCounts::incrementFailure);
  }

  protected void startQueueRunningSummaryLogging() {
    CoordinatorSummaryLogger summaryLogger =
        new CoordinatorSummaryLogger(ctx, this.jobQueues, this.RUNNING_CACHE, compactorCounts);

    ScheduledFuture<?> future = ctx.getScheduledExecutor()
        .scheduleWithFixedDelay(summaryLogger::logSummary, 0, 1, TimeUnit.MINUTES);
    ThreadPools.watchNonCriticalScheduledTask(future);
  }

  protected void startFailureSummaryLogging() {
    ScheduledFuture<?> future =
        ctx.getScheduledExecutor().scheduleWithFixedDelay(this::printStats, 0, 5, TimeUnit.MINUTES);
    ThreadPools.watchNonCriticalScheduledTask(future);
  }

  private <T> void printStats(String logPrefix, ConcurrentHashMap<T,FailureCounts> failureCounts,
      boolean logSuccessAtTrace) {
    for (var key : failureCounts.keySet()) {
      failureCounts.compute(key, (k, counts) -> {
        if (counts != null) {
          Level level;
          if (counts.failures > 0) {
            level = Level.WARN;
          } else if (logSuccessAtTrace) {
            level = Level.TRACE;
          } else {
            level = Level.DEBUG;
          }

          LOG.atLevel(level).log("{} {} failures:{} successes:{} since last time this was logged ",
              logPrefix, k, counts.failures, counts.successes);
        }

        // clear the counts so they can start building up for the next logging if this key is ever
        // used again
        return null;
      });
    }
  }

  private void printStats() {
    // Remove down compactors from failing list
    Map<String,Set<HostAndPort>> allCompactors = ExternalCompactionUtil.getCompactorAddrs(ctx);
    Set<String> allCompactorAddrs = new HashSet<>();
    allCompactors.values().forEach(l -> l.forEach(c -> allCompactorAddrs.add(c.toString())));
    failingCompactors.keySet().retainAll(allCompactorAddrs);
    printStats("Queue", failingQueues, false);
    printStats("Table", failingTables, false);
    printStats("Compactor", failingCompactors, true);
  }

  private void captureSuccess(ExternalCompactionId ecid, KeyExtent extent) {
    var rc = RUNNING_CACHE.get(ecid);
    if (rc != null) {
      failingQueues.compute(rc.getGroup(), FailureCounts::incrementSuccess);
      final String compactor = rc.getCompactorAddress();
      failingCompactors.compute(compactor, FailureCounts::incrementSuccess);
    }
    failingTables.compute(extent.tableId(), FailureCounts::incrementSuccess);
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
          LONG_RUNNING_COMPACTIONS_BY_RG.computeIfAbsent(rc.getGroup().canonical(),
              k -> new TimeOrderedRunningCompactionSet()).add(rc);
          break;
        case CANCELLED:
        case FAILED:
        case SUCCEEDED:
          var compactionSet = LONG_RUNNING_COMPACTIONS_BY_RG.get(rc.getGroup().canonical());
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
      var compactionSet = LONG_RUNNING_COMPACTIONS_BY_RG.get(rc.getGroup().canonical());
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
      trc.setGroupName(rc.getGroup().canonical());
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
        trc.setGroupName(rc.getGroup().canonical());
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
      trc.setGroupName(rc.getGroup().canonical());
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

    final var zoorw = this.ctx.getZooSession().asReaderWriter();

    try {
      var groups = zoorw.getChildren(Constants.ZCOMPACTORS);

      for (String group : groups) {
        final String qpath = Constants.ZCOMPACTORS + "/" + group;
        final ResourceGroupId cgid = ResourceGroupId.of(group);
        final var compactors = zoorw.getChildren(qpath);

        if (compactors.isEmpty()) {
          deleteEmpty(zoorw, qpath);
          // Group has no compactors, we can clear its
          // associated priority queue of jobs
          CompactionJobPriorityQueue queue = getJobQueues().getQueue(cgid);
          if (queue != null) {
            queue.clearIfInactive(Duration.ofMinutes(10));
          }
        } else {
          for (String compactor : compactors) {
            String cpath = Constants.ZCOMPACTORS + "/" + group + "/" + compactor;
            var lockNodes =
                zoorw.getChildren(Constants.ZCOMPACTORS + "/" + group + "/" + compactor);
            if (lockNodes.isEmpty()) {
              deleteEmpty(zoorw, cpath);
            }
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

  private Set<ResourceGroupId> getCompactionServicesConfigurationGroups()
      throws ReflectiveOperationException, IllegalArgumentException, SecurityException {

    Set<ResourceGroupId> groups = new HashSet<>();
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
    LOG.trace("Current ECIDs in metadata: {}", idsInMetadata.size());
    LOG.trace("Current ECIDs in running cache: {}", idsSnapshot.size());

    final Set<ExternalCompactionId> idsToRemove = Sets.difference(idsSnapshot, idsInMetadata);

    // remove ids that are in the running set but not in the metadata table
    idsToRemove.forEach(this::recordCompletion);
    if (idsToRemove.size() > 0) {
      LOG.debug("Removed stale entries from RUNNING_CACHE : {}", idsToRemove);
    }

    // Get the set of groups being referenced in the current configuration
    Set<ResourceGroupId> groupsInConfiguration = null;
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
    final Set<ResourceGroupId> groupsWithJobs = jobQueues.getQueueIds();

    final Set<ResourceGroupId> jobGroupsNotInConfiguration =
        Sets.difference(groupsWithJobs, groupsInConfiguration);

    if (jobGroupsNotInConfiguration != null && !jobGroupsNotInConfiguration.isEmpty()) {
      RUNNING_CACHE.values().forEach(rc -> {
        if (jobGroupsNotInConfiguration.contains(ResourceGroupId.of(rc.getGroup().canonical()))) {
          LOG.warn(
              "External compaction {} running in group {} on compactor {},"
                  + " but group not found in current configuration. Failing compaction...",
              rc.getJob().getExternalCompactionId(), rc.getGroup(), rc.getCompactorAddress());
          cancelCompactionOnCompactor(rc.getCompactorAddress(),
              rc.getJob().getExternalCompactionId());
        }
      });

      final Set<ResourceGroupId> trackedGroups = Set.copyOf(TIME_COMPACTOR_LAST_CHECKED.keySet());
      TIME_COMPACTOR_LAST_CHECKED.keySet().retainAll(groupsInConfiguration);
      LOG.debug("No longer tracking compactor check-in times for groups: {}",
          Sets.difference(trackedGroups, TIME_COMPACTOR_LAST_CHECKED.keySet()));
    }

    final Set<ServerId> runningCompactors = getRunningCompactors();

    final Set<ResourceGroupId> runningCompactorGroups = new HashSet<>();
    runningCompactors.forEach(
        c -> runningCompactorGroups.add(ResourceGroupId.of(c.getResourceGroup().canonical())));

    final Set<ResourceGroupId> groupsWithNoCompactors =
        Sets.difference(groupsInConfiguration, runningCompactorGroups);
    if (groupsWithNoCompactors != null && !groupsWithNoCompactors.isEmpty()) {
      for (ResourceGroupId group : groupsWithNoCompactors) {
        long queuedJobCount = jobQueues.getQueuedJobs(group);
        if (queuedJobCount > 0) {
          LOG.warn("Compactor group {} has {} queued compactions but no running compactors", group,
              queuedJobCount);
        }
      }
    }

    final Set<ResourceGroupId> compactorsWithNoGroups =
        Sets.difference(runningCompactorGroups, groupsInConfiguration);
    if (compactorsWithNoGroups != null && !compactorsWithNoGroups.isEmpty()) {
      LOG.warn(
          "The following groups have running compactors, but are not in the current configuration: {}",
          compactorsWithNoGroups);
    }

    final long now = System.currentTimeMillis();
    final long warningTime = getMissingCompactorWarningTime();
    Map<String,Set<HostAndPort>> idleCompactors = getIdleCompactors(runningCompactors);
    for (ResourceGroupId groupName : groupsInConfiguration) {
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
}

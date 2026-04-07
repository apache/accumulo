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
import static java.util.stream.Collectors.toSet;
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
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.TableDeletedException;
import org.apache.accumulo.core.client.admin.CompactionConfig;
import org.apache.accumulo.core.client.admin.compaction.CompactableFile;
import org.apache.accumulo.core.clientImpl.thrift.SecurityErrorCode;
import org.apache.accumulo.core.clientImpl.thrift.TInfo;
import org.apache.accumulo.core.clientImpl.thrift.ThriftSecurityException;
import org.apache.accumulo.core.compaction.thrift.CompactionCoordinatorService;
import org.apache.accumulo.core.compaction.thrift.TCompactionState;
import org.apache.accumulo.core.compaction.thrift.TDequeuedCompactionJob;
import org.apache.accumulo.core.compaction.thrift.TNextCompactionJob;
import org.apache.accumulo.core.compaction.thrift.TResolvedCompactionJob;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.AbstractId;
import org.apache.accumulo.core.data.ResourceGroupId;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.dataImpl.thrift.TKeyExtent;
import org.apache.accumulo.core.fate.Fate;
import org.apache.accumulo.core.fate.FateClient;
import org.apache.accumulo.core.fate.FateId;
import org.apache.accumulo.core.fate.FateInstanceType;
import org.apache.accumulo.core.fate.FateKey;
import org.apache.accumulo.core.iteratorsImpl.system.SystemIteratorUtil;
import org.apache.accumulo.core.logging.ConditionalLogger.ConditionalLogAction;
import org.apache.accumulo.core.logging.TabletLogger;
import org.apache.accumulo.core.manager.state.tables.TableState;
import org.apache.accumulo.core.metadata.CompactableFileImpl;
import org.apache.accumulo.core.metadata.ReferencedTabletFile;
import org.apache.accumulo.core.metadata.schema.Ample;
import org.apache.accumulo.core.metadata.schema.Ample.DataLevel;
import org.apache.accumulo.core.metadata.schema.Ample.RejectionHandler;
import org.apache.accumulo.core.metadata.schema.CompactionMetadata;
import org.apache.accumulo.core.metadata.schema.ExternalCompactionId;
import org.apache.accumulo.core.metadata.schema.TabletMetadata;
import org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType;
import org.apache.accumulo.core.metrics.MetricsProducer;
import org.apache.accumulo.core.securityImpl.thrift.TCredentials;
import org.apache.accumulo.core.spi.compaction.CompactionJob;
import org.apache.accumulo.core.spi.compaction.CompactionKind;
import org.apache.accumulo.core.tabletserver.thrift.IteratorConfig;
import org.apache.accumulo.core.tabletserver.thrift.TCompactionKind;
import org.apache.accumulo.core.tabletserver.thrift.TCompactionStats;
import org.apache.accumulo.core.tabletserver.thrift.TExternalCompactionJob;
import org.apache.accumulo.core.util.cache.Caches.CacheName;
import org.apache.accumulo.core.util.compaction.ExternalCompactionUtil;
import org.apache.accumulo.core.util.threads.ThreadPools;
import org.apache.accumulo.core.util.threads.Threads;
import org.apache.accumulo.manager.Manager;
import org.apache.accumulo.manager.compaction.coordinator.commit.CommitCompaction;
import org.apache.accumulo.manager.compaction.coordinator.commit.CompactionCommitData;
import org.apache.accumulo.manager.compaction.coordinator.commit.RenameCompactionFile;
import org.apache.accumulo.manager.compaction.queue.CompactionJobQueues;
import org.apache.accumulo.manager.compaction.queue.ResolvedCompactionJob;
import org.apache.accumulo.manager.tableOps.FateEnv;
import org.apache.accumulo.manager.upgrade.UpgradeCheck;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.compaction.CompactionConfigStorage;
import org.apache.accumulo.server.compaction.CompactionPluginUtils;
import org.apache.accumulo.server.security.AuditedSecurityOperation;
import org.apache.accumulo.server.tablets.TabletNameGenerator;
import org.apache.accumulo.server.util.FindCompactionTmpFiles;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.CacheLoader;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.github.benmanes.caffeine.cache.Weigher;
import com.google.common.base.Preconditions;
import com.google.common.base.Suppliers;
import com.google.common.net.HostAndPort;

import io.micrometer.core.instrument.MeterRegistry;

public class CompactionCoordinator
    implements CompactionCoordinatorService.Iface, Runnable, MetricsProducer {

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

  private final ServerContext ctx;
  private final AuditedSecurityOperation security;
  private final CompactionJobQueues jobQueues;
  private final Function<FateInstanceType,FateClient<FateEnv>> fateClients;
  // Exposed for tests
  protected final CountDownLatch shutdown = new CountDownLatch(1);

  private final LoadingCache<FateId,CompactionConfig> compactionConfigCache;
  private final Cache<Path,Integer> tabletDirCache;

  private final QueueMetrics queueMetrics;
  private final Manager manager;

  private final LoadingCache<ResourceGroupId,Integer> compactorCounts;

  private final Map<DataLevel,ThreadPoolExecutor> reservationPools;
  private final Set<String> activeCompactorReservationRequest = ConcurrentHashMap.newKeySet();

  private final UpgradeCheck upgradeCheck;

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

    upgradeCheck = new UpgradeCheck(ctx);
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

  protected void startConfigMonitor(ScheduledThreadPoolExecutor schedExecutor) {
    ScheduledFuture<?> future =
        schedExecutor.scheduleWithFixedDelay(this::checkForConfigChanges, 0, 1, TimeUnit.MINUTES);
    ThreadPools.watchNonCriticalScheduledTask(future);
  }

  private void checkForConfigChanges() {
    long jobQueueMaxSize =
        ctx.getConfiguration().getAsBytes(Property.MANAGER_COMPACTION_SERVICE_PRIORITY_QUEUE_SIZE);
    jobQueues.resetMaxSize(jobQueueMaxSize);

    var config = ctx.getConfiguration();
    ThreadPools.resizePool(reservationPools.get(DataLevel.ROOT), config,
        Property.COMPACTION_COORDINATOR_RESERVATION_THREADS_ROOT);
    ThreadPools.resizePool(reservationPools.get(DataLevel.METADATA), config,
        Property.COMPACTION_COORDINATOR_RESERVATION_THREADS_META);
    ThreadPools.resizePool(reservationPools.get(DataLevel.USER), config,
        Property.COMPACTION_COORDINATOR_RESERVATION_THREADS_USER);

  }

  @Override
  public void run() {
    startConfigMonitor(ctx.getScheduledExecutor());

    startFailureSummaryLogging();

    try {
      shutdown.await();
    } catch (InterruptedException e) {
      LOG.warn("Interrupted waiting for shutdown latch.", e);
    }

    LOG.info("Shutting down");
  }

  /**
   * Return the next compaction job from the queue to a Compactor
   *
   * @param groupName group
   * @throws ThriftSecurityException when permission error
   * @return compaction job
   */
  @Override
  public TDequeuedCompactionJob getCompactionJob(TInfo tinfo, TCredentials credentials,
      String groupName) throws ThriftSecurityException, TException {
    // do not expect users to call this directly, expect compactors to call this method
    if (!security.canPerformSystemActions(credentials)) {
      throw new AccumuloSecurityException(credentials.getPrincipal(),
          SecurityErrorCode.PERMISSION_DENIED).asThriftException();
    }
    ResourceGroupId groupId = ResourceGroupId.of(groupName);
    ResolvedCompactionJob rcJob = (ResolvedCompactionJob) jobQueues.poll(groupId);
    return new TDequeuedCompactionJob(rcJob == null ? null : rcJob.toThrift(),
        compactorCounts.get(groupId));
  }

  @Override
  public TNextCompactionJob reserveCompactionJob(TInfo tinfo, TCredentials credentials,
      TResolvedCompactionJob job, String compactorAddress, String externalCompactionId)
      throws ThriftSecurityException, TException {

    // do not expect users to call this directly, expect compactors to call this method
    if (!security.canPerformSystemActions(credentials)) {
      throw new AccumuloSecurityException(credentials.getPrincipal(),
          SecurityErrorCode.PERMISSION_DENIED).asThriftException();
    }
    ResourceGroupId groupId = ResourceGroupId.of(job.group);
    LOG.trace("reserveCompactionJob called for group {} by compactor {}", groupId,
        compactorAddress);

    TExternalCompactionJob result = null;

    ResolvedCompactionJob rcJob = ResolvedCompactionJob.fromThrift(job);

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
      TabletLogger.compacting(rcJob.getExtent(), rcJob.getSelectedFateId(), cid, compactorAddress,
          rcJob, ecm.getCompactTmpName());
    }

    if (result == null) {
      LOG.trace("No jobs found for group {}, returning empty job to compactor {}", groupId,
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

    var files = rcJob.getThriftFiles();

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

  @Override
  public void beginFullJobScan(TInfo tinfo, TCredentials credentials, String dataLevel)
      throws TException {
    if (!security.canPerformSystemActions(credentials)) {
      throw new AccumuloSecurityException(credentials.getPrincipal(),
          SecurityErrorCode.PERMISSION_DENIED).asThriftException();
    }
    jobQueues.beginFullScan(DataLevel.valueOf(dataLevel));
  }

  @Override
  public void addJobs(TInfo tinfo, TCredentials credentials, List<TResolvedCompactionJob> tjobs)
      throws TException {
    if (!security.canPerformSystemActions(credentials)) {
      LOG.warn("Thrift call attempted to add job and did not have proper access. {}",
          credentials.getPrincipal());
      return;
    }

    Map<KeyExtent,List<CompactionJob>> jobs = new HashMap<>();
    for (var tjob : tjobs) {
      var job = ResolvedCompactionJob.fromThrift(tjob);
      LOG.trace("Adding compaction job {} {} {} {} {}", job.getGroup(), job.getPriority(),
          job.getKind(), job.getExtent(), job.getJobFiles().size());
      jobs.computeIfAbsent(job.getExtent(), e -> new ArrayList<>()).add(job);
    }

    // its important to add all jobs for an extent at once instead of one by one because the job
    // queue deletes all existing jobs for an extent when adding an extent
    jobs.forEach(jobQueues::add);
  }

  @Override
  public void endFullJobScan(TInfo tinfo, TCredentials credentials, String dataLevel)
      throws TException {
    if (!security.canPerformSystemActions(credentials)) {
      throw new AccumuloSecurityException(credentials.getPrincipal(),
          SecurityErrorCode.PERMISSION_DENIED).asThriftException();
    }
    jobQueues.endFullScan(DataLevel.valueOf(dataLevel));
  }

  private static class UpdateId {
    long updateId;
    boolean set;
  }

  private final UpdateId expectedUpdateId = new UpdateId();

  @Override
  public Set<String> getResourceGroups(TInfo tinfo, TCredentials credentials, long updateId)
      throws ThriftSecurityException, TException {
    if (!security.canPerformSystemActions(credentials)) {
      throw new AccumuloSecurityException(credentials.getPrincipal(),
          SecurityErrorCode.PERMISSION_DENIED).asThriftException();
    }

    synchronized (expectedUpdateId) {
      // invalidate any outstanding updates and set a new one time use update id
      expectedUpdateId.updateId = updateId;
      expectedUpdateId.set = true;
      return jobQueues.getAllowedGroups().stream().map(AbstractId::canonical).collect(toSet());
    }
  }

  @Override
  public void setResourceGroups(TInfo tinfo, TCredentials credentials, long updateId,
      Set<String> groups) throws ThriftSecurityException, TException {
    if (!security.canPerformSystemActions(credentials)) {
      throw new AccumuloSecurityException(credentials.getPrincipal(),
          SecurityErrorCode.PERMISSION_DENIED).asThriftException();
    }

    // Do not expect this to be called before upgrade is complete
    Preconditions.checkState(upgradeCheck.isUpgradeComplete());

    // only allow an update id to be used once
    synchronized (expectedUpdateId) {
      if (expectedUpdateId.updateId == updateId && expectedUpdateId.set) {
        jobQueues.setAllowedGroups(groups.stream().map(ResourceGroupId::of).collect(toSet()));
        LOG.debug("Set allowed resource groups to {}", groups);
        expectedUpdateId.set = false;
      } else {
        LOG.debug("Did not set resource groups because update id did not match");
      }
    }
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
      String externalCompactionId, TKeyExtent textent, TCompactionStats stats, String groupName,
      String compactorAddress) throws ThriftSecurityException {
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
    captureSuccess(ResourceGroupId.of(groupName), compactorAddress, extent);
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
      compactionsFailed(ctx, Map.of(ecid, extent));
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
      TKeyExtent extent, String exceptionMessage, TCompactionState failureState, String groupName,
      String compactorAddress) throws ThriftSecurityException {
    // do not expect users to call this directly, expect other tservers to call this method
    if (!security.canPerformSystemActions(credentials)) {
      throw new AccumuloSecurityException(credentials.getPrincipal(),
          SecurityErrorCode.PERMISSION_DENIED).asThriftException();
    }
    if (failureState != TCompactionState.CANCELLED && failureState != TCompactionState.FAILED) {
      LOG.error("Unexpected failure state sent to compactionFailed: {}. This is likely a bug.",
          failureState);
    }
    KeyExtent fromThriftExtent = KeyExtent.fromThrift(extent);
    LOG.info("Compaction {}: id: {}, extent: {}, compactor exception:{}", failureState,
        externalCompactionId, fromThriftExtent, exceptionMessage);
    final var ecid = ExternalCompactionId.of(externalCompactionId);
    if (failureState == TCompactionState.FAILED) {
      captureFailure(ResourceGroupId.of(groupName), compactorAddress, fromThriftExtent);
    }
    compactionsFailed(ctx, Map.of(ecid, KeyExtent.fromThrift(extent)));
  }

  private void captureFailure(ResourceGroupId group, String compactorAddress, KeyExtent extent) {
    failingQueues.compute(group, FailureCounts::incrementFailure);
    failingCompactors.compute(compactorAddress, FailureCounts::incrementFailure);
    failingTables.compute(extent.tableId(), FailureCounts::incrementFailure);
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
          ConditionalLogAction logAction = Logger::debug;
          if (counts.failures > 0) {
            logAction = Logger::warn;
          } else if (logSuccessAtTrace) {
            logAction = Logger::trace;
          }

          logAction.log(LOG, "{} {} failures:{} successes:{} since last time this was logged ",
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

  private void captureSuccess(ResourceGroupId groupId, String compactorAddress, KeyExtent extent) {
    failingQueues.compute(groupId, FailureCounts::incrementSuccess);
    failingCompactors.compute(compactorAddress, FailureCounts::incrementSuccess);
    failingTables.compute(extent.tableId(), FailureCounts::incrementSuccess);
  }

  static void compactionsFailed(ServerContext ctx,
      Map<ExternalCompactionId,KeyExtent> compactions) {
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
        .forEach((dataLevel, levelCompactions) -> compactionFailedForLevel(ctx, levelCompactions));
  }

  static void compactionFailedForLevel(ServerContext ctx,
      Map<KeyExtent,Set<ExternalCompactionId>> compactions) {

    // CompactionFailed is called from the Compactor when either a compaction fails or is cancelled
    // and it's called from the DeadCompactionDetector. Remove compaction tmp files from the tablet
    // directory that have a corresponding ecid in the name. Must delete any tmp files before
    // removing compaction entry from metadata table. This ensures that in the event of process
    // death that the dead compaction will be detected in the future and the files removed then.
    try (var tablets = ctx.getAmple().readTablets()
        .forTablets(compactions.keySet(), Optional.empty()).fetch(ColumnType.DIR).build()) {
      for (TabletMetadata tm : tablets) {
        var extent = tm.getExtent();
        var ecidsForTablet = compactions.get(extent);
        FindCompactionTmpFiles.deleteTmpFiles(ctx, extent.tableId(), tm.getDirName(),
            ecidsForTablet);
      }
    }

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

      tabletsMutator.process().forEach((extent, result) -> {
        if (result.getStatus() != Ample.ConditionalResult.Status.ACCEPTED) {
          // this should try again later when the dead compaction detector runs, lets log it in case
          // its a persistent problem
          if (LOG.isDebugEnabled()) {
            LOG.debug("Unable to remove failed compaction {} {}", extent, compactions.get(extent));
          }
        }
      });
    }
  }

  /* Method exists to be called from test */
  public CompactionJobQueues getJobQueues() {
    return jobQueues;
  }
}

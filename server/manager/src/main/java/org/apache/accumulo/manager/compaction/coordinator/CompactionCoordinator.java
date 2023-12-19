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

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.COMPACTED;
import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.ECOMP;
import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.FILES;
import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.LOCATION;
import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.OPID;
import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.PREV_ROW;
import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.SCANS;
import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.SELECTED;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.TableDeletedException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.CompactionConfig;
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
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.NamespaceId;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.dataImpl.thrift.TKeyExtent;
import org.apache.accumulo.core.fate.FateTxId;
import org.apache.accumulo.core.fate.zookeeper.ZooReaderWriter;
import org.apache.accumulo.core.iteratorsImpl.system.SystemIteratorUtil;
import org.apache.accumulo.core.metadata.AbstractTabletFile;
import org.apache.accumulo.core.metadata.CompactableFileImpl;
import org.apache.accumulo.core.metadata.ReferencedTabletFile;
import org.apache.accumulo.core.metadata.StoredTabletFile;
import org.apache.accumulo.core.metadata.schema.Ample;
import org.apache.accumulo.core.metadata.schema.Ample.Refreshes.RefreshEntry;
import org.apache.accumulo.core.metadata.schema.Ample.RejectionHandler;
import org.apache.accumulo.core.metadata.schema.DataFileValue;
import org.apache.accumulo.core.metadata.schema.ExternalCompactionId;
import org.apache.accumulo.core.metadata.schema.ExternalCompactionMetadata;
import org.apache.accumulo.core.metadata.schema.SelectedFiles;
import org.apache.accumulo.core.metadata.schema.TabletMetadata;
import org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType;
import org.apache.accumulo.core.metrics.MetricsProducer;
import org.apache.accumulo.core.rpc.ThriftUtil;
import org.apache.accumulo.core.securityImpl.thrift.TCredentials;
import org.apache.accumulo.core.spi.compaction.CompactionJob;
import org.apache.accumulo.core.spi.compaction.CompactionKind;
import org.apache.accumulo.core.tabletserver.thrift.InputFile;
import org.apache.accumulo.core.tabletserver.thrift.IteratorConfig;
import org.apache.accumulo.core.tabletserver.thrift.TCompactionKind;
import org.apache.accumulo.core.tabletserver.thrift.TCompactionStats;
import org.apache.accumulo.core.tabletserver.thrift.TExternalCompactionJob;
import org.apache.accumulo.core.tabletserver.thrift.TabletServerClientService;
import org.apache.accumulo.core.util.Retry;
import org.apache.accumulo.core.util.UtilWaitThread;
import org.apache.accumulo.core.util.cache.Caches.CacheName;
import org.apache.accumulo.core.util.compaction.CompactionExecutorIdImpl;
import org.apache.accumulo.core.util.compaction.ExternalCompactionUtil;
import org.apache.accumulo.core.util.compaction.RunningCompaction;
import org.apache.accumulo.core.util.threads.ThreadPools;
import org.apache.accumulo.core.util.threads.Threads;
import org.apache.accumulo.core.volume.Volume;
import org.apache.accumulo.manager.EventCoordinator;
import org.apache.accumulo.manager.compaction.queue.CompactionJobQueues;
import org.apache.accumulo.manager.tableOps.bulkVer2.TabletRefresher;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.compaction.CompactionConfigStorage;
import org.apache.accumulo.server.compaction.CompactionPluginUtils;
import org.apache.accumulo.server.manager.LiveTServerSet;
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
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterators;
import com.google.common.collect.Sets;
import com.google.common.net.HostAndPort;
import com.google.common.util.concurrent.MoreExecutors;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;

public class CompactionCoordinator
    implements CompactionCoordinatorService.Iface, Runnable, MetricsProducer {

  private static final Logger LOG = LoggerFactory.getLogger(CompactionCoordinator.class);
  private static final long FIFTEEN_MINUTES = TimeUnit.MINUTES.toMillis(15);

  /*
   * Map of compactionId to RunningCompactions. This is an informational cache of what external
   * compactions may be running. Its possible it may contain external compactions that are not
   * actually running. It may not contain compactions that are actually running. The metadata table
   * is the most authoritative source of what external compactions are currently running, but it
   * does not have the stats that this map has.
   */
  protected static final Map<ExternalCompactionId,RunningCompaction> RUNNING_CACHE =
      new ConcurrentHashMap<>();

  /*
   * When the manager starts up any refreshes that were in progress when the last manager process
   * died must be completed before new refresh entries are written. This map of countdown latches
   * helps achieve that goal.
   */
  private final Map<Ample.DataLevel,CountDownLatch> refreshLatches;

  /* Map of group name to last time compactor called to get a compaction job */
  // ELASTICITY_TODO need to clean out groups that are no longer configured..
  private static final Map<String,Long> TIME_COMPACTOR_LAST_CHECKED = new ConcurrentHashMap<>();

  private final ServerContext ctx;
  private final LiveTServerSet tserverSet;
  private final SecurityOperation security;
  private final CompactionJobQueues jobQueues;
  private final EventCoordinator eventCoordinator;
  // Exposed for tests
  protected volatile Boolean shutdown = false;

  private final ScheduledThreadPoolExecutor schedExecutor;

  private final Cache<ExternalCompactionId,RunningCompaction> completed;
  private LoadingCache<Long,CompactionConfig> compactionConfigCache;
  private final Cache<Path,Integer> checked_tablet_dir_cache;
  private final DeadCompactionDetector deadCompactionDetector;

  private final QueueMetrics queueMetrics;

  public CompactionCoordinator(ServerContext ctx, LiveTServerSet tservers,
      SecurityOperation security, EventCoordinator eventCoordinator) {
    this.ctx = ctx;
    this.tserverSet = tservers;
    this.schedExecutor = this.ctx.getScheduledExecutor();
    this.security = security;
    this.eventCoordinator = eventCoordinator;

    this.jobQueues = new CompactionJobQueues(
        ctx.getConfiguration().getCount(Property.MANAGER_COMPACTION_SERVICE_PRIORITY_QUEUE_SIZE));

    this.queueMetrics = new QueueMetrics(jobQueues);

    var refreshLatches = new EnumMap<Ample.DataLevel,CountDownLatch>(Ample.DataLevel.class);
    refreshLatches.put(Ample.DataLevel.ROOT, new CountDownLatch(1));
    refreshLatches.put(Ample.DataLevel.METADATA, new CountDownLatch(1));
    refreshLatches.put(Ample.DataLevel.USER, new CountDownLatch(1));
    this.refreshLatches = Collections.unmodifiableMap(refreshLatches);

    completed = ctx.getCaches().createNewBuilder(CacheName.COMPACTIONS_COMPLETED, true)
        .maximumSize(200).expireAfterWrite(10, TimeUnit.MINUTES).build();

    CacheLoader<Long,CompactionConfig> loader =
        txid -> CompactionConfigStorage.getConfig(ctx, txid);

    // Keep a small short lived cache of compaction config. Compaction config never changes, however
    // when a compaction is canceled it is deleted which is why there is a time limit. It does not
    // hurt to let a job that was canceled start, it will be canceled later. Caching this immutable
    // config will help avoid reading the same data over and over.
    compactionConfigCache = ctx.getCaches().createNewBuilder(CacheName.COMPACTION_CONFIGS, true)
        .expireAfterWrite(30, SECONDS).maximumSize(100).build(loader);

    Weigher<Path,Integer> weigher = (path, count) -> {
      return path.toUri().toString().length();
    };

    checked_tablet_dir_cache =
        ctx.getCaches().createNewBuilder(CacheName.COMPACTION_DIR_CACHE, true)
            .maximumWeight(10485760L).weigher(weigher).build();

    deadCompactionDetector = new DeadCompactionDetector(this.ctx, this, schedExecutor);
    // At this point the manager does not have its lock so no actions should be taken yet
  }

  private volatile Thread serviceThread = null;

  public void start() {
    serviceThread = Threads.createThread("CompactionCoordinator Thread", this);
    serviceThread.start();
  }

  public void shutdown() {
    shutdown = true;
    var localThread = serviceThread;
    if (localThread != null) {
      try {
        localThread.join();
      } catch (InterruptedException e) {
        LOG.error("Exception stopping compaction coordinator thread", e);
      }
    }
  }

  protected void startCompactionCleaner(ScheduledThreadPoolExecutor schedExecutor) {
    ScheduledFuture<?> future =
        schedExecutor.scheduleWithFixedDelay(this::cleanUpCompactors, 0, 5, TimeUnit.MINUTES);
    ThreadPools.watchNonCriticalScheduledTask(future);
  }

  protected void startRunningCleaner(ScheduledThreadPoolExecutor schedExecutor) {
    ScheduledFuture<?> future =
        schedExecutor.scheduleWithFixedDelay(this::cleanUpRunning, 0, 5, TimeUnit.MINUTES);
    ThreadPools.watchNonCriticalScheduledTask(future);
  }

  private void processRefreshes(Ample.DataLevel dataLevel) {
    try (var refreshStream = ctx.getAmple().refreshes(dataLevel).stream()) {
      // process batches of refresh entries to avoid reading all into memory at once
      Iterators.partition(refreshStream.iterator(), 10000).forEachRemaining(refreshEntries -> {
        LOG.info("Processing {} tablet refreshes for {}", refreshEntries.size(), dataLevel);

        var extents =
            refreshEntries.stream().map(RefreshEntry::getExtent).collect(Collectors.toList());
        var tabletsMeta = new HashMap<KeyExtent,TabletMetadata>();
        try (var tablets = ctx.getAmple().readTablets().forTablets(extents, Optional.empty())
            .fetch(PREV_ROW, LOCATION, SCANS).build()) {
          tablets.stream().forEach(tm -> tabletsMeta.put(tm.getExtent(), tm));
        }

        var tserverRefreshes = new HashMap<TabletMetadata.Location,List<TKeyExtent>>();

        refreshEntries.forEach(refreshEntry -> {
          var tm = tabletsMeta.get(refreshEntry.getExtent());

          // only need to refresh if the tablet is still on the same tserver instance
          if (tm != null && tm.getLocation() != null
              && tm.getLocation().getServerInstance().equals(refreshEntry.getTserver())) {
            KeyExtent extent = tm.getExtent();
            Collection<StoredTabletFile> scanfiles = tm.getScans();
            var ttr = extent.toThrift();
            tserverRefreshes.computeIfAbsent(tm.getLocation(), k -> new ArrayList<>()).add(ttr);
          }
        });

        String logId = "Coordinator:" + dataLevel;
        ThreadPoolExecutor threadPool =
            ctx.threadPools().createFixedThreadPool(10, "Tablet refresh " + logId, false);
        try {
          TabletRefresher.refreshTablets(threadPool, logId, ctx, tserverSet::getCurrentServers,
              tserverRefreshes);
        } finally {
          threadPool.shutdownNow();
        }

        ctx.getAmple().refreshes(dataLevel).delete(refreshEntries);
      });
    }
    // allow new refreshes to be written now that all preexisting ones are processed
    refreshLatches.get(dataLevel).countDown();
  }

  @Override
  public void run() {

    processRefreshes(Ample.DataLevel.ROOT);
    processRefreshes(Ample.DataLevel.METADATA);
    processRefreshes(Ample.DataLevel.USER);

    startCompactionCleaner(schedExecutor);
    startRunningCleaner(schedExecutor);

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
        update.setMessage("Coordinator restarted, compaction found in progress");
        rc.addUpdate(System.currentTimeMillis(), update);
        RUNNING_CACHE.put(ExternalCompactionId.of(rc.getJob().getExternalCompactionId()), rc);
      });
    }

    startDeadCompactionDetector();

    // ELASTICITY_TODO the main function of the following loop was getting group summaries from
    // tservers. Its no longer doing that. May be best to remove the loop and make the remaining
    // task a scheduled one.

    LOG.info("Starting loop to check tservers for compaction summaries");
    while (!shutdown) {
      long start = System.currentTimeMillis();

      long now = System.currentTimeMillis();
      TIME_COMPACTOR_LAST_CHECKED.forEach((k, v) -> {
        if ((now - v) > getMissingCompactorWarningTime()) {
          // ELASTICITY_TODO may want to consider of the group has any jobs queued OR if the group
          // still exist in configuration
          LOG.warn("No compactors have checked in with coordinator for group {} in {}ms", k,
              getMissingCompactorWarningTime());
        }
      });

      long checkInterval = getTServerCheckInterval();
      long duration = (System.currentTimeMillis() - start);
      if (checkInterval - duration > 0) {
        LOG.debug("Waiting {}ms for next group check", (checkInterval - duration));
        UtilWaitThread.sleep(checkInterval - duration);
      }
    }

    LOG.info("Shutting down");
  }

  protected void startDeadCompactionDetector() {
    deadCompactionDetector.start();
  }

  protected long getMissingCompactorWarningTime() {
    return FIFTEEN_MINUTES;
  }

  protected long getTServerCheckInterval() {
    return this.ctx.getConfiguration()
        .getTimeInMillis(Property.COMPACTION_COORDINATOR_TSERVER_COMPACTION_CHECK_INTERVAL);
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
  public TExternalCompactionJob getCompactionJob(TInfo tinfo, TCredentials credentials,
      String groupName, String compactorAddress, String externalCompactionId)
      throws ThriftSecurityException {

    // do not expect users to call this directly, expect compactors to call this method
    if (!security.canPerformSystemActions(credentials)) {
      throw new AccumuloSecurityException(credentials.getPrincipal(),
          SecurityErrorCode.PERMISSION_DENIED).asThriftException();
    }
    final String group = groupName.intern();
    LOG.trace("getCompactionJob called for group {} by compactor {}", group, compactorAddress);
    TIME_COMPACTOR_LAST_CHECKED.put(group, System.currentTimeMillis());

    TExternalCompactionJob result = null;

    CompactionJobQueues.MetaJob metaJob =
        jobQueues.poll(CompactionExecutorIdImpl.externalId(groupName));

    while (metaJob != null) {

      Optional<CompactionConfig> compactionConfig = getCompactionConfig(metaJob);

      // this method may reread the metadata, do not use the metadata in metaJob for anything after
      // this method
      ExternalCompactionMetadata ecm = null;

      var kind = metaJob.getJob().getKind();

      // Only reserve user compactions when the config is present. When compactions are canceled the
      // config is deleted.
      if (kind == CompactionKind.SYSTEM
          || (kind == CompactionKind.USER && compactionConfig.isPresent())) {
        ecm = reserveCompaction(metaJob, compactorAddress,
            ExternalCompactionId.from(externalCompactionId));
      }

      if (ecm != null) {
        result = createThriftJob(externalCompactionId, ecm, metaJob, compactionConfig);
        // It is possible that by the time this added that the the compactor that made this request
        // is dead. In this cases the compaction is not actually running.
        RUNNING_CACHE.put(ExternalCompactionId.of(result.getExternalCompactionId()),
            new RunningCompaction(result, compactorAddress, group));
        LOG.debug("Returning external job {} to {} with {} files", result.externalCompactionId,
            compactorAddress, ecm.getJobFiles().size());
        break;
      } else {
        LOG.debug("Unable to reserve compaction job for {}, pulling another off the queue ",
            metaJob.getTabletMetadata().getExtent());
        metaJob = jobQueues.poll(CompactionExecutorIdImpl.externalId(groupName));
      }
    }

    if (metaJob == null) {
      LOG.debug("No jobs found in group {} ", group);
    }

    if (result == null) {
      LOG.trace("No jobs found for group {}, returning empty job to compactor {}", group,
          compactorAddress);
      result = new TExternalCompactionJob();
    }

    return result;

  }

  // ELASTICITY_TODO unit test this code
  private boolean canReserveCompaction(TabletMetadata tablet, CompactionJob job,
      Set<StoredTabletFile> jobFiles) {

    if (tablet == null) {
      // the tablet no longer exist
      return false;
    }

    if (tablet.getOperationId() != null) {
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

    switch (job.getKind()) {
      case SYSTEM:
        if (tablet.getSelectedFiles() != null
            && !Collections.disjoint(jobFiles, tablet.getSelectedFiles().getFiles())) {
          return false;
        }
        break;
      case USER:
      case SELECTOR:
        if (tablet.getSelectedFiles() == null
            || !tablet.getSelectedFiles().getFiles().containsAll(jobFiles)) {
          return false;
        }
        break;
      default:
        throw new UnsupportedOperationException("Not currently handling " + job.getKind());
    }

    return true;
  }

  private void checkTabletDir(KeyExtent extent, Path path) {
    try {
      if (checked_tablet_dir_cache.getIfPresent(path) == null) {
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
        checked_tablet_dir_cache.put(path, 1);
      }
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private ExternalCompactionMetadata createExternalCompactionMetadata(CompactionJob job,
      Set<StoredTabletFile> jobFiles, TabletMetadata tablet, String compactorAddress,
      ExternalCompactionId externalCompactionId) {
    boolean propDels;

    Long fateTxId = null;

    switch (job.getKind()) {
      case SYSTEM: {
        boolean compactingAll = tablet.getFiles().equals(jobFiles);
        propDels = !compactingAll;
      }
        break;
      case SELECTOR:
      case USER: {
        boolean compactingAll = tablet.getSelectedFiles().initiallySelectedAll()
            && tablet.getSelectedFiles().getFiles().equals(jobFiles);
        propDels = !compactingAll;
        fateTxId = tablet.getSelectedFiles().getFateTxId();
      }
        break;
      default:
        throw new IllegalArgumentException();
    }

    Consumer<String> directoryCreator = dir -> checkTabletDir(tablet.getExtent(), new Path(dir));
    ReferencedTabletFile newFile = TabletNameGenerator.getNextDataFilenameForMajc(propDels, ctx,
        tablet, directoryCreator, externalCompactionId);

    return new ExternalCompactionMetadata(jobFiles, newFile, compactorAddress, job.getKind(),
        job.getPriority(), job.getExecutor(), propDels, fateTxId);

  }

  private ExternalCompactionMetadata reserveCompaction(CompactionJobQueues.MetaJob metaJob,
      String compactorAddress, ExternalCompactionId externalCompactionId) {

    Preconditions.checkArgument(metaJob.getJob().getKind() == CompactionKind.SYSTEM
        || metaJob.getJob().getKind() == CompactionKind.USER);

    var tabletMetadata = metaJob.getTabletMetadata();

    var jobFiles = metaJob.getJob().getFiles().stream().map(CompactableFileImpl::toStoredTabletFile)
        .collect(Collectors.toSet());

    Retry retry =
        Retry.builder().maxRetries(5).retryAfter(100, MILLISECONDS).incrementBy(100, MILLISECONDS)
            .maxWait(10, SECONDS).backOffFactor(1.5).logInterval(3, MINUTES).createRetry();

    while (retry.canRetry()) {
      try (var tabletsMutator = ctx.getAmple().conditionallyMutateTablets()) {
        var extent = metaJob.getTabletMetadata().getExtent();

        if (!canReserveCompaction(tabletMetadata, metaJob.getJob(), jobFiles)) {
          return null;
        }

        var ecm = createExternalCompactionMetadata(metaJob.getJob(), jobFiles, tabletMetadata,
            compactorAddress, externalCompactionId);

        // any data that is read from the tablet to make a decision about if it can compact or not
        // must be included in the requireSame call
        var tabletMutator = tabletsMutator.mutateTablet(extent).requireAbsentOperation()
            .requireSame(tabletMetadata, FILES, SELECTED, ECOMP);

        tabletMutator.putExternalCompaction(externalCompactionId, ecm);
        tabletMutator.submit(tm -> tm.getExternalCompactions().containsKey(externalCompactionId));

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
  }

  TExternalCompactionJob createThriftJob(String externalCompactionId,
      ExternalCompactionMetadata ecm, CompactionJobQueues.MetaJob metaJob,
      Optional<CompactionConfig> compactionConfig) {

    Map<String,String> overrides = CompactionPluginUtils.computeOverrides(compactionConfig, ctx,
        metaJob.getTabletMetadata().getExtent(), metaJob.getJob().getFiles());

    IteratorConfig iteratorSettings = SystemIteratorUtil
        .toIteratorConfig(compactionConfig.map(CompactionConfig::getIterators).orElse(List.of()));

    var files = ecm.getJobFiles().stream().map(storedTabletFile -> {
      var dfv = metaJob.getTabletMetadata().getFilesMap().get(storedTabletFile);
      return new InputFile(storedTabletFile.getMetadata(), dfv.getSize(), dfv.getNumEntries(),
          dfv.getTime());
    }).collect(Collectors.toList());

    long fateTxid = 0;
    if (metaJob.getJob().getKind() == CompactionKind.USER) {
      fateTxid = metaJob.getTabletMetadata().getSelectedFiles().getFateTxId();
    }

    return new TExternalCompactionJob(externalCompactionId,
        metaJob.getTabletMetadata().getExtent().toThrift(), files, iteratorSettings,
        ecm.getCompactTmpName().getNormalizedPathStr(), ecm.getPropagateDeletes(),
        TCompactionKind.valueOf(ecm.getKind().name()), fateTxid, overrides);
  }

  @Override
  public void registerMetrics(MeterRegistry registry) {
    Gauge.builder(METRICS_MAJC_QUEUED, jobQueues, CompactionJobQueues::getQueuedJobCount)
        .description("Number of queued major compactions").register(registry);
    Gauge.builder(METRICS_MAJC_RUNNING, this, CompactionCoordinator::getNumRunningCompactions)
        .description("Number of running major compactions").register(registry);

    queueMetrics.registerMetrics(registry);
  }

  public void addJobs(TabletMetadata tabletMetadata, Collection<CompactionJob> jobs) {
    jobQueues.add(tabletMetadata, jobs);
  }

  public CompactionCoordinatorService.Iface getThriftService() {
    return this;
  }

  class RefreshWriter {

    private final ExternalCompactionId ecid;
    private final KeyExtent extent;

    private RefreshEntry writtenEntry;

    RefreshWriter(ExternalCompactionId ecid, KeyExtent extent) {
      this.ecid = ecid;
      this.extent = extent;

      var dataLevel = Ample.DataLevel.of(extent.tableId());
      try {
        // Wait for any refresh entries from the previous manager process to be processed before
        // writing new ones.
        refreshLatches.get(dataLevel).await();
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }

    public void writeRefresh(TabletMetadata.Location location) {
      Objects.requireNonNull(location);

      if (writtenEntry != null) {
        if (location.getServerInstance().equals(writtenEntry.getTserver())) {
          // the location was already written so nothing to do
          return;
        } else {
          deleteRefresh();
        }
      }

      var entry = new RefreshEntry(ecid, extent, location.getServerInstance());

      ctx.getAmple().refreshes(Ample.DataLevel.of(extent.tableId())).add(List.of(entry));

      LOG.debug("wrote refresh entry for {}", ecid);

      writtenEntry = entry;
    }

    public void deleteRefresh() {
      if (writtenEntry != null) {
        ctx.getAmple().refreshes(Ample.DataLevel.of(extent.tableId()))
            .delete(List.of(writtenEntry));
        LOG.debug("deleted refresh entry for {}", ecid);
        writtenEntry = null;
      }
    }
  }

  private Optional<CompactionConfig> getCompactionConfig(CompactionJobQueues.MetaJob metaJob) {
    if (metaJob.getJob().getKind() == CompactionKind.USER
        && metaJob.getTabletMetadata().getSelectedFiles() != null) {
      var cconf =
          compactionConfigCache.get(metaJob.getTabletMetadata().getSelectedFiles().getFateTxId());
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
   * <li>If the compaction can commit then a ~refresh entry may be written to the metadata table.
   * This is done before attempting to commit to cover the case of process failure after commit. If
   * the manager dies after commit then when it restarts it will see the ~refresh entry and refresh
   * that tablet. The ~refresh entry is only written when its a system compaction on a tablet with a
   * location.</li>
   * <li>Commit the compaction using a conditional mutation. If the tablets files or location
   * changed since reading the tablets metadata, then conditional mutation will fail. When this
   * happens it will reread the metadata and go back to step 1 conceptually. When committing a
   * compaction the compacted files are removed and scan entries are added to the tablet in case the
   * files are in use, this prevents GC from deleting the files between updating tablet metadata and
   * refreshing the tablet. The scan entries are only added when a tablet has a location.</li>
   * <li>After successful commit a refresh request is sent to the tablet if it has a location. This
   * will cause the tablet to start using the newly compacted files for future scans. Also the
   * tablet can delete the scan entries if there are no active scans using them.</li>
   * <li>If a ~refresh entry was written, delete it since the refresh was successful.</li>
   * </ol>
   *
   * <p>
   * User compactions will be refreshed as part of the fate operation. The user compaction fate
   * operation will see the compaction was committed after this code updates the tablet metadata,
   * however if it were to rely on this code to do the refresh it would not be able to know when the
   * refresh was actually done. Therefore, user compactions will refresh as part of the fate
   * operation so that it's known to be done before the fate operation returns. Since the fate
   * operation will do it, there is no need to do it here for user compactions.
   * </p>
   *
   * <p>
   * The ~refresh entries serve a similar purpose to FATE operations, it ensures that code executes
   * even when a process dies. FATE was intentionally not used for compaction commit because FATE
   * stores its data in zookeeper. The refresh entry is stored in the metadata table, which is much
   * more scalable than zookeeper. The number of system compactions of small files could be large
   * and this would be a large number of writes to zookeeper. Zookeeper scales somewhat with reads,
   * but not with writes.
   * </p>
   *
   * <p>
   * Issue #3559 was opened to explore the possibility of making compaction commit a fate operation
   * which would remove the need for the ~refresh section.
   * </p>
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

    var extent = KeyExtent.fromThrift(textent);
    LOG.info("Compaction completed, id: {}, stats: {}, extent: {}", externalCompactionId, stats,
        extent);
    final var ecid = ExternalCompactionId.of(externalCompactionId);

    var tabletMeta =
        ctx.getAmple().readTablet(extent, ECOMP, SELECTED, LOCATION, FILES, COMPACTED, OPID);

    if (!canCommitCompaction(ecid, tabletMeta)) {
      return;
    }

    ExternalCompactionMetadata ecm = tabletMeta.getExternalCompactions().get(ecid);

    // ELASTICITY_TODO this code does not handle race conditions or faults. Need to ensure refresh
    // happens in the case of manager process death between commit and refresh.
    ReferencedTabletFile newDatafile =
        TabletNameGenerator.computeCompactionFileDest(ecm.getCompactTmpName());

    Optional<ReferencedTabletFile> optionalNewFile;
    try {
      optionalNewFile = renameOrDeleteFile(stats, ecm, newDatafile);
    } catch (IOException e) {
      LOG.warn("Can not commit complete compaction {} because unable to delete or rename {} ", ecid,
          ecm.getCompactTmpName(), e);
      compactionFailed(Map.of(ecid, extent));
      return;
    }

    RefreshWriter refreshWriter = new RefreshWriter(ecid, extent);

    try {
      tabletMeta = commitCompaction(stats, ecid, tabletMeta, optionalNewFile, refreshWriter);
    } catch (RuntimeException e) {
      LOG.warn("Failed to commit complete compaction {} {}", ecid, extent, e);
      compactionFailed(Map.of(ecid, extent));
    }

    if (ecm.getKind() != CompactionKind.USER) {
      refreshTablet(tabletMeta);
    }

    // if a refresh entry was written, it can be removed after the tablet was refreshed
    refreshWriter.deleteRefresh();

    // It's possible that RUNNING might not have an entry for this ecid in the case
    // of a coordinator restart when the Coordinator can't find the TServer for the
    // corresponding external compaction.
    recordCompletion(ecid);

    // This will causes the tablet to be reexamined to see if it needs any more compactions.
    eventCoordinator.event(extent, "Compaction completed %s", extent);
  }

  private Optional<ReferencedTabletFile> renameOrDeleteFile(TCompactionStats stats,
      ExternalCompactionMetadata ecm, ReferencedTabletFile newDatafile) throws IOException {
    if (stats.getEntriesWritten() == 0) {
      // the compaction produced no output so do not need to rename or add a file to the metadata
      // table, only delete the input files.
      if (!ctx.getVolumeManager().delete(ecm.getCompactTmpName().getPath())) {
        throw new IOException("delete returned false");
      }

      return Optional.empty();
    } else {
      if (!ctx.getVolumeManager().rename(ecm.getCompactTmpName().getPath(),
          newDatafile.getPath())) {
        throw new IOException("rename returned false");
      }

      return Optional.of(newDatafile);
    }
  }

  private void refreshTablet(TabletMetadata metadata) {
    var location = metadata.getLocation();
    if (location != null) {
      KeyExtent extent = metadata.getExtent();

      // there is a single tserver and single tablet, do not need a thread pool. The direct executor
      // will run everything in the current thread
      ExecutorService executorService = MoreExecutors.newDirectExecutorService();
      try {
        TabletRefresher.refreshTablets(executorService,
            "compaction:" + metadata.getExtent().toString(), ctx, tserverSet::getCurrentServers,
            Map.of(metadata.getLocation(), List.of(extent.toThrift())));
      } finally {
        executorService.shutdownNow();
      }
    }
  }

  // ELASTICITY_TODO unit test this method
  private boolean canCommitCompaction(ExternalCompactionId ecid, TabletMetadata tabletMetadata) {

    if (tabletMetadata == null) {
      LOG.debug("Received completion notification for nonexistent tablet {}", ecid);
      return false;
    }

    var extent = tabletMetadata.getExtent();

    if (tabletMetadata.getOperationId() != null) {
      // split, merge, and delete tablet should delete the compaction entry in the tablet
      LOG.debug("Received completion notification for tablet with active operation {} {} {}", ecid,
          extent, tabletMetadata.getOperationId());
      return false;
    }

    ExternalCompactionMetadata ecm = tabletMetadata.getExternalCompactions().get(ecid);

    if (ecm == null) {
      LOG.debug("Received completion notification for unknown compaction {} {}", ecid, extent);
      return false;
    }

    if (ecm.getKind() == CompactionKind.USER || ecm.getKind() == CompactionKind.SELECTOR) {
      if (tabletMetadata.getSelectedFiles() == null) {
        // when the compaction is canceled, selected files are deleted
        LOG.debug(
            "Received completion notification for user compaction and tablet has no selected files {} {}",
            ecid, extent);
        return false;
      }

      if (ecm.getFateTxId() != tabletMetadata.getSelectedFiles().getFateTxId()) {
        // maybe the compaction was cancled and another user compaction was started on the tablet.
        LOG.debug(
            "Received completion notification for user compaction where its fate txid did not match the tablets {} {} {} {}",
            ecid, extent, FateTxId.formatTid(ecm.getFateTxId()),
            FateTxId.formatTid(tabletMetadata.getSelectedFiles().getFateTxId()));
      }

      if (!tabletMetadata.getSelectedFiles().getFiles().containsAll(ecm.getJobFiles())) {
        // this is not expected to happen
        LOG.error("User compaction contained files not in the selected set {} {} {} {} {}",
            tabletMetadata.getExtent(), ecid, ecm.getKind(),
            Optional.ofNullable(tabletMetadata.getSelectedFiles()).map(SelectedFiles::getFiles),
            ecm.getJobFiles());
        return false;
      }
    }

    if (!tabletMetadata.getFiles().containsAll(ecm.getJobFiles())) {
      // this is not expected to happen
      LOG.error("Compaction contained files not in the tablet files set {} {} {} {}",
          tabletMetadata.getExtent(), ecid, tabletMetadata.getFiles(), ecm.getJobFiles());
      return false;
    }

    return true;
  }

  private TabletMetadata commitCompaction(TCompactionStats stats, ExternalCompactionId ecid,
      TabletMetadata tablet, Optional<ReferencedTabletFile> newDatafile,
      RefreshWriter refreshWriter) {

    KeyExtent extent = tablet.getExtent();

    Retry retry = Retry.builder().infiniteRetries().retryAfter(100, MILLISECONDS)
        .incrementBy(100, MILLISECONDS).maxWait(10, SECONDS).backOffFactor(1.5)
        .logInterval(3, MINUTES).createRetry();

    while (canCommitCompaction(ecid, tablet)) {
      ExternalCompactionMetadata ecm = tablet.getExternalCompactions().get(ecid);

      // the compacted files should not exists in the tablet already
      var tablet2 = tablet;
      newDatafile.ifPresent(
          newFile -> Preconditions.checkState(!tablet2.getFiles().contains(newFile.insert()),
              "File already exists in tablet %s %s", newFile, tablet2.getFiles()));

      if (tablet.getLocation() != null
          && tablet.getExternalCompactions().get(ecid).getKind() != CompactionKind.USER) {
        // Write the refresh entry before attempting to update tablet metadata, this ensures that
        // refresh will happen even if this process dies. In the case where this process does not
        // die refresh will happen after commit. User compactions will make refresh calls in their
        // fate operation, so it does not need to be done here.
        refreshWriter.writeRefresh(tablet.getLocation());
      }

      try (var tabletsMutator = ctx.getAmple().conditionallyMutateTablets()) {
        var tabletMutator = tabletsMutator.mutateTablet(extent).requireAbsentOperation()
            .requireCompaction(ecid).requireSame(tablet, FILES, LOCATION);

        if (ecm.getKind() == CompactionKind.USER || ecm.getKind() == CompactionKind.SELECTOR) {
          tabletMutator.requireSame(tablet, SELECTED, COMPACTED);
        }

        // make the needed updates to the tablet
        updateTabletForCompaction(stats, ecid, tablet, newDatafile, extent, ecm, tabletMutator);

        tabletMutator
            .submit(tabletMetadata -> !tabletMetadata.getExternalCompactions().containsKey(ecid));

        // TODO expensive logging
        LOG.debug("Compaction completed {} added {} removed {}", tablet.getExtent(), newDatafile,
            ecm.getJobFiles().stream().map(AbstractTabletFile::getFileName)
                .collect(Collectors.toList()));

        // ELASTICITY_TODO check return value and retry, could fail because of race conditions
        var result = tabletsMutator.process().get(extent);
        if (result.getStatus() == Ample.ConditionalResult.Status.ACCEPTED) {
          // compaction was committed, mark the compaction input files for deletion
          //
          // ELASTICITIY_TODO in the case of process death the GC candidates would never be added
          // like #3811. If compaction commit were moved to FATE per #3559 then this would not
          // be an issue. If compaction commit is never moved to FATE, then this addition could
          // moved to the compaction refresh process. The compaction refresh process will go away
          // if compaction commit is moved to FATE, so should only do this if not moving to FATE.
          ctx.getAmple().putGcCandidates(extent.tableId(), ecm.getJobFiles());
          break;
        } else {
          // compaction failed to commit, maybe something changed on the tablet so lets reread the
          // metadata and try again
          tablet = result.readMetadata();
        }

        retry.waitForNextAttempt(LOG, "Failed to commit " + ecid + " for tablet " + extent);
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }

    return tablet;
  }

  private void updateTabletForCompaction(TCompactionStats stats, ExternalCompactionId ecid,
      TabletMetadata tablet, Optional<ReferencedTabletFile> newDatafile, KeyExtent extent,
      ExternalCompactionMetadata ecm, Ample.ConditionalTabletMutator tabletMutator) {
    // ELASTICITY_TODO improve logging adapt to use existing tablet files logging
    if (ecm.getKind() == CompactionKind.USER) {
      if (tablet.getSelectedFiles().getFiles().equals(ecm.getJobFiles())) {
        // all files selected for the user compactions are finished, so the tablet is finish and
        // its compaction id needs to be updated.

        long fateTxId = tablet.getSelectedFiles().getFateTxId();

        Preconditions.checkArgument(!tablet.getCompacted().contains(fateTxId),
            "Tablet %s unexpected has selected files and compacted columns for %s",
            tablet.getExtent(), fateTxId);

        // TODO set to trace
        LOG.debug("All selected files compcated for {} setting compacted for {}",
            tablet.getExtent(), FateTxId.formatTid(tablet.getSelectedFiles().getFateTxId()));

        tabletMutator.deleteSelectedFiles();
        tabletMutator.putCompacted(fateTxId);

      } else {
        // not all of the selected files were finished, so need to add the new file to the
        // selected set

        Set<StoredTabletFile> newSelectedFileSet =
            new HashSet<>(tablet.getSelectedFiles().getFiles());
        newSelectedFileSet.removeAll(ecm.getJobFiles());

        if (newDatafile.isPresent()) {
          // TODO set to trace
          LOG.debug(
              "Not all selected files for {} are done, adding new selected file {} from compaction",
              tablet.getExtent(), newDatafile.orElseThrow().getPath().getName());
          newSelectedFileSet.add(newDatafile.orElseThrow().insert());
        } else {
          // TODO set to trace
          LOG.debug(
              "Not all selected files for {} are done, compaction produced no output so not adding to selected set.",
              tablet.getExtent());
        }

        tabletMutator.putSelectedFiles(
            new SelectedFiles(newSelectedFileSet, tablet.getSelectedFiles().initiallySelectedAll(),
                tablet.getSelectedFiles().getFateTxId()));
      }
    }

    if (tablet.getLocation() != null) {
      // add scan entries to prevent GC in case the hosted tablet is currently using the files for
      // scan
      ecm.getJobFiles().forEach(tabletMutator::putScan);
    }
    ecm.getJobFiles().forEach(tabletMutator::deleteFile);
    tabletMutator.deleteExternalCompaction(ecid);

    if (newDatafile.isPresent()) {
      tabletMutator.putFile(newDatafile.orElseThrow(),
          new DataFileValue(stats.getFileSize(), stats.getEntriesWritten()));
    }
  }

  @Override
  public void compactionFailed(TInfo tinfo, TCredentials credentials, String externalCompactionId,
      TKeyExtent extent) throws ThriftSecurityException {
    // do not expect users to call this directly, expect other tservers to call this method
    if (!security.canPerformSystemActions(credentials)) {
      throw new AccumuloSecurityException(credentials.getPrincipal(),
          SecurityErrorCode.PERMISSION_DENIED).asThriftException();
    }
    LOG.info("Compaction failed, id: {}", externalCompactionId);
    final var ecid = ExternalCompactionId.of(externalCompactionId);
    compactionFailed(Map.of(ecid, KeyExtent.fromThrift(extent)));
  }

  void compactionFailed(Map<ExternalCompactionId,KeyExtent> compactions) {

    try (var tabletsMutator = ctx.getAmple().conditionallyMutateTablets()) {
      compactions.forEach((ecid, extent) -> {
        try {
          ctx.requireNotDeleted(extent.tableId());
          tabletsMutator.mutateTablet(extent).requireAbsentOperation().requireCompaction(ecid)
              .deleteExternalCompaction(ecid).submit(new RejectionHandler() {

                @Override
                public boolean callWhenTabletDoesNotExists() {
                  return true;
                }

                @Override
                public boolean test(TabletMetadata tabletMetadata) {
                  return tabletMetadata == null
                      || !tabletMetadata.getExternalCompactions().containsKey(ecid);
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
            var ecid =
                compactions.entrySet().stream().filter(entry -> entry.getValue().equals(extent))
                    .findFirst().map(Map.Entry::getKey).orElse(null);
            LOG.debug("Unable to remove failed compaction {} {}", extent, ecid);
          }
        } else {
          // compactionFailed is called from the Compactor when either a compaction fails or
          // is cancelled and it's called from the DeadCompactionDetector. This block is
          // entered when the conditional mutator above successfully deletes an ecid from
          // the tablet metadata. Remove compaction tmp files from the tablet directory
          // that have a corresponding ecid in the name.

          ecidsForTablet.clear();
          compactions.entrySet().stream().filter(e -> e.getValue().compareTo(extent) == 0)
              .map(Entry::getKey).forEach(ecidsForTablet::add);

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
                    FileStatus[] files = fs.listStatus(new Path(volPath), (path) -> {
                      return path.getName().endsWith(fileSuffix);
                    });
                    if (files.length > 0) {
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

    compactions.forEach((k, v) -> recordCompletion(k));
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
    }
  }

  private void recordCompletion(ExternalCompactionId ecid) {
    var rc = RUNNING_CACHE.remove(ecid);
    if (rc != null) {
      completed.put(ecid, rc);
    }
  }

  protected Set<ExternalCompactionId> readExternalCompactionIds() {
    return this.ctx.getAmple().readTablets().forLevel(Ample.DataLevel.USER).fetch(ECOMP).build()
        .stream().flatMap(tm -> tm.getExternalCompactions().keySet().stream())
        .collect(Collectors.toSet());
  }

  /**
   * The RUNNING_CACHE set may contain external compactions that are not actually running. This
   * method periodically cleans those up.
   */
  protected void cleanUpRunning() {

    // grab a snapshot of the ids in the set before reading the metadata table. This is done to
    // avoid removing things that are added while reading the metadata.
    Set<ExternalCompactionId> idsSnapshot = Set.copyOf(RUNNING_CACHE.keySet());

    // grab the ids that are listed as running in the metadata table. It important that this is done
    // after getting the snapshot.
    Set<ExternalCompactionId> idsInMetadata = readExternalCompactionIds();

    var idsToRemove = Sets.difference(idsSnapshot, idsInMetadata);

    // remove ids that are in the running set but not in the metadata table
    idsToRemove.forEach(this::recordCompletion);

    if (idsToRemove.size() > 0) {
      LOG.debug("Removed stale entries from RUNNING_CACHE : {}", idsToRemove);
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
  public TExternalCompactionList getRunningCompactions(TInfo tinfo, TCredentials credentials)
      throws ThriftSecurityException {
    // do not expect users to call this directly, expect other tservers to call this method
    if (!security.canPerformSystemActions(credentials)) {
      throw new AccumuloSecurityException(credentials.getPrincipal(),
          SecurityErrorCode.PERMISSION_DENIED).asThriftException();
    }

    final TExternalCompactionList result = new TExternalCompactionList();
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
   * Return information about recently completed compactions
   *
   * @param tinfo trace info
   * @param credentials tcredentials object
   * @return map of ECID to TExternalCompaction objects
   * @throws ThriftSecurityException permission error
   */
  @Override
  public TExternalCompactionList getCompletedCompactions(TInfo tinfo, TCredentials credentials)
      throws ThriftSecurityException {
    // do not expect users to call this directly, expect other tservers to call this method
    if (!security.canPerformSystemActions(credentials)) {
      throw new AccumuloSecurityException(credentials.getPrincipal(),
          SecurityErrorCode.PERMISSION_DENIED).asThriftException();
    }
    final TExternalCompactionList result = new TExternalCompactionList();
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

  /* Method exists to be overridden in test to hide static method */
  protected String getTServerAddressString(HostAndPort tserverAddress) {
    return ExternalCompactionUtil.getHostPortString(tserverAddress);
  }

  /* Method exists to be overridden in test to hide static method */
  protected List<RunningCompaction> getCompactionsRunningOnCompactors() {
    return ExternalCompactionUtil.getCompactionsRunningOnCompactors(this.ctx);
  }

  /* Method exists to be overridden in test to hide static method */
  protected void cancelCompactionOnCompactor(String address, String externalCompactionId) {
    HostAndPort hostPort = HostAndPort.fromString(address);
    ExternalCompactionUtil.cancelCompaction(this.ctx, hostPort, externalCompactionId);
  }

  /* Method exists to be overridden in test to hide static method */
  protected void returnTServerClient(TabletServerClientService.Client client) {
    ThriftUtil.returnClient(client, this.ctx);
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

  private void cleanUpCompactors() {
    final String compactorQueuesPath = this.ctx.getZooKeeperRoot() + Constants.ZCOMPACTORS;

    var zoorw = this.ctx.getZooReaderWriter();

    try {
      var groups = zoorw.getChildren(compactorQueuesPath);

      for (String group : groups) {
        String qpath = compactorQueuesPath + "/" + group;

        var compactors = zoorw.getChildren(qpath);

        if (compactors.isEmpty()) {
          deleteEmpty(zoorw, qpath);
        }

        for (String compactor : compactors) {
          String cpath = compactorQueuesPath + "/" + group + "/" + compactor;
          var lockNodes = zoorw.getChildren(compactorQueuesPath + "/" + group + "/" + compactor);
          if (lockNodes.isEmpty()) {
            deleteEmpty(zoorw, cpath);
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

}

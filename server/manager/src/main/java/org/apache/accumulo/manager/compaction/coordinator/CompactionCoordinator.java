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
import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.SELECTED;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.TableDeletedException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.CompactionConfig;
import org.apache.accumulo.core.client.admin.compaction.CompactableFile;
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
import org.apache.accumulo.core.fate.Fate;
import org.apache.accumulo.core.fate.FateInstanceType;
import org.apache.accumulo.core.fate.zookeeper.ZooReaderWriter;
import org.apache.accumulo.core.iterators.user.HasExternalCompactionsFilter;
import org.apache.accumulo.core.iteratorsImpl.system.SystemIteratorUtil;
import org.apache.accumulo.core.metadata.CompactableFileImpl;
import org.apache.accumulo.core.metadata.ReferencedTabletFile;
import org.apache.accumulo.core.metadata.StoredTabletFile;
import org.apache.accumulo.core.metadata.schema.Ample;
import org.apache.accumulo.core.metadata.schema.Ample.RejectionHandler;
import org.apache.accumulo.core.metadata.schema.CompactionMetadata;
import org.apache.accumulo.core.metadata.schema.ExternalCompactionId;
import org.apache.accumulo.core.metadata.schema.TabletMetadata;
import org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType;
import org.apache.accumulo.core.metrics.MetricsProducer;
import org.apache.accumulo.core.securityImpl.thrift.TCredentials;
import org.apache.accumulo.core.spi.compaction.CompactionJob;
import org.apache.accumulo.core.spi.compaction.CompactionKind;
import org.apache.accumulo.core.tabletserver.thrift.InputFile;
import org.apache.accumulo.core.tabletserver.thrift.IteratorConfig;
import org.apache.accumulo.core.tabletserver.thrift.TCompactionKind;
import org.apache.accumulo.core.tabletserver.thrift.TCompactionStats;
import org.apache.accumulo.core.tabletserver.thrift.TExternalCompactionJob;
import org.apache.accumulo.core.util.Retry;
import org.apache.accumulo.core.util.UtilWaitThread;
import org.apache.accumulo.core.util.cache.Caches.CacheName;
import org.apache.accumulo.core.util.compaction.CompactorGroupIdImpl;
import org.apache.accumulo.core.util.compaction.ExternalCompactionUtil;
import org.apache.accumulo.core.util.compaction.RunningCompaction;
import org.apache.accumulo.core.util.threads.ThreadPools;
import org.apache.accumulo.core.util.threads.Threads;
import org.apache.accumulo.core.volume.Volume;
import org.apache.accumulo.manager.EventCoordinator;
import org.apache.accumulo.manager.Manager;
import org.apache.accumulo.manager.compaction.coordinator.commit.CommitCompaction;
import org.apache.accumulo.manager.compaction.coordinator.commit.CompactionCommitData;
import org.apache.accumulo.manager.compaction.coordinator.commit.RenameCompactionFile;
import org.apache.accumulo.manager.compaction.queue.CompactionJobQueues;
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
import com.google.common.collect.Sets;
import com.google.common.net.HostAndPort;

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

  /* Map of group name to last time compactor called to get a compaction job */
  // ELASTICITY_TODO need to clean out groups that are no longer configured..
  private static final Map<String,Long> TIME_COMPACTOR_LAST_CHECKED = new ConcurrentHashMap<>();

  private final ServerContext ctx;
  private final SecurityOperation security;
  private final CompactionJobQueues jobQueues;
  private final EventCoordinator eventCoordinator;
  private final AtomicReference<Map<FateInstanceType,Fate<Manager>>> fateInstances;
  // Exposed for tests
  protected volatile Boolean shutdown = false;

  private final ScheduledThreadPoolExecutor schedExecutor;

  private final Cache<ExternalCompactionId,RunningCompaction> completed;
  private LoadingCache<Long,CompactionConfig> compactionConfigCache;
  private final Cache<Path,Integer> checked_tablet_dir_cache;
  private final DeadCompactionDetector deadCompactionDetector;

  private final QueueMetrics queueMetrics;

  public CompactionCoordinator(ServerContext ctx, LiveTServerSet tservers,
      SecurityOperation security, EventCoordinator eventCoordinator,
      AtomicReference<Map<FateInstanceType,Fate<Manager>>> fateInstances) {
    this.ctx = ctx;
    this.schedExecutor = this.ctx.getScheduledExecutor();
    this.security = security;
    this.eventCoordinator = eventCoordinator;

    this.jobQueues = new CompactionJobQueues(
        ctx.getConfiguration().getCount(Property.MANAGER_COMPACTION_SERVICE_PRIORITY_QUEUE_SIZE));

    this.queueMetrics = new QueueMetrics(jobQueues);

    this.fateInstances = fateInstances;

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

  @Override
  public void run() {

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

    CompactionJobQueues.MetaJob metaJob = jobQueues.poll(CompactorGroupIdImpl.groupId(groupName));

    while (metaJob != null) {

      Optional<CompactionConfig> compactionConfig = getCompactionConfig(metaJob);

      // this method may reread the metadata, do not use the metadata in metaJob for anything after
      // this method
      CompactionMetadata ecm = null;

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
        metaJob = jobQueues.poll(CompactorGroupIdImpl.groupId(groupName));
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

  private CompactionMetadata createExternalCompactionMetadata(CompactionJob job,
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

    return new CompactionMetadata(jobFiles, newFile, compactorAddress, job.getKind(),
        job.getPriority(), job.getGroup(), propDels, fateTxId);

  }

  private CompactionMetadata reserveCompaction(CompactionJobQueues.MetaJob metaJob,
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

  TExternalCompactionJob createThriftJob(String externalCompactionId, CompactionMetadata ecm,
      CompactionJobQueues.MetaJob metaJob, Optional<CompactionConfig> compactionConfig) {

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
      if (shutdown) {
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

    if (!CommitCompaction.canCommitCompaction(ecid, tabletMeta)) {
      return;
    }

    CompactionMetadata ecm = tabletMeta.getExternalCompactions().get(ecid);
    var renameOp = new RenameCompactionFile(new CompactionCommitData(ecid, extent, ecm, stats));

    // ELASTICITY_TODO add tag to fate that ECID can be added to. This solves two problem. First it
    // defends against starting multiple fate txs for the same thing. This will help the split code
    // also. Second the tag can be used by the dead compaction detector to ignore committing
    // compactions. The imple coould hash the key to produce the fate tx id.
    var txid = localFate.startTransaction();
    localFate.seedTransaction("COMMIT_COMPACTION", txid, renameOp, true,
        "Commit compaction " + ecid);

    // ELASTICITY_TODO need to remove this wait. It is here because when the dead compaction
    // detector ask a compactor what its currently running it expects that cover commit. To remove
    // this wait would need another way for the dead compaction detector to know about committing
    // compactions. Could add a tag to the fate tx with the ecid and have dead compaction detector
    // scan these tags. This wait makes the code running in fate not be fault tolerant because in
    // the
    // case of faults the dead compaction detector may remove the compaction entry.
    localFate.waitForCompletion(txid);

    // It's possible that RUNNING might not have an entry for this ecid in the case
    // of a coordinator restart when the Coordinator can't find the TServer for the
    // corresponding external compaction.
    recordCompletion(ecid);
    // ELASTICITY_TODO should above call move into fate code?
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
    return this.ctx.getAmple().readTablets().forLevel(Ample.DataLevel.USER)
        .filter(new HasExternalCompactionsFilter()).fetch(ECOMP).build().stream()
        .flatMap(tm -> tm.getExternalCompactions().keySet().stream()).collect(Collectors.toSet());
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
  protected List<RunningCompaction> getCompactionsRunningOnCompactors() {
    return ExternalCompactionUtil.getCompactionsRunningOnCompactors(this.ctx);
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

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
package org.apache.accumulo.coordinator;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.accumulo.core.conf.Property.COMPACTION_COORDINATOR_SUMMARIES_MAXTHREADS;
import static org.apache.accumulo.core.util.UtilWaitThread.sleepUninterruptibly;
import static org.apache.accumulo.core.util.threads.ThreadPoolNames.COMPACTION_COORDINATOR_SUMMARY_POOL;

import java.net.UnknownHostException;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import org.apache.accumulo.coordinator.QueueSummaries.PrioTserver;
import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.clientImpl.thrift.SecurityErrorCode;
import org.apache.accumulo.core.clientImpl.thrift.TableOperation;
import org.apache.accumulo.core.clientImpl.thrift.TableOperationExceptionType;
import org.apache.accumulo.core.clientImpl.thrift.ThriftSecurityException;
import org.apache.accumulo.core.clientImpl.thrift.ThriftTableOperationException;
import org.apache.accumulo.core.compaction.thrift.CompactionCoordinatorService;
import org.apache.accumulo.core.compaction.thrift.TCompactionState;
import org.apache.accumulo.core.compaction.thrift.TCompactionStatusUpdate;
import org.apache.accumulo.core.compaction.thrift.TExternalCompaction;
import org.apache.accumulo.core.compaction.thrift.TExternalCompactionList;
import org.apache.accumulo.core.compaction.thrift.TNextCompactionJob;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.NamespaceId;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.dataImpl.thrift.TKeyExtent;
import org.apache.accumulo.core.fate.zookeeper.ServiceLock;
import org.apache.accumulo.core.fate.zookeeper.ServiceLockSupport.HAServiceLockWatcher;
import org.apache.accumulo.core.fate.zookeeper.ZooReaderWriter;
import org.apache.accumulo.core.metadata.TServerInstance;
import org.apache.accumulo.core.metadata.schema.Ample;
import org.apache.accumulo.core.metadata.schema.ExternalCompactionId;
import org.apache.accumulo.core.metadata.schema.TabletMetadata;
import org.apache.accumulo.core.metrics.MetricsInfo;
import org.apache.accumulo.core.process.thrift.ServerProcessService;
import org.apache.accumulo.core.rpc.ThriftUtil;
import org.apache.accumulo.core.rpc.clients.ThriftClientTypes;
import org.apache.accumulo.core.securityImpl.thrift.TCredentials;
import org.apache.accumulo.core.tabletserver.thrift.TCompactionQueueSummary;
import org.apache.accumulo.core.tabletserver.thrift.TCompactionStats;
import org.apache.accumulo.core.tabletserver.thrift.TExternalCompactionJob;
import org.apache.accumulo.core.tabletserver.thrift.TabletClientService;
import org.apache.accumulo.core.trace.TraceUtil;
import org.apache.accumulo.core.trace.thrift.TInfo;
import org.apache.accumulo.core.util.HostAndPort;
import org.apache.accumulo.core.util.compaction.ExternalCompactionUtil;
import org.apache.accumulo.core.util.compaction.RunningCompaction;
import org.apache.accumulo.core.util.threads.ThreadPools;
import org.apache.accumulo.server.AbstractServer;
import org.apache.accumulo.server.GarbageCollectionLogger;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.ServerOpts;
import org.apache.accumulo.server.manager.LiveTServerSet;
import org.apache.accumulo.server.manager.LiveTServerSet.TServerConnection;
import org.apache.accumulo.server.rpc.ServerAddress;
import org.apache.accumulo.server.rpc.TServerUtils;
import org.apache.accumulo.server.rpc.ThriftProcessorTypes;
import org.apache.accumulo.server.security.AuditedSecurityOperation;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.google.common.collect.Sets;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.micrometer.core.instrument.Tag;

public class CompactionCoordinator extends AbstractServer implements
    CompactionCoordinatorService.Iface, LiveTServerSet.Listener, ServerProcessService.Iface {

  private static final Logger LOG = LoggerFactory.getLogger(CompactionCoordinator.class);

  private static final Logger STATUS_LOG =
      LoggerFactory.getLogger(CompactionCoordinator.class.getName() + ".compaction.status");
  private static final long TIME_BETWEEN_GC_CHECKS = 5000;
  protected static final QueueSummaries QUEUE_SUMMARIES = new QueueSummaries();

  /*
   * Map of compactionId to RunningCompactions. This is an informational cache of what external
   * compactions may be running. Its possible it may contain external compactions that are not
   * actually running. It may not contain compactions that are actually running. The metadata table
   * is the most authoritative source of what external compactions are currently running, but it
   * does not have the stats that this map has.
   */
  protected static final Map<ExternalCompactionId,RunningCompaction> RUNNING_CACHE =
      new ConcurrentHashMap<>();

  private static final Cache<ExternalCompactionId,RunningCompaction> COMPLETED =
      Caffeine.newBuilder().maximumSize(200).expireAfterWrite(10, TimeUnit.MINUTES).build();

  /* Map of queue name to last time compactor called to get a compaction job */
  private static final Map<String,Long> TIME_COMPACTOR_LAST_CHECKED = new ConcurrentHashMap<>();

  private final ConcurrentHashMap<String,AtomicLong> failingQueues = new ConcurrentHashMap<>();
  private final ConcurrentHashMap<String,AtomicLong> failingCompactors = new ConcurrentHashMap<>();
  private final ConcurrentHashMap<TableId,AtomicLong> failingTables = new ConcurrentHashMap<>();

  private final GarbageCollectionLogger gcLogger = new GarbageCollectionLogger();
  protected AuditedSecurityOperation security;
  protected final AccumuloConfiguration aconf;
  protected CompactionFinalizer compactionFinalizer;
  protected LiveTServerSet tserverSet;

  private ServiceLock coordinatorLock;

  private final LoadingCache<String,Integer> compactorCounts;

  protected CompactionCoordinator(ServerOpts opts, String[] args) {
    this(opts, args, null);
  }

  protected CompactionCoordinator(ServerOpts opts, String[] args, AccumuloConfiguration conf) {
    super("compaction-coordinator", opts, args);
    aconf = conf == null ? super.getConfiguration() : conf;
    compactionFinalizer = createCompactionFinalizer();
    tserverSet = createLiveTServerSet();
    setupSecurity();
    startGCLogger();
    printStartupMsg();
    startCompactionCleaner();
    startRunningCleaner();
    compactorCounts = Caffeine.newBuilder().expireAfterWrite(30, TimeUnit.SECONDS)
        .build(queue -> ExternalCompactionUtil.countCompactors(queue, getContext()));
  }

  @Override
  public AccumuloConfiguration getConfiguration() {
    return aconf;
  }

  protected CompactionFinalizer createCompactionFinalizer() {
    return new CompactionFinalizer(getContext(), getContext().getScheduledExecutor());
  }

  protected LiveTServerSet createLiveTServerSet() {
    return new LiveTServerSet(getContext(), this);
  }

  protected void setupSecurity() {
    security = getContext().getSecurityOperation();
  }

  protected void startGCLogger() {
    ScheduledFuture<?> future = getContext().getScheduledExecutor().scheduleWithFixedDelay(
        () -> gcLogger.logGCInfo(getConfiguration()), 0, TIME_BETWEEN_GC_CHECKS,
        TimeUnit.MILLISECONDS);
    ThreadPools.watchNonCriticalScheduledTask(future);
  }

  protected void startCompactionCleaner() {
    ScheduledFuture<?> future = getContext().getScheduledExecutor()
        .scheduleWithFixedDelay(this::cleanUpCompactors, 0, 5, TimeUnit.MINUTES);
    ThreadPools.watchNonCriticalScheduledTask(future);
  }

  protected void startRunningCleaner() {
    ScheduledFuture<?> future = getContext().getScheduledExecutor()
        .scheduleWithFixedDelay(this::cleanUpRunning, 0, 5, TimeUnit.MINUTES);
    ThreadPools.watchNonCriticalScheduledTask(future);
  }

  protected void printStartupMsg() {
    LOG.info("Version " + Constants.VERSION);
    LOG.info("Instance " + getContext().getInstanceID());
  }

  /**
   * Set up nodes and locks in ZooKeeper for this CompactionCoordinator
   *
   * @param clientAddress address of this Compactor
   * @throws KeeperException zookeeper error
   * @throws InterruptedException thread interrupted
   */
  protected void getCoordinatorLock(HostAndPort clientAddress)
      throws KeeperException, InterruptedException {
    LOG.info("trying to get coordinator lock");

    final String coordinatorClientAddress = ExternalCompactionUtil.getHostPortString(clientAddress);
    final String lockPath = getContext().getZooKeeperRoot() + Constants.ZCOORDINATOR_LOCK;
    final UUID zooLockUUID = UUID.randomUUID();

    coordinatorLock = new ServiceLock(getContext().getZooReaderWriter().getZooKeeper(),
        ServiceLock.path(lockPath), zooLockUUID);
    HAServiceLockWatcher coordinatorLockWatcher =
        new HAServiceLockWatcher("coordinator", () -> getShutdownComplete().get());
    while (true) {

      coordinatorLock.lock(coordinatorLockWatcher, coordinatorClientAddress.getBytes(UTF_8));

      coordinatorLockWatcher.waitForChange();
      if (coordinatorLockWatcher.isLockAcquired()) {
        break;
      }
      if (!coordinatorLockWatcher.isFailedToAcquireLock()) {
        throw new IllegalStateException("manager lock in unknown state");
      }
      coordinatorLock.tryToCancelAsyncLockOrUnlock();

      sleepUninterruptibly(1000, TimeUnit.MILLISECONDS);
    }
  }

  /**
   * Start this CompactionCoordinator thrift service to handle incoming client requests
   *
   * @return address of this CompactionCoordinator client service
   * @throws UnknownHostException host unknown
   */
  protected ServerAddress startCoordinatorClientService() throws UnknownHostException {
    var processor = ThriftProcessorTypes.getCoordinatorTProcessor(this, this, getContext());
    @SuppressWarnings("deprecation")
    var maxMessageSizeProperty = getConfiguration().resolve(Property.RPC_MAX_MESSAGE_SIZE,
        Property.GENERAL_MAX_MESSAGE_SIZE);
    ServerAddress sp = TServerUtils.startServer(getContext(), getBindAddress(),
        Property.COMPACTION_COORDINATOR_CLIENTPORT, processor, this.getClass().getSimpleName(),
        "Thrift Client Server", Property.COMPACTION_COORDINATOR_THRIFTCLIENT_PORTSEARCH,
        Property.COMPACTION_COORDINATOR_MINTHREADS,
        Property.COMPACTION_COORDINATOR_MINTHREADS_TIMEOUT,
        Property.COMPACTION_COORDINATOR_THREADCHECK, maxMessageSizeProperty);
    LOG.info("address = {}", sp.address);
    return sp;
  }

  protected Collection<Tag> getServiceTags(HostAndPort clientAddress) {
    return (MetricsInfo.serviceTags(getContext().getInstanceName(), getApplicationName(),
        clientAddress, ""));
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

    ServerAddress coordinatorAddress = null;
    try {
      coordinatorAddress = startCoordinatorClientService();
    } catch (UnknownHostException e1) {
      throw new RuntimeException("Failed to start the coordinator service", e1);
    }
    updateAdvertiseAddress(coordinatorAddress.getAddress());
    final HostAndPort clientAddress = getAdvertiseAddress();

    try {
      getCoordinatorLock(clientAddress);
    } catch (KeeperException | InterruptedException e) {
      throw new IllegalStateException("Exception getting Coordinator lock", e);
    }

    MetricsInfo metricsInfo = getContext().getMetricsInfo();
    metricsInfo.init(getServiceTags(clientAddress));

    // On a re-start of the coordinator it's possible that external compactions are in-progress.
    // Attempt to get the running compactions on the compactors and then resolve which tserver
    // the external compaction came from to re-populate the RUNNING collection.
    LOG.info("Checking for running external compactions");
    // On re-start contact the running Compactors to try and seed the list of running compactions
    List<RunningCompaction> running =
        ExternalCompactionUtil.getCompactionsRunningOnCompactors(getContext());
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

    tserverSet.startListeningForTabletServerChanges();
    startDeadCompactionDetector();
    startFailureSummaryLogging();

    LOG.info("Starting loop to check tservers for compaction summaries");
    while (!isShutdownRequested()) {
      if (Thread.currentThread().isInterrupted()) {
        LOG.info("Server process thread has been interrupted, shutting down");
        break;
      }
      try {
        long start = System.currentTimeMillis();

        updateSummaries();

        long now = System.currentTimeMillis();
        LOG.debug("Time spent checking compaction summaries: {}ms", (now - start));

        Map<String,List<HostAndPort>> idleCompactors = getIdleCompactors();
        TIME_COMPACTOR_LAST_CHECKED.forEach((queue, lastCheckTime) -> {
          if ((now - lastCheckTime) > getMissingCompactorWarningTime()
              && QUEUE_SUMMARIES.isCompactionsQueued(queue) && idleCompactors.containsKey(queue)) {
            LOG.warn("No compactors have checked in with coordinator for queue {} in {}ms", queue,
                getMissingCompactorWarningTime());
          }
        });

        long checkInterval = getTServerCheckInterval();
        long duration = (System.currentTimeMillis() - start);
        if (checkInterval - duration > 0) {
          LOG.debug("Waiting {}ms for next tserver check", (checkInterval - duration));
          Thread.sleep(checkInterval - duration);
        }
      } catch (InterruptedException e) {
        LOG.info("Interrupt Exception received, shutting down");
        gracefulShutdown(getContext().rpcCreds());
      }
    }

    LOG.debug("Stopping Thrift Servers");
    if (coordinatorAddress.server != null) {
      coordinatorAddress.server.stop();
    }
    getShutdownComplete().set(true);
    LOG.info("stop requested. exiting ... ");
    try {
      coordinatorLock.unlock();
    } catch (Exception e) {
      LOG.warn("Failed to release Coordinator lock", e);
    }

  }

  private Map<String,List<HostAndPort>> getIdleCompactors() {

    Map<String,List<HostAndPort>> allCompactors =
        ExternalCompactionUtil.getCompactorAddrs(getContext());

    Set<String> emptyQueues = new HashSet<>();

    // Remove all of the compactors that are running a compaction
    RUNNING_CACHE.values().forEach(rc -> {
      List<HostAndPort> busyCompactors = allCompactors.get(rc.getQueueName());
      if (busyCompactors != null
          && busyCompactors.remove(HostAndPort.fromString(rc.getCompactorAddress()))) {
        if (busyCompactors.isEmpty()) {
          emptyQueues.add(rc.getQueueName());
        }
      }
    });
    // Remove entries with empty queues
    emptyQueues.forEach(e -> allCompactors.remove(e));
    return allCompactors;
  }

  private void updateSummaries() {
    int maxThreads =
        Integer.parseInt(getConfiguration().get(COMPACTION_COORDINATOR_SUMMARIES_MAXTHREADS));
    ExecutorService executor =
        ThreadPools.getServerThreadPools().getPoolBuilder(COMPACTION_COORDINATOR_SUMMARY_POOL)
            .numCoreThreads(10).numMaxThreads(maxThreads).build();
    try {
      Set<String> queuesSeen = new ConcurrentSkipListSet<>();

      tserverSet.getCurrentServers().forEach(tsi -> {
        executor.execute(() -> updateSummaries(tsi, queuesSeen));
      });

      executor.shutdown();

      try {
        while (!executor.awaitTermination(1, TimeUnit.MINUTES)) {}
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new RuntimeException(e);
      }

      // remove any queues that were seen in the past, but were not seen in the latest gathering of
      // summaries
      TIME_COMPACTOR_LAST_CHECKED.keySet().retainAll(queuesSeen);

      // add any queues that were never seen before
      queuesSeen.forEach(q -> {
        TIME_COMPACTOR_LAST_CHECKED.computeIfAbsent(q, k -> System.currentTimeMillis());
      });
    } finally {
      executor.shutdownNow();
    }
  }

  private void updateSummaries(TServerInstance tsi, Set<String> queuesSeen) {
    try {
      TabletClientService.Client client = null;
      try {
        LOG.trace("Contacting tablet server {} to get external compaction summaries",
            tsi.getHostPort());
        client = getTabletServerConnection(tsi);
        if (client != null) {
          List<TCompactionQueueSummary> summaries =
              client.getCompactionQueueInfo(TraceUtil.traceInfo(), getContext().rpcCreds());
          QUEUE_SUMMARIES.update(tsi, summaries);
          summaries.forEach(summary -> {
            queuesSeen.add(summary.getQueue());
          });
        } else {
          LOG.trace("Connection to get summaries could not be established {} ",
              tsi.getHostAndPort());
          QUEUE_SUMMARIES.remove(Set.of(tsi));
        }
      } finally {
        ThriftUtil.returnClient(client, getContext());
      }
    } catch (TException e) {
      LOG.warn("Error getting external compaction summaries from tablet server: {}",
          tsi.getHostAndPort(), e);
      QUEUE_SUMMARIES.remove(Set.of(tsi));
    }
  }

  protected void startDeadCompactionDetector() {
    new DeadCompactionDetector(getContext(), this, getContext().getScheduledExecutor()).start();
  }

  protected long getMissingCompactorWarningTime() {
    return getConfiguration().getTimeInMillis(Property.COMPACTOR_MAX_JOB_WAIT_TIME) * 3;
  }

  protected long getTServerCheckInterval() {
    return getConfiguration()
        .getTimeInMillis(Property.COMPACTION_COORDINATOR_TSERVER_COMPACTION_CHECK_INTERVAL);
  }

  /**
   * Callback for the LiveTServerSet object to update current set of tablet servers, including ones
   * that were deleted and added
   *
   * @param current current set of live tservers
   * @param deleted set of tservers that were removed from current since last update
   * @param added set of tservers that were added to current since last update
   */
  @Override
  public void update(LiveTServerSet current, Set<TServerInstance> deleted,
      Set<TServerInstance> added) {

    // run() will iterate over the current and added tservers and add them to the internal
    // data structures. For tservers that are deleted, we need to remove them from QUEUES
    // and INDEX
    QUEUE_SUMMARIES.remove(deleted);
  }

  /**
   * Return the next compaction job from the queue to a Compactor
   *
   * @param queueName queue
   * @param compactorAddress compactor address
   * @throws ThriftSecurityException when permission error
   * @return compaction job
   */
  @Override
  public TNextCompactionJob getCompactionJob(TInfo tinfo, TCredentials credentials,
      String queueName, String compactorAddress, String externalCompactionId)
      throws ThriftSecurityException {

    // do not expect users to call this directly, expect compactors to call this method
    if (!security.canPerformSystemActions(credentials)) {
      throw new AccumuloSecurityException(credentials.getPrincipal(),
          SecurityErrorCode.PERMISSION_DENIED).asThriftException();
    }
    final String queue = queueName.intern();
    LOG.trace("getCompactionJob called for queue {} by compactor {}", queue, compactorAddress);
    TIME_COMPACTOR_LAST_CHECKED.put(queue, System.currentTimeMillis());

    TExternalCompactionJob result = null;

    PrioTserver prioTserver = QUEUE_SUMMARIES.getNextTserver(queue);

    while (prioTserver != null) {
      TServerInstance tserver = prioTserver.tserver;

      LOG.trace("Getting compaction for queue {} from tserver {}", queue, tserver.getHostAndPort());
      // Get a compaction from the tserver
      TabletClientService.Client client = null;
      try {
        client = getTabletServerConnection(tserver);
        if (client == null) {
          LOG.trace("No connection established for queue {} on tserver {}, trying next tserver",
              queue, tserver.getHostAndPort());
          QUEUE_SUMMARIES.removeSummary(tserver, queue, prioTserver.prio);
          prioTserver = QUEUE_SUMMARIES.getNextTserver(queue);
          continue;
        }

        TExternalCompactionJob job =
            client.reserveCompactionJob(TraceUtil.traceInfo(), getContext().rpcCreds(), queue,
                prioTserver.prio, compactorAddress, externalCompactionId);
        if (null == job.getExternalCompactionId()) {
          LOG.trace("No compactions found for queue {} on tserver {}, trying next tserver", queue,
              tserver.getHostAndPort());
          QUEUE_SUMMARIES.removeSummary(tserver, queue, prioTserver.prio);
          prioTserver = QUEUE_SUMMARIES.getNextTserver(queue);
          continue;
        }

        // It is possible that by the time this added that the tablet has already canceled the
        // compaction or the compactor that made this request is dead. In these cases the compaction
        // is not actually running.
        RUNNING_CACHE.put(ExternalCompactionId.of(job.getExternalCompactionId()),
            new RunningCompaction(job, compactorAddress, queue));
        LOG.debug("Returning external job {} to {}", job.externalCompactionId, compactorAddress);
        result = job;
        break;
      } catch (TException e) {
        LOG.warn("Error from tserver {} while trying to reserve compaction, trying next tserver",
            ExternalCompactionUtil.getHostPortString(tserver.getHostAndPort()), e);
        QUEUE_SUMMARIES.removeSummary(tserver, queue, prioTserver.prio);
        prioTserver = QUEUE_SUMMARIES.getNextTserver(queue);
      } finally {
        ThriftUtil.returnClient(client, getContext());
      }
    }

    if (result == null) {
      LOG.trace("No tservers found for queue {}, returning empty job to compactor {}", queue,
          compactorAddress);
      result = new TExternalCompactionJob();
    }

    return new TNextCompactionJob(result, compactorCounts.get(queue));

  }

  /**
   * Return the Thrift client for the TServer
   *
   * @param tserver tserver instance
   * @return thrift client
   * @throws TTransportException thrift error
   */
  protected TabletClientService.Client getTabletServerConnection(TServerInstance tserver)
      throws TTransportException {
    TServerConnection connection = tserverSet.getConnection(tserver);
    if (connection == null) {
      return null;
    }
    ServerContext serverContext = getContext();
    TTransport transport = serverContext.getTransportPool().getTransport(
        ThriftClientTypes.TABLET_SERVER, connection.getAddress(), 0, serverContext, true);
    return ThriftUtil.createClient(ThriftClientTypes.TABLET_SERVER, transport);
  }

  /**
   * Compactor calls compactionCompleted passing in the CompactionStats
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
    LOG.debug("Compaction completed, id: {}, stats: {}, extent: {}", externalCompactionId, stats,
        extent);
    final var ecid = ExternalCompactionId.of(externalCompactionId);
    captureSuccess(ecid, extent);
    compactionFinalizer.commitCompaction(ecid, extent, stats.fileSize, stats.entriesWritten);
    // It's possible that RUNNING might not have an entry for this ecid in the case
    // of a coordinator restart when the Coordinator can't find the TServer for the
    // corresponding external compaction.
    recordCompletion(ecid);
  }

  @Override
  public void compactionFailed(TInfo tinfo, TCredentials credentials, String externalCompactionId,
      TKeyExtent extent, String exceptionClassName) throws ThriftSecurityException {
    // do not expect users to call this directly, expect other tservers to call this method
    if (!security.canPerformSystemActions(credentials)) {
      throw new AccumuloSecurityException(credentials.getPrincipal(),
          SecurityErrorCode.PERMISSION_DENIED).asThriftException();
    }
    KeyExtent fromThriftExtent = KeyExtent.fromThrift(extent);
    LOG.info("Compaction failed: id: {}, extent: {}, compactor exception:{}", externalCompactionId,
        fromThriftExtent, exceptionClassName);
    final var ecid = ExternalCompactionId.of(externalCompactionId);
    if (exceptionClassName != null) {
      captureFailure(ecid, fromThriftExtent);
    }
    compactionFailed(Map.of(ecid, KeyExtent.fromThrift(extent)));
  }

  private void captureFailure(ExternalCompactionId ecid, KeyExtent extent) {
    var rc = RUNNING_CACHE.get(ecid);
    if (rc != null) {
      final String queue = rc.getQueueName();
      failingQueues.computeIfAbsent(queue, q -> new AtomicLong(0)).incrementAndGet();
      final String compactor = rc.getCompactorAddress();
      failingCompactors.computeIfAbsent(compactor, c -> new AtomicLong(0)).incrementAndGet();
    }
    failingTables.computeIfAbsent(extent.tableId(), t -> new AtomicLong(0)).incrementAndGet();
  }

  protected void startFailureSummaryLogging() {
    ScheduledFuture<?> future = getContext().getScheduledExecutor()
        .scheduleWithFixedDelay(this::printFailures, 0, 5, TimeUnit.MINUTES);
    ThreadPools.watchNonCriticalScheduledTask(future);
  }

  private void printFailures() {

    // Remove down compactors from failing list
    Map<String,List<HostAndPort>> allCompactors =
        ExternalCompactionUtil.getCompactorAddrs(getContext());
    Set<String> allCompactorAddrs = new HashSet<>();
    allCompactors.values().forEach(l -> l.forEach(c -> allCompactorAddrs.add(c.toString())));
    failingCompactors.keySet().retainAll(allCompactorAddrs);

    LOG.warn("Compaction failure summary:");
    LOG.warn("Queue failures: {}", failingQueues);
    LOG.warn("Table failures: {}", failingTables);
    LOG.warn("Compactor failures: {}", failingCompactors);
  }

  private void captureSuccess(ExternalCompactionId ecid, KeyExtent extent) {
    var rc = RUNNING_CACHE.get(ecid);
    if (rc != null) {
      final String queue = rc.getQueueName();
      failingQueues.computeIfPresent(queue, (q, curr) -> {
        long currentValue = Math.max(0L, curr.get() - 1);
        if (currentValue == 0) {
          return null; // remove from the map
        } else {
          curr.set(currentValue);
          return curr;
        }
      });
      final String compactor = rc.getCompactorAddress();
      failingCompactors.computeIfPresent(compactor, (c, curr) -> {
        long currentValue = Math.max(0L, curr.get() - 1);
        if (currentValue == 0) {
          return null; // remove from the map
        } else {
          curr.set(currentValue);
          return curr;
        }
      });
    }
    failingTables.computeIfPresent(extent.tableId(), (t, curr) -> {
      long currentValue = Math.max(0L, curr.get() - 1);
      if (currentValue == 0) {
        return null; // remove from the map
      } else {
        curr.set(currentValue);
        return curr;
      }
    });
  }

  void compactionFailed(Map<ExternalCompactionId,KeyExtent> compactions) {
    compactionFinalizer.failCompactions(compactions);
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
    STATUS_LOG.trace("Compaction status update, id: {}, timestamp: {}, update: {}",
        externalCompactionId, timestamp, update);
    final RunningCompaction rc = RUNNING_CACHE.get(ExternalCompactionId.of(externalCompactionId));
    if (null != rc) {
      rc.addUpdate(timestamp, update);
    }
  }

  private void recordCompletion(ExternalCompactionId ecid) {
    var rc = RUNNING_CACHE.remove(ecid);
    if (rc != null) {
      COMPLETED.put(ecid, rc);
    }
  }

  protected Set<ExternalCompactionId> readExternalCompactionIds() {
    return getContext().getAmple().readTablets().forLevel(Ample.DataLevel.USER)
        .fetch(TabletMetadata.ColumnType.ECOMP).build().stream()
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
    LOG.debug("Current ECIDs in metadata: {}", idsInMetadata.size());
    LOG.debug("Current ECIDs in running cache: {}", idsSnapshot.size());

    var idsToRemove = Sets.difference(idsSnapshot, idsInMetadata);

    // remove ids that are in the running set but not in the metadata table
    idsToRemove.forEach(this::recordCompletion);

    if (!idsToRemove.isEmpty()) {
      LOG.debug("Removed {} stale entries from RUNNING_CACHE", idsToRemove.size());
      if (LOG.isTraceEnabled()) {
        idsToRemove.forEach(ecid -> LOG.trace("Removing stale entry: {} from RUNNING_CACHE", ecid));
      }
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
      trc.setQueueName(rc.getQueueName());
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
    COMPLETED.asMap().forEach((ecid, rc) -> {
      TExternalCompaction trc = new TExternalCompaction();
      trc.setQueueName(rc.getQueueName());
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
      NamespaceId nsId = getContext().getNamespaceId(extent.tableId());
      if (!security.canCompact(credentials, extent.tableId(), nsId)) {
        throw new AccumuloSecurityException(credentials.getPrincipal(),
            SecurityErrorCode.PERMISSION_DENIED).asThriftException();
      }
    } catch (TableNotFoundException e) {
      throw new ThriftTableOperationException(extent.tableId().canonical(), null,
          TableOperation.COMPACT_CANCEL, TableOperationExceptionType.NOTFOUND, e.getMessage());
    }

    HostAndPort address = HostAndPort.fromString(runningCompaction.getCompactorAddress());
    ExternalCompactionUtil.cancelCompaction(getContext(), address, externalCompactionId);
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
    final String compactorQueuesPath = getContext().getZooKeeperRoot() + Constants.ZCOMPACTORS;

    var zoorw = getContext().getZooReaderWriter();

    try {
      var queues = zoorw.getChildren(compactorQueuesPath);

      for (String queue : queues) {
        String qpath = compactorQueuesPath + "/" + queue;

        var compactors = zoorw.getChildren(qpath);

        if (compactors.isEmpty()) {
          deleteEmpty(zoorw, qpath);
        }

        for (String compactor : compactors) {
          String cpath = compactorQueuesPath + "/" + queue + "/" + compactor;
          var lockNodes = zoorw.getChildren(compactorQueuesPath + "/" + queue + "/" + compactor);
          if (lockNodes.isEmpty()) {
            deleteEmpty(zoorw, cpath);
          }
        }
      }

    } catch (KeeperException | RuntimeException e) {
      LOG.warn("Failed to clean up compactors", e);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(e);
    }
  }

  @Override
  public ServiceLock getLock() {
    return coordinatorLock;
  }

  public static void main(String[] args) throws Exception {
    try (CompactionCoordinator compactor = new CompactionCoordinator(new ServerOpts(), args)) {
      compactor.runServer();
    }
  }

}

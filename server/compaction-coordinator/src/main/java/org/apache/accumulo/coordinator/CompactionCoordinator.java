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
package org.apache.accumulo.coordinator;

import static org.apache.accumulo.fate.util.UtilWaitThread.sleepUninterruptibly;

import java.net.UnknownHostException;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.coordinator.QueueSummaries.PrioTserver;
import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.clientImpl.ThriftTransportPool;
import org.apache.accumulo.core.clientImpl.thrift.SecurityErrorCode;
import org.apache.accumulo.core.clientImpl.thrift.ThriftSecurityException;
import org.apache.accumulo.core.compaction.thrift.CompactionCoordinatorService;
import org.apache.accumulo.core.compaction.thrift.CompactionCoordinatorService.Iface;
import org.apache.accumulo.core.compaction.thrift.TCompactionState;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.dataImpl.thrift.TKeyExtent;
import org.apache.accumulo.core.metadata.TServerInstance;
import org.apache.accumulo.core.metadata.schema.ExternalCompactionId;
import org.apache.accumulo.core.rpc.ThriftUtil;
import org.apache.accumulo.core.securityImpl.thrift.TCredentials;
import org.apache.accumulo.core.tabletserver.thrift.TCompactionQueueSummary;
import org.apache.accumulo.core.tabletserver.thrift.TCompactionStats;
import org.apache.accumulo.core.tabletserver.thrift.TExternalCompactionJob;
import org.apache.accumulo.core.tabletserver.thrift.TabletClientService;
import org.apache.accumulo.core.trace.TraceUtil;
import org.apache.accumulo.core.trace.thrift.TInfo;
import org.apache.accumulo.core.util.HostAndPort;
import org.apache.accumulo.core.util.compaction.ExternalCompactionUtil;
import org.apache.accumulo.core.util.threads.ThreadPools;
import org.apache.accumulo.fate.util.UtilWaitThread;
import org.apache.accumulo.fate.zookeeper.ServiceLock;
import org.apache.accumulo.fate.zookeeper.ZooReaderWriter;
import org.apache.accumulo.server.AbstractServer;
import org.apache.accumulo.server.GarbageCollectionLogger;
import org.apache.accumulo.server.ServerOpts;
import org.apache.accumulo.server.manager.LiveTServerSet;
import org.apache.accumulo.server.manager.LiveTServerSet.TServerConnection;
import org.apache.accumulo.server.rpc.ServerAddress;
import org.apache.accumulo.server.rpc.TCredentialsUpdatingWrapper;
import org.apache.accumulo.server.rpc.TServerUtils;
import org.apache.accumulo.server.rpc.ThriftServerType;
import org.apache.accumulo.server.security.AuditedSecurityOperation;
import org.apache.accumulo.server.security.SecurityOperation;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CompactionCoordinator extends AbstractServer
    implements CompactionCoordinatorService.Iface, LiveTServerSet.Listener {

  private static final Logger LOG = LoggerFactory.getLogger(CompactionCoordinator.class);
  private static final long TIME_BETWEEN_GC_CHECKS = 5000;
  private static final long FIFTEEN_MINUTES =
      TimeUnit.MILLISECONDS.convert(Duration.of(15, TimeUnit.MINUTES.toChronoUnit()));

  protected static final QueueSummaries QUEUE_SUMMARIES = new QueueSummaries();

  /* Map of compactionId to RunningCompactions */
  protected static final Map<ExternalCompactionId,RunningCompaction> RUNNING =
      new ConcurrentHashMap<>();

  /* Map of queue name to last time compactor called to get a compaction job */
  private static final Map<String,Long> TIME_COMPACTOR_LAST_CHECKED = new ConcurrentHashMap<>();

  private final GarbageCollectionLogger gcLogger = new GarbageCollectionLogger();
  protected SecurityOperation security;
  protected final AccumuloConfiguration aconf;
  protected CompactionFinalizer compactionFinalizer;
  protected LiveTServerSet tserverSet;

  private ServiceLock coordinatorLock;

  // Exposed for tests
  protected volatile Boolean shutdown = false;

  private ScheduledThreadPoolExecutor schedExecutor;

  protected CompactionCoordinator(ServerOpts opts, String[] args) {
    super("compaction-coordinator", opts, args);
    aconf = getConfiguration();
    schedExecutor = ThreadPools.createGeneralScheduledExecutorService(aconf);
    compactionFinalizer = createCompactionFinalizer(schedExecutor);
    tserverSet = createLiveTServerSet();
    setupSecurity();
    startGCLogger(schedExecutor);
    printStartupMsg();
    startCompactionCleaner(schedExecutor);
  }

  protected CompactionCoordinator(ServerOpts opts, String[] args, AccumuloConfiguration conf) {
    super("compaction-coordinator", opts, args);
    aconf = conf;
    schedExecutor = ThreadPools.createGeneralScheduledExecutorService(aconf);
    compactionFinalizer = createCompactionFinalizer(schedExecutor);
    tserverSet = createLiveTServerSet();
    setupSecurity();
    startGCLogger(schedExecutor);
    printStartupMsg();
    startCompactionCleaner(schedExecutor);
  }

  protected CompactionFinalizer
      createCompactionFinalizer(ScheduledThreadPoolExecutor schedExecutor) {
    return new CompactionFinalizer(getContext(), schedExecutor);
  }

  protected LiveTServerSet createLiveTServerSet() {
    return new LiveTServerSet(getContext(), this);
  }

  protected void setupSecurity() {
    security = AuditedSecurityOperation.getInstance(getContext());
  }

  protected void startGCLogger(ScheduledThreadPoolExecutor schedExecutor) {
    schedExecutor.scheduleWithFixedDelay(() -> gcLogger.logGCInfo(getConfiguration()), 0,
        TIME_BETWEEN_GC_CHECKS, TimeUnit.MILLISECONDS);
  }

  protected void startCompactionCleaner(ScheduledThreadPoolExecutor schedExecutor) {
    schedExecutor.scheduleWithFixedDelay(() -> cleanUpCompactors(), 0, 5, TimeUnit.MINUTES);
  }

  protected void printStartupMsg() {
    LOG.info("Version " + Constants.VERSION);
    LOG.info("Instance " + getContext().getInstanceID());
  }

  /**
   * Set up nodes and locks in ZooKeeper for this CompactionCoordinator
   *
   * @param clientAddress
   *          address of this Compactor
   * @throws KeeperException
   *           zookeeper error
   * @throws InterruptedException
   *           thread interrupted
   */
  protected void getCoordinatorLock(HostAndPort clientAddress)
      throws KeeperException, InterruptedException {
    LOG.info("trying to get coordinator lock");

    final String coordinatorClientAddress = ExternalCompactionUtil.getHostPortString(clientAddress);
    final String lockPath = getContext().getZooKeeperRoot() + Constants.ZCOORDINATOR_LOCK;
    final UUID zooLockUUID = UUID.randomUUID();

    while (true) {

      CoordinatorLockWatcher coordinatorLockWatcher = new CoordinatorLockWatcher();
      coordinatorLock = new ServiceLock(getContext().getZooReaderWriter().getZooKeeper(),
          ServiceLock.path(lockPath), zooLockUUID);
      coordinatorLock.lock(coordinatorLockWatcher, coordinatorClientAddress.getBytes());

      coordinatorLockWatcher.waitForChange();
      if (coordinatorLockWatcher.isAcquiredLock()) {
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
   * @throws UnknownHostException
   *           host unknown
   */
  protected ServerAddress startCoordinatorClientService() throws UnknownHostException {
    Iface rpcProxy = TraceUtil.wrapService(this);
    if (getContext().getThriftServerType() == ThriftServerType.SASL) {
      rpcProxy = TCredentialsUpdatingWrapper.service(rpcProxy, CompactionCoordinator.class,
          getConfiguration());
    }
    final CompactionCoordinatorService.Processor<Iface> processor =
        new CompactionCoordinatorService.Processor<>(rpcProxy);
    Property maxMessageSizeProperty =
        (aconf.get(Property.COMPACTION_COORDINATOR_MAX_MESSAGE_SIZE) != null
            ? Property.COMPACTION_COORDINATOR_MAX_MESSAGE_SIZE : Property.GENERAL_MAX_MESSAGE_SIZE);
    ServerAddress sp = TServerUtils.startServer(getMetricsSystem(), getContext(), getHostname(),
        Property.COMPACTION_COORDINATOR_CLIENTPORT, processor, this.getClass().getSimpleName(),
        "Thrift Client Server", Property.COMPACTION_COORDINATOR_THRIFTCLIENT_PORTSEARCH,
        Property.COMPACTION_COORDINATOR_MINTHREADS,
        Property.COMPACTION_COORDINATOR_MINTHREADS_TIMEOUT,
        Property.COMPACTION_COORDINATOR_THREADCHECK, maxMessageSizeProperty);
    LOG.info("address = {}", sp.address);
    return sp;
  }

  @Override
  public void run() {

    ServerAddress coordinatorAddress = null;
    try {
      coordinatorAddress = startCoordinatorClientService();
    } catch (UnknownHostException e1) {
      throw new RuntimeException("Failed to start the coordinator service", e1);
    }
    final HostAndPort clientAddress = coordinatorAddress.address;

    try {
      getCoordinatorLock(clientAddress);
    } catch (KeeperException | InterruptedException e) {
      throw new IllegalStateException("Exception getting Coordinator lock", e);
    }

    // On a re-start of the coordinator it's possible that external compactions are in-progress.
    // Attempt to get the running compactions on the compactors and then resolve which tserver
    // the external compaction came from to re-populate the RUNNING collection.
    LOG.info("Checking for running external compactions");
    // On re-start contact the running Compactors to try and seed the list of running compactions
    Map<HostAndPort,TExternalCompactionJob> running =
        ExternalCompactionUtil.getCompactionsRunningOnCompactors(getContext());
    if (running.isEmpty()) {
      LOG.info("No running external compactions found");
    } else {
      LOG.info("Found {} running external compactions", running.size());
      running.forEach((hp, job) -> {
        RUNNING.put(ExternalCompactionId.of(job.getExternalCompactionId()),
            new RunningCompaction(job, ExternalCompactionUtil.getHostPortString(hp)));
      });
    }

    tserverSet.startListeningForTabletServerChanges();
    startDeadCompactionDetector();

    LOG.info("Starting loop to check tservers for compaction summaries");
    while (!shutdown) {
      long start = System.currentTimeMillis();

      updateSummaries();

      long now = System.currentTimeMillis();
      TIME_COMPACTOR_LAST_CHECKED.forEach((k, v) -> {
        if ((now - v) > getMissingCompactorWarningTime()) {
          LOG.warn("No compactors have checked in with coordinator for queue {} in {}ms", k,
              getMissingCompactorWarningTime());
        }
      });

      long checkInterval = getTServerCheckInterval();
      long duration = (System.currentTimeMillis() - start);
      if (checkInterval - duration > 0) {
        LOG.debug("Waiting {}ms for next tserver check", (checkInterval - duration));
        UtilWaitThread.sleep(checkInterval - duration);
      }
    }

    LOG.info("Shutting down");
  }

  private void updateSummaries() {
    ExecutorService executor =
        ThreadPools.createFixedThreadPool(10, "Compaction Summary Gatherer", false);
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
        LOG.debug("Contacting tablet server {} to get external compaction summaries",
            tsi.getHostPort());
        client = getTabletServerConnection(tsi);
        List<TCompactionQueueSummary> summaries =
            client.getCompactionQueueInfo(TraceUtil.traceInfo(), getContext().rpcCreds());
        QUEUE_SUMMARIES.update(tsi, summaries);
        summaries.forEach(summary -> {
          queuesSeen.add(summary.getQueue());
        });
      } finally {
        ThriftUtil.returnClient(client);
      }
    } catch (TException e) {
      LOG.warn("Error getting external compaction summaries from tablet server: {}",
          tsi.getHostAndPort(), e);
      QUEUE_SUMMARIES.remove(Set.of(tsi));
    }
  }

  protected void startDeadCompactionDetector() {
    new DeadCompactionDetector(getContext(), this, schedExecutor).start();
  }

  protected long getMissingCompactorWarningTime() {
    return FIFTEEN_MINUTES;
  }

  protected long getTServerCheckInterval() {
    return this.aconf
        .getTimeInMillis(Property.COMPACTION_COORDINATOR_TSERVER_COMPACTION_CHECK_INTERVAL);
  }

  /**
   * Callback for the LiveTServerSet object to update current set of tablet servers, including ones
   * that were deleted and added
   *
   * @param current
   *          current set of live tservers
   * @param deleted
   *          set of tservers that were removed from current since last update
   * @param added
   *          set of tservers that were added to current since last update
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
   * @param queueName
   *          queue
   * @param compactorAddress
   *          compactor address
   * @throws ThriftSecurityException
   *           when permission error
   * @return compaction job
   */
  @Override
  public TExternalCompactionJob getCompactionJob(TInfo tinfo, TCredentials credentials,
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

    PrioTserver prioTserver = QUEUE_SUMMARIES.getNextTserver(queueName);

    while (prioTserver != null) {
      TServerInstance tserver = prioTserver.tserver;

      LOG.trace("Getting compaction for queue {} from tserver {}", queue, tserver.getHostAndPort());
      // Get a compaction from the tserver
      TabletClientService.Client client = null;
      try {
        client = getTabletServerConnection(tserver);
        TExternalCompactionJob job =
            client.reserveCompactionJob(TraceUtil.traceInfo(), getContext().rpcCreds(), queue,
                prioTserver.prio, compactorAddress, externalCompactionId);
        if (null == job.getExternalCompactionId()) {
          LOG.trace("No compactions found for queue {} on tserver {}, trying next tserver", queue,
              tserver.getHostAndPort(), compactorAddress);

          QUEUE_SUMMARIES.removeSummary(tserver, queueName, prioTserver.prio);
          prioTserver = QUEUE_SUMMARIES.getNextTserver(queueName);
          continue;
        }
        RUNNING.put(ExternalCompactionId.of(job.getExternalCompactionId()),
            new RunningCompaction(job, compactorAddress));
        LOG.debug("Returning external job {} to {}", job.externalCompactionId, compactorAddress);
        result = job;
        break;
      } catch (TException e) {
        LOG.warn("Error from tserver {} while trying to reserve compaction, trying next tserver",
            ExternalCompactionUtil.getHostPortString(tserver.getHostAndPort()), e);
        QUEUE_SUMMARIES.removeSummary(tserver, queueName, prioTserver.prio);
        prioTserver = QUEUE_SUMMARIES.getNextTserver(queueName);
      } finally {
        ThriftUtil.returnClient(client);
      }
    }

    if (result == null) {
      LOG.trace("No tservers found for queue {}, returning empty job to compactor {}", queue,
          compactorAddress);
      result = new TExternalCompactionJob();
    }

    return result;

  }

  /**
   * Return the Thrift client for the TServer
   *
   * @param tserver
   *          tserver instance
   * @return thrift client
   * @throws TTransportException
   *           thrift error
   */
  protected TabletClientService.Client getTabletServerConnection(TServerInstance tserver)
      throws TTransportException {
    TServerConnection connection = tserverSet.getConnection(tserver);
    TTransport transport =
        ThriftTransportPool.getInstance().getTransport(connection.getAddress(), 0, getContext());
    return ThriftUtil.createClient(new TabletClientService.Client.Factory(), transport);
  }

  /**
   * Compactor calls compactionCompleted passing in the CompactionStats
   *
   * @param tinfo
   *          trace info
   * @param credentials
   *          tcredentials object
   * @param externalCompactionId
   *          compaction id
   * @param textent
   *          tablet extent
   * @param stats
   *          compaction stats
   * @throws ThriftSecurityException
   *           when permission error
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
    compactionFinalizer.commitCompaction(ecid, extent, stats.fileSize, stats.entriesWritten);
    // It's possible that RUNNING might not have an entry for this ecid in the case
    // of a coordinator restart when the Coordinator can't find the TServer for the
    // corresponding external compaction.
    final RunningCompaction rc = RUNNING.get(ecid);
    if (null != rc) {
      RUNNING.remove(ecid, rc);
    } else {
      LOG.warn(
          "Compaction completed called by Compactor for {}, but no running compaction for that id.",
          externalCompactionId);
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
    compactionFinalizer.failCompactions(compactions);
    compactions.forEach((k, v) -> {
      final RunningCompaction rc = RUNNING.get(k);
      if (null != rc) {
        RUNNING.remove(k, rc);
      } else {
        LOG.warn(
            "Compaction failed called by Compactor for {}, but no running compaction for that id.",
            k);
      }
    });
  }

  /**
   * Compactor calls to update the status of the assigned compaction
   *
   * @param tinfo
   *          trace info
   * @param credentials
   *          tcredentials object
   * @param externalCompactionId
   *          compaction id
   * @param state
   *          compaction state
   * @param message
   *          informational message
   * @param timestamp
   *          timestamp of the message
   * @throws ThriftSecurityException
   *           when permission error
   */
  @Override
  public void updateCompactionStatus(TInfo tinfo, TCredentials credentials,
      String externalCompactionId, TCompactionState state, String message, long timestamp)
      throws ThriftSecurityException {
    // do not expect users to call this directly, expect other tservers to call this method
    if (!security.canPerformSystemActions(credentials)) {
      throw new AccumuloSecurityException(credentials.getPrincipal(),
          SecurityErrorCode.PERMISSION_DENIED).asThriftException();
    }
    LOG.debug("Compaction status update, id: {}, timestamp: {}, state: {}, message: {}",
        externalCompactionId, timestamp, state, message);
    final RunningCompaction rc = RUNNING.get(ExternalCompactionId.of(externalCompactionId));
    if (null != rc) {
      rc.addUpdate(timestamp, message, state);
    }
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

  public static void main(String[] args) throws Exception {
    try (CompactionCoordinator compactor = new CompactionCoordinator(new ServerOpts(), args)) {
      compactor.runServer();
    }
  }

}

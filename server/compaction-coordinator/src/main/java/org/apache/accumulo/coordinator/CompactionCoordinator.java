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
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.coordinator.QueueSummaries.PrioTserver;
import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.clientImpl.ThriftTransportPool;
import org.apache.accumulo.core.clientImpl.thrift.SecurityErrorCode;
import org.apache.accumulo.core.compaction.thrift.CompactionCoordinator.Iface;
import org.apache.accumulo.core.compaction.thrift.Compactor;
import org.apache.accumulo.core.compaction.thrift.TCompactionState;
import org.apache.accumulo.core.compaction.thrift.UnknownCompactionIdException;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.dataImpl.thrift.TKeyExtent;
import org.apache.accumulo.core.metadata.TServerInstance;
import org.apache.accumulo.core.metadata.schema.ExternalCompactionId;
import org.apache.accumulo.core.metadata.schema.TabletMetadata;
import org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType;
import org.apache.accumulo.core.metadata.schema.TabletMetadata.LocationType;
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
import org.apache.accumulo.server.compaction.RetryableThriftCall;
import org.apache.accumulo.server.compaction.RetryableThriftCall.RetriesExceededException;
import org.apache.accumulo.server.compaction.RetryableThriftFunction;
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
    implements org.apache.accumulo.core.compaction.thrift.CompactionCoordinator.Iface,
    LiveTServerSet.Listener {

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
    getContext().setupCrypto();
    security = AuditedSecurityOperation.getInstance(getContext());
  }

  protected void startGCLogger(ScheduledThreadPoolExecutor schedExecutor) {
    schedExecutor.scheduleWithFixedDelay(() -> gcLogger.logGCInfo(getConfiguration()), 0,
        TIME_BETWEEN_GC_CHECKS, TimeUnit.MILLISECONDS);
  }

  private void startCompactionCleaner(ScheduledThreadPoolExecutor schedExecutor) {
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
    final org.apache.accumulo.core.compaction.thrift.CompactionCoordinator.Processor<
        Iface> processor =
            new org.apache.accumulo.core.compaction.thrift.CompactionCoordinator.Processor<>(
                rpcProxy);
    Property maxMessageSizeProperty =
        (aconf.get(Property.COORDINATOR_THRIFTCLIENT_MAX_MESSAGE_SIZE) != null
            ? Property.COORDINATOR_THRIFTCLIENT_MAX_MESSAGE_SIZE
            : Property.GENERAL_MAX_MESSAGE_SIZE);
    ServerAddress sp = TServerUtils.startServer(getMetricsSystem(), getContext(), getHostname(),
        Property.COORDINATOR_THRIFTCLIENT_CLIENTPORT, processor, this.getClass().getSimpleName(),
        "Thrift Client Server", Property.COORDINATOR_THRIFTCLIENT_PORTSEARCH,
        Property.COORDINATOR_THRIFTCLIENT_MINTHREADS,
        Property.COORDINATOR_THRIFTCLIENT_MINTHREADS_TIMEOUT,
        Property.COORDINATOR_THRIFTCLIENT_THREADCHECK, maxMessageSizeProperty);
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
    tserverSet.scanServers();
    final Set<TServerInstance> tservers = tserverSet.getCurrentServers();
    if (null != tservers && !tservers.isEmpty()) {
      // On re-start contact the running Compactors to try and seed the list of running compactions
      Map<HostAndPort,TExternalCompactionJob> running =
          ExternalCompactionUtil.getCompactionsRunningOnCompactors(getContext());
      if (running.isEmpty()) {
        LOG.info("No compactions running on Compactors.");
      } else {
        LOG.info("Found {} running external compactions", running.size());
        running.forEach((hp, job) -> {
          // Find the tserver that has this compaction id
          boolean matchFound = false;

          // Attempt to find the TServer hosting the tablet based on the metadata table
          // TODO use #1974 for more efficient metadata reads
          KeyExtent extent = KeyExtent.fromThrift(job.getExtent());
          LOG.debug("Getting tablet metadata for extent: {}", extent);
          TabletMetadata tabletMetadata = getMetadataEntryForExtent(extent);

          if (tabletMetadata != null && tabletMetadata.getExtent().equals(extent)
              && tabletMetadata.getLocation() != null
              && tabletMetadata.getLocation().getType() == LocationType.CURRENT) {

            TServerInstance tsi = tservers.stream()
                .filter(
                    t -> t.getHostAndPort().equals(tabletMetadata.getLocation().getHostAndPort()))
                .findFirst().orElse(null);

            if (null != tsi) {
              TabletClientService.Client client = null;
              try {
                LOG.debug(
                    "Checking to see if tserver {} is running external compaction for extent: {}",
                    tsi.getHostAndPort(), extent);
                client = getTabletServerConnection(tsi);
                boolean tserverMatch = client.isRunningExternalCompaction(TraceUtil.traceInfo(),
                    getContext().rpcCreds(), job.getExternalCompactionId(), job.getExtent());
                if (tserverMatch) {
                  LOG.debug(
                      "Tablet server {} is running external compaction for extent: {}, adding to running list",
                      tsi.getHostAndPort(), extent);
                  RUNNING.put(ExternalCompactionId.of(job.getExternalCompactionId()),
                      new RunningCompaction(job, ExternalCompactionUtil.getHostPortString(hp),
                          tsi));
                  matchFound = true;
                } else {
                  LOG.debug("Tablet server {} is NOT running external compaction for extent: {}",
                      tsi.getHostAndPort(), extent);
                }
              } catch (TException e) {
                LOG.warn("Failed to notify tserver {}",
                    tabletMetadata.getLocation().getHostAndPort(), e);
              } finally {
                ThriftUtil.returnClient(client);
              }
            } else {
              LOG.info("Tablet server {} is not currently in live tserver set",
                  tabletMetadata.getLocation().getHostAndPort());
            }
          } else {
            LOG.info("No current location for extent: {}", extent);
          }

          // As a fallback, try them all
          if (!matchFound) {
            LOG.debug("Checking all tservers for external running compaction, extent: {}", extent);
            for (TServerInstance tsi : tservers) {
              TabletClientService.Client client = null;
              try {
                client = getTabletServerConnection(tsi);
                LOG.debug(
                    "Checking to see if tserver {} is running external compaction for extent: {}",
                    tsi.getHostAndPort(), extent);
                boolean tserverMatch = client.isRunningExternalCompaction(TraceUtil.traceInfo(),
                    getContext().rpcCreds(), job.getExternalCompactionId(), job.getExtent());
                if (tserverMatch) {
                  LOG.debug(
                      "Tablet server {} is running external compaction for extent: {}, adding to running list",
                      tsi.getHostAndPort(), extent);
                  RUNNING.put(ExternalCompactionId.of(job.getExternalCompactionId()),
                      new RunningCompaction(job, ExternalCompactionUtil.getHostPortString(hp),
                          tsi));
                  matchFound = true;
                }
              } catch (TException e) {
                LOG.error(
                    "Error from tserver {} while trying to check if external compaction is running, trying next tserver",
                    ExternalCompactionUtil.getHostPortString(tsi.getHostAndPort()), e);
              } finally {
                ThriftUtil.returnClient(client);
              }
            }
          }

          if (!matchFound) {
            LOG.warn(
                "There is an external compaction running on a compactor, but could not find corresponding tablet server. Extent: {}, Compactor: {}, Compaction: {}",
                extent, hp, job);
          }
        });
      }
      tservers.clear();
    } else {
      LOG.info("No running tablet servers found, continuing startup");
    }

    tserverSet.startListeningForTabletServerChanges();
    startDeadCompactionDetector();

    LOG.info("Starting loop to check tservers for compaction summaries");
    while (!shutdown) {
      long start = System.currentTimeMillis();
      tserverSet.getCurrentServers().forEach(tsi -> {
        try {
          TabletClientService.Client client = null;
          try {
            LOG.debug("Contacting tablet server {} to get external compaction summaries",
                tsi.getHostPort());
            client = getTabletServerConnection(tsi);
            List<TCompactionQueueSummary> summaries =
                client.getCompactionQueueInfo(TraceUtil.traceInfo(), getContext().rpcCreds());
            summaries.forEach(summary -> {
              QueueAndPriority qp =
                  QueueAndPriority.get(summary.getQueue().intern(), summary.getPriority());
              synchronized (qp) {
                TIME_COMPACTOR_LAST_CHECKED.computeIfAbsent(qp.getQueue(), k -> 0L);
                QUEUE_SUMMARIES.update(tsi, summaries);
              }
            });
          } finally {
            ThriftUtil.returnClient(client);
          }
        } catch (TException e) {
          LOG.warn("Error getting external compaction summaries from tablet server: {}",
              tsi.getHostAndPort(), e);
          QUEUE_SUMMARIES.remove(Set.of(tsi));
        }
      });

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

  protected void startDeadCompactionDetector() {
    new DeadCompactionDetector(getContext(), compactionFinalizer, schedExecutor).start();
  }

  protected long getMissingCompactorWarningTime() {
    return FIFTEEN_MINUTES;
  }

  protected long getTServerCheckInterval() {
    return this.aconf.getTimeInMillis(Property.COORDINATOR_TSERVER_COMPACTION_CHECK_INTERVAL);
  }

  protected TabletMetadata getMetadataEntryForExtent(KeyExtent extent) {
    return getContext().getAmple().readTablets().forTablet(extent)
        .fetch(ColumnType.LOCATION, ColumnType.PREV_ROW).build().stream().findFirst().orElse(null);
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
   * @return compaction job
   */
  @Override
  public TExternalCompactionJob getCompactionJob(TInfo tinfo, TCredentials credentials,
      String queueName, String compactorAddress, String externalCompactionId) throws TException {

    // do not expect users to call this directly, expect compactors to call this method
    if (!security.canPerformSystemActions(credentials)) {
      throw new AccumuloSecurityException(credentials.getPrincipal(),
          SecurityErrorCode.PERMISSION_DENIED).asThriftException();
    }
    final String queue = queueName.intern();
    LOG.debug("getCompactionJob called for queue {} by compactor {}", queue, compactorAddress);
    TIME_COMPACTOR_LAST_CHECKED.put(queue, System.currentTimeMillis());

    TExternalCompactionJob result = null;

    PrioTserver prioTserver = QUEUE_SUMMARIES.getNextTserver(queueName);

    while (prioTserver != null) {
      TServerInstance tserver = prioTserver.tserver;

      LOG.debug("Getting compaction for queue {} from tserver {}", queue, tserver.getHostAndPort());
      // Get a compaction from the tserver
      TabletClientService.Client client = null;
      try {
        client = getTabletServerConnection(tserver);
        TExternalCompactionJob job =
            client.reserveCompactionJob(TraceUtil.traceInfo(), getContext().rpcCreds(), queue,
                prioTserver.prio, compactorAddress, externalCompactionId);
        if (null == job.getExternalCompactionId()) {
          LOG.debug("No compactions found for queue {} on tserver {}, trying next tserver", queue,
              tserver.getHostAndPort(), compactorAddress);

          QUEUE_SUMMARIES.removeSummary(tserver, queueName, prioTserver.prio);
          prioTserver = QUEUE_SUMMARIES.getNextTserver(queueName);
          continue;
        }
        RUNNING.put(ExternalCompactionId.of(job.getExternalCompactionId()),
            new RunningCompaction(job, compactorAddress, tserver));
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
      LOG.debug("No tservers found for queue {}, returning empty job to compactor {}", queue,
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
   * Return the Thrift client for the Compactor
   *
   * @param compactorAddress
   *          compactor address
   * @return thrift client
   * @throws TTransportException
   *           thrift error
   */
  protected Compactor.Client getCompactorConnection(HostAndPort compactorAddress)
      throws TTransportException {
    TTransport transport =
        ThriftTransportPool.getInstance().getTransport(compactorAddress, 0, getContext());
    return ThriftUtil.createClient(new Compactor.Client.Factory(), transport);
  }

  /**
   * Called by the TabletServer to cancel the running compaction.
   *
   * @param tinfo
   *          trace info
   * @param credentials
   *          tcredentials object
   * @param externalCompactionId
   *          compaction id
   * @throws TException
   *           thrift error
   */
  @Override
  public void cancelCompaction(TInfo tinfo, TCredentials credentials, String externalCompactionId)
      throws TException {
    // do not expect users to call this directly, expect other tservers to call this method
    if (!security.canPerformSystemActions(credentials)) {
      throw new AccumuloSecurityException(credentials.getPrincipal(),
          SecurityErrorCode.PERMISSION_DENIED).asThriftException();
    }
    cancelCompaction(externalCompactionId);
  }

  private void cancelCompaction(String externalCompactionId) throws TException {
    LOG.info("Compaction cancel requested, id: {}", externalCompactionId);
    final RunningCompaction rc = RUNNING.get(ExternalCompactionId.of(externalCompactionId));
    if (null == rc) {
      return;
    }
    final HostAndPort compactor = HostAndPort.fromString(rc.getCompactorAddress());
    RetryableThriftCall<String> cancelThriftCall = new RetryableThriftCall<>(1000,
        RetryableThriftCall.MAX_WAIT_TIME, 0, new RetryableThriftFunction<String>() {
          @Override
          public String execute() throws TException {
            Compactor.Client compactorConnection = null;
            try {
              compactorConnection = getCompactorConnection(compactor);
              compactorConnection.cancel(TraceUtil.traceInfo(), getContext().rpcCreds(),
                  rc.getJob().getExternalCompactionId());
              return "";
            } catch (TException e) {
              throw e;
            } finally {
              ThriftUtil.returnClient(compactorConnection);
            }
          }
        });
    try {
      cancelThriftCall.run();
    } catch (RetriesExceededException e) {
      LOG.error("Unable to contact Compactor {} to cancel running compaction {}",
          rc.getCompactorAddress(), rc.getJob(), e);
    }
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
   * @throws UnknownCompactionIdException
   *           if compaction is not running
   * @throws TException
   *           thrift error
   */
  @Override
  public void compactionCompleted(TInfo tinfo, TCredentials credentials,
      String externalCompactionId, TKeyExtent textent, TCompactionStats stats) throws TException {
    // do not expect users to call this directly, expect other tservers to call this method
    if (!security.canPerformSystemActions(credentials)) {
      throw new AccumuloSecurityException(credentials.getPrincipal(),
          SecurityErrorCode.PERMISSION_DENIED).asThriftException();
    }
    LOG.info("Compaction completed, id: {}, stats: {}", externalCompactionId, stats);
    final var ecid = ExternalCompactionId.of(externalCompactionId);
    final RunningCompaction rc = RUNNING.get(ecid);
    if (null != rc) {
      RUNNING.remove(ecid, rc);
      compactionFinalizer.commitCompaction(ecid, KeyExtent.fromThrift(textent), stats.fileSize,
          stats.entriesWritten);
    } else {
      LOG.error(
          "Compaction completed called by Compactor for {}, but no running compaction for that id.",
          externalCompactionId);
      throw new UnknownCompactionIdException();
    }
  }

  @Override
  public void compactionFailed(TInfo tinfo, TCredentials credentials, String externalCompactionId,
      TKeyExtent extent) throws TException {
    // do not expect users to call this directly, expect other tservers to call this method
    if (!security.canPerformSystemActions(credentials)) {
      throw new AccumuloSecurityException(credentials.getPrincipal(),
          SecurityErrorCode.PERMISSION_DENIED).asThriftException();
    }
    LOG.info("Compaction failed, id: {}", externalCompactionId);
    final var ecid = ExternalCompactionId.of(externalCompactionId);
    final RunningCompaction rc = RUNNING.get(ecid);
    if (null != rc) {
      RUNNING.remove(ecid, rc);
      compactionFinalizer.failCompactions(Map.of(ecid, KeyExtent.fromThrift(extent)));
    } else {
      LOG.error(
          "Compaction failed called by Compactor for {}, but no running compaction for that id.",
          externalCompactionId);
      throw new UnknownCompactionIdException();
    }
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
   * @throws UnknownCompactionIdException
   *           if compaction is not running
   * @throws TException
   *           thrift error
   */
  @Override
  public void updateCompactionStatus(TInfo tinfo, TCredentials credentials,
      String externalCompactionId, TCompactionState state, String message, long timestamp)
      throws TException {
    // do not expect users to call this directly, expect other tservers to call this method
    if (!security.canPerformSystemActions(credentials)) {
      throw new AccumuloSecurityException(credentials.getPrincipal(),
          SecurityErrorCode.PERMISSION_DENIED).asThriftException();
    }
    LOG.info("Compaction status update, id: {}, timestamp: {}, state: {}, message: {}",
        externalCompactionId, timestamp, state, message);
    final RunningCompaction rc = RUNNING.get(ExternalCompactionId.of(externalCompactionId));
    if (null != rc) {
      rc.addUpdate(timestamp, message, state);
    } else {
      throw new UnknownCompactionIdException();
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

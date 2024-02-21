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

import static com.google.common.util.concurrent.Uninterruptibles.sleepUninterruptibly;
import static org.apache.accumulo.core.util.LazySingletons.RANDOM;
import static org.apache.accumulo.core.util.threads.ThreadPools.watchCriticalFixedDelay;
import static org.apache.accumulo.core.util.threads.ThreadPools.watchCriticalScheduledTask;
import static org.apache.accumulo.core.util.threads.ThreadPools.watchNonCriticalScheduledTask;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.reflect.InvocationTargetException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.classloader.ClassLoaderUtil;
import org.apache.accumulo.core.cli.ConfigOpts;
import org.apache.accumulo.core.client.Durability;
import org.apache.accumulo.core.clientImpl.ClientTabletCache;
import org.apache.accumulo.core.clientImpl.DurabilityImpl;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.conf.SiteConfiguration;
import org.apache.accumulo.core.data.InstanceId;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.fate.zookeeper.ZooCache;
import org.apache.accumulo.core.fate.zookeeper.ZooReaderWriter;
import org.apache.accumulo.core.fate.zookeeper.ZooUtil.NodeExistsPolicy;
import org.apache.accumulo.core.file.blockfile.cache.impl.BlockCacheConfiguration;
import org.apache.accumulo.core.lock.ServiceLock;
import org.apache.accumulo.core.lock.ServiceLock.LockLossReason;
import org.apache.accumulo.core.lock.ServiceLock.LockWatcher;
import org.apache.accumulo.core.lock.ServiceLockData;
import org.apache.accumulo.core.lock.ServiceLockData.ServiceDescriptor;
import org.apache.accumulo.core.lock.ServiceLockData.ServiceDescriptors;
import org.apache.accumulo.core.lock.ServiceLockData.ThriftService;
import org.apache.accumulo.core.manager.thrift.Compacting;
import org.apache.accumulo.core.manager.thrift.ManagerClientService;
import org.apache.accumulo.core.manager.thrift.TableInfo;
import org.apache.accumulo.core.manager.thrift.TabletServerStatus;
import org.apache.accumulo.core.metadata.AccumuloTable;
import org.apache.accumulo.core.metadata.TServerInstance;
import org.apache.accumulo.core.metadata.schema.Ample.TabletsMutator;
import org.apache.accumulo.core.metrics.MetricsUtil;
import org.apache.accumulo.core.rpc.ThriftUtil;
import org.apache.accumulo.core.rpc.clients.ThriftClientTypes;
import org.apache.accumulo.core.spi.fs.VolumeChooserEnvironment;
import org.apache.accumulo.core.spi.ondemand.OnDemandTabletUnloader;
import org.apache.accumulo.core.spi.ondemand.OnDemandTabletUnloader.UnloaderParams;
import org.apache.accumulo.core.tabletserver.UnloaderParamsImpl;
import org.apache.accumulo.core.tabletserver.log.LogEntry;
import org.apache.accumulo.core.util.ComparablePair;
import org.apache.accumulo.core.util.Halt;
import org.apache.accumulo.core.util.MapCounter;
import org.apache.accumulo.core.util.Pair;
import org.apache.accumulo.core.util.Retry;
import org.apache.accumulo.core.util.Retry.RetryFactory;
import org.apache.accumulo.core.util.UtilWaitThread;
import org.apache.accumulo.core.util.threads.Threads;
import org.apache.accumulo.server.AbstractServer;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.ServiceEnvironmentImpl;
import org.apache.accumulo.server.TabletLevel;
import org.apache.accumulo.server.client.ClientServiceHandler;
import org.apache.accumulo.server.compaction.CompactionWatcher;
import org.apache.accumulo.server.compaction.PausedCompactionMetrics;
import org.apache.accumulo.server.conf.TableConfiguration;
import org.apache.accumulo.server.fs.VolumeChooserEnvironmentImpl;
import org.apache.accumulo.server.fs.VolumeManager;
import org.apache.accumulo.server.log.SortedLogState;
import org.apache.accumulo.server.log.WalStateManager;
import org.apache.accumulo.server.log.WalStateManager.WalMarkerException;
import org.apache.accumulo.server.manager.recovery.RecoveryPath;
import org.apache.accumulo.server.rpc.ServerAddress;
import org.apache.accumulo.server.rpc.TServerUtils;
import org.apache.accumulo.server.rpc.ThriftProcessorTypes;
import org.apache.accumulo.server.security.SecurityOperation;
import org.apache.accumulo.server.security.SecurityUtil;
import org.apache.accumulo.server.security.delegation.ZooAuthenticationKeyWatcher;
import org.apache.accumulo.server.util.time.RelativeTime;
import org.apache.accumulo.tserver.log.DfsLogger;
import org.apache.accumulo.tserver.log.LogSorter;
import org.apache.accumulo.tserver.log.MutationReceiver;
import org.apache.accumulo.tserver.log.TabletServerLogger;
import org.apache.accumulo.tserver.managermessage.ManagerMessage;
import org.apache.accumulo.tserver.metrics.TabletServerMetrics;
import org.apache.accumulo.tserver.metrics.TabletServerMinCMetrics;
import org.apache.accumulo.tserver.metrics.TabletServerScanMetrics;
import org.apache.accumulo.tserver.metrics.TabletServerUpdateMetrics;
import org.apache.accumulo.tserver.scan.ScanRunState;
import org.apache.accumulo.tserver.session.Session;
import org.apache.accumulo.tserver.session.SessionManager;
import org.apache.accumulo.tserver.tablet.CommitSession;
import org.apache.accumulo.tserver.tablet.Tablet;
import org.apache.commons.collections4.map.LRUMap;
import org.apache.hadoop.fs.Path;
import org.apache.thrift.TException;
import org.apache.thrift.TProcessor;
import org.apache.thrift.TServiceClient;
import org.apache.thrift.server.TServer;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.net.HostAndPort;

public class TabletServer extends AbstractServer implements TabletHostingServer {

  private static final Logger log = LoggerFactory.getLogger(TabletServer.class);
  private static final long TIME_BETWEEN_LOCATOR_CACHE_CLEARS = TimeUnit.HOURS.toMillis(1);

  final ZooCache managerLockCache;

  final TabletServerLogger logger;

  private TabletServerMetrics metrics;
  TabletServerUpdateMetrics updateMetrics;
  TabletServerScanMetrics scanMetrics;
  TabletServerMinCMetrics mincMetrics;
  PausedCompactionMetrics pausedMetrics;

  @Override
  public TabletServerScanMetrics getScanMetrics() {
    return scanMetrics;
  }

  public TabletServerMinCMetrics getMinCMetrics() {
    return mincMetrics;
  }

  @Override
  public PausedCompactionMetrics getPausedCompactionMetrics() {
    return pausedMetrics;
  }

  private final LogSorter logSorter;
  final TabletStatsKeeper statsKeeper;
  private final AtomicInteger logIdGenerator = new AtomicInteger();

  private final AtomicLong flushCounter = new AtomicLong(0);
  private final AtomicLong syncCounter = new AtomicLong(0);

  final OnlineTablets onlineTablets = new OnlineTablets();
  final SortedSet<KeyExtent> unopenedTablets = Collections.synchronizedSortedSet(new TreeSet<>());
  final SortedSet<KeyExtent> openingTablets = Collections.synchronizedSortedSet(new TreeSet<>());
  final Map<KeyExtent,Long> recentlyUnloadedCache = Collections.synchronizedMap(new LRUMap<>(1000));

  final TabletServerResourceManager resourceManager;
  private final SecurityOperation security;

  private final BlockingDeque<ManagerMessage> managerMessages = new LinkedBlockingDeque<>();

  volatile HostAndPort clientAddress;

  private volatile boolean serverStopRequested = false;
  private volatile boolean shutdownComplete = false;

  private ServiceLock tabletServerLock;

  private TServer server;

  private String lockID;
  private volatile long lockSessionId = -1;

  public static final AtomicLong seekCount = new AtomicLong(0);

  private final AtomicLong totalMinorCompactions = new AtomicLong(0);
  private final AtomicInteger onDemandUnloadedLowMemory = new AtomicInteger(0);

  private final ZooAuthenticationKeyWatcher authKeyWatcher;
  private final WalStateManager walMarker;
  private final ServerContext context;

  public static void main(String[] args) throws Exception {
    try (TabletServer tserver = new TabletServer(new ConfigOpts(), args)) {
      tserver.runServer();
    }
  }

  protected TabletServer(ConfigOpts opts, String[] args) {
    super("tserver", opts, args);
    context = super.getContext();
    this.managerLockCache = new ZooCache(context.getZooReader(), null);
    final AccumuloConfiguration aconf = getConfiguration();
    log.info("Version " + Constants.VERSION);
    log.info("Instance " + getInstanceID());
    this.sessionManager = new SessionManager(context);
    this.logSorter = new LogSorter(context, aconf);
    this.statsKeeper = new TabletStatsKeeper();
    final int numBusyTabletsToLog = aconf.getCount(Property.TSERV_LOG_BUSY_TABLETS_COUNT);
    final long logBusyTabletsDelay =
        aconf.getTimeInMillis(Property.TSERV_LOG_BUSY_TABLETS_INTERVAL);

    // check early whether the WAL directory supports sync. issue warning if
    // it doesn't
    checkWalCanSync(context);

    // This thread will calculate and log out the busiest tablets based on ingest count and
    // query count every #{logBusiestTabletsDelay}
    if (numBusyTabletsToLog > 0) {
      ScheduledFuture<?> future = context.getScheduledExecutor()
          .scheduleWithFixedDelay(Threads.createNamedRunnable("BusyTabletLogger", new Runnable() {
            private BusiestTracker ingestTracker =
                BusiestTracker.newBusiestIngestTracker(numBusyTabletsToLog);
            private BusiestTracker queryTracker =
                BusiestTracker.newBusiestQueryTracker(numBusyTabletsToLog);

            @Override
            public void run() {
              Collection<Tablet> tablets = onlineTablets.snapshot().values();
              logBusyTablets(ingestTracker.computeBusiest(tablets), "ingest count");
              logBusyTablets(queryTracker.computeBusiest(tablets), "query count");
            }

            private void logBusyTablets(List<ComparablePair<Long,KeyExtent>> busyTablets,
                String label) {

              int i = 1;
              for (Pair<Long,KeyExtent> pair : busyTablets) {
                log.debug("{} busiest tablet by {}: {} -- extent: {} ", i, label.toLowerCase(),
                    pair.getFirst(), pair.getSecond());
                i++;
              }
            }
          }), logBusyTabletsDelay, logBusyTabletsDelay, TimeUnit.MILLISECONDS);
      watchNonCriticalScheduledTask(future);
    }

    ScheduledFuture<?> future = context.getScheduledExecutor()
        .scheduleWithFixedDelay(Threads.createNamedRunnable("TabletRateUpdater", () -> {
          long now = System.currentTimeMillis();
          for (Tablet tablet : getOnlineTablets().values()) {
            try {
              tablet.updateRates(now);
            } catch (Exception ex) {
              log.error("Error updating rates for {}", tablet.getExtent(), ex);
            }
          }
        }), 5, 5, TimeUnit.SECONDS);
    watchNonCriticalScheduledTask(future);

    final long walMaxSize = aconf.getAsBytes(Property.TSERV_WAL_MAX_SIZE);
    final long walMaxAge = aconf.getTimeInMillis(Property.TSERV_WAL_MAX_AGE);
    final long minBlockSize =
        context.getHadoopConf().getLong("dfs.namenode.fs-limits.min-block-size", 0);
    if (minBlockSize != 0 && minBlockSize > walMaxSize) {
      throw new RuntimeException("Unable to start TabletServer. Logger is set to use blocksize "
          + walMaxSize + " but hdfs minimum block size is " + minBlockSize
          + ". Either increase the " + Property.TSERV_WAL_MAX_SIZE
          + " or decrease dfs.namenode.fs-limits.min-block-size in hdfs-site.xml.");
    }

    final long toleratedWalCreationFailures =
        aconf.getCount(Property.TSERV_WAL_TOLERATED_CREATION_FAILURES);
    final long walFailureRetryIncrement =
        aconf.getTimeInMillis(Property.TSERV_WAL_TOLERATED_WAIT_INCREMENT);
    final long walFailureRetryMax =
        aconf.getTimeInMillis(Property.TSERV_WAL_TOLERATED_MAXIMUM_WAIT_DURATION);
    final RetryFactory walCreationRetryFactory =
        Retry.builder().maxRetries(toleratedWalCreationFailures)
            .retryAfter(walFailureRetryIncrement, TimeUnit.MILLISECONDS)
            .incrementBy(walFailureRetryIncrement, TimeUnit.MILLISECONDS)
            .maxWait(walFailureRetryMax, TimeUnit.MILLISECONDS).backOffFactor(1.5)
            .logInterval(3, TimeUnit.MINUTES).createFactory();
    // Tolerate infinite failures for the write, however backing off the same as for creation
    // failures.
    final RetryFactory walWritingRetryFactory = Retry.builder().infiniteRetries()
        .retryAfter(walFailureRetryIncrement, TimeUnit.MILLISECONDS)
        .incrementBy(walFailureRetryIncrement, TimeUnit.MILLISECONDS)
        .maxWait(walFailureRetryMax, TimeUnit.MILLISECONDS).backOffFactor(1.5)
        .logInterval(3, TimeUnit.MINUTES).createFactory();

    logger = new TabletServerLogger(this, walMaxSize, syncCounter, flushCounter,
        walCreationRetryFactory, walWritingRetryFactory, walMaxAge);
    this.resourceManager = new TabletServerResourceManager(context, this);
    this.security = context.getSecurityOperation();

    watchCriticalScheduledTask(context.getScheduledExecutor().scheduleWithFixedDelay(
        ClientTabletCache::clearInstances, jitter(), jitter(), TimeUnit.MILLISECONDS));
    walMarker = new WalStateManager(context);

    if (aconf.getBoolean(Property.INSTANCE_RPC_SASL_ENABLED)) {
      log.info("SASL is enabled, creating ZooKeeper watcher for AuthenticationKeys");
      // Watcher to notice new AuthenticationKeys which enable delegation tokens
      authKeyWatcher =
          new ZooAuthenticationKeyWatcher(context.getSecretManager(), context.getZooReaderWriter(),
              context.getZooKeeperRoot() + Constants.ZDELEGATION_TOKEN_KEYS);
    } else {
      authKeyWatcher = null;
    }
    config();
  }

  @Override
  protected String getResourceGroupPropertyValue(SiteConfiguration conf) {
    return conf.get(Property.TSERV_GROUP_NAME);
  }

  public InstanceId getInstanceID() {
    return getContext().getInstanceID();
  }

  public String getVersion() {
    return Constants.VERSION;
  }

  private static long jitter() {
    // add a random 10% wait
    return (long) ((1. + (RANDOM.get().nextDouble() / 10))
        * TabletServer.TIME_BETWEEN_LOCATOR_CACHE_CLEARS);
  }

  final SessionManager sessionManager;

  private final AtomicLong totalQueuedMutationSize = new AtomicLong(0);
  private final ReentrantLock recoveryLock = new ReentrantLock(true);
  private ClientServiceHandler clientHandler;
  private TabletClientHandler thriftClientHandler;
  private ThriftScanClientHandler scanClientHandler;

  String getLockID() {
    return lockID;
  }

  void requestStop() {
    log.info("Stop requested.");
    serverStopRequested = true;
  }

  public long updateTotalQueuedMutationSize(long additionalMutationSize) {
    return totalQueuedMutationSize.addAndGet(additionalMutationSize);
  }

  @Override
  public Session getSession(long sessionId) {
    return sessionManager.getSession(sessionId);
  }

  // add a message for the main thread to send back to the manager
  public void enqueueManagerMessage(ManagerMessage m) {
    managerMessages.addLast(m);
  }

  void acquireRecoveryMemory(KeyExtent extent) {
    if (!extent.isMeta()) {
      recoveryLock.lock();
    }
  }

  void releaseRecoveryMemory(KeyExtent extent) {
    if (!extent.isMeta()) {
      recoveryLock.unlock();
    }
  }

  private HostAndPort startServer(AccumuloConfiguration conf, String address, TProcessor processor)
      throws UnknownHostException {
    Property maxMessageSizeProperty = (conf.get(Property.TSERV_MAX_MESSAGE_SIZE) != null
        ? Property.TSERV_MAX_MESSAGE_SIZE : Property.GENERAL_MAX_MESSAGE_SIZE);
    ServerAddress sp = TServerUtils.startServer(getContext(), address, Property.TSERV_CLIENTPORT,
        processor, this.getClass().getSimpleName(), "Thrift Client Server",
        Property.TSERV_PORTSEARCH, Property.TSERV_MINTHREADS, Property.TSERV_MINTHREADS_TIMEOUT,
        Property.TSERV_THREADCHECK, maxMessageSizeProperty);
    this.server = sp.server;
    return sp.address;
  }

  private HostAndPort getManagerAddress() {
    try {
      List<String> locations = getContext().getManagerLocations();
      if (locations.isEmpty()) {
        return null;
      }
      return HostAndPort.fromString(locations.get(0));
    } catch (Exception e) {
      log.warn("Failed to obtain manager host " + e);
    }

    return null;
  }

  // Connect to the manager for posting asynchronous results
  private ManagerClientService.Client managerConnection(HostAndPort address) {
    try {
      if (address == null) {
        return null;
      }
      // log.info("Listener API to manager has been opened");
      return ThriftUtil.getClient(ThriftClientTypes.MANAGER, address, getContext());
    } catch (Exception e) {
      log.warn("Issue with managerConnection (" + address + ") " + e, e);
    }
    return null;
  }

  protected ClientServiceHandler newClientHandler() {
    return new ClientServiceHandler(context);
  }

  // exists to be overridden in tests
  protected TabletClientHandler newTabletClientHandler(WriteTracker writeTracker) {
    return new TabletClientHandler(this, writeTracker);
  }

  protected ThriftScanClientHandler newThriftScanClientHandler(WriteTracker writeTracker) {
    return new ThriftScanClientHandler(this, writeTracker);
  }

  private void returnManagerConnection(ManagerClientService.Client client) {
    ThriftUtil.returnClient(client, context);
  }

  private HostAndPort startTabletClientService() throws UnknownHostException {
    // start listening for client connection last
    WriteTracker writeTracker = new WriteTracker();
    clientHandler = newClientHandler();
    thriftClientHandler = newTabletClientHandler(writeTracker);
    scanClientHandler = newThriftScanClientHandler(writeTracker);

    TProcessor processor =
        ThriftProcessorTypes.getTabletServerTProcessor(clientHandler, thriftClientHandler,
            scanClientHandler, thriftClientHandler, thriftClientHandler, getContext());
    HostAndPort address = startServer(getConfiguration(), clientAddress.getHost(), processor);
    log.info("address = {}", address);
    return address;
  }

  @Override
  public ServiceLock getLock() {
    return tabletServerLock;
  }

  @Override
  public ZooCache getManagerLockCache() {
    return managerLockCache;
  }

  private void announceExistence() {
    ZooReaderWriter zoo = getContext().getZooReaderWriter();
    try {
      var zLockPath = ServiceLock.path(
          getContext().getZooKeeperRoot() + Constants.ZTSERVERS + "/" + getClientAddressString());

      try {
        zoo.putPersistentData(zLockPath.toString(), new byte[] {}, NodeExistsPolicy.SKIP);
      } catch (KeeperException e) {
        if (e.code() == KeeperException.Code.NOAUTH) {
          log.error("Failed to write to ZooKeeper. Ensure that"
              + " accumulo.properties, specifically instance.secret, is consistent.");
        }
        throw e;
      }

      UUID tabletServerUUID = UUID.randomUUID();
      tabletServerLock = new ServiceLock(zoo.getZooKeeper(), zLockPath, tabletServerUUID);

      LockWatcher lw = new LockWatcher() {

        @Override
        public void lostLock(final LockLossReason reason) {
          Halt.halt(serverStopRequested ? 0 : 1, () -> {
            if (!serverStopRequested) {
              log.error("Lost tablet server lock (reason = {}), exiting.", reason);
            }
            context.getLowMemoryDetector().logGCInfo(getConfiguration());
          });
        }

        @Override
        public void unableToMonitorLockNode(final Exception e) {
          Halt.halt(1, () -> log.error("Lost ability to monitor tablet server lock, exiting.", e));

        }
      };

      for (int i = 0; i < 120 / 5; i++) {
        zoo.putPersistentData(zLockPath.toString(), new byte[0], NodeExistsPolicy.SKIP);

        ServiceDescriptors descriptors = new ServiceDescriptors();
        for (ThriftService svc : new ThriftService[] {ThriftService.CLIENT,
            ThriftService.TABLET_INGEST, ThriftService.TABLET_MANAGEMENT, ThriftService.TABLET_SCAN,
            ThriftService.TSERV}) {
          descriptors.addService(new ServiceDescriptor(tabletServerUUID, svc,
              getClientAddressString(), this.getResourceGroup()));
        }

        if (tabletServerLock.tryLock(lw, new ServiceLockData(descriptors))) {
          lockID = tabletServerLock.getLockID()
              .serialize(getContext().getZooKeeperRoot() + Constants.ZTSERVERS + "/");
          lockSessionId = tabletServerLock.getSessionId();
          log.debug("Obtained tablet server lock {} {}", tabletServerLock.getLockPath(),
              getTabletSession());
          return;
        }
        log.info("Waiting for tablet server lock");
        sleepUninterruptibly(5, TimeUnit.SECONDS);
      }
      String msg = "Too many retries, exiting.";
      log.info(msg);
      throw new RuntimeException(msg);
    } catch (Exception e) {
      log.info("Could not obtain tablet server lock, exiting.", e);
      throw new RuntimeException(e);
    }
  }

  // main loop listens for client requests
  @Override
  public void run() {
    SecurityUtil.serverLogin(getConfiguration());

    if (authKeyWatcher != null) {
      log.info("Seeding ZooKeeper watcher for authentication keys");
      try {
        authKeyWatcher.updateAuthKeys();
      } catch (KeeperException | InterruptedException e) {
        // TODO Does there need to be a better check? What are the error conditions that we'd fall
        // out here? AUTH_FAILURE?
        // If we get the error, do we just put it on a timer and retry the exists(String, Watcher)
        // call?
        log.error("Failed to perform initial check for authentication tokens in"
            + " ZooKeeper. Delegation token authentication will be unavailable.", e);
      }
    }
    try {
      clientAddress = startTabletClientService();
    } catch (UnknownHostException e1) {
      throw new RuntimeException("Failed to start the tablet client service", e1);
    }

    try {
      MetricsUtil.initializeMetrics(context.getConfiguration(), this.applicationName, clientAddress,
          getContext().getInstanceName(), this.getResourceGroup());

      metrics = new TabletServerMetrics(this);
      updateMetrics = new TabletServerUpdateMetrics();
      scanMetrics = new TabletServerScanMetrics();
      mincMetrics = new TabletServerMinCMetrics();
      pausedMetrics = new PausedCompactionMetrics();
      MetricsUtil.initializeProducers(this, metrics, updateMetrics, scanMetrics, mincMetrics,
          pausedMetrics);

    } catch (ClassNotFoundException | InstantiationException | IllegalAccessException
        | IllegalArgumentException | InvocationTargetException | NoSuchMethodException
        | SecurityException e1) {
      log.error("Error initializing metrics, metrics will not be emitted.", e1);
    }

    announceExistence();

    try {
      walMarker.initWalMarker(getTabletSession());
    } catch (Exception e) {
      log.error("Unable to create WAL marker node in zookeeper", e);
      throw new RuntimeException(e);
    }

    int threadPoolSize =
        getContext().getConfiguration().getCount(Property.TSERV_WAL_SORT_MAX_CONCURRENT);
    if (threadPoolSize > 0) {
      try {
        // Attempt to process all existing log sorting work and start a background
        // thread to look for log sorting work in the future
        logSorter.startWatchingForRecoveryLogs(threadPoolSize);
      } catch (Exception ex) {
        log.error("Error starting LogSorter");
        throw new RuntimeException(ex);
      }
    } else {
      log.info(
          "Log sorting for tablet recovery is disabled, TSERV_WAL_SORT_MAX_CONCURRENT is less than 1.");
    }

    final AccumuloConfiguration aconf = getConfiguration();

    final long onDemandUnloaderInterval =
        aconf.getTimeInMillis(Property.TSERV_ONDEMAND_UNLOADER_INTERVAL);
    watchCriticalFixedDelay(aconf, onDemandUnloaderInterval, () -> {
      evaluateOnDemandTabletsForUnload();
    });

    HostAndPort managerHost;
    while (!serverStopRequested) {

      idleProcessCheck(() -> getOnlineTablets().isEmpty());

      // send all of the pending messages
      try {
        ManagerMessage mm = null;
        ManagerClientService.Client iface = null;

        try {
          // wait until a message is ready to send, or a server stop
          // was requested
          while (mm == null && !serverStopRequested) {
            mm = managerMessages.poll(1, TimeUnit.SECONDS);
            idleProcessCheck(() -> getOnlineTablets().isEmpty());
          }

          // have a message to send to the manager, so grab a
          // connection
          managerHost = getManagerAddress();
          iface = managerConnection(managerHost);
          TServiceClient client = iface;

          // if while loop does not execute at all and mm != null,
          // then finally block should place mm back on queue
          while (!serverStopRequested && mm != null && client != null
              && client.getOutputProtocol() != null
              && client.getOutputProtocol().getTransport() != null
              && client.getOutputProtocol().getTransport().isOpen()) {
            try {
              mm.send(getContext().rpcCreds(), getClientAddressString(), iface);
              mm = null;
            } catch (TException ex) {
              log.warn("Error sending message: queuing message again");
              managerMessages.putFirst(mm);
              mm = null;
              throw ex;
            }

            // if any messages are immediately available grab em and
            // send them
            mm = managerMessages.poll();
            idleProcessCheck(() -> getOnlineTablets().isEmpty());
          }

        } finally {

          if (mm != null) {
            managerMessages.putFirst(mm);
          }
          returnManagerConnection(iface);

          sleepUninterruptibly(1, TimeUnit.SECONDS);
        }
      } catch (InterruptedException e) {
        log.info("Interrupt Exception received, shutting down");
        serverStopRequested = true;
      } catch (Exception e) {
        // may have lost connection with manager
        // loop back to the beginning and wait for a new one
        // this way we survive manager failures
        log.error(getClientAddressString() + ": TServerInfo: Exception. Manager down?", e);
      }
    }

    // wait for shutdown
    // if the main thread exits oldServer the manager listener, the JVM will
    // kill the other threads and finalize objects. We want the shutdown that is
    // running in the manager listener thread to complete oldServer this happens.
    // consider making other threads daemon threads so that objects don't
    // get prematurely finalized
    synchronized (this) {
      while (!shutdownComplete) {
        try {
          this.wait(1000);
        } catch (InterruptedException e) {
          log.error(e.toString());
        }
      }
    }

    log.debug("Stopping Thrift Servers");
    if (server != null) {
      server.stop();
    }

    try {
      log.debug("Closing filesystems");
      getVolumeManager().close();
    } catch (IOException e) {
      log.warn("Failed to close filesystem : {}", e.getMessage(), e);
    }

    context.getLowMemoryDetector().logGCInfo(getConfiguration());

    log.info("TServerInfo: stop requested. exiting ... ");

    try {
      tabletServerLock.unlock();
    } catch (Exception e) {
      log.warn("Failed to release tablet server lock", e);
    }
  }

  public String getClientAddressString() {
    if (clientAddress == null) {
      return null;
    }
    return clientAddress.getHost() + ":" + clientAddress.getPort();
  }

  public TServerInstance getTabletSession() {
    String address = getClientAddressString();
    if (address == null) {
      return null;
    }
    if (lockSessionId == -1) {
      return null;
    }

    try {
      return new TServerInstance(address, lockSessionId);
    } catch (Exception ex) {
      log.warn("Unable to read session from tablet server lock" + ex);
      return null;
    }
  }

  private static void checkWalCanSync(ServerContext context) {
    VolumeChooserEnvironment chooserEnv =
        new VolumeChooserEnvironmentImpl(VolumeChooserEnvironment.Scope.LOGGER, context);
    Set<String> prefixes;
    var options = context.getBaseUris();
    try {
      prefixes = context.getVolumeManager().choosable(chooserEnv, options);
    } catch (RuntimeException e) {
      log.warn("Unable to determine if WAL directories ({}) support sync or flush. "
          + "Data loss may occur.", Arrays.asList(options), e);
      return;
    }

    boolean warned = false;
    for (String prefix : prefixes) {
      String logPath = prefix + Path.SEPARATOR + Constants.WAL_DIR;
      if (!context.getVolumeManager().canSyncAndFlush(new Path(logPath))) {
        // sleep a few seconds in case this is at cluster start...give monitor
        // time to start so the warning will be more visible
        if (!warned) {
          UtilWaitThread.sleep(5000);
          warned = true;
        }
        log.warn("WAL directory ({}) implementation does not support sync or flush."
            + " Data loss may occur.", logPath);
      }
    }
  }

  private void config() {
    log.info("Tablet server starting on {}", getHostname());
    CompactionWatcher.startWatching(context);

    clientAddress = HostAndPort.fromParts(getHostname(), 0);
  }

  public TabletServerStatus getStats(Map<TableId,MapCounter<ScanRunState>> scanCounts) {
    long start = System.currentTimeMillis();
    TabletServerStatus result = new TabletServerStatus();

    final Map<String,TableInfo> tables = new HashMap<>();

    getOnlineTablets().forEach((ke, tablet) -> {
      String tableId = ke.tableId().canonical();
      TableInfo table = tables.get(tableId);
      if (table == null) {
        table = new TableInfo();
        table.minors = new Compacting();
        tables.put(tableId, table);
      }
      long recs = tablet.getNumEntries();
      table.tablets++;
      table.onlineTablets++;
      table.recs += recs;
      table.queryRate += tablet.queryRate();
      table.queryByteRate += tablet.queryByteRate();
      table.ingestRate += tablet.ingestRate();
      table.ingestByteRate += tablet.ingestByteRate();
      table.scanRate += tablet.scanRate();
      long recsInMemory = tablet.getNumEntriesInMemory();
      table.recsInMemory += recsInMemory;
      if (tablet.isMinorCompactionRunning()) {
        table.minors.running++;
      }
      if (tablet.isMinorCompactionQueued()) {
        table.minors.queued++;
      }
    });

    scanCounts.forEach((tableId, mapCounter) -> {
      TableInfo table = tables.get(tableId.canonical());
      if (table == null) {
        table = new TableInfo();
        tables.put(tableId.canonical(), table);
      }

      if (table.scans == null) {
        table.scans = new Compacting();
      }

      table.scans.queued += mapCounter.getInt(ScanRunState.QUEUED);
      table.scans.running += mapCounter.getInt(ScanRunState.RUNNING);
    });

    ArrayList<KeyExtent> offlineTabletsCopy = new ArrayList<>();
    synchronized (this.unopenedTablets) {
      synchronized (this.openingTablets) {
        offlineTabletsCopy.addAll(this.unopenedTablets);
        offlineTabletsCopy.addAll(this.openingTablets);
      }
    }

    for (KeyExtent extent : offlineTabletsCopy) {
      String tableId = extent.tableId().canonical();
      TableInfo table = tables.get(tableId);
      if (table == null) {
        table = new TableInfo();
        tables.put(tableId, table);
      }
      table.tablets++;
    }

    result.lastContact = RelativeTime.currentTimeMillis();
    result.tableMap = tables;
    result.osLoad = ManagementFactory.getOperatingSystemMXBean().getSystemLoadAverage();
    result.name = getClientAddressString();
    result.holdTime = resourceManager.holdTime();
    result.lookups = seekCount.get();
    result.indexCacheHits = resourceManager.getIndexCache().getStats().hitCount();
    result.indexCacheRequest = resourceManager.getIndexCache().getStats().requestCount();
    result.dataCacheHits = resourceManager.getDataCache().getStats().hitCount();
    result.dataCacheRequest = resourceManager.getDataCache().getStats().requestCount();
    result.logSorts = logSorter.getLogSorts();
    result.flushs = flushCounter.get();
    result.syncs = syncCounter.get();
    result.version = getVersion();
    result.responseTime = System.currentTimeMillis() - start;
    return result;
  }

  private Durability getMincEventDurability(KeyExtent extent) {
    TableConfiguration conf;
    if (extent.isMeta()) {
      conf = getContext().getTableConfiguration(AccumuloTable.ROOT.tableId());
    } else {
      conf = getContext().getTableConfiguration(AccumuloTable.METADATA.tableId());
    }
    return DurabilityImpl.fromString(conf.get(Property.TABLE_DURABILITY));
  }

  public void minorCompactionFinished(CommitSession tablet, long walogSeq) throws IOException {
    Durability durability = getMincEventDurability(tablet.getExtent());
    totalMinorCompactions.incrementAndGet();
    logger.minorCompactionFinished(tablet, walogSeq, durability);
    markUnusedWALs();
  }

  public void minorCompactionStarted(CommitSession tablet, long lastUpdateSequence,
      String newDataFileLocation) throws IOException {
    Durability durability = getMincEventDurability(tablet.getExtent());
    logger.minorCompactionStarted(tablet, lastUpdateSequence, newDataFileLocation, durability);
  }

  public void recover(VolumeManager fs, KeyExtent extent, Collection<LogEntry> logEntries,
      Set<String> tabletFiles, MutationReceiver mutationReceiver) throws IOException {
    List<Path> recoveryDirs = new ArrayList<>();
    for (LogEntry entry : logEntries) {
      Path recovery = null;
      Path finished = RecoveryPath.getRecoveryPath(new Path(entry.getPath()));
      finished = SortedLogState.getFinishedMarkerPath(finished);
      TabletServer.log.debug("Looking for " + finished);
      if (fs.exists(finished)) {
        recovery = finished.getParent();
      }
      if (recovery == null) {
        throw new IOException(
            "Unable to find recovery files for extent " + extent + " logEntry: " + entry);
      }
      recoveryDirs.add(recovery);
    }
    logger.recover(getContext(), extent, recoveryDirs, tabletFiles, mutationReceiver);
  }

  public int createLogId() {
    int logId = logIdGenerator.incrementAndGet();
    if (logId < 0) {
      throw new IllegalStateException("Log Id rolled");
    }
    return logId;
  }

  @Override
  public TableConfiguration getTableConfiguration(KeyExtent extent) {
    return getContext().getTableConfiguration(extent.tableId());
  }

  public SortedMap<KeyExtent,Tablet> getOnlineTablets() {
    return onlineTablets.snapshot();
  }

  @Override
  public Tablet getOnlineTablet(KeyExtent extent) {
    Tablet t = onlineTablets.snapshot().get(extent);
    if (t != null) {
      t.setLastAccessTime();
    }
    return t;
  }

  @Override
  public SessionManager getSessionManager() {
    return sessionManager;
  }

  @Override
  public TabletServerResourceManager getResourceManager() {
    return resourceManager;
  }

  public VolumeManager getVolumeManager() {
    return getContext().getVolumeManager();
  }

  public int getOpeningCount() {
    return openingTablets.size();
  }

  public int getUnopenedCount() {
    return unopenedTablets.size();
  }

  public long getTotalMinorCompactions() {
    return totalMinorCompactions.get();
  }

  public double getHoldTimeMillis() {
    return resourceManager.holdTime();
  }

  // avoid unnecessary redundant markings to meta
  final ConcurrentHashMap<DfsLogger,EnumSet<TabletLevel>> metadataTableLogs =
      new ConcurrentHashMap<>();

  // This is a set of WALs that are closed but may still be referenced by tablets. A LinkedHashSet
  // is used because its very import to know the order in which WALs were closed when deciding if a
  // WAL is eligible for removal. Maintaining the order that logs were used in is currently a simple
  // task because there is only one active log at a time.
  final LinkedHashSet<DfsLogger> closedLogs = new LinkedHashSet<>();

  /**
   * For a closed WAL to be eligible for removal it must be unreferenced AND all closed WALs older
   * than it must be unreferenced. This method finds WALs that meet those conditions. See Github
   * issue #537.
   */
  @VisibleForTesting
  static Set<DfsLogger> findOldestUnreferencedWals(List<DfsLogger> closedLogs,
      Consumer<Set<DfsLogger>> referencedRemover) {
    LinkedHashSet<DfsLogger> unreferenced = new LinkedHashSet<>(closedLogs);

    referencedRemover.accept(unreferenced);

    Iterator<DfsLogger> closedIter = closedLogs.iterator();
    Iterator<DfsLogger> unrefIter = unreferenced.iterator();

    Set<DfsLogger> eligible = new HashSet<>();

    while (closedIter.hasNext() && unrefIter.hasNext()) {
      DfsLogger closed = closedIter.next();
      DfsLogger unref = unrefIter.next();

      if (closed.equals(unref)) {
        eligible.add(unref);
      } else {
        break;
      }
    }

    return eligible;
  }

  private void markUnusedWALs() {

    List<DfsLogger> closedCopy;

    synchronized (closedLogs) {
      closedCopy = List.copyOf(closedLogs);
    }

    Consumer<Set<DfsLogger>> refRemover = candidates -> {
      for (Tablet tablet : getOnlineTablets().values()) {
        tablet.removeInUseLogs(candidates);
        if (candidates.isEmpty()) {
          break;
        }
      }
    };

    Set<DfsLogger> eligible = findOldestUnreferencedWals(closedCopy, refRemover);

    try {
      TServerInstance session = this.getTabletSession();
      for (DfsLogger candidate : eligible) {
        log.info("Marking " + candidate.getPath() + " as unreferenced");
        walMarker.walUnreferenced(session, candidate.getPath());
      }
      synchronized (closedLogs) {
        closedLogs.removeAll(eligible);
      }
    } catch (WalMarkerException ex) {
      log.info(ex.toString(), ex);
    }
  }

  public void addNewLogMarker(DfsLogger copy) throws WalMarkerException {
    log.info("Writing log marker for " + copy.getPath());
    walMarker.addNewWalMarker(getTabletSession(), copy.getPath());
  }

  public void walogClosed(DfsLogger currentLog) throws WalMarkerException {
    metadataTableLogs.remove(currentLog);

    if (currentLog.getWrites() > 0) {
      int clSize;
      synchronized (closedLogs) {
        closedLogs.add(currentLog);
        clSize = closedLogs.size();
      }
      log.info("Marking " + currentLog.getPath() + " as closed. Total closed logs " + clSize);
      walMarker.closeWal(getTabletSession(), currentLog.getPath());

      // whenever a new log is added to the set of closed logs, go through all of the tablets and
      // see if any need to minor compact
      List<DfsLogger> closedCopy;
      synchronized (closedLogs) {
        closedCopy = List.copyOf(closedLogs);
      }

      int maxLogs = getConfiguration().getCount(Property.TSERV_WAL_MAX_REFERENCED);
      if (closedCopy.size() >= maxLogs) {
        for (Entry<KeyExtent,Tablet> entry : getOnlineTablets().entrySet()) {
          Tablet tablet = entry.getValue();
          tablet.checkIfMinorCompactionNeededForLogs(closedCopy, maxLogs);
        }
      }
    } else {
      log.info(
          "Marking " + currentLog.getPath() + " as unreferenced (skipping closed writes == 0)");
      walMarker.walUnreferenced(getTabletSession(), currentLog.getPath());
    }
  }

  @Override
  public BlockCacheConfiguration getBlockCacheConfiguration(AccumuloConfiguration acuConf) {
    return BlockCacheConfiguration.forTabletServer(acuConf);
  }

  public int getOnDemandOnlineUnloadedForLowMemory() {
    return onDemandUnloadedLowMemory.get();
  }

  private boolean isTabletInUse(KeyExtent extent) {
    // Don't call getOnlineTablet as that will update the last access time
    final Tablet t = onlineTablets.snapshot().get(extent);
    if (t == null) {
      return false;
    }
    return t.isInUse();
  }

  public void evaluateOnDemandTabletsForUnload() {

    final SortedMap<KeyExtent,Tablet> online = getOnlineTablets();

    // Sort the extents so that we can process them by table.
    final SortedMap<KeyExtent,Long> sortedOnDemandExtents = new TreeMap<>();
    // We only want to operate on OnDemand Tablets
    online.entrySet().forEach((e) -> {
      if (e.getValue().isOnDemand()) {
        sortedOnDemandExtents.put(e.getKey(), e.getValue().getLastAccessTime());
      }
    });

    if (sortedOnDemandExtents.isEmpty()) {
      return;
    }

    log.debug("Evaluating online on-demand tablets: {}", sortedOnDemandExtents);

    // If the TabletServer is running low on memory, don't call the SPI
    // plugin to evaluate which on-demand tablets to unload, just get the
    // on-demand tablet with the oldest access time and unload it.
    if (getContext().getLowMemoryDetector().isRunningLowOnMemory()) {
      final SortedMap<Long,KeyExtent> timeSortedOnDemandExtents = new TreeMap<>();
      long currTime = System.nanoTime();
      sortedOnDemandExtents.forEach((k, v) -> timeSortedOnDemandExtents.put(v - currTime, k));
      Long oldestAccessTime = timeSortedOnDemandExtents.lastKey();
      KeyExtent oldestKeyExtent = timeSortedOnDemandExtents.get(oldestAccessTime);
      log.warn("Unloading on-demand tablet: {} for table: {} due to low memory", oldestKeyExtent,
          oldestKeyExtent.tableId());
      getContext().getAmple().mutateTablet(oldestKeyExtent).deleteHostingRequested().mutate();
      onDemandUnloadedLowMemory.addAndGet(1);
      return;
    }

    // The access times are updated when getOnlineTablet is called by other methods,
    // but may not necessarily capture whether or not the Tablet is currently being used.
    // For example, getOnlineTablet is called from startScan but not from continueScan.
    // Instead of instrumenting all of the locations where the tablet is touched we
    // can use the Tablet metrics.
    final Set<KeyExtent> onDemandTabletsInUse = new HashSet<>();
    for (KeyExtent extent : sortedOnDemandExtents.keySet()) {
      if (isTabletInUse(extent)) {
        onDemandTabletsInUse.add(extent);
      }
    }
    if (!onDemandTabletsInUse.isEmpty()) {
      log.debug("Removing onDemandAccessTimes for tablets as tablets are in use: {}",
          onDemandTabletsInUse);
      onDemandTabletsInUse.forEach(sortedOnDemandExtents::remove);
      if (sortedOnDemandExtents.isEmpty()) {
        return;
      }
    }

    Set<TableId> tableIds = sortedOnDemandExtents.keySet().stream().map((k) -> {
      return k.tableId();
    }).distinct().collect(Collectors.toSet());
    log.debug("Tables that have online on-demand tablets: {}", tableIds);
    final Map<TableId,OnDemandTabletUnloader> unloaders = new HashMap<>();
    tableIds.forEach(tid -> {
      TableConfiguration tconf = getContext().getTableConfiguration(tid);
      String tableContext = ClassLoaderUtil.tableContext(tconf);
      String unloaderClassName = tconf.get(Property.TABLE_ONDEMAND_UNLOADER);
      try {
        Class<? extends OnDemandTabletUnloader> clazz = ClassLoaderUtil.loadClass(tableContext,
            unloaderClassName, OnDemandTabletUnloader.class);
        unloaders.put(tid, clazz.getConstructor().newInstance());
      } catch (ClassNotFoundException | InstantiationException | IllegalAccessException
          | IllegalArgumentException | InvocationTargetException | NoSuchMethodException
          | SecurityException e) {
        log.error(
            "Error constructing OnDemandTabletUnloader implementation, not unloading on-demand tablets",
            e);
        return;
      }
    });
    tableIds.forEach(tid -> {
      Map<KeyExtent,
          Long> subset = sortedOnDemandExtents.entrySet().stream()
              .filter((e) -> e.getKey().tableId().equals(tid))
              .collect(Collectors.toMap(e -> e.getKey(), e -> e.getValue()));
      Set<KeyExtent> onDemandTabletsToUnload = new HashSet<>();
      log.debug("Evaluating on-demand tablets for unload for table {}, extents {}", tid,
          subset.keySet());
      UnloaderParams params = new UnloaderParamsImpl(tid, new ServiceEnvironmentImpl(context),
          subset, onDemandTabletsToUnload);
      unloaders.get(tid).evaluate(params);
      try (TabletsMutator tm = getContext().getAmple().mutateTablets()) {
        onDemandTabletsToUnload.forEach(ke -> {
          log.debug("Unloading on-demand tablet: {} for table: {}", ke, tid);
          tm.mutateTablet(ke).deleteHostingRequested().mutate();
        });
      }
    });
  }

}

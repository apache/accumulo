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
package org.apache.accumulo.manager;

import static com.google.common.util.concurrent.Uninterruptibles.sleepUninterruptibly;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySortedMap;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

import java.io.IOException;
import java.net.UnknownHostException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.cli.ConfigOpts;
import org.apache.accumulo.core.client.admin.CompactionConfig;
import org.apache.accumulo.core.clientImpl.ClientContext;
import org.apache.accumulo.core.clientImpl.thrift.TableOperation;
import org.apache.accumulo.core.clientImpl.thrift.TableOperationExceptionType;
import org.apache.accumulo.core.clientImpl.thrift.ThriftTableOperationException;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.conf.SiteConfiguration;
import org.apache.accumulo.core.data.InstanceId;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.fate.Fate;
import org.apache.accumulo.core.fate.FateCleaner;
import org.apache.accumulo.core.fate.FateId;
import org.apache.accumulo.core.fate.FateInstanceType;
import org.apache.accumulo.core.fate.FateStore;
import org.apache.accumulo.core.fate.user.UserFateStore;
import org.apache.accumulo.core.fate.zookeeper.MetaFateStore;
import org.apache.accumulo.core.fate.zookeeper.ZooCache.ZcStat;
import org.apache.accumulo.core.fate.zookeeper.ZooReaderWriter;
import org.apache.accumulo.core.fate.zookeeper.ZooUtil;
import org.apache.accumulo.core.fate.zookeeper.ZooUtil.NodeExistsPolicy;
import org.apache.accumulo.core.lock.ServiceLock;
import org.apache.accumulo.core.lock.ServiceLock.LockLossReason;
import org.apache.accumulo.core.lock.ServiceLockData;
import org.apache.accumulo.core.lock.ServiceLockData.ServiceDescriptor;
import org.apache.accumulo.core.lock.ServiceLockData.ServiceDescriptors;
import org.apache.accumulo.core.lock.ServiceLockData.ThriftService;
import org.apache.accumulo.core.lock.ServiceLockPaths.AddressSelector;
import org.apache.accumulo.core.lock.ServiceLockPaths.ServiceLockPath;
import org.apache.accumulo.core.manager.balancer.AssignmentParamsImpl;
import org.apache.accumulo.core.manager.balancer.BalanceParamsImpl;
import org.apache.accumulo.core.manager.balancer.TServerStatusImpl;
import org.apache.accumulo.core.manager.balancer.TabletServerIdImpl;
import org.apache.accumulo.core.manager.state.tables.TableState;
import org.apache.accumulo.core.manager.thrift.BulkImportState;
import org.apache.accumulo.core.manager.thrift.ManagerClientService;
import org.apache.accumulo.core.manager.thrift.ManagerGoalState;
import org.apache.accumulo.core.manager.thrift.ManagerMonitorInfo;
import org.apache.accumulo.core.manager.thrift.ManagerState;
import org.apache.accumulo.core.manager.thrift.TableInfo;
import org.apache.accumulo.core.manager.thrift.TabletServerStatus;
import org.apache.accumulo.core.metadata.AccumuloTable;
import org.apache.accumulo.core.metadata.TServerInstance;
import org.apache.accumulo.core.metadata.schema.Ample.DataLevel;
import org.apache.accumulo.core.metadata.schema.TabletMetadata;
import org.apache.accumulo.core.metrics.MetricsInfo;
import org.apache.accumulo.core.metrics.MetricsProducer;
import org.apache.accumulo.core.metrics.thrift.MetricSource;
import org.apache.accumulo.core.spi.balancer.BalancerEnvironment;
import org.apache.accumulo.core.spi.balancer.SimpleLoadBalancer;
import org.apache.accumulo.core.spi.balancer.TabletBalancer;
import org.apache.accumulo.core.spi.balancer.data.TServerStatus;
import org.apache.accumulo.core.spi.balancer.data.TabletMigration;
import org.apache.accumulo.core.spi.balancer.data.TabletServerId;
import org.apache.accumulo.core.trace.TraceUtil;
import org.apache.accumulo.core.util.Halt;
import org.apache.accumulo.core.util.Retry;
import org.apache.accumulo.core.util.Timer;
import org.apache.accumulo.core.util.threads.ThreadPools;
import org.apache.accumulo.core.util.threads.Threads;
import org.apache.accumulo.core.util.time.SteadyTime;
import org.apache.accumulo.manager.compaction.coordinator.CompactionCoordinator;
import org.apache.accumulo.manager.metrics.BalancerMetrics;
import org.apache.accumulo.manager.metrics.ManagerMetrics;
import org.apache.accumulo.manager.recovery.RecoveryManager;
import org.apache.accumulo.manager.split.Splitter;
import org.apache.accumulo.manager.state.TableCounts;
import org.apache.accumulo.manager.tableOps.TraceRepo;
import org.apache.accumulo.manager.upgrade.PreUpgradeValidation;
import org.apache.accumulo.manager.upgrade.UpgradeCoordinator;
import org.apache.accumulo.server.AbstractServer;
import org.apache.accumulo.server.HighlyAvailableService;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.compaction.CompactionConfigStorage;
import org.apache.accumulo.server.fs.VolumeManager;
import org.apache.accumulo.server.manager.LiveTServerSet;
import org.apache.accumulo.server.manager.LiveTServerSet.LiveTServersSnapshot;
import org.apache.accumulo.server.manager.LiveTServerSet.TServerConnection;
import org.apache.accumulo.server.manager.balancer.BalancerEnvironmentImpl;
import org.apache.accumulo.server.manager.state.DeadServerList;
import org.apache.accumulo.server.manager.state.TabletServerState;
import org.apache.accumulo.server.manager.state.TabletStateStore;
import org.apache.accumulo.server.manager.state.UnassignedTablet;
import org.apache.accumulo.server.metrics.MetricServiceHandler;
import org.apache.accumulo.server.rpc.HighlyAvailableServiceWrapper;
import org.apache.accumulo.server.rpc.ServerAddress;
import org.apache.accumulo.server.rpc.TServerUtils;
import org.apache.accumulo.server.rpc.ThriftProcessorTypes;
import org.apache.accumulo.server.security.SecurityOperation;
import org.apache.accumulo.server.security.delegation.AuthenticationTokenKeyManager;
import org.apache.accumulo.server.security.delegation.ZooAuthenticationKeyDistributor;
import org.apache.accumulo.server.tables.TableManager;
import org.apache.accumulo.server.tables.TableObserver;
import org.apache.accumulo.server.util.ScanServerMetadataEntries;
import org.apache.accumulo.server.util.ServerBulkImportStatus;
import org.apache.accumulo.server.util.TableInfoUtil;
import org.apache.thrift.TException;
import org.apache.thrift.server.TServer;
import org.apache.thrift.transport.TTransportException;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.NoAuthException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.Comparators;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.RateLimiter;
import com.google.common.util.concurrent.Uninterruptibles;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.micrometer.core.instrument.MeterRegistry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.context.Scope;

/**
 * The Manager is responsible for assigning and balancing tablets to tablet servers.
 * <p>
 * The manager will also coordinate log recoveries and reports general status.
 */
public class Manager extends AbstractServer
    implements LiveTServerSet.Listener, TableObserver, HighlyAvailableService {

  static final Logger log = LoggerFactory.getLogger(Manager.class);

  static final int ONE_SECOND = 1000;
  private static final long CLEANUP_INTERVAL_MINUTES = 5;
  static final long WAIT_BETWEEN_ERRORS = ONE_SECOND;
  private static final long DEFAULT_WAIT_FOR_WATCHER = 10 * ONE_SECOND;
  private static final int MAX_CLEANUP_WAIT_TIME = ONE_SECOND;
  private static final int TIME_TO_WAIT_BETWEEN_LOCK_CHECKS = ONE_SECOND;
  static final int MAX_TSERVER_WORK_CHUNK = 5000;
  private static final int MAX_BAD_STATUS_COUNT = 3;
  private static final double MAX_SHUTDOWNS_PER_SEC = 10D / 60D;

  private final Object balancedNotifier = new Object();
  final LiveTServerSet tserverSet;
  private final List<TabletGroupWatcher> watchers = new ArrayList<>();
  final SecurityOperation security;
  final Map<TServerInstance,AtomicInteger> badServers =
      Collections.synchronizedMap(new HashMap<>());
  final Set<TServerInstance> serversToShutdown = Collections.synchronizedSet(new HashSet<>());
  final SortedMap<KeyExtent,TServerInstance> migrations =
      Collections.synchronizedSortedMap(new TreeMap<>());
  final EventCoordinator nextEvent = new EventCoordinator();
  RecoveryManager recoveryManager = null;
  private final ManagerTime timeKeeper;

  // Delegation Token classes
  private final boolean delegationTokensAvailable;
  private ZooAuthenticationKeyDistributor keyDistributor;
  private AuthenticationTokenKeyManager authenticationTokenKeyManager;

  ServiceLock managerLock = null;
  private TServer clientService = null;
  protected volatile TabletBalancer tabletBalancer;
  private final BalancerEnvironment balancerEnvironment;
  private final BalancerMetrics balancerMetrics = new BalancerMetrics();

  private ManagerState state = ManagerState.INITIAL;

  // fateReadyLatch and fateRefs go together; when this latch is ready, then the fate references
  // should already have been set; ConcurrentHashMap will guarantee that all threads will see
  // the initialized fate references after the latch is ready
  private final CountDownLatch fateReadyLatch = new CountDownLatch(1);
  private final AtomicReference<Map<FateInstanceType,Fate<Manager>>> fateRefs =
      new AtomicReference<>();

  volatile SortedMap<TServerInstance,TabletServerStatus> tserverStatus = emptySortedMap();
  volatile Map<String,Set<TServerInstance>> tServerGroupingForBalancer = emptyMap();

  final ServerBulkImportStatus bulkImportStatus = new ServerBulkImportStatus();

  private final AtomicBoolean managerInitialized = new AtomicBoolean(false);

  private final long timeToCacheRecoveryWalExistence;
  private ExecutorService tableInformationStatusPool = null;
  private ThreadPoolExecutor tabletRefreshThreadPool;

  private final TabletStateStore rootTabletStore;
  private final TabletStateStore metadataTabletStore;
  private final TabletStateStore userTabletStore;

  public synchronized ManagerState getManagerState() {
    return state;
  }

  public Map<FateId,Map<String,String>> getCompactionHints(DataLevel level) {
    Predicate<TableId> tablePredicate = (tableId) -> DataLevel.of(tableId) == level;
    Map<FateId,CompactionConfig> allConfig;
    try {
      allConfig = CompactionConfigStorage.getAllConfig(getContext(), tablePredicate);
    } catch (InterruptedException | KeeperException e) {
      throw new RuntimeException(e);
    }
    return Maps.transformValues(allConfig, CompactionConfig::getExecutionHints);
  }

  public boolean stillManager() {
    return getManagerState() != ManagerState.STOP;
  }

  /**
   * Retrieve the Fate object, blocking until it is ready. This could cause problems if Fate
   * operations are attempted to be used prior to the Manager being ready for them. If these
   * operations are triggered by a client side request from a tserver or client, it should be safe
   * to wait to handle those until Fate is ready, but if it occurs during an upgrade, or some other
   * time in the Manager before Fate is started, that may result in a deadlock and will need to be
   * fixed.
   *
   * @return the Fate object, only after the fate components are running and ready
   */
  public Fate<Manager> fate(FateInstanceType type) {
    try {
      // block up to 30 seconds until it's ready; if it's still not ready, introduce some logging
      if (!fateReadyLatch.await(30, SECONDS)) {
        String msgPrefix = "Unexpected use of fate in thread " + Thread.currentThread().getName()
            + " at time " + System.currentTimeMillis();
        // include stack trace so we know where it's coming from, in case we need to troubleshoot it
        log.warn("{} blocked until fate starts", msgPrefix,
            new IllegalStateException("Attempted fate action before manager finished starting up; "
                + "if this doesn't make progress, please report it as a bug to the developers"));
        int minutes = 0;
        while (!fateReadyLatch.await(5, MINUTES)) {
          minutes += 5;
          log.warn("{} still blocked after {} minutes; this is getting weird", msgPrefix, minutes);
        }
        log.debug("{} no longer blocked", msgPrefix);
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException("Thread was interrupted; cannot proceed");
    }
    return getFateRefs().get(type);
  }

  static final boolean X = true;
  static final boolean O = false;
  // @formatter:off
  static final boolean[][] transitionOK = {
      //                            INITIAL HAVE_LOCK SAFE_MODE NORMAL UNLOAD_META UNLOAD_ROOT STOP
      /* INITIAL */                 {X, X, O, O, O, O, X},
      /* HAVE_LOCK */               {O, X, X, X, O, O, X},
      /* SAFE_MODE */               {O, O, X, X, X, O, X},
      /* NORMAL */                  {O, O, X, X, X, O, X},
      /* UNLOAD_METADATA_TABLETS */ {O, O, X, X, X, X, X},
      /* UNLOAD_ROOT_TABLET */      {O, O, O, X, X, X, X},
      /* STOP */                    {O, O, O, O, O, X, X}};
  //@formatter:on
  synchronized void setManagerState(final ManagerState newState) {
    if (state == newState) {
      return;
    }
    if (!transitionOK[state.ordinal()][newState.ordinal()]) {
      throw new IllegalStateException(String.format(
          "Programmer error: manager should not transition from %s to %s", state, newState));
    }
    final ManagerState oldState = state;
    state = newState;
    nextEvent.event("State changed from %s to %s", oldState, newState);
    switch (newState) {
      case STOP:
        // Give the server a little time before shutdown so the client
        // thread requesting the stop can return
        final var future = getContext().getScheduledExecutor().scheduleWithFixedDelay(() -> {
          // This frees the main thread and will cause the manager to exit
          clientService.stop();
          Manager.this.nextEvent.event("stopped event loop");
        }, 100L, 1000L, MILLISECONDS);
        ThreadPools.watchNonCriticalScheduledTask(future);
        break;
      case HAVE_LOCK:
        if (isUpgrading()) {
          new PreUpgradeValidation().validate(getContext(), nextEvent);
          upgradeCoordinator.upgradeZookeeper(getContext(), nextEvent);
        }
        break;
      case NORMAL:
        if (isUpgrading()) {
          upgradeMetadataFuture = upgradeCoordinator.upgradeMetadata(getContext(), nextEvent);
        }
        break;
      default:
        break;
    }
  }

  private final UpgradeCoordinator upgradeCoordinator = new UpgradeCoordinator();

  private Future<Void> upgradeMetadataFuture;

  private FateServiceHandler fateServiceHandler;
  private ManagerClientServiceHandler managerClientHandler;
  private CompactionCoordinator compactionCoordinator;

  private int assignedOrHosted(TableId tableId) {
    int result = 0;
    for (TabletGroupWatcher watcher : watchers) {
      TableCounts count = watcher.getStats(tableId);
      result += count.hosted() + count.assigned();
    }
    return result;
  }

  private int totalAssignedOrHosted() {
    int result = 0;
    for (TabletGroupWatcher watcher : watchers) {
      for (TableCounts counts : watcher.getStats().values()) {
        log.debug(
            "Watcher: {}: Assigned Tablets: {}, Dead tserver assignments: {}, Suspended Tablets: {}",
            watcher.getName(), counts.assigned(), counts.assignedToDeadServers(),
            counts.suspended());
        result += counts.assigned() + counts.hosted();
      }
    }
    return result;
  }

  private int nonMetaDataTabletsAssignedOrHosted() {
    return totalAssignedOrHosted() - assignedOrHosted(AccumuloTable.METADATA.tableId())
        - assignedOrHosted(AccumuloTable.ROOT.tableId());
  }

  private int notHosted() {
    int result = 0;
    for (TabletGroupWatcher watcher : watchers) {
      for (TableCounts counts : watcher.getStats().values()) {
        result += counts.assigned() + counts.assignedToDeadServers() + counts.suspended();
      }
    }
    return result;
  }

  // The number of unassigned tablets that should be assigned: displayed on the monitor page
  int displayUnassigned() {
    int result = 0;
    switch (getManagerState()) {
      case NORMAL:
        // Count offline tablets for online tables
        for (TabletGroupWatcher watcher : watchers) {
          TableManager manager = getContext().getTableManager();
          for (Entry<TableId,TableCounts> entry : watcher.getStats().entrySet()) {
            TableId tableId = entry.getKey();
            TableCounts counts = entry.getValue();
            if (manager.getTableState(tableId) == TableState.ONLINE) {
              result += counts.unassigned() + counts.assignedToDeadServers() + counts.assigned()
                  + counts.suspended();
            }
          }
        }
        break;
      case SAFE_MODE:
        // Count offline tablets for the metadata table
        for (TabletGroupWatcher watcher : watchers) {
          TableCounts counts = watcher.getStats(AccumuloTable.METADATA.tableId());
          result += counts.unassigned() + counts.suspended();
        }
        break;
      case UNLOAD_METADATA_TABLETS:
      case UNLOAD_ROOT_TABLET:
        for (TabletGroupWatcher watcher : watchers) {
          TableCounts counts = watcher.getStats(AccumuloTable.METADATA.tableId());
          result += counts.unassigned() + counts.suspended();
        }
        break;
      default:
        break;
    }
    return result;
  }

  public void mustBeOnline(final TableId tableId) throws ThriftTableOperationException {
    ServerContext context = getContext();
    context.clearTableListCache();
    if (context.getTableState(tableId) != TableState.ONLINE) {
      throw new ThriftTableOperationException(tableId.canonical(), null, TableOperation.MERGE,
          TableOperationExceptionType.OFFLINE, "table is not online");
    }
  }

  public TableManager getTableManager() {
    return getContext().getTableManager();
  }

  public ThreadPoolExecutor getTabletRefreshThreadPool() {
    return tabletRefreshThreadPool;
  }

  public static void main(String[] args) throws Exception {
    try (Manager manager = new Manager(new ConfigOpts(), ServerContext::new, args)) {
      manager.runServer();
    }
  }

  protected Manager(ConfigOpts opts, Function<SiteConfiguration,ServerContext> serverContextFactory,
      String[] args) throws IOException {
    super("manager", opts, serverContextFactory, args);
    ServerContext context = super.getContext();
    balancerEnvironment = new BalancerEnvironmentImpl(context);

    AccumuloConfiguration aconf = context.getConfiguration();

    log.info("Version {}", Constants.VERSION);
    log.info("Instance {}", getInstanceID());
    timeKeeper = new ManagerTime(this, aconf);
    tserverSet = new LiveTServerSet(context, this);
    initializeBalancer();

    this.security = context.getSecurityOperation();

    final long tokenLifetime = aconf.getTimeInMillis(Property.GENERAL_DELEGATION_TOKEN_LIFETIME);

    this.rootTabletStore = TabletStateStore.getStoreForLevel(DataLevel.ROOT, context);
    this.metadataTabletStore = TabletStateStore.getStoreForLevel(DataLevel.METADATA, context);
    this.userTabletStore = TabletStateStore.getStoreForLevel(DataLevel.USER, context);

    authenticationTokenKeyManager = null;
    keyDistributor = null;
    if (getConfiguration().getBoolean(Property.INSTANCE_RPC_SASL_ENABLED)) {
      // SASL is enabled, create the key distributor (ZooKeeper) and manager (generates/rolls secret
      // keys)
      log.info("SASL is enabled, creating delegation token key manager and distributor");
      final long tokenUpdateInterval =
          aconf.getTimeInMillis(Property.GENERAL_DELEGATION_TOKEN_UPDATE_INTERVAL);
      keyDistributor = new ZooAuthenticationKeyDistributor(context.getZooReaderWriter(),
          getZooKeeperRoot() + Constants.ZDELEGATION_TOKEN_KEYS);
      authenticationTokenKeyManager = new AuthenticationTokenKeyManager(context.getSecretManager(),
          keyDistributor, tokenUpdateInterval, tokenLifetime);
      delegationTokensAvailable = true;
    } else {
      log.info("SASL is not enabled, delegation tokens will not be available");
      delegationTokensAvailable = false;
    }
    this.timeToCacheRecoveryWalExistence =
        aconf.getTimeInMillis(Property.MANAGER_RECOVERY_WAL_EXISTENCE_CACHE_TIME);
  }

  public InstanceId getInstanceID() {
    return getContext().getInstanceID();
  }

  public String getZooKeeperRoot() {
    return getContext().getZooKeeperRoot();
  }

  public TServerConnection getConnection(TServerInstance server) {
    return tserverSet.getConnection(server);
  }

  void setManagerGoalState(ManagerGoalState state) {
    try {
      getContext().getZooReaderWriter().putPersistentData(
          getZooKeeperRoot() + Constants.ZMANAGER_GOAL_STATE, state.name().getBytes(UTF_8),
          NodeExistsPolicy.OVERWRITE);
    } catch (Exception ex) {
      log.error("Unable to set manager goal state in zookeeper");
    }
  }

  ManagerGoalState getManagerGoalState() {
    while (true) {
      try {
        byte[] data = getContext().getZooReaderWriter()
            .getData(getZooKeeperRoot() + Constants.ZMANAGER_GOAL_STATE);
        return ManagerGoalState.valueOf(new String(data, UTF_8));
      } catch (Exception e) {
        log.error("Problem getting real goal state from zookeeper: ", e);
        sleepUninterruptibly(1, SECONDS);
      }
    }
  }

  public void clearMigrations(TableId tableId) {
    synchronized (migrations) {
      migrations.keySet().removeIf(extent -> extent.tableId().equals(tableId));
    }
  }

  private Splitter splitter;

  public Splitter getSplitter() {
    return splitter;
  }

  public MetricsProducer getBalancerMetrics() {
    return balancerMetrics;
  }

  public UpgradeCoordinator.UpgradeStatus getUpgradeStatus() {
    return upgradeCoordinator.getStatus();
  }

  public CompactionCoordinator getCompactionCoordinator() {
    return compactionCoordinator;
  }

  public void hostOndemand(List<KeyExtent> extents) {
    extents.forEach(e -> Preconditions.checkArgument(DataLevel.of(e.tableId()) == DataLevel.USER));

    for (var watcher : watchers) {
      if (watcher.getLevel() == DataLevel.USER) {
        watcher.hostOndemand(extents);
      }
    }
  }

  private class MigrationCleanupThread implements Runnable {

    @Override
    public void run() {
      while (stillManager()) {
        if (!migrations.isEmpty()) {
          try {
            cleanupOfflineMigrations();
            cleanupNonexistentMigrations(getContext());
          } catch (Exception ex) {
            log.error("Error cleaning up migrations", ex);
          }
        }
        sleepUninterruptibly(CLEANUP_INTERVAL_MINUTES, MINUTES);
      }
    }

    /**
     * If a migrating tablet splits, and the tablet dies before sending the manager a message, the
     * migration will refer to a non-existing tablet, so it can never complete. Periodically scan
     * the metadata table and remove any migrating tablets that no longer exist.
     */
    private void cleanupNonexistentMigrations(final ClientContext clientContext) {

      Map<DataLevel,Set<KeyExtent>> notSeen;

      synchronized (migrations) {
        notSeen = partitionMigrations(migrations.keySet());
      }

      // for each level find the set of migrating tablets that do not exists in metadata store
      for (DataLevel dataLevel : DataLevel.values()) {
        var notSeenForLevel = notSeen.getOrDefault(dataLevel, Set.of());
        if (notSeenForLevel.isEmpty() || dataLevel == DataLevel.ROOT) {
          // No need to scan this level if there are no migrations. The root tablet is always
          // expected to exists, so no need to read its metadata.
          continue;
        }

        try (var tablets = clientContext.getAmple().readTablets().forLevel(dataLevel)
            .fetch(TabletMetadata.ColumnType.PREV_ROW).build()) {
          // A goal of this code is to avoid reading all extents in the metadata table into memory
          // when finding extents that exists in the migrating set and not in the metadata table.
          tablets.forEach(tabletMeta -> notSeenForLevel.remove(tabletMeta.getExtent()));
        }

        // remove any tablets that previously existed in migrations for this level but were not seen
        // in the metadata table for the level
        migrations.keySet().removeAll(notSeenForLevel);
      }
    }

    /**
     * If migrating a tablet for a table that is offline, the migration can never succeed because no
     * tablet server will load the tablet. check for offline tables and remove their migrations.
     */
    private void cleanupOfflineMigrations() {
      ServerContext context = getContext();
      TableManager manager = context.getTableManager();
      for (TableId tableId : context.getTableIdToNameMap().keySet()) {
        TableState state = manager.getTableState(tableId);
        if (state == TableState.OFFLINE) {
          clearMigrations(tableId);
        }
      }
    }
  }

  private class ScanServerZKCleaner implements Runnable {

    @Override
    public void run() {

      final ZooReaderWriter zrw = getContext().getZooReaderWriter();

      while (stillManager()) {
        try {
          Set<ServiceLockPath> scanServerPaths =
              getContext().getServerPaths().getScanServer(rg -> true, AddressSelector.all(), false);
          for (ServiceLockPath path : scanServerPaths) {

            ZcStat stat = new ZcStat();
            Optional<ServiceLockData> lockData =
                ServiceLock.getLockData(getContext().getZooCache(), path, stat);

            if (lockData.isEmpty()) {
              try {
                log.debug("Deleting empty ScanServer ZK node {}", path);
                zrw.delete(path.toString());
              } catch (KeeperException.NotEmptyException e) {
                log.debug(
                    "Failed to delete ScanServer ZK node {} its not empty, likely an expected race condition.",
                    path);
              }
            }
          }
        } catch (KeeperException e) {
          log.error("Exception trying to delete empty scan server ZNodes, will retry", e);
        } catch (InterruptedException e) {
          Thread.interrupted();
          log.error("Interrupted trying to delete empty scan server ZNodes, will retry", e);
        } finally {
          // sleep for 5 mins
          sleepUninterruptibly(CLEANUP_INTERVAL_MINUTES, MINUTES);
        }
      }
    }

  }

  /**
   * balanceTablets() balances tables by DataLevel. Return the current set of migrations partitioned
   * by DataLevel
   */
  private static Map<DataLevel,Set<KeyExtent>>
      partitionMigrations(final Set<KeyExtent> migrations) {
    final Map<DataLevel,Set<KeyExtent>> partitionedMigrations = new EnumMap<>(DataLevel.class);
    // populate to prevent NPE
    for (DataLevel dl : DataLevel.values()) {
      partitionedMigrations.put(dl, new HashSet<>());
    }
    migrations.forEach(ke -> {
      partitionedMigrations.get(DataLevel.of(ke.tableId())).add(ke);
    });
    return partitionedMigrations;
  }

  private class StatusThread implements Runnable {

    private boolean goodStats() {
      int start;
      switch (getManagerState()) {
        case UNLOAD_METADATA_TABLETS:
          start = 1;
          break;
        case UNLOAD_ROOT_TABLET:
          start = 2;
          break;
        default:
          start = 0;
      }
      for (int i = start; i < watchers.size(); i++) {
        TabletGroupWatcher watcher = watchers.get(i);
        if (watcher.stats.getLastManagerState() != getManagerState()) {
          log.debug("{}: {} != {}", watcher.getName(), watcher.stats.getLastManagerState(),
              getManagerState());
          return false;
        }
      }
      return true;
    }

    @Override
    public void run() {
      EventCoordinator.Tracker eventTracker = nextEvent.getTracker();
      while (stillManager()) {
        long wait;
        try {
          switch (getManagerGoalState()) {
            case NORMAL:
              setManagerState(ManagerState.NORMAL);
              break;
            case SAFE_MODE:
              if (getManagerState() == ManagerState.NORMAL
                  || getManagerState() == ManagerState.HAVE_LOCK) {
                setManagerState(ManagerState.SAFE_MODE);
              }
              break;
            case CLEAN_STOP:
              switch (getManagerState()) {
                case NORMAL:
                  // USER fate stores its data in a user table and its operations may interact with
                  // all tables, need to completely shut it down before unloading user tablets
                  fate(FateInstanceType.USER).shutdown(1, MINUTES);
                  setManagerState(ManagerState.SAFE_MODE);
                  break;
                case SAFE_MODE: {
                  // META fate stores its data in Zookeeper and its operations interact with
                  // metadata and root tablets, need to completely shut it down before unloading
                  // metadata and root tablets
                  fate(FateInstanceType.META).shutdown(1, MINUTES);
                  int count = nonMetaDataTabletsAssignedOrHosted();
                  log.debug(
                      String.format("There are %d non-metadata tablets assigned or hosted", count));
                  if (count == 0 && goodStats()) {
                    setManagerState(ManagerState.UNLOAD_METADATA_TABLETS);
                  }
                }
                  break;
                case UNLOAD_METADATA_TABLETS: {
                  int count = assignedOrHosted(AccumuloTable.METADATA.tableId());
                  log.debug(
                      String.format("There are %d metadata tablets assigned or hosted", count));
                  if (count == 0 && goodStats()) {
                    setManagerState(ManagerState.UNLOAD_ROOT_TABLET);
                  }
                }
                  break;
                case UNLOAD_ROOT_TABLET:
                  int count = assignedOrHosted(AccumuloTable.METADATA.tableId());
                  if (count > 0 && goodStats()) {
                    log.debug(String.format("%d metadata tablets online", count));
                    setManagerState(ManagerState.UNLOAD_ROOT_TABLET);
                  }
                  int root_count = assignedOrHosted(AccumuloTable.ROOT.tableId());
                  if (root_count > 0 && goodStats()) {
                    log.debug("The root tablet is still assigned or hosted");
                  }
                  if (count + root_count == 0 && goodStats()) {
                    Set<TServerInstance> currentServers = tserverSet.getCurrentServers();
                    log.debug("stopping {} tablet servers", currentServers.size());
                    for (TServerInstance server : currentServers) {
                      try {
                        serversToShutdown.add(server);
                        tserverSet.getConnection(server).fastHalt(managerLock);
                      } catch (TException e) {
                        // its probably down, and we don't care
                      } finally {
                        tserverSet.remove(server);
                      }
                    }
                    if (currentServers.isEmpty()) {
                      setManagerState(ManagerState.STOP);
                    }
                  }
                  break;
                default:
                  break;
              }
          }
        } catch (Exception t) {
          log.error("Error occurred reading / switching manager goal state. Will"
              + " continue with attempt to update status", t);
        }

        Span span = TraceUtil.startSpan(this.getClass(), "run::updateStatus");
        try (Scope scope = span.makeCurrent()) {
          wait = updateStatus();
          eventTracker.waitForEvents(wait);
        } catch (Exception t) {
          TraceUtil.setException(span, t, false);
          log.error("Error balancing tablets, will wait for {} (seconds) and then retry ",
              WAIT_BETWEEN_ERRORS / ONE_SECOND, t);
          sleepUninterruptibly(WAIT_BETWEEN_ERRORS, MILLISECONDS);
        } finally {
          span.end();
        }
      }
    }

    private long updateStatus() {
      var tseversSnapshot = tserverSet.getSnapshot();
      tserverStatus = gatherTableInformation(tseversSnapshot.getTservers());
      tServerGroupingForBalancer = tseversSnapshot.getTserverGroups();

      checkForHeldServer(tserverStatus);

      if (!badServers.isEmpty()) {
        log.debug("not balancing because the balance information is out-of-date {}",
            badServers.keySet());
      } else if (getManagerGoalState() == ManagerGoalState.CLEAN_STOP) {
        log.debug("not balancing because the manager is attempting to stop cleanly");
      } else if (!serversToShutdown.isEmpty()) {
        log.debug("not balancing while shutting down servers {}", serversToShutdown);
      } else {
        for (TabletGroupWatcher tgw : watchers) {
          if (!tgw.isSameTserversAsLastScan(tseversSnapshot.getTservers())) {
            log.debug("not balancing just yet, as collection of live tservers is in flux");
            return DEFAULT_WAIT_FOR_WATCHER;
          }
        }
        return balanceTablets();
      }
      return DEFAULT_WAIT_FOR_WATCHER;
    }

    private void checkForHeldServer(SortedMap<TServerInstance,TabletServerStatus> tserverStatus) {
      TServerInstance instance = null;
      int crazyHoldTime = 0;
      int someHoldTime = 0;
      final long maxWait = getConfiguration().getTimeInMillis(Property.TSERV_HOLD_TIME_SUICIDE);
      for (Entry<TServerInstance,TabletServerStatus> entry : tserverStatus.entrySet()) {
        if (entry.getValue().getHoldTime() > 0) {
          someHoldTime++;
          if (entry.getValue().getHoldTime() > maxWait) {
            instance = entry.getKey();
            crazyHoldTime++;
          }
        }
      }
      if (crazyHoldTime == 1 && someHoldTime == 1 && tserverStatus.size() > 1) {
        log.warn("Tablet server {} exceeded maximum hold time: attempting to kill it", instance);
        try {
          TServerConnection connection = tserverSet.getConnection(instance);
          if (connection != null) {
            connection.fastHalt(managerLock);
          }
        } catch (TException e) {
          log.error("{}", e.getMessage(), e);
        }
        badServers.putIfAbsent(instance, new AtomicInteger(1));
      }
    }

    /**
     * Given the current tserverStatus map and a DataLevel, return a view of the tserverStatus map
     * that only contains entries for tables in the DataLevel
     */
    private SortedMap<TServerInstance,TabletServerStatus> createTServerStatusView(
        final DataLevel dl, final SortedMap<TServerInstance,TabletServerStatus> status) {
      final SortedMap<TServerInstance,TabletServerStatus> tserverStatusForLevel = new TreeMap<>();
      status.forEach((tsi, tss) -> {
        final TabletServerStatus copy = tss.deepCopy();
        final Map<String,TableInfo> oldTableMap = copy.getTableMap();
        final Map<String,TableInfo> newTableMap =
            new HashMap<>(dl == DataLevel.USER ? oldTableMap.size() : 1);
        if (dl == DataLevel.ROOT) {
          if (oldTableMap.containsKey(AccumuloTable.ROOT.tableName())) {
            newTableMap.put(AccumuloTable.ROOT.tableName(),
                oldTableMap.get(AccumuloTable.ROOT.tableName()));
          }
        } else if (dl == DataLevel.METADATA) {
          if (oldTableMap.containsKey(AccumuloTable.METADATA.tableName())) {
            newTableMap.put(AccumuloTable.METADATA.tableName(),
                oldTableMap.get(AccumuloTable.METADATA.tableName()));
          }
        } else if (dl == DataLevel.USER) {
          if (!oldTableMap.containsKey(AccumuloTable.METADATA.tableName())
              && !oldTableMap.containsKey(AccumuloTable.ROOT.tableName())) {
            newTableMap.putAll(oldTableMap);
          } else {
            oldTableMap.forEach((table, info) -> {
              if (!table.equals(AccumuloTable.ROOT.tableName())
                  && !table.equals(AccumuloTable.METADATA.tableName())) {
                newTableMap.put(table, info);
              }
            });
          }
        } else {
          throw new IllegalArgumentException("Unhandled DataLevel value: " + dl);
        }
        copy.setTableMap(newTableMap);
        tserverStatusForLevel.put(tsi, copy);
      });
      return tserverStatusForLevel;
    }

    private long balanceTablets() {

      final int tabletsNotHosted = notHosted();
      BalanceParamsImpl params = null;
      long wait = 0;
      long totalMigrationsOut = 0;
      final Map<DataLevel,Set<KeyExtent>> partitionedMigrations =
          partitionMigrations(migrationsSnapshot().keySet());
      int levelsCompleted = 0;

      for (DataLevel dl : DataLevel.values()) {
        if (dl == DataLevel.USER && tabletsNotHosted > 0) {
          log.debug("not balancing user tablets because there are {} unhosted tablets",
              tabletsNotHosted);
          continue;
        }
        // Create a view of the tserver status such that it only contains the tables
        // for this level in the tableMap.
        final SortedMap<TServerInstance,TabletServerStatus> tserverStatusForLevel =
            createTServerStatusView(dl, tserverStatus);
        // Construct the Thrift variant of the map above for the BalancerParams
        final SortedMap<TabletServerId,TServerStatus> tserverStatusForBalancerLevel =
            new TreeMap<>();
        tserverStatusForLevel.forEach((tsi, status) -> tserverStatusForBalancerLevel
            .put(new TabletServerIdImpl(tsi), TServerStatusImpl.fromThrift(status)));

        long migrationsOutForLevel = 0;
        int attemptNum = 0;
        do {
          log.debug("Balancing for tables at level {}, times-in-loop: {}", dl, ++attemptNum);
          params = BalanceParamsImpl.fromThrift(tserverStatusForBalancerLevel,
              tServerGroupingForBalancer, tserverStatusForLevel, partitionedMigrations.get(dl));
          wait = Math.max(tabletBalancer.balance(params), wait);
          migrationsOutForLevel = params.migrationsOut().size();
          for (TabletMigration m : checkMigrationSanity(tserverStatusForBalancerLevel.keySet(),
              params.migrationsOut())) {
            final KeyExtent ke = KeyExtent.fromTabletId(m.getTablet());
            if (migrations.containsKey(ke)) {
              log.warn("balancer requested migration more than once, skipping {}", m);
              continue;
            }
            migrations.put(ke, TabletServerIdImpl.toThrift(m.getNewTabletServer()));
            log.debug("migration {}", m);
          }
        } while (migrationsOutForLevel > 0 && (dl == DataLevel.ROOT || dl == DataLevel.METADATA));
        totalMigrationsOut += migrationsOutForLevel;

        // increment this at end of loop to signal complete run w/o any continue
        levelsCompleted++;
      }
      balancerMetrics.assignMigratingCount(migrations::size);

      if (totalMigrationsOut == 0 && levelsCompleted == DataLevel.values().length) {
        synchronized (balancedNotifier) {
          balancedNotifier.notifyAll();
        }
      } else if (totalMigrationsOut > 0) {
        nextEvent.event("Migrating %d more tablets, %d total", totalMigrationsOut,
            migrations.size());
      }
      return wait;
    }

    private List<TabletMigration> checkMigrationSanity(Set<TabletServerId> current,
        List<TabletMigration> migrations) {
      return migrations.stream().filter(m -> {
        boolean includeMigration = false;
        if (m.getTablet() == null) {
          log.error("Balancer gave back a null tablet {}", m);
        } else if (m.getNewTabletServer() == null) {
          log.error("Balancer did not set the destination {}", m);
        } else if (m.getOldTabletServer() == null) {
          log.error("Balancer did not set the source {}", m);
        } else if (!current.contains(m.getOldTabletServer())) {
          log.warn("Balancer wants to move a tablet from a server that is not current: {}", m);
        } else if (!current.contains(m.getNewTabletServer())) {
          log.warn("Balancer wants to move a tablet to a server that is not current: {}", m);
        } else {
          includeMigration = true;
        }
        return includeMigration;
      }).collect(Collectors.toList());
    }

  }

  private SortedMap<TServerInstance,TabletServerStatus>
      gatherTableInformation(Set<TServerInstance> currentServers) {
    final long rpcTimeout = getConfiguration().getTimeInMillis(Property.GENERAL_RPC_TIMEOUT);
    int threads = getConfiguration().getCount(Property.MANAGER_STATUS_THREAD_POOL_SIZE);
    long start = System.currentTimeMillis();
    final SortedMap<TServerInstance,TabletServerStatus> result = new ConcurrentSkipListMap<>();
    final RateLimiter shutdownServerRateLimiter = RateLimiter.create(MAX_SHUTDOWNS_PER_SEC);
    final ArrayList<Future<?>> tasks = new ArrayList<>();
    for (TServerInstance serverInstance : currentServers) {
      final TServerInstance server = serverInstance;
      if (threads == 0) {
        // Since an unbounded thread pool is being used, rate limit how fast task are added to the
        // executor. This prevents the threads from growing large unless there are lots of
        // unresponsive tservers.
        sleepUninterruptibly(Math.max(1, rpcTimeout / 120_000), MILLISECONDS);
      }
      tasks.add(tableInformationStatusPool.submit(() -> {
        try {
          Thread t = Thread.currentThread();
          String oldName = t.getName();
          try {
            String message = "Getting status from " + server;
            t.setName(message);
            long startForServer = System.currentTimeMillis();
            log.trace(message);
            TServerConnection connection1 = tserverSet.getConnection(server);
            if (connection1 == null) {
              throw new IOException("No connection to " + server);
            }
            TabletServerStatus status = connection1.getTableMap(false);
            result.put(server, status);

            long duration = System.currentTimeMillis() - startForServer;
            log.trace("Got status from {} in {} ms", server, duration);

          } finally {
            t.setName(oldName);
          }
        } catch (Exception ex) {
          log.error("unable to get tablet server status {} {}", server, ex.toString());
          log.debug("unable to get tablet server status {}", server, ex);
          // Attempt to shutdown server only if able to acquire. If unable, this tablet server
          // will be removed from the badServers set below and status will be reattempted again
          // MAX_BAD_STATUS_COUNT times
          if (badServers.computeIfAbsent(server, k -> new AtomicInteger(0)).incrementAndGet()
              > MAX_BAD_STATUS_COUNT) {
            if (shutdownServerRateLimiter.tryAcquire()) {
              log.warn("attempting to stop {}", server);
              try {
                TServerConnection connection2 = tserverSet.getConnection(server);
                if (connection2 != null) {
                  connection2.halt(managerLock);
                }
              } catch (TTransportException e1) {
                // ignore: it's probably down
              } catch (Exception e2) {
                log.info("error talking to troublesome tablet server", e2);
              }
            } else {
              log.warn("Unable to shutdown {} as over the shutdown limit of {} per minute", server,
                  MAX_SHUTDOWNS_PER_SEC * 60);
            }
            badServers.remove(server);
          }
        }
      }));
    }
    // wait at least 10 seconds
    final Duration timeToWait =
        Comparators.max(Duration.ofSeconds(10), Duration.ofMillis(rpcTimeout / 3));
    final Timer startTime = Timer.startNew();
    // Wait for all tasks to complete
    while (!tasks.isEmpty()) {
      boolean cancel = startTime.hasElapsed(timeToWait);
      Iterator<Future<?>> iter = tasks.iterator();
      while (iter.hasNext()) {
        Future<?> f = iter.next();
        if (cancel) {
          f.cancel(true);
        } else {
          if (f.isDone()) {
            iter.remove();
          }
        }
      }
      Uninterruptibles.sleepUninterruptibly(1, MILLISECONDS);
    }

    // Threads may still modify map after shutdownNow is called, so create an immutable snapshot.
    SortedMap<TServerInstance,TabletServerStatus> info = ImmutableSortedMap.copyOf(result);

    synchronized (badServers) {
      badServers.keySet().retainAll(currentServers);
      badServers.keySet().removeAll(info.keySet());
    }
    log.debug(String.format("Finished gathering information from %d of %d servers in %.2f seconds",
        info.size(), currentServers.size(), (System.currentTimeMillis() - start) / 1000.));

    return info;
  }

  @Override
  public void run() {
    final ServerContext context = getContext();
    final String zroot = getZooKeeperRoot();

    // ACCUMULO-4424 Put up the Thrift servers before getting the lock as a sign of process health
    // when a hot-standby
    //
    // Start the Manager's Fate Service
    fateServiceHandler = new FateServiceHandler(this);
    managerClientHandler = new ManagerClientServiceHandler(this);
    compactionCoordinator = new CompactionCoordinator(context, security, fateRefs, this);

    // Start the Manager's Client service
    // Ensure that calls before the manager gets the lock fail
    ManagerClientService.Iface haProxy =
        HighlyAvailableServiceWrapper.service(managerClientHandler, this);

    ServerAddress sa;
    MetricServiceHandler metricHandler = createMetricServiceHandler(MetricSource.MANAGER);
    var processor = ThriftProcessorTypes.getManagerTProcessor(fateServiceHandler,
        compactionCoordinator.getThriftService(), haProxy, metricHandler, getContext());

    try {
      sa = TServerUtils.startServer(context, getHostname(), Property.MANAGER_CLIENTPORT, processor,
          "Manager", "Manager Client Service Handler", null, Property.MANAGER_MINTHREADS,
          Property.MANAGER_MINTHREADS_TIMEOUT, Property.MANAGER_THREADCHECK);
    } catch (UnknownHostException e) {
      throw new IllegalStateException("Unable to start server on host " + getHostname(), e);
    }
    clientService = sa.server;
    metricHandler.setHost(sa.address);
    log.info("Started Manager client service at {}", sa.address);

    // block until we can obtain the ZK lock for the manager
    ServiceLockData sld;
    try {
      sld = getManagerLock(context.getServerPaths().createManagerPath());
    } catch (KeeperException | InterruptedException e) {
      throw new IllegalStateException("Exception getting manager lock", e);
    }

    MetricsInfo metricsInfo = getContext().getMetricsInfo();
    ManagerMetrics managerMetrics = new ManagerMetrics(getConfiguration(), this);
    var producers = managerMetrics.getProducers(getConfiguration(), this);
    producers.add(balancerMetrics);

    metricsInfo.addMetricsProducers(producers.toArray(new MetricsProducer[0]));
    metricsInfo.init(MetricsInfo.serviceTags(getContext().getInstanceName(), getApplicationName(),
        sa.getAddress(), getResourceGroup()));

    recoveryManager = new RecoveryManager(this, timeToCacheRecoveryWalExistence);

    context.getTableManager().addObserver(this);

    tableInformationStatusPool = ThreadPools.getServerThreadPools()
        .createExecutorService(getConfiguration(), Property.MANAGER_STATUS_THREAD_POOL_SIZE, false);

    tabletRefreshThreadPool = ThreadPools.getServerThreadPools().getPoolBuilder("Tablet refresh ")
        .numCoreThreads(getConfiguration().getCount(Property.MANAGER_TABLET_REFRESH_MINTHREADS))
        .numMaxThreads(getConfiguration().getCount(Property.MANAGER_TABLET_REFRESH_MAXTHREADS))
        .build();

    Thread statusThread = Threads.createThread("Status Thread", new StatusThread());
    statusThread.start();

    Threads.createThread("Migration Cleanup Thread", new MigrationCleanupThread()).start();

    tserverSet.startListeningForTabletServerChanges();

    Threads.createThread("ScanServer Cleanup Thread", new ScanServerZKCleaner()).start();

    try {
      blockForTservers();
    } catch (InterruptedException ex) {
      Thread.currentThread().interrupt();
    }

    // Don't call start the CompactionCoordinator until we have tservers.
    compactionCoordinator.start();

    ZooReaderWriter zReaderWriter = context.getZooReaderWriter();

    try {
      zReaderWriter.getChildren(zroot + Constants.ZRECOVERY, new Watcher() {
        @Override
        public void process(WatchedEvent event) {
          nextEvent.event("Noticed recovery changes %s", event.getType());
          try {
            // watcher only fires once, add it back
            zReaderWriter.getChildren(zroot + Constants.ZRECOVERY, this);
          } catch (Exception e) {
            log.error("Failed to add log recovery watcher back", e);
          }
        }
      });
    } catch (KeeperException | InterruptedException e) {
      throw new IllegalStateException("Unable to read " + zroot + Constants.ZRECOVERY, e);
    }

    this.splitter = new Splitter(context);
    this.splitter.start();

    watchers.add(new TabletGroupWatcher(this, this.userTabletStore, null, managerMetrics) {
      @Override
      boolean canSuspendTablets() {
        // Always allow user data tablets to enter suspended state.
        return true;
      }
    });

    watchers.add(
        new TabletGroupWatcher(this, this.metadataTabletStore, watchers.get(0), managerMetrics) {
          @Override
          boolean canSuspendTablets() {
            // Allow metadata tablets to enter suspended state only if so configured. Generally
            // we'll want metadata tablets to
            // be immediately reassigned, even if there's a global table.suspension.duration
            // setting.
            return getConfiguration().getBoolean(Property.MANAGER_METADATA_SUSPENDABLE);
          }
        });

    watchers
        .add(new TabletGroupWatcher(this, this.rootTabletStore, watchers.get(1), managerMetrics) {
          @Override
          boolean canSuspendTablets() {
            // Never allow root tablet to enter suspended state.
            return false;
          }
        });
    for (TabletGroupWatcher watcher : watchers) {
      watcher.start();
    }

    // Once we are sure the upgrade is complete, we can safely allow fate use.
    try {
      // wait for metadata upgrade running in background to complete
      if (upgradeMetadataFuture != null) {
        upgradeMetadataFuture.get();
      }
    } catch (ExecutionException | InterruptedException e) {
      throw new IllegalStateException("Metadata upgrade failed", e);
    }

    // Everything should be fully upgraded by this point, but check before starting fate
    if (isUpgrading()) {
      throw new IllegalStateException("Upgrade coordinator is unexpectedly not complete");
    }
    try {
      Predicate<ZooUtil.LockID> isLockHeld =
          lock -> ServiceLock.isLockHeld(context.getZooCache(), lock);
      var metaInstance =
          initializeFateInstance(context, new MetaFateStore<>(getZooKeeperRoot() + Constants.ZFATE,
              context.getZooReaderWriter(), managerLock.getLockID(), isLockHeld));
      var userInstance = initializeFateInstance(context, new UserFateStore<>(context,
          AccumuloTable.FATE.tableName(), managerLock.getLockID(), isLockHeld));

      if (!fateRefs.compareAndSet(null,
          Map.of(FateInstanceType.META, metaInstance, FateInstanceType.USER, userInstance))) {
        throw new IllegalStateException(
            "Unexpected previous fate reference map already initialized");
      }
      fateReadyLatch.countDown();
    } catch (KeeperException | InterruptedException e) {
      throw new IllegalStateException("Exception setting up FaTE cleanup thread", e);
    }

    ThreadPools.watchCriticalScheduledTask(context.getScheduledExecutor()
        .scheduleWithFixedDelay(() -> ScanServerMetadataEntries.clean(context), 10, 10, MINUTES));

    // Make sure that we have a secret key (either a new one or an old one from ZK) before we start
    // the manager client service.
    Thread authenticationTokenKeyManagerThread = null;
    if (authenticationTokenKeyManager != null && keyDistributor != null) {
      log.info("Starting delegation-token key manager");
      try {
        keyDistributor.initialize();
      } catch (KeeperException | InterruptedException e) {
        throw new IllegalStateException("Exception setting up delegation-token key manager", e);
      }
      authenticationTokenKeyManagerThread =
          Threads.createThread("Delegation Token Key Manager", authenticationTokenKeyManager);
      authenticationTokenKeyManagerThread.start();
      boolean logged = false;
      while (!authenticationTokenKeyManager.isInitialized()) {
        // Print out a status message when we start waiting for the key manager to get initialized
        if (!logged) {
          log.info("Waiting for AuthenticationTokenKeyManager to be initialized");
          logged = true;
        }
        sleepUninterruptibly(200, MILLISECONDS);
      }
      // And log when we are initialized
      log.info("AuthenticationTokenSecretManager is initialized");
    }

    String address = sa.address.toString();
    UUID uuid = sld.getServerUUID(ThriftService.MANAGER);
    ServiceDescriptors descriptors = new ServiceDescriptors();
    for (ThriftService svc : new ThriftService[] {ThriftService.MANAGER, ThriftService.COORDINATOR,
        ThriftService.FATE}) {
      descriptors.addService(new ServiceDescriptor(uuid, svc, address, this.getResourceGroup()));
    }

    sld = new ServiceLockData(descriptors);
    log.info("Setting manager lock data to {}", sld);
    try {
      managerLock.replaceLockData(sld);
    } catch (KeeperException | InterruptedException e) {
      throw new IllegalStateException("Exception updating manager lock", e);
    }

    while (!clientService.isServing()) {
      sleepUninterruptibly(100, MILLISECONDS);
    }

    // The manager is fully initialized. Clients are allowed to connect now.
    managerInitialized.set(true);

    while (clientService.isServing()) {
      sleepUninterruptibly(500, MILLISECONDS);
    }
    log.info("Shutting down fate.");
    getFateRefs().keySet().forEach(type -> fate(type).shutdown(0, MINUTES));

    splitter.stop();

    final long deadline = System.currentTimeMillis() + MAX_CLEANUP_WAIT_TIME;
    try {
      statusThread.join(remaining(deadline));
    } catch (InterruptedException e) {
      throw new IllegalStateException("Exception stopping status thread", e);
    }

    tableInformationStatusPool.shutdownNow();
    tabletRefreshThreadPool.shutdownNow();

    compactionCoordinator.shutdown();

    // Signal that we want it to stop, and wait for it to do so.
    if (authenticationTokenKeyManager != null) {
      authenticationTokenKeyManager.gracefulStop();
      try {
        if (null != authenticationTokenKeyManagerThread) {
          authenticationTokenKeyManagerThread.join(remaining(deadline));
        }
      } catch (InterruptedException e) {
        throw new IllegalStateException("Exception waiting on delegation-token key manager", e);
      }
    }

    // quit, even if the tablet servers somehow jam up and the watchers
    // don't stop
    for (TabletGroupWatcher watcher : watchers) {
      try {
        watcher.join(remaining(deadline));
      } catch (InterruptedException e) {
        throw new IllegalStateException("Exception waiting on watcher", e);
      }
    }
    log.info("exiting");
  }

  protected Fate<Manager> initializeFateInstance(ServerContext context, FateStore<Manager> store) {

    final Fate<Manager> fateInstance =
        new Fate<>(this, store, true, TraceRepo::toLogString, getConfiguration());

    var fateCleaner = new FateCleaner<>(store, Duration.ofHours(8), this::getSteadyTime);
    ThreadPools.watchCriticalScheduledTask(context.getScheduledExecutor()
        .scheduleWithFixedDelay(fateCleaner::ageOff, 10, 4 * 60, MINUTES));

    return fateInstance;
  }

  /**
   * Allows property configuration to block manager start-up waiting for a minimum number of
   * tservers to register in zookeeper. It also accepts a maximum time to wait - if the time
   * expires, the start-up will continue with any tservers available. This check is only performed
   * at manager initialization, when the manager acquires the lock. The following properties are
   * used to control the behaviour:
   * <ul>
   * <li>MANAGER_STARTUP_TSERVER_AVAIL_MIN_COUNT - when set to 0 or less, no blocking occurs
   * (default behaviour) otherwise will block until the number of tservers are available.</li>
   * <li>MANAGER_STARTUP_TSERVER_AVAIL_MAX_WAIT - time to wait in milliseconds. When set to 0 or
   * less, will block indefinitely.</li>
   * </ul>
   *
   * @throws InterruptedException if interrupted while blocking, propagated for caller to handle.
   */
  private void blockForTservers() throws InterruptedException {
    long waitStart = System.nanoTime();

    long minTserverCount =
        getConfiguration().getCount(Property.MANAGER_STARTUP_TSERVER_AVAIL_MIN_COUNT);

    if (minTserverCount <= 0) {
      log.info("tserver availability check disabled, continuing with-{} servers. To enable, set {}",
          tserverSet.size(), Property.MANAGER_STARTUP_TSERVER_AVAIL_MIN_COUNT.getKey());
      return;
    }
    long userWait = MILLISECONDS.toSeconds(
        getConfiguration().getTimeInMillis(Property.MANAGER_STARTUP_TSERVER_AVAIL_MAX_WAIT));

    // Setting retry values for defined wait timeouts
    long retries = 10;
    // Set these to the same value so the max possible wait time always matches the provided maxWait
    long initialWait = userWait / retries;
    long maxWaitPeriod = initialWait;
    long waitIncrement = 0;

    if (userWait <= 0) {
      log.info("tserver availability check set to block indefinitely, To change, set {} > 0.",
          Property.MANAGER_STARTUP_TSERVER_AVAIL_MAX_WAIT.getKey());
      userWait = Long.MAX_VALUE;

      // If indefinitely blocking, change retry values to support incremental backoff and logging.
      retries = userWait;
      initialWait = 1;
      maxWaitPeriod = 30;
      waitIncrement = 5;
    }

    Retry tserverRetry = Retry.builder().maxRetries(retries)
        .retryAfter(Duration.ofSeconds(initialWait)).incrementBy(Duration.ofSeconds(waitIncrement))
        .maxWait(Duration.ofSeconds(maxWaitPeriod)).backOffFactor(1)
        .logInterval(Duration.ofSeconds(30)).createRetry();

    log.info("Checking for tserver availability - need to reach {} servers. Have {}",
        minTserverCount, tserverSet.size());

    boolean needTservers = tserverSet.size() < minTserverCount;

    while (needTservers && tserverRetry.canRetry()) {

      tserverRetry.waitForNextAttempt(log, "block until minimum tservers reached");

      needTservers = tserverSet.size() < minTserverCount;

      // suppress last message once threshold reached.
      if (needTservers) {
        tserverRetry.logRetry(log, String.format(
            "Blocking for tserver availability - need to reach %s servers. Have %s Time spent blocking %s seconds.",
            minTserverCount, tserverSet.size(),
            NANOSECONDS.toSeconds(System.nanoTime() - waitStart)));
      }
      tserverRetry.useRetry();
    }

    if (tserverSet.size() < minTserverCount) {
      log.warn(
          "tserver availability check time expired - continuing. Requested {}, have {} tservers on line. "
              + " Time waiting {} sec",
          tserverSet.size(), minTserverCount, NANOSECONDS.toSeconds(System.nanoTime() - waitStart));

    } else {
      log.info(
          "tserver availability check completed. Requested {}, have {} tservers on line. "
              + " Time waiting {} sec",
          tserverSet.size(), minTserverCount, NANOSECONDS.toSeconds(System.nanoTime() - waitStart));
    }
  }

  private long remaining(long deadline) {
    return Math.max(1, deadline - System.currentTimeMillis());
  }

  public ServiceLock getManagerLock() {
    return managerLock;
  }

  private static class ManagerLockWatcher implements ServiceLock.AccumuloLockWatcher {

    boolean acquiredLock = false;
    boolean failedToAcquireLock = false;

    @Override
    public void lostLock(LockLossReason reason) {
      Halt.halt("Manager lock in zookeeper lost (reason = " + reason + "), exiting!", -1);
    }

    @Override
    public void unableToMonitorLockNode(final Exception e) {
      // ACCUMULO-3651 Changed level to error and added FATAL to message for slf4j compatibility
      Halt.halt(-1, () -> log.error("FATAL: No longer able to monitor manager lock node", e));

    }

    @Override
    public synchronized void acquiredLock() {
      log.debug("Acquired manager lock");

      if (acquiredLock || failedToAcquireLock) {
        Halt.halt("Zoolock in unexpected state AL " + acquiredLock + " " + failedToAcquireLock, -1);
      }

      acquiredLock = true;
      notifyAll();
    }

    @Override
    public synchronized void failedToAcquireLock(Exception e) {
      log.warn("Failed to get manager lock", e);

      if (e instanceof NoAuthException) {
        String msg = "Failed to acquire manager lock due to incorrect ZooKeeper authentication.";
        log.error("{} Ensure instance.secret is consistent across Accumulo configuration", msg, e);
        Halt.halt(msg, -1);
      }

      if (acquiredLock) {
        Halt.halt("Zoolock in unexpected state acquiredLock true with FAL " + failedToAcquireLock,
            -1);
      }

      failedToAcquireLock = true;
      notifyAll();
    }

    public synchronized void waitForChange() {
      while (!acquiredLock && !failedToAcquireLock) {
        try {
          wait();
        } catch (InterruptedException e) {
          // empty
        }
      }
    }
  }

  private ServiceLockData getManagerLock(final ServiceLockPath zManagerLoc)
      throws KeeperException, InterruptedException {
    var zooKeeper = getContext().getZooReaderWriter().getZooKeeper();
    log.info("trying to get manager lock");

    final String managerClientAddress =
        getHostname() + ":" + getConfiguration().getPort(Property.MANAGER_CLIENTPORT)[0];

    UUID zooLockUUID = UUID.randomUUID();

    ServiceDescriptors descriptors = new ServiceDescriptors();
    descriptors.addService(new ServiceDescriptor(zooLockUUID, ThriftService.MANAGER,
        managerClientAddress, this.getResourceGroup()));

    ServiceLockData sld = new ServiceLockData(descriptors);

    managerLock = new ServiceLock(zooKeeper, zManagerLoc, zooLockUUID);

    while (true) {

      ManagerLockWatcher managerLockWatcher = new ManagerLockWatcher();
      managerLock.lock(managerLockWatcher, sld);

      managerLockWatcher.waitForChange();

      if (managerLockWatcher.acquiredLock) {
        break;
      }

      if (!managerLockWatcher.failedToAcquireLock) {
        throw new IllegalStateException("manager lock in unknown state");
      }

      managerLock.tryToCancelAsyncLockOrUnlock();

      sleepUninterruptibly(TIME_TO_WAIT_BETWEEN_LOCK_CHECKS, MILLISECONDS);
    }

    this.getContext().setServiceLock(getManagerLock());
    setManagerState(ManagerState.HAVE_LOCK);
    return sld;
  }

  @Override
  public void update(LiveTServerSet current, Set<TServerInstance> deleted,
      Set<TServerInstance> added) {

    // if we have deleted or added tservers, then adjust our dead server list
    if (!deleted.isEmpty() || !added.isEmpty()) {
      DeadServerList obit = new DeadServerList(getContext());
      if (!added.isEmpty()) {
        log.info("New servers: {}", added);
        for (TServerInstance up : added) {
          obit.delete(up.getHostPort());
        }
      }
      for (TServerInstance dead : deleted) {
        String cause = "unexpected failure";
        if (serversToShutdown.contains(dead)) {
          cause = "clean shutdown"; // maybe an incorrect assumption
        }
        if (!getManagerGoalState().equals(ManagerGoalState.CLEAN_STOP)) {
          obit.post(dead.getHostPort(), cause);
        }
      }

      Set<TServerInstance> unexpected = new HashSet<>(deleted);
      unexpected.removeAll(this.serversToShutdown);
      if (!unexpected.isEmpty()
          && (stillManager() && !getManagerGoalState().equals(ManagerGoalState.CLEAN_STOP))) {
        log.warn("Lost servers {}", unexpected);
      }
      serversToShutdown.removeAll(deleted);
      badServers.keySet().removeAll(deleted);
      // clear out any bad server with the same host/port as a new server
      synchronized (badServers) {
        cleanListByHostAndPort(badServers.keySet(), deleted, added);
      }
      synchronized (serversToShutdown) {
        cleanListByHostAndPort(serversToShutdown, deleted, added);
      }

      synchronized (migrations) {
        Iterator<Entry<KeyExtent,TServerInstance>> iter = migrations.entrySet().iterator();
        while (iter.hasNext()) {
          Entry<KeyExtent,TServerInstance> entry = iter.next();
          if (deleted.contains(entry.getValue())) {
            log.info("Canceling migration of {} to {}", entry.getKey(), entry.getValue());
            iter.remove();
          }
        }
      }
      nextEvent.event("There are now %d tablet servers", current.size());
    }

    // clear out any servers that are no longer current
    // this is needed when we are using a fate operation to shutdown a tserver as it
    // will continue to add the server to the serversToShutdown (ACCUMULO-4410)
    serversToShutdown.retainAll(current.getCurrentServers());
  }

  private static void cleanListByHostAndPort(Collection<TServerInstance> badServers,
      Set<TServerInstance> deleted, Set<TServerInstance> added) {
    Iterator<TServerInstance> badIter = badServers.iterator();
    while (badIter.hasNext()) {
      TServerInstance bad = badIter.next();
      for (TServerInstance add : added) {
        if (bad.getHostPort().equals(add.getHostPort())) {
          badIter.remove();
          break;
        }
      }
      for (TServerInstance del : deleted) {
        if (bad.getHostPort().equals(del.getHostPort())) {
          badIter.remove();
          break;
        }
      }
    }
  }

  @Override
  public void stateChanged(TableId tableId, TableState state) {
    nextEvent.event(tableId, "Table state in zookeeper changed for %s to %s", tableId, state);
    if (state == TableState.OFFLINE) {
      clearMigrations(tableId);
    }
  }

  @Override
  public void initialize() {}

  @Override
  public void sessionExpired() {}

  public Set<TableId> onlineTables() {
    Set<TableId> result = new HashSet<>();
    if (getManagerState() != ManagerState.NORMAL) {
      if (getManagerState() != ManagerState.UNLOAD_METADATA_TABLETS) {
        result.add(AccumuloTable.METADATA.tableId());
      }
      if (getManagerState() != ManagerState.UNLOAD_ROOT_TABLET) {
        result.add(AccumuloTable.ROOT.tableId());
      }
      return result;
    }
    ServerContext context = getContext();
    TableManager manager = context.getTableManager();

    for (TableId tableId : context.getTableIdToNameMap().keySet()) {
      TableState state = manager.getTableState(tableId);
      if (state == TableState.ONLINE) {
        result.add(tableId);
      }
    }
    return result;
  }

  public Set<TServerInstance> onlineTabletServers() {
    return tserverSet.getSnapshot().getTservers();
  }

  public LiveTServersSnapshot tserversSnapshot() {
    return tserverSet.getSnapshot();
  }

  // recovers state from the persistent transaction to shutdown a server
  public void shutdownTServer(TServerInstance server) {
    nextEvent.event("Tablet Server shutdown requested for %s", server);
    serversToShutdown.add(server);
  }

  public EventCoordinator getEventCoordinator() {
    return nextEvent;
  }

  public VolumeManager getVolumeManager() {
    return getContext().getVolumeManager();
  }

  public void assignedTablet(KeyExtent extent) {
    if (extent.isMeta() && getManagerState() == ManagerState.UNLOAD_ROOT_TABLET) {
      setManagerState(ManagerState.UNLOAD_METADATA_TABLETS);
    }
    // probably too late, but try anyhow
    if (extent.isRootTablet() && getManagerState() == ManagerState.STOP) {
      setManagerState(ManagerState.UNLOAD_ROOT_TABLET);
    }
  }

  @SuppressFBWarnings(value = "UW_UNCOND_WAIT", justification = "TODO needs triage")
  public void waitForBalance() {
    synchronized (balancedNotifier) {
      long eventCounter;
      do {
        eventCounter = nextEvent.waitForEvents(0, 0);
        try {
          balancedNotifier.wait();
        } catch (InterruptedException e) {
          log.debug(e.toString(), e);
        }
      } while (displayUnassigned() > 0 || !migrations.isEmpty()
          || eventCounter != nextEvent.waitForEvents(0, 0));
    }
  }

  public ManagerMonitorInfo getManagerMonitorInfo() {
    final ManagerMonitorInfo result = new ManagerMonitorInfo();

    result.tServerInfo = new ArrayList<>();
    result.tableMap = new HashMap<>();
    for (Entry<TServerInstance,TabletServerStatus> serverEntry : tserverStatus.entrySet()) {
      final TabletServerStatus status = serverEntry.getValue();
      result.tServerInfo.add(status);
      for (Entry<String,TableInfo> entry : status.tableMap.entrySet()) {
        TableInfoUtil.add(result.tableMap.computeIfAbsent(entry.getKey(), k -> new TableInfo()),
            entry.getValue());
      }
    }
    result.badTServers = new HashMap<>();
    synchronized (badServers) {
      for (TServerInstance bad : badServers.keySet()) {
        result.badTServers.put(bad.getHostPort(), TabletServerState.UNRESPONSIVE.getId());
      }
    }
    result.state = getManagerState();
    result.goalState = getManagerGoalState();
    result.unassignedTablets = displayUnassigned();
    result.serversShuttingDown = new HashSet<>();
    synchronized (serversToShutdown) {
      for (TServerInstance server : serversToShutdown) {
        result.serversShuttingDown.add(server.getHostPort());
      }
    }
    DeadServerList obit = new DeadServerList(getContext());
    result.deadTabletServers = obit.getList();
    result.bulkImports = bulkImportStatus.getBulkLoadStatus();
    return result;
  }

  /**
   * Can delegation tokens be generated for users
   */
  public boolean delegationTokensAvailable() {
    return delegationTokensAvailable;
  }

  public Map<KeyExtent,TServerInstance> migrationsSnapshot() {
    synchronized (migrations) {
      return Map.copyOf(migrations);
    }
  }

  public Set<TServerInstance> shutdownServers() {
    synchronized (serversToShutdown) {
      return Set.copyOf(serversToShutdown);
    }
  }

  public void updateBulkImportStatus(String directory, BulkImportState state) {
    bulkImportStatus.updateBulkImportStatus(Collections.singletonList(directory), state);
  }

  public void removeBulkImportStatus(String directory) {
    bulkImportStatus.removeBulkImportStatus(Collections.singletonList(directory));
  }

  /**
   * Return how long there has been a manager overseeing this cluster. This is an approximately
   * monotonic clock, which will be approximately consistent between different managers or different
   * runs of the same manager. SteadyTime supports both nanoseconds and milliseconds.
   */
  public SteadyTime getSteadyTime() {
    return timeKeeper.getTime();
  }

  @Override
  public boolean isActiveService() {
    return managerInitialized.get();
  }

  @Override
  public boolean isUpgrading() {
    return upgradeCoordinator.getStatus() != UpgradeCoordinator.UpgradeStatus.COMPLETE;
  }

  void initializeBalancer() {
    var localTabletBalancer = Property.createInstanceFromPropertyName(getConfiguration(),
        Property.MANAGER_TABLET_BALANCER, TabletBalancer.class, new SimpleLoadBalancer());
    localTabletBalancer.init(balancerEnvironment);
    tabletBalancer = localTabletBalancer;
  }

  Class<?> getBalancerClass() {
    return tabletBalancer.getClass();
  }

  void getAssignments(SortedMap<TServerInstance,TabletServerStatus> currentStatus,
      Map<String,Set<TServerInstance>> currentTServerGroups,
      Map<KeyExtent,UnassignedTablet> unassigned, Map<KeyExtent,TServerInstance> assignedOut) {
    AssignmentParamsImpl params =
        AssignmentParamsImpl.fromThrift(currentStatus, currentTServerGroups,
            unassigned.entrySet().stream().collect(HashMap::new,
                (m, e) -> m.put(e.getKey(),
                    e.getValue().getLastLocation() == null ? null
                        : e.getValue().getLastLocation().getServerInstance()),
                Map::putAll),
            assignedOut);
    tabletBalancer.getAssignments(params);
  }

  public TabletStateStore getTabletStateStore(DataLevel level) {
    switch (level) {
      case METADATA:
        return this.metadataTabletStore;
      case ROOT:
        return this.rootTabletStore;
      case USER:
        return this.userTabletStore;
      default:
        throw new IllegalStateException("Unhandled DataLevel value: " + level);
    }
  }

  @Override
  public void registerMetrics(MeterRegistry registry) {
    super.registerMetrics(registry);
    compactionCoordinator.registerMetrics(registry);
  }

  private Map<FateInstanceType,Fate<Manager>> getFateRefs() {
    var fateRefs = this.fateRefs.get();
    Preconditions.checkState(fateRefs != null, "Unexpected null fate references map");
    return fateRefs;
  }
}

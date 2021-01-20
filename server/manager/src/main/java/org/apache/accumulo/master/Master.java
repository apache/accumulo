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
package org.apache.accumulo.master;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.accumulo.fate.util.UtilWaitThread.sleepUninterruptibly;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.clientImpl.Tables;
import org.apache.accumulo.core.clientImpl.ThriftTransportPool;
import org.apache.accumulo.core.clientImpl.thrift.TableOperation;
import org.apache.accumulo.core.clientImpl.thrift.TableOperationExceptionType;
import org.apache.accumulo.core.clientImpl.thrift.ThriftTableOperationException;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.master.state.tables.TableState;
import org.apache.accumulo.core.master.thrift.BulkImportState;
import org.apache.accumulo.core.master.thrift.MasterClientService.Iface;
import org.apache.accumulo.core.master.thrift.MasterClientService.Processor;
import org.apache.accumulo.core.master.thrift.MasterGoalState;
import org.apache.accumulo.core.master.thrift.MasterMonitorInfo;
import org.apache.accumulo.core.master.thrift.MasterState;
import org.apache.accumulo.core.master.thrift.TableInfo;
import org.apache.accumulo.core.master.thrift.TabletServerStatus;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.metadata.RootTable;
import org.apache.accumulo.core.metadata.TServerInstance;
import org.apache.accumulo.core.metadata.TabletLocationState;
import org.apache.accumulo.core.metadata.TabletState;
import org.apache.accumulo.core.metadata.schema.Ample.DataLevel;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.TabletColumnFamily;
import org.apache.accumulo.core.replication.thrift.ReplicationCoordinator;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.tabletserver.thrift.TUnloadTabletGoal;
import org.apache.accumulo.core.trace.TraceUtil;
import org.apache.accumulo.core.util.Halt;
import org.apache.accumulo.core.util.threads.ThreadPools;
import org.apache.accumulo.core.util.threads.Threads;
import org.apache.accumulo.fate.AgeOffStore;
import org.apache.accumulo.fate.Fate;
import org.apache.accumulo.fate.util.Retry;
import org.apache.accumulo.fate.zookeeper.ZooLock;
import org.apache.accumulo.fate.zookeeper.ZooLock.LockLossReason;
import org.apache.accumulo.fate.zookeeper.ZooReaderWriter;
import org.apache.accumulo.fate.zookeeper.ZooUtil;
import org.apache.accumulo.fate.zookeeper.ZooUtil.NodeExistsPolicy;
import org.apache.accumulo.fate.zookeeper.ZooUtil.NodeMissingPolicy;
import org.apache.accumulo.master.metrics.MasterMetricsFactory;
import org.apache.accumulo.master.recovery.RecoveryManager;
import org.apache.accumulo.master.replication.MasterReplicationCoordinator;
import org.apache.accumulo.master.replication.ReplicationDriver;
import org.apache.accumulo.master.replication.WorkDriver;
import org.apache.accumulo.master.state.TableCounts;
import org.apache.accumulo.master.tableOps.TraceRepo;
import org.apache.accumulo.master.upgrade.UpgradeCoordinator;
import org.apache.accumulo.server.AbstractServer;
import org.apache.accumulo.server.HighlyAvailableService;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.ServerOpts;
import org.apache.accumulo.server.fs.VolumeManager;
import org.apache.accumulo.server.master.LiveTServerSet;
import org.apache.accumulo.server.master.LiveTServerSet.TServerConnection;
import org.apache.accumulo.server.master.balancer.DefaultLoadBalancer;
import org.apache.accumulo.server.master.balancer.TabletBalancer;
import org.apache.accumulo.server.master.state.CurrentState;
import org.apache.accumulo.server.master.state.DeadServerList;
import org.apache.accumulo.server.master.state.MergeInfo;
import org.apache.accumulo.server.master.state.MergeState;
import org.apache.accumulo.server.master.state.TabletMigration;
import org.apache.accumulo.server.master.state.TabletServerState;
import org.apache.accumulo.server.master.state.TabletStateStore;
import org.apache.accumulo.server.replication.ZooKeeperInitialization;
import org.apache.accumulo.server.rpc.HighlyAvailableServiceWrapper;
import org.apache.accumulo.server.rpc.ServerAddress;
import org.apache.accumulo.server.rpc.TCredentialsUpdatingWrapper;
import org.apache.accumulo.server.rpc.TServerUtils;
import org.apache.accumulo.server.rpc.ThriftServerType;
import org.apache.accumulo.server.security.AuditedSecurityOperation;
import org.apache.accumulo.server.security.SecurityOperation;
import org.apache.accumulo.server.security.delegation.AuthenticationTokenKeyManager;
import org.apache.accumulo.server.security.delegation.AuthenticationTokenSecretManager;
import org.apache.accumulo.server.security.delegation.ZooAuthenticationKeyDistributor;
import org.apache.accumulo.server.tables.TableManager;
import org.apache.accumulo.server.tables.TableObserver;
import org.apache.accumulo.server.util.ServerBulkImportStatus;
import org.apache.accumulo.server.util.TableInfoUtil;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.thrift.TException;
import org.apache.thrift.server.TServer;
import org.apache.thrift.transport.TTransportException;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.NoAuthException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableSortedMap;
import com.google.common.util.concurrent.RateLimiter;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * The Master is responsible for assigning and balancing tablets to tablet servers.
 * <p>
 * The master will also coordinate log recoveries and reports general status.
 */
public class Master extends AbstractServer
    implements LiveTServerSet.Listener, TableObserver, CurrentState, HighlyAvailableService {

  static final Logger log = LoggerFactory.getLogger(Master.class);

  static final int ONE_SECOND = 1000;
  static final long TIME_TO_WAIT_BETWEEN_SCANS = 60 * ONE_SECOND;
  // made this less than TIME_TO_WAIT_BETWEEN_SCANS, so that the cache is cleared between cycles
  static final long TIME_TO_CACHE_RECOVERY_WAL_EXISTENCE = TIME_TO_WAIT_BETWEEN_SCANS / 4;
  private static final long TIME_BETWEEN_MIGRATION_CLEANUPS = 5 * 60 * ONE_SECOND;
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
  private final Object mergeLock = new Object();
  private Thread replicationWorkThread;
  private Thread replicationAssignerThread;
  RecoveryManager recoveryManager = null;
  private final MasterTime timeKeeper;

  // Delegation Token classes
  private final boolean delegationTokensAvailable;
  private ZooAuthenticationKeyDistributor keyDistributor;
  private AuthenticationTokenKeyManager authenticationTokenKeyManager;

  ZooLock masterLock = null;
  private TServer clientService = null;
  TabletBalancer tabletBalancer;

  private MasterState state = MasterState.INITIAL;

  Fate<Master> fate;

  volatile SortedMap<TServerInstance,TabletServerStatus> tserverStatus =
      Collections.unmodifiableSortedMap(new TreeMap<>());
  final ServerBulkImportStatus bulkImportStatus = new ServerBulkImportStatus();

  private final AtomicBoolean masterInitialized = new AtomicBoolean(false);

  @Override
  public synchronized MasterState getMasterState() {
    return state;
  }

  public boolean stillMaster() {
    return getMasterState() != MasterState.STOP;
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
  synchronized void setMasterState(MasterState newState) {
    if (state.equals(newState)) {
      return;
    }
    if (!transitionOK[state.ordinal()][newState.ordinal()]) {
      log.error("Programmer error: master should not transition from {} to {}", state, newState);
    }
    MasterState oldState = state;
    state = newState;
    nextEvent.event("State changed from %s to %s", oldState, newState);
    if (newState == MasterState.STOP) {
      // Give the server a little time before shutdown so the client
      // thread requesting the stop can return
      ThreadPools.createGeneralScheduledExecutorService(getConfiguration())
          .scheduleWithFixedDelay(() -> {
            // This frees the main thread and will cause the master to exit
            clientService.stop();
            Master.this.nextEvent.event("stopped event loop");
          }, 100L, 1000L, TimeUnit.MILLISECONDS);
    }

    if (oldState != newState && (newState == MasterState.HAVE_LOCK)) {
      upgradeCoordinator.upgradeZookeeper(getContext(), nextEvent);
    }

    if (oldState != newState && (newState == MasterState.NORMAL)) {
      if (fate != null) {
        throw new IllegalStateException("Access to Fate should not have been"
            + " initialized prior to the Master finishing upgrades. Please save"
            + " all logs and file a bug.");
      }
      upgradeMetadataFuture = upgradeCoordinator.upgradeMetadata(getContext(), nextEvent);
    }
  }

  private final UpgradeCoordinator upgradeCoordinator = new UpgradeCoordinator();

  private Future<Void> upgradeMetadataFuture;

  private MasterClientServiceHandler clientHandler;

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
        result += counts.assigned() + counts.hosted();
      }
    }
    return result;
  }

  private int nonMetaDataTabletsAssignedOrHosted() {
    return totalAssignedOrHosted() - assignedOrHosted(MetadataTable.ID)
        - assignedOrHosted(RootTable.ID);
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
    switch (getMasterState()) {
      case NORMAL:
        // Count offline tablets for online tables
        for (TabletGroupWatcher watcher : watchers) {
          TableManager manager = getContext().getTableManager();
          for (Entry<TableId,TableCounts> entry : watcher.getStats().entrySet()) {
            TableId tableId = entry.getKey();
            TableCounts counts = entry.getValue();
            TableState tableState = manager.getTableState(tableId);
            if (tableState != null && tableState.equals(TableState.ONLINE)) {
              result += counts.unassigned() + counts.assignedToDeadServers() + counts.assigned()
                  + counts.suspended();
            }
          }
        }
        break;
      case SAFE_MODE:
        // Count offline tablets for the metadata table
        for (TabletGroupWatcher watcher : watchers) {
          TableCounts counts = watcher.getStats(MetadataTable.ID);
          result += counts.unassigned() + counts.suspended();
        }
        break;
      case UNLOAD_METADATA_TABLETS:
      case UNLOAD_ROOT_TABLET:
        for (TabletGroupWatcher watcher : watchers) {
          TableCounts counts = watcher.getStats(MetadataTable.ID);
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
    Tables.clearCache(context);
    if (!Tables.getTableState(context, tableId).equals(TableState.ONLINE)) {
      throw new ThriftTableOperationException(tableId.canonical(), null, TableOperation.MERGE,
          TableOperationExceptionType.OFFLINE, "table is not online");
    }
  }

  public TableManager getTableManager() {
    return getContext().getTableManager();
  }

  public static void main(String[] args) throws Exception {
    try (Master master = new Master(new ServerOpts(), args)) {
      master.runServer();
    }
  }

  Master(ServerOpts opts, String[] args) throws IOException {
    super("master", opts, args);
    ServerContext context = super.getContext();

    AccumuloConfiguration aconf = context.getConfiguration();

    log.info("Version {}", Constants.VERSION);
    log.info("Instance {}", getInstanceID());
    timeKeeper = new MasterTime(this, aconf);
    ThriftTransportPool.getInstance()
        .setIdleTime(aconf.getTimeInMillis(Property.GENERAL_RPC_TIMEOUT));
    tserverSet = new LiveTServerSet(context, this);
    this.tabletBalancer = Property.createInstanceFromPropertyName(aconf,
        Property.MASTER_TABLET_BALANCER, TabletBalancer.class, new DefaultLoadBalancer());
    this.tabletBalancer.init(context);

    this.security = AuditedSecurityOperation.getInstance(context);

    // Create the secret manager (can generate and verify delegation tokens)
    final long tokenLifetime = aconf.getTimeInMillis(Property.GENERAL_DELEGATION_TOKEN_LIFETIME);
    context.setSecretManager(new AuthenticationTokenSecretManager(getInstanceID(), tokenLifetime));

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
  }

  public String getInstanceID() {
    return getContext().getInstanceID();
  }

  public String getZooKeeperRoot() {
    return getContext().getZooKeeperRoot();
  }

  public TServerConnection getConnection(TServerInstance server) {
    return tserverSet.getConnection(server);
  }

  public MergeInfo getMergeInfo(TableId tableId) {
    ServerContext context = getContext();
    synchronized (mergeLock) {
      try {
        String path = getZooKeeperRoot() + Constants.ZTABLES + "/" + tableId + "/merge";
        if (!context.getZooReaderWriter().exists(path)) {
          return new MergeInfo();
        }
        byte[] data = context.getZooReaderWriter().getData(path);
        DataInputBuffer in = new DataInputBuffer();
        in.reset(data, data.length);
        MergeInfo info = new MergeInfo();
        info.readFields(in);
        return info;
      } catch (KeeperException.NoNodeException ex) {
        log.info("Error reading merge state, it probably just finished");
        return new MergeInfo();
      } catch (Exception ex) {
        log.warn("Unexpected error reading merge state", ex);
        return new MergeInfo();
      }
    }
  }

  public void setMergeState(MergeInfo info, MergeState state)
      throws KeeperException, InterruptedException {
    ServerContext context = getContext();
    synchronized (mergeLock) {
      String path =
          getZooKeeperRoot() + Constants.ZTABLES + "/" + info.getExtent().tableId() + "/merge";
      info.setState(state);
      if (state.equals(MergeState.NONE)) {
        context.getZooReaderWriter().recursiveDelete(path, NodeMissingPolicy.SKIP);
      } else {
        DataOutputBuffer out = new DataOutputBuffer();
        try {
          info.write(out);
        } catch (IOException ex) {
          throw new AssertionError("Unlikely", ex);
        }
        context.getZooReaderWriter().putPersistentData(path, out.getData(),
            state.equals(MergeState.STARTED) ? ZooUtil.NodeExistsPolicy.FAIL
                : ZooUtil.NodeExistsPolicy.OVERWRITE);
      }
      mergeLock.notifyAll();
    }
    nextEvent.event("Merge state of %s set to %s", info.getExtent(), state);
  }

  public void clearMergeState(TableId tableId) throws KeeperException, InterruptedException {
    synchronized (mergeLock) {
      String path = getZooKeeperRoot() + Constants.ZTABLES + "/" + tableId + "/merge";
      getContext().getZooReaderWriter().recursiveDelete(path, NodeMissingPolicy.SKIP);
      mergeLock.notifyAll();
    }
    nextEvent.event("Merge state of %s cleared", tableId);
  }

  void setMasterGoalState(MasterGoalState state) {
    try {
      getContext().getZooReaderWriter().putPersistentData(
          getZooKeeperRoot() + Constants.ZMASTER_GOAL_STATE, state.name().getBytes(),
          NodeExistsPolicy.OVERWRITE);
    } catch (Exception ex) {
      log.error("Unable to set master goal state in zookeeper");
    }
  }

  MasterGoalState getMasterGoalState() {
    while (true) {
      try {
        byte[] data = getContext().getZooReaderWriter()
            .getData(getZooKeeperRoot() + Constants.ZMASTER_GOAL_STATE);
        return MasterGoalState.valueOf(new String(data));
      } catch (Exception e) {
        log.error("Problem getting real goal state from zookeeper: ", e);
        sleepUninterruptibly(1, TimeUnit.SECONDS);
      }
    }
  }

  public boolean hasCycled(long time) {
    for (TabletGroupWatcher watcher : watchers) {
      if (watcher.stats.lastScanFinished() < time) {
        return false;
      }
    }

    return true;
  }

  public void clearMigrations(TableId tableId) {
    synchronized (migrations) {
      migrations.keySet().removeIf(extent -> extent.tableId().equals(tableId));
    }
  }

  enum TabletGoalState {
    HOSTED(TUnloadTabletGoal.UNKNOWN),
    UNASSIGNED(TUnloadTabletGoal.UNASSIGNED),
    DELETED(TUnloadTabletGoal.DELETED),
    SUSPENDED(TUnloadTabletGoal.SUSPENDED);

    private final TUnloadTabletGoal unloadGoal;

    TabletGoalState(TUnloadTabletGoal unloadGoal) {
      this.unloadGoal = unloadGoal;
    }

    /** The purpose of unloading this tablet. */
    public TUnloadTabletGoal howUnload() {
      return unloadGoal;
    }
  }

  TabletGoalState getSystemGoalState(TabletLocationState tls) {
    switch (getMasterState()) {
      case NORMAL:
        return TabletGoalState.HOSTED;
      case HAVE_LOCK: // fall-through intended
      case INITIAL: // fall-through intended
      case SAFE_MODE:
        if (tls.extent.isMeta()) {
          return TabletGoalState.HOSTED;
        }
        return TabletGoalState.UNASSIGNED;
      case UNLOAD_METADATA_TABLETS:
        if (tls.extent.isRootTablet()) {
          return TabletGoalState.HOSTED;
        }
        return TabletGoalState.UNASSIGNED;
      case UNLOAD_ROOT_TABLET:
        return TabletGoalState.UNASSIGNED;
      case STOP:
        return TabletGoalState.UNASSIGNED;
      default:
        throw new IllegalStateException("Unknown Master State");
    }
  }

  TabletGoalState getTableGoalState(KeyExtent extent) {
    TableState tableState = getContext().getTableManager().getTableState(extent.tableId());
    if (tableState == null) {
      return TabletGoalState.DELETED;
    }
    switch (tableState) {
      case DELETING:
        return TabletGoalState.DELETED;
      case OFFLINE:
      case NEW:
        return TabletGoalState.UNASSIGNED;
      default:
        return TabletGoalState.HOSTED;
    }
  }

  TabletGoalState getGoalState(TabletLocationState tls, MergeInfo mergeInfo) {
    KeyExtent extent = tls.extent;
    // Shutting down?
    TabletGoalState state = getSystemGoalState(tls);
    if (state == TabletGoalState.HOSTED) {
      if (!upgradeCoordinator.getStatus().isParentLevelUpgraded(extent)) {
        // The place where this tablet stores its metadata was not upgraded, so do not assign this
        // tablet yet.
        return TabletGoalState.UNASSIGNED;
      }

      if (tls.current != null && serversToShutdown.contains(tls.current)) {
        return TabletGoalState.SUSPENDED;
      }
      // Handle merge transitions
      if (mergeInfo.getExtent() != null) {

        final boolean overlaps = mergeInfo.overlaps(extent);

        if (overlaps) {
          log.debug("mergeInfo overlaps: {} true", extent);
          switch (mergeInfo.getState()) {
            case NONE:
            case COMPLETE:
              break;
            case STARTED:
            case SPLITTING:
              return TabletGoalState.HOSTED;
            case WAITING_FOR_CHOPPED:
              if (tls.getState(tserverSet.getCurrentServers()).equals(TabletState.HOSTED)) {
                if (tls.chopped) {
                  return TabletGoalState.UNASSIGNED;
                }
              } else {
                if (tls.chopped && tls.walogs.isEmpty()) {
                  return TabletGoalState.UNASSIGNED;
                }
              }

              return TabletGoalState.HOSTED;
            case WAITING_FOR_OFFLINE:
            case MERGING:
              return TabletGoalState.UNASSIGNED;
          }
        } else {
          log.trace("mergeInfo overlaps: {} false", extent);
        }
      }

      // taking table offline?
      state = getTableGoalState(extent);
      if (state == TabletGoalState.HOSTED) {
        // Maybe this tablet needs to be migrated
        TServerInstance dest = migrations.get(extent);
        if (dest != null && tls.current != null && !dest.equals(tls.current)) {
          return TabletGoalState.UNASSIGNED;
        }
      }
    }
    return state;
  }

  private class MigrationCleanupThread implements Runnable {

    @Override
    public void run() {
      while (stillMaster()) {
        if (!migrations.isEmpty()) {
          try {
            cleanupOfflineMigrations();
            cleanupNonexistentMigrations(getContext());
          } catch (Exception ex) {
            log.error("Error cleaning up migrations", ex);
          }
        }
        sleepUninterruptibly(TIME_BETWEEN_MIGRATION_CLEANUPS, TimeUnit.MILLISECONDS);
      }
    }

    /**
     * If a migrating tablet splits, and the tablet dies before sending the master a message, the
     * migration will refer to a non-existing tablet, so it can never complete. Periodically scan
     * the metadata table and remove any migrating tablets that no longer exist.
     */
    private void cleanupNonexistentMigrations(final AccumuloClient accumuloClient)
        throws TableNotFoundException {
      Scanner scanner = accumuloClient.createScanner(MetadataTable.NAME, Authorizations.EMPTY);
      TabletColumnFamily.PREV_ROW_COLUMN.fetch(scanner);
      Set<KeyExtent> found = new HashSet<>();
      for (Entry<Key,Value> entry : scanner) {
        KeyExtent extent = KeyExtent.fromMetaPrevRow(entry);
        if (migrations.containsKey(extent)) {
          found.add(extent);
        }
      }
      migrations.keySet().retainAll(found);
    }

    /**
     * If migrating a tablet for a table that is offline, the migration can never succeed because no
     * tablet server will load the tablet. check for offline tables and remove their migrations.
     */
    private void cleanupOfflineMigrations() {
      ServerContext context = getContext();
      TableManager manager = context.getTableManager();
      for (TableId tableId : Tables.getIdToNameMap(context).keySet()) {
        TableState state = manager.getTableState(tableId);
        if (state == TableState.OFFLINE) {
          clearMigrations(tableId);
        }
      }
    }
  }

  private class StatusThread implements Runnable {

    private boolean goodStats() {
      int start;
      switch (getMasterState()) {
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
        if (watcher.stats.getLastMasterState() != getMasterState()) {
          log.debug("{}: {} != {}", watcher.getName(), watcher.stats.getLastMasterState(),
              getMasterState());
          return false;
        }
      }
      return true;
    }

    @Override
    public void run() {
      EventCoordinator.Listener eventListener = nextEvent.getListener();
      while (stillMaster()) {
        long wait = DEFAULT_WAIT_FOR_WATCHER;
        try {
          switch (getMasterGoalState()) {
            case NORMAL:
              setMasterState(MasterState.NORMAL);
              break;
            case SAFE_MODE:
              if (getMasterState() == MasterState.NORMAL) {
                setMasterState(MasterState.SAFE_MODE);
              }
              if (getMasterState() == MasterState.HAVE_LOCK) {
                setMasterState(MasterState.SAFE_MODE);
              }
              break;
            case CLEAN_STOP:
              switch (getMasterState()) {
                case NORMAL:
                  setMasterState(MasterState.SAFE_MODE);
                  break;
                case SAFE_MODE: {
                  int count = nonMetaDataTabletsAssignedOrHosted();
                  log.debug(
                      String.format("There are %d non-metadata tablets assigned or hosted", count));
                  if (count == 0 && goodStats()) {
                    setMasterState(MasterState.UNLOAD_METADATA_TABLETS);
                  }
                }
                  break;
                case UNLOAD_METADATA_TABLETS: {
                  int count = assignedOrHosted(MetadataTable.ID);
                  log.debug(
                      String.format("There are %d metadata tablets assigned or hosted", count));
                  if (count == 0 && goodStats()) {
                    setMasterState(MasterState.UNLOAD_ROOT_TABLET);
                  }
                }
                  break;
                case UNLOAD_ROOT_TABLET:
                  int count = assignedOrHosted(MetadataTable.ID);
                  if (count > 0 && goodStats()) {
                    log.debug(String.format("%d metadata tablets online", count));
                    setMasterState(MasterState.UNLOAD_ROOT_TABLET);
                  }
                  int root_count = assignedOrHosted(RootTable.ID);
                  if (root_count > 0 && goodStats()) {
                    log.debug("The root tablet is still assigned or hosted");
                  }
                  if (count + root_count == 0 && goodStats()) {
                    Set<TServerInstance> currentServers = tserverSet.getCurrentServers();
                    log.debug("stopping {} tablet servers", currentServers.size());
                    for (TServerInstance server : currentServers) {
                      try {
                        serversToShutdown.add(server);
                        tserverSet.getConnection(server).fastHalt(masterLock);
                      } catch (TException e) {
                        // its probably down, and we don't care
                      } finally {
                        tserverSet.remove(server);
                      }
                    }
                    if (currentServers.isEmpty()) {
                      setMasterState(MasterState.STOP);
                    }
                  }
                  break;
                default:
                  break;
              }
          }
        } catch (Exception t) {
          log.error("Error occurred reading / switching master goal state. Will"
              + " continue with attempt to update status", t);
        }

        try {
          wait = updateStatus();
          eventListener.waitForEvents(wait);
        } catch (Exception t) {
          log.error("Error balancing tablets, will wait for {} (seconds) and then retry ",
              WAIT_BETWEEN_ERRORS / ONE_SECOND, t);
          sleepUninterruptibly(WAIT_BETWEEN_ERRORS, TimeUnit.MILLISECONDS);
        }
      }
    }

    private long updateStatus() {
      Set<TServerInstance> currentServers = tserverSet.getCurrentServers();
      tserverStatus = gatherTableInformation(currentServers);
      checkForHeldServer(tserverStatus);

      if (!badServers.isEmpty()) {
        log.debug("not balancing because the balance information is out-of-date {}",
            badServers.keySet());
      } else if (notHosted() > 0) {
        log.debug("not balancing because there are unhosted tablets: {}", notHosted());
      } else if (getMasterGoalState() == MasterGoalState.CLEAN_STOP) {
        log.debug("not balancing because the master is attempting to stop cleanly");
      } else if (!serversToShutdown.isEmpty()) {
        log.debug("not balancing while shutting down servers {}", serversToShutdown);
      } else {
        for (TabletGroupWatcher tgw : watchers) {
          if (!tgw.isSameTserversAsLastScan(currentServers)) {
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
            connection.fastHalt(masterLock);
          }
        } catch (TException e) {
          log.error("{}", e.getMessage(), e);
        }
        tserverSet.remove(instance);
      }
    }

    private long balanceTablets() {
      List<TabletMigration> migrationsOut = new ArrayList<>();
      long wait = tabletBalancer.balance(Collections.unmodifiableSortedMap(tserverStatus),
          migrationsSnapshot(), migrationsOut);

      for (TabletMigration m : TabletBalancer.checkMigrationSanity(tserverStatus.keySet(),
          migrationsOut)) {
        if (migrations.containsKey(m.tablet)) {
          log.warn("balancer requested migration more than once, skipping {}", m);
          continue;
        }
        migrations.put(m.tablet, m.newServer);
        log.debug("migration {}", m);
      }
      if (migrationsOut.isEmpty()) {
        synchronized (balancedNotifier) {
          balancedNotifier.notifyAll();
        }
      } else {
        nextEvent.event("Migrating %d more tablets, %d total", migrationsOut.size(),
            migrations.size());
      }
      return wait;
    }

  }

  private SortedMap<TServerInstance,TabletServerStatus>
      gatherTableInformation(Set<TServerInstance> currentServers) {
    final long rpcTimeout = getConfiguration().getTimeInMillis(Property.GENERAL_RPC_TIMEOUT);
    int threads = getConfiguration().getCount(Property.MASTER_STATUS_THREAD_POOL_SIZE);
    ExecutorService tp = ThreadPools.createExecutorService(getConfiguration(),
        Property.MASTER_STATUS_THREAD_POOL_SIZE);
    long start = System.currentTimeMillis();
    final SortedMap<TServerInstance,TabletServerStatus> result = new ConcurrentSkipListMap<>();
    final RateLimiter shutdownServerRateLimiter = RateLimiter.create(MAX_SHUTDOWNS_PER_SEC);
    for (TServerInstance serverInstance : currentServers) {
      final TServerInstance server = serverInstance;
      if (threads == 0) {
        // Since an unbounded thread pool is being used, rate limit how fast task are added to the
        // executor. This prevents the threads from growing large unless there are lots of
        // unresponsive tservers.
        sleepUninterruptibly(Math.max(1, rpcTimeout / 120_000), TimeUnit.MILLISECONDS);
      }
      tp.submit(() -> {
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
                  connection2.halt(masterLock);
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
      });
    }
    tp.shutdown();
    try {
      tp.awaitTermination(Math.max(10000, rpcTimeout / 3), TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      log.debug("Interrupted while fetching status");
    }

    tp.shutdownNow();

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
    // Start the Master's Client service
    clientHandler = new MasterClientServiceHandler(this);
    // Ensure that calls before the master gets the lock fail
    Iface haProxy = HighlyAvailableServiceWrapper.service(clientHandler, this);
    Iface rpcProxy = TraceUtil.wrapService(haProxy);
    final Processor<Iface> processor;
    if (context.getThriftServerType() == ThriftServerType.SASL) {
      Iface tcredsProxy = TCredentialsUpdatingWrapper.service(rpcProxy, clientHandler.getClass(),
          getConfiguration());
      processor = new Processor<>(tcredsProxy);
    } else {
      processor = new Processor<>(rpcProxy);
    }
    ServerAddress sa;
    try {
      sa = TServerUtils.startServer(getMetricsSystem(), context, getHostname(),
          Property.MASTER_CLIENTPORT, processor, "Master", "Master Client Service Handler", null,
          Property.MASTER_MINTHREADS, Property.MASTER_MINTHREADS_TIMEOUT,
          Property.MASTER_THREADCHECK, Property.GENERAL_MAX_MESSAGE_SIZE);
    } catch (UnknownHostException e) {
      throw new IllegalStateException("Unable to start server on host " + getHostname(), e);
    }
    clientService = sa.server;
    log.info("Started Master client service at {}", sa.address);

    // block until we can obtain the ZK lock for the master
    try {
      getMasterLock(zroot + Constants.ZMASTER_LOCK);
    } catch (KeeperException | InterruptedException e) {
      throw new IllegalStateException("Exception getting master lock", e);
    }

    recoveryManager = new RecoveryManager(this, TIME_TO_CACHE_RECOVERY_WAL_EXISTENCE);

    context.getTableManager().addObserver(this);

    Thread statusThread = Threads.createThread("Status Thread", new StatusThread());
    statusThread.start();

    Threads.createThread("Migration Cleanup Thread", new MigrationCleanupThread()).start();

    tserverSet.startListeningForTabletServerChanges();

    try {
      blockForTservers();
    } catch (InterruptedException ex) {
      Thread.currentThread().interrupt();
    }

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

    watchers.add(new TabletGroupWatcher(this,
        TabletStateStore.getStoreForLevel(DataLevel.USER, context, this), null) {
      @Override
      boolean canSuspendTablets() {
        // Always allow user data tablets to enter suspended state.
        return true;
      }
    });

    watchers.add(new TabletGroupWatcher(this,
        TabletStateStore.getStoreForLevel(DataLevel.METADATA, context, this), watchers.get(0)) {
      @Override
      boolean canSuspendTablets() {
        // Allow metadata tablets to enter suspended state only if so configured. Generally
        // we'll want metadata tablets to
        // be immediately reassigned, even if there's a global table.suspension.duration
        // setting.
        return getConfiguration().getBoolean(Property.MASTER_METADATA_SUSPENDABLE);
      }
    });

    watchers.add(new TabletGroupWatcher(this,
        TabletStateStore.getStoreForLevel(DataLevel.ROOT, context), watchers.get(1)) {
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
      if (null != upgradeMetadataFuture) {
        upgradeMetadataFuture.get();
      }
    } catch (ExecutionException | InterruptedException e) {
      throw new IllegalStateException("Metadata upgrade failed", e);
    }

    try {
      final AgeOffStore<Master> store = new AgeOffStore<>(new org.apache.accumulo.fate.ZooStore<>(
          getZooKeeperRoot() + Constants.ZFATE, context.getZooReaderWriter()), 1000 * 60 * 60 * 8);

      fate = new Fate<>(this, store, TraceRepo::toLogString);
      fate.startTransactionRunners(getConfiguration());

      ThreadPools.createGeneralScheduledExecutorService(getConfiguration())
          .scheduleWithFixedDelay(store::ageOff, 63000, 63000, TimeUnit.MILLISECONDS);
    } catch (KeeperException | InterruptedException e) {
      throw new IllegalStateException("Exception setting up FaTE cleanup thread", e);
    }

    try {
      ZooKeeperInitialization.ensureZooKeeperInitialized(zReaderWriter, zroot);
    } catch (KeeperException | InterruptedException e) {
      throw new IllegalStateException("Exception while ensuring ZooKeeper is initialized", e);
    }

    // Make sure that we have a secret key (either a new one or an old one from ZK) before we start
    // the master client service.
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
        sleepUninterruptibly(200, TimeUnit.MILLISECONDS);
      }
      // And log when we are initialized
      log.info("AuthenticationTokenSecretManager is initialized");
    }

    String address = sa.address.toString();
    log.info("Setting master lock data to {}", address);
    try {
      masterLock.replaceLockData(address.getBytes());
    } catch (KeeperException | InterruptedException e) {
      throw new IllegalStateException("Exception updating master lock", e);
    }

    while (!clientService.isServing()) {
      sleepUninterruptibly(100, TimeUnit.MILLISECONDS);
    }

    // if the replication name is ever set, then start replication services
    final AtomicReference<TServer> replServer = new AtomicReference<>();
    ThreadPools.createGeneralScheduledExecutorService(getConfiguration())
        .scheduleWithFixedDelay(() -> {
          try {
            if (replServer.get() == null) {
              if (!getConfiguration().get(Property.REPLICATION_NAME).isEmpty()) {
                log.info(Property.REPLICATION_NAME.getKey() + " was set, starting repl services.");
                replServer.set(setupReplication());
              }
            }
          } catch (UnknownHostException | KeeperException | InterruptedException e) {
            log.error("Error occurred starting replication services. ", e);
          }
        }, 0, 5000, TimeUnit.MILLISECONDS);

    // Register metrics modules
    int failureCount = new MasterMetricsFactory(getConfiguration()).register(this);

    if (failureCount > 0) {
      log.info("Failed to register {} metrics modules", failureCount);
    } else {
      log.info("All metrics modules registered");
    }

    // checking stored user hashes if any of them uses an outdated algorithm
    security.validateStoredUserCreditentials();

    // The master is fully initialized. Clients are allowed to connect now.
    masterInitialized.set(true);

    while (clientService.isServing()) {
      sleepUninterruptibly(500, TimeUnit.MILLISECONDS);
    }
    log.info("Shutting down fate.");
    fate.shutdown();

    final long deadline = System.currentTimeMillis() + MAX_CLEANUP_WAIT_TIME;
    try {
      statusThread.join(remaining(deadline));
      if (null != replicationAssignerThread) {
        replicationAssignerThread.join(remaining(deadline));
      }
      if (null != replicationWorkThread) {
        replicationWorkThread.join(remaining(deadline));
      }
    } catch (InterruptedException e) {
      throw new IllegalStateException("Exception stopping replication workers", e);
    }
    TServerUtils.stopTServer(replServer.get());

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

  /**
   * Allows property configuration to block master start-up waiting for a minimum number of tservers
   * to register in zookeeper. It also accepts a maximum time to wait - if the time expires, the
   * start-up will continue with any tservers available. This check is only performed at master
   * initialization, when the master acquires the lock. The following properties are used to control
   * the behaviour:
   * <ul>
   * <li>MASTER_STARTUP_TSERVER_AVAIL_MIN_COUNT - when set to 0 or less, no blocking occurs (default
   * behaviour) otherwise will block until the number of tservers are available.</li>
   * <li>MASTER_STARTUP_TSERVER_AVAIL_MAX_WAIT - time to wait in milliseconds. When set to 0 or
   * less, will block indefinitely.</li>
   * </ul>
   *
   * @throws InterruptedException
   *           if interrupted while blocking, propagated for caller to handle.
   */
  private void blockForTservers() throws InterruptedException {

    long waitStart = System.currentTimeMillis();

    long minTserverCount =
        getConfiguration().getCount(Property.MASTER_STARTUP_TSERVER_AVAIL_MIN_COUNT);

    if (minTserverCount <= 0) {
      log.info("tserver availability check disabled, continuing with-{} servers. To enable, set {}",
          tserverSet.size(), Property.MASTER_STARTUP_TSERVER_AVAIL_MIN_COUNT.getKey());
      return;
    }

    long maxWait =
        getConfiguration().getTimeInMillis(Property.MASTER_STARTUP_TSERVER_AVAIL_MAX_WAIT);

    if (maxWait <= 0) {
      log.info("tserver availability check set to block indefinitely, To change, set {} > 0.",
          Property.MASTER_STARTUP_TSERVER_AVAIL_MAX_WAIT.getKey());
      maxWait = Long.MAX_VALUE;
    }

    // honor Retry condition that initial wait < max wait, otherwise use small value to allow thread
    // yield to happen
    long initialWait = Math.min(50, maxWait / 2);

    Retry tserverRetry =
        Retry.builder().infiniteRetries().retryAfter(initialWait, TimeUnit.MILLISECONDS)
            .incrementBy(15_000, TimeUnit.MILLISECONDS).maxWait(maxWait, TimeUnit.MILLISECONDS)
            .backOffFactor(1).logInterval(30_000, TimeUnit.MILLISECONDS).createRetry();

    log.info("Checking for tserver availability - need to reach {} servers. Have {}",
        minTserverCount, tserverSet.size());

    boolean needTservers = tserverSet.size() < minTserverCount;

    while (needTservers && tserverRetry.canRetry()) {

      tserverRetry.waitForNextAttempt();

      needTservers = tserverSet.size() < minTserverCount;

      // suppress last message once threshold reached.
      if (needTservers) {
        log.info(
            "Blocking for tserver availability - need to reach {} servers. Have {}"
                + " Time spent blocking {} sec.",
            minTserverCount, tserverSet.size(),
            TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis() - waitStart));
      }
    }

    if (tserverSet.size() < minTserverCount) {
      log.warn(
          "tserver availability check time expired - continuing. Requested {}, have {} tservers on line. "
              + " Time waiting {} ms",
          tserverSet.size(), minTserverCount,
          TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis() - waitStart));

    } else {
      log.info(
          "tserver availability check completed. Requested {}, have {} tservers on line. "
              + " Time waiting {} ms",
          tserverSet.size(), minTserverCount,
          TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis() - waitStart));
    }
  }

  private TServer setupReplication()
      throws UnknownHostException, KeeperException, InterruptedException {
    ServerContext context = getContext();
    // Start the replication coordinator which assigns tservers to service replication requests
    MasterReplicationCoordinator impl = new MasterReplicationCoordinator(this);
    ReplicationCoordinator.Iface haReplicationProxy =
        HighlyAvailableServiceWrapper.service(impl, this);
    ReplicationCoordinator.Processor<ReplicationCoordinator.Iface> replicationCoordinatorProcessor =
        new ReplicationCoordinator.Processor<>(TraceUtil.wrapService(haReplicationProxy));
    ServerAddress replAddress = TServerUtils.startServer(getMetricsSystem(), context, getHostname(),
        Property.MASTER_REPLICATION_COORDINATOR_PORT, replicationCoordinatorProcessor,
        "Master Replication Coordinator", "Replication Coordinator", null,
        Property.MASTER_REPLICATION_COORDINATOR_MINTHREADS, null,
        Property.MASTER_REPLICATION_COORDINATOR_THREADCHECK, Property.GENERAL_MAX_MESSAGE_SIZE);

    log.info("Started replication coordinator service at " + replAddress.address);
    // Start the daemon to scan the replication table and make units of work
    replicationWorkThread = Threads.createThread("Replication Driver", new ReplicationDriver(this));
    replicationWorkThread.start();

    // Start the daemon to assign work to tservers to replicate to our peers
    WorkDriver wd = new WorkDriver(this);
    replicationAssignerThread = Threads.createThread(wd.getName(), wd);
    replicationAssignerThread.start();

    // Advertise that port we used so peers don't have to be told what it is
    context.getZooReaderWriter().putPersistentData(
        getZooKeeperRoot() + Constants.ZMASTER_REPLICATION_COORDINATOR_ADDR,
        replAddress.address.toString().getBytes(UTF_8), NodeExistsPolicy.OVERWRITE);
    return replAddress.server;
  }

  private long remaining(long deadline) {
    return Math.max(1, deadline - System.currentTimeMillis());
  }

  public ZooLock getMasterLock() {
    return masterLock;
  }

  private static class MasterLockWatcher implements ZooLock.AsyncLockWatcher {

    boolean acquiredLock = false;
    boolean failedToAcquireLock = false;

    @Override
    public void lostLock(LockLossReason reason) {
      Halt.halt("Master lock in zookeeper lost (reason = " + reason + "), exiting!", -1);
    }

    @Override
    public void unableToMonitorLockNode(final Exception e) {
      // ACCUMULO-3651 Changed level to error and added FATAL to message for slf4j compatibility
      Halt.halt(-1, () -> log.error("FATAL: No longer able to monitor master lock node", e));

    }

    @Override
    public synchronized void acquiredLock() {
      log.debug("Acquired master lock");

      if (acquiredLock || failedToAcquireLock) {
        Halt.halt("Zoolock in unexpected state AL " + acquiredLock + " " + failedToAcquireLock, -1);
      }

      acquiredLock = true;
      notifyAll();
    }

    @Override
    public synchronized void failedToAcquireLock(Exception e) {
      log.warn("Failed to get master lock", e);

      if (e instanceof NoAuthException) {
        String msg = "Failed to acquire master lock due to incorrect ZooKeeper authentication.";
        log.error("{} Ensure instance.secret is consistent across Accumulo configuration", msg, e);
        Halt.halt(msg, -1);
      }

      if (acquiredLock) {
        Halt.halt("Zoolock in unexpected state FAL " + acquiredLock + " " + failedToAcquireLock,
            -1);
      }

      failedToAcquireLock = true;
      notifyAll();
    }

    public synchronized void waitForChange() {
      while (!acquiredLock && !failedToAcquireLock) {
        try {
          wait();
        } catch (InterruptedException e) {}
      }
    }
  }

  private void getMasterLock(final String zMasterLoc) throws KeeperException, InterruptedException {
    ServerContext context = getContext();
    log.info("trying to get master lock");

    final String masterClientAddress =
        getHostname() + ":" + getConfiguration().getPort(Property.MASTER_CLIENTPORT)[0];

    while (true) {

      MasterLockWatcher masterLockWatcher = new MasterLockWatcher();
      masterLock = new ZooLock(context.getZooReaderWriter(), zMasterLoc);
      masterLock.lockAsync(masterLockWatcher, masterClientAddress.getBytes());

      masterLockWatcher.waitForChange();

      if (masterLockWatcher.acquiredLock) {
        break;
      }

      if (!masterLockWatcher.failedToAcquireLock) {
        throw new IllegalStateException("master lock in unknown state");
      }

      masterLock.tryToCancelAsyncLockOrUnlock();

      sleepUninterruptibly(TIME_TO_WAIT_BETWEEN_LOCK_CHECKS, TimeUnit.MILLISECONDS);
    }

    setMasterState(MasterState.HAVE_LOCK);
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
        if (!getMasterGoalState().equals(MasterGoalState.CLEAN_STOP)) {
          obit.post(dead.getHostPort(), cause);
        }
      }

      Set<TServerInstance> unexpected = new HashSet<>(deleted);
      unexpected.removeAll(this.serversToShutdown);
      if (!unexpected.isEmpty()) {
        if (stillMaster() && !getMasterGoalState().equals(MasterGoalState.CLEAN_STOP)) {
          log.warn("Lost servers {}", unexpected);
        }
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
    nextEvent.event("Table state in zookeeper changed for %s to %s", tableId, state);
    if (state == TableState.OFFLINE) {
      clearMigrations(tableId);
    }
  }

  @Override
  public void initialize() {}

  @Override
  public void sessionExpired() {}

  @Override
  public Set<TableId> onlineTables() {
    Set<TableId> result = new HashSet<>();
    if (getMasterState() != MasterState.NORMAL) {
      if (getMasterState() != MasterState.UNLOAD_METADATA_TABLETS) {
        result.add(MetadataTable.ID);
      }
      if (getMasterState() != MasterState.UNLOAD_ROOT_TABLET) {
        result.add(RootTable.ID);
      }
      return result;
    }
    ServerContext context = getContext();
    TableManager manager = context.getTableManager();

    for (TableId tableId : Tables.getIdToNameMap(context).keySet()) {
      TableState state = manager.getTableState(tableId);
      if (state != null) {
        if (state == TableState.ONLINE) {
          result.add(tableId);
        }
      }
    }
    return result;
  }

  @Override
  public Set<TServerInstance> onlineTabletServers() {
    return tserverSet.getCurrentServers();
  }

  @Override
  public Collection<MergeInfo> merges() {
    List<MergeInfo> result = new ArrayList<>();
    for (TableId tableId : Tables.getIdToNameMap(getContext()).keySet()) {
      result.add(getMergeInfo(tableId));
    }
    return result;
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
    if (extent.isMeta()) {
      if (getMasterState().equals(MasterState.UNLOAD_ROOT_TABLET)) {
        setMasterState(MasterState.UNLOAD_METADATA_TABLETS);
      }
    }
    if (extent.isRootTablet()) {
      // probably too late, but try anyhow
      if (getMasterState().equals(MasterState.STOP)) {
        setMasterState(MasterState.UNLOAD_ROOT_TABLET);
      }
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

  public MasterMonitorInfo getMasterMonitorInfo() {
    final MasterMonitorInfo result = new MasterMonitorInfo();

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
    result.state = getMasterState();
    result.goalState = getMasterGoalState();
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

  @Override
  public Set<KeyExtent> migrationsSnapshot() {
    Set<KeyExtent> migrationKeys = new HashSet<>();
    synchronized (migrations) {
      migrationKeys.addAll(migrations.keySet());
    }
    return Collections.unmodifiableSet(migrationKeys);
  }

  @Override
  public Set<TServerInstance> shutdownServers() {
    synchronized (serversToShutdown) {
      return new HashSet<>(serversToShutdown);
    }
  }

  public void updateBulkImportStatus(String directory, BulkImportState state) {
    bulkImportStatus.updateBulkImportStatus(Collections.singletonList(directory), state);
  }

  public void removeBulkImportStatus(String directory) {
    bulkImportStatus.removeBulkImportStatus(Collections.singletonList(directory));
  }

  /**
   * Return how long (in milliseconds) there has been a master overseeing this cluster. This is an
   * approximately monotonic clock, which will be approximately consistent between different masters
   * or different runs of the same master.
   */
  public Long getSteadyTime() {
    return timeKeeper.getTime();
  }

  @Override
  public boolean isActiveService() {
    return masterInitialized.get();
  }

}

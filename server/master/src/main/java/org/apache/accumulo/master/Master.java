/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.accumulo.master;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.accumulo.fate.util.UtilWaitThread.sleepUninterruptibly;

import java.io.IOException;
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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.impl.Namespace;
import org.apache.accumulo.core.client.impl.Namespaces;
import org.apache.accumulo.core.client.impl.Table;
import org.apache.accumulo.core.client.impl.Tables;
import org.apache.accumulo.core.client.impl.ThriftTransportPool;
import org.apache.accumulo.core.client.impl.thrift.TableOperation;
import org.apache.accumulo.core.client.impl.thrift.TableOperationExceptionType;
import org.apache.accumulo.core.client.impl.thrift.ThriftTableOperationException;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.conf.SiteConfiguration;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.data.impl.KeyExtent;
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
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection;
import org.apache.accumulo.core.replication.ReplicationTable;
import org.apache.accumulo.core.replication.thrift.ReplicationCoordinator;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.NamespacePermission;
import org.apache.accumulo.core.security.TablePermission;
import org.apache.accumulo.core.tabletserver.thrift.TUnloadTabletGoal;
import org.apache.accumulo.core.trace.DistributedTrace;
import org.apache.accumulo.core.trace.thrift.TInfo;
import org.apache.accumulo.core.util.Daemon;
import org.apache.accumulo.core.util.Pair;
import org.apache.accumulo.core.zookeeper.ZooUtil;
import org.apache.accumulo.fate.AgeOffStore;
import org.apache.accumulo.fate.Fate;
import org.apache.accumulo.fate.zookeeper.IZooReaderWriter;
import org.apache.accumulo.fate.zookeeper.ZooLock.LockLossReason;
import org.apache.accumulo.fate.zookeeper.ZooUtil.NodeExistsPolicy;
import org.apache.accumulo.fate.zookeeper.ZooUtil.NodeMissingPolicy;
import org.apache.accumulo.master.metrics.MasterMetricsFactory;
import org.apache.accumulo.master.recovery.RecoveryManager;
import org.apache.accumulo.master.replication.MasterReplicationCoordinator;
import org.apache.accumulo.master.replication.ReplicationDriver;
import org.apache.accumulo.master.replication.WorkDriver;
import org.apache.accumulo.master.state.TableCounts;
import org.apache.accumulo.server.Accumulo;
import org.apache.accumulo.server.AccumuloServerContext;
import org.apache.accumulo.server.HighlyAvailableService;
import org.apache.accumulo.server.ServerConstants;
import org.apache.accumulo.server.ServerOpts;
import org.apache.accumulo.server.client.HdfsZooInstance;
import org.apache.accumulo.server.conf.ServerConfigurationFactory;
import org.apache.accumulo.server.fs.VolumeChooserEnvironment;
import org.apache.accumulo.server.fs.VolumeManager;
import org.apache.accumulo.server.fs.VolumeManager.FileType;
import org.apache.accumulo.server.fs.VolumeManagerImpl;
import org.apache.accumulo.server.init.Initialize;
import org.apache.accumulo.server.log.WalStateManager;
import org.apache.accumulo.server.log.WalStateManager.WalMarkerException;
import org.apache.accumulo.server.master.LiveTServerSet;
import org.apache.accumulo.server.master.LiveTServerSet.TServerConnection;
import org.apache.accumulo.server.master.balancer.DefaultLoadBalancer;
import org.apache.accumulo.server.master.balancer.TabletBalancer;
import org.apache.accumulo.server.master.state.CurrentState;
import org.apache.accumulo.server.master.state.DeadServerList;
import org.apache.accumulo.server.master.state.MergeInfo;
import org.apache.accumulo.server.master.state.MergeState;
import org.apache.accumulo.server.master.state.MetaDataStateStore;
import org.apache.accumulo.server.master.state.RootTabletStateStore;
import org.apache.accumulo.server.master.state.TServerInstance;
import org.apache.accumulo.server.master.state.TabletLocationState;
import org.apache.accumulo.server.master.state.TabletMigration;
import org.apache.accumulo.server.master.state.TabletServerState;
import org.apache.accumulo.server.master.state.TabletState;
import org.apache.accumulo.server.master.state.ZooStore;
import org.apache.accumulo.server.master.state.ZooTabletStateStore;
import org.apache.accumulo.server.metrics.Metrics;
import org.apache.accumulo.server.metrics.MetricsSystemHelper;
import org.apache.accumulo.server.replication.ZooKeeperInitialization;
import org.apache.accumulo.server.rpc.HighlyAvailableServiceWrapper;
import org.apache.accumulo.server.rpc.RpcWrapper;
import org.apache.accumulo.server.rpc.ServerAddress;
import org.apache.accumulo.server.rpc.TCredentialsUpdatingWrapper;
import org.apache.accumulo.server.rpc.TServerUtils;
import org.apache.accumulo.server.rpc.ThriftServerType;
import org.apache.accumulo.server.security.AuditedSecurityOperation;
import org.apache.accumulo.server.security.SecurityOperation;
import org.apache.accumulo.server.security.SecurityUtil;
import org.apache.accumulo.server.security.delegation.AuthenticationTokenKeyManager;
import org.apache.accumulo.server.security.delegation.AuthenticationTokenSecretManager;
import org.apache.accumulo.server.security.delegation.ZooAuthenticationKeyDistributor;
import org.apache.accumulo.server.security.handler.ZKPermHandler;
import org.apache.accumulo.server.tables.TableManager;
import org.apache.accumulo.server.tables.TableObserver;
import org.apache.accumulo.server.util.DefaultMap;
import org.apache.accumulo.server.util.Halt;
import org.apache.accumulo.server.util.MetadataTableUtil;
import org.apache.accumulo.server.util.ServerBulkImportStatus;
import org.apache.accumulo.server.util.TableInfoUtil;
import org.apache.accumulo.server.util.time.SimpleTimer;
import org.apache.accumulo.server.zookeeper.ZooLock;
import org.apache.accumulo.server.zookeeper.ZooReaderWriter;
import org.apache.accumulo.start.classloader.vfs.AccumuloVFSClassLoader;
import org.apache.accumulo.start.classloader.vfs.ContextManager;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.thrift.TException;
import org.apache.thrift.server.TServer;
import org.apache.thrift.transport.TTransportException;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.NoAuthException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Iterables;

/**
 * The Master is responsible for assigning and balancing tablets to tablet servers.
 *
 * The master will also coordinate log recoveries and reports general status.
 */
public class Master extends AccumuloServerContext implements LiveTServerSet.Listener, TableObserver, CurrentState, HighlyAvailableService {

  final static Logger log = LoggerFactory.getLogger(Master.class);

  final static int ONE_SECOND = 1000;
  final static long TIME_TO_WAIT_BETWEEN_SCANS = 60 * ONE_SECOND;
  final private static long TIME_BETWEEN_MIGRATION_CLEANUPS = 5 * 60 * ONE_SECOND;
  final static long WAIT_BETWEEN_ERRORS = ONE_SECOND;
  final private static long DEFAULT_WAIT_FOR_WATCHER = 10 * ONE_SECOND;
  final private static int MAX_CLEANUP_WAIT_TIME = ONE_SECOND;
  final private static int TIME_TO_WAIT_BETWEEN_LOCK_CHECKS = ONE_SECOND;
  final static int MAX_TSERVER_WORK_CHUNK = 5000;
  final private static int MAX_BAD_STATUS_COUNT = 3;

  final VolumeManager fs;
  final private String hostname;
  final private Object balancedNotifier = new Object();
  final LiveTServerSet tserverSet;
  final private List<TabletGroupWatcher> watchers = new ArrayList<>();
  final SecurityOperation security;
  final Map<TServerInstance,AtomicInteger> badServers = Collections.synchronizedMap(new DefaultMap<TServerInstance,AtomicInteger>(new AtomicInteger()));
  final Set<TServerInstance> serversToShutdown = Collections.synchronizedSet(new HashSet<TServerInstance>());
  final SortedMap<KeyExtent,TServerInstance> migrations = Collections.synchronizedSortedMap(new TreeMap<KeyExtent,TServerInstance>());
  final EventCoordinator nextEvent = new EventCoordinator();
  final private Object mergeLock = new Object();
  private ReplicationDriver replicationWorkDriver;
  private WorkDriver replicationWorkAssigner;
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

  volatile SortedMap<TServerInstance,TabletServerStatus> tserverStatus = Collections.unmodifiableSortedMap(new TreeMap<TServerInstance,TabletServerStatus>());
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
  static final boolean transitionOK[][] = {
      //                              INITIAL HAVE_LOCK SAFE_MODE NORMAL UNLOAD_META UNLOAD_ROOT STOP
      /* INITIAL */                   {X,     X,        O,        O,      O,         O,          X},
      /* HAVE_LOCK */                 {O,     X,        X,        X,      O,         O,          X},
      /* SAFE_MODE */                 {O,     O,        X,        X,      X,         O,          X},
      /* NORMAL */                    {O,     O,        X,        X,      X,         O,          X},
      /* UNLOAD_METADATA_TABLETS */   {O,     O,        X,        X,      X,         X,          X},
      /* UNLOAD_ROOT_TABLET */        {O,     O,        O,        X,      X,         X,          X},
      /* STOP */                      {O,     O,        O,        O,      O,         X,          X}};
  //@formatter:on
  synchronized void setMasterState(MasterState newState) {
    if (state.equals(newState))
      return;
    if (!transitionOK[state.ordinal()][newState.ordinal()]) {
      log.error("Programmer error: master should not transition from {} to {}", state, newState);
    }
    MasterState oldState = state;
    state = newState;
    nextEvent.event("State changed from %s to %s", oldState, newState);
    if (newState == MasterState.STOP) {
      // Give the server a little time before shutdown so the client
      // thread requesting the stop can return
      SimpleTimer.getInstance(getConfiguration()).schedule(new Runnable() {
        @Override
        public void run() {
          // This frees the main thread and will cause the master to exit
          clientService.stop();
          Master.this.nextEvent.event("stopped event loop");
        }

      }, 100l, 1000l);
    }

    if (oldState != newState && (newState == MasterState.HAVE_LOCK)) {
      upgradeZookeeper();
    }

    if (oldState != newState && (newState == MasterState.NORMAL)) {
      upgradeMetadata();
    }
  }

  private void moveRootTabletToRootTable(IZooReaderWriter zoo) throws Exception {
    String dirZPath = ZooUtil.getRoot(getInstance()) + RootTable.ZROOT_TABLET_PATH;

    if (!zoo.exists(dirZPath)) {
      Path oldPath = fs.getFullPath(FileType.TABLE, "/" + MetadataTable.ID + "/root_tablet");
      if (fs.exists(oldPath)) {
        VolumeChooserEnvironment chooserEnv = new VolumeChooserEnvironment(RootTable.ID);
        String newPath = fs.choose(chooserEnv, ServerConstants.getBaseUris()) + Constants.HDFS_TABLES_DIR + Path.SEPARATOR + RootTable.ID;
        fs.mkdirs(new Path(newPath));
        if (!fs.rename(oldPath, new Path(newPath))) {
          throw new IOException("Failed to move root tablet from " + oldPath + " to " + newPath);
        }

        log.info("Upgrade renamed {} to {}", oldPath, newPath);
      }

      Path location = null;

      for (String basePath : ServerConstants.getTablesDirs()) {
        Path path = new Path(basePath + "/" + RootTable.ID + RootTable.ROOT_TABLET_LOCATION);
        if (fs.exists(path)) {
          if (location != null) {
            throw new IllegalStateException("Root table at multiple locations " + location + " " + path);
          }

          location = path;
        }
      }

      if (location == null)
        throw new IllegalStateException("Failed to find root tablet");

      log.info("Upgrade setting root table location in zookeeper {}", location);
      zoo.putPersistentData(dirZPath, location.toString().getBytes(), NodeExistsPolicy.FAIL);
    }
  }

  private boolean haveUpgradedZooKeeper = false;

  private void upgradeZookeeper() {
    // 1.5.1 and 1.6.0 both do some state checking after obtaining the zoolock for the
    // monitor and before starting up. It's not tied to the data version at all (and would
    // introduce unnecessary complexity to try to make the master do it), but be aware
    // that the master is not the only thing that may alter zookeeper before starting.

    final int accumuloPersistentVersion = Accumulo.getAccumuloPersistentVersion(fs);
    if (Accumulo.persistentVersionNeedsUpgrade(accumuloPersistentVersion)) {
      // This Master hasn't started Fate yet, so any outstanding transactions must be from before the upgrade.
      // Change to Guava's Verify once we use Guava 17.
      if (null != fate) {
        throw new IllegalStateException(
            "Access to Fate should not have been initialized prior to the Master transitioning to active. Please save all logs and file a bug.");
      }
      Accumulo.abortIfFateTransactions();
      try {
        log.info("Upgrading zookeeper");

        IZooReaderWriter zoo = ZooReaderWriter.getInstance();
        final String zooRoot = ZooUtil.getRoot(getInstance());

        log.debug("Handling updates for version {}", accumuloPersistentVersion);

        log.debug("Cleaning out remnants of logger role.");
        zoo.recursiveDelete(zooRoot + "/loggers", NodeMissingPolicy.SKIP);
        zoo.recursiveDelete(zooRoot + "/dead/loggers", NodeMissingPolicy.SKIP);

        final byte[] zero = new byte[] {'0'};
        log.debug("Initializing recovery area.");
        zoo.putPersistentData(zooRoot + Constants.ZRECOVERY, zero, NodeExistsPolicy.SKIP);

        for (String id : zoo.getChildren(zooRoot + Constants.ZTABLES)) {
          log.debug("Prepping table {} for compaction cancellations.", id);
          zoo.putPersistentData(zooRoot + Constants.ZTABLES + "/" + id + Constants.ZTABLE_COMPACT_CANCEL_ID, zero, NodeExistsPolicy.SKIP);
        }

        @SuppressWarnings("deprecation")
        String zpath = zooRoot + Constants.ZCONFIG + "/" + Property.TSERV_WAL_SYNC_METHOD.getKey();
        // is the entire instance set to use flushing vs sync?
        boolean flushDefault = false;
        try {
          byte data[] = zoo.getData(zpath, null);
          if (new String(data, UTF_8).endsWith("flush")) {
            flushDefault = true;
          }
        } catch (KeeperException.NoNodeException ex) {
          // skip
        }
        for (String id : zoo.getChildren(zooRoot + Constants.ZTABLES)) {
          log.debug("Converting table {} WALog setting to Durability", id);
          try {
            @SuppressWarnings("deprecation")
            String path = zooRoot + Constants.ZTABLES + "/" + id + Constants.ZTABLE_CONF + "/" + Property.TABLE_WALOG_ENABLED.getKey();
            byte[] data = zoo.getData(path, null);
            boolean useWAL = Boolean.parseBoolean(new String(data, UTF_8));
            zoo.recursiveDelete(path, NodeMissingPolicy.FAIL);
            path = zooRoot + Constants.ZTABLES + "/" + id + Constants.ZTABLE_CONF + "/" + Property.TABLE_DURABILITY.getKey();
            if (useWAL) {
              if (flushDefault) {
                zoo.putPersistentData(path, "flush".getBytes(), NodeExistsPolicy.SKIP);
              } else {
                zoo.putPersistentData(path, "sync".getBytes(), NodeExistsPolicy.SKIP);
              }
            } else {
              zoo.putPersistentData(path, "none".getBytes(), NodeExistsPolicy.SKIP);
            }
          } catch (KeeperException.NoNodeException ex) {
            // skip it
          }
        }

        // create initial namespaces
        String namespaces = ZooUtil.getRoot(getInstance()) + Constants.ZNAMESPACES;
        zoo.putPersistentData(namespaces, new byte[0], NodeExistsPolicy.SKIP);
        for (Pair<String,Namespace.ID> namespace : Iterables.concat(Collections.singleton(new Pair<>(Namespace.ACCUMULO, Namespace.ID.ACCUMULO)),
            Collections.singleton(new Pair<>(Namespace.DEFAULT, Namespace.ID.DEFAULT)))) {
          String ns = namespace.getFirst();
          Namespace.ID id = namespace.getSecond();
          log.debug("Upgrade creating namespace \"{}\" (ID: {})", ns, id);
          if (!Namespaces.exists(getInstance(), id))
            TableManager.prepareNewNamespaceState(getInstance().getInstanceID(), id, ns, NodeExistsPolicy.SKIP);
        }

        // create replication table in zk
        log.debug("Upgrade creating table {} (ID: {})", ReplicationTable.NAME, ReplicationTable.ID);
        TableManager.prepareNewTableState(getInstance().getInstanceID(), ReplicationTable.ID, Namespace.ID.ACCUMULO, ReplicationTable.NAME, TableState.OFFLINE,
            NodeExistsPolicy.SKIP);

        // create root table
        log.debug("Upgrade creating table {} (ID: {})", RootTable.NAME, RootTable.ID);
        TableManager.prepareNewTableState(getInstance().getInstanceID(), RootTable.ID, Namespace.ID.ACCUMULO, RootTable.NAME, TableState.ONLINE,
            NodeExistsPolicy.SKIP);
        Initialize.initSystemTablesConfig();
        // ensure root user can flush root table
        security.grantTablePermission(rpcCreds(), security.getRootUsername(), RootTable.ID, TablePermission.ALTER_TABLE, Namespace.ID.ACCUMULO);

        // put existing tables in the correct namespaces
        String tables = ZooUtil.getRoot(getInstance()) + Constants.ZTABLES;
        for (String tableId : zoo.getChildren(tables)) {
          Namespace.ID targetNamespace = (MetadataTable.ID.canonicalID().equals(tableId) || RootTable.ID.canonicalID().equals(tableId)) ? Namespace.ID.ACCUMULO
              : Namespace.ID.DEFAULT;
          log.debug("Upgrade moving table {} (ID: {}) into namespace with ID {}", new String(zoo.getData(tables + "/" + tableId + Constants.ZTABLE_NAME, null),
              UTF_8), tableId, targetNamespace);
          zoo.putPersistentData(tables + "/" + tableId + Constants.ZTABLE_NAMESPACE, targetNamespace.getUtf8(), NodeExistsPolicy.SKIP);
        }

        // rename metadata table
        log.debug("Upgrade renaming table {} (ID: {}) to {}", MetadataTable.OLD_NAME, MetadataTable.ID, MetadataTable.NAME);
        zoo.putPersistentData(tables + "/" + MetadataTable.ID + Constants.ZTABLE_NAME, Tables.qualify(MetadataTable.NAME).getSecond().getBytes(UTF_8),
            NodeExistsPolicy.OVERWRITE);

        moveRootTabletToRootTable(zoo);

        // add system namespace permissions to existing users
        ZKPermHandler perm = new ZKPermHandler();
        perm.initialize(getInstance().getInstanceID(), true);
        String users = ZooUtil.getRoot(getInstance()) + "/users";
        for (String user : zoo.getChildren(users)) {
          zoo.putPersistentData(users + "/" + user + "/Namespaces", new byte[0], NodeExistsPolicy.SKIP);
          perm.grantNamespacePermission(user, Namespace.ID.ACCUMULO, NamespacePermission.READ);
        }
        perm.grantNamespacePermission("root", Namespace.ID.ACCUMULO, NamespacePermission.ALTER_TABLE);

        // add the currlog location for root tablet current logs
        zoo.putPersistentData(ZooUtil.getRoot(getInstance()) + RootTable.ZROOT_TABLET_CURRENT_LOGS, new byte[0], NodeExistsPolicy.SKIP);

        // create tablet server wal logs node in ZK
        zoo.putPersistentData(ZooUtil.getRoot(getInstance()) + WalStateManager.ZWALS, new byte[0], NodeExistsPolicy.SKIP);

        haveUpgradedZooKeeper = true;
      } catch (Exception ex) {
        // ACCUMULO-3651 Changed level to error and added FATAL to message for slf4j compatibility
        log.error("FATAL: Error performing upgrade", ex);
        System.exit(1);
      }
    }
  }

  private final AtomicBoolean upgradeMetadataRunning = new AtomicBoolean(false);
  private final CountDownLatch waitForMetadataUpgrade = new CountDownLatch(1);

  private final ServerConfigurationFactory serverConfig;

  private MasterClientServiceHandler clientHandler;

  private void upgradeMetadata() {
    // we make sure we're only doing the rest of this method once so that we can signal to other threads that an upgrade wasn't needed.
    if (upgradeMetadataRunning.compareAndSet(false, true)) {
      final int accumuloPersistentVersion = Accumulo.getAccumuloPersistentVersion(fs);
      if (Accumulo.persistentVersionNeedsUpgrade(accumuloPersistentVersion)) {
        // sanity check that we passed the Fate verification prior to ZooKeeper upgrade, and that Fate still hasn't been started.
        // Change both to use Guava's Verify once we use Guava 17.
        if (!haveUpgradedZooKeeper) {
          throw new IllegalStateException(
              "We should only attempt to upgrade Accumulo's metadata table if we've already upgraded ZooKeeper. Please save all logs and file a bug.");
        }
        if (null != fate) {
          throw new IllegalStateException(
              "Access to Fate should not have been initialized prior to the Master finishing upgrades. Please save all logs and file a bug.");
        }
        Runnable upgradeTask = new Runnable() {
          int version = accumuloPersistentVersion;

          @Override
          public void run() {
            try {
              log.info("Starting to upgrade metadata table.");
              if (version == ServerConstants.MOVE_DELETE_MARKERS - 1) {
                log.info("Updating Delete Markers in metadata table for version 1.4");
                MetadataTableUtil.moveMetaDeleteMarkersFrom14(Master.this);
                version++;
              }
              if (version == ServerConstants.MOVE_TO_ROOT_TABLE - 1) {
                log.info("Updating Delete Markers in metadata table.");
                MetadataTableUtil.moveMetaDeleteMarkers(Master.this);
                version++;
              }
              if (version == ServerConstants.MOVE_TO_REPLICATION_TABLE - 1) {
                log.info("Updating metadata table with entries for the replication table");
                MetadataTableUtil.createReplicationTable(Master.this);
                version++;
              }
              log.info("Updating persistent data version.");
              Accumulo.updateAccumuloVersion(fs, accumuloPersistentVersion);
              log.info("Upgrade complete");
              waitForMetadataUpgrade.countDown();
            } catch (Exception ex) {
              // ACCUMULO-3651 Changed level to error and added FATAL to message for slf4j compatibility
              log.error("FATAL: Error performing upgrade", ex);
              System.exit(1);
            }

          }
        };

        // need to run this in a separate thread because a lock is held that prevents metadata tablets from being assigned and this task writes to the
        // metadata table
        new Thread(upgradeTask).start();
      } else {
        waitForMetadataUpgrade.countDown();
      }
    }
  }

  private int assignedOrHosted(Table.ID tableId) {
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
    return totalAssignedOrHosted() - assignedOrHosted(MetadataTable.ID) - assignedOrHosted(RootTable.ID);
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
          TableManager manager = TableManager.getInstance();
          for (Entry<Table.ID,TableCounts> entry : watcher.getStats().entrySet()) {
            Table.ID tableId = entry.getKey();
            TableCounts counts = entry.getValue();
            TableState tableState = manager.getTableState(tableId);
            if (tableState != null && tableState.equals(TableState.ONLINE)) {
              result += counts.unassigned() + counts.assignedToDeadServers() + counts.assigned() + counts.suspended();
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

  public void mustBeOnline(final Table.ID tableId) throws ThriftTableOperationException {
    Tables.clearCache(getInstance());
    if (!Tables.getTableState(getInstance(), tableId).equals(TableState.ONLINE))
      throw new ThriftTableOperationException(tableId.canonicalID(), null, TableOperation.MERGE, TableOperationExceptionType.OFFLINE, "table is not online");
  }

  public Master(Instance instance, ServerConfigurationFactory config, VolumeManager fs, String hostname) throws IOException {
    super(instance, config);
    this.serverConfig = config;
    this.fs = fs;
    this.hostname = hostname;

    AccumuloConfiguration aconf = serverConfig.getSystemConfiguration();

    log.info("Version {}", Constants.VERSION);
    log.info("Instance {}", getInstance().getInstanceID());
    timeKeeper = new MasterTime(this);
    ThriftTransportPool.getInstance().setIdleTime(aconf.getTimeInMillis(Property.GENERAL_RPC_TIMEOUT));
    tserverSet = new LiveTServerSet(this, this);
    this.tabletBalancer = Property.createInstanceFromPropertyName(aconf, Property.MASTER_TABLET_BALANCER, TabletBalancer.class, new DefaultLoadBalancer());
    this.tabletBalancer.init(this);

    try {
      AccumuloVFSClassLoader.getContextManager().setContextConfig(new ContextManager.DefaultContextsConfig(new Iterable<Entry<String,String>>() {
        @Override
        public Iterator<Entry<String,String>> iterator() {
          return getConfiguration().iterator();
        }
      }));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    this.security = AuditedSecurityOperation.getInstance(this);

    // Create the secret manager (can generate and verify delegation tokens)
    final long tokenLifetime = aconf.getTimeInMillis(Property.GENERAL_DELEGATION_TOKEN_LIFETIME);
    setSecretManager(new AuthenticationTokenSecretManager(getInstance(), tokenLifetime));

    authenticationTokenKeyManager = null;
    keyDistributor = null;
    if (getConfiguration().getBoolean(Property.INSTANCE_RPC_SASL_ENABLED)) {
      // SASL is enabled, create the key distributor (ZooKeeper) and manager (generates/rolls secret keys)
      log.info("SASL is enabled, creating delegation token key manager and distributor");
      final long tokenUpdateInterval = aconf.getTimeInMillis(Property.GENERAL_DELEGATION_TOKEN_UPDATE_INTERVAL);
      keyDistributor = new ZooAuthenticationKeyDistributor(ZooReaderWriter.getInstance(), ZooUtil.getRoot(getInstance()) + Constants.ZDELEGATION_TOKEN_KEYS);
      authenticationTokenKeyManager = new AuthenticationTokenKeyManager(getSecretManager(), keyDistributor, tokenUpdateInterval, tokenLifetime);
      delegationTokensAvailable = true;
    } else {
      log.info("SASL is not enabled, delegation tokens will not be available");
      delegationTokensAvailable = false;
    }

  }

  public TServerConnection getConnection(TServerInstance server) {
    return tserverSet.getConnection(server);
  }

  public MergeInfo getMergeInfo(Table.ID tableId) {
    synchronized (mergeLock) {
      try {
        String path = ZooUtil.getRoot(getInstance().getInstanceID()) + Constants.ZTABLES + "/" + tableId + "/merge";
        if (!ZooReaderWriter.getInstance().exists(path))
          return new MergeInfo();
        byte[] data = ZooReaderWriter.getInstance().getData(path, new Stat());
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

  public void setMergeState(MergeInfo info, MergeState state) throws IOException, KeeperException, InterruptedException {
    synchronized (mergeLock) {
      String path = ZooUtil.getRoot(getInstance().getInstanceID()) + Constants.ZTABLES + "/" + info.getExtent().getTableId() + "/merge";
      info.setState(state);
      if (state.equals(MergeState.NONE)) {
        ZooReaderWriter.getInstance().recursiveDelete(path, NodeMissingPolicy.SKIP);
      } else {
        DataOutputBuffer out = new DataOutputBuffer();
        try {
          info.write(out);
        } catch (IOException ex) {
          throw new RuntimeException("Unlikely", ex);
        }
        ZooReaderWriter.getInstance().putPersistentData(path, out.getData(),
            state.equals(MergeState.STARTED) ? ZooUtil.NodeExistsPolicy.FAIL : ZooUtil.NodeExistsPolicy.OVERWRITE);
      }
      mergeLock.notifyAll();
    }
    nextEvent.event("Merge state of %s set to %s", info.getExtent(), state);
  }

  public void clearMergeState(Table.ID tableId) throws IOException, KeeperException, InterruptedException {
    synchronized (mergeLock) {
      String path = ZooUtil.getRoot(getInstance().getInstanceID()) + Constants.ZTABLES + "/" + tableId + "/merge";
      ZooReaderWriter.getInstance().recursiveDelete(path, NodeMissingPolicy.SKIP);
      mergeLock.notifyAll();
    }
    nextEvent.event("Merge state of %s cleared", tableId);
  }

  void setMasterGoalState(MasterGoalState state) {
    try {
      ZooReaderWriter.getInstance().putPersistentData(ZooUtil.getRoot(getInstance()) + Constants.ZMASTER_GOAL_STATE, state.name().getBytes(),
          NodeExistsPolicy.OVERWRITE);
    } catch (Exception ex) {
      log.error("Unable to set master goal state in zookeeper");
    }
  }

  MasterGoalState getMasterGoalState() {
    while (true)
      try {
        byte[] data = ZooReaderWriter.getInstance().getData(ZooUtil.getRoot(getInstance()) + Constants.ZMASTER_GOAL_STATE, null);
        return MasterGoalState.valueOf(new String(data));
      } catch (Exception e) {
        log.error("Problem getting real goal state from zookeeper: ", e);
        sleepUninterruptibly(1, TimeUnit.SECONDS);
      }
  }

  public boolean hasCycled(long time) {
    for (TabletGroupWatcher watcher : watchers) {
      if (watcher.stats.lastScanFinished() < time)
        return false;
    }

    return true;
  }

  public void clearMigrations(Table.ID tableId) {
    synchronized (migrations) {
      Iterator<KeyExtent> iterator = migrations.keySet().iterator();
      while (iterator.hasNext()) {
        KeyExtent extent = iterator.next();
        if (extent.getTableId().equals(tableId)) {
          iterator.remove();
        }
      }
    }
  }

  static enum TabletGoalState {
    HOSTED(TUnloadTabletGoal.UNKNOWN), UNASSIGNED(TUnloadTabletGoal.UNASSIGNED), DELETED(TUnloadTabletGoal.DELETED), SUSPENDED(TUnloadTabletGoal.SUSPENDED);

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
        if (tls.extent.isMeta())
          return TabletGoalState.HOSTED;
        return TabletGoalState.UNASSIGNED;
      case UNLOAD_METADATA_TABLETS:
        if (tls.extent.isRootTablet())
          return TabletGoalState.HOSTED;
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
    TableState tableState = TableManager.getInstance().getTableState(extent.getTableId());
    if (tableState == null)
      return TabletGoalState.DELETED;
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
      if (tls.current != null && serversToShutdown.contains(tls.current)) {
        return TabletGoalState.SUSPENDED;
      }
      // Handle merge transitions
      if (mergeInfo.getExtent() != null) {
        log.debug("mergeInfo overlaps: {} {}", extent, mergeInfo.overlaps(extent));
        if (mergeInfo.overlaps(extent)) {
          switch (mergeInfo.getState()) {
            case NONE:
            case COMPLETE:
              break;
            case STARTED:
            case SPLITTING:
              return TabletGoalState.HOSTED;
            case WAITING_FOR_CHOPPED:
              if (tls.getState(tserverSet.getCurrentServers()).equals(TabletState.HOSTED)) {
                if (tls.chopped)
                  return TabletGoalState.UNASSIGNED;
              } else {
                if (tls.chopped && tls.walogs.isEmpty())
                  return TabletGoalState.UNASSIGNED;
              }

              return TabletGoalState.HOSTED;
            case WAITING_FOR_OFFLINE:
            case MERGING:
              return TabletGoalState.UNASSIGNED;
          }
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

  private class MigrationCleanupThread extends Daemon {

    @Override
    public void run() {
      setName("Migration Cleanup Thread");
      while (stillMaster()) {
        if (!migrations.isEmpty()) {
          try {
            cleanupOfflineMigrations();
            cleanupNonexistentMigrations(getConnector());
          } catch (Exception ex) {
            log.error("Error cleaning up migrations", ex);
          }
        }
        sleepUninterruptibly(TIME_BETWEEN_MIGRATION_CLEANUPS, TimeUnit.MILLISECONDS);
      }
    }

    /**
     * If a migrating tablet splits, and the tablet dies before sending the master a message, the migration will refer to a non-existing tablet, so it can never
     * complete. Periodically scan the metadata table and remove any migrating tablets that no longer exist.
     */
    private void cleanupNonexistentMigrations(final Connector connector) throws AccumuloException, AccumuloSecurityException, TableNotFoundException {
      Scanner scanner = connector.createScanner(MetadataTable.NAME, Authorizations.EMPTY);
      TabletsSection.TabletColumnFamily.PREV_ROW_COLUMN.fetch(scanner);
      Set<KeyExtent> found = new HashSet<>();
      for (Entry<Key,Value> entry : scanner) {
        KeyExtent extent = new KeyExtent(entry.getKey().getRow(), entry.getValue());
        if (migrations.containsKey(extent)) {
          found.add(extent);
        }
      }
      migrations.keySet().retainAll(found);
    }

    /**
     * If migrating a tablet for a table that is offline, the migration can never succeed because no tablet server will load the tablet. check for offline
     * tables and remove their migrations.
     */
    private void cleanupOfflineMigrations() {
      TableManager manager = TableManager.getInstance();
      for (Table.ID tableId : Tables.getIdToNameMap(getInstance()).keySet()) {
        TableState state = manager.getTableState(tableId);
        if (TableState.OFFLINE == state) {
          clearMigrations(tableId);
        }
      }
    }
  }

  private class StatusThread extends Daemon {

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
          log.debug("{}: {} != {}", watcher.getName(), watcher.stats.getLastMasterState(), getMasterState());
          return false;
        }
      }
      return true;
    }

    @Override
    public void run() {
      setName("Status Thread");
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
                  log.debug(String.format("There are %d non-metadata tablets assigned or hosted", count));
                  if (count == 0 && goodStats())
                    setMasterState(MasterState.UNLOAD_METADATA_TABLETS);
                }
                  break;
                case UNLOAD_METADATA_TABLETS: {
                  int count = assignedOrHosted(MetadataTable.ID);
                  log.debug(String.format("There are %d metadata tablets assigned or hosted", count));
                  if (count == 0 && goodStats())
                    setMasterState(MasterState.UNLOAD_ROOT_TABLET);
                }
                  break;
                case UNLOAD_ROOT_TABLET: {
                  int count = assignedOrHosted(MetadataTable.ID);
                  if (count > 0 && goodStats()) {
                    log.debug(String.format("%d metadata tablets online", count));
                    setMasterState(MasterState.UNLOAD_ROOT_TABLET);
                  }
                  int root_count = assignedOrHosted(RootTable.ID);
                  if (root_count > 0 && goodStats())
                    log.debug("The root tablet is still assigned or hosted");
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
                    if (currentServers.size() == 0)
                      setMasterState(MasterState.STOP);
                  }
                }
                  break;
                default:
                  break;
              }
          }
        } catch (Throwable t) {
          log.error("Error occurred reading / switching master goal state. Will continue with attempt to update status", t);
        }

        try {
          wait = updateStatus();
          eventListener.waitForEvents(wait);
        } catch (Throwable t) {
          log.error("Error balancing tablets, will wait for {} (seconds) and then retry ", WAIT_BETWEEN_ERRORS / ONE_SECOND, t);
          sleepUninterruptibly(WAIT_BETWEEN_ERRORS, TimeUnit.MILLISECONDS);
        }
      }
    }

    private long updateStatus() throws AccumuloException, AccumuloSecurityException, TableNotFoundException {
      Set<TServerInstance> currentServers = tserverSet.getCurrentServers();
      tserverStatus = Collections.synchronizedSortedMap(gatherTableInformation(currentServers));
      checkForHeldServer(tserverStatus);

      if (!badServers.isEmpty()) {
        log.debug("not balancing because the balance information is out-of-date {}", badServers.keySet());
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
          if (connection != null)
            connection.fastHalt(masterLock);
        } catch (TException e) {
          log.error("{}", e.getMessage(), e);
        }
        tserverSet.remove(instance);
      }
    }

    private long balanceTablets() {
      List<TabletMigration> migrationsOut = new ArrayList<>();
      long wait = tabletBalancer.balance(Collections.unmodifiableSortedMap(tserverStatus), migrationsSnapshot(), migrationsOut);

      for (TabletMigration m : TabletBalancer.checkMigrationSanity(tserverStatus.keySet(), migrationsOut)) {
        if (migrations.containsKey(m.tablet)) {
          log.warn("balancer requested migration more than once, skipping {}", m);
          continue;
        }
        migrations.put(m.tablet, m.newServer);
        log.debug("migration {}", m);
      }
      if (migrationsOut.size() > 0) {
        nextEvent.event("Migrating %d more tablets, %d total", migrationsOut.size(), migrations.size());
      } else {
        synchronized (balancedNotifier) {
          balancedNotifier.notifyAll();
        }
      }
      return wait;
    }

  }

  private SortedMap<TServerInstance,TabletServerStatus> gatherTableInformation(Set<TServerInstance> currentServers) {
    long start = System.currentTimeMillis();
    int threads = Math.max(getConfiguration().getCount(Property.MASTER_STATUS_THREAD_POOL_SIZE), 1);
    ExecutorService tp = Executors.newFixedThreadPool(threads);
    final SortedMap<TServerInstance,TabletServerStatus> result = new TreeMap<>();
    for (TServerInstance serverInstance : currentServers) {
      final TServerInstance server = serverInstance;
      tp.submit(new Runnable() {
        @Override
        public void run() {
          try {
            Thread t = Thread.currentThread();
            String oldName = t.getName();
            try {
              t.setName("Getting status from " + server);
              TServerConnection connection = tserverSet.getConnection(server);
              if (connection == null)
                throw new IOException("No connection to " + server);
              TabletServerStatus status = connection.getTableMap(false);
              result.put(server, status);
            } finally {
              t.setName(oldName);
            }
          } catch (Exception ex) {
            log.error("unable to get tablet server status {} {}", server, ex.toString());
            log.debug("unable to get tablet server status {}", server, ex);
            if (badServers.get(server).incrementAndGet() > MAX_BAD_STATUS_COUNT) {
              log.warn("attempting to stop {}", server);
              try {
                TServerConnection connection = tserverSet.getConnection(server);
                if (connection != null) {
                  connection.halt(masterLock);
                }
              } catch (TTransportException e) {
                // ignore: it's probably down
              } catch (Exception e) {
                log.info("error talking to troublesome tablet server", e);
              }
              badServers.remove(server);
            }
          }
        }
      });
    }
    tp.shutdown();
    try {
      tp.awaitTermination(getConfiguration().getTimeInMillis(Property.TSERV_CLIENT_TIMEOUT) * 2, TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      log.debug("Interrupted while fetching status");
    }
    synchronized (badServers) {
      badServers.keySet().retainAll(currentServers);
      badServers.keySet().removeAll(result.keySet());
    }
    log.debug(String.format("Finished gathering information from %d servers in %.2f seconds", result.size(), (System.currentTimeMillis() - start) / 1000.));
    return result;
  }

  public void run() throws IOException, InterruptedException, KeeperException {
    final String zroot = ZooUtil.getRoot(getInstance());

    // ACCUMULO-4424 Put up the Thrift servers before getting the lock as a sign of process health when a hot-standby
    //
    // Start the Master's Client service
    clientHandler = new MasterClientServiceHandler(this);
    // Ensure that calls before the master gets the lock fail
    Iface haProxy = HighlyAvailableServiceWrapper.service(clientHandler, this);
    Iface rpcProxy = RpcWrapper.service(haProxy);
    final Processor<Iface> processor;
    if (ThriftServerType.SASL == getThriftServerType()) {
      Iface tcredsProxy = TCredentialsUpdatingWrapper.service(rpcProxy, clientHandler.getClass(), getConfiguration());
      processor = new Processor<>(tcredsProxy);
    } else {
      processor = new Processor<>(rpcProxy);
    }
    ServerAddress sa = TServerUtils.startServer(this, hostname, Property.MASTER_CLIENTPORT, processor, "Master", "Master Client Service Handler", null,
        Property.MASTER_MINTHREADS, Property.MASTER_THREADCHECK, Property.GENERAL_MAX_MESSAGE_SIZE);
    clientService = sa.server;
    log.info("Started Master client service at {}", sa.address);

    // Start the replication coordinator which assigns tservers to service replication requests
    MasterReplicationCoordinator impl = new MasterReplicationCoordinator(this);
    ReplicationCoordinator.Iface haReplicationProxy = HighlyAvailableServiceWrapper.service(impl, this);
    ReplicationCoordinator.Processor<ReplicationCoordinator.Iface> replicationCoordinatorProcessor = new ReplicationCoordinator.Processor<>(
        RpcWrapper.service(haReplicationProxy));
    ServerAddress replAddress = TServerUtils.startServer(this, hostname, Property.MASTER_REPLICATION_COORDINATOR_PORT, replicationCoordinatorProcessor,
        "Master Replication Coordinator", "Replication Coordinator", null, Property.MASTER_REPLICATION_COORDINATOR_MINTHREADS,
        Property.MASTER_REPLICATION_COORDINATOR_THREADCHECK, Property.GENERAL_MAX_MESSAGE_SIZE);

    log.info("Started replication coordinator service at " + replAddress.address);

    // block until we can obtain the ZK lock for the master
    getMasterLock(zroot + Constants.ZMASTER_LOCK);

    recoveryManager = new RecoveryManager(this);

    TableManager.getInstance().addObserver(this);

    StatusThread statusThread = new StatusThread();
    statusThread.start();

    MigrationCleanupThread migrationCleanupThread = new MigrationCleanupThread();
    migrationCleanupThread.start();

    tserverSet.startListeningForTabletServerChanges();

    ZooReaderWriter zReaderWriter = ZooReaderWriter.getInstance();

    zReaderWriter.getChildren(zroot + Constants.ZRECOVERY, new Watcher() {
      @Override
      public void process(WatchedEvent event) {
        nextEvent.event("Noticed recovery changes", event.getType());
        try {
          // watcher only fires once, add it back
          ZooReaderWriter.getInstance().getChildren(zroot + Constants.ZRECOVERY, this);
        } catch (Exception e) {
          log.error("Failed to add log recovery watcher back", e);
        }
      }
    });

    watchers.add(new TabletGroupWatcher(this, new MetaDataStateStore(this, this), null) {
      @Override
      boolean canSuspendTablets() {
        // Always allow user data tablets to enter suspended state.
        return true;
      }
    });

    watchers.add(new TabletGroupWatcher(this, new RootTabletStateStore(this, this), watchers.get(0)) {
      @Override
      boolean canSuspendTablets() {
        // Allow metadata tablets to enter suspended state only if so configured. Generally we'll want metadata tablets to
        // be immediately reassigned, even if there's a global table.suspension.duration setting.
        return getConfiguration().getBoolean(Property.MASTER_METADATA_SUSPENDABLE);
      }
    });

    watchers.add(new TabletGroupWatcher(this, new ZooTabletStateStore(new ZooStore(zroot)), watchers.get(1)) {
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
    waitForMetadataUpgrade.await();

    try {
      final AgeOffStore<Master> store = new AgeOffStore<>(new org.apache.accumulo.fate.ZooStore<Master>(ZooUtil.getRoot(getInstance()) + Constants.ZFATE,
          ZooReaderWriter.getInstance()), 1000 * 60 * 60 * 8);

      int threads = getConfiguration().getCount(Property.MASTER_FATE_THREADPOOL_SIZE);

      fate = new Fate<>(this, store);
      fate.startTransactionRunners(threads);

      SimpleTimer.getInstance(getConfiguration()).schedule(new Runnable() {

        @Override
        public void run() {
          store.ageOff();
        }
      }, 63000, 63000);
    } catch (KeeperException e) {
      throw new IOException(e);
    } catch (InterruptedException e) {
      throw new IOException(e);
    }

    ZooKeeperInitialization.ensureZooKeeperInitialized(zReaderWriter, zroot);

    // Make sure that we have a secret key (either a new one or an old one from ZK) before we start
    // the master client service.
    if (null != authenticationTokenKeyManager && null != keyDistributor) {
      log.info("Starting delegation-token key manager");
      keyDistributor.initialize();
      authenticationTokenKeyManager.start();
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
    masterLock.replaceLockData(address.getBytes());

    while (!clientService.isServing()) {
      sleepUninterruptibly(100, TimeUnit.MILLISECONDS);
    }

    // Start the daemon to scan the replication table and make units of work
    replicationWorkDriver = new ReplicationDriver(this);
    replicationWorkDriver.start();

    // Start the daemon to assign work to tservers to replicate to our peers
    try {
      replicationWorkAssigner = new WorkDriver(this);
    } catch (AccumuloException | AccumuloSecurityException e) {
      log.error("Caught exception trying to initialize replication WorkDriver", e);
      throw new RuntimeException(e);
    }
    replicationWorkAssigner.start();

    // Advertise that port we used so peers don't have to be told what it is
    ZooReaderWriter.getInstance().putPersistentData(ZooUtil.getRoot(getInstance()) + Constants.ZMASTER_REPLICATION_COORDINATOR_ADDR,
        replAddress.address.toString().getBytes(UTF_8), NodeExistsPolicy.OVERWRITE);

    // Register replication metrics
    MasterMetricsFactory factory = new MasterMetricsFactory(getConfiguration(), this);
    Metrics replicationMetrics = factory.createReplicationMetrics();
    try {
      replicationMetrics.register();
    } catch (Exception e) {
      log.error("Failed to register replication metrics", e);
    }

    // The master is fully initialized. Clients are allowed to connect now.
    masterInitialized.set(true);

    while (clientService.isServing()) {
      sleepUninterruptibly(500, TimeUnit.MILLISECONDS);
    }
    log.info("Shutting down fate.");
    fate.shutdown();

    log.info("Shutting down timekeeping.");
    timeKeeper.shutdown();

    final long deadline = System.currentTimeMillis() + MAX_CLEANUP_WAIT_TIME;
    statusThread.join(remaining(deadline));
    replicationWorkAssigner.join(remaining(deadline));
    replicationWorkDriver.join(remaining(deadline));
    replAddress.server.stop();
    // Signal that we want it to stop, and wait for it to do so.
    if (authenticationTokenKeyManager != null) {
      authenticationTokenKeyManager.gracefulStop();
      authenticationTokenKeyManager.join(remaining(deadline));
    }

    // quit, even if the tablet servers somehow jam up and the watchers
    // don't stop
    for (TabletGroupWatcher watcher : watchers) {
      watcher.join(remaining(deadline));
    }
    log.info("exiting");
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
    public void unableToMonitorLockNode(final Throwable e) {
      // ACCUMULO-3651 Changed level to error and added FATAL to message for slf4j compatibility
      Halt.halt(-1, new Runnable() {
        @Override
        public void run() {
          log.error("FATAL: No longer able to monitor master lock node", e);
        }
      });

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
        Halt.halt("Zoolock in unexpected state FAL " + acquiredLock + " " + failedToAcquireLock, -1);
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
    log.info("trying to get master lock");

    final String masterClientAddress = hostname + ":" + getConfiguration().getPort(Property.MASTER_CLIENTPORT)[0];

    while (true) {

      MasterLockWatcher masterLockWatcher = new MasterLockWatcher();
      masterLock = new ZooLock(zMasterLoc);
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

  public static void main(String[] args) throws Exception {
    try {
      final String app = "master";
      ServerOpts opts = new ServerOpts();
      opts.parseArgs(app, args);
      SecurityUtil.serverLogin(SiteConfiguration.getInstance());
      String hostname = opts.getAddress();
      Instance instance = HdfsZooInstance.getInstance();
      ServerConfigurationFactory conf = new ServerConfigurationFactory(instance);
      VolumeManager fs = VolumeManagerImpl.get();
      MetricsSystemHelper.configure(Master.class.getSimpleName());
      Accumulo.init(fs, instance, conf, app);
      Master master = new Master(instance, conf, fs, hostname);
      DistributedTrace.enable(hostname, app, conf.getSystemConfiguration());
      master.run();
    } catch (Exception ex) {
      log.error("Unexpected exception, exiting", ex);
      System.exit(1);
    } finally {
      DistributedTrace.disable();
    }
  }

  @Override
  public void update(LiveTServerSet current, Set<TServerInstance> deleted, Set<TServerInstance> added) {
    DeadServerList obit = new DeadServerList(ZooUtil.getRoot(getInstance()) + Constants.ZDEADTSERVERS);
    if (added.size() > 0) {
      log.info("New servers: {}", added);
      for (TServerInstance up : added)
        obit.delete(up.hostPort());
    }
    for (TServerInstance dead : deleted) {
      String cause = "unexpected failure";
      if (serversToShutdown.contains(dead))
        cause = "clean shutdown"; // maybe an incorrect assumption
      if (!getMasterGoalState().equals(MasterGoalState.CLEAN_STOP))
        obit.post(dead.hostPort(), cause);
    }

    Set<TServerInstance> unexpected = new HashSet<>(deleted);
    unexpected.removeAll(this.serversToShutdown);
    if (unexpected.size() > 0) {
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

  private static void cleanListByHostAndPort(Collection<TServerInstance> badServers, Set<TServerInstance> deleted, Set<TServerInstance> added) {
    Iterator<TServerInstance> badIter = badServers.iterator();
    while (badIter.hasNext()) {
      TServerInstance bad = badIter.next();
      for (TServerInstance add : added) {
        if (bad.hostPort().equals(add.hostPort())) {
          badIter.remove();
          break;
        }
      }
      for (TServerInstance del : deleted) {
        if (bad.hostPort().equals(del.hostPort())) {
          badIter.remove();
          break;
        }
      }
    }
  }

  @Override
  public void stateChanged(Table.ID tableId, TableState state) {
    nextEvent.event("Table state in zookeeper changed for %s to %s", tableId, state);
    if (TableState.OFFLINE == state) {
      clearMigrations(tableId);
    }
  }

  @Override
  public void initialize(Map<Table.ID,TableState> tableIdToStateMap) {}

  @Override
  public void sessionExpired() {}

  @Override
  public Set<Table.ID> onlineTables() {
    Set<Table.ID> result = new HashSet<>();
    if (getMasterState() != MasterState.NORMAL) {
      if (getMasterState() != MasterState.UNLOAD_METADATA_TABLETS)
        result.add(MetadataTable.ID);
      if (getMasterState() != MasterState.UNLOAD_ROOT_TABLET)
        result.add(RootTable.ID);
      return result;
    }
    TableManager manager = TableManager.getInstance();

    for (Table.ID tableId : Tables.getIdToNameMap(getInstance()).keySet()) {
      TableState state = manager.getTableState(tableId);
      if (state != null) {
        if (state == TableState.ONLINE)
          result.add(tableId);
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
    for (Table.ID tableId : Tables.getIdToNameMap(getInstance()).keySet()) {
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

  public ServerConfigurationFactory getConfigurationFactory() {
    return serverConfig;
  }

  public VolumeManager getFileSystem() {
    return this.fs;
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

  public void waitForBalance(TInfo tinfo) {
    synchronized (balancedNotifier) {
      long eventCounter;
      do {
        eventCounter = nextEvent.waitForEvents(0, 0);
        try {
          balancedNotifier.wait();
        } catch (InterruptedException e) {
          log.debug(e.toString(), e);
        }
      } while (displayUnassigned() > 0 || migrations.size() > 0 || eventCounter != nextEvent.waitForEvents(0, 0));
    }
  }

  public MasterMonitorInfo getMasterMonitorInfo() {
    final MasterMonitorInfo result = new MasterMonitorInfo();

    result.tServerInfo = new ArrayList<>();
    result.tableMap = new DefaultMap<>(new TableInfo());
    for (Entry<TServerInstance,TabletServerStatus> serverEntry : tserverStatus.entrySet()) {
      final TabletServerStatus status = serverEntry.getValue();
      result.tServerInfo.add(status);
      for (Entry<String,TableInfo> entry : status.tableMap.entrySet()) {
        TableInfoUtil.add(result.tableMap.get(entry.getKey()), entry.getValue());
      }
    }
    result.badTServers = new HashMap<>();
    synchronized (badServers) {
      for (TServerInstance bad : badServers.keySet()) {
        result.badTServers.put(bad.hostPort(), TabletServerState.UNRESPONSIVE.getId());
      }
    }
    result.state = getMasterState();
    result.goalState = getMasterGoalState();
    result.unassignedTablets = displayUnassigned();
    result.serversShuttingDown = new HashSet<>();
    synchronized (serversToShutdown) {
      for (TServerInstance server : serversToShutdown)
        result.serversShuttingDown.add(server.hostPort());
    }
    DeadServerList obit = new DeadServerList(ZooUtil.getRoot(getInstance()) + Constants.ZDEADTSERVERS);
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

  public void markDeadServerLogsAsClosed(Map<TServerInstance,List<Path>> logsForDeadServers) throws WalMarkerException {
    WalStateManager mgr = new WalStateManager(this.inst, ZooReaderWriter.getInstance());
    for (Entry<TServerInstance,List<Path>> server : logsForDeadServers.entrySet()) {
      for (Path path : server.getValue()) {
        mgr.closeWal(server.getKey(), path);
      }
    }
  }

  public void updateBulkImportStatus(String directory, BulkImportState state) {
    bulkImportStatus.updateBulkImportStatus(Collections.singletonList(directory), state);
  }

  public void removeBulkImportStatus(String directory) {
    bulkImportStatus.removeBulkImportStatus(Collections.singletonList(directory));
  }

  /**
   * Return how long (in milliseconds) there has been a master overseeing this cluster. This is an approximately monotonic clock, which will be approximately
   * consistent between different masters or different runs of the same master.
   */
  public Long getSteadyTime() {
    return timeKeeper.getTime();
  }

  @Override
  public boolean isActiveService() {
    return masterInitialized.get();
  }
}

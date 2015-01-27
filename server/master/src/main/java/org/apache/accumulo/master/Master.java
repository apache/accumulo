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

import static com.google.common.base.Charsets.UTF_8;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.impl.Namespaces;
import org.apache.accumulo.core.client.impl.Tables;
import org.apache.accumulo.core.client.impl.ThriftTransportPool;
import org.apache.accumulo.core.client.impl.thrift.TableOperation;
import org.apache.accumulo.core.client.impl.thrift.TableOperationExceptionType;
import org.apache.accumulo.core.client.impl.thrift.ThriftTableOperationException;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.KeyExtent;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.master.state.tables.TableState;
import org.apache.accumulo.core.master.thrift.MasterClientService.Iface;
import org.apache.accumulo.core.master.thrift.MasterClientService.Processor;
import org.apache.accumulo.core.master.thrift.MasterGoalState;
import org.apache.accumulo.core.master.thrift.MasterState;
import org.apache.accumulo.core.master.thrift.TabletServerStatus;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.metadata.RootTable;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.Credentials;
import org.apache.accumulo.core.security.NamespacePermission;
import org.apache.accumulo.core.security.TablePermission;
import org.apache.accumulo.core.util.Daemon;
import org.apache.accumulo.core.util.Pair;
import org.apache.accumulo.core.util.UtilWaitThread;
import org.apache.accumulo.core.zookeeper.ZooUtil;
import org.apache.accumulo.fate.AgeOffStore;
import org.apache.accumulo.fate.Fate;
import org.apache.accumulo.fate.zookeeper.IZooReaderWriter;
import org.apache.accumulo.fate.zookeeper.ZooLock.LockLossReason;
import org.apache.accumulo.fate.zookeeper.ZooUtil.NodeExistsPolicy;
import org.apache.accumulo.fate.zookeeper.ZooUtil.NodeMissingPolicy;
import org.apache.accumulo.master.recovery.RecoveryManager;
import org.apache.accumulo.master.state.TableCounts;
import org.apache.accumulo.server.Accumulo;
import org.apache.accumulo.server.ServerConstants;
import org.apache.accumulo.server.ServerOpts;
import org.apache.accumulo.server.client.HdfsZooInstance;
import org.apache.accumulo.server.conf.ServerConfiguration;
import org.apache.accumulo.server.fs.VolumeManager;
import org.apache.accumulo.server.fs.VolumeManager.FileType;
import org.apache.accumulo.server.fs.VolumeManagerImpl;
import org.apache.accumulo.server.init.Initialize;
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
import org.apache.accumulo.server.master.state.TabletState;
import org.apache.accumulo.server.master.state.ZooStore;
import org.apache.accumulo.server.master.state.ZooTabletStateStore;
import org.apache.accumulo.server.security.AuditedSecurityOperation;
import org.apache.accumulo.server.security.SecurityOperation;
import org.apache.accumulo.server.security.SecurityUtil;
import org.apache.accumulo.server.security.SystemCredentials;
import org.apache.accumulo.server.security.handler.ZKPermHandler;
import org.apache.accumulo.server.tables.TableManager;
import org.apache.accumulo.server.tables.TableObserver;
import org.apache.accumulo.server.util.DefaultMap;
import org.apache.accumulo.server.util.Halt;
import org.apache.accumulo.server.util.MetadataTableUtil;
import org.apache.accumulo.server.util.RpcWrapper;
import org.apache.accumulo.server.util.TServerUtils;
import org.apache.accumulo.server.util.TServerUtils.ServerAddress;
import org.apache.accumulo.server.util.time.SimpleTimer;
import org.apache.accumulo.server.zookeeper.ZooLock;
import org.apache.accumulo.server.zookeeper.ZooReaderWriter;
import org.apache.accumulo.start.classloader.vfs.AccumuloVFSClassLoader;
import org.apache.accumulo.start.classloader.vfs.ContextManager;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;
import org.apache.thrift.TException;
import org.apache.thrift.server.TServer;
import org.apache.thrift.transport.TTransportException;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.Stat;

import com.google.common.collect.Iterables;

/**
 * The Master is responsible for assigning and balancing tablets to tablet servers.
 *
 * The master will also coordinate log recoveries and reports general status.
 */
public class Master implements LiveTServerSet.Listener, TableObserver, CurrentState {

  final static Logger log = Logger.getLogger(Master.class);

  final static int ONE_SECOND = 1000;
  final private static Text METADATA_TABLE_ID = new Text(MetadataTable.ID);
  final private static Text ROOT_TABLE_ID = new Text(RootTable.ID);
  final static long TIME_TO_WAIT_BETWEEN_SCANS = 60 * ONE_SECOND;
  final private static long TIME_BETWEEN_MIGRATION_CLEANUPS = 5 * 60 * ONE_SECOND;
  final static long WAIT_BETWEEN_ERRORS = ONE_SECOND;
  final private static long DEFAULT_WAIT_FOR_WATCHER = 10 * ONE_SECOND;
  final private static int MAX_CLEANUP_WAIT_TIME = ONE_SECOND;
  final private static int TIME_TO_WAIT_BETWEEN_LOCK_CHECKS = ONE_SECOND;
  final static int MAX_TSERVER_WORK_CHUNK = 5000;
  final private static int MAX_BAD_STATUS_COUNT = 3;

  final VolumeManager fs;
  final private Instance instance;
  final private String hostname;
  final LiveTServerSet tserverSet;
  final private List<TabletGroupWatcher> watchers = new ArrayList<TabletGroupWatcher>();
  final SecurityOperation security;
  final Map<TServerInstance,AtomicInteger> badServers = Collections.synchronizedMap(new DefaultMap<TServerInstance,AtomicInteger>(new AtomicInteger()));
  final Set<TServerInstance> serversToShutdown = Collections.synchronizedSet(new HashSet<TServerInstance>());
  final SortedMap<KeyExtent,TServerInstance> migrations = Collections.synchronizedSortedMap(new TreeMap<KeyExtent,TServerInstance>());
  final EventCoordinator nextEvent = new EventCoordinator();
  final private Object mergeLock = new Object();
  RecoveryManager recoveryManager = null;

  ZooLock masterLock = null;
  private TServer clientService = null;
  TabletBalancer tabletBalancer;

  private MasterState state = MasterState.INITIAL;

  Fate<Master> fate;

  volatile SortedMap<TServerInstance,TabletServerStatus> tserverStatus = Collections.unmodifiableSortedMap(new TreeMap<TServerInstance,TabletServerStatus>());

  synchronized MasterState getMasterState() {
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
      log.error("Programmer error: master should not transition from " + state + " to " + newState);
    }
    MasterState oldState = state;
    state = newState;
    nextEvent.event("State changed from %s to %s", oldState, newState);
    if (newState == MasterState.STOP) {
      // Give the server a little time before shutdown so the client
      // thread requesting the stop can return
      SimpleTimer.getInstance().schedule(new Runnable() {
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
    String dirZPath = ZooUtil.getRoot(instance) + RootTable.ZROOT_TABLET_PATH;

    if (!zoo.exists(dirZPath)) {
      Path oldPath = fs.getFullPath(FileType.TABLE, "/" + MetadataTable.ID + "/root_tablet");
      if (fs.exists(oldPath)) {
        String newPath = fs.choose(ServerConstants.getBaseUris()) + Constants.HDFS_TABLES_DIR + Path.SEPARATOR + RootTable.ID;
        fs.mkdirs(new Path(newPath));
        if (!fs.rename(oldPath, new Path(newPath))) {
          throw new IOException("Failed to move root tablet from " + oldPath + " to " + newPath);
        }

        log.info("Upgrade renamed " + oldPath + " to " + newPath);
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

      log.info("Upgrade setting root table location in zookeeper " + location);
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

      if (serverConfig.getConfiguration().get(Property.INSTANCE_VOLUMES).trim().length() > 0) {
        throw new IllegalStateException("Do not set property " + Property.INSTANCE_VOLUMES.getKey()
            + " until after upgrade. Kill all Accumulo processes, unset property, and try again.");
      }

      try {
        log.info("Upgrading zookeeper");

        IZooReaderWriter zoo = ZooReaderWriter.getInstance();
        final String zooRoot = ZooUtil.getRoot(instance);

        if (accumuloPersistentVersion == ServerConstants.TWO_DATA_VERSIONS_AGO) {
          log.debug("Handling updates for version " + ServerConstants.TWO_DATA_VERSIONS_AGO);

          log.debug("Cleaning out remnants of logger role.");
          zoo.recursiveDelete(zooRoot + "/loggers", NodeMissingPolicy.SKIP);
          zoo.recursiveDelete(zooRoot + "/dead/loggers", NodeMissingPolicy.SKIP);

          final byte[] zero = new byte[] {'0'};
          log.debug("Initializing recovery area.");
          zoo.putPersistentData(zooRoot + Constants.ZRECOVERY, zero, NodeExistsPolicy.SKIP);

          for (String id : zoo.getChildren(zooRoot + Constants.ZTABLES)) {
            log.debug("Prepping table " + id + " for compaction cancellations.");
            zoo.putPersistentData(zooRoot + Constants.ZTABLES + "/" + id + Constants.ZTABLE_COMPACT_CANCEL_ID, zero, NodeExistsPolicy.SKIP);
          }
        }

        // create initial namespaces
        String namespaces = ZooUtil.getRoot(instance) + Constants.ZNAMESPACES;
        zoo.putPersistentData(namespaces, new byte[0], NodeExistsPolicy.SKIP);
        for (Pair<String,String> namespace : Iterables.concat(
            Collections.singleton(new Pair<String,String>(Namespaces.ACCUMULO_NAMESPACE, Namespaces.ACCUMULO_NAMESPACE_ID)),
            Collections.singleton(new Pair<String,String>(Namespaces.DEFAULT_NAMESPACE, Namespaces.DEFAULT_NAMESPACE_ID)))) {
          String ns = namespace.getFirst();
          String id = namespace.getSecond();
          log.debug("Upgrade creating namespace \"" + ns + "\" (ID: " + id + ")");
          if (!Namespaces.exists(instance, id))
            TableManager.prepareNewNamespaceState(instance.getInstanceID(), id, ns, NodeExistsPolicy.SKIP);
        }

        // create root table
        log.debug("Upgrade creating table " + RootTable.NAME + " (ID: " + RootTable.ID + ")");
        TableManager.prepareNewTableState(instance.getInstanceID(), RootTable.ID, Namespaces.ACCUMULO_NAMESPACE_ID, RootTable.NAME, TableState.ONLINE,
            NodeExistsPolicy.SKIP);
        Initialize.initMetadataConfig();
        // ensure root user can flush root table
        security.grantTablePermission(SystemCredentials.get().toThrift(instance), security.getRootUsername(), RootTable.ID, TablePermission.ALTER_TABLE,
            Namespaces.ACCUMULO_NAMESPACE_ID);

        // put existing tables in the correct namespaces
        String tables = ZooUtil.getRoot(instance) + Constants.ZTABLES;
        for (String tableId : zoo.getChildren(tables)) {
          String targetNamespace = (MetadataTable.ID.equals(tableId) || RootTable.ID.equals(tableId)) ? Namespaces.ACCUMULO_NAMESPACE_ID
              : Namespaces.DEFAULT_NAMESPACE_ID;
          log.debug("Upgrade moving table " + new String(zoo.getData(tables + "/" + tableId + Constants.ZTABLE_NAME, null), UTF_8) + " (ID: " + tableId
              + ") into namespace with ID " + targetNamespace);
          zoo.putPersistentData(tables + "/" + tableId + Constants.ZTABLE_NAMESPACE, targetNamespace.getBytes(UTF_8), NodeExistsPolicy.SKIP);
        }

        // rename metadata table
        log.debug("Upgrade renaming table " + MetadataTable.OLD_NAME + " (ID: " + MetadataTable.ID + ") to " + MetadataTable.NAME);
        zoo.putPersistentData(tables + "/" + MetadataTable.ID + Constants.ZTABLE_NAME, Tables.qualify(MetadataTable.NAME).getSecond().getBytes(UTF_8),
            NodeExistsPolicy.OVERWRITE);

        moveRootTabletToRootTable(zoo);

        // add system namespace permissions to existing users
        ZKPermHandler perm = new ZKPermHandler();
        perm.initialize(instance.getInstanceID(), true);
        String users = ZooUtil.getRoot(instance) + "/users";
        for (String user : zoo.getChildren(users)) {
          zoo.putPersistentData(users + "/" + user + "/Namespaces", new byte[0], NodeExistsPolicy.SKIP);
          perm.grantNamespacePermission(user, Namespaces.ACCUMULO_NAMESPACE_ID, NamespacePermission.READ);
        }
        perm.grantNamespacePermission("root", Namespaces.ACCUMULO_NAMESPACE_ID, NamespacePermission.ALTER_TABLE);
        haveUpgradedZooKeeper = true;
      } catch (Exception ex) {
        log.fatal("Error performing upgrade", ex);
        System.exit(1);
      }
    }
  }

  private final AtomicBoolean upgradeMetadataRunning = new AtomicBoolean(false);
  private final CountDownLatch waitForMetadataUpgrade = new CountDownLatch(1);

  private final ServerConfiguration serverConfig;

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
          @Override
          public void run() {
            try {
              log.info("Starting to upgrade metadata table.");
              if (accumuloPersistentVersion == ServerConstants.TWO_DATA_VERSIONS_AGO) {
                log.info("Updating Delete Markers in metadata table for version 1.4");
                MetadataTableUtil.moveMetaDeleteMarkersFrom14(instance, SystemCredentials.get());
              } else {
                log.info("Updating Delete Markers in metadata table.");
                MetadataTableUtil.moveMetaDeleteMarkers(instance, SystemCredentials.get());
              }
              log.info("Updating persistent data version.");
              Accumulo.updateAccumuloVersion(fs, accumuloPersistentVersion);
              log.info("Upgrade complete");
              waitForMetadataUpgrade.countDown();
            } catch (Exception ex) {
              log.fatal("Error performing upgrade", ex);
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

  private int assignedOrHosted(Text tableId) {
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
    return totalAssignedOrHosted() - assignedOrHosted(new Text(MetadataTable.ID)) - assignedOrHosted(new Text(RootTable.ID));
  }

  private int notHosted() {
    int result = 0;
    for (TabletGroupWatcher watcher : watchers) {
      for (TableCounts counts : watcher.getStats().values()) {
        result += counts.assigned() + counts.assignedToDeadServers();
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
          for (Entry<Text,TableCounts> entry : watcher.getStats().entrySet()) {
            Text tableId = entry.getKey();
            TableCounts counts = entry.getValue();
            TableState tableState = manager.getTableState(tableId.toString());
            if (tableState != null && tableState.equals(TableState.ONLINE)) {
              result += counts.unassigned() + counts.assignedToDeadServers() + counts.assigned();
            }
          }
        }
        break;
      case SAFE_MODE:
        // Count offline tablets for the metadata table
        for (TabletGroupWatcher watcher : watchers) {
          result += watcher.getStats(METADATA_TABLE_ID).unassigned();
        }
        break;
      case UNLOAD_METADATA_TABLETS:
      case UNLOAD_ROOT_TABLET:
        for (TabletGroupWatcher watcher : watchers) {
          result += watcher.getStats(METADATA_TABLE_ID).unassigned();
        }
        break;
      default:
        break;
    }
    return result;
  }

  public void mustBeOnline(final String tableId) throws ThriftTableOperationException {
    Tables.clearCache(instance);
    if (!Tables.getTableState(instance, tableId).equals(TableState.ONLINE))
      throw new ThriftTableOperationException(tableId, null, TableOperation.MERGE, TableOperationExceptionType.OFFLINE, "table is not online");
  }

  public Connector getConnector() throws AccumuloException, AccumuloSecurityException {
    return instance.getConnector(SystemCredentials.get().getPrincipal(), SystemCredentials.get().getToken());
  }

  private Master(ServerConfiguration config, VolumeManager fs, String hostname) throws IOException {
    this.serverConfig = config;
    this.instance = config.getInstance();
    this.fs = fs;
    this.hostname = hostname;

    AccumuloConfiguration aconf = serverConfig.getConfiguration();

    log.info("Version " + Constants.VERSION);
    log.info("Instance " + instance.getInstanceID());
    ThriftTransportPool.getInstance().setIdleTime(aconf.getTimeInMillis(Property.GENERAL_RPC_TIMEOUT));
    security = AuditedSecurityOperation.getInstance();
    tserverSet = new LiveTServerSet(instance, config.getConfiguration(), this);
    this.tabletBalancer = aconf.instantiateClassProperty(Property.MASTER_TABLET_BALANCER, TabletBalancer.class, new DefaultLoadBalancer());
    this.tabletBalancer.init(serverConfig);

    try {
      AccumuloVFSClassLoader.getContextManager().setContextConfig(new ContextManager.DefaultContextsConfig(new Iterable<Entry<String,String>>() {
        @Override
        public Iterator<Entry<String,String>> iterator() {
          return getSystemConfiguration().iterator();
        }
      }));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

  }

  public TServerConnection getConnection(TServerInstance server) {
    return tserverSet.getConnection(server);
  }

  public MergeInfo getMergeInfo(Text tableId) {
    synchronized (mergeLock) {
      try {
        String path = ZooUtil.getRoot(instance.getInstanceID()) + Constants.ZTABLES + "/" + tableId.toString() + "/merge";
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
      String path = ZooUtil.getRoot(instance.getInstanceID()) + Constants.ZTABLES + "/" + info.getExtent().getTableId().toString() + "/merge";
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

  public void clearMergeState(Text tableId) throws IOException, KeeperException, InterruptedException {
    synchronized (mergeLock) {
      String path = ZooUtil.getRoot(instance.getInstanceID()) + Constants.ZTABLES + "/" + tableId.toString() + "/merge";
      ZooReaderWriter.getInstance().recursiveDelete(path, NodeMissingPolicy.SKIP);
      mergeLock.notifyAll();
    }
    nextEvent.event("Merge state of %s cleared", tableId);
  }

  void setMasterGoalState(MasterGoalState state) {
    try {
      ZooReaderWriter.getInstance().putPersistentData(ZooUtil.getRoot(instance) + Constants.ZMASTER_GOAL_STATE, state.name().getBytes(),
          NodeExistsPolicy.OVERWRITE);
    } catch (Exception ex) {
      log.error("Unable to set master goal state in zookeeper");
    }
  }

  MasterGoalState getMasterGoalState() {
    while (true)
      try {
        byte[] data = ZooReaderWriter.getInstance().getData(ZooUtil.getRoot(instance) + Constants.ZMASTER_GOAL_STATE, null);
        return MasterGoalState.valueOf(new String(data));
      } catch (Exception e) {
        log.error("Problem getting real goal state from zookeeper: " + e);
        UtilWaitThread.sleep(1000);
      }
  }

  public boolean hasCycled(long time) {
    for (TabletGroupWatcher watcher : watchers) {
      if (watcher.stats.lastScanFinished() < time)
        return false;
    }

    return true;
  }

  public void clearMigrations(String tableId) {
    synchronized (migrations) {
      Iterator<KeyExtent> iterator = migrations.keySet().iterator();
      while (iterator.hasNext()) {
        KeyExtent extent = iterator.next();
        if (extent.getTableId().toString().equals(tableId)) {
          iterator.remove();
        }
      }
    }
  }

  static enum TabletGoalState {
    HOSTED, UNASSIGNED, DELETED
  };

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
    TableState tableState = TableManager.getInstance().getTableState(extent.getTableId().toString());
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
        return TabletGoalState.UNASSIGNED;
      }
      // Handle merge transitions
      if (mergeInfo.getExtent() != null) {
        log.debug("mergeInfo overlaps: " + extent + " " + mergeInfo.overlaps(extent));
        if (mergeInfo.overlaps(extent)) {
          switch (mergeInfo.getState()) {
            case NONE:
            case COMPLETE:
              break;
            case STARTED:
            case SPLITTING:
              return TabletGoalState.HOSTED;
            case WAITING_FOR_CHOPPED:
              if (tls.getState(onlineTabletServers()).equals(TabletState.HOSTED)) {
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
        UtilWaitThread.sleep(TIME_BETWEEN_MIGRATION_CLEANUPS);
      }
    }

    /**
     * If a migrating tablet splits, and the tablet dies before sending the master a message, the migration will refer to a non-existing tablet, so it can never
     * complete. Periodically scan the metadata table and remove any migrating tablets that no longer exist.
     */
    private void cleanupNonexistentMigrations(final Connector connector) throws AccumuloException, AccumuloSecurityException, TableNotFoundException {
      Scanner scanner = connector.createScanner(MetadataTable.NAME, Authorizations.EMPTY);
      TabletsSection.TabletColumnFamily.PREV_ROW_COLUMN.fetch(scanner);
      Set<KeyExtent> found = new HashSet<KeyExtent>();
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
      for (String tableId : Tables.getIdToNameMap(instance).keySet()) {
        TableState state = manager.getTableState(tableId);
        if (TableState.OFFLINE == state) {
          clearMigrations(tableId);
        }
      }
    }
  }

  private class StatusThread extends Daemon {

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
                  if (count == 0)
                    setMasterState(MasterState.UNLOAD_METADATA_TABLETS);
                }
                  break;
                case UNLOAD_METADATA_TABLETS: {
                  int count = assignedOrHosted(METADATA_TABLE_ID);
                  log.debug(String.format("There are %d metadata tablets assigned or hosted", count));
                  if (count == 0)
                    setMasterState(MasterState.UNLOAD_ROOT_TABLET);
                }
                  break;
                case UNLOAD_ROOT_TABLET: {
                  int count = assignedOrHosted(METADATA_TABLE_ID);
                  if (count > 0) {
                    log.debug(String.format("%d metadata tablets online", count));
                    setMasterState(MasterState.UNLOAD_ROOT_TABLET);
                  }
                  int root_count = assignedOrHosted(ROOT_TABLE_ID);
                  if (root_count > 0)
                    log.debug("The root tablet is still assigned or hosted");
                  if (count + root_count == 0) {
                    Set<TServerInstance> currentServers = tserverSet.getCurrentServers();
                    log.debug("stopping " + currentServers.size() + " tablet servers");
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
        }catch(Throwable t) {
          log.error("Error occurred reading / switching master goal state. Will continue with attempt to update status", t);
        }

        try {
          wait = updateStatus();
          eventListener.waitForEvents(wait);
        } catch (Throwable t) {
          log.error("Error balancing tablets, will wait for " + WAIT_BETWEEN_ERRORS / ONE_SECOND + " (seconds) and then retry", t);
          UtilWaitThread.sleep(WAIT_BETWEEN_ERRORS);
        }
      }
    }

    private long updateStatus() throws AccumuloException, AccumuloSecurityException, TableNotFoundException {
      tserverStatus = Collections.synchronizedSortedMap(gatherTableInformation());
      checkForHeldServer(tserverStatus);

      if (!badServers.isEmpty()) {
        log.debug("not balancing because the balance information is out-of-date " + badServers.keySet());
      } else if (notHosted() > 0) {
        log.debug("not balancing because there are unhosted tablets: " + notHosted());
      } else if (getMasterGoalState() == MasterGoalState.CLEAN_STOP) {
        log.debug("not balancing because the master is attempting to stop cleanly");
      } else if (!serversToShutdown.isEmpty()) {
        log.debug("not balancing while shutting down servers " + serversToShutdown);
      } else {
        return balanceTablets();
      }
      return DEFAULT_WAIT_FOR_WATCHER;
    }

    private void checkForHeldServer(SortedMap<TServerInstance,TabletServerStatus> tserverStatus) {
      TServerInstance instance = null;
      int crazyHoldTime = 0;
      int someHoldTime = 0;
      final long maxWait = getSystemConfiguration().getTimeInMillis(Property.TSERV_HOLD_TIME_SUICIDE);
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
        log.warn("Tablet server " + instance + " exceeded maximum hold time: attempting to kill it");
        try {
          TServerConnection connection = tserverSet.getConnection(instance);
          if (connection != null)
            connection.fastHalt(masterLock);
        } catch (TException e) {
          log.error(e, e);
        }
        tserverSet.remove(instance);
      }
    }

    private long balanceTablets() {
      List<TabletMigration> migrationsOut = new ArrayList<TabletMigration>();
      Set<KeyExtent> migrationsCopy = new HashSet<KeyExtent>();
      synchronized (migrations) {
        migrationsCopy.addAll(migrations.keySet());
      }
      long wait = tabletBalancer.balance(Collections.unmodifiableSortedMap(tserverStatus), Collections.unmodifiableSet(migrationsCopy), migrationsOut);

      for (TabletMigration m : TabletBalancer.checkMigrationSanity(tserverStatus.keySet(), migrationsOut)) {
        if (migrations.containsKey(m.tablet)) {
          log.warn("balancer requested migration more than once, skipping " + m);
          continue;
        }
        migrations.put(m.tablet, m.newServer);
        log.debug("migration " + m);
      }
      if (migrationsOut.size() > 0) {
        nextEvent.event("Migrating %d more tablets, %d total", migrationsOut.size(), migrations.size());
      }
      return wait;
    }

  }

  private SortedMap<TServerInstance,TabletServerStatus> gatherTableInformation() {
    long start = System.currentTimeMillis();
    SortedMap<TServerInstance,TabletServerStatus> result = new TreeMap<TServerInstance,TabletServerStatus>();
    Set<TServerInstance> currentServers = tserverSet.getCurrentServers();
    for (TServerInstance server : currentServers) {
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
        log.error("unable to get tablet server status " + server + " " + ex.toString());
        log.debug("unable to get tablet server status " + server, ex);
        if (badServers.get(server).incrementAndGet() > MAX_BAD_STATUS_COUNT) {
          log.warn("attempting to stop " + server);
          try {
            TServerConnection connection = tserverSet.getConnection(server);
            if (connection != null)
              connection.halt(masterLock);
          } catch (TTransportException e) {
            // ignore: it's probably down
          } catch (Exception e) {
            log.info("error talking to troublesome tablet server ", e);
          }
          badServers.remove(server);
          tserverSet.remove(server);
        }
      }
    }
    synchronized (badServers) {
      badServers.keySet().retainAll(currentServers);
      badServers.keySet().removeAll(result.keySet());
    }
    log.debug(String.format("Finished gathering information from %d servers in %.2f seconds", result.size(), (System.currentTimeMillis() - start) / 1000.));
    return result;
  }

  public void run() throws IOException, InterruptedException, KeeperException {
    final String zroot = ZooUtil.getRoot(instance);

    getMasterLock(zroot + Constants.ZMASTER_LOCK);

    recoveryManager = new RecoveryManager(this);

    TableManager.getInstance().addObserver(this);

    StatusThread statusThread = new StatusThread();
    statusThread.start();

    MigrationCleanupThread migrationCleanupThread = new MigrationCleanupThread();
    migrationCleanupThread.start();

    tserverSet.startListeningForTabletServerChanges();

    ZooReaderWriter.getInstance().getChildren(zroot + Constants.ZRECOVERY, new Watcher() {
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

    Credentials systemCreds = SystemCredentials.get();
    watchers.add(new TabletGroupWatcher(this, new MetaDataStateStore(instance, systemCreds, this), null));
    watchers.add(new TabletGroupWatcher(this, new RootTabletStateStore(instance, systemCreds, this), watchers.get(0)));
    watchers.add(new TabletGroupWatcher(this, new ZooTabletStateStore(new ZooStore(zroot)), watchers.get(1)));
    for (TabletGroupWatcher watcher : watchers) {
      watcher.start();
    }

    // Once we are sure the upgrade is complete, we can safely allow fate use.
    waitForMetadataUpgrade.await();

    try {
      final AgeOffStore<Master> store = new AgeOffStore<Master>(new org.apache.accumulo.fate.ZooStore<Master>(ZooUtil.getRoot(instance) + Constants.ZFATE,
          ZooReaderWriter.getInstance()), 1000 * 60 * 60 * 8);

      int threads = this.getConfiguration().getConfiguration().getCount(Property.MASTER_FATE_THREADPOOL_SIZE);

      fate = new Fate<Master>(this, store, threads);

      SimpleTimer.getInstance().schedule(new Runnable() {

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

    Processor<Iface> processor = new Processor<Iface>(RpcWrapper.service(new MasterClientServiceHandler(this)));
    ServerAddress sa = TServerUtils.startServer(getSystemConfiguration(), hostname, Property.MASTER_CLIENTPORT, processor, "Master",
        "Master Client Service Handler", null, Property.MASTER_MINTHREADS, Property.MASTER_THREADCHECK, Property.GENERAL_MAX_MESSAGE_SIZE);
    clientService = sa.server;
    String address = sa.address.toString();
    log.info("Setting master lock data to " + address);
    masterLock.replaceLockData(address.getBytes());

    while (!clientService.isServing()) {
      UtilWaitThread.sleep(100);
    }
    while (clientService.isServing()) {
      UtilWaitThread.sleep(500);
    }
    log.info("Shutting down fate.");
    fate.shutdown();

    final long deadline = System.currentTimeMillis() + MAX_CLEANUP_WAIT_TIME;
    statusThread.join(remaining(deadline));

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
      Halt.halt(-1, new Runnable() {
        @Override
        public void run() {
          log.fatal("No longer able to monitor master lock node", e);
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
      log.warn("Failed to get master lock " + e);

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

    final String masterClientAddress = hostname + ":" + getSystemConfiguration().getPort(Property.MASTER_CLIENTPORT);

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

      UtilWaitThread.sleep(TIME_TO_WAIT_BETWEEN_LOCK_CHECKS);
    }

    setMasterState(MasterState.HAVE_LOCK);
  }

  public static void main(String[] args) throws Exception {
    try {
      SecurityUtil.serverLogin(ServerConfiguration.getSiteConfiguration());

      ServerOpts opts = new ServerOpts();
      final String app = "master";
      opts.parseArgs(app, args);
      String hostname = opts.getAddress();
      Accumulo.setupLogging(app);
      Instance instance = HdfsZooInstance.getInstance();
      ServerConfiguration conf = new ServerConfiguration(instance);
      VolumeManager fs = VolumeManagerImpl.get();
      Accumulo.init(fs, conf, app);
      Master master = new Master(conf, fs, hostname);
      Accumulo.enableTracing(hostname, app);
      master.run();
    } catch (Exception ex) {
      log.error("Unexpected exception, exiting", ex);
      System.exit(1);
    }
  }

  @Override
  public void update(LiveTServerSet current, Set<TServerInstance> deleted, Set<TServerInstance> added) {
    DeadServerList obit = new DeadServerList(ZooUtil.getRoot(instance) + Constants.ZDEADTSERVERS);
    if (added.size() > 0) {
      log.info("New servers: " + added);
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

    Set<TServerInstance> unexpected = new HashSet<TServerInstance>(deleted);
    unexpected.removeAll(this.serversToShutdown);
    if (unexpected.size() > 0) {
      if (stillMaster() && !getMasterGoalState().equals(MasterGoalState.CLEAN_STOP)) {
        log.warn("Lost servers " + unexpected);
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
          log.info("Canceling migration of " + entry.getKey() + " to " + entry.getValue());
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
  public void stateChanged(String tableId, TableState state) {
    nextEvent.event("Table state in zookeeper changed for %s to %s", tableId, state);
    if (TableState.OFFLINE == state) {
      clearMigrations(tableId);
    }
  }

  @Override
  public void initialize(Map<String,TableState> tableIdToStateMap) {}

  @Override
  public void sessionExpired() {}

  @Override
  public Set<String> onlineTables() {
    Set<String> result = new HashSet<String>();
    if (getMasterState() != MasterState.NORMAL) {
      if (getMasterState() != MasterState.UNLOAD_METADATA_TABLETS)
        result.add(MetadataTable.ID);
      if (getMasterState() != MasterState.UNLOAD_ROOT_TABLET)
        result.add(RootTable.ID);
      return result;
    }
    TableManager manager = TableManager.getInstance();

    for (String tableId : Tables.getIdToNameMap(instance).keySet()) {
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
    List<MergeInfo> result = new ArrayList<MergeInfo>();
    for (String tableId : Tables.getIdToNameMap(instance).keySet()) {
      result.add(getMergeInfo(new Text(tableId)));
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

  public Instance getInstance() {
    return this.instance;
  }

  public AccumuloConfiguration getSystemConfiguration() {
    return serverConfig.getConfiguration();
  }

  public ServerConfiguration getConfiguration() {
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
}

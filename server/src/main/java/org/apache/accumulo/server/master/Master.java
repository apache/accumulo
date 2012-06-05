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
package org.apache.accumulo.server.master;

import static java.lang.Math.min;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
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
import java.util.TimerTask;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.accumulo.cloudtrace.instrument.thrift.TraceWrap;
import org.apache.accumulo.cloudtrace.thrift.TInfo;
import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.IsolatedScanner;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.RowIterator;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.impl.Tables;
import org.apache.accumulo.core.client.impl.ThriftTransportPool;
import org.apache.accumulo.core.client.impl.thrift.TableOperation;
import org.apache.accumulo.core.client.impl.thrift.TableOperationExceptionType;
import org.apache.accumulo.core.client.impl.thrift.ThriftTableOperationException;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.KeyExtent;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.data.thrift.TKeyExtent;
import org.apache.accumulo.core.file.FileUtil;
import org.apache.accumulo.core.iterators.IteratorUtil;
import org.apache.accumulo.core.master.state.tables.TableState;
import org.apache.accumulo.core.master.thrift.MasterClientService;
import org.apache.accumulo.core.master.thrift.MasterClientService.Processor;
import org.apache.accumulo.core.master.thrift.MasterGoalState;
import org.apache.accumulo.core.master.thrift.MasterMonitorInfo;
import org.apache.accumulo.core.master.thrift.MasterState;
import org.apache.accumulo.core.master.thrift.TableInfo;
import org.apache.accumulo.core.master.thrift.TabletLoadState;
import org.apache.accumulo.core.master.thrift.TabletServerStatus;
import org.apache.accumulo.core.master.thrift.TabletSplit;
import org.apache.accumulo.core.security.SystemPermission;
import org.apache.accumulo.core.security.TablePermission;
import org.apache.accumulo.core.security.thrift.AuthInfo;
import org.apache.accumulo.core.security.thrift.SecurityErrorCode;
import org.apache.accumulo.core.security.thrift.ThriftSecurityException;
import org.apache.accumulo.core.util.ByteBufferUtil;
import org.apache.accumulo.core.util.CachedConfiguration;
import org.apache.accumulo.core.util.ColumnFQ;
import org.apache.accumulo.core.util.Daemon;
import org.apache.accumulo.core.util.UtilWaitThread;
import org.apache.accumulo.core.zookeeper.ZooUtil;
import org.apache.accumulo.core.zookeeper.ZooUtil.NodeExistsPolicy;
import org.apache.accumulo.core.zookeeper.ZooUtil.NodeMissingPolicy;
import org.apache.accumulo.server.Accumulo;
import org.apache.accumulo.server.client.HdfsZooInstance;
import org.apache.accumulo.server.conf.ServerConfiguration;
import org.apache.accumulo.server.fate.Fate;
import org.apache.accumulo.server.fate.TStore.TStatus;
import org.apache.accumulo.server.iterators.MetadataBulkLoadFilter;
import org.apache.accumulo.server.master.LiveTServerSet.TServerConnection;
import org.apache.accumulo.server.master.balancer.DefaultLoadBalancer;
import org.apache.accumulo.server.master.balancer.TabletBalancer;
import org.apache.accumulo.server.master.recovery.RecoverLease;
import org.apache.accumulo.server.master.state.Assignment;
import org.apache.accumulo.server.master.state.CurrentState;
import org.apache.accumulo.server.master.state.DeadServerList;
import org.apache.accumulo.server.master.state.DistributedStoreException;
import org.apache.accumulo.server.master.state.MergeInfo;
import org.apache.accumulo.server.master.state.MergeState;
import org.apache.accumulo.server.master.state.MergeStats;
import org.apache.accumulo.server.master.state.MetaDataStateStore;
import org.apache.accumulo.server.master.state.RootTabletStateStore;
import org.apache.accumulo.server.master.state.TServerInstance;
import org.apache.accumulo.server.master.state.TableCounts;
import org.apache.accumulo.server.master.state.TableStats;
import org.apache.accumulo.server.master.state.TabletLocationState;
import org.apache.accumulo.server.master.state.TabletMigration;
import org.apache.accumulo.server.master.state.TabletServerState;
import org.apache.accumulo.server.master.state.TabletState;
import org.apache.accumulo.server.master.state.TabletStateStore;
import org.apache.accumulo.server.master.state.ZooStore;
import org.apache.accumulo.server.master.state.ZooTabletStateStore;
import org.apache.accumulo.server.master.state.tables.TableManager;
import org.apache.accumulo.server.master.state.tables.TableObserver;
import org.apache.accumulo.server.master.tableOps.BulkImport;
import org.apache.accumulo.server.master.tableOps.ChangeTableState;
import org.apache.accumulo.server.master.tableOps.CloneTable;
import org.apache.accumulo.server.master.tableOps.CompactRange;
import org.apache.accumulo.server.master.tableOps.CreateTable;
import org.apache.accumulo.server.master.tableOps.DeleteTable;
import org.apache.accumulo.server.master.tableOps.RenameTable;
import org.apache.accumulo.server.master.tableOps.TableRangeOp;
import org.apache.accumulo.server.master.tableOps.TraceRepo;
import org.apache.accumulo.server.master.tserverOps.ShutdownTServer;
import org.apache.accumulo.server.monitor.Monitor;
import org.apache.accumulo.server.security.Authenticator;
import org.apache.accumulo.server.security.SecurityConstants;
import org.apache.accumulo.server.security.SecurityUtil;
import org.apache.accumulo.server.security.ZKAuthenticator;
import org.apache.accumulo.server.tabletserver.TabletTime;
import org.apache.accumulo.server.trace.TraceFileSystem;
import org.apache.accumulo.server.util.AddressUtil;
import org.apache.accumulo.server.util.DefaultMap;
import org.apache.accumulo.server.util.Halt;
import org.apache.accumulo.server.util.MetadataTable;
import org.apache.accumulo.server.util.SystemPropUtil;
import org.apache.accumulo.server.util.TServerUtils;
import org.apache.accumulo.server.util.TablePropUtil;
import org.apache.accumulo.server.util.TabletIterator.TabletDeletedException;
import org.apache.accumulo.server.util.time.SimpleTimer;
import org.apache.accumulo.server.zookeeper.IZooReaderWriter;
import org.apache.accumulo.server.zookeeper.ZooLock;
import org.apache.accumulo.server.zookeeper.ZooLock.LockLossReason;
import org.apache.accumulo.server.zookeeper.ZooLock.LockWatcher;
import org.apache.accumulo.server.zookeeper.ZooReaderWriter;
import org.apache.accumulo.server.zookeeper.ZooReaderWriter.Mutator;
import org.apache.accumulo.start.classloader.AccumuloClassLoader;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;
import org.apache.thrift.TException;
import org.apache.thrift.server.TServer;
import org.apache.thrift.transport.TTransportException;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;


/**
 * The Master is responsible for assigning and balancing tablets to tablet servers.
 * 
 * The master will also coordinate log recoveries and reports general status.
 */
public class Master implements LiveTServerSet.Listener, TableObserver, CurrentState {
  
  final private static Logger log = Logger.getLogger(Master.class);
  
  final private static int ONE_SECOND = 1000;
  final private static Text METADATA_TABLE_ID = new Text(Constants.METADATA_TABLE_ID);
  final private static long TIME_TO_WAIT_BETWEEN_SCANS = 60 * ONE_SECOND;
  final private static long TIME_BETWEEN_MIGRATION_CLEANUPS = 5 * 60 * ONE_SECOND;
  final private static long WAIT_BETWEEN_ERRORS = ONE_SECOND;
  final private static long DEFAULT_WAIT_FOR_WATCHER = 10 * ONE_SECOND;
  final private static int MAX_CLEANUP_WAIT_TIME = 1000;
  final private static int TIME_TO_WAIT_BETWEEN_LOCK_CHECKS = 1000;
  final private static int MAX_TSERVER_WORK_CHUNK = 5000;
  final private static int MAX_BAD_STATUS_COUNT = 3;
  
  final private FileSystem fs;
  final private Instance instance;
  final private String hostname;
  final private LiveTServerSet tserverSet;
  final private List<TabletGroupWatcher> watchers = new ArrayList<TabletGroupWatcher>();
  final private Authenticator authenticator;
  final private Map<TServerInstance,AtomicInteger> badServers = Collections.synchronizedMap(new DefaultMap<TServerInstance,AtomicInteger>(new AtomicInteger()));
  final private Set<TServerInstance> serversToShutdown = Collections.synchronizedSet(new HashSet<TServerInstance>());
  final private SortedMap<KeyExtent,TServerInstance> migrations = Collections.synchronizedSortedMap(new TreeMap<KeyExtent,TServerInstance>());
  final private EventCoordinator nextEvent = new EventCoordinator();
  final private Object mergeLock = new Object();
  
  private ZooLock masterLock = null;
  private TServer clientService = null;
  private TabletBalancer tabletBalancer;
  
  private MasterState state = MasterState.INITIAL;
  
  private Fate<Master> fate;
  
  volatile private SortedMap<TServerInstance,TabletServerStatus> tserverStatus = Collections
      .unmodifiableSortedMap(new TreeMap<TServerInstance,TabletServerStatus>());
  
  private Set<String> recoveriesInProgress = Collections.synchronizedSet(new HashSet<String>());

  synchronized private MasterState getMasterState() {
    return state;
  }
  
  public boolean stillMaster() {
    return getMasterState() != MasterState.STOP;
  }
  
  static final boolean X = true;
  static final boolean _ = false;
  static final boolean transitionOK[][] = {
      //                            INITIAL HAVE_LOCK SAFE_MODE NORMAL UNLOAD_META UNLOAD_ROOT STOP
      /* INITIAL                 */{X,      X,        _,        _,     _,          _,          X},
      /* HAVE_LOCK               */{_,      X,        X,        X,     _,          _,          X},
      /* SAFE_MODE               */{_,      _,        X,        X,     X,          _,          X},
      /* NORMAL                  */{_,      _,        X,        X,     X,          _,          X},
      /* UNLOAD_METADATA_TABLETS */{_,      _,        X,        X,     X,          X,          X},
      /* UNLOAD_ROOT_TABLET      */{_,      _,        _,        _,     _,          X,          X},
      /* STOP                    */{_,      _,        _,        _,     _,          _,          X},
      };
  
  synchronized private void setMasterState(MasterState newState) {
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
      SimpleTimer.getInstance().schedule(new TimerTask() {
        @Override
        public void run() {
          // This frees the main thread and will cause the master to exit
          clientService.stop();
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
  

  private void upgradeZookeeper() {
    if (Accumulo.getAccumuloPersistentVersion(fs) == Constants.PREV_DATA_VERSION) {
      try {
        log.info("Upgrading zookeeper");

        IZooReaderWriter zoo = ZooReaderWriter.getInstance();
        
        TablePropUtil.setTableProperty(Constants.METADATA_TABLE_ID, Property.TABLE_ITERATOR_PREFIX.getKey() + "majc.bulkLoadFilter", "20,"
            + MetadataBulkLoadFilter.class.getName());
        
        zoo.putPersistentData(ZooUtil.getRoot(instance) + Constants.ZTABLE_LOCKS, new byte[0], NodeExistsPolicy.SKIP);
        zoo.putPersistentData(ZooUtil.getRoot(instance) + Constants.ZHDFS_RESERVATIONS, new byte[0], NodeExistsPolicy.SKIP);
        zoo.putPersistentData(ZooUtil.getRoot(instance) + Constants.ZNEXT_FILE, new byte[] {'0'}, NodeExistsPolicy.SKIP);

        String[] tablePropsToDelete = new String[] {"table.scan.cache.size", "table.scan.cache.enable"};

        for (String id : Tables.getIdToNameMap(instance).keySet()) {
          zoo.putPersistentData(ZooUtil.getRoot(instance) + Constants.ZTABLES + "/" + id + Constants.ZTABLE_FLUSH_ID, "0".getBytes(), NodeExistsPolicy.SKIP);
          zoo.putPersistentData(ZooUtil.getRoot(instance) + Constants.ZTABLES + "/" + id + Constants.ZTABLE_COMPACT_ID, "0".getBytes(), NodeExistsPolicy.SKIP);
          
          for (String prop : tablePropsToDelete) {
            String propPath = ZooUtil.getRoot(instance) + Constants.ZTABLES + "/" + id + Constants.ZTABLE_CONF + "/" + prop;
            if (zoo.exists(propPath))
              zoo.delete(propPath, -1);
          }
        }
        
        setACLs(zoo, ZooUtil.getRoot(instance), ZooUtil.getRoot(instance) + Constants.ZUSERS);

      } catch (Exception ex) {
        log.fatal("Error performing upgrade", ex);
        System.exit(1);
      }
    }
  }
  
  private void setACLs(IZooReaderWriter zoo, String root, String users) throws Exception {
    Stat stat = new Stat();
    List<ACL> acls = zoo.getZooKeeper().getACL(root, stat);
    if (acls.equals(ZooDefs.Ids.OPEN_ACL_UNSAFE)) {
      if (root.startsWith(users)) {
        zoo.getZooKeeper().setACL(root, ZooUtil.PRIVATE, -1);
      } else {
        zoo.getZooKeeper().setACL(root, ZooUtil.PUBLIC, -1);
      }
      for (String child : zoo.getChildren(root)) {
        setACLs(zoo, root + "/" + child, users);
      }
    }
  }

  private AtomicBoolean upgradeMetadataRunning = new AtomicBoolean(false);

  private ServerConfiguration serverConfig;

  private void upgradeMetadata() {
    if (Accumulo.getAccumuloPersistentVersion(fs) == Constants.PREV_DATA_VERSION) {
      if (upgradeMetadataRunning.compareAndSet(false, true)) {
        Runnable upgradeTask = new Runnable() {
          @Override
          public void run() {
            try {
              // add delete entries to metadata table for bulk dirs
              
              log.info("Adding bulk dir delete entries to !METADATA table for upgrade");

              BatchWriter bw = getConnector().createBatchWriter(Constants.METADATA_TABLE_NAME, 10000000, 60000l, 4);
              
              FileStatus[] tables = fs.globStatus(new Path(Constants.getTablesDir(getSystemConfiguration()) + "/*"));
              for (FileStatus tableDir : tables) {
                FileStatus[] bulkDirs = fs.globStatus(new Path(tableDir.getPath() + "/bulk_*"));
                for (FileStatus bulkDir : bulkDirs) {
                  bw.addMutation(MetadataTable.createDeleteMutation(tableDir.getPath().getName(), "/" + bulkDir.getPath().getName()));
                }
              }
              
              bw.close();
              
              Accumulo.updateAccumuloVersion(fs);
              
              log.info("Upgrade complete");

            } catch (Exception ex) {
              log.fatal("Error performing upgrade", ex);
              System.exit(1);
            }
            
          }
        };
        
        // need to run this in a separate thread because a lock is held that prevents !METADATA tablets from being assigned and this task writes to the
        // !METADATA table
        new Thread(upgradeTask).start();
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
    return totalAssignedOrHosted() - assignedOrHosted(new Text(Constants.METADATA_TABLE_ID));
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
  private int displayUnassigned() {
    int result = 0;
    Text meta = new Text(Constants.METADATA_TABLE_ID);
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
        // Count offline tablets for the METADATA table
        for (TabletGroupWatcher watcher : watchers) {
          result += watcher.getStats(meta).unassigned();
        }
        break;
      case UNLOAD_METADATA_TABLETS:
      case UNLOAD_ROOT_TABLET:
        for (TabletGroupWatcher watcher : watchers) {
          result += watcher.getStats(meta).unassigned();
        }
        break;
      default:
        break;
    }
    return result;
  }
  
  private void checkNotMetadataTable(String tableName, TableOperation operation) throws ThriftTableOperationException {
    if (tableName.compareTo(Constants.METADATA_TABLE_NAME) == 0) {
      String why = "Table names cannot be == " + Constants.METADATA_TABLE_NAME;
      log.warn(why);
      throw new ThriftTableOperationException(null, tableName, operation, TableOperationExceptionType.OTHER, why);
    }
  }
  
  private void checkTableName(String tableName, TableOperation operation) throws ThriftTableOperationException {
    if (!tableName.matches(Constants.VALID_TABLE_NAME_REGEX)) {
      String why = "Table names must only contain word characters (letters, digits, and underscores): " + tableName;
      log.warn(why);
      throw new ThriftTableOperationException(null, tableName, operation, TableOperationExceptionType.OTHER, why);
    }
  }
  
  private void verify(AuthInfo credentials, String tableId, TableOperation op, boolean match) throws ThriftSecurityException, ThriftTableOperationException {
    if (!match) {
      Tables.clearCache(instance);
      if (!Tables.exists(instance, tableId))
        throw new ThriftTableOperationException(tableId, null, op, TableOperationExceptionType.NOTFOUND, null);
      else
        throw new AccumuloSecurityException(credentials.user, SecurityErrorCode.PERMISSION_DENIED).asThriftException();
    }
  }
  
  private void verify(AuthInfo credentials, boolean match) throws ThriftSecurityException {
    if (!match)
      throw new AccumuloSecurityException(credentials.user, SecurityErrorCode.PERMISSION_DENIED).asThriftException();
  }
  
  private boolean check(AuthInfo credentials, SystemPermission permission) throws ThriftSecurityException {
    try {
      // clear the cache so the check is done using current info
      authenticator.clearCache(credentials.user);
      return authenticator.hasSystemPermission(credentials, credentials.user, permission);
    } catch (AccumuloSecurityException e) {
      throw e.asThriftException();
    }
  }
  
  private boolean check(AuthInfo credentials, String tableId, TablePermission permission) throws ThriftSecurityException {
    try {
      // clear the cache so the check is done using current info
      authenticator.clearCache(credentials.user, tableId);
      return authenticator.hasTablePermission(credentials, credentials.user, tableId, permission);
    } catch (AccumuloSecurityException e) {
      throw e.asThriftException();
    }
  }
  
  public void mustBeOnline(final String tableId) throws ThriftTableOperationException {
    Tables.clearCache(instance);
    if (!Tables.getTableState(instance, tableId).equals(TableState.ONLINE))
      throw new ThriftTableOperationException(tableId, null, TableOperation.MERGE, TableOperationExceptionType.OFFLINE, "table is not online");
  }
  
  Connector getConnector() throws AccumuloException, AccumuloSecurityException {
    return instance.getConnector(SecurityConstants.getSystemCredentials());
  }
  
  private void waitAround(EventCoordinator.Listener listener) {
    listener.waitForEvents(ONE_SECOND);
  }
  
  // @TODO: maybe move this to Property? We do this in TabletServer, Master, TableLoadBalancer, etc.
  public static <T> T createInstanceFromPropertyName(AccumuloConfiguration conf, Property property, Class<T> base, T defaultInstance) {
    String clazzName = conf.get(property);
    T instance = null;
    
    try {
      Class<? extends T> clazz = AccumuloClassLoader.loadClass(clazzName, base);
      instance = clazz.newInstance();
      log.info("Loaded class : " + clazzName);
    } catch (Exception e) {
      log.warn("Failed to load class ", e);
    }
    
    if (instance == null) {
      log.info("Using " + defaultInstance.getClass().getName());
      instance = defaultInstance;
    }
    return instance;
  }
  
  public Master(ServerConfiguration config, FileSystem fs, String hostname) throws IOException {
    this.serverConfig = config;
    this.instance = config.getInstance();
    this.fs = TraceFileSystem.wrap(fs);
    this.hostname = hostname;
    
    AccumuloConfiguration aconf = serverConfig.getConfiguration();

    log.info("Version " + Constants.VERSION);
    log.info("Instance " + instance.getInstanceID());
    ThriftTransportPool.getInstance().setIdleTime(aconf.getTimeInMillis(Property.GENERAL_RPC_TIMEOUT));
    authenticator = ZKAuthenticator.getInstance();
    tserverSet = new LiveTServerSet(instance, config.getConfiguration(), this);
    this.tabletBalancer = createInstanceFromPropertyName(aconf, Property.MASTER_TABLET_BALANCER, TabletBalancer.class, new DefaultLoadBalancer());
    this.tabletBalancer.init(serverConfig);
  }
  
  public TServerConnection getConnection(TServerInstance server) {
    try {
      return tserverSet.getConnection(server);
    } catch (TException ex) {
      return null;
    }
  }
  
  private class MasterClientServiceHandler implements MasterClientService.Iface {
    
    protected String checkTableId(String tableName, TableOperation operation) throws ThriftTableOperationException {
      final String tableId = Tables.getNameToIdMap(HdfsZooInstance.getInstance()).get(tableName);
      if (tableId == null)
        throw new ThriftTableOperationException(null, tableName, operation, TableOperationExceptionType.NOTFOUND, null);
      return tableId;
    }
    
    @Override
    public long initiateFlush(TInfo tinfo, AuthInfo c, String tableId) throws ThriftSecurityException, ThriftTableOperationException, TException {
      verify(c, tableId, TableOperation.FLUSH, check(c, tableId, TablePermission.WRITE) || check(c, tableId, TablePermission.ALTER_TABLE));
      
      String zTablePath = Constants.ZROOT + "/" + HdfsZooInstance.getInstance().getInstanceID() + Constants.ZTABLES + "/" + tableId + Constants.ZTABLE_FLUSH_ID;
      
      IZooReaderWriter zoo = ZooReaderWriter.getInstance();
      byte fid[];
      try {
        fid = zoo.mutate(zTablePath, null, null, new Mutator() {
          @Override
          public byte[] mutate(byte[] currentValue) throws Exception {
            long flushID = Long.parseLong(new String(currentValue));
            flushID++;
            return ("" + flushID).getBytes();
          }
        });
      } catch (NoNodeException nne) {
        throw new ThriftTableOperationException(tableId, null, TableOperation.FLUSH, TableOperationExceptionType.NOTFOUND, null);
      } catch (Exception e) {
        log.warn(e.getMessage(), e);
        throw new ThriftTableOperationException(tableId, null, TableOperation.FLUSH, TableOperationExceptionType.OTHER, null);
      }
      return Long.parseLong(new String(fid));
    }
    
    @Override
    public void waitForFlush(TInfo tinfo, AuthInfo c, String tableId, ByteBuffer startRow, ByteBuffer endRow, long flushID, long maxLoops)
        throws ThriftSecurityException, ThriftTableOperationException, TException {
      verify(c, tableId, TableOperation.FLUSH, check(c, tableId, TablePermission.WRITE) || check(c, tableId, TablePermission.ALTER_TABLE));
      
      if (endRow != null && startRow != null && ByteBufferUtil.toText(startRow).compareTo(ByteBufferUtil.toText(endRow)) >= 0)
        throw new ThriftTableOperationException(tableId, null, TableOperation.FLUSH, TableOperationExceptionType.BAD_RANGE,
            "start row must be less than end row");
      
      Set<TServerInstance> serversToFlush = new HashSet<TServerInstance>(tserverSet.getCurrentServers());
      
      for (long l = 0; l < maxLoops; l++) {
        
        for (TServerInstance instance : serversToFlush) {
          try {
            final TServerConnection server = tserverSet.getConnection(instance);
            if (server != null)
              server.flush(masterLock, tableId, ByteBufferUtil.toBytes(startRow), ByteBufferUtil.toBytes(endRow));
          } catch (TException ex) {
            log.error(ex.toString());
          }
        }
        
        if (l == maxLoops - 1)
          break;
        
        UtilWaitThread.sleep(50);
        
        serversToFlush.clear();
        
        try {
          Connector conn = getConnector();
          Scanner scanner = new IsolatedScanner(conn.createScanner(Constants.METADATA_TABLE_NAME, Constants.NO_AUTHS));
          ColumnFQ.fetch(scanner, Constants.METADATA_FLUSH_COLUMN);
          ColumnFQ.fetch(scanner, Constants.METADATA_DIRECTORY_COLUMN);
          scanner.fetchColumnFamily(Constants.METADATA_CURRENT_LOCATION_COLUMN_FAMILY);
          scanner.fetchColumnFamily(Constants.METADATA_LOG_COLUMN_FAMILY);
          scanner.setRange(new KeyExtent(new Text(tableId), null, ByteBufferUtil.toText(startRow)).toMetadataRange());
          
          // TODO this used to be TabletIterator... any problems with splits/merges?
          RowIterator ri = new RowIterator(scanner);
          
          int tabletsToWaitFor = 0;
          int tabletCount = 0;
          
          Text ert = ByteBufferUtil.toText(endRow);
          
          while (ri.hasNext()) {
            Iterator<Entry<Key,Value>> row = ri.next();
            long tabletFlushID = -1;
            int logs = 0;
            boolean online = false;
            
            TServerInstance server = null;
            
            Entry<Key,Value> entry = null;
            while (row.hasNext()) {
              entry = row.next();
              Key key = entry.getKey();
              
              if (Constants.METADATA_FLUSH_COLUMN.equals(key.getColumnFamily(), key.getColumnQualifier())) {
                tabletFlushID = Long.parseLong(entry.getValue().toString());
              }
              
              if (Constants.METADATA_LOG_COLUMN_FAMILY.equals(key.getColumnFamily()))
                logs++;
              
              if (Constants.METADATA_CURRENT_LOCATION_COLUMN_FAMILY.equals(key.getColumnFamily())) {
                online = true;
                server = new TServerInstance(entry.getValue(), key.getColumnQualifier());
              }
              
            }
            
            // when tablet is not online and has no logs, there is no reason to wait for it
            if ((online || logs > 0) && tabletFlushID < flushID) {
              tabletsToWaitFor++;
              if (server != null)
                serversToFlush.add(server);
            }
            
            tabletCount++;
            
            Text tabletEndRow = new KeyExtent(entry.getKey().getRow(), (Text) null).getEndRow();
            if (tabletEndRow == null || (ert != null && tabletEndRow.compareTo(ert) >= 0))
              break;
          }
          
          if (tabletsToWaitFor == 0)
            break;
          
          // TODO detect case of table offline AND tablets w/ logs?
          
          if (tabletCount == 0 && !Tables.exists(instance, tableId))
            throw new ThriftTableOperationException(tableId, null, TableOperation.FLUSH, TableOperationExceptionType.NOTFOUND, null);
          
        } catch (AccumuloException e) {
          log.debug("Failed to scan !METADATA table to wait for flush " + tableId, e);
        } catch (TabletDeletedException tde) {
          log.debug("Failed to scan !METADATA table to wait for flush " + tableId, tde);
        } catch (AccumuloSecurityException e) {
          log.warn(e.getMessage(), e);
          throw new ThriftSecurityException();
        } catch (TableNotFoundException e) {
          log.error(e.getMessage(), e);
          throw new ThriftTableOperationException();
        }
      }
      
    }
    
    @Override
    public MasterMonitorInfo getMasterStats(TInfo info, AuthInfo credentials) throws ThriftSecurityException, TException {
      final MasterMonitorInfo result = new MasterMonitorInfo();
      
      result.tServerInfo = new ArrayList<TabletServerStatus>();
      result.tableMap = new DefaultMap<String,TableInfo>(new TableInfo());
      for (Entry<TServerInstance,TabletServerStatus> serverEntry : tserverStatus.entrySet()) {
        final TabletServerStatus status = serverEntry.getValue();
        result.tServerInfo.add(status);
        for (Entry<String,TableInfo> entry : status.tableMap.entrySet()) {
          String table = entry.getKey();
          TableInfo summary = result.tableMap.get(table);
          Monitor.add(summary, entry.getValue());
        }
      }
      result.badTServers = new HashMap<String,Byte>();
      synchronized (badServers) {
        for (TServerInstance bad : badServers.keySet()) {
          result.badTServers.put(bad.hostPort(), TabletServerState.UNRESPONSIVE.getId());
        }
      }
      result.state = getMasterState();
      result.goalState = getMasterGoalState();
      result.unassignedTablets = Master.this.displayUnassigned();
      result.serversShuttingDown = new HashSet<String>();
      synchronized (serversToShutdown) {
        for (TServerInstance server : serversToShutdown)
          result.serversShuttingDown.add(server.hostPort());
      }
      DeadServerList obit = new DeadServerList(ZooUtil.getRoot(instance) + Constants.ZDEADTSERVERS);
      result.deadTabletServers = obit.getList();
      return result;
    }
    
    private void alterTableProperty(AuthInfo c, String tableName, String property, String value, TableOperation op) throws ThriftSecurityException,
        ThriftTableOperationException {
      final String tableId = checkTableId(tableName, op);
      verify(c, tableId, op, check(c, SystemPermission.ALTER_TABLE) || check(c, tableId, TablePermission.ALTER_TABLE));
      
      try {
        if (value == null) {
          TablePropUtil.removeTableProperty(tableId, property);
        } else if (!TablePropUtil.setTableProperty(tableId, property, value)) {
          throw new Exception("Invalid table property.");
        }
      } catch (Exception e) {
        log.error("Problem altering table property", e);
        throw new ThriftTableOperationException(tableId, tableName, op, TableOperationExceptionType.OTHER, e.getMessage());
      }
    }
    
    @Override
    public void removeTableProperty(TInfo info, AuthInfo credentials, String tableName, String property) throws ThriftSecurityException,
        ThriftTableOperationException, TException {
      alterTableProperty(credentials, tableName, property, null, TableOperation.REMOVE_PROPERTY);
    }
    
    @Override
    public void setTableProperty(TInfo info, AuthInfo credentials, String tableName, String property, String value) throws ThriftSecurityException,
        ThriftTableOperationException, TException {
      alterTableProperty(credentials, tableName, property, value, TableOperation.SET_PROPERTY);
    }
    
    @Override
    public void shutdown(TInfo info, AuthInfo c, boolean stopTabletServers) throws ThriftSecurityException, TException {
      verify(c, check(c, SystemPermission.SYSTEM));
      Master.this.shutdown(stopTabletServers);
    }
    
    @Override
    public void shutdownTabletServer(TInfo info, AuthInfo c, String tabletServer, boolean force) throws ThriftSecurityException, TException {
      verify(c, check(c, SystemPermission.SYSTEM));
      
      final InetSocketAddress addr = AddressUtil.parseAddress(tabletServer, Property.TSERV_CLIENTPORT);
      final String addrString = org.apache.accumulo.core.util.AddressUtil.toString(addr);
      final TServerInstance doomed = tserverSet.find(addrString);
      if (!force) {
        final TServerConnection server = tserverSet.getConnection(doomed);
        if (server == null) {
          log.warn("No server found for name " + tabletServer);
          return;
        }
      }
      
      long tid = fate.startTransaction();
      fate.seedTransaction(tid, new TraceRepo<Master>(new ShutdownTServer(doomed, force)), false);
      fate.waitForCompletion(tid);
      fate.delete(tid);
    }
    
    @Override
    public void reportSplitExtent(TInfo info, AuthInfo credentials, String serverName, TabletSplit split) throws TException {
      if (migrations.remove(new KeyExtent(split.oldTablet)) != null) {
        log.info("Canceled migration of " + split.oldTablet);
      }
      for (TServerInstance instance : tserverSet.getCurrentServers()) {
        if (serverName.equals(instance.hostPort())) {
          nextEvent.event("%s reported split %s, %s", serverName, new KeyExtent(split.newTablets.get(0)), new KeyExtent(split.newTablets.get(1)));
          return;
        }
      }
      log.warn("Got a split from a server we don't recognize: " + serverName);
    }
    
    @Override
    public void reportTabletStatus(TInfo info, AuthInfo credentials, String serverName, TabletLoadState status, TKeyExtent ttablet) throws TException {
      KeyExtent tablet = new KeyExtent(ttablet);
      
      switch (status) {
        case LOAD_FAILURE:
          log.error(serverName + " reports assignment failed for tablet " + tablet);
          break;
        case LOADED:
          nextEvent.event("tablet %s was loaded on %s", tablet, serverName);
          break;
        case UNLOADED:
          nextEvent.event("tablet %s was unloaded from %s", tablet, serverName);
          break;
        case UNLOAD_ERROR:
          log.error(serverName + " reports unload failed for tablet " + tablet);
          break;
        case UNLOAD_FAILURE_NOT_SERVING:
          if (log.isTraceEnabled()) {
            log.trace(serverName + " reports unload failed: not serving tablet, could be a split: " + tablet);
          }
          break;
        case CHOPPED:
          nextEvent.event("tablet %s chopped", tablet);
          break;
      }
    }
    
    @Override
    public void setMasterGoalState(TInfo info, AuthInfo c, MasterGoalState state) throws ThriftSecurityException, TException {
      verify(c, check(c, SystemPermission.SYSTEM));
      Master.this.setMasterGoalState(state);
    }
    
    private void updatePlugins(String property) {
      if (property.equals(Property.MASTER_TABLET_BALANCER.getKey())) {
        TabletBalancer balancer = createInstanceFromPropertyName(instance.getConfiguration(), Property.MASTER_TABLET_BALANCER, TabletBalancer.class,
            new DefaultLoadBalancer());
        balancer.init(serverConfig);
        tabletBalancer = balancer;
        log.info("tablet balancer changed to " + tabletBalancer.getClass().getName());
      }
    }

    @Override
    public void removeSystemProperty(TInfo info, AuthInfo c, String property) throws ThriftSecurityException, TException {
      
      verify(c, check(c, SystemPermission.SYSTEM));
      try {
        SystemPropUtil.removeSystemProperty(property);
        updatePlugins(property);
      } catch (Exception e) {
        log.error("Problem removing config property in zookeeper", e);
        throw new TException(e.getMessage());
      }
    }
    
    @Override
    public void setSystemProperty(TInfo info, AuthInfo credentials, String property, String value) throws ThriftSecurityException, TException {
      verify(credentials, check(credentials, SystemPermission.SYSTEM));
      try {
        SystemPropUtil.setSystemProperty(property, value);
        updatePlugins(property);
      } catch (Exception e) {
        log.error("Problem setting config property in zookeeper", e);
        throw new TException(e.getMessage());
      }
    }
    
    private void authenticate(AuthInfo credentials) throws ThriftSecurityException {
      try {
        if (!authenticator.authenticateUser(credentials, credentials.user, credentials.password))
          throw new ThriftSecurityException(credentials.user, SecurityErrorCode.BAD_CREDENTIALS);
      } catch (AccumuloSecurityException e) {
        throw e.asThriftException();
      }
    }
    
    @Override
    public long beginTableOperation(TInfo tinfo, AuthInfo credentials) throws ThriftSecurityException, TException {
      authenticate(credentials);
      return fate.startTransaction();
    }
    
    @Override
    public void executeTableOperation(TInfo tinfo, AuthInfo c, long opid, org.apache.accumulo.core.master.thrift.TableOperation op, List<ByteBuffer> arguments,
        Map<String,String> options, boolean autoCleanup) throws ThriftSecurityException, ThriftTableOperationException, TException {
      
      authenticate(c);
      
      switch (op) {
        case CREATE: {
          String tableName = ByteBufferUtil.toString(arguments.get(0));
          
          verify(c, check(c, SystemPermission.CREATE_TABLE));
          checkNotMetadataTable(tableName, TableOperation.CREATE);
          checkTableName(tableName, TableOperation.CREATE);
          
          org.apache.accumulo.core.client.admin.TimeType timeType = org.apache.accumulo.core.client.admin.TimeType.valueOf(ByteBufferUtil.toString(arguments
              .get(1)));
          fate.seedTransaction(opid, new TraceRepo<Master>(new CreateTable(c.user, tableName, timeType, options)), autoCleanup);
          
          break;
        }
        case RENAME: {
          String oldTableName = ByteBufferUtil.toString(arguments.get(0));
          String newTableName = ByteBufferUtil.toString(arguments.get(1));
          
          String tableId = checkTableId(oldTableName, TableOperation.RENAME);
          checkNotMetadataTable(oldTableName, TableOperation.RENAME);
          checkNotMetadataTable(newTableName, TableOperation.RENAME);
          checkTableName(newTableName, TableOperation.RENAME);
          verify(c, tableId, TableOperation.RENAME, check(c, tableId, TablePermission.ALTER_TABLE) || check(c, SystemPermission.ALTER_TABLE));
          
          fate.seedTransaction(opid, new TraceRepo<Master>(new RenameTable(tableId, oldTableName, newTableName)), autoCleanup);
          
          break;
        }
        case CLONE: {
          String srcTableId = ByteBufferUtil.toString(arguments.get(0));
          String tableName = ByteBufferUtil.toString(arguments.get(1));
          
          checkNotMetadataTable(tableName, TableOperation.CLONE);
          checkTableName(tableName, TableOperation.CLONE);
          verify(c, srcTableId, TableOperation.CLONE, check(c, SystemPermission.CREATE_TABLE) && check(c, srcTableId, TablePermission.READ));
          
          Map<String,String> propertiesToSet = new HashMap<String,String>();
          Set<String> propertiesToExclude = new HashSet<String>();
          
          for (Entry<String,String> entry : options.entrySet()) {
            if (entry.getValue() == null) {
              propertiesToExclude.add(entry.getKey());
              continue;
            }
            
            if (!TablePropUtil.isPropertyValid(entry.getKey(), entry.getValue())) {
              throw new ThriftTableOperationException(null, tableName, TableOperation.CLONE, TableOperationExceptionType.OTHER, "Property or value not valid "
                  + entry.getKey() + "=" + entry.getValue());
            }
            
            propertiesToSet.put(entry.getKey(), entry.getValue());
          }
          
          fate.seedTransaction(opid, new TraceRepo<Master>(new CloneTable(c.user, srcTableId, tableName, propertiesToSet, propertiesToExclude)), autoCleanup);
          
          break;
        }
        case DELETE: {
          String tableName = ByteBufferUtil.toString(arguments.get(0));
          final String tableId = checkTableId(tableName, TableOperation.DELETE);
          checkNotMetadataTable(tableName, TableOperation.DELETE);
          verify(c, tableId, TableOperation.DELETE, check(c, SystemPermission.DROP_TABLE) || check(c, tableId, TablePermission.DROP_TABLE));
          
          fate.seedTransaction(opid, new TraceRepo<Master>(new DeleteTable(tableId)), autoCleanup);
          break;
        }
        case ONLINE: {
          String tableName = ByteBufferUtil.toString(arguments.get(0));
          final String tableId = checkTableId(tableName, TableOperation.ONLINE);
          checkNotMetadataTable(tableName, TableOperation.ONLINE);
          verify(c, tableId, TableOperation.ONLINE,
              check(c, SystemPermission.SYSTEM) || check(c, SystemPermission.ALTER_TABLE) || check(c, tableId, TablePermission.ALTER_TABLE));
          
          fate.seedTransaction(opid, new TraceRepo<Master>(new ChangeTableState(tableId, TableOperation.ONLINE)), autoCleanup);
          break;
        }
        case OFFLINE: {
          String tableName = ByteBufferUtil.toString(arguments.get(0));
          final String tableId = checkTableId(tableName, TableOperation.OFFLINE);
          checkNotMetadataTable(tableName, TableOperation.OFFLINE);
          verify(c, tableId, TableOperation.OFFLINE,
              check(c, SystemPermission.SYSTEM) || check(c, SystemPermission.ALTER_TABLE) || check(c, tableId, TablePermission.ALTER_TABLE));
          
          fate.seedTransaction(opid, new TraceRepo<Master>(new ChangeTableState(tableId, TableOperation.OFFLINE)), autoCleanup);
          break;
        }
        case MERGE: {
          String tableName = ByteBufferUtil.toString(arguments.get(0));
          Text startRow = ByteBufferUtil.toText(arguments.get(1));
          Text endRow = ByteBufferUtil.toText(arguments.get(2));
          final String tableId = checkTableId(tableName, TableOperation.MERGE);
          if (tableName.equals(Constants.METADATA_TABLE_NAME)) {
            if (startRow.compareTo(new Text("0")) < 0) {
              startRow = new Text("0");
              if (endRow.getLength() != 0 && endRow.compareTo(startRow) < 0)
                throw new ThriftTableOperationException(null, tableName, TableOperation.MERGE, TableOperationExceptionType.OTHER,
                    "end-row specification is in the root tablet, which cannot be merged or split");
            }
          }
          log.debug("Creating merge op: " + tableId + " " + startRow + " " + endRow);
          verify(c, tableId, TableOperation.MERGE,
              check(c, SystemPermission.SYSTEM) || check(c, SystemPermission.ALTER_TABLE) || check(c, tableId, TablePermission.ALTER_TABLE));
          fate.seedTransaction(opid, new TraceRepo<Master>(new TableRangeOp(MergeInfo.Operation.MERGE, tableId, startRow, endRow)), autoCleanup);
          break;
        }
        case DELETE_RANGE: {
          String tableName = ByteBufferUtil.toString(arguments.get(0));
          Text startRow = ByteBufferUtil.toText(arguments.get(1));
          Text endRow = ByteBufferUtil.toText(arguments.get(2));
          
          final String tableId = checkTableId(tableName, TableOperation.DELETE_RANGE);
          checkNotMetadataTable(tableName, TableOperation.DELETE_RANGE);
          verify(c, tableId, TableOperation.DELETE_RANGE, check(c, SystemPermission.SYSTEM) || check(c, tableId, TablePermission.WRITE));
          
          fate.seedTransaction(opid, new TraceRepo<Master>(new TableRangeOp(MergeInfo.Operation.DELETE, tableId, startRow, endRow)), autoCleanup);
          break;
        }
        case BULK_IMPORT: {
          String tableName = ByteBufferUtil.toString(arguments.get(0));
          String dir = ByteBufferUtil.toString(arguments.get(1));
          String failDir = ByteBufferUtil.toString(arguments.get(2));
          boolean setTime = Boolean.parseBoolean(ByteBufferUtil.toString(arguments.get(3)));
          
          final String tableId = checkTableId(tableName, TableOperation.BULK_IMPORT);
          checkNotMetadataTable(tableName, TableOperation.BULK_IMPORT);
          verify(c, tableId, TableOperation.BULK_IMPORT, check(c, tableId, TablePermission.BULK_IMPORT));
          
          fate.seedTransaction(opid, new TraceRepo<Master>(new BulkImport(tableId, dir, failDir, setTime)), autoCleanup);
          break;
        }
        case COMPACT: {
          String tableId = ByteBufferUtil.toString(arguments.get(0));
          byte[] startRow = ByteBufferUtil.toBytes(arguments.get(1));
          byte[] endRow = ByteBufferUtil.toBytes(arguments.get(2));
          List<IteratorSetting> iterators = IteratorUtil.decodeIteratorSettings(ByteBufferUtil.toBytes(arguments.get(3)));
          
          verify(c, tableId, TableOperation.COMPACT,
              check(c, tableId, TablePermission.WRITE) || check(c, tableId, TablePermission.ALTER_TABLE) || check(c, SystemPermission.ALTER_TABLE));
          
          fate.seedTransaction(opid, new TraceRepo<Master>(new CompactRange(tableId, startRow, endRow, iterators)), autoCleanup);
          break;
        }
        default:
          throw new UnsupportedOperationException();
      }
      
    }
    
    @Override
    public String waitForTableOperation(TInfo tinfo, AuthInfo credentials, long opid) throws ThriftSecurityException, ThriftTableOperationException, TException {
      authenticate(credentials);
      
      TStatus status = fate.waitForCompletion(opid);
      if (status == TStatus.FAILED) {
        Exception e = fate.getException(opid);
        if (e instanceof ThriftTableOperationException)
          throw (ThriftTableOperationException) e;
        if (e instanceof ThriftSecurityException)
          throw (ThriftSecurityException) e;
        else if (e instanceof RuntimeException)
          throw (RuntimeException) e;
        else
          throw new RuntimeException(e);
      }
      
      String ret = fate.getReturn(opid);
      if (ret == null)
        ret = ""; // thrift does not like returning null
      return ret;
    }
    
    @Override
    public void finishTableOperation(TInfo tinfo, AuthInfo credentials, long opid) throws ThriftSecurityException, TException {
      authenticate(credentials);
      
      fate.delete(opid);
      
    }
    
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
      String path = ZooUtil.getRoot(instance.getInstanceID()) + Constants.ZTABLES + "/" + info.getRange().getTableId().toString() + "/merge";
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
    nextEvent.event("Merge state of %s set to %s", info.getRange(), state);
  }
  
  public void clearMergeState(Text tableId) throws IOException, KeeperException, InterruptedException {
    synchronized (mergeLock) {
      String path = ZooUtil.getRoot(instance.getInstanceID()) + Constants.ZTABLES + "/" + tableId.toString() + "/merge";
      ZooReaderWriter.getInstance().recursiveDelete(path, NodeMissingPolicy.SKIP);
      mergeLock.notifyAll();
    }
    nextEvent.event("Merge state of %s cleared", tableId);
  }
  
  private void setMasterGoalState(MasterGoalState state) {
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
        log.error("Problem getting real goal state: " + e);
        UtilWaitThread.sleep(1000);
      }
  }
  
  private void shutdown(boolean stopTabletServers) {
    if (stopTabletServers) {
      setMasterGoalState(MasterGoalState.CLEAN_STOP);
      EventCoordinator.Listener eventListener = nextEvent.getListener();
      do {
        waitAround(eventListener);
      } while (tserverSet.size() > 0);
    }
    setMasterState(MasterState.STOP);
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
    }
    // unreachable
    return TabletGoalState.HOSTED;
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
      if (mergeInfo.getRange() != null) {
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
  
  private class TabletGroupWatcher extends Daemon {
    
    final TabletStateStore store;
    final TableStats stats = new TableStats();
    
    TabletGroupWatcher(TabletStateStore store) {
      this.store = store;
    }
    
    Map<Text,TableCounts> getStats() {
      return stats.getLast();
    }
    
    TableCounts getStats(Text tableId) {
      return stats.getLast(tableId);
    }
    
    public void run() {
      
      Thread.currentThread().setName("Watching " + store.name());
      int[] oldCounts = new int[TabletState.values().length];
      EventCoordinator.Listener eventListener = nextEvent.getListener();
      
      while (stillMaster()) {
        int totalUnloaded = 0;
        int unloaded = 0;
        try {
          Map<Text,MergeStats> mergeStatsCache = new HashMap<Text,MergeStats>();
          
          // Get the current status for the current list of tservers
          SortedMap<TServerInstance,TabletServerStatus> currentTServers = new TreeMap<TServerInstance,TabletServerStatus>();
          for (TServerInstance entry : tserverSet.getCurrentServers()) {
            currentTServers.put(entry, tserverStatus.get(entry));
          }
          
          if (currentTServers.size() == 0) {
            eventListener.waitForEvents(TIME_TO_WAIT_BETWEEN_SCANS);
            continue;
          }
          
          // Don't move tablets to servers that are shutting down
          SortedMap<TServerInstance,TabletServerStatus> destinations = new TreeMap<TServerInstance,TabletServerStatus>(currentTServers);
          destinations.keySet().removeAll(serversToShutdown);
          
          List<Assignment> assignments = new ArrayList<Assignment>();
          List<Assignment> assigned = new ArrayList<Assignment>();
          List<TabletLocationState> assignedToDeadServers = new ArrayList<TabletLocationState>();
          Map<KeyExtent,TServerInstance> unassigned = new HashMap<KeyExtent,TServerInstance>();
          
          int[] counts = new int[TabletState.values().length];
          stats.begin();
          // Walk through the tablets in our store, and work tablets
          // towards their goal
          for (TabletLocationState tls : store) {
            if (tls == null) {
              continue;
            }
            
            // Don't overwhelm the tablet servers with work
            if (unassigned.size() + unloaded > MAX_TSERVER_WORK_CHUNK * currentTServers.size()) {
              flushChanges(destinations, assignments, assigned, assignedToDeadServers, unassigned);
              assignments.clear();
              assigned.clear();
              assignedToDeadServers.clear();
              unassigned.clear();
              unloaded = 0;
              eventListener.waitForEvents(TIME_TO_WAIT_BETWEEN_SCANS);
            }
            Text tableId = tls.extent.getTableId();
            MergeStats mergeStats = mergeStatsCache.get(tableId);
            if (mergeStats == null) {
              mergeStatsCache.put(tableId, mergeStats = new MergeStats(getMergeInfo(tableId)));
            }
            TabletGoalState goal = getGoalState(tls, mergeStats.getMergeInfo());
            TServerInstance server = tls.getServer();
            TabletState state = tls.getState(currentTServers.keySet());
            stats.update(tableId, state);
            mergeStats.update(tls.extent, state, tls.chopped, !tls.walogs.isEmpty());
            sendChopRequest(mergeStats.getMergeInfo(), state, tls);
            sendSplitRequest(mergeStats.getMergeInfo(), state, tls);
            
            // Always follow through with assignments
            if (state == TabletState.ASSIGNED) {
              goal = TabletGoalState.HOSTED;
            }
            
            if (goal == TabletGoalState.HOSTED) {
              if (state != TabletState.HOSTED && !tls.walogs.isEmpty()) {
                if (recoverLogs(tls.extent, tls.walogs))
                  continue;
              }
              switch (state) {
                case HOSTED:
                  if (server.equals(migrations.get(tls.extent)))
                    migrations.remove(tls.extent);
                  break;
                case ASSIGNED_TO_DEAD_SERVER:
                  assignedToDeadServers.add(tls);
                  if (server.equals(migrations.get(tls.extent)))
                    migrations.remove(tls.extent);
                  // log.info("Current servers " + currentTServers.keySet());
                  break;
                case UNASSIGNED:
                  // maybe it's a finishing migration
                  TServerInstance dest = migrations.get(tls.extent);
                  if (dest != null) {
                    // if destination is still good, assign it
                    if (destinations.keySet().contains(dest)) {
                      assignments.add(new Assignment(tls.extent, dest));
                    } else {
                      // get rid of this migration
                      migrations.remove(tls.extent);
                      unassigned.put(tls.extent, server);
                    }
                  } else {
                    unassigned.put(tls.extent, server);
                  }
                  break;
                case ASSIGNED:
                  // Send another reminder
                  assigned.add(new Assignment(tls.extent, tls.future));
                  break;
              }
            } else {
              switch (state) {
                case UNASSIGNED:
                  break;
                case ASSIGNED_TO_DEAD_SERVER:
                  assignedToDeadServers.add(tls);
                  // log.info("Current servers " + currentTServers.keySet());
                  break;
                case HOSTED:
                  TServerConnection conn = tserverSet.getConnection(server);
                  if (conn != null) {
                    conn.unloadTablet(masterLock, tls.extent, goal != TabletGoalState.DELETED);
                    unloaded++;
                    totalUnloaded++;
                  } else {
                    log.warn("Could not connect to server " + server);
                  }
                  break;
                case ASSIGNED:
                  break;
              }
            }
            counts[state.ordinal()]++;
          }
          
          flushChanges(destinations, assignments, assigned, assignedToDeadServers, unassigned);
          
          // provide stats after flushing changes to avoid race conditions w/ delete table
          stats.end();
          
          // Report changes
          for (TabletState state : TabletState.values()) {
            int i = state.ordinal();
            if (counts[i] > 0 && counts[i] != oldCounts[i]) {
              nextEvent.event("[%s]: %d tablets are %s", store.name(), counts[i], state.name());
            }
          }
          log.debug(String.format("[%s]: scan time %.2f seconds", store.name(), stats.getScanTime() / 1000.));
          oldCounts = counts;
          if (totalUnloaded > 0) {
            nextEvent.event("[%s]: %d tablets unloaded", store.name(), totalUnloaded);
          }
          
          updateMergeState(mergeStatsCache);
          
          log.debug(String.format("[%s] sleeping for %.2f seconds", store.name(), TIME_TO_WAIT_BETWEEN_SCANS / 1000.));
          eventListener.waitForEvents(TIME_TO_WAIT_BETWEEN_SCANS);
        } catch (Exception ex) {
          log.error("Error processing table state for store " + store.name(), ex);
          UtilWaitThread.sleep(WAIT_BETWEEN_ERRORS);
        }
      }
    }
    
    private void sendSplitRequest(MergeInfo info, TabletState state, TabletLocationState tls) {
      // Already split?
      if (!info.getState().equals(MergeState.SPLITTING))
        return;
      // Merges don't split
      if (!info.isDelete())
        return;
      // Online and ready to split?
      if (!state.equals(TabletState.HOSTED))
        return;
      // Does this extent cover the end points of the delete?
      KeyExtent range = info.getRange();
      if (tls.extent.overlaps(range)) {
        for (Text splitPoint : new Text[] {range.getPrevEndRow(), range.getEndRow()}) {
          if (splitPoint == null)
            continue;
          if (!tls.extent.contains(splitPoint))
            continue;
          if (splitPoint.equals(tls.extent.getEndRow()))
            continue;
          if (splitPoint.equals(tls.extent.getPrevEndRow()))
            continue;
          try {
            TServerConnection conn;
            conn = tserverSet.getConnection(tls.current);
            if (conn != null) {
              log.info("Asking " + tls.current + " to split " + tls.extent + " at " + splitPoint);
              conn.splitTablet(masterLock, tls.extent, splitPoint);
            } else {
              log.warn("Not connected to server " + tls.current);
            }
          } catch (Exception e) {
            log.warn("Error asking tablet server to split a tablet: " + e);
          }
        }
      }
    }
    
    private void sendChopRequest(MergeInfo info, TabletState state, TabletLocationState tls) {
      // Don't bother if we're in the wrong state
      if (!info.getState().equals(MergeState.WAITING_FOR_CHOPPED))
        return;
      // Tablet must be online
      if (!state.equals(TabletState.HOSTED))
        return;
      // Tablet isn't already chopped
      if (tls.chopped)
        return;
      // Tablet ranges intersect
      if (info.needsToBeChopped(tls.extent)) {
        TServerConnection conn;
        try {
          conn = tserverSet.getConnection(tls.current);
          if (conn != null) {
            log.info("Asking " + tls.current + " to chop " + tls.extent);
            conn.chop(masterLock, tls.extent);
          } else {
            log.warn("Could not connect to server " + tls.current);
          }
        } catch (TException e) {
          log.warn("Communications error asking tablet server to chop a tablet");
        }
      }
    }
    
    private void updateMergeState(Map<Text,MergeStats> mergeStatsCache) {
      for (MergeStats stats : mergeStatsCache.values()) {
        try {
          MergeState update = stats.nextMergeState(getConnector(), Master.this);
          
          // when next state is MERGING, its important to persist this before
          // starting the merge... the verification check that is done before
          // moving into the merging state could fail if merge starts but does
          // not finish
          if (update == MergeState.COMPLETE)
            update = MergeState.NONE;
          if (update != stats.getMergeInfo().getState()) {
            setMergeState(stats.getMergeInfo(), update);
          }

          if (update == MergeState.MERGING) {
            try {
              if (stats.getMergeInfo().isDelete()) {
                deleteTablets(stats.getMergeInfo());
              } else {
                mergeMetadataRecords(stats.getMergeInfo());
              }
              setMergeState(stats.getMergeInfo(), update = MergeState.COMPLETE);
            } catch (Exception ex) {
              log.error("Unable merge metadata table records", ex);
            }
          }
        } catch (Exception ex) {
          log.error("Unable to update merge state for merge " + stats.getMergeInfo().getRange(), ex);
        }
      }
    }

    private void deleteTablets(MergeInfo info) throws AccumuloException {
      KeyExtent range = info.getRange();
      log.debug("Deleting tablets for " + range);
      char timeType = '\0';
      KeyExtent followingTablet = null;
      if (range.getEndRow() != null) {
        Key nextExtent = new Key(range.getEndRow()).followingKey(PartialKey.ROW);
        followingTablet = getHighTablet(new KeyExtent(range.getTableId(), nextExtent.getRow(), range.getEndRow()));
        log.debug("Found following tablet " + followingTablet);
      }
      try {
        Connector conn = getConnector();
        Text start = range.getPrevEndRow();
        if (start == null) {
          start = new Text();
        }
        log.debug("Making file deletion entries for " + range);
        Range deleteRange = new Range(KeyExtent.getMetadataEntry(range.getTableId(), start), false, KeyExtent.getMetadataEntry(range.getTableId(),
            range.getEndRow()), true);
        Scanner scanner = conn.createScanner(Constants.METADATA_TABLE_NAME, Constants.NO_AUTHS);
        scanner.setRange(deleteRange);
        ColumnFQ.fetch(scanner, Constants.METADATA_DIRECTORY_COLUMN);
        ColumnFQ.fetch(scanner, Constants.METADATA_TIME_COLUMN);
        scanner.fetchColumnFamily(Constants.METADATA_DATAFILE_COLUMN_FAMILY);
        scanner.fetchColumnFamily(Constants.METADATA_CURRENT_LOCATION_COLUMN_FAMILY);
        Set<String> datafiles = new TreeSet<String>();
        for (Entry<Key,Value> entry : scanner) {
          Key key = entry.getKey();
          if (key.compareColumnFamily(Constants.METADATA_DATAFILE_COLUMN_FAMILY) == 0) {
            datafiles.add(key.getColumnQualifier().toString());
            if (datafiles.size() > 1000) {
              MetadataTable.addDeleteEntries(range, datafiles, SecurityConstants.getSystemCredentials());
              datafiles.clear();
            }
          } else if (Constants.METADATA_TIME_COLUMN.hasColumns(key)) {
            timeType = entry.getValue().toString().charAt(0);
          } else if (key.compareColumnFamily(Constants.METADATA_CURRENT_LOCATION_COLUMN_FAMILY) == 0) {
            throw new IllegalStateException("Tablet " + key.getRow() + " is assigned during a merge!");
          } else if (Constants.METADATA_DIRECTORY_COLUMN.hasColumns(key)) {
            datafiles.add(entry.getValue().toString());
            if (datafiles.size() > 1000) {
              MetadataTable.addDeleteEntries(range, datafiles, SecurityConstants.getSystemCredentials());
              datafiles.clear();
            }
          }
        }
        MetadataTable.addDeleteEntries(range, datafiles, SecurityConstants.getSystemCredentials());
        
        BatchWriter bw = conn.createBatchWriter(Constants.METADATA_TABLE_NAME, 1000000l, 100l, 1);
        try {
          deleteTablets(deleteRange, bw, conn);
        } finally {
          bw.close();
        }

        if (followingTablet != null) {
          log.debug("Updating prevRow of " + followingTablet + " to " + range.getPrevEndRow());
          bw = conn.createBatchWriter(Constants.METADATA_TABLE_NAME, 1000l, 100l, 1);
          try {
            Mutation m = new Mutation(followingTablet.getMetadataEntry());
            ColumnFQ.put(m, Constants.METADATA_PREV_ROW_COLUMN, KeyExtent.encodePrevEndRow(range.getPrevEndRow()));
            ColumnFQ.putDelete(m, Constants.METADATA_CHOPPED_COLUMN);
            bw.addMutation(m);
            bw.flush();
          } finally {
            bw.close();
          }
        } else {
          // Recreate the default tablet to hold the end of the table
          log.debug("Recreating the last tablet to point to " + range.getPrevEndRow());
          MetadataTable.addTablet(new KeyExtent(range.getTableId(), null, range.getPrevEndRow()), Constants.DEFAULT_TABLET_LOCATION,
              SecurityConstants.getSystemCredentials(), timeType, masterLock);
        }
      } catch (Exception ex) {
        throw new AccumuloException(ex);
      }
    }
    
    private void mergeMetadataRecords(MergeInfo info) throws AccumuloException {
      KeyExtent range = info.getRange();
      log.debug("Merging metadata for " + range);
      KeyExtent stop = getHighTablet(range);
      log.debug("Highest tablet is " + stop);
      Value firstPrevRowValue = null;
      Text stopRow = stop.getMetadataEntry();
      Text start = range.getPrevEndRow();
      if (start == null) {
        start = new Text();
      }
      Range scanRange = new Range(KeyExtent.getMetadataEntry(range.getTableId(), start), false, stopRow, false);
      if (range.isMeta())
        scanRange = scanRange.clip(Constants.METADATA_ROOT_TABLET_KEYSPACE);

      BatchWriter bw = null;
      try {
        long fileCount = 0;
        Connector conn = getConnector();
        // Make file entries in highest tablet
        bw = conn.createBatchWriter(Constants.METADATA_TABLE_NAME, 1000000L, 1000L, 1);
        Scanner scanner = conn.createScanner(Constants.METADATA_TABLE_NAME, Constants.NO_AUTHS);
        scanner.setRange(scanRange);
        ColumnFQ.fetch(scanner, Constants.METADATA_PREV_ROW_COLUMN);
        ColumnFQ.fetch(scanner, Constants.METADATA_TIME_COLUMN);
        ColumnFQ.fetch(scanner, Constants.METADATA_DIRECTORY_COLUMN);
        scanner.fetchColumnFamily(Constants.METADATA_DATAFILE_COLUMN_FAMILY);
        Mutation m = new Mutation(stopRow);
        String maxLogicalTime = null;
        for (Entry<Key,Value> entry : scanner) {
          Key key = entry.getKey();
          Value value = entry.getValue();
          if (key.getColumnFamily().equals(Constants.METADATA_DATAFILE_COLUMN_FAMILY)) {
            m.put(key.getColumnFamily(), key.getColumnQualifier(), value);
            fileCount++;
          } else if (Constants.METADATA_PREV_ROW_COLUMN.hasColumns(key) && firstPrevRowValue == null) {
            log.debug("prevRow entry for lowest tablet is " + value);
            firstPrevRowValue = new Value(value);
          } else if (Constants.METADATA_TIME_COLUMN.hasColumns(key)) {
            maxLogicalTime = TabletTime.maxMetadataTime(maxLogicalTime, value.toString());
          } else if (Constants.METADATA_DIRECTORY_COLUMN.hasColumns(key)) {
            if (!range.isMeta())
              bw.addMutation(MetadataTable.createDeleteMutation(range.getTableId().toString(), entry.getValue().toString()));
          }
        }
        
        // read the logical time from the last tablet in the merge range, it is not included in
        // the loop above
        scanner = conn.createScanner(Constants.METADATA_TABLE_NAME, Constants.NO_AUTHS);
        Range last = new Range(stopRow);
        if (range.isMeta())
          last = last.clip(Constants.METADATA_ROOT_TABLET_KEYSPACE);
        scanner.setRange(last);
        ColumnFQ.fetch(scanner, Constants.METADATA_TIME_COLUMN);
        for (Entry<Key,Value> entry : scanner) {
          if (Constants.METADATA_TIME_COLUMN.hasColumns(entry.getKey())) {
            maxLogicalTime = TabletTime.maxMetadataTime(maxLogicalTime, entry.getValue().toString());
          }
        }
        
        if (maxLogicalTime != null) ColumnFQ.put(m, Constants.METADATA_TIME_COLUMN, new Value(maxLogicalTime.getBytes()));
        
        if (!m.getUpdates().isEmpty()) {
          bw.addMutation(m);
        }
        
        bw.flush();

        log.debug("Moved " + fileCount + " files to " + stop);
        
        if (firstPrevRowValue == null) {
          log.debug("tablet already merged");
          return;
        }
        
        stop.setPrevEndRow(KeyExtent.decodePrevEndRow(firstPrevRowValue));
        Mutation updatePrevRow = stop.getPrevRowUpdateMutation();
        log.debug("Setting the prevRow for last tablet: " + stop);
        bw.addMutation(updatePrevRow);
        bw.flush();

        deleteTablets(scanRange, bw, conn);
        
        // Clean-up the last chopped marker
        m = new Mutation(stopRow);
        ColumnFQ.putDelete(m, Constants.METADATA_CHOPPED_COLUMN);
        bw.addMutation(m);
        bw.flush();
        
      } catch (Exception ex) {
        throw new AccumuloException(ex);
      } finally {
        if (bw != null) try {
          bw.close();
        } catch (Exception ex) {
          throw new AccumuloException(ex);
        }
      }
    }

    private void deleteTablets(Range scanRange, BatchWriter bw, Connector conn) throws TableNotFoundException, MutationsRejectedException {
      Scanner scanner;
      Mutation m;
      // Delete everything in the other tablets
      // group all deletes into tablet into one mutation, this makes tablets
      // either dissapear entirely or not all.. this is important for the case
      // where the process terminates in the loop below...
      scanner = conn.createScanner(Constants.METADATA_TABLE_NAME, Constants.NO_AUTHS);
      log.debug("Deleting range " + scanRange);
      scanner.setRange(scanRange);
      RowIterator rowIter = new RowIterator(scanner);
      while (rowIter.hasNext()) {
        Iterator<Entry<Key,Value>> row = rowIter.next();
        m = null;
        while (row.hasNext()) {
          Entry<Key,Value> entry = row.next();
          Key key = entry.getKey();

          if (m == null)
            m = new Mutation(key.getRow());
          
          m.putDelete(key.getColumnFamily(), key.getColumnQualifier());
          log.debug("deleting entry " + key);
        }
        bw.addMutation(m);
      }

      bw.flush();
    }
    
    private KeyExtent getHighTablet(KeyExtent range) throws AccumuloException {
      try {
        Connector conn = getConnector();
        Scanner scanner = conn.createScanner(Constants.METADATA_TABLE_NAME, Constants.NO_AUTHS);
        ColumnFQ.fetch(scanner, Constants.METADATA_PREV_ROW_COLUMN);
        KeyExtent start = new KeyExtent(range.getTableId(), range.getEndRow(), null);
        scanner.setRange(new Range(start.getMetadataEntry(), null));
        Iterator<Entry<Key,Value>> iterator = scanner.iterator();
        if (!iterator.hasNext()) {
          throw new AccumuloException("No last tablet for a merge " + range);
        }
        Entry<Key,Value> entry = iterator.next();
        KeyExtent highTablet = new KeyExtent(entry.getKey().getRow(), KeyExtent.decodePrevEndRow(entry.getValue()));
        if (highTablet.getTableId() != range.getTableId()) {
          throw new AccumuloException("No last tablet for merge " + range + " " + highTablet);
        }
        return highTablet;
      } catch (Exception ex) {
        throw new AccumuloException("Unexpected failure finding the last tablet for a merge " + range, ex);
      }
    }
    
    private void flushChanges(SortedMap<TServerInstance,TabletServerStatus> currentTServers, List<Assignment> assignments, List<Assignment> assigned,
        List<TabletLocationState> assignedToDeadServers, Map<KeyExtent,TServerInstance> unassigned) throws DistributedStoreException, TException {
      if (!assignedToDeadServers.isEmpty()) {
        int maxServersToShow = min(assignedToDeadServers.size(), 100);
        log.debug(assignedToDeadServers.size() + " assigned to dead servers: " + assignedToDeadServers.subList(0, maxServersToShow) + "...");
        store.unassign(assignedToDeadServers);
        nextEvent.event("Marked %d tablets as unassigned because they don't have current servers", assignedToDeadServers.size());
      }
      
      if (!currentTServers.isEmpty()) {
        Map<KeyExtent,TServerInstance> assignedOut = new HashMap<KeyExtent,TServerInstance>();
        tabletBalancer.getAssignments(Collections.unmodifiableSortedMap(currentTServers), Collections.unmodifiableMap(unassigned), assignedOut);
        for (Entry<KeyExtent,TServerInstance> assignment : assignedOut.entrySet()) {
          if (unassigned.containsKey(assignment.getKey())) {
            if (assignment.getValue() != null) {
              log.debug(store.name() + " assigning tablet " + assignment);
              assignments.add(new Assignment(assignment.getKey(), assignment.getValue()));
            }
          } else {
            log.warn(store.name() + " load balancer assigning tablet that was not nominated for assignment " + assignment.getKey());
          }
        }
        if (!unassigned.isEmpty() && assignedOut.isEmpty())
          log.warn("Load balancer failed to assign any tablets");
      }
      
      if (assignments.size() > 0) {
        log.info(String.format("Assigning %d tablets", assignments.size()));
        store.setFutureLocations(assignments);
      }
      assignments.addAll(assigned);
      for (Assignment a : assignments) {
        TServerConnection conn = tserverSet.getConnection(a.server);
        if (conn != null) {
          conn.assignTablet(masterLock, a.tablet);
        } else {
          log.warn("Could not connect to server " + a.server);
        }
      }
    }
    
  }
  

  private class MigrationCleanupThread extends Daemon {
    
    public void run() {
      setName("Migration Cleanup Thread");
      while (stillMaster()) {
        if (!migrations.isEmpty()) {
          try {
            cleanupMutations();
          } catch (Exception ex) {
            log.error("Error cleaning up migrations", ex);
          }
        }
        UtilWaitThread.sleep(TIME_BETWEEN_MIGRATION_CLEANUPS);
      }
    }
    
    // If a migrating tablet splits, and the tablet dies before sending the
    // master a message, the migration will refer to a non-existing tablet,
    // so it can never complete. Periodically scan the metadata table and
    // remove any migrating tablets that no longer exist.
    private void cleanupMutations() throws AccumuloException, AccumuloSecurityException, TableNotFoundException {
      Connector connector = getConnector();
      Scanner scanner = connector.createScanner(Constants.METADATA_TABLE_NAME, Constants.NO_AUTHS);
      ColumnFQ.fetch(scanner, Constants.METADATA_PREV_ROW_COLUMN);
      Set<KeyExtent> found = new HashSet<KeyExtent>();
      for (Entry<Key,Value> entry : scanner) {
        KeyExtent extent = new KeyExtent(entry.getKey().getRow(), entry.getValue());
        if (migrations.containsKey(extent)) {
          found.add(extent);
        }
      }
      migrations.keySet().retainAll(found);
    }
  }
  
  private class StatusThread extends Daemon {
    
    public void run() {
      setName("Status Thread");
      EventCoordinator.Listener eventListener = nextEvent.getListener();
      while (stillMaster()) {
        int count = 0;
        long wait = DEFAULT_WAIT_FOR_WATCHER;
        try {
          switch (getMasterGoalState()) {
            case NORMAL:
              switch (getMasterState()) {
                case HAVE_LOCK:
                case SAFE_MODE:
                  setMasterState(MasterState.NORMAL);
                default:
                  break;
              }
              break;
            case SAFE_MODE:
              if (getMasterState() == MasterState.NORMAL) {
                setMasterState(MasterState.SAFE_MODE);
              }
              break;
            case CLEAN_STOP:
              switch (getMasterState()) {
                case NORMAL:
                  setMasterState(MasterState.SAFE_MODE);
                  break;
                case SAFE_MODE:
                  count = nonMetaDataTabletsAssignedOrHosted();
                  log.debug(String.format("There are %d non-metadata tablets assigned or hosted", count));
                  if (count == 0)
                    setMasterState(MasterState.UNLOAD_METADATA_TABLETS);
                  break;
                case UNLOAD_METADATA_TABLETS:
                  count = assignedOrHosted(METADATA_TABLE_ID);
                  log.debug(String.format("There are %d metadata tablets assigned or hosted", count));
                  // Assumes last tablet hosted is the root tablet;
                  // it's possible
                  // that's not the case (root tablet is offline?)
                  if (count == 1)
                    setMasterState(MasterState.UNLOAD_ROOT_TABLET);
                  break;
                case UNLOAD_ROOT_TABLET:
                  count = assignedOrHosted(METADATA_TABLE_ID);
                  if (count > 0)
                    log.debug(String.format("The root tablet is still assigned or hosted"));
                  if (count == 0) {
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
                  break;
                default:
                  break;
              }
          }
          wait = updateStatus();
          eventListener.waitForEvents(wait);
        } catch (Throwable t) {
          log.error("Error balancing tablets", t);
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
        log.debug("not balancing because there are unhosted tablets");
      } else if (getMasterGoalState() == MasterGoalState.CLEAN_STOP) {
        log.debug("not balancing because the master is attempting to stop cleanly");
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
        TabletServerStatus status = tserverSet.getConnection(server).getTableMap();
        result.put(server, status);
        // TODO maybe remove from bad servers
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
    }
    log.debug(String.format("Finished gathering information from %d servers in %.2f seconds", result.size(), (System.currentTimeMillis() - start) / 1000.));
    return result;
  }
  
  public boolean recoverLogs(KeyExtent extent, Collection<Collection<String>> walogs) throws IOException {
    boolean recoveryNeeded = false;
    for (Collection<String> logs : walogs) {
      for (String log : logs) {
        String parts[] = log.split("/");
        String host = parts[0];
        String filename = parts[1];
        if (fs.exists(new Path(Constants.getRecoveryDir(getSystemConfiguration()) + "/" + filename + "/finished"))) {
          recoveriesInProgress.remove(filename);
          continue;
        }
        recoveryNeeded = true;
        synchronized (recoveriesInProgress) {
          if (!recoveriesInProgress.contains(filename)) {
            Master.log.info("Starting recovery of " + filename + " created for " + host + ", tablet " + extent + " holds a reference");
            long tid = fate.startTransaction();
            fate.seedTransaction(tid, new RecoverLease(host, filename), true);
            recoveriesInProgress.add(filename);
          }
        }
      }
    }
    return recoveryNeeded;
  }

  public void run() throws IOException, InterruptedException, KeeperException {
    final String zroot = ZooUtil.getRoot(instance);
    
    getMasterLock(zroot + Constants.ZMASTER_LOCK);
    
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
      }
    });
    
    AuthInfo systemAuths = SecurityConstants.getSystemCredentials();
    final TabletStateStore stores[] = {new ZooTabletStateStore(new ZooStore(zroot)), new RootTabletStateStore(instance, systemAuths, this),
        new MetaDataStateStore(instance, systemAuths, this)};
    for (int i = 0; i < stores.length; i++) {
      watchers.add(new TabletGroupWatcher(stores[i]));
    }
    for (TabletGroupWatcher watcher : watchers) {
      watcher.start();
    }
    
    // TODO: add shutdown for fate object
    try {
      fate = new Fate<Master>(this, new org.apache.accumulo.server.fate.ZooStore<Master>(ZooUtil.getRoot(instance) + Constants.ZFATE,
          ZooReaderWriter.getRetryingInstance()), 4);
    } catch (KeeperException e) {
      throw new IOException(e);
    } catch (InterruptedException e) {
      throw new IOException(e);
    }
    
    Processor processor = new MasterClientService.Processor(TraceWrap.service(new MasterClientServiceHandler()));
    clientService = TServerUtils.startServer(getSystemConfiguration(), Property.MASTER_CLIENTPORT, processor, "Master", "Master Client Service Handler", null,
        Property.MASTER_MINTHREADS, Property.MASTER_THREADCHECK).server;
    
    while (!clientService.isServing()) {
      UtilWaitThread.sleep(100);
    }
    while (clientService.isServing()) {
      UtilWaitThread.sleep(500);
    }
    
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
  
  private void getMasterLock(final String zMasterLoc) throws KeeperException, InterruptedException {
    log.info("trying to get master lock");
    LockWatcher masterLockWatcher = new ZooLock.LockWatcher() {
      public void lostLock(LockLossReason reason) {
        Halt.halt("Master lock in zookeeper lost (reason = " + reason + "), exiting!", -1);
      }
    };
    long current = System.currentTimeMillis();
    final long waitTime = getSystemConfiguration().getTimeInMillis(Property.INSTANCE_ZK_TIMEOUT);
    final String masterClientAddress = org.apache.accumulo.core.util.AddressUtil.toString(new InetSocketAddress(hostname, getSystemConfiguration().getPort(
        Property.MASTER_CLIENTPORT)));
    
    boolean locked = false;
    while (System.currentTimeMillis() - current < waitTime) {
      masterLock = new ZooLock(zMasterLoc);
      if (masterLock.tryLock(masterLockWatcher, masterClientAddress.getBytes())) {
        locked = true;
        break;
      }
      UtilWaitThread.sleep(TIME_TO_WAIT_BETWEEN_LOCK_CHECKS);
    }
    if (!locked) {
      log.info("Failed to get master lock, even after waiting for session timeout, becoming back-up server");
      while (true) {
        masterLock = new ZooLock(zMasterLoc);
        if (masterLock.tryLock(masterLockWatcher, masterClientAddress.getBytes())) {
          break;
        }
        UtilWaitThread.sleep(TIME_TO_WAIT_BETWEEN_LOCK_CHECKS);
      }
    }
    setMasterState(MasterState.HAVE_LOCK);
  }
  
  public static void main(String[] args) throws Exception {
    try {
      SecurityUtil.serverLogin();
      
      FileSystem fs = FileUtil.getFileSystem(CachedConfiguration.getInstance(), ServerConfiguration.getSiteConfiguration());
      String hostname = Accumulo.getLocalAddress(args);
      Instance instance = HdfsZooInstance.getInstance();
      ServerConfiguration conf = new ServerConfiguration(instance);
      Accumulo.init(fs, conf, "master");
      Master master = new Master(conf, fs, hostname);
      Accumulo.enableTracing(hostname, "master");
      master.run();
    } catch (Exception ex) {
      log.error("Unexpected exception, exiting", ex);
    }
  }
  
  static final String I_DONT_KNOW_WHY = "unexpected failure";
  
  @Override
  public void update(LiveTServerSet current, Set<TServerInstance> deleted, Set<TServerInstance> added) {
    DeadServerList obit = new DeadServerList(ZooUtil.getRoot(instance) + Constants.ZDEADTSERVERS);
    if (added.size() > 0) {
      log.info("New servers: " + added);
      for (TServerInstance up : added)
        obit.delete(up.hostPort());
    }
    for (TServerInstance dead : deleted) {
      String cause = I_DONT_KNOW_WHY;
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
  
  @Override
  public void stateChanged(String tableId, TableState state) {
    nextEvent.event("Table state in zookeeper changed for %s to %s", tableId, state);
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
        result.add(Constants.METADATA_TABLE_ID);
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
  
  public void killTServer(TServerInstance server) {
    nextEvent.event("Forcing server down %s", server);
    serversToShutdown.add(server);
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
  
  public FileSystem getFileSystem() {
    return this.fs;
  }

  public void updateRecoveryInProgress(String file) {
    recoveriesInProgress.add(file);
  }
}

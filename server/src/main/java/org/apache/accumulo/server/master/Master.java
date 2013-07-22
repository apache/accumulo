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
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.IsolatedScanner;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.RowIterator;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.impl.Tables;
import org.apache.accumulo.core.client.impl.ThriftTransportPool;
import org.apache.accumulo.core.client.impl.thrift.SecurityErrorCode;
import org.apache.accumulo.core.client.impl.thrift.TableOperation;
import org.apache.accumulo.core.client.impl.thrift.TableOperationExceptionType;
import org.apache.accumulo.core.client.impl.thrift.ThriftSecurityException;
import org.apache.accumulo.core.client.impl.thrift.ThriftTableOperationException;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.KeyExtent;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.data.thrift.TKeyExtent;
import org.apache.accumulo.core.iterators.IteratorUtil;
import org.apache.accumulo.core.master.state.tables.TableState;
import org.apache.accumulo.core.master.thrift.MasterClientService;
import org.apache.accumulo.core.master.thrift.MasterClientService.Iface;
import org.apache.accumulo.core.master.thrift.MasterClientService.Processor;
import org.apache.accumulo.core.master.thrift.MasterGoalState;
import org.apache.accumulo.core.master.thrift.MasterMonitorInfo;
import org.apache.accumulo.core.master.thrift.MasterState;
import org.apache.accumulo.core.master.thrift.TableInfo;
import org.apache.accumulo.core.master.thrift.TabletLoadState;
import org.apache.accumulo.core.master.thrift.TabletServerStatus;
import org.apache.accumulo.core.master.thrift.TabletSplit;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.metadata.RootTable;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.LogColumnFamily;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.SecurityUtil;
import org.apache.accumulo.core.security.thrift.TCredentials;
import org.apache.accumulo.core.util.ByteBufferUtil;
import org.apache.accumulo.core.util.Daemon;
import org.apache.accumulo.core.util.UtilWaitThread;
import org.apache.accumulo.core.zookeeper.ZooUtil;
import org.apache.accumulo.fate.AgeOffStore;
import org.apache.accumulo.fate.Fate;
import org.apache.accumulo.fate.TStore.TStatus;
import org.apache.accumulo.fate.zookeeper.IZooReaderWriter;
import org.apache.accumulo.fate.zookeeper.IZooReaderWriter.Mutator;
import org.apache.accumulo.fate.zookeeper.ZooLock.LockLossReason;
import org.apache.accumulo.fate.zookeeper.ZooUtil.NodeExistsPolicy;
import org.apache.accumulo.fate.zookeeper.ZooUtil.NodeMissingPolicy;
import org.apache.accumulo.server.Accumulo;
import org.apache.accumulo.server.ServerConstants;
import org.apache.accumulo.server.client.HdfsZooInstance;
import org.apache.accumulo.server.conf.ServerConfiguration;
import org.apache.accumulo.server.fs.VolumeManager;
import org.apache.accumulo.server.fs.VolumeManagerImpl;
import org.apache.accumulo.server.master.LiveTServerSet.TServerConnection;
import org.apache.accumulo.server.master.balancer.DefaultLoadBalancer;
import org.apache.accumulo.server.master.balancer.TabletBalancer;
import org.apache.accumulo.server.master.recovery.RecoveryManager;
import org.apache.accumulo.server.master.state.CurrentState;
import org.apache.accumulo.server.master.state.DeadServerList;
import org.apache.accumulo.server.master.state.MergeInfo;
import org.apache.accumulo.server.master.state.MergeState;
import org.apache.accumulo.server.master.state.MetaDataStateStore;
import org.apache.accumulo.server.master.state.RootTabletStateStore;
import org.apache.accumulo.server.master.state.TServerInstance;
import org.apache.accumulo.server.master.state.TableCounts;
import org.apache.accumulo.server.master.state.TabletLocationState;
import org.apache.accumulo.server.master.state.TabletMigration;
import org.apache.accumulo.server.master.state.TabletServerState;
import org.apache.accumulo.server.master.state.TabletState;
import org.apache.accumulo.server.master.state.ZooStore;
import org.apache.accumulo.server.master.state.ZooTabletStateStore;
import org.apache.accumulo.server.master.state.tables.TableManager;
import org.apache.accumulo.server.master.state.tables.TableObserver;
import org.apache.accumulo.server.master.tableOps.BulkImport;
import org.apache.accumulo.server.master.tableOps.CancelCompactions;
import org.apache.accumulo.server.master.tableOps.ChangeTableState;
import org.apache.accumulo.server.master.tableOps.CloneTable;
import org.apache.accumulo.server.master.tableOps.CompactRange;
import org.apache.accumulo.server.master.tableOps.CreateTable;
import org.apache.accumulo.server.master.tableOps.DeleteTable;
import org.apache.accumulo.server.master.tableOps.ExportTable;
import org.apache.accumulo.server.master.tableOps.ImportTable;
import org.apache.accumulo.server.master.tableOps.RenameTable;
import org.apache.accumulo.server.master.tableOps.TableRangeOp;
import org.apache.accumulo.server.master.tableOps.TraceRepo;
import org.apache.accumulo.server.master.tserverOps.ShutdownTServer;
import org.apache.accumulo.server.monitor.Monitor;
import org.apache.accumulo.server.security.AuditedSecurityOperation;
import org.apache.accumulo.server.security.SecurityOperation;
import org.apache.accumulo.server.security.SystemCredentials;
import org.apache.accumulo.server.util.AddressUtil;
import org.apache.accumulo.server.util.DefaultMap;
import org.apache.accumulo.server.util.Halt;
import org.apache.accumulo.server.util.MetadataTableUtil;
import org.apache.accumulo.server.util.SystemPropUtil;
import org.apache.accumulo.server.util.TServerUtils;
import org.apache.accumulo.server.util.TablePropUtil;
import org.apache.accumulo.server.util.TabletIterator.TabletDeletedException;
import org.apache.accumulo.server.util.time.SimpleTimer;
import org.apache.accumulo.server.zookeeper.ZooLock;
import org.apache.accumulo.server.zookeeper.ZooReaderWriter;
import org.apache.accumulo.start.classloader.vfs.AccumuloVFSClassLoader;
import org.apache.accumulo.trace.instrument.thrift.TraceWrap;
import org.apache.accumulo.trace.thrift.TInfo;
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
import org.apache.zookeeper.data.Stat;

/**
 * The Master is responsible for assigning and balancing tablets to tablet servers.
 * 
 * The master will also coordinate log recoveries and reports general status.
 */
public class Master implements LiveTServerSet.Listener, TableObserver, CurrentState {
  
  final static Logger log = Logger.getLogger(Master.class);
  
  final private static int ONE_SECOND = 1000;
  final private static Text METADATA_TABLE_ID = new Text(MetadataTable.ID);
  final static long TIME_TO_WAIT_BETWEEN_SCANS = 60 * ONE_SECOND;
  final private static long TIME_BETWEEN_MIGRATION_CLEANUPS = 5 * 60 * ONE_SECOND;
  final static long WAIT_BETWEEN_ERRORS = ONE_SECOND;
  final private static long DEFAULT_WAIT_FOR_WATCHER = 10 * ONE_SECOND;
  final private static int MAX_CLEANUP_WAIT_TIME = ONE_SECOND;
  final private static int TIME_TO_WAIT_BETWEEN_LOCK_CHECKS = ONE_SECOND;
  final static int MAX_TSERVER_WORK_CHUNK = 5 * ONE_SECOND;
  final private static int MAX_BAD_STATUS_COUNT = 3;
  
  final VolumeManager fs;
  final private Instance instance;
  final private String hostname;
  final LiveTServerSet tserverSet;
  final private List<TabletGroupWatcher> watchers = new ArrayList<TabletGroupWatcher>();
  final private SecurityOperation security;
  final private Map<TServerInstance,AtomicInteger> badServers = Collections.synchronizedMap(new DefaultMap<TServerInstance,AtomicInteger>(new AtomicInteger()));
  final Set<TServerInstance> serversToShutdown = Collections.synchronizedSet(new HashSet<TServerInstance>());
  final SortedMap<KeyExtent,TServerInstance> migrations = Collections.synchronizedSortedMap(new TreeMap<KeyExtent,TServerInstance>());
  final EventCoordinator nextEvent = new EventCoordinator();
  final private Object mergeLock = new Object();
  RecoveryManager recoveryManager = null;
  
  ZooLock masterLock = null;
  private TServer clientService = null;
  TabletBalancer tabletBalancer;
  
  private MasterState state = MasterState.INITIAL;
  
  private Fate<Master> fate;
  
  volatile SortedMap<TServerInstance,TabletServerStatus> tserverStatus = Collections.unmodifiableSortedMap(new TreeMap<TServerInstance,TabletServerStatus>());
  
  private final Set<String> recoveriesInProgress = Collections.synchronizedSet(new HashSet<String>());
  
  synchronized private MasterState getMasterState() {
    return state;
  }
  
  public boolean stillMaster() {
    return getMasterState() != MasterState.STOP;
  }
  
  static final boolean X = true;
  static final boolean _ = false;
  // @formatter:off
  static final boolean transitionOK[][] = {
      //                              INITIAL HAVE_LOCK SAFE_MODE NORMAL UNLOAD_META UNLOAD_ROOT STOP
      /* INITIAL */                   {X,     X,        _,        _,      _,         _,          X},
      /* HAVE_LOCK */                 {_,     X,        X,        X,      _,         _,          X},
      /* SAFE_MODE */                 {_,     _,        X,        X,      X,         _,          X},
      /* NORMAL */                    {_,     _,        X,        X,      X,         _,          X},
      /* UNLOAD_METADATA_TABLETS */   {_,     _,        X,        X,      X,         X,          X},
      /* UNLOAD_ROOT_TABLET */        {_,     _,        _,        X,      _,         X,          X},
      /* STOP */                      {_,     _,        _,        _,      _,         _,          X}};
  //@formatter:on
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
  
  private void upgradeZookeeper() {
    if (Accumulo.getAccumuloPersistentVersion(fs) == ServerConstants.PREV_DATA_VERSION) {
      try {
        log.info("Upgrading zookeeper");
        
        IZooReaderWriter zoo = ZooReaderWriter.getInstance();
        
        zoo.recursiveDelete(ZooUtil.getRoot(instance) + "/loggers", NodeMissingPolicy.SKIP);
        zoo.recursiveDelete(ZooUtil.getRoot(instance) + "/dead/loggers", NodeMissingPolicy.SKIP);
        
        zoo.putPersistentData(ZooUtil.getRoot(instance) + Constants.ZRECOVERY, new byte[] {'0'}, NodeExistsPolicy.SKIP);
        
        for (String id : Tables.getIdToNameMap(instance).keySet()) {
          
          zoo.putPersistentData(ZooUtil.getRoot(instance) + Constants.ZTABLES + "/" + id + Constants.ZTABLE_COMPACT_CANCEL_ID, "0".getBytes(),
              NodeExistsPolicy.SKIP);
        }
      } catch (Exception ex) {
        log.fatal("Error performing upgrade", ex);
        System.exit(1);
      }
    }
  }
  
  private final AtomicBoolean upgradeMetadataRunning = new AtomicBoolean(false);
  
  private final ServerConfiguration serverConfig;
  
  private void upgradeMetadata() {
    if (Accumulo.getAccumuloPersistentVersion(fs) == ServerConstants.PREV_DATA_VERSION) {
      if (upgradeMetadataRunning.compareAndSet(false, true)) {
        Runnable upgradeTask = new Runnable() {
          @Override
          public void run() {
            try {
              MetadataTableUtil.moveMetaDeleteMarkers(instance, SystemCredentials.get().getAsThrift());
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
  private int displayUnassigned() {
    int result = 0;
    Text meta = new Text(MetadataTable.ID);
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
    if (MetadataTable.NAME.equals(tableName) || RootTable.NAME.equals(tableName)) {
      String why = "Table names cannot be == " + RootTable.NAME + " or " + MetadataTable.NAME;
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
    if (Tables.getNameToIdMap(HdfsZooInstance.getInstance()).containsKey(tableName)) {
      String why = "Table name already exists: " + tableName;
      throw new ThriftTableOperationException(null, tableName, operation, TableOperationExceptionType.EXISTS, why);
    }
    
  }
  
  public void mustBeOnline(final String tableId) throws ThriftTableOperationException {
    Tables.clearCache(instance);
    if (!Tables.getTableState(instance, tableId).equals(TableState.ONLINE))
      throw new ThriftTableOperationException(tableId, null, TableOperation.MERGE, TableOperationExceptionType.OFFLINE, "table is not online");
  }
  
  public Connector getConnector() throws AccumuloException, AccumuloSecurityException {
    return instance.getConnector(SystemCredentials.get().getPrincipal(), SystemCredentials.get().getToken());
  }
  
  private void waitAround(EventCoordinator.Listener listener) {
    listener.waitForEvents(ONE_SECOND);
  }
  
  // TODO: maybe move this to Property? We do this in TabletServer, Master, TableLoadBalancer, etc. - ACCUMULO-1295
  public static <T> T createInstanceFromPropertyName(AccumuloConfiguration conf, Property property, Class<T> base, T defaultInstance) {
    String clazzName = conf.get(property);
    T instance = null;
    
    try {
      Class<? extends T> clazz = AccumuloVFSClassLoader.loadClass(clazzName, base);
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
  
  public Master(ServerConfiguration config, VolumeManager fs, String hostname) throws IOException {
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
      final String tableId = Tables.getNameToIdMap(getConfiguration().getInstance()).get(tableName);
      if (tableId == null)
        throw new ThriftTableOperationException(null, tableName, operation, TableOperationExceptionType.NOTFOUND, null);
      return tableId;
    }
    
    @Override
    public long initiateFlush(TInfo tinfo, TCredentials c, String tableId) throws ThriftSecurityException, ThriftTableOperationException, TException {
      security.canFlush(c, tableId);
      
      String zTablePath = Constants.ZROOT + "/" + getConfiguration().getInstance().getInstanceID() + Constants.ZTABLES + "/" + tableId
          + Constants.ZTABLE_FLUSH_ID;
      
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
    public void waitForFlush(TInfo tinfo, TCredentials c, String tableId, ByteBuffer startRow, ByteBuffer endRow, long flushID, long maxLoops)
        throws ThriftSecurityException, ThriftTableOperationException, TException {
      security.canFlush(c, tableId);
      
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
          Scanner scanner = new IsolatedScanner(conn.createScanner(MetadataTable.NAME, Authorizations.EMPTY));
          TabletsSection.ServerColumnFamily.FLUSH_COLUMN.fetch(scanner);
          TabletsSection.ServerColumnFamily.DIRECTORY_COLUMN.fetch(scanner);
          scanner.fetchColumnFamily(TabletsSection.CurrentLocationColumnFamily.NAME);
          scanner.fetchColumnFamily(LogColumnFamily.NAME);
          scanner.setRange(new KeyExtent(new Text(tableId), null, ByteBufferUtil.toText(startRow)).toMetadataRange());
          
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
              
              if (TabletsSection.ServerColumnFamily.FLUSH_COLUMN.equals(key.getColumnFamily(), key.getColumnQualifier())) {
                tabletFlushID = Long.parseLong(entry.getValue().toString());
              }
              
              if (LogColumnFamily.NAME.equals(key.getColumnFamily()))
                logs++;
              
              if (TabletsSection.CurrentLocationColumnFamily.NAME.equals(key.getColumnFamily())) {
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
          
          // TODO detect case of table offline AND tablets w/ logs? - ACCUMULO-1296
          
          if (tabletCount == 0 && !Tables.exists(instance, tableId))
            throw new ThriftTableOperationException(tableId, null, TableOperation.FLUSH, TableOperationExceptionType.NOTFOUND, null);
          
        } catch (AccumuloException e) {
          log.debug("Failed to scan " + MetadataTable.NAME + " table to wait for flush " + tableId, e);
        } catch (TabletDeletedException tde) {
          log.debug("Failed to scan " + MetadataTable.NAME + " table to wait for flush " + tableId, tde);
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
    public MasterMonitorInfo getMasterStats(TInfo info, TCredentials credentials) throws ThriftSecurityException, TException {
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
    
    private void alterTableProperty(TCredentials c, String tableName, String property, String value, TableOperation op) throws ThriftSecurityException,
        ThriftTableOperationException {
      final String tableId = checkTableId(tableName, op);
      if (!security.canAlterTable(c, tableId))
        throw new ThriftSecurityException(c.getPrincipal(), SecurityErrorCode.PERMISSION_DENIED);
      
      try {
        if (value == null) {
          TablePropUtil.removeTableProperty(tableId, property);
        } else if (!TablePropUtil.setTableProperty(tableId, property, value)) {
          throw new Exception("Invalid table property.");
        }
      } catch (KeeperException.NoNodeException e) {
        // race condition... table no longer exists? This call will throw an exception if the table was deleted:
        checkTableId(tableName, op);
        log.info("Error altering table property", e);
        throw new ThriftTableOperationException(tableId, tableName, op, TableOperationExceptionType.OTHER, "Problem altering table property");
      } catch (Exception e) {
        log.error("Problem altering table property", e);
        throw new ThriftTableOperationException(tableId, tableName, op, TableOperationExceptionType.OTHER, "Problem altering table property");
      }
    }
    
    @Override
    public void removeTableProperty(TInfo info, TCredentials credentials, String tableName, String property) throws ThriftSecurityException,
        ThriftTableOperationException, TException {
      alterTableProperty(credentials, tableName, property, null, TableOperation.REMOVE_PROPERTY);
    }
    
    @Override
    public void setTableProperty(TInfo info, TCredentials credentials, String tableName, String property, String value) throws ThriftSecurityException,
        ThriftTableOperationException, TException {
      alterTableProperty(credentials, tableName, property, value, TableOperation.SET_PROPERTY);
    }
    
    @Override
    public void shutdown(TInfo info, TCredentials c, boolean stopTabletServers) throws ThriftSecurityException, TException {
      security.canPerformSystemActions(c);
      Master.this.shutdown(stopTabletServers);
    }
    
    @Override
    public void shutdownTabletServer(TInfo info, TCredentials c, String tabletServer, boolean force) throws ThriftSecurityException, TException {
      security.canPerformSystemActions(c);
      
      final InetSocketAddress addr = AddressUtil.parseAddress(tabletServer);
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
    public void reportSplitExtent(TInfo info, TCredentials credentials, String serverName, TabletSplit split) throws TException {
      KeyExtent oldTablet = new KeyExtent(split.oldTablet);
      if (migrations.remove(oldTablet) != null) {
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
    public void reportTabletStatus(TInfo info, TCredentials credentials, String serverName, TabletLoadState status, TKeyExtent ttablet) throws TException {
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
    public void setMasterGoalState(TInfo info, TCredentials c, MasterGoalState state) throws ThriftSecurityException, TException {
      security.canPerformSystemActions(c);
      
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
    public void removeSystemProperty(TInfo info, TCredentials c, String property) throws ThriftSecurityException, TException {
      security.canPerformSystemActions(c);
      
      try {
        SystemPropUtil.removeSystemProperty(property);
        updatePlugins(property);
      } catch (Exception e) {
        log.error("Problem removing config property in zookeeper", e);
        throw new TException(e.getMessage());
      }
    }
    
    @Override
    public void setSystemProperty(TInfo info, TCredentials c, String property, String value) throws ThriftSecurityException, TException {
      security.canPerformSystemActions(c);
      
      try {
        SystemPropUtil.setSystemProperty(property, value);
        updatePlugins(property);
      } catch (Exception e) {
        log.error("Problem setting config property in zookeeper", e);
        throw new TException(e.getMessage());
      }
    }
    
    private void authenticate(TCredentials c) throws ThriftSecurityException {
      if (!security.authenticateUser(c, c))
        throw new ThriftSecurityException(c.getPrincipal(), SecurityErrorCode.BAD_CREDENTIALS);
      
    }
    
    @Override
    public long beginTableOperation(TInfo tinfo, TCredentials credentials) throws ThriftSecurityException, TException {
      authenticate(credentials);
      return fate.startTransaction();
    }
    
    @Override
    public void executeTableOperation(TInfo tinfo, TCredentials c, long opid, org.apache.accumulo.core.master.thrift.TableOperation op,
        List<ByteBuffer> arguments, Map<String,String> options, boolean autoCleanup) throws ThriftSecurityException, ThriftTableOperationException, TException {
      authenticate(c);
      
      switch (op) {
        case CREATE: {
          String tableName = ByteBufferUtil.toString(arguments.get(0));
          if (!security.canCreateTable(c, tableName))
            throw new ThriftSecurityException(c.getPrincipal(), SecurityErrorCode.PERMISSION_DENIED);
          checkNotMetadataTable(tableName, TableOperation.CREATE);
          checkTableName(tableName, TableOperation.CREATE);
          
          org.apache.accumulo.core.client.admin.TimeType timeType = org.apache.accumulo.core.client.admin.TimeType.valueOf(ByteBufferUtil.toString(arguments
              .get(1)));
          fate.seedTransaction(opid, new TraceRepo<Master>(new CreateTable(c.getPrincipal(), tableName, timeType, options)), autoCleanup);
          
          break;
        }
        case RENAME: {
          String oldTableName = ByteBufferUtil.toString(arguments.get(0));
          String newTableName = ByteBufferUtil.toString(arguments.get(1));
          
          String tableId = checkTableId(oldTableName, TableOperation.RENAME);
          checkNotMetadataTable(oldTableName, TableOperation.RENAME);
          checkNotMetadataTable(newTableName, TableOperation.RENAME);
          checkTableName(newTableName, TableOperation.RENAME);
          if (!security.canRenameTable(c, tableId, oldTableName, newTableName))
            throw new ThriftSecurityException(c.getPrincipal(), SecurityErrorCode.PERMISSION_DENIED);
          
          fate.seedTransaction(opid, new TraceRepo<Master>(new RenameTable(tableId, oldTableName, newTableName)), autoCleanup);
          
          break;
        }
        case CLONE: {
          String srcTableId = ByteBufferUtil.toString(arguments.get(0));
          String tableName = ByteBufferUtil.toString(arguments.get(1));
          checkNotMetadataTable(tableName, TableOperation.CLONE);
          checkTableName(tableName, TableOperation.CLONE);
          if (!security.canCloneTable(c, srcTableId, tableName))
            throw new ThriftSecurityException(c.getPrincipal(), SecurityErrorCode.PERMISSION_DENIED);
          
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
          
          fate.seedTransaction(opid, new TraceRepo<Master>(new CloneTable(c.getPrincipal(), srcTableId, tableName, propertiesToSet, propertiesToExclude)),
              autoCleanup);
          
          break;
        }
        case DELETE: {
          String tableName = ByteBufferUtil.toString(arguments.get(0));
          final String tableId = checkTableId(tableName, TableOperation.DELETE);
          checkNotMetadataTable(tableName, TableOperation.DELETE);
          if (!security.canDeleteTable(c, tableId))
            throw new ThriftSecurityException(c.getPrincipal(), SecurityErrorCode.PERMISSION_DENIED);
          
          fate.seedTransaction(opid, new TraceRepo<Master>(new DeleteTable(tableId)), autoCleanup);
          break;
        }
        case ONLINE: {
          String tableName = ByteBufferUtil.toString(arguments.get(0));
          final String tableId = checkTableId(tableName, TableOperation.ONLINE);
          checkNotMetadataTable(tableName, TableOperation.ONLINE);
          
          if (!security.canOnlineOfflineTable(c, tableId, op))
            throw new ThriftSecurityException(c.getPrincipal(), SecurityErrorCode.PERMISSION_DENIED);
          
          fate.seedTransaction(opid, new TraceRepo<Master>(new ChangeTableState(tableId, TableOperation.ONLINE)), autoCleanup);
          break;
        }
        case OFFLINE: {
          String tableName = ByteBufferUtil.toString(arguments.get(0));
          final String tableId = checkTableId(tableName, TableOperation.OFFLINE);
          checkNotMetadataTable(tableName, TableOperation.OFFLINE);
          
          if (!security.canOnlineOfflineTable(c, tableId, op))
            throw new ThriftSecurityException(c.getPrincipal(), SecurityErrorCode.PERMISSION_DENIED);
          
          fate.seedTransaction(opid, new TraceRepo<Master>(new ChangeTableState(tableId, TableOperation.OFFLINE)), autoCleanup);
          break;
        }
        case MERGE: {
          String tableName = ByteBufferUtil.toString(arguments.get(0));
          Text startRow = ByteBufferUtil.toText(arguments.get(1));
          Text endRow = ByteBufferUtil.toText(arguments.get(2));
          final String tableId = checkTableId(tableName, TableOperation.MERGE);
          log.debug("Creating merge op: " + tableId + " " + startRow + " " + endRow);
          
          if (!security.canMerge(c, tableId))
            throw new ThriftSecurityException(c.getPrincipal(), SecurityErrorCode.PERMISSION_DENIED);
          
          fate.seedTransaction(opid, new TraceRepo<Master>(new TableRangeOp(MergeInfo.Operation.MERGE, tableId, startRow, endRow)), autoCleanup);
          break;
        }
        case DELETE_RANGE: {
          String tableName = ByteBufferUtil.toString(arguments.get(0));
          Text startRow = ByteBufferUtil.toText(arguments.get(1));
          Text endRow = ByteBufferUtil.toText(arguments.get(2));
          
          final String tableId = checkTableId(tableName, TableOperation.DELETE_RANGE);
          checkNotMetadataTable(tableName, TableOperation.DELETE_RANGE);
          
          if (!security.canDeleteRange(c, tableId, tableName, startRow, endRow))
            throw new ThriftSecurityException(c.getPrincipal(), SecurityErrorCode.PERMISSION_DENIED);
          
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
          
          if (!security.canBulkImport(c, tableId, tableName, dir, failDir))
            throw new ThriftSecurityException(c.getPrincipal(), SecurityErrorCode.PERMISSION_DENIED);
          
          fate.seedTransaction(opid, new TraceRepo<Master>(new BulkImport(tableId, dir, failDir, setTime)), autoCleanup);
          break;
        }
        case COMPACT: {
          String tableId = ByteBufferUtil.toString(arguments.get(0));
          byte[] startRow = ByteBufferUtil.toBytes(arguments.get(1));
          byte[] endRow = ByteBufferUtil.toBytes(arguments.get(2));
          List<IteratorSetting> iterators = IteratorUtil.decodeIteratorSettings(ByteBufferUtil.toBytes(arguments.get(3)));
          
          if (!security.canCompact(c, tableId))
            throw new ThriftSecurityException(c.getPrincipal(), SecurityErrorCode.PERMISSION_DENIED);
          
          fate.seedTransaction(opid, new TraceRepo<Master>(new CompactRange(tableId, startRow, endRow, iterators)), autoCleanup);
          break;
        }
        case COMPACT_CANCEL: {
          String tableId = ByteBufferUtil.toString(arguments.get(0));
          
          if (!security.canCompact(c, tableId))
            throw new ThriftSecurityException(c.getPrincipal(), SecurityErrorCode.PERMISSION_DENIED);
          
          fate.seedTransaction(opid, new TraceRepo<Master>(new CancelCompactions(tableId)), autoCleanup);
          break;
        }
        case IMPORT: {
          String tableName = ByteBufferUtil.toString(arguments.get(0));
          String exportDir = ByteBufferUtil.toString(arguments.get(1));
          
          if (!security.canImport(c, tableName, exportDir))
            throw new ThriftSecurityException(c.getPrincipal(), SecurityErrorCode.PERMISSION_DENIED);
          
          checkNotMetadataTable(tableName, TableOperation.CREATE);
          checkTableName(tableName, TableOperation.CREATE);
          
          fate.seedTransaction(opid, new TraceRepo<Master>(new ImportTable(c.getPrincipal(), tableName, exportDir)), autoCleanup);
          break;
        }
        case EXPORT: {
          String tableName = ByteBufferUtil.toString(arguments.get(0));
          String exportDir = ByteBufferUtil.toString(arguments.get(1));
          
          String tableId = checkTableId(tableName, TableOperation.EXPORT);
          
          if (!security.canExport(c, tableId, tableName, exportDir))
            throw new ThriftSecurityException(c.getPrincipal(), SecurityErrorCode.PERMISSION_DENIED);
          
          checkNotMetadataTable(tableName, TableOperation.EXPORT);
          
          fate.seedTransaction(opid, new TraceRepo<Master>(new ExportTable(tableName, tableId, exportDir)), autoCleanup);
          break;
        }
        
        default:
          throw new UnsupportedOperationException();
      }
      
    }
    
    @Override
    public String waitForTableOperation(TInfo tinfo, TCredentials credentials, long opid) throws ThriftSecurityException, ThriftTableOperationException,
        TException {
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
    public void finishTableOperation(TInfo tinfo, TCredentials credentials, long opid) throws ThriftSecurityException, TException {
      authenticate(credentials);
      fate.delete(opid);
    }
  }
  
  public MergeInfo getMergeInfo(KeyExtent tablet) {
    return getMergeInfo(tablet.getTableId());
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
  }
  
  private class StatusThread extends Daemon {
    
    @Override
    public void run() {
      setName("Status Thread");
      EventCoordinator.Listener eventListener = nextEvent.getListener();
      while (stillMaster()) {
        int count = 0;
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
                case SAFE_MODE:
                  count = nonMetaDataTabletsAssignedOrHosted();
                  log.debug(String.format("There are %d non-metadata tablets assigned or hosted", count));
                  if (count == 0)
                    setMasterState(MasterState.UNLOAD_METADATA_TABLETS);
                  break;
                case UNLOAD_METADATA_TABLETS:
                  count = assignedOrHosted(METADATA_TABLE_ID);
                  count += assignedOrHosted(new Text(RootTable.ID));
                  log.debug(String.format("There are %d metadata tablets assigned or hosted", count));
                  // Assumes last tablet hosted is the root tablet;
                  // it's possible
                  // that's not the case (root tablet is offline?)
                  if (count == 1)
                    setMasterState(MasterState.UNLOAD_ROOT_TABLET);
                  break;
                case UNLOAD_ROOT_TABLET:
                  count = assignedOrHosted(METADATA_TABLE_ID);
                  count += assignedOrHosted(new Text(RootTable.ID));
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
    
    // TODO: add shutdown for fate object - ACCUMULO-1307
    try {
      final AgeOffStore<Master> store = new AgeOffStore<Master>(new org.apache.accumulo.fate.ZooStore<Master>(ZooUtil.getRoot(instance) + Constants.ZFATE,
          ZooReaderWriter.getRetryingInstance()), 1000 * 60 * 60 * 8);
      
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
    
    TCredentials systemAuths = SystemCredentials.get().getAsThrift();
    watchers.add(new TabletGroupWatcher(this, new MetaDataStateStore(instance, systemAuths, this), null));
    watchers.add(new TabletGroupWatcher(this, new RootTabletStateStore(instance, systemAuths, this), watchers.get(0)));
    watchers.add(new TabletGroupWatcher(this, new ZooTabletStateStore(new ZooStore(zroot)), watchers.get(1)));
    for (TabletGroupWatcher watcher : watchers) {
      watcher.start();
    }
    
    Processor<Iface> processor = new Processor<Iface>(TraceWrap.service(new MasterClientServiceHandler()));
    clientService = TServerUtils.startServer(getSystemConfiguration(), Property.MASTER_CLIENTPORT, processor, "Master", "Master Client Service Handler", null,
        Property.MASTER_MINTHREADS, Property.MASTER_THREADCHECK, Property.GENERAL_MAX_MESSAGE_SIZE).server;
    
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
    
    final String masterClientAddress = org.apache.accumulo.core.util.AddressUtil.toString(new InetSocketAddress(hostname, getSystemConfiguration().getPort(
        Property.MASTER_CLIENTPORT)));
    
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
      SecurityUtil.serverLogin();
      
      VolumeManager fs = VolumeManagerImpl.get();
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
  
  public VolumeManager getFileSystem() {
    return this.fs;
  }
  
  public void updateRecoveryInProgress(String file) {
    recoveriesInProgress.add(file);
  }
}

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
package org.apache.accumulo.server.master.state.tables;

import java.security.SecurityPermission;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.master.state.tables.TableState;
import org.apache.accumulo.core.zookeeper.ZooUtil;
import org.apache.accumulo.core.zookeeper.ZooUtil.NodeExistsPolicy;
import org.apache.accumulo.core.zookeeper.ZooUtil.NodeMissingPolicy;
import org.apache.accumulo.server.client.HdfsZooInstance;
import org.apache.accumulo.server.util.TablePropUtil;
import org.apache.accumulo.server.zookeeper.IZooReaderWriter;
import org.apache.accumulo.server.zookeeper.ZooCache;
import org.apache.accumulo.server.zookeeper.ZooReaderWriter;
import org.apache.accumulo.server.zookeeper.ZooReaderWriter.Mutator;
import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;

public class TableManager {
  private static SecurityPermission TABLE_MANAGER_PERMISSION = new SecurityPermission("tableManagerPermission");
  
  private static final Logger log = Logger.getLogger(TableManager.class);
  private static final Set<TableObserver> observers = Collections.synchronizedSet(new HashSet<TableObserver>());
  private static final Map<String,TableState> tableStateCache = Collections.synchronizedMap(new HashMap<String,TableState>());
  
  private static TableManager tableManager = null;
  
  private final Instance instance;
  private ZooCache zooStateCache;
  
  public static void prepareNewTableState(String instanceId, String tableId, String tableName, TableState state, NodeExistsPolicy existsPolicy)
      throws KeeperException, InterruptedException {
    // state gets created last
    String zTablePath = Constants.ZROOT + "/" + instanceId + Constants.ZTABLES + "/" + tableId;
    IZooReaderWriter zoo = ZooReaderWriter.getRetryingInstance();
    zoo.putPersistentData(zTablePath, new byte[0], existsPolicy);
    zoo.putPersistentData(zTablePath + Constants.ZTABLE_CONF, new byte[0], existsPolicy);
    zoo.putPersistentData(zTablePath + Constants.ZTABLE_NAME, tableName.getBytes(), existsPolicy);
    zoo.putPersistentData(zTablePath + Constants.ZTABLE_STATE, state.name().getBytes(), existsPolicy);
    zoo.putPersistentData(zTablePath + Constants.ZTABLE_FLUSH_ID, "0".getBytes(), existsPolicy);
    zoo.putPersistentData(zTablePath + Constants.ZTABLE_COMPACT_ID, "0".getBytes(), existsPolicy);
  }
  
  public synchronized static TableManager getInstance() {
    SecurityManager sm = System.getSecurityManager();
    if (sm != null) {
      sm.checkPermission(TABLE_MANAGER_PERMISSION);
    }
    if (tableManager == null)
      tableManager = new TableManager();
    return tableManager;
  }
  
  private TableManager() {
    instance = HdfsZooInstance.getInstance();
    zooStateCache = new ZooCache(new TableStateWatcher());
    updateTableStateCache();
  }
  
  public TableState getTableState(String tableId) {
    return tableStateCache.get(tableId);
  }
  
  public class IllegalTableTransitionException extends Exception {
    private static final long serialVersionUID = 1L;
    
    final TableState oldState;
    final TableState newState;
    
    public IllegalTableTransitionException(TableState oldState, TableState newState) {
      this.oldState = oldState;
      this.newState = newState;
    }
    
    public TableState getOldState() {
      return oldState;
    }
    
    public TableState getNewState() {
      return newState;
    }
    
  }
  
  public synchronized void transitionTableState(final String tableId, final TableState newState) {
    String statePath = ZooUtil.getRoot(HdfsZooInstance.getInstance()) + Constants.ZTABLES + "/" + tableId + Constants.ZTABLE_STATE;
    
    try {
      ZooReaderWriter.getRetryingInstance().mutate(statePath, (byte[]) newState.name().getBytes(), ZooUtil.PUBLIC, new Mutator() {
        @Override
        public byte[] mutate(byte[] oldData) throws Exception {
          TableState oldState = TableState.UNKNOWN;
          if (oldData != null)
            oldState = TableState.valueOf(new String(oldData));
          boolean transition = true;
          // +--------+
          // v |
          // NEW -> (ONLINE|OFFLINE)+--- DELETING
          switch (oldState) {
            case NEW:
              transition = (newState == TableState.OFFLINE || newState == TableState.ONLINE);
              break;
            case ONLINE: // fall-through intended
            case UNKNOWN:// fall through intended
            case OFFLINE:
              transition = (newState != TableState.NEW);
              break;
            case DELETING:
              // Can't transition to any state from DELETING
              transition = false;
              break;
          }
          if (!transition)
            throw new IllegalTableTransitionException(oldState, newState);
          log.debug("Transitioning state for table " + tableId + " from " + oldState + " to " + newState);
          return newState.name().getBytes();
        }
      });
    } catch (Exception e) {
      log.fatal("Failed to transition table to state " + newState);
      throw new RuntimeException(e);
    }
  }
  
  private void updateTableStateCache() {
    synchronized (tableStateCache) {
      for (String tableId : zooStateCache.getChildren(ZooUtil.getRoot(instance) + Constants.ZTABLES))
        if (zooStateCache.get(ZooUtil.getRoot(instance) + Constants.ZTABLES + "/" + tableId + Constants.ZTABLE_STATE) != null)
          updateTableStateCache(tableId);
    }
  }
  
  public TableState updateTableStateCache(String tableId) {
    synchronized (tableStateCache) {
      TableState tState = TableState.UNKNOWN;
      byte[] data = zooStateCache.get(ZooUtil.getRoot(instance) + Constants.ZTABLES + "/" + tableId + Constants.ZTABLE_STATE);
      if (data != null) {
        String sState = new String(data);
        try {
          tState = TableState.valueOf(sState);
        } catch (IllegalArgumentException e) {
          log.error("Unrecognized state for table with tableId=" + tableId + ": " + sState);
        }
        tableStateCache.put(tableId, tState);
      }
      return tState;
    }
  }
  
  public void addTable(String tableId, String tableName, NodeExistsPolicy existsPolicy) throws KeeperException, InterruptedException {
    prepareNewTableState(instance.getInstanceID(), tableId, tableName, TableState.NEW, existsPolicy);
    updateTableStateCache(tableId);
  }
  
  public void cloneTable(String srcTable, String tableId, String tableName, Map<String,String> propertiesToSet, Set<String> propertiesToExclude,
      NodeExistsPolicy existsPolicy) throws KeeperException, InterruptedException {
    prepareNewTableState(instance.getInstanceID(), tableId, tableName, TableState.NEW, existsPolicy);
    String srcTablePath = Constants.ZROOT + "/" + instance.getInstanceID() + Constants.ZTABLES + "/" + srcTable + Constants.ZTABLE_CONF;
    String newTablePath = Constants.ZROOT + "/" + instance.getInstanceID() + Constants.ZTABLES + "/" + tableId + Constants.ZTABLE_CONF;
    ZooReaderWriter.getRetryingInstance().recursiveCopyPersistent(srcTablePath, newTablePath, NodeExistsPolicy.OVERWRITE);
    
    for (Entry<String,String> entry : propertiesToSet.entrySet())
      TablePropUtil.setTableProperty(tableId, entry.getKey(), entry.getValue());
    
    for (String prop : propertiesToExclude)
      ZooReaderWriter.getRetryingInstance().recursiveDelete(
          Constants.ZROOT + "/" + instance.getInstanceID() + Constants.ZTABLES + "/" + tableId + Constants.ZTABLE_CONF + "/" + prop, NodeMissingPolicy.SKIP);
    
    updateTableStateCache(tableId);
  }
  
  public void removeTable(String tableId) throws KeeperException, InterruptedException {
    synchronized (tableStateCache) {
      tableStateCache.remove(tableId);
      ZooReaderWriter.getRetryingInstance().recursiveDelete(ZooUtil.getRoot(instance) + Constants.ZTABLES + "/" + tableId + Constants.ZTABLE_STATE,
          NodeMissingPolicy.SKIP);
      ZooReaderWriter.getRetryingInstance().recursiveDelete(ZooUtil.getRoot(instance) + Constants.ZTABLES + "/" + tableId, NodeMissingPolicy.SKIP);
    }
  }
  
  public boolean addObserver(TableObserver to) {
    synchronized (observers) {
      synchronized (tableStateCache) {
        to.initialize(Collections.unmodifiableMap(tableStateCache));
        return observers.add(to);
      }
    }
  }
  
  public boolean removeObserver(TableObserver to) {
    return observers.remove(to);
  }
  
  private class TableStateWatcher implements Watcher {
    @Override
    public void process(WatchedEvent event) {
      if (log.isTraceEnabled())
        log.trace(event);
      
      final String zPath = event.getPath();
      final EventType zType = event.getType();
      
      String tablesPrefix = ZooUtil.getRoot(instance) + Constants.ZTABLES;
      String tableId = null;
      
      if (zPath != null && zPath.startsWith(tablesPrefix + "/")) {
        String suffix = zPath.substring(tablesPrefix.length() + 1);
        if (suffix.contains("/")) {
          String[] sa = suffix.split("/", 2);
          if (Constants.ZTABLE_STATE.equals("/" + sa[1]))
            tableId = sa[0];
        }
        if (tableId == null) {
          log.warn("Unknown path in " + event);
          return;
        }
      }
      
      switch (zType) {
        case NodeChildrenChanged:
          if (zPath != null && zPath.equals(tablesPrefix)) {
            updateTableStateCache();
          } else {
            log.warn("Unexpected path " + zPath);
          }
          break;
        case NodeCreated:
        case NodeDataChanged:
          // state transition
          TableState tState = updateTableStateCache(tableId);
          log.debug("State transition to " + tState + " @ " + event);
          synchronized (observers) {
            for (TableObserver to : observers)
              to.stateChanged(tableId, tState);
          }
          break;
        case NodeDeleted:
          if (zPath != null
              && tableId != null
              && (zPath.equals(tablesPrefix + "/" + tableId + Constants.ZTABLE_STATE) || zPath.equals(tablesPrefix + "/" + tableId + Constants.ZTABLE_CONF) || zPath
                  .equals(tablesPrefix + "/" + tableId + Constants.ZTABLE_NAME)))
            tableStateCache.remove(tableId);
          break;
        case None:
          switch (event.getState()) {
            case Expired:
              if (log.isTraceEnabled())
                log.trace("Session expired " + event);
              synchronized (observers) {
                for (TableObserver to : observers)
                  to.sessionExpired();
              }
              break;
            case SyncConnected:
            default:
              if (log.isTraceEnabled())
                log.trace("Ignored " + event);
          }
          break;
        default:
          log.warn("Unandled " + event);
      }
    }
  }
  
  /*
   * private static boolean verifyTabletAssignments(String tableId) { log.info( "Sending message to load balancer to verify assignment of tablets with tableId="
   * + tableId); // Return true only if transitions to other states did not interrupt // this process. (like deleting the table) return true; }
   * 
   * private static synchronized boolean unloadTable(String tableId) { int loadedTabletCount = 0; while (loadedTabletCount > 0) { // wait for tables to be
   * unloaded } log.info("Table unloaded. tableId=" + tableId); return true; }
   * 
   * private static void cleanupDeletedTable(String tableId) { log.info("Sending message to cleanup the deleted table with tableId=" + tableId); }
   * 
   * switch (tState) { case NEW: // this should really only happen before the watcher // knows about the table log.error("Unexpected transition to " + tState +
   * " @ " + event); break;
   * 
   * case LOADING: // a table has started coming online or has pending // migrations (maybe?) if (verifyTabletAssignments(tableId))
   * TableState.transition(instance, tableId, TableState.ONLINE); break; case ONLINE: log.trace("Table online with tableId=" + tableId); break;
   * 
   * case DISABLING: if (unloadTable(tableId)) TableState.transition(instance, tableId, TableState.DISABLED); break; case DISABLED:
   * log.trace("Table disabled with tableId=" + tableId); break;
   * 
   * case UNLOADING: unloadTable(tableId); TableState.transition(instance, tableId, TableState.OFFLINE); case OFFLINE: break;
   * 
   * case DELETING: unloadTable(tableId); cleanupDeletedTable(tableId); break;
   * 
   * default: log.error("Unrecognized transition to " + tState + " @ " + event); }
   */
}

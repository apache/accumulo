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
package org.apache.accumulo.server.tables;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.NamespaceNotFoundException;
import org.apache.accumulo.core.data.InstanceId;
import org.apache.accumulo.core.data.NamespaceId;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.fate.zookeeper.ZooCache;
import org.apache.accumulo.core.fate.zookeeper.ZooReaderWriter;
import org.apache.accumulo.core.fate.zookeeper.ZooUtil.NodeExistsPolicy;
import org.apache.accumulo.core.fate.zookeeper.ZooUtil.NodeMissingPolicy;
import org.apache.accumulo.core.manager.state.tables.TableState;
import org.apache.accumulo.core.util.Pair;
import org.apache.accumulo.core.util.tables.TableNameUtil;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.conf.store.NamespacePropKey;
import org.apache.accumulo.server.conf.store.PropStore;
import org.apache.accumulo.server.conf.store.TablePropKey;
import org.apache.accumulo.server.util.PropUtil;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

public class TableManager {

  private static final Logger log = LoggerFactory.getLogger(TableManager.class);
  private static final Set<TableObserver> observers = Collections.synchronizedSet(new HashSet<>());
  private static final Map<TableId,TableState> tableStateCache =
      Collections.synchronizedMap(new HashMap<>());
  private static final byte[] ZERO_BYTE = {'0'};

  private final ServerContext context;
  private final String zkRoot;
  private final InstanceId instanceID;
  private final ZooReaderWriter zoo;
  private final ZooCache zooStateCache;

  public static void prepareNewNamespaceState(ZooReaderWriter zoo, final PropStore propStore,
      InstanceId instanceId, NamespaceId namespaceId, String namespace,
      NodeExistsPolicy existsPolicy) throws KeeperException, InterruptedException {
    log.debug("Creating ZooKeeper entries for new namespace {} (ID: {})", namespace, namespaceId);
    String zPath = Constants.ZROOT + "/" + instanceId + Constants.ZNAMESPACES + "/" + namespaceId;

    zoo.putPersistentData(zPath, new byte[0], existsPolicy);
    zoo.putPersistentData(zPath + Constants.ZNAMESPACE_NAME, namespace.getBytes(UTF_8),
        existsPolicy);
    var propKey = NamespacePropKey.of(instanceId, namespaceId);
    if (!propStore.exists(propKey)) {
      propStore.create(propKey, Map.of());
    }
  }

  public static void prepareNewNamespaceState(final ServerContext context, NamespaceId namespaceId,
      String namespace, NodeExistsPolicy existsPolicy)
      throws KeeperException, InterruptedException {
    prepareNewNamespaceState(context.getZooReaderWriter(), context.getPropStore(),
        context.getInstanceID(), namespaceId, namespace, existsPolicy);
  }

  public static void prepareNewTableState(ZooReaderWriter zoo, PropStore propStore,
      InstanceId instanceId, TableId tableId, NamespaceId namespaceId, String tableName,
      TableState state, NodeExistsPolicy existsPolicy)
      throws KeeperException, InterruptedException {
    // state gets created last
    log.debug("Creating ZooKeeper entries for new table {} (ID: {}) in namespace (ID: {})",
        tableName, tableId, namespaceId);
    Pair<String,String> qualifiedTableName = TableNameUtil.qualify(tableName);
    tableName = qualifiedTableName.getSecond();
    String zTablePath = Constants.ZROOT + "/" + instanceId + Constants.ZTABLES + "/" + tableId;
    zoo.putPersistentData(zTablePath, new byte[0], existsPolicy);
    zoo.putPersistentData(zTablePath + Constants.ZTABLE_NAMESPACE,
        namespaceId.canonical().getBytes(UTF_8), existsPolicy);
    zoo.putPersistentData(zTablePath + Constants.ZTABLE_NAME, tableName.getBytes(UTF_8),
        existsPolicy);
    zoo.putPersistentData(zTablePath + Constants.ZTABLE_FLUSH_ID, ZERO_BYTE, existsPolicy);
    zoo.putPersistentData(zTablePath + Constants.ZTABLE_COMPACT_ID, ZERO_BYTE, existsPolicy);
    zoo.putPersistentData(zTablePath + Constants.ZTABLE_COMPACT_CANCEL_ID, ZERO_BYTE, existsPolicy);
    zoo.putPersistentData(zTablePath + Constants.ZTABLE_STATE, state.name().getBytes(UTF_8),
        existsPolicy);
    var propKey = TablePropKey.of(instanceId, tableId);
    if (!propStore.exists(propKey)) {
      propStore.create(propKey, Map.of());
    }
  }

  public static void prepareNewTableState(final ServerContext context, TableId tableId,
      NamespaceId namespaceId, String tableName, TableState state, NodeExistsPolicy existsPolicy)
      throws KeeperException, InterruptedException {
    prepareNewTableState(context.getZooReaderWriter(), context.getPropStore(),
        context.getInstanceID(), tableId, namespaceId, tableName, state, existsPolicy);
  }

  public TableManager(ServerContext context) {
    this.context = context;
    zkRoot = context.getZooKeeperRoot();
    instanceID = context.getInstanceID();
    zoo = context.getZooReaderWriter();
    zooStateCache = new ZooCache(zoo, new TableStateWatcher());
    updateTableStateCache();
  }

  public TableState getTableState(TableId tableId) {
    return tableStateCache.get(tableId);
  }

  public synchronized void transitionTableState(final TableId tableId, final TableState newState) {
    Preconditions.checkArgument(newState != TableState.UNKNOWN);
    String statePath = zkRoot + Constants.ZTABLES + "/" + tableId + Constants.ZTABLE_STATE;

    try {
      zoo.mutateOrCreate(statePath, newState.name().getBytes(UTF_8), oldData -> {
        TableState oldState = TableState.UNKNOWN;
        if (oldData != null) {
          oldState = TableState.valueOf(new String(oldData, UTF_8));
        }

        // this check makes the transition operation idempotent
        if (oldState == newState) {
          return null; // already at desired state, so nothing to do
        }

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
        if (!transition) {
          throw new IllegalTableTransitionException(oldState, newState);
        }
        log.debug("Transitioning state for table {} from {} to {}", tableId, oldState, newState);
        return newState.name().getBytes(UTF_8);
      });
    } catch (Exception e) {
      log.error("FATAL Failed to transition table to state {}", newState);
      throw new RuntimeException(e);
    }
  }

  private void updateTableStateCache() {
    synchronized (tableStateCache) {
      for (String tableId : zooStateCache.getChildren(zkRoot + Constants.ZTABLES)) {
        if (zooStateCache.get(zkRoot + Constants.ZTABLES + "/" + tableId + Constants.ZTABLE_STATE)
            != null) {
          updateTableStateCache(TableId.of(tableId));
        }
      }
    }
  }

  public TableState updateTableStateCache(TableId tableId) {
    synchronized (tableStateCache) {
      TableState tState = TableState.UNKNOWN;
      byte[] data =
          zooStateCache.get(zkRoot + Constants.ZTABLES + "/" + tableId + Constants.ZTABLE_STATE);
      if (data != null) {
        String sState = new String(data, UTF_8);
        try {
          tState = TableState.valueOf(sState);
        } catch (IllegalArgumentException e) {
          log.error("Unrecognized state for table with tableId={}: {}", tableId, sState);
        }
        tableStateCache.put(tableId, tState);
      }
      return tState;
    }
  }

  public void addTable(TableId tableId, NamespaceId namespaceId, String tableName)
      throws KeeperException, InterruptedException, NamespaceNotFoundException {
    prepareNewTableState(zoo, context.getPropStore(), instanceID, tableId, namespaceId, tableName,
        TableState.NEW, NodeExistsPolicy.OVERWRITE);
    updateTableStateCache(tableId);
  }

  public void cloneTable(TableId srcTableId, TableId tableId, String tableName,
      NamespaceId namespaceId, Map<String,String> propertiesToSet, Set<String> propertiesToExclude)
      throws KeeperException, InterruptedException {
    prepareNewTableState(zoo, context.getPropStore(), instanceID, tableId, namespaceId, tableName,
        TableState.NEW, NodeExistsPolicy.OVERWRITE);

    String srcTablePath = Constants.ZROOT + "/" + instanceID + Constants.ZTABLES + "/" + srcTableId
        + Constants.ZCONFIG;
    String newTablePath =
        Constants.ZROOT + "/" + instanceID + Constants.ZTABLES + "/" + tableId + Constants.ZCONFIG;
    zoo.recursiveCopyPersistentOverwrite(srcTablePath, newTablePath);

    PropUtil.setProperties(context, TablePropKey.of(context, tableId), propertiesToSet);
    PropUtil.removeProperties(context, TablePropKey.of(context, tableId), propertiesToExclude);

    updateTableStateCache(tableId);
  }

  public void removeTable(TableId tableId) throws KeeperException, InterruptedException {
    synchronized (tableStateCache) {
      tableStateCache.remove(tableId);
      zoo.recursiveDelete(zkRoot + Constants.ZTABLES + "/" + tableId + Constants.ZTABLE_STATE,
          NodeMissingPolicy.SKIP);
      zoo.recursiveDelete(zkRoot + Constants.ZTABLES + "/" + tableId, NodeMissingPolicy.SKIP);
    }
  }

  public boolean addObserver(TableObserver to) {
    synchronized (observers) {
      synchronized (tableStateCache) {
        to.initialize();
        return observers.add(to);
      }
    }
  }

  private class TableStateWatcher implements Watcher {
    @Override
    public void process(WatchedEvent event) {
      if (log.isTraceEnabled()) {
        log.trace("{}", event);
      }
      final String zPath = event.getPath();
      final EventType zType = event.getType();

      String tablesPrefix = zkRoot + Constants.ZTABLES;
      TableId tableId = null;

      if (zPath != null && zPath.startsWith(tablesPrefix + "/")) {
        String suffix = zPath.substring(tablesPrefix.length() + 1);
        if (suffix.contains("/")) {
          String[] sa = suffix.split("/", 2);
          if (Constants.ZTABLE_STATE.equals("/" + sa[1])) {
            tableId = TableId.of(sa[0]);
          }
        }
        if (tableId == null) {
          log.warn("Unknown path in {}", event);
          return;
        }
      }

      switch (zType) {
        case NodeChildrenChanged:
          if (zPath != null && zPath.equals(tablesPrefix)) {
            updateTableStateCache();
          } else {
            log.warn("Unexpected path {} in {}", zPath, event);
          }
          break;
        case NodeCreated:
        case NodeDataChanged:
          // state transition
          TableState tState = updateTableStateCache(tableId);
          log.debug("State transition to {} @ {}", tState, event);
          synchronized (observers) {
            for (TableObserver to : observers) {
              to.stateChanged(tableId, tState);
            }
          }
          break;
        case NodeDeleted:
          if (zPath != null && tableId != null
              && (zPath.equals(tablesPrefix + "/" + tableId + Constants.ZTABLE_STATE)
                  || zPath.equals(tablesPrefix + "/" + tableId + Constants.ZCONFIG)
                  || zPath.equals(tablesPrefix + "/" + tableId + Constants.ZTABLE_NAME))) {
            tableStateCache.remove(tableId);
          }
          break;
        case None:
          switch (event.getState()) {
            case Expired:
              log.trace("Session expired; {}", event);
              synchronized (observers) {
                for (TableObserver to : observers) {
                  to.sessionExpired();
                }
              }
              break;
            case SyncConnected:
            default:
              log.trace("Ignored {}", event);
          }
          break;
        default:
          log.warn("Unhandled {}", event);
      }
    }
  }

  public void removeNamespace(NamespaceId namespaceId)
      throws KeeperException, InterruptedException {
    zoo.recursiveDelete(zkRoot + Constants.ZNAMESPACES + "/" + namespaceId, NodeMissingPolicy.SKIP);
  }

}

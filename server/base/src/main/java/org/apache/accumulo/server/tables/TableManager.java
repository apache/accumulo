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

import java.util.EnumSet;
import java.util.Map;
import java.util.Set;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.NamespaceNotFoundException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.clientImpl.AcceptableThriftTableOperationException;
import org.apache.accumulo.core.clientImpl.NamespaceMapping;
import org.apache.accumulo.core.clientImpl.thrift.TableOperationExceptionType;
import org.apache.accumulo.core.data.NamespaceId;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.fate.zookeeper.ZooReaderWriter;
import org.apache.accumulo.core.fate.zookeeper.ZooUtil.NodeExistsPolicy;
import org.apache.accumulo.core.fate.zookeeper.ZooUtil.NodeMissingPolicy;
import org.apache.accumulo.core.manager.state.tables.TableState;
import org.apache.accumulo.core.util.tables.TableMapping;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.conf.store.NamespacePropKey;
import org.apache.accumulo.server.conf.store.PropStore;
import org.apache.accumulo.server.conf.store.TablePropKey;
import org.apache.accumulo.server.util.PropUtil;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

public class TableManager {

  private static final Logger log = LoggerFactory.getLogger(TableManager.class);
  private static final byte[] ZERO_BYTE = {'0'};

  private final ServerContext context;
  private final ZooReaderWriter zoo;

  public TableManager(ServerContext context) {
    this.context = context;
    this.zoo = context.getZooSession().asReaderWriter();
  }

  public void prepareNewNamespaceState(NamespaceId namespaceId, String namespace,
      NodeExistsPolicy existsPolicy) throws KeeperException, InterruptedException {
    final PropStore propStore = context.getPropStore();
    log.debug("Creating ZooKeeper entries for new namespace {} (ID: {})", namespace, namespaceId);
    zoo.putPersistentData(Constants.ZNAMESPACES + "/" + namespaceId, new byte[0], existsPolicy);
    zoo.putPersistentData(TableMapping.getZTableMapPath(namespaceId),
        NamespaceMapping.serializeMap(Map.of()), existsPolicy);
    var propKey = NamespacePropKey.of(namespaceId);
    if (!propStore.exists(propKey)) {
      propStore.create(propKey, Map.of());
    }
  }

  public void prepareNewTableState(TableId tableId, NamespaceId namespaceId, String tableName,
      TableState state, NodeExistsPolicy existsPolicy)
      throws KeeperException, InterruptedException {
    // state gets created last
    log.debug("Creating ZooKeeper entries for new table {} (ID: {}) in namespace (ID: {})",
        tableName, tableId, namespaceId);
    String zTablePath =
        Constants.ZNAMESPACES + "/" + namespaceId + Constants.ZTABLES + "/" + tableId;
    zoo.putPersistentData(zTablePath, new byte[0], existsPolicy);
    zoo.putPersistentData(zTablePath + Constants.ZTABLE_FLUSH_ID, ZERO_BYTE, existsPolicy);
    zoo.putPersistentData(zTablePath + Constants.ZTABLE_STATE, state.name().getBytes(UTF_8),
        existsPolicy);
    var propKey = TablePropKey.of(tableId, namespaceId);
    var propStore = context.getPropStore();
    if (!propStore.exists(propKey)) {
      propStore.create(propKey, Map.of());
    }
  }

  public synchronized void transitionTableState(final TableId tableId,
      final NamespaceId namespaceId, final TableState newState,
      final EnumSet<TableState> expectedCurrStates) {
    Preconditions.checkArgument(newState != TableState.UNKNOWN);
    String statePath = Constants.ZNAMESPACES + "/" + namespaceId + Constants.ZTABLES + "/" + tableId
        + Constants.ZTABLE_STATE;

    try {
      zoo.mutateOrCreate(statePath, newState.name().getBytes(UTF_8), currData -> {
        TableState currState = TableState.UNKNOWN;
        if (currData != null) {
          currState = TableState.valueOf(new String(currData, UTF_8));
        }

        // this check makes the transition operation idempotent
        if (currState == newState) {
          return null; // already at desired state, so nothing to do
        }

        boolean transition = true;
        // +--------+
        // v |
        // NEW -> (ONLINE|OFFLINE)+--- DELETING
        switch (currState) {
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
        if (!transition || !expectedCurrStates.contains(currState)) {
          throw new IllegalTableTransitionException(currState, newState);
        }
        log.debug("Transitioning state for table {} from {} to {}", tableId, currState, newState);
        return newState.name().getBytes(UTF_8);
      });
    } catch (Exception e) {
      log.error("FATAL Failed to transition table to state {}", newState);
      throw new IllegalStateException(e);
    }
  }

  public TableState getTableState(TableId tableId) {
    TableState tState = TableState.UNKNOWN;
    NamespaceId namespaceId = null;
    try {
      namespaceId = context.getNamespaceId(tableId);
    } catch (TableNotFoundException e) {
      throw new IllegalStateException("Table not found in ZooKeeper: " + tableId);
    }
    byte[] data = context.getZooCache().get(Constants.ZNAMESPACES + "/" + namespaceId
        + Constants.ZTABLES + "/" + tableId + Constants.ZTABLE_STATE);
    if (data != null) {
      String sState = new String(data, UTF_8);
      try {
        tState = TableState.valueOf(sState);
      } catch (IllegalArgumentException e) {
        log.error("Unrecognized state for table with tableId={}: {}", tableId, sState);
      }
    }
    return tState;
  }

  public void addTable(TableId tableId, NamespaceId namespaceId, String tableName)
      throws KeeperException, InterruptedException, NamespaceNotFoundException {
    prepareNewTableState(tableId, namespaceId, tableName, TableState.NEW,
        NodeExistsPolicy.OVERWRITE);
  }

  public void cloneTable(TableId srcTableId, TableId tableId, String tableName,
                         NamespaceId srcNamespaceId, NamespaceId namespaceId, Map<String,String> propertiesToSet, Set<String> propertiesToExclude)
      throws KeeperException, InterruptedException {
    prepareNewTableState(tableId, namespaceId, tableName, TableState.NEW,
        NodeExistsPolicy.OVERWRITE);

    String srcTablePath = Constants.ZNAMESPACES + "/" + srcNamespaceId + Constants.ZTABLES + "/"
        + srcTableId + Constants.ZCONFIG;
    String newTablePath = Constants.ZNAMESPACES + "/" + namespaceId + Constants.ZTABLES + "/"
        + tableId + Constants.ZCONFIG;
    zoo.recursiveCopyPersistentOverwrite(srcTablePath, newTablePath);

    PropUtil.setProperties(context, TablePropKey.of(tableId, namespaceId), propertiesToSet);
    PropUtil.removeProperties(context, TablePropKey.of(tableId, namespaceId), propertiesToExclude);
  }

  public void removeTable(TableId tableId, NamespaceId namespaceId)
      throws KeeperException, InterruptedException, AcceptableThriftTableOperationException {
    try {
      context.getTableMapping(namespaceId).remove(tableId);
    } catch (AcceptableThriftTableOperationException e) {
      // ignore not found, because that's what we're trying to do anyway
      if (e.getType() != TableOperationExceptionType.NOTFOUND) {
        throw e;
      }
    }
    zoo.recursiveDelete(
        Constants.ZNAMESPACES + "/" + namespaceId + Constants.ZTABLES + "/" + tableId,
        NodeMissingPolicy.SKIP);
  }

  public void removeNamespace(NamespaceId namespaceId)
      throws KeeperException, InterruptedException, AcceptableThriftTableOperationException {
    try {
      context.getNamespaceMapping().remove(Constants.ZNAMESPACES, namespaceId);
    } catch (AcceptableThriftTableOperationException e) {
      // ignore not found, because that's what we're trying to do anyway
      if (e.getType() != TableOperationExceptionType.NAMESPACE_NOTFOUND) {
        throw e;
      }
    }
    zoo.recursiveDelete(Constants.ZNAMESPACES + "/" + namespaceId, NodeMissingPolicy.SKIP);
  }

}

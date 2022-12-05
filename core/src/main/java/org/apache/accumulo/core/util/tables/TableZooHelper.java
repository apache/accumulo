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
package org.apache.accumulo.core.util.tables;

import static com.google.common.base.Preconditions.checkArgument;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.apache.accumulo.core.util.Validators.EXISTING_TABLE_NAME;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutionException;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.NamespaceNotFoundException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.clientImpl.ClientContext;
import org.apache.accumulo.core.clientImpl.Namespaces;
import org.apache.accumulo.core.data.NamespaceId;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.fate.zookeeper.ZooCache;
import org.apache.accumulo.core.manager.state.tables.TableState;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

public class TableZooHelper implements AutoCloseable {

  private final ClientContext context;
  // Per instance cache will expire after 10 minutes in case we
  // encounter an instance not used frequently
  private final Cache<TableZooHelper,TableMap> instanceToMapCache =
      CacheBuilder.newBuilder().expireAfterAccess(10, MINUTES).build();

  public TableZooHelper(ClientContext context) {
    this.context = Objects.requireNonNull(context);
  }

  /**
   * Lookup table ID in ZK.
   *
   * @throws TableNotFoundException if not found; if the namespace was not found, this has a
   *         getCause() of NamespaceNotFoundException
   */
  public TableId getTableId(String tableName) throws TableNotFoundException {
    try {
      return _getTableIdDetectNamespaceNotFound(EXISTING_TABLE_NAME.validate(tableName));
    } catch (NamespaceNotFoundException e) {
      throw new TableNotFoundException(tableName, e);
    }
  }

  /**
   * Lookup table ID in ZK. If not found, clears cache and tries again.
   */
  public TableId _getTableIdDetectNamespaceNotFound(String tableName)
      throws NamespaceNotFoundException, TableNotFoundException {
    TableId tableId = getTableMap().getNameToIdMap().get(tableName);
    if (tableId == null) {
      // maybe the table exist, but the cache was not updated yet...
      // so try to clear the cache and check again
      clearTableListCache();
      tableId = getTableMap().getNameToIdMap().get(tableName);
      if (tableId == null) {
        String namespace = TableNameUtil.qualify(tableName).getFirst();
        if (Namespaces.getNameToIdMap(context).containsKey(namespace)) {
          throw new TableNotFoundException(null, tableName, null);
        } else {
          throw new NamespaceNotFoundException(null, namespace, null);
        }
      }
    }
    return tableId;
  }

  public String getTableName(TableId tableId) throws TableNotFoundException {
    String tableName = getTableMap().getIdtoNameMap().get(tableId);
    if (tableName == null) {
      throw new TableNotFoundException(tableId.canonical(), null, null);
    }
    return tableName;
  }

  /**
   * Get the TableMap from the cache. A new one will be populated when needed. Cache is cleared
   * manually by calling {@link #clearTableListCache()}
   */
  public TableMap getTableMap() {
    final ZooCache zc = context.getZooCache();
    TableMap map = getCachedTableMap();
    if (!map.isCurrent(zc)) {
      instanceToMapCache.invalidateAll();
      map = getCachedTableMap();
    }
    return map;
  }

  private TableMap getCachedTableMap() {
    try {
      return instanceToMapCache.get(this, () -> new TableMap(context));
    } catch (ExecutionException e) {
      throw new RuntimeException(e);
    }
  }

  public boolean tableNodeExists(TableId tableId) {
    ZooCache zc = context.getZooCache();
    List<String> tableIds = zc.getChildren(context.getZooKeeperRoot() + Constants.ZTABLES);
    return tableIds.contains(tableId.canonical());
  }

  public void clearTableListCache() {
    context.getZooCache().clear(context.getZooKeeperRoot() + Constants.ZTABLES);
    context.getZooCache().clear(context.getZooKeeperRoot() + Constants.ZNAMESPACES);
    instanceToMapCache.invalidateAll();
  }

  public String getPrintableTableInfoFromId(TableId tableId) {
    try {
      return _printableTableInfo(getTableName(tableId), tableId);
    } catch (TableNotFoundException e) {
      return _printableTableInfo(null, tableId);
    }
  }

  public String getPrintableTableInfoFromName(String tableName) {
    try {
      return _printableTableInfo(tableName, getTableId(tableName));
    } catch (TableNotFoundException e) {
      return _printableTableInfo(tableName, null);
    }
  }

  private String _printableTableInfo(String tableName, TableId tableId) {
    return String.format("%s(ID:%s)", tableName == null ? "?" : tableName,
        tableId == null ? "?" : tableId.canonical());
  }

  /**
   * Get the current state of the table using the tableid. The boolean clearCache, if true will
   * clear the table state in zookeeper before fetching the state. Added with ACCUMULO-4574.
   *
   * @param tableId the table id
   * @param clearCachedState if true clear the table state in zookeeper before checking status
   * @return the table state.
   */
  public TableState getTableState(TableId tableId, boolean clearCachedState) {
    String statePath = context.getZooKeeperRoot() + Constants.ZTABLES + "/" + tableId.canonical()
        + Constants.ZTABLE_STATE;
    if (clearCachedState) {
      context.getZooCache().clear(context.getZooKeeperRoot() + statePath);
      instanceToMapCache.invalidateAll();
    }
    ZooCache zc = context.getZooCache();
    byte[] state = zc.get(statePath);
    if (state == null) {
      return TableState.UNKNOWN;
    }
    return TableState.valueOf(new String(state, UTF_8));
  }

  /**
   * Returns the namespace id for a given table ID.
   *
   * @param tableId The tableId
   * @return The namespace id which this table resides in.
   * @throws IllegalArgumentException if the table doesn't exist in ZooKeeper
   */
  public NamespaceId getNamespaceId(TableId tableId) throws TableNotFoundException {
    checkArgument(context != null, "instance is null");
    checkArgument(tableId != null, "tableId is null");
    ZooCache zc = context.getZooCache();
    byte[] n = zc.get(context.getZooKeeperRoot() + Constants.ZTABLES + "/" + tableId
        + Constants.ZTABLE_NAMESPACE);
    // We might get null out of ZooCache if this tableID doesn't exist
    if (n == null) {
      throw new TableNotFoundException(tableId.canonical(), null, null);
    }
    return NamespaceId.of(new String(n, UTF_8));
  }

  @Override
  public void close() {
    instanceToMapCache.invalidateAll();
  }
}

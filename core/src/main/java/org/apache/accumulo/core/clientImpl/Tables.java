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
package org.apache.accumulo.core.clientImpl;

import static com.google.common.base.Preconditions.checkArgument;
import static java.nio.charset.StandardCharsets.UTF_8;

import java.security.SecurityPermission;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.NamespaceNotFoundException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.NamespaceId;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.master.state.tables.TableState;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.singletons.SingletonManager;
import org.apache.accumulo.core.singletons.SingletonService;
import org.apache.accumulo.core.util.Pair;
import org.apache.accumulo.fate.zookeeper.ZooCache;
import org.apache.accumulo.fate.zookeeper.ZooCacheFactory;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

public class Tables {

  public static final String VALID_NAME_REGEX = "^(\\w+\\.)?(\\w+)$";

  private static final SecurityPermission TABLES_PERMISSION =
      new SecurityPermission("tablesPermission");
  // Per instance cache will expire after 10 minutes in case we encounter an instance not used
  // frequently
  private static Cache<String,TableMap> instanceToMapCache =
      CacheBuilder.newBuilder().expireAfterAccess(10, TimeUnit.MINUTES).build();

  static {
    SingletonManager.register(new SingletonService() {

      boolean enabled = false;

      @Override
      public synchronized boolean isEnabled() {
        return enabled;
      }

      @Override
      public synchronized void enable() {
        enabled = true;
      }

      @Override
      public synchronized void disable() {
        try {
          instanceToMapCache.invalidateAll();
        } finally {
          enabled = false;
        }
      }
    });
  }

  /**
   * Lookup table ID in ZK. Throw TableNotFoundException if not found. Also wraps
   * NamespaceNotFoundException in TableNotFoundException if namespace is not found.
   */

  public static TableId getTableId(ClientContext context, String tableName)
      throws TableNotFoundException {
    try {
      return _getTableId(context, tableName);
    } catch (NamespaceNotFoundException e) {
      throw new TableNotFoundException(tableName, e);
    }
  }

  /**
   * Return the cached ZooCache for provided context. ZooCache is initially created with a watcher
   * that will clear the TableMap cache for that instance when WatchedEvent occurs.
   */
  private static ZooCache getZooCache(final ClientContext context) {
    SecurityManager sm = System.getSecurityManager();
    if (sm != null) {
      sm.checkPermission(TABLES_PERMISSION);
    }

    return new ZooCacheFactory().getZooCache(context.getZooKeepers(),
        context.getZooKeepersSessionTimeOut());
  }

  /**
   * Lookup table ID in ZK. If not found, clears cache and tries again.
   */
  public static TableId _getTableId(ClientContext context, String tableName)
      throws NamespaceNotFoundException, TableNotFoundException {
    TableId tableId = getNameToIdMap(context).get(tableName);
    if (tableId == null) {
      // maybe the table exist, but the cache was not updated yet... so try to clear the cache and
      // check again
      clearCache(context);
      tableId = getNameToIdMap(context).get(tableName);
      if (tableId == null) {
        String namespace = qualify(tableName).getFirst();
        if (Namespaces.getNameToIdMap(context).containsKey(namespace))
          throw new TableNotFoundException(null, tableName, null);
        else
          throw new NamespaceNotFoundException(null, namespace, null);
      }
    }
    return tableId;
  }

  public static String getTableName(ClientContext context, TableId tableId)
      throws TableNotFoundException {
    String tableName = getIdToNameMap(context).get(tableId);
    if (tableName == null)
      throw new TableNotFoundException(tableId.canonical(), null, null);
    return tableName;
  }

  public static String getTableOfflineMsg(ClientContext context, TableId tableId) {
    if (tableId == null)
      return "Table <unknown table> is offline";
    try {
      String tableName = Tables.getTableName(context, tableId);
      return "Table " + tableName + " (" + tableId.canonical() + ") is offline";
    } catch (TableNotFoundException e) {
      return "Table <unknown table> (" + tableId.canonical() + ") is offline";
    }
  }

  public static Map<String,TableId> getNameToIdMap(ClientContext context) {
    return getTableMap(context).getNameToIdMap();
  }

  public static Map<TableId,String> getIdToNameMap(ClientContext context) {
    return getTableMap(context).getIdtoNameMap();
  }

  /**
   * Get the TableMap from the cache. A new one will be populated when needed. Cache is cleared
   * manually by calling {@link #clearCache(ClientContext)}
   */
  private static TableMap getTableMap(final ClientContext context) {
    TableMap map;

    final ZooCache zc = getZooCache(context);

    map = getTableMap(context, zc);

    if (!map.isCurrent(zc)) {
      instanceToMapCache.invalidate(context.getInstanceID());
      map = getTableMap(context, zc);
    }

    return map;
  }

  private static TableMap getTableMap(final ClientContext context, final ZooCache zc) {
    try {
      return instanceToMapCache.get(context.getInstanceID(), () -> new TableMap(context, zc));
    } catch (ExecutionException e) {
      throw new RuntimeException(e);
    }
  }

  public static boolean exists(ClientContext context, TableId tableId) {
    ZooCache zc = getZooCache(context);
    List<String> tableIds = zc.getChildren(context.getZooKeeperRoot() + Constants.ZTABLES);
    return tableIds.contains(tableId.canonical());
  }

  public static void clearCache(ClientContext context) {
    getZooCache(context).clear(context.getZooKeeperRoot() + Constants.ZTABLES);
    getZooCache(context).clear(context.getZooKeeperRoot() + Constants.ZNAMESPACES);
    instanceToMapCache.invalidate(context.getInstanceID());
  }

  /**
   * Clears the zoo cache from instance/root/{PATH}
   *
   * @param context
   *          The Accumulo client context
   * @param zooPath
   *          A zookeeper path
   */
  public static void clearCacheByPath(ClientContext context, final String zooPath) {
    String thePath = zooPath.startsWith("/") ? zooPath : "/" + zooPath;
    getZooCache(context).clear(context.getZooKeeperRoot() + thePath);
    instanceToMapCache.invalidate(context.getInstanceID());
  }

  public static String getPrintableTableInfoFromId(ClientContext context, TableId tableId) {
    String tableName = null;
    try {
      tableName = getTableName(context, tableId);
    } catch (TableNotFoundException e) {
      // handled in the string formatting
    }
    return tableName == null ? String.format("?(ID:%s)", tableId.canonical())
        : String.format("%s(ID:%s)", tableName, tableId.canonical());
  }

  public static String getPrintableTableInfoFromName(ClientContext context, String tableName) {
    TableId tableId = null;
    try {
      tableId = getTableId(context, tableName);
    } catch (TableNotFoundException e) {
      // handled in the string formatting
    }
    return tableId == null ? String.format("%s(?)", tableName)
        : String.format("%s(ID:%s)", tableName, tableId.canonical());
  }

  public static TableState getTableState(ClientContext context, TableId tableId) {
    return getTableState(context, tableId, false);
  }

  /**
   * Get the current state of the table using the tableid. The boolean clearCache, if true will
   * clear the table state in zookeeper before fetching the state. Added with ACCUMULO-4574.
   *
   * @param context
   *          the Accumulo client context
   * @param tableId
   *          the table id
   * @param clearCachedState
   *          if true clear the table state in zookeeper before checking status
   * @return the table state.
   */
  public static TableState getTableState(ClientContext context, TableId tableId,
      boolean clearCachedState) {

    String statePath = context.getZooKeeperRoot() + Constants.ZTABLES + "/" + tableId.canonical()
        + Constants.ZTABLE_STATE;

    if (clearCachedState) {
      Tables.clearCacheByPath(context, statePath);
    }

    ZooCache zc = getZooCache(context);
    byte[] state = zc.get(statePath);
    if (state == null)
      return TableState.UNKNOWN;

    return TableState.valueOf(new String(state, UTF_8));

  }

  public static String qualified(String tableName) {
    return qualified(tableName, Namespace.DEFAULT.name());
  }

  public static String qualified(String tableName, String defaultNamespace) {
    Pair<String,String> qualifiedTableName = qualify(tableName, defaultNamespace);
    if (Namespace.DEFAULT.name().equals(qualifiedTableName.getFirst()))
      return qualifiedTableName.getSecond();
    else
      return qualifiedTableName.toString("", ".", "");
  }

  public static Pair<String,String> qualify(String tableName) {
    return qualify(tableName, Namespace.DEFAULT.name());
  }

  public static Pair<String,String> qualify(String tableName, String defaultNamespace) {
    if (!tableName.matches(VALID_NAME_REGEX))
      throw new IllegalArgumentException("Invalid table name '" + tableName + "'");
    if (MetadataTable.OLD_NAME.equals(tableName))
      tableName = MetadataTable.NAME;
    if (tableName.contains(".")) {
      String[] s = tableName.split("\\.", 2);
      return new Pair<>(s[0], s[1]);
    }
    return new Pair<>(defaultNamespace, tableName);
  }

  /**
   * Returns the namespace id for a given table ID.
   *
   * @param context
   *          The Accumulo client context
   * @param tableId
   *          The tableId
   * @return The namespace id which this table resides in.
   * @throws IllegalArgumentException
   *           if the table doesn't exist in ZooKeeper
   */
  public static NamespaceId getNamespaceId(ClientContext context, TableId tableId)
      throws TableNotFoundException {
    checkArgument(context != null, "instance is null");
    checkArgument(tableId != null, "tableId is null");

    ZooCache zc = getZooCache(context);
    byte[] n = zc.get(context.getZooKeeperRoot() + Constants.ZTABLES + "/" + tableId
        + Constants.ZTABLE_NAMESPACE);

    // We might get null out of ZooCache if this tableID doesn't exist
    if (n == null) {
      throw new TableNotFoundException(tableId.canonical(), null, null);
    }

    return NamespaceId.of(new String(n, UTF_8));
  }
}

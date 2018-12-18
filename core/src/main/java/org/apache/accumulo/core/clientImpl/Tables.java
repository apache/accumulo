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

  private static final SecurityPermission TABLES_PERMISSION = new SecurityPermission(
      "tablesPermission");
  // Per instance cache will expire after 10 minutes in case we encounter an instance not used
  // frequently
  private static Cache<String,TableMap> instanceToMapCache = CacheBuilder.newBuilder()
      .expireAfterAccess(10, TimeUnit.MINUTES).build();
  private static Cache<String,ZooCache> instanceToZooCache = CacheBuilder.newBuilder()
      .expireAfterAccess(10, TimeUnit.MINUTES).build();

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
          instanceToZooCache.invalidateAll();
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

  public static Table.ID getTableId(ClientContext context, String tableName)
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

    final String uuid = context.getInstanceID();

    try {
      return instanceToZooCache.get(uuid, () -> {
        final String zks = context.getZooKeepers();
        final int timeOut = context.getZooKeepersSessionTimeOut();
        return new ZooCacheFactory().getZooCache(zks, timeOut,
            watchedEvent -> instanceToMapCache.invalidate(uuid));
      });
    } catch (ExecutionException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Lookup table ID in ZK. If not found, clears cache and tries again.
   */
  public static Table.ID _getTableId(ClientContext context, String tableName)
      throws NamespaceNotFoundException, TableNotFoundException {
    Table.ID tableId = getNameToIdMap(context).get(tableName);
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

  public static String getTableName(ClientContext context, Table.ID tableId)
      throws TableNotFoundException {
    String tableName = getIdToNameMap(context).get(tableId);
    if (tableName == null)
      throw new TableNotFoundException(tableId.canonicalID(), null, null);
    return tableName;
  }

  public static String getTableOfflineMsg(ClientContext context, Table.ID tableId) {
    if (tableId == null)
      return "Table <unknown table> is offline";
    try {
      String tableName = Tables.getTableName(context, tableId);
      return "Table " + tableName + " (" + tableId.canonicalID() + ") is offline";
    } catch (TableNotFoundException e) {
      return "Table <unknown table> (" + tableId.canonicalID() + ") is offline";
    }
  }

  public static Map<String,Table.ID> getNameToIdMap(ClientContext context) {
    return getTableMap(context).getNameToIdMap();
  }

  public static Map<Table.ID,String> getIdToNameMap(ClientContext context) {
    return getTableMap(context).getIdtoNameMap();
  }

  /**
   * Get the TableMap from the cache. A new one will be populated when needed. Cache is cleared
   * manually by calling {@link #clearCache(ClientContext)} or automatically cleared by ZooCache
   * watcher created in {@link #getZooCache(ClientContext)}. See ACCUMULO-4778.
   */
  private static TableMap getTableMap(final ClientContext context) {
    TableMap map;
    try {
      map = instanceToMapCache.get(context.getInstanceID(),
          () -> new TableMap(context, getZooCache(context)));
    } catch (ExecutionException e) {
      throw new RuntimeException(e);
    }
    return map;
  }

  public static boolean exists(ClientContext context, Table.ID tableId) {
    ZooCache zc = getZooCache(context);
    List<String> tableIds = zc.getChildren(context.getZooKeeperRoot() + Constants.ZTABLES);
    return tableIds.contains(tableId.canonicalID());
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

  public static String getPrintableTableInfoFromId(ClientContext context, Table.ID tableId) {
    String tableName = null;
    try {
      tableName = getTableName(context, tableId);
    } catch (TableNotFoundException e) {
      // handled in the string formatting
    }
    return tableName == null ? String.format("?(ID:%s)", tableId.canonicalID())
        : String.format("%s(ID:%s)", tableName, tableId.canonicalID());
  }

  public static String getPrintableTableInfoFromName(ClientContext context, String tableName) {
    Table.ID tableId = null;
    try {
      tableId = getTableId(context, tableName);
    } catch (TableNotFoundException e) {
      // handled in the string formatting
    }
    return tableId == null ? String.format("%s(?)", tableName)
        : String.format("%s(ID:%s)", tableName, tableId.canonicalID());
  }

  public static TableState getTableState(ClientContext context, Table.ID tableId) {
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
  public static TableState getTableState(ClientContext context, Table.ID tableId,
      boolean clearCachedState) {

    String statePath = context.getZooKeeperRoot() + Constants.ZTABLES + "/" + tableId.canonicalID()
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
    return qualified(tableName, Namespace.DEFAULT);
  }

  public static String qualified(String tableName, String defaultNamespace) {
    Pair<String,String> qualifiedTableName = qualify(tableName, defaultNamespace);
    if (Namespace.DEFAULT.equals(qualifiedTableName.getFirst()))
      return qualifiedTableName.getSecond();
    else
      return qualifiedTableName.toString("", ".", "");
  }

  public static Pair<String,String> qualify(String tableName) {
    return qualify(tableName, Namespace.DEFAULT);
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
  public static Namespace.ID getNamespaceId(ClientContext context, Table.ID tableId)
      throws TableNotFoundException {
    checkArgument(context != null, "instance is null");
    checkArgument(tableId != null, "tableId is null");

    ZooCache zc = getZooCache(context);
    byte[] n = zc.get(context.getZooKeeperRoot() + Constants.ZTABLES + "/" + tableId
        + Constants.ZTABLE_NAMESPACE);

    // We might get null out of ZooCache if this tableID doesn't exist
    if (n == null) {
      throw new TableNotFoundException(tableId.canonicalID(), null, null);
    }

    return Namespace.ID.of(new String(n, UTF_8));
  }
}

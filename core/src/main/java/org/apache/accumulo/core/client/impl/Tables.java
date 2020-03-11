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
package org.apache.accumulo.core.client.impl;

import static com.google.common.base.Preconditions.checkArgument;
import static java.nio.charset.StandardCharsets.UTF_8;

import java.security.SecurityPermission;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.NamespaceNotFoundException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.master.state.tables.TableState;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.util.Pair;
import org.apache.accumulo.core.zookeeper.ZooUtil;
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

  /**
   * Return the cached ZooCache for provided instance. ZooCache is initially created with a watcher
   * that will clear the TableMap cache for that instance when WatchedEvent occurs.
   */
  private static ZooCache getZooCache(final Instance instance) {
    SecurityManager sm = System.getSecurityManager();
    if (sm != null) {
      sm.checkPermission(TABLES_PERMISSION);
    }

    return new ZooCacheFactory().getZooCache(instance.getZooKeepers(),
        instance.getZooKeepersSessionTimeOut());
  }

  public static String getTableId(Instance instance, String tableName)
      throws TableNotFoundException {
    try {
      return _getTableId(instance, tableName);
    } catch (NamespaceNotFoundException e) {
      throw new TableNotFoundException(tableName, e);
    }
  }

  public static String _getTableId(Instance instance, String tableName)
      throws NamespaceNotFoundException, TableNotFoundException {
    String tableId = getNameToIdMap(instance).get(tableName);
    if (tableId == null) {
      // maybe the table exist, but the cache was not updated yet... so try to clear the cache and
      // check again
      clearCache(instance);
      tableId = getNameToIdMap(instance).get(tableName);
      if (tableId == null) {
        String namespace = qualify(tableName).getFirst();
        if (Namespaces.getNameToIdMap(instance).containsKey(namespace))
          throw new TableNotFoundException(null, tableName, null);
        else
          throw new NamespaceNotFoundException(null, namespace, null);
      }
    }
    return tableId;
  }

  public static String getTableName(Instance instance, String tableId)
      throws TableNotFoundException {
    String tableName = getIdToNameMap(instance).get(tableId);
    if (tableName == null)
      throw new TableNotFoundException(tableId, null, null);
    return tableName;
  }

  public static Map<String,String> getNameToIdMap(Instance instance) {
    return getTableMap(instance).getNameToIdMap();
  }

  public static Map<String,String> getIdToNameMap(Instance instance) {
    return getTableMap(instance).getIdtoNameMap();
  }

  /**
   * Get the TableMap from the cache. A new one will be populated when needed. Cache is cleared
   * manually by calling {@link #clearCache(Instance)}
   */
  private static TableMap getTableMap(final Instance instance) {

    TableMap map;

    final ZooCache zc = getZooCache(instance);

    map = getTableMap(instance, zc);

    if (!map.isCurrent(zc)) {
      instanceToMapCache.invalidate(instance.getInstanceID());
      map = getTableMap(instance, zc);
    }

    return map;
  }

  private static TableMap getTableMap(final Instance instance, final ZooCache zc) {
    try {
      return instanceToMapCache.get(instance.getInstanceID(), new Callable<TableMap>() {
        @Override
        public TableMap call() {
          return new TableMap(instance, zc);
        }
      });
    } catch (ExecutionException e) {
      throw new RuntimeException(e);
    }
  }

  public static boolean exists(Instance instance, String tableId) {
    ZooCache zc = getZooCache(instance);
    List<String> tableIds = zc.getChildren(ZooUtil.getRoot(instance) + Constants.ZTABLES);
    return tableIds.contains(tableId);
  }

  public static void clearCache(Instance instance) {
    getZooCache(instance).clear(ZooUtil.getRoot(instance) + Constants.ZTABLES);
    getZooCache(instance).clear(ZooUtil.getRoot(instance) + Constants.ZNAMESPACES);
    instanceToMapCache.invalidate(instance.getInstanceID());
  }

  /**
   * Clears the zoo cache from instance/root/{PATH}
   *
   * @param instance
   *          The Accumulo Instance
   * @param zooPath
   *          A zookeeper path
   */
  public static void clearCacheByPath(Instance instance, final String zooPath) {
    String thePath = zooPath.startsWith("/") ? zooPath : "/" + zooPath;
    getZooCache(instance).clear(ZooUtil.getRoot(instance) + thePath);
    instanceToMapCache.invalidate(instance.getInstanceID());
  }

  public static String getPrintableTableNameFromId(Map<String,String> tidToNameMap,
      String tableId) {
    String tableName = tidToNameMap.get(tableId);
    return tableName == null ? "(ID:" + tableId + ")" : tableName;
  }

  public static String getPrintableTableInfoFromId(Instance instance, String tableId) {
    String tableName = null;
    try {
      tableName = getTableName(instance, tableId);
    } catch (TableNotFoundException e) {
      // handled in the string formatting
    }
    return tableName == null ? String.format("?(ID:%s)", tableId)
        : String.format("%s(ID:%s)", tableName, tableId);
  }

  public static String getPrintableTableInfoFromName(Instance instance, String tableName) {
    String tableId = null;
    try {
      tableId = getTableId(instance, tableName);
    } catch (TableNotFoundException e) {
      // handled in the string formatting
    }
    return tableId == null ? String.format("%s(?)", tableName)
        : String.format("%s(ID:%s)", tableName, tableId);
  }

  public static TableState getTableState(Instance instance, String tableId) {
    return getTableState(instance, tableId, false);
  }

  /**
   * Get the current state of the table using the tableid. The boolean clearCache, if true will
   * clear the table state in zookeeper before fetching the state. Added with ACCUMULO-4574.
   *
   * @param instance
   *          the Accumulo instance.
   * @param tableId
   *          the table id
   * @param clearCachedState
   *          if true clear the table state in zookeeper before checking status
   * @return the table state.
   */
  public static TableState getTableState(Instance instance, String tableId,
      boolean clearCachedState) {

    String statePath =
        ZooUtil.getRoot(instance) + Constants.ZTABLES + "/" + tableId + Constants.ZTABLE_STATE;

    if (clearCachedState) {
      Tables.clearCacheByPath(instance, statePath);
    }

    ZooCache zc = getZooCache(instance);
    byte[] state = zc.get(statePath);
    if (state == null)
      return TableState.UNKNOWN;

    return TableState.valueOf(new String(state, UTF_8));

  }

  public static String qualified(String tableName) {
    return qualified(tableName, Namespaces.DEFAULT_NAMESPACE);
  }

  public static String qualified(String tableName, String defaultNamespace) {
    Pair<String,String> qualifiedTableName = qualify(tableName, defaultNamespace);
    if (Namespaces.DEFAULT_NAMESPACE.equals(qualifiedTableName.getFirst()))
      return qualifiedTableName.getSecond();
    else
      return qualifiedTableName.toString("", ".", "");
  }

  public static Pair<String,String> qualify(String tableName) {
    return qualify(tableName, Namespaces.DEFAULT_NAMESPACE);
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
   * @param instance
   *          The Accumulo Instance
   * @param tableId
   *          The tableId
   * @return The namespace id which this table resides in.
   * @throws IllegalArgumentException
   *           if the table doesn't exist in ZooKeeper
   */
  public static String getNamespaceId(Instance instance, String tableId)
      throws TableNotFoundException {
    checkArgument(instance != null, "instance is null");
    checkArgument(tableId != null, "tableId is null");

    ZooCache zc = getZooCache(instance);
    byte[] n = zc.get(
        ZooUtil.getRoot(instance) + Constants.ZTABLES + "/" + tableId + Constants.ZTABLE_NAMESPACE);

    // We might get null out of ZooCache if this tableID doesn't exist
    if (null == n) {
      throw new TableNotFoundException(tableId, null, null);
    }

    return new String(n, UTF_8);
  }

}

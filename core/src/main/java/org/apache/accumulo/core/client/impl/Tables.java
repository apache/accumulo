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

import static com.google.common.base.Charsets.UTF_8;

import java.security.SecurityPermission;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.NamespaceNotFoundException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.master.state.tables.TableState;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.metadata.RootTable;
import org.apache.accumulo.core.util.ArgumentChecker;
import org.apache.accumulo.core.util.ArgumentChecker.Validator;
import org.apache.accumulo.core.util.Pair;
import org.apache.accumulo.core.zookeeper.ZooUtil;
import org.apache.accumulo.fate.zookeeper.ZooCache;
import org.apache.accumulo.fate.zookeeper.ZooCacheFactory;
import org.apache.log4j.Logger;

public class Tables {
  public static final String VALID_NAME_REGEX = "^(\\w+\\.)?(\\w+)$";
  public static final String VALID_ID_REGEX = "^([a-z0-9]+)$"; // BigDecimal base36
  public static final Validator<String> VALID_NAME = new Validator<String>() {
    @Override
    public boolean isValid(String tableName) {
      return tableName != null && tableName.matches(VALID_NAME_REGEX);
    }

    @Override
    public String invalidMessage(String tableName) {
      if (tableName == null)
        return "Table name cannot be null";
      return "Table names must only contain word characters (letters, digits, and underscores): " + tableName;
    }
  };

  public static final Validator<String> VALID_ID = new Validator<String>() {
    @Override
    public boolean isValid(String tableId) {
      return tableId != null && (RootTable.ID.equals(tableId) || MetadataTable.ID.equals(tableId) || tableId.matches(VALID_ID_REGEX));
    }

    @Override
    public String invalidMessage(String tableId) {
      if (tableId == null)
        return "Table id cannot be null";
      return "Table IDs are base-36 numbers, represented with lowercase alphanumeric digits: " + tableId;
    }
  };

  public static final Validator<String> NOT_SYSTEM = new Validator<String>() {

    @Override
    public boolean isValid(String tableName) {
      return !Namespaces.ACCUMULO_NAMESPACE.equals(Tables.qualify(tableName).getFirst());
    }

    @Override
    public String invalidMessage(String tableName) {
      return "Table cannot be in the " + Namespaces.ACCUMULO_NAMESPACE + " namespace";
    }
  };

  public static final Validator<String> NOT_ROOT = new Validator<String>() {

    @Override
    public boolean isValid(String tableName) {
      return !RootTable.NAME.equals(tableName);
    }

    @Override
    public String invalidMessage(String tableName) {
      return "Table cannot be the " + RootTable.NAME + "(Id: " + RootTable.ID + ") table";
    }
  };

  public static final Validator<String> NOT_ROOT_ID = new Validator<String>() {

    @Override
    public boolean isValid(String tableId) {
      return !RootTable.ID.equals(tableId);
    }

    @Override
    public String invalidMessage(String tableId) {
      return "Table cannot be the " + RootTable.NAME + "(Id: " + RootTable.ID + ") table";
    }
  };

  private static SecurityPermission TABLES_PERMISSION = new SecurityPermission("tablesPermission");
  private static AtomicLong cacheResetCount = new AtomicLong(0);
  private static final Logger log = Logger.getLogger(Tables.class);

  private static ZooCache getZooCache(Instance instance) {
    SecurityManager sm = System.getSecurityManager();
    if (sm != null) {
      sm.checkPermission(TABLES_PERMISSION);
    }
    return new ZooCacheFactory().getZooCache(instance.getZooKeepers(), instance.getZooKeepersSessionTimeOut());
  }

  private static SortedMap<String,String> getMap(Instance instance, boolean nameAsKey) {
    ZooCache zc = getZooCache(instance);

    List<String> tableIds = zc.getChildren(ZooUtil.getRoot(instance) + Constants.ZTABLES);
    TreeMap<String,String> tableMap = new TreeMap<String,String>();
    Map<String,String> namespaceIdToNameMap = new HashMap<String,String>();

    for (String tableId : tableIds) {
      byte[] tableName = zc.get(ZooUtil.getRoot(instance) + Constants.ZTABLES + "/" + tableId + Constants.ZTABLE_NAME);
      byte[] nId = zc.get(ZooUtil.getRoot(instance) + Constants.ZTABLES + "/" + tableId + Constants.ZTABLE_NAMESPACE);
      String namespaceName = Namespaces.DEFAULT_NAMESPACE;
      // create fully qualified table name
      if (nId == null) {
        namespaceName = null;
      } else {
        String namespaceId = new String(nId, UTF_8);
        if (!namespaceId.equals(Namespaces.DEFAULT_NAMESPACE_ID)) {
          try {
            namespaceName = namespaceIdToNameMap.get(namespaceId);
            if (namespaceName == null) {
              namespaceName = Namespaces.getNamespaceName(instance, namespaceId);
              namespaceIdToNameMap.put(namespaceId, namespaceName);
            }
          } catch (NamespaceNotFoundException e) {
            log.error("Table (" + tableId + ") contains reference to namespace (" + namespaceId + ") that doesn't exist", e);
            continue;
          }
        }
      }
      if (tableName != null && namespaceName != null) {
        String tableNameStr = qualified(new String(tableName, UTF_8), namespaceName);
        if (nameAsKey)
          tableMap.put(tableNameStr, tableId);
        else
          tableMap.put(tableId, tableNameStr);
      }
    }

    return tableMap;
  }

  public static String getTableId(Instance instance, String tableName) throws TableNotFoundException {
    try {
      return _getTableId(instance, tableName);
    } catch (NamespaceNotFoundException e) {
      throw new TableNotFoundException(tableName, e);
    }
  }

  public static String _getTableId(Instance instance, String tableName) throws NamespaceNotFoundException, TableNotFoundException {
    String tableId = getNameToIdMap(instance).get(tableName);
    if (tableId == null) {
      // maybe the table exist, but the cache was not updated yet... so try to clear the cache and check again
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

  public static String getTableName(Instance instance, String tableId) throws TableNotFoundException {
    String tableName = getIdToNameMap(instance).get(tableId);
    if (tableName == null)
      throw new TableNotFoundException(tableId, null, null);
    return tableName;
  }

  public static SortedMap<String,String> getNameToIdMap(Instance instance) {
    return getMap(instance, true);
  }

  public static SortedMap<String,String> getIdToNameMap(Instance instance) {
    return getMap(instance, false);
  }

  public static boolean exists(Instance instance, String tableId) {
    ZooCache zc = getZooCache(instance);
    List<String> tableIds = zc.getChildren(ZooUtil.getRoot(instance) + Constants.ZTABLES);
    return tableIds.contains(tableId);
  }

  public static void clearCache(Instance instance) {
    cacheResetCount.incrementAndGet();
    getZooCache(instance).clear(ZooUtil.getRoot(instance) + Constants.ZTABLES);
    getZooCache(instance).clear(ZooUtil.getRoot(instance) + Constants.ZNAMESPACES);
  }

  public static String getPrintableTableNameFromId(Map<String,String> tidToNameMap, String tableId) {
    String tableName = tidToNameMap.get(tableId);
    return tableName == null ? "(ID:" + tableId + ")" : tableName;
  }

  public static String getPrintableTableIdFromName(Map<String,String> nameToIdMap, String tableName) {
    String tableId = nameToIdMap.get(tableName);
    return tableId == null ? "(NAME:" + tableName + ")" : tableId;
  }

  public static String getPrintableTableInfoFromId(Instance instance, String tableId) {
    String tableName = null;
    try {
      tableName = getTableName(instance, tableId);
    } catch (TableNotFoundException e) {
      // handled in the string formatting
    }
    return tableName == null ? String.format("?(ID:%s)", tableId) : String.format("%s(ID:%s)", tableName, tableId);
  }

  public static String getPrintableTableInfoFromName(Instance instance, String tableName) {
    String tableId = null;
    try {
      tableId = getTableId(instance, tableName);
    } catch (TableNotFoundException e) {
      // handled in the string formatting
    }
    return tableId == null ? String.format("%s(?)", tableName) : String.format("%s(ID:%s)", tableName, tableId);
  }

  public static TableState getTableState(Instance instance, String tableId) {
    String statePath = ZooUtil.getRoot(instance) + Constants.ZTABLES + "/" + tableId + Constants.ZTABLE_STATE;
    ZooCache zc = getZooCache(instance);
    byte[] state = zc.get(statePath);
    if (state == null)
      return TableState.UNKNOWN;

    return TableState.valueOf(new String(state, UTF_8));
  }

  public static long getCacheResetCount() {
    return cacheResetCount.get();
  }

  public static String qualified(String tableName) {
    return qualified(tableName, Namespaces.DEFAULT_NAMESPACE);
  }

  public static String qualified(String tableName, String defaultNamespace) {
    Pair<String,String> qualifiedTableName = Tables.qualify(tableName, defaultNamespace);
    if (Namespaces.DEFAULT_NAMESPACE.equals(qualifiedTableName.getFirst()))
      return qualifiedTableName.getSecond();
    else
      return qualifiedTableName.toString("", ".", "");
  }

  public static Pair<String,String> qualify(String tableName) {
    return qualify(tableName, Namespaces.DEFAULT_NAMESPACE);
  }

  public static Pair<String,String> qualify(String tableName, String defaultNamespace) {
    if (!tableName.matches(Tables.VALID_NAME_REGEX))
      throw new IllegalArgumentException("Invalid table name '" + tableName + "'");
    if (MetadataTable.OLD_NAME.equals(tableName))
      tableName = MetadataTable.NAME;
    if (tableName.contains(".")) {
      String[] s = tableName.split("\\.", 2);
      return new Pair<String,String>(s[0], s[1]);
    }
    return new Pair<String,String>(defaultNamespace, tableName);
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
  public static String getNamespaceId(Instance instance, String tableId) throws IllegalArgumentException {
    ArgumentChecker.notNull(instance, tableId);

    ZooCache zc = getZooCache(instance);
    byte[] n = zc.get(ZooUtil.getRoot(instance) + Constants.ZTABLES + "/" + tableId + Constants.ZTABLE_NAMESPACE);

    // We might get null out of ZooCache if this tableID doesn't exist
    if (null == n) {
      throw new IllegalArgumentException("Table with id " + tableId + " does not exist");
    }

    return new String(n, UTF_8);
  }

}

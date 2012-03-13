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

import java.security.SecurityPermission;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.master.state.tables.TableState;
import org.apache.accumulo.core.zookeeper.ZooCache;
import org.apache.accumulo.core.zookeeper.ZooUtil;

public class Tables {
  private static SecurityPermission TABLES_PERMISSION = new SecurityPermission("tablesPermission");
  
  private static ZooCache getZooCache(Instance instance) {
    SecurityManager sm = System.getSecurityManager();
    if (sm != null) {
      sm.checkPermission(TABLES_PERMISSION);
    }
    return ZooCache.getInstance(instance.getZooKeepers(), instance.getZooKeepersSessionTimeOut());
  }
  
  private static SortedMap<String,String> getMap(Instance instance, boolean nameAsKey) {
    ZooCache zc = getZooCache(instance);
    
    List<String> tableIds = zc.getChildren(ZooUtil.getRoot(instance) + Constants.ZTABLES);
    
    TreeMap<String,String> tableMap = new TreeMap<String,String>();
    
    for (String tableId : tableIds) {
      byte[] tblPath = zc.get(ZooUtil.getRoot(instance) + Constants.ZTABLES + "/" + tableId + Constants.ZTABLE_NAME);
      if (tblPath != null) {
        if (nameAsKey)
          tableMap.put(new String(tblPath), tableId);
        else
          tableMap.put(tableId, new String(tblPath));
      }
    }
    
    return tableMap;
  }
  
  public static String getTableId(Instance instance, String tableName) throws TableNotFoundException {
    String tableId = getNameToIdMap(instance).get(tableName);
    if (tableId == null)
      throw new TableNotFoundException(tableId, tableName, null);
    return tableId;
  }
  
  public static String getTableName(Instance instance, String tableId) throws TableNotFoundException {
    String tableName = getIdToNameMap(instance).get(tableId);
    if (tableName == null)
      throw new TableNotFoundException(tableId, tableName, null);
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
    getZooCache(instance).clear(ZooUtil.getRoot(instance) + Constants.ZTABLES);
  }
  
  public static String getPrintableTableNameFromId(Map<String,String> tidToNameMap, String tableId) {
    String tableName = tidToNameMap.get(tableId);
    return tableName == null ? "(ID:" + tableId + ")" : tableName;
  }
  
  public static String getPrintableTableIdFromName(Map<String,String> nameToIdMap, String tableName) {
    String tableId = nameToIdMap.get(tableName);
    return tableId == null ? "(NAME:" + tableName + ")" : tableId;
  }
  
  public static TableState getTableState(Instance instance, String tableId) {
    String statePath = ZooUtil.getRoot(instance) + Constants.ZTABLES + "/" + tableId + Constants.ZTABLE_STATE;
    ZooCache zc = getZooCache(instance);
    byte[] state = zc.get(statePath);
    if (state == null)
      return TableState.UNKNOWN;
    
    return TableState.valueOf(new String(state));
  }
}

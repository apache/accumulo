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
import java.util.LinkedList;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.TableNamespaceNotFoundException;
import org.apache.accumulo.core.zookeeper.ZooUtil;
import org.apache.accumulo.fate.zookeeper.ZooCache;

public class TableNamespaces {
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
    
    List<String> namespaceIds = zc.getChildren(ZooUtil.getRoot(instance) + Constants.ZNAMESPACES);
    
    TreeMap<String,String> namespaceMap = new TreeMap<String,String>();
    
    for (String id : namespaceIds) {
      byte[] path = zc.get(ZooUtil.getRoot(instance) + Constants.ZNAMESPACES + "/" + id + Constants.ZNAMESPACE_NAME);
      if (path != null) {
        if (nameAsKey)
          namespaceMap.put(new String(path, Constants.UTF8), id);
        else
          namespaceMap.put(id, new String(path, Constants.UTF8));
      }
    }
    return namespaceMap;
  }
  
  public static String getNamespaceId(Instance instance, String namespace) throws TableNamespaceNotFoundException {
    String id = getNameToIdMap(instance).get(namespace);
    if (id == null)
      throw new TableNamespaceNotFoundException(null, namespace, "getNamespaceId() failed to find namespace");
    return id;
  }
  
  public static String getNamespaceName(Instance instance, String namespaceId) throws TableNamespaceNotFoundException {
    String namespaceName = getIdToNameMap(instance).get(namespaceId);
    if (namespaceName == null)
      throw new TableNamespaceNotFoundException(namespaceId, null, "getNamespaceName() failed to find namespace");
    return namespaceName;
  }
  
  public static SortedMap<String,String> getNameToIdMap(Instance instance) {
    return getMap(instance, true);
  }
  
  public static SortedMap<String,String> getIdToNameMap(Instance instance) {
    return getMap(instance, false);
  }
  
  public static List<String> getTableIds(Instance instance, String namespaceId) throws TableNamespaceNotFoundException {
    List<String> l = new LinkedList<String>();
    for (String id : Tables.getIdToNameMap(instance).keySet()) {
      if (Tables.getNamespace(instance, id).equals(namespaceId)) {
        l.add(id);
      }
    }
    return l;
  }
  
  public static List<String> getTableNames(Instance instance, String namespaceId) throws TableNamespaceNotFoundException {
    ZooCache zc = getZooCache(instance);
    List<String> ids = getTableIds(instance, namespaceId);
    List<String> names = new LinkedList<String>();
    String namespace = getNamespaceName(instance, namespaceId) + ".";
    if (namespaceId.equals(Constants.DEFAULT_TABLE_NAMESPACE_ID) || namespaceId.equals(Constants.SYSTEM_TABLE_NAMESPACE_ID)) {
      // default and system namespaces aren't displayed for backwards compatibility
      namespace = "";
    }
    for (String id : ids) {
      names.add(namespace + new String(zc.get(ZooUtil.getRoot(instance) + Constants.ZTABLES + "/" + id + Constants.ZTABLE_NAME), Constants.UTF8));
    }
    return names;
  }
}

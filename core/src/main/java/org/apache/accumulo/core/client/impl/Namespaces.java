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
import java.util.LinkedList;
import java.util.List;
import java.util.Map.Entry;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.NamespaceNotFoundException;
import org.apache.accumulo.core.util.ArgumentChecker.Validator;
import org.apache.accumulo.core.zookeeper.ZooUtil;
import org.apache.accumulo.fate.zookeeper.ZooCache;
import org.apache.accumulo.fate.zookeeper.ZooCacheFactory;

public class Namespaces {
  public static final String VALID_NAME_REGEX = "^\\w*$";
  public static final Validator<String> VALID_NAME = new Validator<String>() {
    @Override
    public boolean isValid(String namespace) {
      return namespace != null && namespace.matches(VALID_NAME_REGEX);
    }

    @Override
    public String invalidMessage(String namespace) {
      if (namespace == null)
        return "Namespace cannot be null";
      return "Namespaces must only contain word characters (letters, digits, and underscores): " + namespace;
    }
  };

  public static final Validator<String> NOT_DEFAULT = new Validator<String>() {
    @Override
    public boolean isValid(String namespace) {
      return !Namespaces.DEFAULT_NAMESPACE.equals(namespace);
    }

    @Override
    public String invalidMessage(String namespace) {
      return "Namespace cannot be the reserved empty namespace";
    }
  };

  public static final Validator<String> NOT_ACCUMULO = new Validator<String>() {
    @Override
    public boolean isValid(String namespace) {
      return !Namespaces.ACCUMULO_NAMESPACE.equals(namespace);
    }

    @Override
    public String invalidMessage(String namespace) {
      return "Namespace cannot be the reserved namespace, " + Namespaces.ACCUMULO_NAMESPACE;
    }
  };

  private static SecurityPermission TABLES_PERMISSION = new SecurityPermission("tablesPermission");

  public static final String DEFAULT_NAMESPACE_ID = "+default";
  public static final String DEFAULT_NAMESPACE = "";
  public static final String ACCUMULO_NAMESPACE_ID = "+accumulo";
  public static final String ACCUMULO_NAMESPACE = "accumulo";

  private static ZooCache getZooCache(Instance instance) {
    SecurityManager sm = System.getSecurityManager();
    if (sm != null) {
      sm.checkPermission(TABLES_PERMISSION);
    }
    return new ZooCacheFactory().getZooCache(instance.getZooKeepers(), instance.getZooKeepersSessionTimeOut());
  }

  private static SortedMap<String,String> getMap(Instance instance, boolean nameAsKey) {
    ZooCache zc = getZooCache(instance);

    List<String> namespaceIds = zc.getChildren(ZooUtil.getRoot(instance) + Constants.ZNAMESPACES);

    TreeMap<String,String> namespaceMap = new TreeMap<String,String>();

    for (String id : namespaceIds) {
      byte[] path = zc.get(ZooUtil.getRoot(instance) + Constants.ZNAMESPACES + "/" + id + Constants.ZNAMESPACE_NAME);
      if (path != null) {
        if (nameAsKey)
          namespaceMap.put(new String(path, UTF_8), id);
        else
          namespaceMap.put(id, new String(path, UTF_8));
      }
    }
    return namespaceMap;
  }

  public static boolean exists(Instance instance, String namespaceId) {
    ZooCache zc = getZooCache(instance);
    List<String> namespaceIds = zc.getChildren(ZooUtil.getRoot(instance) + Constants.ZNAMESPACES);
    return namespaceIds.contains(namespaceId);
  }

  public static String getNamespaceId(Instance instance, String namespace) throws NamespaceNotFoundException {
    String id = getNameToIdMap(instance).get(namespace);
    if (id == null)
      throw new NamespaceNotFoundException(null, namespace, "getNamespaceId() failed to find namespace");
    return id;
  }

  public static String getNamespaceName(Instance instance, String namespaceId) throws NamespaceNotFoundException {
    String namespaceName = getIdToNameMap(instance).get(namespaceId);
    if (namespaceName == null)
      throw new NamespaceNotFoundException(namespaceId, null, "getNamespaceName() failed to find namespace");
    return namespaceName;
  }

  public static SortedMap<String,String> getNameToIdMap(Instance instance) {
    return getMap(instance, true);
  }

  public static SortedMap<String,String> getIdToNameMap(Instance instance) {
    return getMap(instance, false);
  }

  public static List<String> getTableIds(Instance instance, String namespaceId) throws NamespaceNotFoundException {
    String namespace = getNamespaceName(instance, namespaceId);
    List<String> names = new LinkedList<String>();
    for (Entry<String,String> nameToId : Tables.getNameToIdMap(instance).entrySet())
      if (namespace.equals(Tables.qualify(nameToId.getKey()).getFirst()))
        names.add(nameToId.getValue());
    return names;
  }

  public static List<String> getTableNames(Instance instance, String namespaceId) throws NamespaceNotFoundException {
    String namespace = getNamespaceName(instance, namespaceId);
    List<String> names = new LinkedList<String>();
    for (String name : Tables.getNameToIdMap(instance).keySet())
      if (namespace.equals(Tables.qualify(name).getFirst()))
        names.add(name);
    return names;
  }

}

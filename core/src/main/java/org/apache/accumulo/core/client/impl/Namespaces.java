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

import static java.nio.charset.StandardCharsets.UTF_8;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map.Entry;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.function.BiConsumer;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.NamespaceNotFoundException;
import org.apache.accumulo.core.util.Validator;
import org.apache.accumulo.core.zookeeper.ZooUtil;
import org.apache.accumulo.fate.zookeeper.ZooCache;
import org.apache.accumulo.fate.zookeeper.ZooCacheFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Namespaces {
  private static final Logger log = LoggerFactory.getLogger(Namespaces.class);

  public static final String VALID_NAME_REGEX = "^\\w*$";
  public static final Validator<String> VALID_NAME = new Validator<String>() {
    @Override
    public boolean test(String namespace) {
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
    public boolean test(String namespace) {
      return !Namespace.DEFAULT.equals(namespace);
    }

    @Override
    public String invalidMessage(String namespace) {
      return "Namespace cannot be the reserved empty namespace";
    }
  };

  public static final Validator<String> NOT_ACCUMULO = new Validator<String>() {
    @Override
    public boolean test(String namespace) {
      return !Namespace.ACCUMULO.equals(namespace);
    }

    @Override
    public String invalidMessage(String namespace) {
      return "Namespace cannot be the reserved namespace, " + Namespace.ACCUMULO;
    }
  };

  private static ZooCache getZooCache(Instance instance) {
    return new ZooCacheFactory().getZooCache(instance.getZooKeepers(), instance.getZooKeepersSessionTimeOut());
  }

  public static boolean exists(Instance instance, Namespace.ID namespaceId) {
    ZooCache zc = getZooCache(instance);
    List<String> namespaceIds = zc.getChildren(ZooUtil.getRoot(instance) + Constants.ZNAMESPACES);
    return namespaceIds.contains(namespaceId.canonicalID());
  }

  public static List<Table.ID> getTableIds(Instance instance, Namespace.ID namespaceId) throws NamespaceNotFoundException {
    String namespace = getNamespaceName(instance, namespaceId);
    List<Table.ID> tableIds = new LinkedList<>();
    for (Entry<String,Table.ID> nameToId : Tables.getNameToIdMap(instance).entrySet())
      if (namespace.equals(Tables.qualify(nameToId.getKey()).getFirst()))
        tableIds.add(nameToId.getValue());
    return tableIds;
  }

  public static List<String> getTableNames(Instance instance, Namespace.ID namespaceId) throws NamespaceNotFoundException {
    String namespace = getNamespaceName(instance, namespaceId);
    List<String> names = new LinkedList<>();
    for (String name : Tables.getNameToIdMap(instance).keySet())
      if (namespace.equals(Tables.qualify(name).getFirst()))
        names.add(name);
    return names;
  }

  /**
   * Gets all the namespaces from ZK. The first arg (t) the BiConsumer accepts is the ID and the second (u) is the namespaceName.
   */
  private static void getAllNamespaces(Instance instance, BiConsumer<String,String> biConsumer) {
    final ZooCache zc = getZooCache(instance);
    List<String> namespaceIds = zc.getChildren(ZooUtil.getRoot(instance) + Constants.ZNAMESPACES);
    for (String id : namespaceIds) {
      byte[] path = zc.get(ZooUtil.getRoot(instance) + Constants.ZNAMESPACES + "/" + id + Constants.ZNAMESPACE_NAME);
      if (path != null) {
        biConsumer.accept(id, new String(path, UTF_8));
      }
    }
  }

  /**
   * Return sorted map with key = ID, value = namespaceName
   */
  public static SortedMap<Namespace.ID,String> getIdToNameMap(Instance instance) {
    SortedMap<Namespace.ID,String> idMap = new TreeMap<>();
    getAllNamespaces(instance, (id, name) -> idMap.put(Namespace.ID.of(id), name));
    return idMap;
  }

  /**
   * Return sorted map with key = namespaceName, value = ID
   */
  public static SortedMap<String,Namespace.ID> getNameToIdMap(Instance instance) {
    SortedMap<String,Namespace.ID> nameMap = new TreeMap<>();
    getAllNamespaces(instance, (id, name) -> nameMap.put(name, Namespace.ID.of(id)));
    return nameMap;
  }

  /**
   * Look for namespace ID in ZK. Throw NamespaceNotFoundException if not found.
   */
  public static Namespace.ID getNamespaceId(Instance instance, String namespaceName) throws NamespaceNotFoundException {
    final ArrayList<Namespace.ID> singleId = new ArrayList<>(1);
    getAllNamespaces(instance, (id, name) -> {
      if (name.equals(namespaceName))
        singleId.add(Namespace.ID.of(id));
    });
    if (singleId.isEmpty())
      throw new NamespaceNotFoundException(null, namespaceName, "getNamespaceId() failed to find namespace");
    return singleId.get(0);
  }

  /**
   * Look for namespace ID in ZK. Fail quietly by logging and returning null.
   */
  public static Namespace.ID lookupNamespaceId(Instance instance, String namespaceName) {
    Namespace.ID id = null;
    try {
      id = getNamespaceId(instance, namespaceName);
    } catch (NamespaceNotFoundException e) {
      if (log.isDebugEnabled())
        log.debug("Failed to find namespace ID from name: " + namespaceName, e);
    }
    return id;
  }

  /**
   * Return true if namespace name exists
   */
  public static boolean namespaceNameExists(Instance instance, String namespaceName) {
    return lookupNamespaceId(instance, namespaceName) != null;
  }

  /**
   * Look for namespace name in ZK. Throw NamespaceNotFoundException if not found.
   */
  public static String getNamespaceName(Instance instance, Namespace.ID namespaceId) throws NamespaceNotFoundException {
    String name;
    ZooCache zc = getZooCache(instance);
    byte[] path = zc.get(ZooUtil.getRoot(instance) + Constants.ZNAMESPACES + "/" + namespaceId.canonicalID() + Constants.ZNAMESPACE_NAME);
    if (path != null)
      name = new String(path, UTF_8);
    else
      throw new NamespaceNotFoundException(namespaceId.canonicalID(), null, "getNamespaceName() failed to find namespace");
    return name;
  }

}

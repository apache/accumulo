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
package org.apache.accumulo.core.clientImpl;

import java.util.LinkedList;
import java.util.List;
import java.util.Map.Entry;
import java.util.SortedMap;

import org.apache.accumulo.core.client.NamespaceNotFoundException;
import org.apache.accumulo.core.data.NamespaceId;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.util.tables.TableNameUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Namespaces {
  private static final Logger log = LoggerFactory.getLogger(Namespaces.class);

  public static boolean exists(ClientContext context, NamespaceId namespaceId) {
    return context.getNamespaces().getIdToNameMap().containsKey(namespaceId);
  }

  public static List<TableId> getTableIds(ClientContext context, NamespaceId namespaceId)
      throws NamespaceNotFoundException {
    String namespace = getNamespaceName(context, namespaceId);
    List<TableId> tableIds = new LinkedList<>();
    for (Entry<String,TableId> nameToId : context.getTableNameToIdMap().entrySet()) {
      if (namespace.equals(TableNameUtil.qualify(nameToId.getKey()).getFirst())) {
        tableIds.add(nameToId.getValue());
      }
    }
    return tableIds;
  }

  public static List<String> getTableNames(ClientContext context, NamespaceId namespaceId)
      throws NamespaceNotFoundException {
    String namespace = getNamespaceName(context, namespaceId);
    List<String> names = new LinkedList<>();
    for (String name : context.getTableNameToIdMap().keySet()) {
      if (namespace.equals(TableNameUtil.qualify(name).getFirst())) {
        names.add(name);
      }
    }
    return names;
  }

  /**
   * Return sorted map with key = ID, value = namespaceName
   */
  public static SortedMap<NamespaceId,String> getIdToNameMap(ClientContext context) {
    return context.getNamespaces().getIdToNameMap();
  }

  /**
   * Return sorted map with key = namespaceName, value = ID
   */
  public static SortedMap<String,NamespaceId> getNameToIdMap(ClientContext context) {
    return context.getNamespaces().getNameToIdMap();
  }

  /**
   * Look for namespace ID in ZK. Throw NamespaceNotFoundException if not found.
   */
  public static NamespaceId getNamespaceId(ClientContext context, String namespaceName)
      throws NamespaceNotFoundException {
    var id = context.getNamespaces().getNameToIdMap().get(namespaceName);
    if (id == null) {
      throw new NamespaceNotFoundException(null, namespaceName,
          "getNamespaceId() failed to find namespace");
    }
    return id;
  }

  /**
   * Look for namespace ID in ZK. Fail quietly by logging and returning null.
   */
  public static NamespaceId lookupNamespaceId(ClientContext context, String namespaceName) {
    NamespaceId id = null;
    try {
      id = getNamespaceId(context, namespaceName);
    } catch (NamespaceNotFoundException e) {
      if (log.isDebugEnabled()) {
        log.debug("Failed to find namespace ID from name: " + namespaceName, e);
      }
    }
    return id;
  }

  /**
   * Return true if namespace name exists
   */
  public static boolean namespaceNameExists(ClientContext context, String namespaceName) {
    return lookupNamespaceId(context, namespaceName) != null;
  }

  /**
   * Look for namespace name in ZK. Throw NamespaceNotFoundException if not found.
   */
  public static String getNamespaceName(ClientContext context, NamespaceId namespaceId)
      throws NamespaceNotFoundException {
    String name = getIdToNameMap(context).get(namespaceId);
    if (name == null) {
      throw new NamespaceNotFoundException(namespaceId.canonical(), null,
          "getNamespaceName() failed to find namespace");
    }
    return name;
  }

}

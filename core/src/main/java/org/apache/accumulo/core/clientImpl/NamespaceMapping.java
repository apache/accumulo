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

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Collections.emptySortedMap;

import java.lang.reflect.Type;
import java.util.Map;
import java.util.SortedMap;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.clientImpl.thrift.TableOperation;
import org.apache.accumulo.core.clientImpl.thrift.TableOperationExceptionType;
import org.apache.accumulo.core.data.NamespaceId;
import org.apache.accumulo.core.fate.zookeeper.ZooCache;
import org.apache.accumulo.core.fate.zookeeper.ZooReaderWriter;
import org.apache.zookeeper.KeeperException;

import com.google.common.collect.ImmutableSortedMap;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;

public class NamespaceMapping {
  private static final Gson gson = new Gson();
  private final ClientContext context;
  private volatile SortedMap<NamespaceId,String> currentNamespaceMap = emptySortedMap();
  private volatile SortedMap<String,NamespaceId> currentNamespaceReverseMap = emptySortedMap();
  private volatile long lastMzxid;

  public NamespaceMapping(ClientContext context) {
    this.context = context;
  }

  public static void put(ZooReaderWriter zoo, String zPath, NamespaceId namespaceId,
      String namespaceName)
      throws InterruptedException, KeeperException, AcceptableThriftTableOperationException {
    if (Namespace.DEFAULT.id().equals(namespaceId) || Namespace.ACCUMULO.id().equals(namespaceId)) {
      throw new AssertionError(
          "Putting built-in namespaces in map should not be possible after init");
    }
    zoo.mutateExisting(zPath, data -> {
      var namespaces = deserialize(data);
      if (namespaces.containsKey(namespaceId.canonical())) {
        throw new AcceptableThriftTableOperationException(null, namespaceId.canonical(),
            TableOperation.CREATE, TableOperationExceptionType.NAMESPACE_EXISTS,
            "Namespace Id already exists");
      }
      namespaces.put(namespaceId.canonical(), namespaceName);
      return serialize(namespaces);
    });
  }

  public static void remove(ZooReaderWriter zoo, String zPath, NamespaceId namespaceId)
      throws InterruptedException, KeeperException, AcceptableThriftTableOperationException {
    zoo.mutateExisting(zPath, data -> {
      var namespaces = NamespaceMapping.deserialize(data);
      if (!namespaces.containsKey(namespaceId.canonical())) {
        throw new AcceptableThriftTableOperationException(null, namespaceId.canonical(),
            TableOperation.DELETE, TableOperationExceptionType.NAMESPACE_NOTFOUND,
            "Namespace already removed while processing");
      }
      namespaces.remove(namespaceId.canonical());
      return NamespaceMapping.serialize(namespaces);
    });
  }

  public static void rename(ZooReaderWriter zoo, String zPath, NamespaceId namespaceId,
      String oldName, String newName)
      throws InterruptedException, KeeperException, AcceptableThriftTableOperationException {
    zoo.mutateExisting(zPath, current -> {
      var currentNamespaceMap = NamespaceMapping.deserialize(current);
      final String currentName = currentNamespaceMap.get(namespaceId.canonical());
      if (currentName.equals(newName)) {
        return null; // assume in this case the operation is running again, so we are done
      }
      if (!currentName.equals(oldName)) {
        throw new AcceptableThriftTableOperationException(null, oldName, TableOperation.RENAME,
            TableOperationExceptionType.NAMESPACE_NOTFOUND, "Name changed while processing");
      }
      currentNamespaceMap.put(namespaceId.canonical(), newName);
      return NamespaceMapping.serialize(currentNamespaceMap);
    });
  }

  public static byte[] serialize(Map<String,String> map) {
    var sortedMap = ImmutableSortedMap.<String,String>naturalOrder().putAll(map).build();
    String jsonData = gson.toJson(sortedMap);
    return jsonData.getBytes(UTF_8);
  }

  public static Map<String,String> deserialize(byte[] data) {
    if (data == null) {
      throw new AssertionError("/namespaces node should not be null");
    }
    String jsonData = new String(data, UTF_8);
    Type type = new TypeToken<Map<String,String>>() {}.getType();
    return gson.fromJson(jsonData, type);
  }

  private synchronized void update() {
    final ZooCache zc = context.getZooCache();
    final String zPath = context.getZooKeeperRoot() + Constants.ZNAMESPACES;
    final ZooCache.ZcStat stat = new ZooCache.ZcStat();

    byte[] data = zc.get(zPath, stat);
    if (stat.getMzxid() > lastMzxid) {
      if (data == null) {
        throw new IllegalStateException("namespaces node should not be null");
      } else {
        Map<String,String> idToName = deserialize(data);
        if (!idToName.containsKey(Namespace.DEFAULT.id().canonical())
            || !idToName.containsKey(Namespace.ACCUMULO.id().canonical())) {
          throw new IllegalStateException("Built-in namespace is not present in map");
        }
        var converted = ImmutableSortedMap.<NamespaceId,String>naturalOrder();
        var convertedReverse = ImmutableSortedMap.<String,NamespaceId>naturalOrder();
        idToName.forEach((idString, name) -> {
          var id = NamespaceId.of(idString);
          converted.put(id, name);
          convertedReverse.put(name, id);
        });
        currentNamespaceMap = converted.build();
        currentNamespaceReverseMap = convertedReverse.build();
      }
      lastMzxid = stat.getMzxid();
    }
  }

  public SortedMap<NamespaceId,String> getIdToNameMap() {
    update();
    return currentNamespaceMap;
  }

  public SortedMap<String,NamespaceId> getNameToIdMap() {
    update();
    return currentNamespaceReverseMap;
  }

}

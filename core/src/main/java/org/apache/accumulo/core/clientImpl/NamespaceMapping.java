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
import static java.util.Objects.requireNonNull;
import static org.apache.accumulo.core.util.LazySingletons.GSON;

import java.lang.reflect.Type;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.clientImpl.thrift.TableOperation;
import org.apache.accumulo.core.clientImpl.thrift.TableOperationExceptionType;
import org.apache.accumulo.core.data.NamespaceId;
import org.apache.accumulo.core.fate.zookeeper.ZooCache;
import org.apache.accumulo.core.fate.zookeeper.ZooReaderWriter;
import org.apache.zookeeper.KeeperException;

import com.google.common.collect.ImmutableSortedMap;
import com.google.gson.reflect.TypeToken;

public class NamespaceMapping {
  // type token must represent a mutable type, so it can be altered in the mutateExisting methods
  // without needing to make a copy
  private static final Type MAP_TYPE = new TypeToken<TreeMap<String,String>>() {}.getType();
  private final ClientContext context;
  private volatile SortedMap<NamespaceId,String> currentNamespaceMap = emptySortedMap();
  private volatile SortedMap<String,NamespaceId> currentNamespaceReverseMap = emptySortedMap();
  private volatile long lastMzxid;

  public NamespaceMapping(ClientContext context) {
    this.context = context;
  }

  public static void put(final ZooReaderWriter zoo, final String zPath,
      final NamespaceId namespaceId, final String namespaceName)
      throws InterruptedException, KeeperException, AcceptableThriftTableOperationException {
    requireNonNull(zoo);
    requireNonNull(zPath);
    requireNonNull(namespaceId);
    requireNonNull(namespaceName);
    if (Namespace.DEFAULT.id().equals(namespaceId) || Namespace.ACCUMULO.id().equals(namespaceId)) {
      throw new AssertionError(
          "Putting built-in namespaces in map should not be possible after init");
    }
    zoo.mutateExisting(zPath, data -> {
      var namespaces = deserialize(data);
      final String currentName = namespaces.get(namespaceId.canonical());
      if (namespaceName.equals(currentName)) {
        return null; // mapping already exists; operation is idempotent, so no change needed
      }
      if (currentName != null) {
        throw new AcceptableThriftTableOperationException(null, namespaceId.canonical(),
            TableOperation.CREATE, TableOperationExceptionType.NAMESPACE_EXISTS,
            "Namespace Id already exists");
      }
      if (namespaces.containsValue(namespaceName)) {
        throw new AcceptableThriftTableOperationException(null, namespaceId.canonical(),
            TableOperation.CREATE, TableOperationExceptionType.NAMESPACE_EXISTS,
            "Namespace name already exists");
      }
      namespaces.put(namespaceId.canonical(), namespaceName);
      return serialize(namespaces);
    });
  }

  public static void remove(final ZooReaderWriter zoo, final String zPath,
      final NamespaceId namespaceId)
      throws InterruptedException, KeeperException, AcceptableThriftTableOperationException {
    requireNonNull(zoo);
    requireNonNull(zPath);
    requireNonNull(namespaceId);
    if (Namespace.DEFAULT.id().equals(namespaceId) || Namespace.ACCUMULO.id().equals(namespaceId)) {
      throw new AssertionError("Removing built-in namespaces in map should not be possible");
    }
    zoo.mutateExisting(zPath, data -> {
      var namespaces = deserialize(data);
      if (!namespaces.containsKey(namespaceId.canonical())) {
        throw new AcceptableThriftTableOperationException(null, namespaceId.canonical(),
            TableOperation.DELETE, TableOperationExceptionType.NAMESPACE_NOTFOUND,
            "Namespace already removed while processing");
      }
      namespaces.remove(namespaceId.canonical());
      return serialize(namespaces);
    });
  }

  public static void rename(final ZooReaderWriter zoo, final String zPath,
      final NamespaceId namespaceId, final String oldName, final String newName)
      throws InterruptedException, KeeperException, AcceptableThriftTableOperationException {
    requireNonNull(zoo);
    requireNonNull(zPath);
    requireNonNull(namespaceId);
    requireNonNull(oldName);
    requireNonNull(newName);
    if (Namespace.DEFAULT.id().equals(namespaceId) || Namespace.ACCUMULO.id().equals(namespaceId)) {
      throw new AssertionError("Renaming built-in namespaces in map should not be possible");
    }
    zoo.mutateExisting(zPath, current -> {
      var namespaces = deserialize(current);
      final String currentName = namespaces.get(namespaceId.canonical());
      if (newName.equals(currentName)) {
        return null; // mapping already exists; operation is idempotent, so no change needed
      }
      if (!oldName.equals(currentName)) {
        throw new AcceptableThriftTableOperationException(null, oldName, TableOperation.RENAME,
            TableOperationExceptionType.NAMESPACE_NOTFOUND, "Name changed while processing");
      }
      if (namespaces.containsValue(newName)) {
        throw new AcceptableThriftTableOperationException(null, newName, TableOperation.RENAME,
            TableOperationExceptionType.NAMESPACE_EXISTS, "Namespace name already exists");
      }
      namespaces.put(namespaceId.canonical(), newName);
      return serialize(namespaces);
    });
  }

  public static byte[] serialize(Map<String,String> map) {
    return GSON.get().toJson(new TreeMap<>(map), MAP_TYPE).getBytes(UTF_8);
  }

  public static Map<String,String> deserialize(byte[] data) {
    requireNonNull(data);
    return GSON.get().fromJson(new String(data, UTF_8), MAP_TYPE);
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

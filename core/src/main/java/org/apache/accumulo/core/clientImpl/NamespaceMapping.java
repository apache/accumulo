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
import org.apache.accumulo.core.data.NamespaceId;
import org.apache.accumulo.core.fate.zookeeper.ZooCache;
import org.apache.accumulo.core.fate.zookeeper.ZooReaderWriter;
import org.apache.accumulo.core.fate.zookeeper.ZooUtil;
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

  public static void initializeNamespaceMap(ZooReaderWriter zoo, String zPath)
      throws InterruptedException, KeeperException {
    Map<String,String> map = Map.of(Namespace.DEFAULT.id().canonical(), Namespace.DEFAULT.name(),
        Namespace.ACCUMULO.id().canonical(), Namespace.ACCUMULO.name());
    zoo.putPersistentData(zPath, serialize(map), ZooUtil.NodeExistsPolicy.OVERWRITE);
  }

  public static byte[] writeNamespaceToMap(ZooReaderWriter zoo, String zPath,
      NamespaceId namespaceId, String namespaceName) throws InterruptedException, KeeperException {
    byte[] updatedMap = new byte[0];
    if (!Namespace.DEFAULT.id().equals(namespaceId)
        && !Namespace.ACCUMULO.id().equals(namespaceId)) {
      byte[] data = zoo.getData(zPath);
      Map<String,String> namespaceMap = deserialize(data);
      namespaceMap.put(namespaceId.canonical(), namespaceName);
      updatedMap = serialize(namespaceMap);
    }
    return updatedMap;
  }

  public static byte[] serialize(Map<String,String> map) {
    String jsonData = gson.toJson(map);
    return jsonData.getBytes(UTF_8);
  }

  public static Map<String,String> deserialize(byte[] data) {
    String jsonData = new String(data, UTF_8);
    Type type = new TypeToken<Map<String,String>>() {}.getType();
    return gson.fromJson(jsonData, type);
  }

  private synchronized void update() {
    final ZooCache zc = context.getZooCache();
    final String zPath = context.getZooKeeperRoot() + Constants.ZNAMESPACES;
    final ZooCache.ZcStat stat = new ZooCache.ZcStat();
    zc.clear();

    // Retrieve the current data and stat from ZooCache
    byte[] data = zc.get(zPath, stat);
    if (stat.getMzxid() > lastMzxid) {
      if (data == null) {
        currentNamespaceMap = emptySortedMap();
        currentNamespaceReverseMap = emptySortedMap();
      } else {
        Map<String,String> idToName = deserialize(data);
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

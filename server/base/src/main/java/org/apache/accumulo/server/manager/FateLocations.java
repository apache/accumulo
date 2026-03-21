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
package org.apache.accumulo.server.manager;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.stream.Collectors.toUnmodifiableSet;
import static org.apache.accumulo.core.util.LazySingletons.GSON;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.fate.FateId;
import org.apache.accumulo.core.fate.FatePartition;
import org.apache.accumulo.core.fate.zookeeper.ZooReaderWriter;
import org.apache.accumulo.core.fate.zookeeper.ZooUtil.NodeExistsPolicy;
import org.apache.accumulo.server.ServerContext;
import org.apache.zookeeper.KeeperException;

import com.google.common.base.Preconditions;
import com.google.common.net.HostAndPort;
import com.google.common.reflect.TypeToken;

public class FateLocations {

  private final ServerContext context;

  private long lastUpdateCount;
  private Map<HostAndPort,Set<FatePartition>> lastLocations = null;

  public FateLocations(ServerContext context) {
    this.context = context;
  }

  public synchronized Map<HostAndPort,Set<FatePartition>> getLocations() {

    var zooCache = context.getZooCache();

    if (lastLocations == null || lastUpdateCount != zooCache.getUpdateCount()) {
      lastUpdateCount = zooCache.getUpdateCount();
      var json = new String(context.getZooCache().get(Constants.ZMANAGER_FATE_ASSIGNMENTS), UTF_8);
      var type = new TypeToken<Map<String,List<List<String>>>>() {}.getType();
      Map<String,List<List<String>>> stringMap = GSON.get().fromJson(json, type);
      Map<HostAndPort,Set<FatePartition>> locations = new HashMap<>();
      stringMap.forEach((hp, parts) -> {
        var partsSet = parts.stream().peek(part -> Preconditions.checkArgument(part.size() == 2))
            .map(part -> new FatePartition(FateId.from(part.get(0)), FateId.from(part.get(1))))
            .collect(toUnmodifiableSet());
        locations.put(HostAndPort.fromString(hp), partsSet);
      });
      lastLocations = Map.copyOf(locations);
    }

    return lastLocations;
  }

  private static byte[] serialize(Map<HostAndPort,Set<FatePartition>> assignments) {
    Map<String,List<List<String>>> jsonMap = new HashMap<>();
    assignments.forEach((hp, parts) -> {
      var listParts = parts.stream()
          .map(part -> List.of(part.start().canonical(), part.end().canonical())).toList();
      jsonMap.put(hp.toString(), listParts);
    });

    var json = GSON.get().toJson(jsonMap);
    return json.getBytes(UTF_8);
  }

  public static void storeLocations(ZooReaderWriter zoo,
      Map<HostAndPort,Set<FatePartition>> assignments, NodeExistsPolicy nodeExistsPolicy) {
    try {
      zoo.putPersistentData(Constants.ZMANAGER_FATE_ASSIGNMENTS, serialize(assignments),
          nodeExistsPolicy);
    } catch (KeeperException | InterruptedException e) {
      throw new IllegalStateException("Unable to set fate locations in zookeeper", e);
    }
  }

  public static void storeLocations(ServerContext context,
      Map<HostAndPort,Set<FatePartition>> assignments) {
    try {
      context.getZooSession().setData(Constants.ZMANAGER_FATE_ASSIGNMENTS, serialize(assignments),
          -1);
    } catch (KeeperException | InterruptedException e) {
      throw new IllegalStateException("Unable to set fate locations in zookeeper", e);
    }
  }

}

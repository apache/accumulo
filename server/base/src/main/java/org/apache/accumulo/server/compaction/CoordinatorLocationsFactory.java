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
package org.apache.accumulo.server.compaction;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.accumulo.core.util.LazySingletons.GSON;

import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.data.ResourceGroupId;
import org.apache.accumulo.core.fate.zookeeper.ZooReaderWriter;
import org.apache.accumulo.core.fate.zookeeper.ZooUtil.NodeExistsPolicy;
import org.apache.accumulo.server.ServerContext;
import org.apache.zookeeper.KeeperException;

import com.google.common.net.HostAndPort;
import com.google.common.reflect.TypeToken;

/**
 * Reads and writes a map of coordinators to zookeeper. The map contains compactor resource group to
 * manager address mappings.
 */
public class CoordinatorLocationsFactory {
  private final ServerContext context;

  private long lastUpdateCount;
  private CoordinatorLocations lastLocations;

  public CoordinatorLocationsFactory(ServerContext context) {
    this.context = context;
  }

  public record CoordinatorLocations(Map<ResourceGroupId,HostAndPort> locations,
      List<HostAndPort> sortedUniqueHost) {
  }

  public synchronized CoordinatorLocations getLocations() {
    var zooCache = context.getZooCache();
    if (lastLocations == null || lastUpdateCount != zooCache.getUpdateCount()) {
      lastUpdateCount = zooCache.getUpdateCount();
      byte[] serializedMap = zooCache.get(Constants.ZMANAGER_COORDINATOR);
      var type = new TypeToken<Map<String,String>>() {}.getType();
      Map<String,String> stringMap = GSON.get().fromJson(new String(serializedMap, UTF_8), type);
      Map<ResourceGroupId,HostAndPort> locations = new HashMap<>();
      stringMap
          .forEach((rg, hp) -> locations.put(ResourceGroupId.of(rg), HostAndPort.fromString(hp)));
      lastLocations = new CoordinatorLocations(Map.copyOf(locations), locations.values().stream()
          .distinct().sorted(Comparator.comparing(HostAndPort::toString)).toList());
    }
    return lastLocations;
  }

  public static void setLocations(ZooReaderWriter zk, Map<ResourceGroupId,HostAndPort> locations,
      NodeExistsPolicy nodeExistsPolicy) throws InterruptedException, KeeperException {
    Map<String,String> stringMap = new HashMap<>(locations.size());
    locations.forEach((rg, hp) -> {
      stringMap.put(rg.canonical(), hp.toString());
    });
    byte[] serializedMap = GSON.get().toJson(stringMap).getBytes(UTF_8);
    zk.putPersistentData(Constants.ZMANAGER_COORDINATOR, serializedMap, nodeExistsPolicy);
  }
}

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

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.data.AbstractId;
import org.apache.accumulo.core.data.ResourceGroupId;
import org.apache.accumulo.core.fate.zookeeper.ZooReaderWriter;
import org.apache.accumulo.core.fate.zookeeper.ZooUtil.NodeExistsPolicy;
import org.apache.accumulo.server.ServerContext;
import org.apache.zookeeper.KeeperException;

import com.google.common.base.Preconditions;
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

  public static final class CoordinatorLocations {
    private final Map<HostAndPort,Set<ResourceGroupId>> hostAssignments;
    private final Map<ResourceGroupId,HostAndPort> groupAssignments;
    private final List<HostAndPort> sortedUniqueHost;

    private CoordinatorLocations(Map<HostAndPort,List<ResourceGroupId>> assignments) {
      final HashSet<ResourceGroupId> seenGroups = new HashSet<>();
      final Map<HostAndPort,Set<ResourceGroupId>> hostAssignments = new HashMap<>();
      final Map<ResourceGroupId,HostAndPort> groupAssignments = new HashMap<>();
      final List<HostAndPort> sortedUniqueHost = new ArrayList<>(assignments.size());

      assignments.entrySet().forEach(e -> {
        var hostAndPort = e.getKey();
        var resourceGroups = e.getValue();
        resourceGroups.forEach(rgid -> {
          Preconditions.checkArgument(seenGroups.add(rgid), "Duplicated resource group %s %s", rgid,
              assignments);
          groupAssignments.put(rgid, hostAndPort);
        });
        hostAssignments.put(hostAndPort, Set.copyOf(resourceGroups));
        sortedUniqueHost.add(hostAndPort);
      });

      this.hostAssignments = Map.copyOf(hostAssignments);
      this.groupAssignments = Map.copyOf(groupAssignments);
      sortedUniqueHost.sort(Comparator.comparing(HostAndPort::toString));
      this.sortedUniqueHost = List.copyOf(sortedUniqueHost);
    }

    /**
     * @return the compactor resource groups assigned to each coordinator. If a coordinator has no
     *         assigned resource groups, then it should have an empty set as the value.
     */
    public Map<HostAndPort,Set<ResourceGroupId>> hostAssignments() {
      return hostAssignments;
    }

    /**
     * @return mapping of the coordinator that each resource group is assigned to, if a coordinator
     *         has no assigned resource groups then it will not show up in this map.
     */
    public Map<ResourceGroupId,HostAndPort> groupAssignments() {
      return groupAssignments;
    }

    /**
     * @return all coordinators, even coordinators with no assigned resource groups, in sorted
     *         order.
     */
    public List<HostAndPort> sortedUniqueHost() {
      return sortedUniqueHost;
    }
  }

  private static final Type MAP_TYPE = new TypeToken<Map<String,List<String>>>() {}.getType();

  public synchronized CoordinatorLocations getLocations() {
    var zooCache = context.getZooCache();
    if (lastLocations == null || lastUpdateCount != zooCache.getUpdateCount()) {
      lastUpdateCount = zooCache.getUpdateCount();
      byte[] serializedMap = zooCache.get(Constants.ZMANAGER_COORDINATOR);
      Map<String,List<String>> stringMap =
          GSON.get().fromJson(new String(serializedMap, UTF_8), MAP_TYPE);
      Map<HostAndPort,List<ResourceGroupId>> locations = new HashMap<>(stringMap.size());
      stringMap.forEach((hp, rgids) -> locations.put(HostAndPort.fromString(hp),
          rgids.stream().map(ResourceGroupId::of).toList()));
      lastLocations = new CoordinatorLocations(locations);
    }
    return lastLocations;
  }

  public static void setLocations(ZooReaderWriter zk,
      Map<HostAndPort,Set<ResourceGroupId>> locations, NodeExistsPolicy nodeExistsPolicy)
      throws InterruptedException, KeeperException {
    Map<String,List<String>> stringMap = new HashMap<>(locations.size());
    locations.forEach((hp, rgids) -> {
      stringMap.put(hp.toString(), rgids.stream().map(AbstractId::canonical).toList());
    });
    byte[] serializedMap = GSON.get().toJson(stringMap).getBytes(UTF_8);
    zk.putPersistentData(Constants.ZMANAGER_COORDINATOR, serializedMap, nodeExistsPolicy);
  }
}

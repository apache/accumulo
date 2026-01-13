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
package org.apache.accumulo.core.spi.scan;

import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.mapping;
import static java.util.stream.Collectors.toUnmodifiableSet;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.accumulo.core.data.ResourceGroupId;

import com.google.common.net.HostAndPort;

/**
 * An immutable snapshot of information about scan servers
 */
// This class is intentionally package private
class ScanServersSnapshot {

  private static class PerHostInfo {
    final List<String> hosts;
    final Map<String,List<String>> serversByHost;

    PerHostInfo(Collection<String> servers) {
      this.serversByHost = new HashMap<>();

      servers.forEach(server -> {
        var hp = HostAndPort.fromString(server);
        serversByHost.computeIfAbsent(hp.getHost(), h -> new ArrayList<>()).add(server);
      });

      hosts = List.copyOf(serversByHost.keySet());
    }

    List<String> getServersForHost(String host) {
      return Collections.unmodifiableList(serversByHost.getOrDefault(host, List.of()));
    }
  }

  // All code in this class assumes this map is an immutable snapshot and computes derivative data
  // structures from its data as needed.
  private final Map<ResourceGroupId,Set<String>> serversByGroup;
  private final Map<ResourceGroupId,PerHostInfo> perHostInfo;
  private final Map<ResourceGroupId,List<String>> serversByGroupList;

  private ScanServersSnapshot(Map<ResourceGroupId,Set<String>> serversByGroup) {
    this.serversByGroup = serversByGroup;
    this.perHostInfo = new ConcurrentHashMap<>();
    this.serversByGroupList = new ConcurrentHashMap<>();
  }

  Set<String> getServersForGroup(ResourceGroupId group) {
    return serversByGroup.getOrDefault(group, Set.of());
  }

  List<String> getServersForGroupAsList(ResourceGroupId group) {
    // compute the list as needed since it is not always needed
    return serversByGroupList.computeIfAbsent(group, g -> List.copyOf(getServersForGroup(g)));
  }

  private PerHostInfo getPerHostInfo(ResourceGroupId group) {
    return perHostInfo.computeIfAbsent(group, g -> new PerHostInfo(getServersForGroup(g)));
  }

  List<String> getHostsForGroup(ResourceGroupId group) {
    return getPerHostInfo(group).hosts;
  }

  List<String> getServersForHost(ResourceGroupId group, String host) {
    return getPerHostInfo(group).getServersForHost(host);
  }

  @Override
  public boolean equals(Object o) {
    if (o instanceof ScanServersSnapshot) {
      // only need to compare this fields as the other fields are derived from the information in
      // it.
      return serversByGroup.equals(((ScanServersSnapshot) o).serversByGroup);
    }
    return false;
  }

  @Override
  public int hashCode() {
    return serversByGroup.hashCode();
  }

  static ScanServersSnapshot from(Collection<ScanServerInfo> scanServerInfos) {
    Objects.requireNonNull(scanServerInfos);
    // create an initial immutable snapshot of the information
    var initialSnapshot =
        Map.copyOf(scanServerInfos.stream().collect(groupingBy(ScanServerInfo::getGroup,
            mapping(ScanServerInfo::getAddress, toUnmodifiableSet()))));
    return new ScanServersSnapshot(initialSnapshot);
  }

  static ScanServersSnapshot from(Map<ResourceGroupId,Set<String>> serversByGroup) {
    var copy = new HashMap<ResourceGroupId,Set<String>>();
    serversByGroup.forEach((k, v) -> copy.put(k, Set.copyOf(v)));
    return new ScanServersSnapshot(Map.copyOf(copy));
  }
}

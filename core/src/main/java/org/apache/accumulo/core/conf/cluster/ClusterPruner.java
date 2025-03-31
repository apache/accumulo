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
package org.apache.accumulo.core.conf.cluster;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.accumulo.core.clientImpl.ClientContext;
import org.apache.accumulo.core.util.HostAndPort;
import org.apache.accumulo.core.util.compaction.ExternalCompactionUtil;

/**
 * Finds extra server process that exists in zookeeper but are not specified by the cluster yaml
 * config.
 */
public class ClusterPruner {
  public static void main(String[] args) throws Exception {
    // TODO validate args and usage
    String clusterYaml = args[0];
    String accumuoProps = args[1];
    boolean removeLocks = Boolean.parseBoolean(args[2]);

    ClientContext context = getContext(accumuoProps);

    // get the actual counts from zookeeper
    Map<ClusterKey,Integer> actualCounts = getActualCounts(context);

    // get the desired counts from cluster yaml
    Map<ClusterKey,Integer> desiredCounts = getDesiredCounts(clusterYaml);

    actualCounts.forEach((ckey, actual) -> {
      int desired = desiredCounts.getOrDefault(ckey, 0);
      if (actual > desired) {
        // TODO would be much better to sort output, so use tree map and make ClusterKey comparable
        System.out.printf("Saw %d extra servers on %s %s %s", (actual - desired), ckey.serverType,
            ckey.group, ckey.host);
        if (removeLocks) {
          removeLocks(context, ckey, actual - desired);
        }
      }
    });
  }

  private static ClientContext getContext(String accumuoProps) {
    // TODO should this code be in a server package so that it can create a server context?
    return null;
  }

  private static void removeLocks(ClientContext context, ClusterKey ckey, int numLocksToRemove) {
    // TODO remove locks
  }

  private static Map<ClusterKey,Integer> getActualCounts(ClientContext context) {
    // TODO need to get counts for scan and tablet servers. This will be much easier to do in main.
    Map<String,List<HostAndPort>> compactors = ExternalCompactionUtil.getCompactorAddrs(context);
    var counts = new HashMap<ClusterKey,Integer>();

    compactors.forEach((group, hosts) -> {
      hosts.forEach(hostPort -> {
        var ckey = new ClusterKey("compactor", group, hostPort.getHost());
        counts.merge(ckey, 1, Integer::sum);
      });
    });

    return counts;
  }

  private static Map<ClusterKey,Integer> getDesiredCounts(String clusterYaml) throws Exception {
    // TODO need to get counts for scan and tablet servers
    Map<String,String> config = ClusterConfigParser.parseConfiguration(clusterYaml);

    String compactorPrefix = "compaction.compactor.";
    Set<String> compactorQueues =
        config.keySet().stream().filter(k -> k.startsWith(compactorPrefix))
            .map(k -> k.substring(compactorPrefix.length())).collect(Collectors.toSet());
    var counts = new HashMap<ClusterKey,Integer>();
    for (String queue : compactorQueues) {
      // TODO not sure this parsing of the list is correct, may need to get rid of spaces
      String[] hosts = config.get("compaction.compactor." + queue).split(",");
      int numCompactors =
          Integer.parseInt(config.getOrDefault("compactors_per_host." + queue, "1"));

      for (String host : hosts) {
        counts.put(new ClusterKey("compactor", queue, host), numCompactors);
      }
    }

    return counts;
  }

  public static class ClusterKey {

    final String serverType;
    final String group;
    final String host;

    public ClusterKey(String serverType, String group, String host) {
      this.serverType = serverType;
      this.group = group;
      this.host = host;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o)
        return true;
      if (!(o instanceof ClusterKey))
        return false;
      ClusterKey that = (ClusterKey) o;
      return Objects.equals(host, that.host) && Objects.equals(serverType, that.serverType)
          && Objects.equals(group, that.group);
    }

    @Override
    public int hashCode() {
      return Objects.hash(host, serverType, group);
    }
  }
}

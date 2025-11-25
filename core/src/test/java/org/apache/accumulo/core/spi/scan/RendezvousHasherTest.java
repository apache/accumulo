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

import static java.util.stream.Collectors.toSet;
import static org.apache.accumulo.core.spi.scan.ConfigurableScanServerSelectorTest.nti;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.accumulo.core.spi.scan.RendezvousHasher.Mode;
import org.apache.accumulo.core.util.HostAndPort;
import org.junit.jupiter.api.Test;

public class RendezvousHasherTest {
  @Test
  public void testSameServerCountPerHost() {
    Map<String,Set<String>> serversByGroup = new HashMap<>();

    Set<String> g1Servers = new HashSet<>();
    for (int h = 0; h < 10; h++) {
      String host = "g1h" + h;
      for (int p = 1000; p < 1005; p++) {
        g1Servers.add(host + ":" + p);
      }
    }
    serversByGroup.put("g1", g1Servers);

    Set<String> g2Servers = Set.of("g2h1:5000", "g2h1:5001", "g2h1:5002");
    serversByGroup.put("g2", g2Servers);

    ScanServersSnapshot snapshot = ScanServersSnapshot.from(serversByGroup);
    RendezvousHasher hasher = new RendezvousHasher(snapshot, 1_000_000);

    for (int i = 1; i < 5; i++) {
      var g2rh = hasher.rendezvous(Mode.SERVER, "g2", nti("1", "e"), "s1", i);
      assertEquals(Math.min(i, 3), g2rh.size());
      assertTrue(g2Servers.containsAll(g2rh));

      g2rh = hasher.rendezvous(Mode.HOST, "g2", nti("1", "e"), "s1", i);
      assertEquals(List.of("g2h1"), g2rh);
    }

    int diffCount = 0;

    Map<String,Integer> useCounts = new HashMap<>();

    for (int t = 0; t < 200; t++) {
      var tablet = nti("1", "" + t);
      var servers_s1_1 = Set.copyOf(hasher.rendezvous(Mode.SERVER, "g1", tablet, "s1", 1));
      var servers_s1_2 = Set.copyOf(hasher.rendezvous(Mode.SERVER, "g1", tablet, "s1", 2));
      var servers_s1_3 = Set.copyOf(hasher.rendezvous(Mode.SERVER, "g1", tablet, "s1", 3));
      // the same tablet and salt should hash to the same servers
      assertEquals(servers_s1_1, Set.copyOf(hasher.rendezvous(Mode.SERVER, "g1", tablet, "s1", 1)));
      assertEquals(servers_s1_2, Set.copyOf(hasher.rendezvous(Mode.SERVER, "g1", tablet, "s1", 2)));
      assertEquals(servers_s1_3, Set.copyOf(hasher.rendezvous(Mode.SERVER, "g1", tablet, "s1", 3)));
      assertEquals(1, servers_s1_1.size());
      assertEquals(2, servers_s1_2.size());
      assertEquals(3, servers_s1_3.size());
      // should contain the smaller set of servers
      assertTrue(servers_s1_2.containsAll(servers_s1_1));
      assertTrue(servers_s1_3.containsAll(servers_s1_2));

      assertTrue(g1Servers.containsAll(servers_s1_1));
      assertTrue(g1Servers.containsAll(servers_s1_2));
      assertTrue(g1Servers.containsAll(servers_s1_3));

      for (var server : servers_s1_3) {
        useCounts.merge(server, 1, Integer::sum);
      }

      var hosts_s1_1 = hasher.rendezvous(Mode.HOST, "g1", tablet, "s1", 1);
      var hosts_s1_2 = hasher.rendezvous(Mode.HOST, "g1", tablet, "s1", 2);
      var hosts_s1_3 = hasher.rendezvous(Mode.HOST, "g1", tablet, "s1", 3);
      // the same tablet and salt should hash to the same hosts
      assertEquals(hosts_s1_1, hasher.rendezvous(Mode.HOST, "g1", tablet, "s1", 1));
      assertEquals(hosts_s1_2, hasher.rendezvous(Mode.HOST, "g1", tablet, "s1", 2));
      assertEquals(hosts_s1_3, hasher.rendezvous(Mode.HOST, "g1", tablet, "s1", 3));
      assertEquals(1, hosts_s1_1.size());
      assertEquals(2, hosts_s1_2.size());
      assertEquals(3, hosts_s1_3.size());
      assertTrue(hosts_s1_2.containsAll(hosts_s1_1));
      assertTrue(hosts_s1_3.containsAll(hosts_s1_2));
      assertTrue(hosts_s1_3.stream().noneMatch(h -> h.contains(":")));

      // the way these two are computed the hosts within the servers should be a subset of the
      // rendezvous host
      assertEquals(hosts_s1_1.iterator().next(),
          HostAndPort.fromString(servers_s1_1.iterator().next()).getHost());
      assertTrue(Set.copyOf(hosts_s1_2).containsAll(
          servers_s1_2.stream().map(s -> HostAndPort.fromString(s).getHost()).collect(toSet())));
      assertTrue(Set.copyOf(hosts_s1_3).containsAll(
          servers_s1_3.stream().map(s -> HostAndPort.fromString(s).getHost()).collect(toSet())));

      // try a different salt, should usually result in different servers
      var servers_s2_3 = Set.copyOf(hasher.rendezvous(Mode.SERVER, "g1", tablet, "s2", 3));
      assertEquals(servers_s2_3, Set.copyOf(hasher.rendezvous(Mode.SERVER, "g1", tablet, "s2", 3)));
      assertEquals(3, servers_s2_3.size());
      assertTrue(g1Servers.containsAll(servers_s2_3));
      if (!servers_s1_3.equals(servers_s2_3)) {
        diffCount++;
      }
    }

    var stats = useCounts.values().stream().mapToInt(i -> i).summaryStatistics();
    assertEquals(50, stats.getCount());
    assertEquals(12, stats.getAverage());
    assertEquals(7, stats.getMin());
    assertEquals(18, stats.getMax());

    assertEquals(200, diffCount);
  }

  @Test
  public void testDifferentServerCountPerHost() {
    Map<String,Set<String>> serversByGroup = new HashMap<>();

    Set<String> g1Servers = new HashSet<>();
    // 10 host with 10 servers
    for (int h = 0; h < 10; h++) {
      String host = "g1h" + h;
      for (int p = 1000; p < 1010; p++) {
        g1Servers.add(host + ":" + p);
      }
    }
    // four host w/ two servers
    g1Servers.add("gh1a:1000");
    g1Servers.add("gh1a:1001");
    g1Servers.add("gh1b:1000");
    g1Servers.add("gh1b:1001");
    g1Servers.add("gh1c:1000");
    g1Servers.add("gh1c:1001");
    g1Servers.add("gh1d:1000");
    g1Servers.add("gh1d:1001");

    serversByGroup.put("g1", g1Servers);

    ScanServersSnapshot snapshot = ScanServersSnapshot.from(serversByGroup);
    RendezvousHasher hasher = new RendezvousHasher(snapshot, 1_000_000);

    Map<String,Integer> useCounts = new HashMap<>();

    for (int t = 0; t < 10_000; t++) {
      var tablet = nti("1", "" + t);
      var hosts_s1_3 = hasher.rendezvous(Mode.HOST, "g1", tablet, "s1", 3);
      for (var host : hosts_s1_3) {
        useCounts.merge(host, 1, Integer::sum);
      }
    }

    // Verify the hosts with only 2 servers gets picked a lot less than the host with 10 servers.
    // There are 108 total servers, ideally the 4 hosts with only 2 servers would get
    // 8.0/108/.0=7.4% of the total usage and not 4/14=29% based being 4 of 14 host.
    var stats = useCounts.entrySet().stream().filter(e -> !e.getKey().matches("gh1[abcd]"))
        .mapToInt(Map.Entry::getValue).summaryStatistics();

    // the 10 host w/ 10 servers should get around 30,000 * 100.0 / 108.0 / 10 = 2777 usage each
    assertTrue(stats.getAverage() > 2777 - 100 && stats.getAverage() < 2777 + 100);
    assertTrue(stats.getMin() > 2777 - 200);
    assertTrue(stats.getMax() < 2777 + 200);

    // the 4 host w/ 2 servers should get around 30,000 * 8/108/4 = 555 usage each. Experimenting w/
    // different scenarios, anecdotally this function seems to skew higher than the desired average.
    // Suspect this is an artifact of the greedy algorithm used to implement host base rendezvous
    // hashing.
    var stats2 = useCounts.entrySet().stream().filter(e -> e.getKey().matches("gh1[abcd]"))
        .mapToInt(Map.Entry::getValue).summaryStatistics();
    assertTrue(stats2.getAverage() > 555 - 100 && stats2.getAverage() < 555 + 100);
    assertTrue(stats2.getMin() > 555 - 200);
    assertTrue(stats2.getMax() < 555 + 200);

    // the two groups of hosts should get all usage
    assertEquals(30_000, stats2.getSum() + stats.getSum());
  }
}

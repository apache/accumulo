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
package org.apache.accumulo.core.spi.balancer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.function.Function;

import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.data.TabletId;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.dataImpl.TabletIdImpl;
import org.apache.accumulo.core.manager.balancer.BalanceParamsImpl;
import org.apache.accumulo.core.manager.balancer.TServerStatusImpl;
import org.apache.accumulo.core.manager.balancer.TabletServerIdImpl;
import org.apache.accumulo.core.spi.balancer.data.TServerStatus;
import org.apache.accumulo.core.spi.balancer.data.TabletMigration;
import org.apache.accumulo.core.spi.balancer.data.TabletServerId;
import org.apache.accumulo.core.util.MapCounter;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.Test;

public class GroupBalancerTest {

  private static final SecureRandom random = new SecureRandom();

  private static final Function<TabletId,String> partitioner =
      input -> (input == null || input.getEndRow() == null) ? null
          : input.getEndRow().toString().substring(0, 2);

  public static class TabletServers {
    private final Set<TabletServerId> tservers = new HashSet<>();
    private final Map<TabletId,TabletServerId> tabletLocs = new HashMap<>();

    public void addTservers(String... locs) {
      for (String loc : locs) {
        int idx = loc.indexOf(':');
        addTserver(loc.substring(0, idx), Integer.parseInt(loc.substring(idx + 1)));
      }
    }

    public void addTserver(String host, int port) {
      tservers.add(new TabletServerIdImpl(host, port, Long.toHexString(6)));
    }

    public void addTablet(String er, String host, int port) {
      TabletServerId tsi = new TabletServerIdImpl(host, port, Long.toHexString(6));
      tabletLocs.put(
          new TabletIdImpl(new KeyExtent(TableId.of("b"), er == null ? null : new Text(er), null)),
          new TabletServerIdImpl(host, port, Long.toHexString(6)));
      tservers.add(tsi);
    }

    public void balance() {
      balance(10000);
    }

    public void balance(final int maxMigrations) {
      GroupBalancer balancer = new GroupBalancer(TableId.of("1")) {

        @Override
        protected Map<TabletId,TabletServerId> getLocationProvider() {
          return tabletLocs;
        }

        @Override
        protected Function<TabletId,String> getPartitioner() {
          return partitioner;
        }

        @Override
        protected long getWaitTime() {
          return 0;
        }

        @Override
        protected int getMaxMigrations() {
          return maxMigrations;
        }
      };

      balance(balancer, maxMigrations);
    }

    public void balance(TabletBalancer balancer, int maxMigrations) {

      while (true) {
        Set<TabletId> migrations = new HashSet<>();
        List<TabletMigration> migrationsOut = new ArrayList<>();
        SortedMap<TabletServerId,TServerStatus> current = new TreeMap<>();

        for (TabletServerId tsi : tservers) {
          current.put(tsi, new TServerStatusImpl(
              new org.apache.accumulo.core.master.thrift.TabletServerStatus()));
        }

        balancer.balance(new BalanceParamsImpl(current, migrations, migrationsOut));

        assertTrue(migrationsOut.size() <= (maxMigrations + 5),
            "Max Migration exceeded " + maxMigrations + " " + migrationsOut.size());

        for (TabletMigration tabletMigration : migrationsOut) {
          assertEquals(tabletLocs.get(tabletMigration.getTablet()),
              tabletMigration.getOldTabletServer());
          assertTrue(tservers.contains(tabletMigration.getNewTabletServer()));

          tabletLocs.put(tabletMigration.getTablet(), tabletMigration.getNewTabletServer());
        }

        if (migrationsOut.isEmpty()) {
          break;
        }
      }

      checkBalance();
    }

    void checkBalance() {
      MapCounter<String> groupCounts = new MapCounter<>();
      Map<TabletServerId,MapCounter<String>> tserverGroupCounts = new HashMap<>();

      for (Entry<TabletId,TabletServerId> entry : tabletLocs.entrySet()) {
        String group = partitioner.apply(entry.getKey());
        TabletServerId loc = entry.getValue();

        groupCounts.increment(group, 1);
        MapCounter<String> tgc = tserverGroupCounts.get(loc);
        if (tgc == null) {
          tgc = new MapCounter<>();
          tserverGroupCounts.put(loc, tgc);
        }

        tgc.increment(group, 1);
      }

      Map<String,Integer> expectedCounts = new HashMap<>();

      int totalExtra = 0;
      for (String group : groupCounts.keySet()) {
        long groupCount = groupCounts.get(group);
        totalExtra += groupCount % tservers.size();
        expectedCounts.put(group, (int) (groupCount / tservers.size()));
      }

      // The number of extra tablets from all groups that each tserver must have.
      int expectedExtra = totalExtra / tservers.size();
      int maxExtraGroups = expectedExtra + ((totalExtra % tservers.size() > 0) ? 1 : 0);

      for (Entry<TabletServerId,MapCounter<String>> entry : tserverGroupCounts.entrySet()) {
        MapCounter<String> tgc = entry.getValue();
        int tserverExtra = 0;
        for (String group : groupCounts.keySet()) {
          assertTrue(tgc.get(group) >= expectedCounts.get(group));
          assertTrue(tgc.get(group) <= expectedCounts.get(group) + 1,
              "Group counts not as expected group:" + group + " actual:" + tgc.get(group)
                  + " expected:" + (expectedCounts.get(group) + 1) + " tserver:" + entry.getKey());
          tserverExtra += tgc.get(group) - expectedCounts.get(group);
        }

        assertTrue(tserverExtra >= expectedExtra);
        assertTrue(tserverExtra <= maxExtraGroups);
      }
    }
  }

  @Test
  public void testSingleGroup() {

    String[][] tests = {new String[] {"a", "b", "c", "d"}, new String[] {"a", "b", "c"},
        new String[] {"a", "b", "c", "d", "e"}, new String[] {"a", "b", "c", "d", "e", "f", "g"},
        new String[] {"a", "b", "c", "d", "e", "f", "g", "h"},
        new String[] {"a", "b", "c", "d", "e", "f", "g", "h", "i"}, new String[] {"a"}};

    for (String[] suffixes : tests) {
      for (int maxTS = 1; maxTS <= 4; maxTS++) {
        TabletServers tservers = new TabletServers();
        int ts = 0;
        for (String s : suffixes) {
          tservers.addTablet("01" + s, "192.168.1." + ((ts++ % maxTS) + 1), 9997);
        }

        tservers.addTservers("192.168.1.2:9997", "192.168.1.3:9997", "192.168.1.4:9997");
        tservers.balance();
        tservers.balance();
      }
    }
  }

  @Test
  public void testTwoGroups() {
    String[][] tests = {new String[] {"a", "b", "c", "d"}, new String[] {"a", "b", "c"},
        new String[] {"a", "b", "c", "d", "e"}, new String[] {"a", "b", "c", "d", "e", "f", "g"},
        new String[] {"a", "b", "c", "d", "e", "f", "g", "h"},
        new String[] {"a", "b", "c", "d", "e", "f", "g", "h", "i"}, new String[] {"a"}};

    for (String[] suffixes1 : tests) {
      for (String[] suffixes2 : tests) {
        for (int maxTS = 1; maxTS <= 4; maxTS++) {
          TabletServers tservers = new TabletServers();
          int ts = 0;
          for (String s : suffixes1) {
            tservers.addTablet("01" + s, "192.168.1." + ((ts++ % maxTS) + 1), 9997);
          }

          for (String s : suffixes2) {
            tservers.addTablet("02" + s, "192.168.1." + ((ts++ % maxTS) + 1), 9997);
          }

          tservers.addTservers("192.168.1.2:9997", "192.168.1.3:9997", "192.168.1.4:9997");
          tservers.balance();
          tservers.balance();
        }
      }
    }
  }

  @Test
  public void testThreeGroups() {
    String[][] tests = {new String[] {"a", "b", "c", "d"}, new String[] {"a", "b", "c"},
        new String[] {"a", "b", "c", "d", "e"}, new String[] {"a", "b", "c", "d", "e", "f", "g"},
        new String[] {"a", "b", "c", "d", "e", "f", "g", "h"},
        new String[] {"a", "b", "c", "d", "e", "f", "g", "h", "i"}, new String[] {"a"}};

    for (String[] suffixes1 : tests) {
      for (String[] suffixes2 : tests) {
        for (String[] suffixes3 : tests) {
          for (int maxTS = 1; maxTS <= 4; maxTS++) {
            TabletServers tservers = new TabletServers();
            int ts = 0;
            for (String s : suffixes1) {
              tservers.addTablet("01" + s, "192.168.1." + ((ts++ % maxTS) + 1), 9997);
            }

            for (String s : suffixes2) {
              tservers.addTablet("02" + s, "192.168.1." + ((ts++ % maxTS) + 1), 9997);
            }

            for (String s : suffixes3) {
              tservers.addTablet("03" + s, "192.168.1." + ((ts++ % maxTS) + 1), 9997);
            }

            tservers.addTservers("192.168.1.2:9997", "192.168.1.3:9997", "192.168.1.4:9997");
            tservers.balance();
            tservers.balance();
          }
        }
      }
    }
  }

  @Test
  public void testManySingleTabletGroups() {

    for (int numGroups = 1; numGroups <= 13; numGroups++) {
      for (int maxTS = 1; maxTS <= 4; maxTS++) {
        TabletServers tservers = new TabletServers();
        int ts = 0;

        for (int group = 1; group <= numGroups; group++) {
          tservers.addTablet(String.format("%02d:p", group), "192.168.1." + ((ts++ % maxTS) + 1),
              9997);
        }

        tservers.addTservers("192.168.1.2:9997", "192.168.1.3:9997", "192.168.1.4:9997");

        tservers.balance();
        tservers.balance();
      }
    }
  }

  @Test
  public void testMaxMigrations() {

    for (int max : new int[] {1, 2, 3, 7, 10, 30}) {
      TabletServers tservers = new TabletServers();

      for (int i = 1; i <= 9; i++) {
        tservers.addTablet("01" + i, "192.168.1.1", 9997);
      }

      for (int i = 1; i <= 4; i++) {
        tservers.addTablet("02" + i, "192.168.1.2", 9997);
      }

      for (int i = 1; i <= 5; i++) {
        tservers.addTablet("03" + i, "192.168.1.3", 9997);
      }

      tservers.addTservers("192.168.1.4:9997", "192.168.1.5:9997");

      tservers.balance(max);
    }
  }

  @Test
  public void bigTest() {
    TabletServers tservers = new TabletServers();

    for (int g = 1; g <= 60; g++) {
      for (int t = 1; t <= 241; t++) {
        tservers.addTablet(String.format("%02d:%d", g, t), "192.168.1." + (random.nextInt(249) + 1),
            9997);
      }
    }

    for (int i = 1; i <= 250; i++) {
      tservers.addTserver("192.168.1." + i, 9997);
    }

    tservers.balance(1000);
  }

  @Test
  public void bigTest2() {
    TabletServers tservers = new TabletServers();

    for (int g = 1; g <= 60; g++) {
      for (int t = 1; t <= random.nextInt(1000); t++) {
        tservers.addTablet(String.format("%02d:%d", g, t), "192.168.1." + (random.nextInt(249) + 1),
            9997);
      }
    }

    for (int i = 1; i <= 250; i++) {
      tservers.addTserver("192.168.1." + i, 9997);
    }

    tservers.balance(1000);
  }
}

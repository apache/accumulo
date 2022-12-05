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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.data.TabletId;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.dataImpl.TabletIdImpl;
import org.apache.accumulo.core.manager.balancer.BalanceParamsImpl;
import org.apache.accumulo.core.manager.balancer.TServerStatusImpl;
import org.apache.accumulo.core.manager.balancer.TabletServerIdImpl;
import org.apache.accumulo.core.manager.balancer.TabletStatisticsImpl;
import org.apache.accumulo.core.master.thrift.TableInfo;
import org.apache.accumulo.core.spi.balancer.data.TServerStatus;
import org.apache.accumulo.core.spi.balancer.data.TabletMigration;
import org.apache.accumulo.core.spi.balancer.data.TabletServerId;
import org.apache.accumulo.core.spi.balancer.data.TabletStatistics;
import org.apache.accumulo.core.tabletserver.thrift.TabletStats;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class SimpleLoadBalancerTest {

  static class FakeTServer {
    List<TabletId> tablets = new ArrayList<>();

    TServerStatus getStatus() {
      org.apache.accumulo.core.master.thrift.TabletServerStatus result =
          new org.apache.accumulo.core.master.thrift.TabletServerStatus();
      result.tableMap = new HashMap<>();
      for (TabletId tabletId : tablets) {
        TableInfo info = result.tableMap.get(tabletId.getTable().canonical());
        if (info == null) {
          result.tableMap.put(tabletId.getTable().canonical(), info = new TableInfo());
        }
        info.onlineTablets++;
        info.recs = info.onlineTablets;
        info.ingestRate = 123.;
        info.queryRate = 456.;
      }
      return new TServerStatusImpl(result);
    }
  }

  Map<TabletServerId,FakeTServer> servers = new HashMap<>();
  Map<TabletId,TabletServerId> last = new HashMap<>();

  class TestSimpleLoadBalancer extends SimpleLoadBalancer {

    @Override
    protected List<TabletStatistics> getOnlineTabletsForTable(TabletServerId tserver,
        TableId tableId) {
      List<TabletStatistics> result = new ArrayList<>();
      for (TabletId tabletId : servers.get(tserver).tablets) {
        if (tabletId.getTable().equals(tableId)) {
          KeyExtent extent = new KeyExtent(tableId, tabletId.getEndRow(), tabletId.getPrevEndRow());
          TabletStats stats =
              new TabletStats(new TabletStats(extent.toThrift(), null, null, null, 0L, 0., 0., 0));
          result.add(new TabletStatisticsImpl(stats));
        }
      }
      return result;
    }
  }

  @BeforeEach
  public void setUp() {
    last.clear();
    servers.clear();
  }

  @Test
  public void testAssignMigrations() {
    servers.put(new TabletServerIdImpl("127.0.0.1", 1234, "a"), new FakeTServer());
    servers.put(new TabletServerIdImpl("127.0.0.2", 1234, "b"), new FakeTServer());
    servers.put(new TabletServerIdImpl("127.0.0.3", 1234, "c"), new FakeTServer());
    List<TabletId> metadataTable = new ArrayList<>();
    String table = "t1";
    metadataTable.add(makeTablet(table, null, null));
    table = "t2";
    metadataTable.add(makeTablet(table, "a", null));
    metadataTable.add(makeTablet(table, null, "a"));
    table = "t3";
    metadataTable.add(makeTablet(table, "a", null));
    metadataTable.add(makeTablet(table, "b", "a"));
    metadataTable.add(makeTablet(table, "c", "b"));
    metadataTable.add(makeTablet(table, "d", "c"));
    metadataTable.add(makeTablet(table, "e", "d"));
    metadataTable.add(makeTablet(table, null, "e"));
    Collections.sort(metadataTable);

    TestSimpleLoadBalancer balancer = new TestSimpleLoadBalancer();

    SortedMap<TabletServerId,TServerStatus> current = new TreeMap<>();
    for (Entry<TabletServerId,FakeTServer> entry : servers.entrySet()) {
      current.put(entry.getKey(), entry.getValue().getStatus());
    }
    assignTablets(metadataTable, servers, current, balancer);

    // Verify that the counts on the tables are correct
    Map<String,Integer> expectedCounts = new HashMap<>();
    expectedCounts.put("t1", 1);
    expectedCounts.put("t2", 1);
    expectedCounts.put("t3", 2);
    checkBalance(metadataTable, servers, expectedCounts);

    // Rebalance once
    for (Entry<TabletServerId,FakeTServer> entry : servers.entrySet()) {
      current.put(entry.getKey(), entry.getValue().getStatus());
    }

    // Nothing should happen, we are balanced
    ArrayList<TabletMigration> out = new ArrayList<>();
    balancer.getMigrations(current, out);
    assertEquals(out.size(), 0);

    // Take down a tabletServer
    TabletServerId first = current.keySet().iterator().next();
    current.remove(first);
    FakeTServer remove = servers.remove(first);

    // reassign offline extents
    assignTablets(remove.tablets, servers, current, balancer);
    checkBalance(metadataTable, servers, null);
  }

  private void assignTablets(List<TabletId> metadataTable, Map<TabletServerId,FakeTServer> servers,
      SortedMap<TabletServerId,TServerStatus> status, TestSimpleLoadBalancer balancer) {
    // Assign tablets
    for (TabletId tabletId : metadataTable) {
      TabletServerId assignment = balancer.getAssignment(status, last.get(tabletId));
      assertNotNull(assignment);
      assertFalse(servers.get(assignment).tablets.contains(tabletId));
      servers.get(assignment).tablets.add(tabletId);
      last.put(tabletId, assignment);
    }
  }

  SortedMap<TabletServerId,TServerStatus> getAssignments(Map<TabletServerId,FakeTServer> servers) {
    SortedMap<TabletServerId,TServerStatus> result = new TreeMap<>();
    for (Entry<TabletServerId,FakeTServer> entry : servers.entrySet()) {
      result.put(entry.getKey(), entry.getValue().getStatus());
    }
    return result;
  }

  @Test
  public void testUnevenAssignment() {
    for (char c : "abcdefghijklmnopqrstuvwxyz".toCharArray()) {
      String cString = Character.toString(c);
      TabletServerId tsid = new TabletServerIdImpl("127.0.0.1", c, cString);
      FakeTServer fakeTServer = new FakeTServer();
      servers.put(tsid, fakeTServer);
      fakeTServer.tablets.add(makeTablet(cString, null, null));
    }
    // Put more tablets on one server, but not more than the number of servers
    Entry<TabletServerId,FakeTServer> first = servers.entrySet().iterator().next();
    first.getValue().tablets.add(makeTablet("newTable", "a", null));
    first.getValue().tablets.add(makeTablet("newTable", "b", "a"));
    first.getValue().tablets.add(makeTablet("newTable", "c", "b"));
    first.getValue().tablets.add(makeTablet("newTable", "d", "c"));
    first.getValue().tablets.add(makeTablet("newTable", "e", "d"));
    first.getValue().tablets.add(makeTablet("newTable", "f", "e"));
    first.getValue().tablets.add(makeTablet("newTable", "g", "f"));
    first.getValue().tablets.add(makeTablet("newTable", "h", "g"));
    first.getValue().tablets.add(makeTablet("newTable", "i", null));
    TestSimpleLoadBalancer balancer = new TestSimpleLoadBalancer();
    Set<TabletId> migrations = Collections.emptySet();
    int moved = 0;
    // balance until we can't balance no more!
    while (true) {
      List<TabletMigration> migrationsOut = new ArrayList<>();
      balancer.balance(new BalanceParamsImpl(getAssignments(servers), migrations, migrationsOut));
      if (migrationsOut.isEmpty()) {
        break;
      }
      for (TabletMigration migration : migrationsOut) {
        if (servers.get(migration.getOldTabletServer()).tablets.remove(migration.getTablet())) {
          moved++;
        }
        servers.get(migration.getNewTabletServer()).tablets.add(migration.getTablet());
      }
    }
    assertEquals(8, moved);
  }

  @Test
  public void testUnevenAssignment2() {
    // make 26 servers
    for (char c : "abcdefghijklmnopqrstuvwxyz".toCharArray()) {
      TabletServerId tsid = new TabletServerIdImpl("127.0.0.1", c, Character.toString(c));
      FakeTServer fakeTServer = new FakeTServer();
      servers.put(tsid, fakeTServer);
    }
    // put 60 tablets on 25 of them
    List<Entry<TabletServerId,FakeTServer>> shortList = new ArrayList<>(servers.entrySet());
    Entry<TabletServerId,FakeTServer> shortServer = shortList.remove(0);
    int c = 0;
    for (int i = 0; i < 60; i++) {
      for (Entry<TabletServerId,FakeTServer> entry : shortList) {
        entry.getValue().tablets.add(makeTablet("t" + c, null, null));
      }
    }
    // put 10 on the that short server:
    for (int i = 0; i < 10; i++) {
      shortServer.getValue().tablets.add(makeTablet("s" + i, null, null));
    }

    TestSimpleLoadBalancer balancer = new TestSimpleLoadBalancer();
    Set<TabletId> migrations = Collections.emptySet();
    int moved = 0;
    // balance until we can't balance no more!
    while (true) {
      List<TabletMigration> migrationsOut = new ArrayList<>();
      balancer.balance(new BalanceParamsImpl(getAssignments(servers), migrations, migrationsOut));
      if (migrationsOut.isEmpty()) {
        break;
      }
      for (TabletMigration migration : migrationsOut) {
        if (servers.get(migration.getOldTabletServer()).tablets.remove(migration.getTablet())) {
          moved++;
        }
        last.remove(migration.getTablet());
        servers.get(migration.getNewTabletServer()).tablets.add(migration.getTablet());
        last.put(migration.getTablet(), migration.getNewTabletServer());
      }
    }
    // average is 58, with 2 at 59: we need 48 more moved to the short server
    assertEquals(48, moved);
  }

  private void checkBalance(List<TabletId> metadataTable, Map<TabletServerId,FakeTServer> servers,
      Map<String,Integer> expectedCounts) {
    // Verify they are spread evenly over the cluster
    int average = metadataTable.size() / servers.size();
    for (FakeTServer server : servers.values()) {
      int diff = server.tablets.size() - average;
      if (diff < 0) {
        fail("average number of tablets is " + average + " but a server has "
            + server.tablets.size());
      }
      if (diff > 1) {
        fail("average number of tablets is " + average + " but a server has "
            + server.tablets.size());
      }
    }

    if (expectedCounts != null) {
      for (FakeTServer server : servers.values()) {
        Map<String,Integer> counts = new HashMap<>();
        server.tablets.forEach(tabletId -> {
          String t = tabletId.getTable().canonical();
          counts.putIfAbsent(t, 0);
          counts.put(t, counts.get(t) + 1);
        });
        counts.forEach((k, v) -> assertEquals(expectedCounts.get(k), v));
      }
    }
  }

  private static TabletId makeTablet(String table, String end, String prev) {
    return new TabletIdImpl(new KeyExtent(TableId.of(table), toText(end), toText(prev)));
  }

  private static Text toText(String value) {
    if (value != null) {
      return new Text(value);
    }
    return null;
  }

}

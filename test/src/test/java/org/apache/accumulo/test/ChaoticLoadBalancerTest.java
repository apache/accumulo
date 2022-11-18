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
package org.apache.accumulo.test;

import static org.junit.jupiter.api.Assertions.assertEquals;

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
import org.apache.accumulo.core.manager.balancer.AssignmentParamsImpl;
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
import org.junit.jupiter.api.Test;

public class ChaoticLoadBalancerTest {

  static class FakeTServer {
    List<TabletId> tablets = new ArrayList<>();

    TServerStatus getStatus() {
      org.apache.accumulo.core.master.thrift.TabletServerStatus thriftStatus =
          new org.apache.accumulo.core.master.thrift.TabletServerStatus();
      thriftStatus.tableMap = new HashMap<>();
      for (TabletId extent : tablets) {
        TableId table = extent.getTable();
        TableInfo info = thriftStatus.tableMap.get(table.canonical());
        if (info == null) {
          thriftStatus.tableMap.put(table.canonical(), info = new TableInfo());
        }
        info.onlineTablets++;
        info.recs = info.onlineTablets;
        info.ingestRate = 123.;
        info.queryRate = 456.;
      }

      return new TServerStatusImpl(thriftStatus);
    }
  }

  Map<TabletServerId,FakeTServer> servers = new HashMap<>();

  class TestChaoticLoadBalancer extends ChaoticLoadBalancer {

    @Override
    public List<TabletStatistics> getOnlineTabletsForTable(TabletServerId tserver, TableId table) {
      List<TabletStatistics> result = new ArrayList<>();
      for (TabletId tabletId : servers.get(tserver).tablets) {
        if (tabletId.getTable().equals(table)) {
          KeyExtent extent =
              new KeyExtent(tabletId.getTable(), tabletId.getEndRow(), tabletId.getPrevEndRow());
          TabletStats tstats = new TabletStats(extent.toThrift(), null, null, null, 0L, 0., 0., 0);
          result.add(new TabletStatisticsImpl(tstats));
        }
      }
      return result;
    }
  }

  @Test
  public void testAssignMigrations() {
    servers.clear();
    servers.put(new TabletServerIdImpl("127.0.0.1", 1234, "a"), new FakeTServer());
    servers.put(new TabletServerIdImpl("127.0.0.1", 1235, "b"), new FakeTServer());
    servers.put(new TabletServerIdImpl("127.0.0.1", 1236, "c"), new FakeTServer());
    Map<TabletId,TabletServerId> metadataTable = new TreeMap<>();
    String table = "t1";
    metadataTable.put(makeTablet(table, null, null), null);
    table = "t2";
    metadataTable.put(makeTablet(table, "a", null), null);
    metadataTable.put(makeTablet(table, null, "a"), null);
    table = "t3";
    metadataTable.put(makeTablet(table, "a", null), null);
    metadataTable.put(makeTablet(table, "b", "a"), null);
    metadataTable.put(makeTablet(table, "c", "b"), null);
    metadataTable.put(makeTablet(table, "d", "c"), null);
    metadataTable.put(makeTablet(table, "e", "d"), null);
    metadataTable.put(makeTablet(table, null, "e"), null);

    TestChaoticLoadBalancer balancer = new TestChaoticLoadBalancer();

    Map<TabletId,TabletServerId> assignments = new HashMap<>();
    balancer.getAssignments(
        new AssignmentParamsImpl(getAssignments(servers), metadataTable, assignments));

    assertEquals(assignments.size(), metadataTable.size());
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
    servers.clear();
    for (char c : "abcdefghijklmnopqrstuvwxyz".toCharArray()) {
      String cString = Character.toString(c);
      TabletServerId tsi = new TabletServerIdImpl("127.0.0.1", c, cString);
      FakeTServer fakeTServer = new FakeTServer();
      servers.put(tsi, fakeTServer);
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
    TestChaoticLoadBalancer balancer = new TestChaoticLoadBalancer();
    Set<TabletId> migrations = Collections.emptySet();

    // Just want to make sure it gets some migrations, randomness prevents guarantee of a defined
    // amount, or even expected amount
    List<TabletMigration> migrationsOut = new ArrayList<>();
    while (!migrationsOut.isEmpty()) {
      balancer.balance(new BalanceParamsImpl(getAssignments(servers), migrations, migrationsOut));
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

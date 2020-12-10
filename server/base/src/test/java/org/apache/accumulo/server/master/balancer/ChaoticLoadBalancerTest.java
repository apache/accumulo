/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.accumulo.server.master.balancer;

import static org.junit.Assert.assertEquals;

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
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.master.thrift.TableInfo;
import org.apache.accumulo.core.master.thrift.TabletServerStatus;
import org.apache.accumulo.core.metadata.TServerInstance;
import org.apache.accumulo.core.tabletserver.thrift.TabletStats;
import org.apache.accumulo.core.util.HostAndPort;
import org.apache.accumulo.server.master.state.TabletMigration;
import org.apache.hadoop.io.Text;
import org.junit.Test;

public class ChaoticLoadBalancerTest {

  class FakeTServer {
    List<KeyExtent> extents = new ArrayList<>();

    TabletServerStatus getStatus() {
      TabletServerStatus result = new TabletServerStatus();
      result.tableMap = new HashMap<>();
      for (KeyExtent extent : extents) {
        TableId table = extent.tableId();
        TableInfo info = result.tableMap.get(table.canonical());
        if (info == null)
          result.tableMap.put(table.canonical(), info = new TableInfo());
        info.onlineTablets++;
        info.recs = info.onlineTablets;
        info.ingestRate = 123.;
        info.queryRate = 456.;
      }
      return result;
    }
  }

  Map<TServerInstance,FakeTServer> servers = new HashMap<>();

  class TestChaoticLoadBalancer extends ChaoticLoadBalancer {

    @Override
    public List<TabletStats> getOnlineTabletsForTable(TServerInstance tserver, TableId table) {
      List<TabletStats> result = new ArrayList<>();
      for (KeyExtent extent : servers.get(tserver).extents) {
        if (extent.tableId().equals(table)) {
          result.add(new TabletStats(extent.toThrift(), null, null, null, 0L, 0., 0., 0));
        }
      }
      return result;
    }
  }

  @Test
  public void testAssignMigrations() {
    servers.clear();
    servers.put(new TServerInstance(HostAndPort.fromParts("127.0.0.1", 1234), "a"),
        new FakeTServer());
    servers.put(new TServerInstance(HostAndPort.fromParts("127.0.0.1", 1235), "b"),
        new FakeTServer());
    servers.put(new TServerInstance(HostAndPort.fromParts("127.0.0.1", 1236), "c"),
        new FakeTServer());
    Map<KeyExtent,TServerInstance> metadataTable = new TreeMap<>();
    String table = "t1";
    metadataTable.put(makeExtent(table, null, null), null);
    table = "t2";
    metadataTable.put(makeExtent(table, "a", null), null);
    metadataTable.put(makeExtent(table, null, "a"), null);
    table = "t3";
    metadataTable.put(makeExtent(table, "a", null), null);
    metadataTable.put(makeExtent(table, "b", "a"), null);
    metadataTable.put(makeExtent(table, "c", "b"), null);
    metadataTable.put(makeExtent(table, "d", "c"), null);
    metadataTable.put(makeExtent(table, "e", "d"), null);
    metadataTable.put(makeExtent(table, null, "e"), null);

    TestChaoticLoadBalancer balancer = new TestChaoticLoadBalancer();

    SortedMap<TServerInstance,TabletServerStatus> current = new TreeMap<>();
    for (Entry<TServerInstance,FakeTServer> entry : servers.entrySet()) {
      current.put(entry.getKey(), entry.getValue().getStatus());
    }

    Map<KeyExtent,TServerInstance> assignments = new HashMap<>();
    balancer.getAssignments(getAssignments(servers), metadataTable, assignments);

    assertEquals(assignments.size(), metadataTable.size());
  }

  SortedMap<TServerInstance,TabletServerStatus>
      getAssignments(Map<TServerInstance,FakeTServer> servers) {
    SortedMap<TServerInstance,TabletServerStatus> result = new TreeMap<>();
    for (Entry<TServerInstance,FakeTServer> entry : servers.entrySet()) {
      result.put(entry.getKey(), entry.getValue().getStatus());
    }
    return result;
  }

  @Test
  public void testUnevenAssignment() {
    servers.clear();
    for (char c : "abcdefghijklmnopqrstuvwxyz".toCharArray()) {
      String cString = Character.toString(c);
      HostAndPort fakeAddress = HostAndPort.fromParts("127.0.0.1", c);
      TServerInstance tsi = new TServerInstance(fakeAddress, cString);
      FakeTServer fakeTServer = new FakeTServer();
      servers.put(tsi, fakeTServer);
      fakeTServer.extents.add(makeExtent(cString, null, null));
    }
    // Put more tablets on one server, but not more than the number of servers
    Entry<TServerInstance,FakeTServer> first = servers.entrySet().iterator().next();
    first.getValue().extents.add(makeExtent("newTable", "a", null));
    first.getValue().extents.add(makeExtent("newTable", "b", "a"));
    first.getValue().extents.add(makeExtent("newTable", "c", "b"));
    first.getValue().extents.add(makeExtent("newTable", "d", "c"));
    first.getValue().extents.add(makeExtent("newTable", "e", "d"));
    first.getValue().extents.add(makeExtent("newTable", "f", "e"));
    first.getValue().extents.add(makeExtent("newTable", "g", "f"));
    first.getValue().extents.add(makeExtent("newTable", "h", "g"));
    first.getValue().extents.add(makeExtent("newTable", "i", null));
    TestChaoticLoadBalancer balancer = new TestChaoticLoadBalancer();
    Set<KeyExtent> migrations = Collections.emptySet();

    // Just want to make sure it gets some migrations, randomness prevents guarantee of a defined
    // amount, or even expected amount
    List<TabletMigration> migrationsOut = new ArrayList<>();
    while (!migrationsOut.isEmpty()) {
      balancer.balance(getAssignments(servers), migrations, migrationsOut);
    }
  }

  private static KeyExtent makeExtent(String table, String end, String prev) {
    return new KeyExtent(TableId.of(table), toText(end), toText(prev));
  }

  private static Text toText(String value) {
    if (value != null)
      return new Text(value);
    return null;
  }

}

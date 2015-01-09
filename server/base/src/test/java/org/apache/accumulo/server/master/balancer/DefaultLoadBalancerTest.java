/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.accumulo.server.master.balancer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.accumulo.core.client.impl.thrift.ThriftSecurityException;
import org.apache.accumulo.core.data.KeyExtent;
import org.apache.accumulo.core.master.thrift.TableInfo;
import org.apache.accumulo.core.master.thrift.TabletServerStatus;
import org.apache.accumulo.core.tabletserver.thrift.TabletStats;
import org.apache.accumulo.server.master.state.TServerInstance;
import org.apache.accumulo.server.master.state.TabletMigration;
import org.apache.hadoop.io.Text;
import org.apache.thrift.TException;
import org.junit.Before;
import org.junit.Test;

import com.google.common.net.HostAndPort;

public class DefaultLoadBalancerTest {

  class FakeTServer {
    List<KeyExtent> extents = new ArrayList<KeyExtent>();

    TabletServerStatus getStatus(TServerInstance server) {
      TabletServerStatus result = new TabletServerStatus();
      result.tableMap = new HashMap<String,TableInfo>();
      for (KeyExtent extent : extents) {
        String table = extent.getTableId().toString();
        TableInfo info = result.tableMap.get(table);
        if (info == null)
          result.tableMap.put(table, info = new TableInfo());
        info.onlineTablets++;
        info.recs = info.onlineTablets;
        info.ingestRate = 123.;
        info.queryRate = 456.;
      }
      return result;
    }
  }

  Map<TServerInstance,FakeTServer> servers = new HashMap<TServerInstance,FakeTServer>();
  Map<KeyExtent,TServerInstance> last = new HashMap<KeyExtent,TServerInstance>();

  class TestDefaultLoadBalancer extends DefaultLoadBalancer {

    @Override
    public List<TabletStats> getOnlineTabletsForTable(TServerInstance tserver, String table) throws ThriftSecurityException, TException {
      List<TabletStats> result = new ArrayList<TabletStats>();
      for (KeyExtent extent : servers.get(tserver).extents) {
        if (extent.getTableId().toString().equals(table)) {
          result.add(new TabletStats(extent.toThrift(), null, null, null, 0l, 0., 0., 0));
        }
      }
      return result;
    }
  }

  @Before
  public void setUp() {
    last.clear();
    servers.clear();
  }

  @Test
  public void testAssignMigrations() {
    servers.put(new TServerInstance(HostAndPort.fromParts("127.0.0.1", 1234), "a"), new FakeTServer());
    servers.put(new TServerInstance(HostAndPort.fromParts("127.0.0.2", 1234), "b"), new FakeTServer());
    servers.put(new TServerInstance(HostAndPort.fromParts("127.0.0.3", 1234), "c"), new FakeTServer());
    List<KeyExtent> metadataTable = new ArrayList<KeyExtent>();
    String table = "t1";
    metadataTable.add(makeExtent(table, null, null));
    table = "t2";
    metadataTable.add(makeExtent(table, "a", null));
    metadataTable.add(makeExtent(table, null, "a"));
    table = "t3";
    metadataTable.add(makeExtent(table, "a", null));
    metadataTable.add(makeExtent(table, "b", "a"));
    metadataTable.add(makeExtent(table, "c", "b"));
    metadataTable.add(makeExtent(table, "d", "c"));
    metadataTable.add(makeExtent(table, "e", "d"));
    metadataTable.add(makeExtent(table, null, "e"));
    Collections.sort(metadataTable);

    TestDefaultLoadBalancer balancer = new TestDefaultLoadBalancer();

    SortedMap<TServerInstance,TabletServerStatus> current = new TreeMap<TServerInstance,TabletServerStatus>();
    for (Entry<TServerInstance,FakeTServer> entry : servers.entrySet()) {
      current.put(entry.getKey(), entry.getValue().getStatus(entry.getKey()));
    }
    assignTablets(metadataTable, servers, current, balancer);

    // Verify that the counts on the tables are correct
    Map<String,Integer> expectedCounts = new HashMap<String,Integer>();
    expectedCounts.put("t1", 1);
    expectedCounts.put("t2", 1);
    expectedCounts.put("t3", 2);
    checkBalance(metadataTable, servers, expectedCounts);

    // Rebalance once
    for (Entry<TServerInstance,FakeTServer> entry : servers.entrySet()) {
      current.put(entry.getKey(), entry.getValue().getStatus(entry.getKey()));
    }

    // Nothing should happen, we are balanced
    ArrayList<TabletMigration> out = new ArrayList<TabletMigration>();
    balancer.getMigrations(current, out);
    assertEquals(out.size(), 0);

    // Take down a tabletServer
    TServerInstance first = current.keySet().iterator().next();
    current.remove(first);
    FakeTServer remove = servers.remove(first);

    // reassign offline extents
    assignTablets(remove.extents, servers, current, balancer);
    checkBalance(metadataTable, servers, null);
  }

  private void assignTablets(List<KeyExtent> metadataTable, Map<TServerInstance,FakeTServer> servers, SortedMap<TServerInstance,TabletServerStatus> status,
      TestDefaultLoadBalancer balancer) {
    // Assign tablets
    for (KeyExtent extent : metadataTable) {
      TServerInstance assignment = balancer.getAssignment(status, extent, last.get(extent));
      assertNotNull(assignment);
      assertFalse(servers.get(assignment).extents.contains(extent));
      servers.get(assignment).extents.add(extent);
      last.put(extent, assignment);
    }
  }

  SortedMap<TServerInstance,TabletServerStatus> getAssignments(Map<TServerInstance,FakeTServer> servers) {
    SortedMap<TServerInstance,TabletServerStatus> result = new TreeMap<TServerInstance,TabletServerStatus>();
    for (Entry<TServerInstance,FakeTServer> entry : servers.entrySet()) {
      result.put(entry.getKey(), entry.getValue().getStatus(entry.getKey()));
    }
    return result;
  }

  @Test
  public void testUnevenAssignment() {
    for (char c : "abcdefghijklmnopqrstuvwxyz".toCharArray()) {
      String cString = Character.toString(c);
      HostAndPort fakeAddress = HostAndPort.fromParts("127.0.0.1", (int) c);
      String fakeInstance = cString;
      TServerInstance tsi = new TServerInstance(fakeAddress, fakeInstance);
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
    TestDefaultLoadBalancer balancer = new TestDefaultLoadBalancer();
    Set<KeyExtent> migrations = Collections.emptySet();
    int moved = 0;
    // balance until we can't balance no more!
    while (true) {
      List<TabletMigration> migrationsOut = new ArrayList<TabletMigration>();
      balancer.balance(getAssignments(servers), migrations, migrationsOut);
      if (migrationsOut.size() == 0)
        break;
      for (TabletMigration migration : migrationsOut) {
        if (servers.get(migration.oldServer).extents.remove(migration.tablet))
          moved++;
        servers.get(migration.newServer).extents.add(migration.tablet);
      }
    }
    assertEquals(8, moved);
  }

  @Test
  public void testUnevenAssignment2() {
    // make 26 servers
    for (char c : "abcdefghijklmnopqrstuvwxyz".toCharArray()) {
      String cString = Character.toString(c);
      HostAndPort fakeAddress = HostAndPort.fromParts("127.0.0.1", (int) c);
      String fakeInstance = cString;
      TServerInstance tsi = new TServerInstance(fakeAddress, fakeInstance);
      FakeTServer fakeTServer = new FakeTServer();
      servers.put(tsi, fakeTServer);
    }
    // put 60 tablets on 25 of them
    List<Entry<TServerInstance,FakeTServer>> shortList = new ArrayList<Entry<TServerInstance,FakeTServer>>(servers.entrySet());
    Entry<TServerInstance,FakeTServer> shortServer = shortList.remove(0);
    int c = 0;
    for (int i = 0; i < 60; i++) {
      for (Entry<TServerInstance,FakeTServer> entry : shortList) {
        entry.getValue().extents.add(makeExtent("t" + c, null, null));
      }
    }
    // put 10 on the that short server:
    for (int i = 0; i < 10; i++) {
      shortServer.getValue().extents.add(makeExtent("s" + i, null, null));
    }

    TestDefaultLoadBalancer balancer = new TestDefaultLoadBalancer();
    Set<KeyExtent> migrations = Collections.emptySet();
    int moved = 0;
    // balance until we can't balance no more!
    while (true) {
      List<TabletMigration> migrationsOut = new ArrayList<TabletMigration>();
      balancer.balance(getAssignments(servers), migrations, migrationsOut);
      if (migrationsOut.size() == 0)
        break;
      for (TabletMigration migration : migrationsOut) {
        if (servers.get(migration.oldServer).extents.remove(migration.tablet))
          moved++;
        last.remove(migration.tablet);
        servers.get(migration.newServer).extents.add(migration.tablet);
        last.put(migration.tablet, migration.newServer);
      }
    }
    // average is 58, with 2 at 59: we need 48 more moved to the short server
    assertEquals(48, moved);
  }

  private void checkBalance(List<KeyExtent> metadataTable, Map<TServerInstance,FakeTServer> servers, Map<String,Integer> expectedCounts) {
    // Verify they are spread evenly over the cluster
    int average = metadataTable.size() / servers.size();
    for (FakeTServer server : servers.values()) {
      int diff = server.extents.size() - average;
      if (diff < 0)
        fail("average number of tablets is " + average + " but a server has " + server.extents.size());
      if (diff > 1)
        fail("average number of tablets is " + average + " but a server has " + server.extents.size());
    }

    if (expectedCounts != null) {
      for (FakeTServer server : servers.values()) {
        Map<String,Integer> counts = new HashMap<String,Integer>();
        for (KeyExtent extent : server.extents) {
          String t = extent.getTableId().toString();
          if (counts.get(t) == null)
            counts.put(t, 0);
          counts.put(t, counts.get(t) + 1);
        }
        for (Entry<String,Integer> entry : counts.entrySet()) {
          assertEquals(expectedCounts.get(entry.getKey()), counts.get(entry.getKey()));
        }
      }
    }
  }

  private static KeyExtent makeExtent(String table, String end, String prev) {
    return new KeyExtent(new Text(table), toText(end), toText(prev));
  }

  private static Text toText(String value) {
    if (value != null)
      return new Text(value);
    return null;
  }

}

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
package org.apache.accumulo.server.master.balancer;

import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.UUID;

import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.InstanceId;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.master.thrift.TableInfo;
import org.apache.accumulo.core.master.thrift.TabletServerStatus;
import org.apache.accumulo.core.metadata.TServerInstance;
import org.apache.accumulo.core.tabletserver.thrift.TabletStats;
import org.apache.accumulo.core.util.HostAndPort;
import org.apache.accumulo.server.MockServerContext;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.conf.TableConfiguration;
import org.apache.accumulo.server.master.state.TabletMigration;
import org.apache.hadoop.io.Text;
import org.easymock.EasyMock;
import org.junit.jupiter.api.Test;

@Deprecated(since = "2.1.0")
public class TableLoadBalancerTest {

  private static Map<String,String> TABLE_ID_MAP = Map.of("t1", "a1", "t2", "b12", "t3", "c4");

  private static TServerInstance mkts(String address, String session) {
    return new TServerInstance(HostAndPort.fromParts(address, 1234), session);
  }

  private static TabletServerStatus status(Object... config) {
    TabletServerStatus result = new TabletServerStatus();
    result.tableMap = new HashMap<>();
    String tablename = null;
    for (Object c : config) {
      if (c instanceof String) {
        tablename = (String) c;
      } else {
        TableInfo info = new TableInfo();
        int count = (Integer) c;
        info.onlineTablets = count;
        info.tablets = count;
        result.tableMap.put(tablename, info);
      }
    }
    return result;
  }

  static SortedMap<TServerInstance,TabletServerStatus> state;

  static List<TabletStats> generateFakeTablets(TServerInstance tserver, TableId tableId) {
    List<TabletStats> result = new ArrayList<>();
    TabletServerStatus tableInfo = state.get(tserver);
    // generate some fake tablets
    for (int i = 0; i < tableInfo.tableMap.get(tableId.canonical()).onlineTablets; i++) {
      TabletStats stats = new TabletStats();
      stats.extent =
          new KeyExtent(tableId, new Text(tserver.getHost() + String.format("%03d", i + 1)),
              new Text(tserver.getHost() + String.format("%03d", i))).toThrift();
      result.add(stats);
    }
    return result;
  }

  static class DefaultLoadBalancer
      extends org.apache.accumulo.server.master.balancer.DefaultLoadBalancer {

    public DefaultLoadBalancer(TableId table) {
      super(table);
    }

    @Override
    public void init(ServerContext context) {}

    @Override
    public List<TabletStats> getOnlineTabletsForTable(TServerInstance tserver, TableId tableId) {
      return generateFakeTablets(tserver, tableId);
    }
  }

  // ugh... so wish I had provided mock objects to the LoadBalancer in the manager
  class TableLoadBalancer extends org.apache.accumulo.server.master.balancer.TableLoadBalancer {

    // use our new classname to test class loading
    @Override
    protected String getLoadBalancerClassNameForTable(TableId table) {
      return DefaultLoadBalancer.class.getName();
    }

    // we don't have real tablet servers to ask: invent some online tablets
    @Override
    public List<TabletStats> getOnlineTabletsForTable(TServerInstance tserver, TableId tableId) {
      return generateFakeTablets(tserver, tableId);
    }

    @Override
    protected TableOperations getTableOperations() {
      TableOperations tops = createMock(TableOperations.class);
      expect(tops.tableIdMap()).andReturn(TABLE_ID_MAP).anyTimes();
      replay(tops);
      return tops;
    }
  }

  private ServerContext createMockContext() {
    final InstanceId instanceId =
        InstanceId.of(UUID.nameUUIDFromBytes(new byte[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 0}));
    return MockServerContext.getWithZK(instanceId, "10.0.0.1:1234", 30_000);
  }

  @Test
  public void test() {
    final ServerContext context = createMockContext();
    TableConfiguration conf = createMock(TableConfiguration.class);
    // Eclipse might show @SuppressWarnings("removal") as unnecessary.
    // Eclipse is wrong. See https://bugs.eclipse.org/bugs/show_bug.cgi?id=565271
    @SuppressWarnings("removal")
    Property TABLE_CLASSPATH = Property.TABLE_CLASSPATH;
    expect(conf.resolve(Property.TABLE_CLASSLOADER_CONTEXT, TABLE_CLASSPATH))
        .andReturn(Property.TABLE_CLASSLOADER_CONTEXT).anyTimes();
    expect(conf.get(Property.TABLE_CLASSLOADER_CONTEXT)).andReturn("").anyTimes();
    expect(context.getTableConfiguration(EasyMock.anyObject())).andReturn(conf).anyTimes();
    replay(context, conf);

    String t1Id = TABLE_ID_MAP.get("t1"), t2Id = TABLE_ID_MAP.get("t2"),
        t3Id = TABLE_ID_MAP.get("t3");
    state = new TreeMap<>();
    TServerInstance svr = mkts("10.0.0.1", "0x01020304");
    state.put(svr, status(t1Id, 10, t2Id, 10, t3Id, 10));

    Set<KeyExtent> migrations = Collections.emptySet();
    List<TabletMigration> migrationsOut = new ArrayList<>();
    TableLoadBalancer tls = new TableLoadBalancer();
    tls.init(context);
    tls.balance(state, migrations, migrationsOut);
    assertEquals(0, migrationsOut.size());

    state.put(mkts("10.0.0.2", "0x02030405"), status());
    tls = new TableLoadBalancer();
    tls.init(context);
    tls.balance(state, migrations, migrationsOut);
    int count = 0;
    Map<TableId,Integer> movedByTable = new HashMap<>();
    movedByTable.put(TableId.of(t1Id), 0);
    movedByTable.put(TableId.of(t2Id), 0);
    movedByTable.put(TableId.of(t3Id), 0);
    for (TabletMigration migration : migrationsOut) {
      if (migration.oldServer.equals(svr)) {
        count++;
      }
      TableId key = migration.tablet.tableId();
      movedByTable.put(key, movedByTable.get(key) + 1);
    }
    assertEquals(15, count);
    for (Integer moved : movedByTable.values()) {
      assertEquals(5, moved.intValue());
    }
  }

}

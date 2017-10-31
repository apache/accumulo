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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.UUID;

import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.client.impl.Table;
import org.apache.accumulo.core.client.impl.thrift.ThriftSecurityException;
import org.apache.accumulo.core.conf.DefaultConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.impl.KeyExtent;
import org.apache.accumulo.core.master.thrift.TableInfo;
import org.apache.accumulo.core.master.thrift.TabletServerStatus;
import org.apache.accumulo.core.tabletserver.thrift.TabletStats;
import org.apache.accumulo.core.util.HostAndPort;
import org.apache.accumulo.server.AccumuloServerContext;
import org.apache.accumulo.server.conf.NamespaceConfiguration;
import org.apache.accumulo.server.conf.ServerConfigurationFactory;
import org.apache.accumulo.server.conf.TableConfiguration;
import org.apache.accumulo.server.master.state.TServerInstance;
import org.apache.accumulo.server.master.state.TabletMigration;
import org.apache.hadoop.io.Text;
import org.apache.thrift.TException;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;

public class TableLoadBalancerTest {

  private static Map<String,String> TABLE_ID_MAP = ImmutableMap.of("t1", "a1", "t2", "b12", "t3", "c4");

  static private TServerInstance mkts(String address, String session) throws Exception {
    return new TServerInstance(HostAndPort.fromParts(address, 1234), session);
  }

  static private TabletServerStatus status(Object... config) {
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

  static List<TabletStats> generateFakeTablets(TServerInstance tserver, Table.ID tableId) {
    List<TabletStats> result = new ArrayList<>();
    TabletServerStatus tableInfo = state.get(tserver);
    // generate some fake tablets
    for (int i = 0; i < tableInfo.tableMap.get(tableId.canonicalID()).onlineTablets; i++) {
      TabletStats stats = new TabletStats();
      stats.extent = new KeyExtent(tableId, new Text(tserver.host() + String.format("%03d", i + 1)), new Text(tserver.host() + String.format("%03d", i)))
          .toThrift();
      result.add(stats);
    }
    return result;
  }

  static class DefaultLoadBalancer extends org.apache.accumulo.server.master.balancer.DefaultLoadBalancer {

    public DefaultLoadBalancer(Table.ID table) {
      super(table);
    }

    @Override
    public void init(AccumuloServerContext context) {}

    @Override
    public List<TabletStats> getOnlineTabletsForTable(TServerInstance tserver, Table.ID tableId) throws ThriftSecurityException, TException {
      return generateFakeTablets(tserver, tableId);
    }
  }

  // ugh... so wish I had provided mock objects to the LoadBalancer in the master
  class TableLoadBalancer extends org.apache.accumulo.server.master.balancer.TableLoadBalancer {

    TableLoadBalancer() {
      super();
    }

    // use our new classname to test class loading
    @Override
    protected String getLoadBalancerClassNameForTable(Table.ID table) {
      return DefaultLoadBalancer.class.getName();
    }

    // we don't have real tablet servers to ask: invent some online tablets
    @Override
    public List<TabletStats> getOnlineTabletsForTable(TServerInstance tserver, Table.ID tableId) throws ThriftSecurityException, TException {
      return generateFakeTablets(tserver, tableId);
    }

    @Override
    protected TableOperations getTableOperations() {
      TableOperations tops = EasyMock.createMock(TableOperations.class);
      EasyMock.expect(tops.tableIdMap()).andReturn(TABLE_ID_MAP).anyTimes();
      EasyMock.replay(tops);
      return tops;
    }
  }

  @Test
  public void test() throws Exception {
    final Instance inst = EasyMock.createMock(Instance.class);
    EasyMock.expect(inst.getInstanceID()).andReturn(UUID.nameUUIDFromBytes(new byte[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 0}).toString()).anyTimes();
    EasyMock.expect(inst.getZooKeepers()).andReturn("10.0.0.1:1234").anyTimes();
    EasyMock.expect(inst.getZooKeepersSessionTimeOut()).andReturn(30_000).anyTimes();
    EasyMock.replay(inst);

    ServerConfigurationFactory confFactory = new ServerConfigurationFactory(inst) {
      @Override
      public TableConfiguration getTableConfiguration(Table.ID tableId) {
        // create a dummy namespaceConfiguration to satisfy requireNonNull in TableConfiguration constructor
        NamespaceConfiguration dummyConf = new NamespaceConfiguration(null, null, null);
        return new TableConfiguration(inst, tableId, dummyConf) {
          @Override
          public String get(Property property) {
            // fake the get table configuration so the test doesn't try to look in zookeeper for per-table classpath stuff
            return DefaultConfiguration.getInstance().get(property);
          }
        };
      }
    };

    String t1Id = TABLE_ID_MAP.get("t1"), t2Id = TABLE_ID_MAP.get("t2"), t3Id = TABLE_ID_MAP.get("t3");
    state = new TreeMap<>();
    TServerInstance svr = mkts("10.0.0.1", "0x01020304");
    state.put(svr, status(t1Id, 10, t2Id, 10, t3Id, 10));

    Set<KeyExtent> migrations = Collections.emptySet();
    List<TabletMigration> migrationsOut = new ArrayList<>();
    TableLoadBalancer tls = new TableLoadBalancer();
    tls.init(new AccumuloServerContext(inst, confFactory));
    tls.balance(state, migrations, migrationsOut);
    Assert.assertEquals(0, migrationsOut.size());

    state.put(mkts("10.0.0.2", "0x02030405"), status());
    tls = new TableLoadBalancer();
    tls.init(new AccumuloServerContext(inst, confFactory));
    tls.balance(state, migrations, migrationsOut);
    int count = 0;
    Map<Table.ID,Integer> movedByTable = new HashMap<>();
    movedByTable.put(Table.ID.of(t1Id), Integer.valueOf(0));
    movedByTable.put(Table.ID.of(t2Id), Integer.valueOf(0));
    movedByTable.put(Table.ID.of(t3Id), Integer.valueOf(0));
    for (TabletMigration migration : migrationsOut) {
      if (migration.oldServer.equals(svr))
        count++;
      Table.ID key = migration.tablet.getTableId();
      movedByTable.put(key, movedByTable.get(key) + 1);
    }
    Assert.assertEquals(15, count);
    for (Integer moved : movedByTable.values()) {
      Assert.assertEquals(5, moved.intValue());
    }
  }

}

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

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.client.impl.thrift.ThriftSecurityException;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.KeyExtent;
import org.apache.accumulo.core.master.thrift.TableInfo;
import org.apache.accumulo.core.master.thrift.TabletServerStatus;
import org.apache.accumulo.core.tabletserver.thrift.TabletStats;
import org.apache.accumulo.server.master.state.TServerInstance;
import org.apache.accumulo.server.master.state.TabletMigration;
import org.apache.hadoop.io.Text;
import org.apache.thrift.TException;
import org.junit.Assert;
import org.junit.Test;

import com.google.common.net.HostAndPort;

public class TableLoadBalancerTest {

  static private TServerInstance mkts(String address, String session) throws Exception {
    return new TServerInstance(HostAndPort.fromParts(address, 1234), session);
  }

  static private TabletServerStatus status(Object... config) {
    TabletServerStatus result = new TabletServerStatus();
    result.tableMap = new HashMap<String,TableInfo>();
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

  static MockInstance instance = new MockInstance("mockamatic");

  static SortedMap<TServerInstance,TabletServerStatus> state;

  static List<TabletStats> generateFakeTablets(TServerInstance tserver, String tableId) {
    List<TabletStats> result = new ArrayList<TabletStats>();
    TabletServerStatus tableInfo = state.get(tserver);
    // generate some fake tablets
    for (int i = 0; i < tableInfo.tableMap.get(tableId).onlineTablets; i++) {
      TabletStats stats = new TabletStats();
      stats.extent = new KeyExtent(new Text(tableId), new Text(tserver.host() + String.format("%03d", i + 1)), new Text(tserver.host()
          + String.format("%03d", i))).toThrift();
      result.add(stats);
    }
    return result;
  }

  static class DefaultLoadBalancer extends org.apache.accumulo.server.master.balancer.DefaultLoadBalancer {

    public DefaultLoadBalancer(String table) {
      super(table);
    }

    @Override
    public List<TabletStats> getOnlineTabletsForTable(TServerInstance tserver, String tableId) throws ThriftSecurityException, TException {
      return generateFakeTablets(tserver, tableId);
    }
  }

  // ugh... so wish I had provided mock objects to the LoadBalancer in the master
  static class TableLoadBalancer extends org.apache.accumulo.server.master.balancer.TableLoadBalancer {

    TableLoadBalancer() {
      super();
    }

    // need to use our mock instance
    @Override
    protected TableOperations getTableOperations() {
      try {
        return instance.getConnector("user", new PasswordToken("pass")).tableOperations();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }

    // use our new classname to test class loading
    @Override
    protected String getLoadBalancerClassNameForTable(String table) {
      return DefaultLoadBalancer.class.getName();
    }

    // we don't have real tablet servers to ask: invent some online tablets
    @Override
    public List<TabletStats> getOnlineTabletsForTable(TServerInstance tserver, String tableId) throws ThriftSecurityException, TException {
      return generateFakeTablets(tserver, tableId);
    }
  }

  @Test
  public void test() throws Exception {
    Connector c = instance.getConnector("user", new PasswordToken("pass"));
    TableOperations tops = c.tableOperations();
    tops.create("t1");
    tops.create("t2");
    tops.create("t3");
    String t1Id = tops.tableIdMap().get("t1"), t2Id = tops.tableIdMap().get("t2"), t3Id = tops.tableIdMap().get("t3");
    state = new TreeMap<TServerInstance,TabletServerStatus>();
    TServerInstance svr = mkts("10.0.0.1", "0x01020304");
    state.put(svr, status(t1Id, 10, t2Id, 10, t3Id, 10));

    Set<KeyExtent> migrations = Collections.emptySet();
    List<TabletMigration> migrationsOut = new ArrayList<TabletMigration>();
    TableLoadBalancer tls = new TableLoadBalancer();
    tls.balance(state, migrations, migrationsOut);
    Assert.assertEquals(0, migrationsOut.size());

    state.put(mkts("10.0.0.2", "0x02030405"), status());
    tls = new TableLoadBalancer();
    tls.balance(state, migrations, migrationsOut);
    int count = 0;
    Map<String,Integer> movedByTable = new HashMap<String,Integer>();
    movedByTable.put(t1Id, new Integer(0));
    movedByTable.put(t2Id, new Integer(0));
    movedByTable.put(t3Id, new Integer(0));
    for (TabletMigration migration : migrationsOut) {
      if (migration.oldServer.equals(svr))
        count++;
      String key = migration.tablet.getTableId().toString();
      movedByTable.put(key, movedByTable.get(key) + 1);
    }
    Assert.assertEquals(15, count);
    for (Integer moved : movedByTable.values()) {
      Assert.assertEquals(5, moved.intValue());
    }
  }

}

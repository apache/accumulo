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

import static org.easymock.EasyMock.anyObject;
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
import java.util.stream.Collectors;

import org.apache.accumulo.core.conf.ConfigurationCopy;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.data.TabletId;
import org.apache.accumulo.core.dataImpl.KeyExtent;
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
import org.apache.accumulo.core.util.ConfigurationImpl;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.Test;

public class TableLoadBalancerTest {

  private static final Map<String,String> TABLE_ID_MAP =
      Map.of("t1", "a1", "t2", "b12", "t3", "c4");

  private static TabletServerId mkts(String host, int port, String session) {
    return new TabletServerIdImpl(host, port, session);
  }

  private static TServerStatus status(Object... config) {
    org.apache.accumulo.core.master.thrift.TabletServerStatus thriftStatus =
        new org.apache.accumulo.core.master.thrift.TabletServerStatus();
    thriftStatus.tableMap = new HashMap<>();
    String tablename = null;
    for (Object c : config) {
      if (c instanceof String) {
        tablename = (String) c;
      } else {
        TableInfo info = new TableInfo();
        int count = (Integer) c;
        info.onlineTablets = count;
        info.tablets = count;
        thriftStatus.tableMap.put(tablename, info);
      }
    }
    return new TServerStatusImpl(thriftStatus);
  }

  private static final SortedMap<TabletServerId,TServerStatus> state = new TreeMap<>();

  static List<TabletStatistics> generateFakeTablets(TabletServerId tserver, TableId tableId) {
    List<TabletStatistics> result = new ArrayList<>();
    TServerStatus tableInfo = state.get(tserver);
    // generate some fake tablets
    for (int i = 0; i < tableInfo.getTableMap().get(tableId.canonical()).getOnlineTabletCount();
        i++) {
      TabletStats stats = new TabletStats();
      stats.extent =
          new KeyExtent(tableId, new Text(tserver.getHost() + String.format("%03d", i + 1)),
              new Text(tserver.getHost() + String.format("%03d", i))).toThrift();
      result.add(new TabletStatisticsImpl(stats));
    }
    return result;
  }

  public static class TestSimpleLoadBalancer extends SimpleLoadBalancer {

    public TestSimpleLoadBalancer(TableId table) {
      super(table);
    }

    @Override
    public void init(BalancerEnvironment balancerEnvironment) {}

    @Override
    public List<TabletStatistics> getOnlineTabletsForTable(TabletServerId tserver,
        TableId tableId) {
      return generateFakeTablets(tserver, tableId);
    }
  }

  @Test
  public void test() {
    BalancerEnvironment environment = createMock(BalancerEnvironment.class);
    ConfigurationCopy cc = new ConfigurationCopy(
        Map.of(Property.TABLE_LOAD_BALANCER.getKey(), TestSimpleLoadBalancer.class.getName()));
    ConfigurationImpl tableConfig = new ConfigurationImpl(cc);

    Map<String,TableId> tableIdMap = TABLE_ID_MAP.entrySet().stream()
        .collect(Collectors.toMap(Map.Entry::getKey, e -> TableId.of(e.getValue())));
    expect(environment.getTableIdMap()).andReturn(tableIdMap).anyTimes();
    expect(environment.isTableOnline(anyObject(TableId.class))).andReturn(true).anyTimes();
    expect(environment.getConfiguration(anyObject(TableId.class))).andReturn(tableConfig)
        .anyTimes();
    expect(environment.tableContext(anyObject(TableId.class))).andReturn(null).anyTimes();

    replay(environment);

    String t1Id = TABLE_ID_MAP.get("t1"), t2Id = TABLE_ID_MAP.get("t2"),
        t3Id = TABLE_ID_MAP.get("t3");
    state.clear();
    TabletServerId svr = mkts("10.0.0.1", 1234, "0x01020304");
    state.put(svr, status(t1Id, 10, t2Id, 10, t3Id, 10));

    Set<TabletId> migrations = Collections.emptySet();
    List<TabletMigration> migrationsOut = new ArrayList<>();
    TableLoadBalancer tls = new TableLoadBalancer();
    tls.init(environment);
    tls.balance(new BalanceParamsImpl(state, migrations, migrationsOut));
    assertEquals(0, migrationsOut.size());

    state.put(mkts("10.0.0.2", 2345, "0x02030405"), status());
    tls = new TableLoadBalancer();
    tls.init(environment);
    tls.balance(new BalanceParamsImpl(state, migrations, migrationsOut));
    int count = 0;
    Map<TableId,Integer> movedByTable = new HashMap<>();
    movedByTable.put(TableId.of(t1Id), 0);
    movedByTable.put(TableId.of(t2Id), 0);
    movedByTable.put(TableId.of(t3Id), 0);
    for (TabletMigration migration : migrationsOut) {
      if (migration.getOldTabletServer().equals(svr)) {
        count++;
      }
      TableId key = migration.getTablet().getTable();
      movedByTable.put(key, movedByTable.get(key) + 1);
    }
    assertEquals(15, count);
    for (Integer moved : movedByTable.values()) {
      assertEquals(5, moved.intValue());
    }
  }

}

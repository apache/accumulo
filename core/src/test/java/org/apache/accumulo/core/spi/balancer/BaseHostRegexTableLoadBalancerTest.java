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

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.data.TabletId;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.dataImpl.TabletIdImpl;
import org.apache.accumulo.core.manager.balancer.TServerStatusImpl;
import org.apache.accumulo.core.manager.balancer.TableStatisticsImpl;
import org.apache.accumulo.core.manager.balancer.TabletServerIdImpl;
import org.apache.accumulo.core.manager.balancer.TabletStatisticsImpl;
import org.apache.accumulo.core.master.thrift.TableInfo;
import org.apache.accumulo.core.spi.balancer.data.TServerStatus;
import org.apache.accumulo.core.spi.balancer.data.TableStatistics;
import org.apache.accumulo.core.spi.balancer.data.TabletServerId;
import org.apache.accumulo.core.spi.balancer.data.TabletStatistics;
import org.apache.accumulo.core.tabletserver.thrift.TabletStats;
import org.apache.hadoop.io.Text;

public abstract class BaseHostRegexTableLoadBalancerTest extends HostRegexTableLoadBalancer {

  protected static class TestTable {
    private final String tableName;
    private final TableId id;

    TestTable(String tableName, TableId id) {
      this.tableName = tableName;
      this.id = id;
    }

    public String getTableName() {
      return tableName;
    }

    public TableId getId() {
      return id;
    }
  }

  protected static final HashMap<String,String> DEFAULT_TABLE_PROPERTIES = new HashMap<>();
  {
    DEFAULT_TABLE_PROPERTIES.put(HostRegexTableLoadBalancer.HOST_BALANCER_OOB_CHECK_KEY, "7s");
    DEFAULT_TABLE_PROPERTIES.put(HostRegexTableLoadBalancer.HOST_BALANCER_REGEX_MAX_MIGRATIONS_KEY,
        "4");
    DEFAULT_TABLE_PROPERTIES
        .put(HostRegexTableLoadBalancer.HOST_BALANCER_OUTSTANDING_MIGRATIONS_KEY, "10");
    DEFAULT_TABLE_PROPERTIES
        .put(HostRegexTableLoadBalancer.HOST_BALANCER_PREFIX + FOO.getTableName(), "r01.*");
    DEFAULT_TABLE_PROPERTIES
        .put(HostRegexTableLoadBalancer.HOST_BALANCER_PREFIX + BAR.getTableName(), "r02.*");
    DEFAULT_TABLE_PROPERTIES.put(Property.TABLE_LOAD_BALANCER.getKey(),
        TestSimpleBalancer.class.getName());
  }

  protected static final TestTable FOO = new TestTable("foo", TableId.of("1"));
  protected static final TestTable BAR = new TestTable("bar", TableId.of("2"));
  protected static final TestTable BAZ = new TestTable("baz", TableId.of("3"));

  protected class TestSimpleBalancer extends SimpleLoadBalancer {
    @Override
    public List<TabletStatistics> getOnlineTabletsForTable(TabletServerId tserver,
        TableId tableId) {
      String tableName = idToTableName(tableId);
      TabletServerId initialLocation = initialTableLocation.get(tableName);
      if (tserver.equals(initialLocation)) {
        List<TabletStatistics> list = new ArrayList<>(5);
        for (TabletId tabletId : tableTablets.get(tableName)) {
          TabletStats thriftStats = new TabletStats();
          thriftStats.setExtent(
              new KeyExtent(tabletId.getTable(), tabletId.getEndRow(), tabletId.getPrevEndRow())
                  .toThrift());
          TabletStatistics stats = new TabletStatisticsImpl(thriftStats);
          list.add(stats);
        }
        return list;
      }
      return null;
    }
  }

  protected final Map<String,String> servers = new HashMap<>(15);
  protected final SortedMap<TabletServerId,TServerStatus> allTabletServers = new TreeMap<>();
  protected final Map<String,List<TabletId>> tableTablets = new HashMap<>(3);
  protected final Map<String,TabletServerId> initialTableLocation = new HashMap<>(3);

  {
    servers.put("192.168.0.1", "r01s01");
    servers.put("192.168.0.2", "r01s02");
    servers.put("192.168.0.3", "r01s03");
    servers.put("192.168.0.4", "r01s04");
    servers.put("192.168.0.5", "r01s05");
    servers.put("192.168.0.6", "r02s01");
    servers.put("192.168.0.7", "r02s02");
    servers.put("192.168.0.8", "r02s03");
    servers.put("192.168.0.9", "r02s04");
    servers.put("192.168.0.10", "r02s05");
    servers.put("192.168.0.11", "r03s01");
    servers.put("192.168.0.12", "r03s02");
    servers.put("192.168.0.13", "r03s03");
    servers.put("192.168.0.14", "r03s04");
    servers.put("192.168.0.15", "r03s05");

    allTabletServers.put(new TabletServerIdImpl("192.168.0.1", 9997, Integer.toHexString(1)),
        new TServerStatusImpl(new org.apache.accumulo.core.master.thrift.TabletServerStatus()));
    allTabletServers.put(new TabletServerIdImpl("192.168.0.2", 9997, Integer.toHexString(1)),
        new TServerStatusImpl(new org.apache.accumulo.core.master.thrift.TabletServerStatus()));
    allTabletServers.put(new TabletServerIdImpl("192.168.0.3", 9997, Integer.toHexString(1)),
        new TServerStatusImpl(new org.apache.accumulo.core.master.thrift.TabletServerStatus()));
    allTabletServers.put(new TabletServerIdImpl("192.168.0.4", 9997, Integer.toHexString(1)),
        new TServerStatusImpl(new org.apache.accumulo.core.master.thrift.TabletServerStatus()));
    allTabletServers.put(new TabletServerIdImpl("192.168.0.5", 9997, Integer.toHexString(1)),
        new TServerStatusImpl(new org.apache.accumulo.core.master.thrift.TabletServerStatus()));
    allTabletServers.put(new TabletServerIdImpl("192.168.0.6", 9997, Integer.toHexString(1)),
        new TServerStatusImpl(new org.apache.accumulo.core.master.thrift.TabletServerStatus()));
    allTabletServers.put(new TabletServerIdImpl("192.168.0.7", 9997, Integer.toHexString(1)),
        new TServerStatusImpl(new org.apache.accumulo.core.master.thrift.TabletServerStatus()));
    allTabletServers.put(new TabletServerIdImpl("192.168.0.8", 9997, Integer.toHexString(1)),
        new TServerStatusImpl(new org.apache.accumulo.core.master.thrift.TabletServerStatus()));
    allTabletServers.put(new TabletServerIdImpl("192.168.0.9", 9997, Integer.toHexString(1)),
        new TServerStatusImpl(new org.apache.accumulo.core.master.thrift.TabletServerStatus()));
    allTabletServers.put(new TabletServerIdImpl("192.168.0.10", 9997, Integer.toHexString(1)),
        new TServerStatusImpl(new org.apache.accumulo.core.master.thrift.TabletServerStatus()));
    allTabletServers.put(new TabletServerIdImpl("192.168.0.11", 9997, Integer.toHexString(1)),
        new TServerStatusImpl(new org.apache.accumulo.core.master.thrift.TabletServerStatus()));
    allTabletServers.put(new TabletServerIdImpl("192.168.0.12", 9997, Integer.toHexString(1)),
        new TServerStatusImpl(new org.apache.accumulo.core.master.thrift.TabletServerStatus()));
    allTabletServers.put(new TabletServerIdImpl("192.168.0.13", 9997, Integer.toHexString(1)),
        new TServerStatusImpl(new org.apache.accumulo.core.master.thrift.TabletServerStatus()));
    allTabletServers.put(new TabletServerIdImpl("192.168.0.14", 9997, Integer.toHexString(1)),
        new TServerStatusImpl(new org.apache.accumulo.core.master.thrift.TabletServerStatus()));
    allTabletServers.put(new TabletServerIdImpl("192.168.0.15", 9997, Integer.toHexString(1)),
        new TServerStatusImpl(new org.apache.accumulo.core.master.thrift.TabletServerStatus()));

    initialTableLocation.put(FOO.getTableName(),
        new TabletServerIdImpl("192.168.0.1", 9997, Integer.toHexString(1)));
    initialTableLocation.put(BAR.getTableName(),
        new TabletServerIdImpl("192.168.0.6", 9997, Integer.toHexString(1)));
    initialTableLocation.put(BAZ.getTableName(),
        new TabletServerIdImpl("192.168.0.11", 9997, Integer.toHexString(1)));

    tableTablets.put(FOO.getTableName(), new ArrayList<>());
    tableTablets.get(FOO.getTableName())
        .add(new TabletIdImpl(new KeyExtent(FOO.getId(), new Text("1"), new Text("0"))));
    tableTablets.get(FOO.getTableName())
        .add(new TabletIdImpl(new KeyExtent(FOO.getId(), new Text("2"), new Text("1"))));
    tableTablets.get(FOO.getTableName())
        .add(new TabletIdImpl(new KeyExtent(FOO.getId(), new Text("3"), new Text("2"))));
    tableTablets.get(FOO.getTableName())
        .add(new TabletIdImpl(new KeyExtent(FOO.getId(), new Text("4"), new Text("3"))));
    tableTablets.get(FOO.getTableName())
        .add(new TabletIdImpl(new KeyExtent(FOO.getId(), new Text("5"), new Text("4"))));
    tableTablets.put(BAR.getTableName(), new ArrayList<>());
    tableTablets.get(BAR.getTableName())
        .add(new TabletIdImpl(new KeyExtent(BAR.getId(), new Text("11"), new Text("10"))));
    tableTablets.get(BAR.getTableName())
        .add(new TabletIdImpl(new KeyExtent(BAR.getId(), new Text("12"), new Text("11"))));
    tableTablets.get(BAR.getTableName())
        .add(new TabletIdImpl(new KeyExtent(BAR.getId(), new Text("13"), new Text("12"))));
    tableTablets.get(BAR.getTableName())
        .add(new TabletIdImpl(new KeyExtent(BAR.getId(), new Text("14"), new Text("13"))));
    tableTablets.get(BAR.getTableName())
        .add(new TabletIdImpl(new KeyExtent(BAR.getId(), new Text("15"), new Text("14"))));
    tableTablets.put(BAZ.getTableName(), new ArrayList<>());
    tableTablets.get(BAZ.getTableName())
        .add(new TabletIdImpl(new KeyExtent(BAZ.getId(), new Text("21"), new Text("20"))));
    tableTablets.get(BAZ.getTableName())
        .add(new TabletIdImpl(new KeyExtent(BAZ.getId(), new Text("22"), new Text("21"))));
    tableTablets.get(BAZ.getTableName())
        .add(new TabletIdImpl(new KeyExtent(BAZ.getId(), new Text("23"), new Text("22"))));
    tableTablets.get(BAZ.getTableName())
        .add(new TabletIdImpl(new KeyExtent(BAZ.getId(), new Text("24"), new Text("23"))));
    tableTablets.get(BAZ.getTableName())
        .add(new TabletIdImpl(new KeyExtent(BAZ.getId(), new Text("25"), new Text("24"))));

  }

  protected boolean tabletInBounds(TabletId tabletId, TabletServerId tsi) {
    String tid = tabletId.getTable().canonical();
    String host = tsi.getHost();
    if (tid.equals("1")
        && (host.equals("192.168.0.1") || host.equals("192.168.0.2") || host.equals("192.168.0.3")
            || host.equals("192.168.0.4") || host.equals("192.168.0.5"))) {
      return true;
    } else if (tid.equals("2")
        && (host.equals("192.168.0.6") || host.equals("192.168.0.7") || host.equals("192.168.0.8")
            || host.equals("192.168.0.9") || host.equals("192.168.0.10"))) {
      return true;
    } else {
      return tid.equals("3") && (host.equals("192.168.0.11") || host.equals("192.168.0.12")
          || host.equals("192.168.0.13") || host.equals("192.168.0.14")
          || host.equals("192.168.0.15"));
    }
  }

  protected String idToTableName(TableId id) {
    if (id.equals(FOO.getId())) {
      return FOO.getTableName();
    } else if (id.equals(BAR.getId())) {
      return BAR.getTableName();
    } else if (id.equals(BAZ.getId())) {
      return BAZ.getTableName();
    } else {
      return null;
    }
  }

  @Override
  protected TabletBalancer getBalancerForTable(TableId table) {
    return new TestSimpleBalancer();
  }

  @Override
  protected String getNameFromIp(String hostIp) throws UnknownHostException {
    if (servers.containsKey(hostIp)) {
      return servers.get(hostIp);
    } else {
      throw new UnknownHostException();
    }
  }

  protected SortedMap<TabletServerId,TServerStatus> createCurrent(int numTservers) {
    String base = "192.168.0.";
    TreeMap<TabletServerId,TServerStatus> current = new TreeMap<>();
    for (int i = 1; i <= numTservers; i++) {
      TServerStatusImpl status =
          new TServerStatusImpl(new org.apache.accumulo.core.master.thrift.TabletServerStatus());
      Map<String,TableStatistics> tableMap = new HashMap<>();
      tableMap.put(FOO.getId().canonical(), new TableStatisticsImpl(new TableInfo()));
      tableMap.put(BAR.getId().canonical(), new TableStatisticsImpl(new TableInfo()));
      tableMap.put(BAZ.getId().canonical(), new TableStatisticsImpl(new TableInfo()));
      status.setTableMap(tableMap);
      current.put(new TabletServerIdImpl(base + i, 9997, Integer.toHexString(1)), status);
    }
    // now put all of the tablets on one server
    for (Map.Entry<String,TabletServerId> entry : initialTableLocation.entrySet()) {
      TServerStatus status = current.get(entry.getValue());
      if (status != null) {
        TableId tableId = environment.getTableIdMap().get(entry.getKey());
        ((TableStatisticsImpl) status.getTableMap().get(tableId.canonical()))
            .setOnlineTabletCount(5);
      }
    }
    return current;
  }
}

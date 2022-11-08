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

import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.eq;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.replay;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.UUID;
import java.util.function.Predicate;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.clientImpl.ClientContext;
import org.apache.accumulo.core.clientImpl.Namespace;
import org.apache.accumulo.core.clientImpl.TableOperationsImpl;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.ConfigurationCopy;
import org.apache.accumulo.core.conf.DefaultConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.conf.SiteConfiguration;
import org.apache.accumulo.core.data.InstanceId;
import org.apache.accumulo.core.data.NamespaceId;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.master.thrift.TableInfo;
import org.apache.accumulo.core.master.thrift.TabletServerStatus;
import org.apache.accumulo.core.metadata.TServerInstance;
import org.apache.accumulo.core.tabletserver.thrift.TabletStats;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.conf.NamespaceConfiguration;
import org.apache.accumulo.server.conf.ServerConfigurationFactory;
import org.apache.accumulo.server.conf.TableConfiguration;
import org.apache.accumulo.server.conf.codec.VersionedProperties;
import org.apache.accumulo.server.conf.store.NamespacePropKey;
import org.apache.accumulo.server.conf.store.PropStore;
import org.apache.accumulo.server.conf.store.TablePropKey;
import org.apache.accumulo.server.conf.store.impl.ZooPropStore;
import org.apache.hadoop.io.Text;
import org.easymock.EasyMock;

@Deprecated(since = "2.1.0")
public abstract class BaseHostRegexTableLoadBalancerTest extends HostRegexTableLoadBalancer {

  protected static class TestTable {
    private String tableName;
    private TableId id;

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
        TestDefaultBalancer.class.getName());
  }

  private static SiteConfiguration siteConfg = SiteConfiguration.empty().build();

  protected static class TestServerConfigurationFactory extends ServerConfigurationFactory {

    final ServerContext context;
    private ConfigurationCopy config;

    public TestServerConfigurationFactory(ServerContext context) {
      super(context, siteConfg);
      this.context = context;
      this.config = new ConfigurationCopy(DEFAULT_TABLE_PROPERTIES);
    }

    @Override
    public synchronized AccumuloConfiguration getSystemConfiguration() {
      return config;
    }

    @Override
    public TableConfiguration getTableConfiguration(final TableId tableId) {
      // create a dummy namespaceConfiguration to satisfy requireNonNull in TableConfiguration
      // constructor
      NamespaceConfiguration dummyConf = new NamespaceConfiguration(context, Namespace.DEFAULT.id(),
          DefaultConfiguration.getInstance());
      return new TableConfiguration(context, tableId, dummyConf) {
        @Override
        public String get(Property property) {
          return getSystemConfiguration().get(property.name());
        }

        @Override
        public void getProperties(Map<String,String> props, Predicate<String> filter) {
          getSystemConfiguration().getProperties(props, filter);
        }

        @Override
        public long getUpdateCount() {
          return 0;
        }
      };
    }
  }

  protected static final TestTable FOO = new TestTable("foo", TableId.of("1"));
  protected static final TestTable BAR = new TestTable("bar", TableId.of("2"));
  protected static final TestTable BAZ = new TestTable("baz", TableId.of("3"));

  protected class TestDefaultBalancer extends DefaultLoadBalancer {
    @Override
    public List<TabletStats> getOnlineTabletsForTable(TServerInstance tserver, TableId tableId) {
      String tableName = idToTableName(tableId);
      TServerInstance initialLocation = initialTableLocation.get(tableName);
      if (tserver.equals(initialLocation)) {
        List<TabletStats> list = new ArrayList<>(5);
        for (KeyExtent extent : tableExtents.get(tableName)) {
          TabletStats stats = new TabletStats();
          stats.setExtent(extent.toThrift());
          list.add(stats);
        }
        return list;
      }
      return null;
    }
  }

  protected ServerContext createMockContext() {
    InstanceId instanceId = InstanceId.of(UUID.randomUUID());

    ServerContext mockContext = createMock(ServerContext.class);
    PropStore propStore = createMock(ZooPropStore.class);
    expect(mockContext.getProperties()).andReturn(new Properties()).anyTimes();
    expect(mockContext.getZooKeepers()).andReturn("").anyTimes();
    expect(mockContext.getInstanceName()).andReturn("test").anyTimes();
    expect(mockContext.getZooKeepersSessionTimeOut()).andReturn(30).anyTimes();
    expect(mockContext.getInstanceID()).andReturn(instanceId).anyTimes();
    expect(mockContext.getZooKeeperRoot()).andReturn(Constants.ZROOT + "/1111").anyTimes();

    expect(mockContext.getPropStore()).andReturn(propStore).anyTimes();
    propStore.registerAsListener(anyObject(), anyObject());
    expectLastCall().anyTimes();

    expect(propStore.get(eq(NamespacePropKey.of(instanceId, NamespaceId.of("+default")))))
        .andReturn(new VersionedProperties()).anyTimes();

    expect(propStore.get(eq(TablePropKey.of(instanceId, TableId.of("1")))))
        .andReturn(new VersionedProperties()).anyTimes();

    expect(propStore.get(eq(TablePropKey.of(instanceId, TableId.of("2")))))
        .andReturn(new VersionedProperties()).anyTimes();

    expect(propStore.get(eq(TablePropKey.of(instanceId, TableId.of("3")))))
        .andReturn(new VersionedProperties()).anyTimes();

    replay(propStore);
    return mockContext;
  }

  protected final Map<String,String> servers = new HashMap<>(15);
  protected final SortedMap<TServerInstance,TabletServerStatus> allTabletServers = new TreeMap<>();
  protected final Map<String,List<KeyExtent>> tableExtents = new HashMap<>(3);
  protected final Map<String,TServerInstance> initialTableLocation = new HashMap<>(3);

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

    allTabletServers.put(new TServerInstance("192.168.0.1:9997", 1), new TabletServerStatus());
    allTabletServers.put(new TServerInstance("192.168.0.2:9997", 1), new TabletServerStatus());
    allTabletServers.put(new TServerInstance("192.168.0.3:9997", 1), new TabletServerStatus());
    allTabletServers.put(new TServerInstance("192.168.0.4:9997", 1), new TabletServerStatus());
    allTabletServers.put(new TServerInstance("192.168.0.5:9997", 1), new TabletServerStatus());
    allTabletServers.put(new TServerInstance("192.168.0.6:9997", 1), new TabletServerStatus());
    allTabletServers.put(new TServerInstance("192.168.0.7:9997", 1), new TabletServerStatus());
    allTabletServers.put(new TServerInstance("192.168.0.8:9997", 1), new TabletServerStatus());
    allTabletServers.put(new TServerInstance("192.168.0.9:9997", 1), new TabletServerStatus());
    allTabletServers.put(new TServerInstance("192.168.0.10:9997", 1), new TabletServerStatus());
    allTabletServers.put(new TServerInstance("192.168.0.11:9997", 1), new TabletServerStatus());
    allTabletServers.put(new TServerInstance("192.168.0.12:9997", 1), new TabletServerStatus());
    allTabletServers.put(new TServerInstance("192.168.0.13:9997", 1), new TabletServerStatus());
    allTabletServers.put(new TServerInstance("192.168.0.14:9997", 1), new TabletServerStatus());
    allTabletServers.put(new TServerInstance("192.168.0.15:9997", 1), new TabletServerStatus());

    initialTableLocation.put(FOO.getTableName(), new TServerInstance("192.168.0.1:9997", 1));
    initialTableLocation.put(BAR.getTableName(), new TServerInstance("192.168.0.6:9997", 1));
    initialTableLocation.put(BAZ.getTableName(), new TServerInstance("192.168.0.11:9997", 1));

    tableExtents.put(FOO.getTableName(), new ArrayList<>());
    tableExtents.get(FOO.getTableName())
        .add(new KeyExtent(FOO.getId(), new Text("1"), new Text("0")));
    tableExtents.get(FOO.getTableName())
        .add(new KeyExtent(FOO.getId(), new Text("2"), new Text("1")));
    tableExtents.get(FOO.getTableName())
        .add(new KeyExtent(FOO.getId(), new Text("3"), new Text("2")));
    tableExtents.get(FOO.getTableName())
        .add(new KeyExtent(FOO.getId(), new Text("4"), new Text("3")));
    tableExtents.get(FOO.getTableName())
        .add(new KeyExtent(FOO.getId(), new Text("5"), new Text("4")));
    tableExtents.put(BAR.getTableName(), new ArrayList<>());
    tableExtents.get(BAR.getTableName())
        .add(new KeyExtent(BAR.getId(), new Text("11"), new Text("10")));
    tableExtents.get(BAR.getTableName())
        .add(new KeyExtent(BAR.getId(), new Text("12"), new Text("11")));
    tableExtents.get(BAR.getTableName())
        .add(new KeyExtent(BAR.getId(), new Text("13"), new Text("12")));
    tableExtents.get(BAR.getTableName())
        .add(new KeyExtent(BAR.getId(), new Text("14"), new Text("13")));
    tableExtents.get(BAR.getTableName())
        .add(new KeyExtent(BAR.getId(), new Text("15"), new Text("14")));
    tableExtents.put(BAZ.getTableName(), new ArrayList<>());
    tableExtents.get(BAZ.getTableName())
        .add(new KeyExtent(BAZ.getId(), new Text("21"), new Text("20")));
    tableExtents.get(BAZ.getTableName())
        .add(new KeyExtent(BAZ.getId(), new Text("22"), new Text("21")));
    tableExtents.get(BAZ.getTableName())
        .add(new KeyExtent(BAZ.getId(), new Text("23"), new Text("22")));
    tableExtents.get(BAZ.getTableName())
        .add(new KeyExtent(BAZ.getId(), new Text("24"), new Text("23")));
    tableExtents.get(BAZ.getTableName())
        .add(new KeyExtent(BAZ.getId(), new Text("25"), new Text("24")));

  }

  protected boolean tabletInBounds(KeyExtent ke, TServerInstance tsi) {
    String tid = ke.tableId().canonical();
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
  protected TableOperations getTableOperations() {
    return new TableOperationsImpl(EasyMock.createMock(ClientContext.class)) {
      @Override
      public Map<String,String> tableIdMap() {
        HashMap<String,String> tables = new HashMap<>();
        tables.put(FOO.getTableName(), FOO.getId().canonical());
        tables.put(BAR.getTableName(), BAR.getId().canonical());
        tables.put(BAZ.getTableName(), BAZ.getId().canonical());
        return tables;
      }

      @Override
      public SortedSet<String> list() {
        TreeSet<String> tables = new TreeSet<>();
        tables.add(BAR.getTableName());
        tables.add(BAZ.getTableName());
        tables.add(FOO.getTableName());
        return tables;
      }
    };
  }

  @Override
  protected TabletBalancer getBalancerForTable(TableId table) {
    return new TestDefaultBalancer();
  }

  @Override
  protected String getNameFromIp(String hostIp) throws UnknownHostException {
    if (servers.containsKey(hostIp)) {
      return servers.get(hostIp);
    } else {
      throw new UnknownHostException();
    }
  }

  protected SortedMap<TServerInstance,TabletServerStatus> createCurrent(int numTservers) {
    String base = "192.168.0.";
    TreeMap<TServerInstance,TabletServerStatus> current = new TreeMap<>();
    for (int i = 1; i <= numTservers; i++) {
      TabletServerStatus status = new TabletServerStatus();
      Map<String,TableInfo> tableMap = new HashMap<>();
      tableMap.put(FOO.getId().canonical(), new TableInfo());
      tableMap.put(BAR.getId().canonical(), new TableInfo());
      tableMap.put(BAZ.getId().canonical(), new TableInfo());
      status.setTableMap(tableMap);
      current.put(new TServerInstance(base + i + ":9997", 1), status);
    }
    // now put all of the tablets on one server
    for (Map.Entry<String,TServerInstance> entry : initialTableLocation.entrySet()) {
      TabletServerStatus status = current.get(entry.getValue());
      if (status != null) {
        String tableId = getTableOperations().tableIdMap().get(entry.getKey());
        status.getTableMap().get(tableId).setOnlineTablets(5);
      }
    }
    return current;
  }
}

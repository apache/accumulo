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

import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.regex.Pattern;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.client.impl.ClientContext;
import org.apache.accumulo.core.client.impl.TableOperationsImpl;
import org.apache.accumulo.core.client.impl.thrift.ThriftSecurityException;
import org.apache.accumulo.core.client.security.tokens.AuthenticationToken;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.ConfigurationCopy;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.impl.KeyExtent;
import org.apache.accumulo.core.data.thrift.TKeyExtent;
import org.apache.accumulo.core.master.thrift.TabletServerStatus;
import org.apache.accumulo.core.tabletserver.thrift.TabletStats;
import org.apache.accumulo.fate.util.UtilWaitThread;
import org.apache.accumulo.server.conf.ServerConfiguration;
import org.apache.accumulo.server.conf.ServerConfigurationFactory;
import org.apache.accumulo.server.conf.TableConfiguration;
import org.apache.accumulo.server.master.state.TServerInstance;
import org.apache.accumulo.server.master.state.TabletMigration;
import org.apache.hadoop.io.Text;
import org.apache.thrift.TException;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Test;

import com.google.common.base.Predicate;

public class HostRegexTableLoadBalancerTest extends HostRegexTableLoadBalancer {

  static class TestInstance implements Instance {

    @Override
    public String getRootTabletLocation() {
      throw new UnsupportedOperationException();
    }

    @Override
    public List<String> getMasterLocations() {
      throw new UnsupportedOperationException();
    }

    @Override
    public String getInstanceID() {
      return "1111";
    }

    @Override
    public String getInstanceName() {
      return "test";
    }

    @Override
    public String getZooKeepers() {
      return "";
    }

    @Override
    public int getZooKeepersSessionTimeOut() {
      return 30;
    }

    @Override
    public Connector getConnector(String user, byte[] pass) throws AccumuloException, AccumuloSecurityException {
      throw new UnsupportedOperationException();
    }

    @Override
    public Connector getConnector(String user, ByteBuffer pass) throws AccumuloException, AccumuloSecurityException {
      throw new UnsupportedOperationException();
    }

    @Override
    public Connector getConnector(String user, CharSequence pass) throws AccumuloException, AccumuloSecurityException {
      throw new UnsupportedOperationException();
    }

    @Override
    public AccumuloConfiguration getConfiguration() {
      throw new UnsupportedOperationException();
    }

    @Override
    public void setConfiguration(AccumuloConfiguration conf) {}

    @Override
    public Connector getConnector(String principal, AuthenticationToken token) throws AccumuloException, AccumuloSecurityException {
      throw new UnsupportedOperationException();
    }

  }

  private static class Table {
    private String tableName;
    private String id;

    Table(String tableName, String id) {
      this.tableName = tableName;
      this.id = id;
    }

    public String getTableName() {
      return tableName;
    }

    public String getId() {
      return id;
    }
  }

  static class TestServerConfigurationFactory extends ServerConfigurationFactory {

    public TestServerConfigurationFactory(Instance instance) {
      super(instance);
    }

    @Override
    public synchronized AccumuloConfiguration getConfiguration() {
      HashMap<String,String> props = new HashMap<>();
      props.put(HostRegexTableLoadBalancer.HOST_BALANCER_OOB_CHECK, "10s");
      props.put(HostRegexTableLoadBalancer.HOST_BALANCER_POOL_RECHECK_KEY, "30s");
      return new ConfigurationCopy(props);
    }

    @Override
    public TableConfiguration getTableConfiguration(String tableId) {
      return new TableConfiguration(getInstance(), tableId, null) {
        HashMap<String,String> tableProperties = new HashMap<>();
        {
          tableProperties.put(HostRegexTableLoadBalancer.HOST_BALANCER_PREFIX + FOO.getTableName(), "r01.*");
          tableProperties.put(HostRegexTableLoadBalancer.HOST_BALANCER_PREFIX + BAR.getTableName(), "r02.*");
        }

        @Override
        public String get(Property property) {
          return tableProperties.get(property.name());
        }

        @Override
        public void getProperties(Map<String,String> props, Predicate<String> filter) {
          for (Entry<String,String> e : tableProperties.entrySet()) {
            if (filter.apply(e.getKey())) {
              props.put(e.getKey(), e.getValue());
            }
          }
        }
      };
    }
  }

  private static final Table FOO = new Table("foo", "1");
  private static final Table BAR = new Table("bar", "2");
  private static final Table BAZ = new Table("baz", "3");

  private final TestInstance instance = new TestInstance();
  private final TestServerConfigurationFactory factory = new TestServerConfigurationFactory(instance);
  private final Map<String,String> servers = new HashMap<>(15);
  private final SortedMap<TServerInstance,TabletServerStatus> allTabletServers = new TreeMap<>();
  private final Map<String,List<KeyExtent>> tableExtents = new HashMap<>(3);

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

    tableExtents.put(FOO.getTableName(), new ArrayList<KeyExtent>());
    tableExtents.get(FOO.getTableName()).add(new KeyExtent(new Text(FOO.getId()), new Text("1"), new Text("0")));
    tableExtents.get(FOO.getTableName()).add(new KeyExtent(new Text(FOO.getId()), new Text("2"), new Text("1")));
    tableExtents.get(FOO.getTableName()).add(new KeyExtent(new Text(FOO.getId()), new Text("3"), new Text("2")));
    tableExtents.get(FOO.getTableName()).add(new KeyExtent(new Text(FOO.getId()), new Text("4"), new Text("3")));
    tableExtents.get(FOO.getTableName()).add(new KeyExtent(new Text(FOO.getId()), new Text("5"), new Text("4")));
    tableExtents.put(BAR.getTableName(), new ArrayList<KeyExtent>());
    tableExtents.get(BAR.getTableName()).add(new KeyExtent(new Text(BAR.getId()), new Text("11"), new Text("10")));
    tableExtents.get(BAR.getTableName()).add(new KeyExtent(new Text(BAR.getId()), new Text("12"), new Text("11")));
    tableExtents.get(BAR.getTableName()).add(new KeyExtent(new Text(BAR.getId()), new Text("13"), new Text("12")));
    tableExtents.get(BAR.getTableName()).add(new KeyExtent(new Text(BAR.getId()), new Text("14"), new Text("13")));
    tableExtents.get(BAR.getTableName()).add(new KeyExtent(new Text(BAR.getId()), new Text("15"), new Text("14")));
    tableExtents.put(BAZ.getTableName(), new ArrayList<KeyExtent>());
    tableExtents.get(BAZ.getTableName()).add(new KeyExtent(new Text(BAZ.getId()), new Text("21"), new Text("20")));
    tableExtents.get(BAZ.getTableName()).add(new KeyExtent(new Text(BAZ.getId()), new Text("22"), new Text("21")));
    tableExtents.get(BAZ.getTableName()).add(new KeyExtent(new Text(BAZ.getId()), new Text("23"), new Text("22")));
    tableExtents.get(BAZ.getTableName()).add(new KeyExtent(new Text(BAZ.getId()), new Text("24"), new Text("23")));
    tableExtents.get(BAZ.getTableName()).add(new KeyExtent(new Text(BAZ.getId()), new Text("25"), new Text("24")));
  }

  @Override
  protected String getNameFromIp(String hostIp) throws UnknownHostException {
    if (servers.containsKey(hostIp)) {
      return servers.get(hostIp);
    } else {
      throw new UnknownHostException();
    }
  }

  @Override
  protected TableOperations getTableOperations() {
    return new TableOperationsImpl(EasyMock.createMock(ClientContext.class)) {
      @Override
      public Map<String,String> tableIdMap() {
        HashMap<String,String> tables = new HashMap<>();
        tables.put(FOO.getTableName(), FOO.getId());
        tables.put(BAR.getTableName(), BAR.getId());
        tables.put(BAZ.getTableName(), BAZ.getId());
        return tables;
      }
    };
  }

  @Override
  protected TabletBalancer getBalancerForTable(String table) {
    return new DefaultLoadBalancer();
  }

  private SortedMap<TServerInstance,TabletServerStatus> createCurrent(int numTservers) {
    String base = "192.168.0.";
    TreeMap<TServerInstance,TabletServerStatus> current = new TreeMap<>();
    for (int i = 1; i <= numTservers; i++) {
      current.put(new TServerInstance(base + i + ":9997", 1), new TabletServerStatus());
    }
    return current;
  }

  @Test
  public void testInit() {
    init((ServerConfiguration) factory);
    Assert.assertEquals("OOB check interval value is incorrect", 10000, this.getOobCheckMillis());
    Assert.assertEquals("Pool check interval value is incorrect", 30000, this.getPoolRecheckMillis());
    Assert.assertFalse(isIpBasedRegex());
    Map<String,Pattern> patterns = this.getPoolNameToRegexPattern();
    Assert.assertEquals(2, patterns.size());
    Assert.assertTrue(patterns.containsKey(FOO.getTableName()));
    Assert.assertEquals(Pattern.compile("r01.*").pattern(), patterns.get(FOO.getTableName()).pattern());
    Assert.assertTrue(patterns.containsKey(BAR.getTableName()));
    Assert.assertEquals(Pattern.compile("r02.*").pattern(), patterns.get(BAR.getTableName()).pattern());
    Map<String,String> tids = this.getTableIdToTableName();
    Assert.assertEquals(3, tids.size());
    Assert.assertTrue(tids.containsKey(FOO.getId()));
    Assert.assertEquals(FOO.getTableName(), tids.get(FOO.getId()));
    Assert.assertTrue(tids.containsKey(BAR.getId()));
    Assert.assertEquals(BAR.getTableName(), tids.get(BAR.getId()));
    Assert.assertTrue(tids.containsKey(BAZ.getId()));
    Assert.assertEquals(BAZ.getTableName(), tids.get(BAZ.getId()));
    Assert.assertEquals(false, this.isIpBasedRegex());
  }

  @Test
  public void testBalanceWithMigrations() {
    List<TabletMigration> migrations = new ArrayList<>();
    init((ServerConfiguration) factory);
    long wait = this.balance(Collections.unmodifiableSortedMap(createCurrent(2)), Collections.singleton(new KeyExtent()), migrations);
    Assert.assertEquals(5000, wait);
    Assert.assertEquals(0, migrations.size());
  }

  @Test
  public void testSplitCurrentByRegexUsingHostname() {
    init((ServerConfiguration) factory);
    Map<String,SortedMap<TServerInstance,TabletServerStatus>> groups = this.splitCurrentByRegex(createCurrent(15));
    Assert.assertEquals(3, groups.size());
    Assert.assertTrue(groups.containsKey(FOO.getTableName()));
    SortedMap<TServerInstance,TabletServerStatus> fooHosts = groups.get(FOO.getTableName());
    Assert.assertEquals(5, fooHosts.size());
    Assert.assertTrue(fooHosts.containsKey(new TServerInstance("192.168.0.1:9997", 1)));
    Assert.assertTrue(fooHosts.containsKey(new TServerInstance("192.168.0.2:9997", 1)));
    Assert.assertTrue(fooHosts.containsKey(new TServerInstance("192.168.0.3:9997", 1)));
    Assert.assertTrue(fooHosts.containsKey(new TServerInstance("192.168.0.4:9997", 1)));
    Assert.assertTrue(fooHosts.containsKey(new TServerInstance("192.168.0.5:9997", 1)));
    Assert.assertTrue(groups.containsKey(BAR.getTableName()));
    SortedMap<TServerInstance,TabletServerStatus> barHosts = groups.get(BAR.getTableName());
    Assert.assertEquals(5, barHosts.size());
    Assert.assertTrue(barHosts.containsKey(new TServerInstance("192.168.0.6:9997", 1)));
    Assert.assertTrue(barHosts.containsKey(new TServerInstance("192.168.0.7:9997", 1)));
    Assert.assertTrue(barHosts.containsKey(new TServerInstance("192.168.0.8:9997", 1)));
    Assert.assertTrue(barHosts.containsKey(new TServerInstance("192.168.0.9:9997", 1)));
    Assert.assertTrue(barHosts.containsKey(new TServerInstance("192.168.0.10:9997", 1)));
    Assert.assertTrue(groups.containsKey(DEFAULT_POOL));
    SortedMap<TServerInstance,TabletServerStatus> defHosts = groups.get(DEFAULT_POOL);
    Assert.assertEquals(5, defHosts.size());
    Assert.assertTrue(defHosts.containsKey(new TServerInstance("192.168.0.11:9997", 1)));
    Assert.assertTrue(defHosts.containsKey(new TServerInstance("192.168.0.12:9997", 1)));
    Assert.assertTrue(defHosts.containsKey(new TServerInstance("192.168.0.13:9997", 1)));
    Assert.assertTrue(defHosts.containsKey(new TServerInstance("192.168.0.14:9997", 1)));
    Assert.assertTrue(defHosts.containsKey(new TServerInstance("192.168.0.15:9997", 1)));
  }

  @Test
  public void testSplitCurrentByRegexUsingOverlappingPools() {
    init((ServerConfiguration) new TestServerConfigurationFactory(instance) {
      @Override
      public TableConfiguration getTableConfiguration(String tableId) {
        return new TableConfiguration(getInstance(), tableId, null) {
          HashMap<String,String> tableProperties = new HashMap<>();
          {
            tableProperties.put(HostRegexTableLoadBalancer.HOST_BALANCER_PREFIX + FOO.getTableName(), "r.*");
            tableProperties.put(HostRegexTableLoadBalancer.HOST_BALANCER_PREFIX + BAR.getTableName(), "r01.*|r02.*");
          }

          @Override
          public String get(Property property) {
            return tableProperties.get(property.name());
          }

          @Override
          public void getProperties(Map<String,String> props, Predicate<String> filter) {
            for (Entry<String,String> e : tableProperties.entrySet()) {
              if (filter.apply(e.getKey())) {
                props.put(e.getKey(), e.getValue());
              }
            }
          }
        };
      }
    });
    Map<String,SortedMap<TServerInstance,TabletServerStatus>> groups = this.splitCurrentByRegex(createCurrent(15));
    Assert.assertEquals(2, groups.size());
    Assert.assertTrue(groups.containsKey(FOO.getTableName()));
    SortedMap<TServerInstance,TabletServerStatus> fooHosts = groups.get(FOO.getTableName());
    Assert.assertEquals(15, fooHosts.size());
    Assert.assertTrue(fooHosts.containsKey(new TServerInstance("192.168.0.1:9997", 1)));
    Assert.assertTrue(fooHosts.containsKey(new TServerInstance("192.168.0.2:9997", 1)));
    Assert.assertTrue(fooHosts.containsKey(new TServerInstance("192.168.0.3:9997", 1)));
    Assert.assertTrue(fooHosts.containsKey(new TServerInstance("192.168.0.4:9997", 1)));
    Assert.assertTrue(fooHosts.containsKey(new TServerInstance("192.168.0.5:9997", 1)));
    Assert.assertTrue(fooHosts.containsKey(new TServerInstance("192.168.0.6:9997", 1)));
    Assert.assertTrue(fooHosts.containsKey(new TServerInstance("192.168.0.7:9997", 1)));
    Assert.assertTrue(fooHosts.containsKey(new TServerInstance("192.168.0.8:9997", 1)));
    Assert.assertTrue(fooHosts.containsKey(new TServerInstance("192.168.0.9:9997", 1)));
    Assert.assertTrue(fooHosts.containsKey(new TServerInstance("192.168.0.10:9997", 1)));
    Assert.assertTrue(fooHosts.containsKey(new TServerInstance("192.168.0.11:9997", 1)));
    Assert.assertTrue(fooHosts.containsKey(new TServerInstance("192.168.0.12:9997", 1)));
    Assert.assertTrue(fooHosts.containsKey(new TServerInstance("192.168.0.13:9997", 1)));
    Assert.assertTrue(fooHosts.containsKey(new TServerInstance("192.168.0.14:9997", 1)));
    Assert.assertTrue(fooHosts.containsKey(new TServerInstance("192.168.0.15:9997", 1)));
    Assert.assertTrue(groups.containsKey(BAR.getTableName()));
    SortedMap<TServerInstance,TabletServerStatus> barHosts = groups.get(BAR.getTableName());
    Assert.assertEquals(10, barHosts.size());
    Assert.assertTrue(barHosts.containsKey(new TServerInstance("192.168.0.1:9997", 1)));
    Assert.assertTrue(barHosts.containsKey(new TServerInstance("192.168.0.2:9997", 1)));
    Assert.assertTrue(barHosts.containsKey(new TServerInstance("192.168.0.3:9997", 1)));
    Assert.assertTrue(barHosts.containsKey(new TServerInstance("192.168.0.4:9997", 1)));
    Assert.assertTrue(barHosts.containsKey(new TServerInstance("192.168.0.5:9997", 1)));
    Assert.assertTrue(barHosts.containsKey(new TServerInstance("192.168.0.6:9997", 1)));
    Assert.assertTrue(barHosts.containsKey(new TServerInstance("192.168.0.7:9997", 1)));
    Assert.assertTrue(barHosts.containsKey(new TServerInstance("192.168.0.8:9997", 1)));
    Assert.assertTrue(barHosts.containsKey(new TServerInstance("192.168.0.9:9997", 1)));
    Assert.assertTrue(barHosts.containsKey(new TServerInstance("192.168.0.10:9997", 1)));
  }

  @Test
  public void testSplitCurrentByRegexUsingIP() {
    init((ServerConfiguration) new TestServerConfigurationFactory(instance) {
      @Override
      public synchronized AccumuloConfiguration getConfiguration() {
        HashMap<String,String> props = new HashMap<>();
        props.put(HostRegexTableLoadBalancer.HOST_BALANCER_OOB_CHECK, "30s");
        props.put(HostRegexTableLoadBalancer.HOST_BALANCER_POOL_RECHECK_KEY, "30s");
        props.put(HostRegexTableLoadBalancer.HOST_BALANCER_REGEX_USING_IPS, "true");
        return new ConfigurationCopy(props);
      }

      @Override
      public TableConfiguration getTableConfiguration(String tableId) {
        return new TableConfiguration(getInstance(), tableId, null) {
          HashMap<String,String> tableProperties = new HashMap<>();
          {
            tableProperties.put(HostRegexTableLoadBalancer.HOST_BALANCER_PREFIX + FOO.getTableName(), "192\\.168\\.0\\.[1-5]");
            tableProperties.put(HostRegexTableLoadBalancer.HOST_BALANCER_PREFIX + BAR.getTableName(), "192\\.168\\.0\\.[6-9]|192\\.168\\.0\\.10");
          }

          @Override
          public String get(Property property) {
            return tableProperties.get(property.name());
          }

          @Override
          public void getProperties(Map<String,String> props, Predicate<String> filter) {
            for (Entry<String,String> e : tableProperties.entrySet()) {
              if (filter.apply(e.getKey())) {
                props.put(e.getKey(), e.getValue());
              }
            }
          }
        };
      }
    });
    Assert.assertTrue(isIpBasedRegex());
    Map<String,SortedMap<TServerInstance,TabletServerStatus>> groups = this.splitCurrentByRegex(createCurrent(15));
    Assert.assertEquals(3, groups.size());
    Assert.assertTrue(groups.containsKey(FOO.getTableName()));
    SortedMap<TServerInstance,TabletServerStatus> fooHosts = groups.get(FOO.getTableName());
    Assert.assertEquals(5, fooHosts.size());
    Assert.assertTrue(fooHosts.containsKey(new TServerInstance("192.168.0.1:9997", 1)));
    Assert.assertTrue(fooHosts.containsKey(new TServerInstance("192.168.0.2:9997", 1)));
    Assert.assertTrue(fooHosts.containsKey(new TServerInstance("192.168.0.3:9997", 1)));
    Assert.assertTrue(fooHosts.containsKey(new TServerInstance("192.168.0.4:9997", 1)));
    Assert.assertTrue(fooHosts.containsKey(new TServerInstance("192.168.0.5:9997", 1)));
    Assert.assertTrue(groups.containsKey(BAR.getTableName()));
    SortedMap<TServerInstance,TabletServerStatus> barHosts = groups.get(BAR.getTableName());
    Assert.assertEquals(5, barHosts.size());
    Assert.assertTrue(barHosts.containsKey(new TServerInstance("192.168.0.6:9997", 1)));
    Assert.assertTrue(barHosts.containsKey(new TServerInstance("192.168.0.7:9997", 1)));
    Assert.assertTrue(barHosts.containsKey(new TServerInstance("192.168.0.8:9997", 1)));
    Assert.assertTrue(barHosts.containsKey(new TServerInstance("192.168.0.9:9997", 1)));
    Assert.assertTrue(barHosts.containsKey(new TServerInstance("192.168.0.10:9997", 1)));
    Assert.assertTrue(groups.containsKey(DEFAULT_POOL));
    SortedMap<TServerInstance,TabletServerStatus> defHosts = groups.get(DEFAULT_POOL);
    Assert.assertEquals(5, defHosts.size());
    Assert.assertTrue(defHosts.containsKey(new TServerInstance("192.168.0.11:9997", 1)));
    Assert.assertTrue(defHosts.containsKey(new TServerInstance("192.168.0.12:9997", 1)));
    Assert.assertTrue(defHosts.containsKey(new TServerInstance("192.168.0.13:9997", 1)));
    Assert.assertTrue(defHosts.containsKey(new TServerInstance("192.168.0.14:9997", 1)));
    Assert.assertTrue(defHosts.containsKey(new TServerInstance("192.168.0.15:9997", 1)));
  }

  @Test
  public void testAllUnassigned() {
    init((ServerConfiguration) factory);
    Map<KeyExtent,TServerInstance> assignments = new HashMap<>();
    Map<KeyExtent,TServerInstance> unassigned = new HashMap<>();
    for (List<KeyExtent> extents : tableExtents.values()) {
      for (KeyExtent ke : extents) {
        unassigned.put(ke, null);
      }
    }
    this.getAssignments(Collections.unmodifiableSortedMap(allTabletServers), Collections.unmodifiableMap(unassigned), assignments);
    Assert.assertEquals(15, assignments.size());
    // Ensure unique tservers
    for (Entry<KeyExtent,TServerInstance> e : assignments.entrySet()) {
      for (Entry<KeyExtent,TServerInstance> e2 : assignments.entrySet()) {
        if (e.getKey().equals(e2.getKey())) {
          continue;
        }
        if (e.getValue().equals(e2.getValue())) {
          Assert.fail("Assignment failure");
        }
      }
    }
    // Ensure assignments are correct
    for (Entry<KeyExtent,TServerInstance> e : assignments.entrySet()) {
      if (!tabletInBounds(e.getKey(), e.getValue())) {
        Assert.fail("tablet not in bounds: " + e.getKey() + " -> " + e.getValue().host());
      }
    }
  }

  @Test
  public void testAllAssigned() {
    init((ServerConfiguration) factory);
    Map<KeyExtent,TServerInstance> assignments = new HashMap<>();
    Map<KeyExtent,TServerInstance> unassigned = new HashMap<>();
    this.getAssignments(Collections.unmodifiableSortedMap(allTabletServers), Collections.unmodifiableMap(unassigned), assignments);
    Assert.assertEquals(0, assignments.size());
  }

  @Test
  public void testPartiallyAssigned() {
    init((ServerConfiguration) factory);
    Map<KeyExtent,TServerInstance> assignments = new HashMap<>();
    Map<KeyExtent,TServerInstance> unassigned = new HashMap<>();
    int i = 0;
    for (List<KeyExtent> extents : tableExtents.values()) {
      for (KeyExtent ke : extents) {
        if ((i % 2) == 0) {
          unassigned.put(ke, null);
        }
        i++;
      }
    }
    this.getAssignments(Collections.unmodifiableSortedMap(allTabletServers), Collections.unmodifiableMap(unassigned), assignments);
    Assert.assertEquals(unassigned.size(), assignments.size());
    // Ensure unique tservers
    for (Entry<KeyExtent,TServerInstance> e : assignments.entrySet()) {
      for (Entry<KeyExtent,TServerInstance> e2 : assignments.entrySet()) {
        if (e.getKey().equals(e2.getKey())) {
          continue;
        }
        if (e.getValue().equals(e2.getValue())) {
          Assert.fail("Assignment failure");
        }
      }
    }
    // Ensure assignments are correct
    for (Entry<KeyExtent,TServerInstance> e : assignments.entrySet()) {
      if (!tabletInBounds(e.getKey(), e.getValue())) {
        Assert.fail("tablet not in bounds: " + e.getKey() + " -> " + e.getValue().host());
      }
    }
  }

  @Test
  public void testUnassignedWithNoTServers() {
    init((ServerConfiguration) factory);
    Map<KeyExtent,TServerInstance> assignments = new HashMap<>();
    Map<KeyExtent,TServerInstance> unassigned = new HashMap<>();
    for (KeyExtent ke : tableExtents.get(BAR.getTableName())) {
      unassigned.put(ke, null);
    }
    SortedMap<TServerInstance,TabletServerStatus> current = createCurrent(15);
    // Remove the BAR tablet servers from current
    List<TServerInstance> removals = new ArrayList<TServerInstance>();
    for (Entry<TServerInstance,TabletServerStatus> e : current.entrySet()) {
      if (e.getKey().host().equals("192.168.0.6") || e.getKey().host().equals("192.168.0.7") || e.getKey().host().equals("192.168.0.8")
          || e.getKey().host().equals("192.168.0.9") || e.getKey().host().equals("192.168.0.10")) {
        removals.add(e.getKey());
      }
    }
    for (TServerInstance r : removals) {
      current.remove(r);
    }
    this.getAssignments(Collections.unmodifiableSortedMap(allTabletServers), Collections.unmodifiableMap(unassigned), assignments);
    Assert.assertEquals(unassigned.size(), assignments.size());
    // Ensure assignments are correct
    for (Entry<KeyExtent,TServerInstance> e : assignments.entrySet()) {
      if (!tabletInBounds(e.getKey(), e.getValue())) {
        Assert.fail("tablet not in bounds: " + e.getKey() + " -> " + e.getValue().host());
      }
    }
  }

  @Test
  public void testOutOfBoundsTablets() {
    init((ServerConfiguration) factory);
    // Wait to trigger the out of bounds check which will call our version of getOnlineTabletsForTable
    UtilWaitThread.sleep(11000);
    Set<KeyExtent> migrations = new HashSet<KeyExtent>();
    List<TabletMigration> migrationsOut = new ArrayList<TabletMigration>();
    this.balance(createCurrent(15), migrations, migrationsOut);
    Assert.assertEquals(2, migrationsOut.size());
  }

  @Override
  public List<TabletStats> getOnlineTabletsForTable(TServerInstance tserver, String tableId) throws ThriftSecurityException, TException {
    // Report incorrect information so that balance will create an assignment
    List<TabletStats> tablets = new ArrayList<>();
    if (tableId.equals(BAR.getId()) && tserver.host().equals("192.168.0.1")) {
      // Report that we have a bar tablet on this server
      TKeyExtent tke = new TKeyExtent();
      tke.setTable(BAR.getId().getBytes());
      tke.setEndRow("11".getBytes());
      tke.setPrevEndRow("10".getBytes());
      TabletStats ts = new TabletStats();
      ts.setExtent(tke);
      tablets.add(ts);
    } else if (tableId.equals(FOO.getId()) && tserver.host().equals("192.168.0.6")) {
      // Report that we have a foo tablet on this server
      TKeyExtent tke = new TKeyExtent();
      tke.setTable(FOO.getId().getBytes());
      tke.setEndRow("1".getBytes());
      tke.setPrevEndRow("0".getBytes());
      TabletStats ts = new TabletStats();
      ts.setExtent(tke);
      tablets.add(ts);

    }
    return tablets;
  }

  private boolean tabletInBounds(KeyExtent ke, TServerInstance tsi) {
    String tid = ke.getTableId().toString();
    String host = tsi.host();
    if (tid.equals("1")
        && (host.equals("192.168.0.1") || host.equals("192.168.0.2") || host.equals("192.168.0.3") || host.equals("192.168.0.4") || host.equals("192.168.0.5"))) {
      return true;
    } else if (tid.equals("2")
        && (host.equals("192.168.0.6") || host.equals("192.168.0.7") || host.equals("192.168.0.8") || host.equals("192.168.0.9") || host.equals("192.168.0.10"))) {
      return true;
    } else if (tid.equals("3")
        && (host.equals("192.168.0.11") || host.equals("192.168.0.12") || host.equals("192.168.0.13") || host.equals("192.168.0.14") || host
            .equals("192.168.0.15"))) {
      return true;
    } else {
      return false;
    }
  }

}

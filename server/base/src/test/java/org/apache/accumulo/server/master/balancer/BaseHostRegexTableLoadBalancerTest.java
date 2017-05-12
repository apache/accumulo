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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.function.Predicate;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.client.impl.ClientContext;
import org.apache.accumulo.core.client.impl.TableOperationsImpl;
import org.apache.accumulo.core.client.security.tokens.AuthenticationToken;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.ConfigurationCopy;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.impl.KeyExtent;
import org.apache.accumulo.core.master.thrift.TabletServerStatus;
import org.apache.accumulo.server.conf.ServerConfigurationFactory;
import org.apache.accumulo.server.conf.TableConfiguration;
import org.apache.accumulo.server.master.state.TServerInstance;
import org.apache.hadoop.io.Text;
import org.easymock.EasyMock;

public abstract class BaseHostRegexTableLoadBalancerTest extends HostRegexTableLoadBalancer {

  protected static class TestInstance implements Instance {

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

    @Deprecated
    @Override
    public Connector getConnector(String user, byte[] pass) throws AccumuloException, AccumuloSecurityException {
      throw new UnsupportedOperationException();
    }

    @Deprecated
    @Override
    public Connector getConnector(String user, ByteBuffer pass) throws AccumuloException, AccumuloSecurityException {
      throw new UnsupportedOperationException();
    }

    @Deprecated
    @Override
    public Connector getConnector(String user, CharSequence pass) throws AccumuloException, AccumuloSecurityException {
      throw new UnsupportedOperationException();
    }

    @Override
    public Connector getConnector(String principal, AuthenticationToken token) throws AccumuloException, AccumuloSecurityException {
      throw new UnsupportedOperationException();
    }

  }

  protected static class Table {
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

  protected static final HashMap<String,String> DEFAULT_TABLE_PROPERTIES = new HashMap<>();
  {
    DEFAULT_TABLE_PROPERTIES.put(HostRegexTableLoadBalancer.HOST_BALANCER_OOB_CHECK_KEY, "2s");
    DEFAULT_TABLE_PROPERTIES.put(HostRegexTableLoadBalancer.HOST_BALANCER_PREFIX + FOO.getTableName(), "r01.*");
    DEFAULT_TABLE_PROPERTIES.put(HostRegexTableLoadBalancer.HOST_BALANCER_PREFIX + BAR.getTableName(), "r02.*");
  }

  protected static class TestServerConfigurationFactory extends ServerConfigurationFactory {

    public TestServerConfigurationFactory(Instance instance) {
      super(instance);
    }

    @Override
    public synchronized AccumuloConfiguration getSystemConfiguration() {
      return new ConfigurationCopy(DEFAULT_TABLE_PROPERTIES);
    }

    @Override
    public TableConfiguration getTableConfiguration(String tableId) {
      return new TableConfiguration(getInstance(), tableId, null) {
        @Override
        public String get(Property property) {
          return DEFAULT_TABLE_PROPERTIES.get(property.name());
        }

        @Override
        public void getProperties(Map<String,String> props, Predicate<String> filter) {
          for (Entry<String,String> e : DEFAULT_TABLE_PROPERTIES.entrySet()) {
            if (filter.test(e.getKey())) {
              props.put(e.getKey(), e.getValue());
            }
          }
        }
      };
    }
  }

  protected static final Table FOO = new Table("foo", "1");
  protected static final Table BAR = new Table("bar", "2");
  protected static final Table BAZ = new Table("baz", "3");

  protected final TestInstance instance = new TestInstance();
  protected final TestServerConfigurationFactory factory = new TestServerConfigurationFactory(instance);
  protected final Map<String,String> servers = new HashMap<>(15);
  protected final SortedMap<TServerInstance,TabletServerStatus> allTabletServers = new TreeMap<>();
  protected final Map<String,List<KeyExtent>> tableExtents = new HashMap<>(3);

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
    tableExtents.get(FOO.getTableName()).add(new KeyExtent(FOO.getId(), new Text("1"), new Text("0")));
    tableExtents.get(FOO.getTableName()).add(new KeyExtent(FOO.getId(), new Text("2"), new Text("1")));
    tableExtents.get(FOO.getTableName()).add(new KeyExtent(FOO.getId(), new Text("3"), new Text("2")));
    tableExtents.get(FOO.getTableName()).add(new KeyExtent(FOO.getId(), new Text("4"), new Text("3")));
    tableExtents.get(FOO.getTableName()).add(new KeyExtent(FOO.getId(), new Text("5"), new Text("4")));
    tableExtents.put(BAR.getTableName(), new ArrayList<KeyExtent>());
    tableExtents.get(BAR.getTableName()).add(new KeyExtent(BAR.getId(), new Text("11"), new Text("10")));
    tableExtents.get(BAR.getTableName()).add(new KeyExtent(BAR.getId(), new Text("12"), new Text("11")));
    tableExtents.get(BAR.getTableName()).add(new KeyExtent(BAR.getId(), new Text("13"), new Text("12")));
    tableExtents.get(BAR.getTableName()).add(new KeyExtent(BAR.getId(), new Text("14"), new Text("13")));
    tableExtents.get(BAR.getTableName()).add(new KeyExtent(BAR.getId(), new Text("15"), new Text("14")));
    tableExtents.put(BAZ.getTableName(), new ArrayList<KeyExtent>());
    tableExtents.get(BAZ.getTableName()).add(new KeyExtent(BAZ.getId(), new Text("21"), new Text("20")));
    tableExtents.get(BAZ.getTableName()).add(new KeyExtent(BAZ.getId(), new Text("22"), new Text("21")));
    tableExtents.get(BAZ.getTableName()).add(new KeyExtent(BAZ.getId(), new Text("23"), new Text("22")));
    tableExtents.get(BAZ.getTableName()).add(new KeyExtent(BAZ.getId(), new Text("24"), new Text("23")));
    tableExtents.get(BAZ.getTableName()).add(new KeyExtent(BAZ.getId(), new Text("25"), new Text("24")));
  }

  protected boolean tabletInBounds(KeyExtent ke, TServerInstance tsi) {
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
  protected TabletBalancer getBalancerForTable(String table) {
    return new DefaultLoadBalancer();
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
      current.put(new TServerInstance(base + i + ":9997", 1), new TabletServerStatus());
    }
    return current;
  }

}

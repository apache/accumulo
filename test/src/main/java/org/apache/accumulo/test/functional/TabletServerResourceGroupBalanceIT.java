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
package org.apache.accumulo.test.functional;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.admin.NewTableConfiguration;
import org.apache.accumulo.core.client.admin.TabletHostingGoal;
import org.apache.accumulo.core.clientImpl.ClientContext;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.fate.zookeeper.ZooCache;
import org.apache.accumulo.core.fate.zookeeper.ZooCache.ZcStat;
import org.apache.accumulo.core.lock.ServiceLock;
import org.apache.accumulo.core.lock.ServiceLockData;
import org.apache.accumulo.core.metadata.schema.Ample;
import org.apache.accumulo.core.metadata.schema.TabletMetadata;
import org.apache.accumulo.core.metadata.schema.TabletMetadata.Location;
import org.apache.accumulo.harness.MiniClusterConfigurationCallback;
import org.apache.accumulo.harness.SharedMiniClusterBase;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloConfigImpl;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.google.common.net.HostAndPort;

public class TabletServerResourceGroupBalanceIT extends SharedMiniClusterBase {

  public static class TSRGBalanceITConfig implements MiniClusterConfigurationCallback {

    @Override
    public void configureMiniCluster(MiniAccumuloConfigImpl cfg, Configuration coreSite) {
      cfg.setProperty(Property.MANAGER_STARTUP_TSERVER_AVAIL_MIN_COUNT, "2");
      cfg.setProperty(Property.MANAGER_STARTUP_TSERVER_AVAIL_MAX_WAIT, "10s");
      cfg.getClusterServerConfiguration().setNumDefaultTabletServers(1);
      cfg.getClusterServerConfiguration().addTabletServerResourceGroup("GROUP1", 1);
    }

  }

  @BeforeAll
  public static void beforeAll() throws Exception {
    SharedMiniClusterBase.startMiniClusterWithConfig(new TSRGBalanceITConfig());
  }

  @AfterAll
  public static void afterAll() throws Exception {
    SharedMiniClusterBase.stopMiniCluster();
  }

  private Map<String,String> getTServerGroups() throws Exception {

    Map<String,String> tservers = new HashMap<>();
    ZooCache zk = getCluster().getServerContext().getZooCache();
    String zpath = getCluster().getServerContext().getZooKeeperRoot() + Constants.ZTSERVERS;

    List<String> children = zk.getChildren(zpath);
    for (String child : children) {
      final var zLockPath = ServiceLock.path(zpath + "/" + child);
      ZcStat stat = new ZcStat();
      Optional<ServiceLockData> sld = ServiceLock.getLockData(zk, zLockPath, stat);
      HostAndPort client = sld.orElseThrow().getAddress(ServiceLockData.ThriftService.TSERV);
      String resourceGroup = sld.orElseThrow().getGroup(ServiceLockData.ThriftService.TSERV);
      tservers.put(client.toString(), resourceGroup);
    }
    return tservers;

  }

  @Test
  public void testBalancerWithResourceGroups() throws Exception {

    SortedSet<Text> splits = new TreeSet<>();
    IntStream.range(97, 122).forEach(i -> splits.add(new Text(new String("" + i))));

    NewTableConfiguration ntc1 = new NewTableConfiguration();
    ntc1.withInitialHostingGoal(TabletHostingGoal.ALWAYS);
    ntc1.withSplits(splits);

    Map<String,String> properties = new HashMap<>();
    properties.put(Property.TABLE_ASSIGNMENT_GROUP.getKey(), "GROUP1");

    NewTableConfiguration ntc2 = new NewTableConfiguration();
    ntc2.withInitialHostingGoal(TabletHostingGoal.ALWAYS);
    ntc2.withSplits(splits);
    ntc2.setProperties(properties);

    String[] names = this.getUniqueNames(2);
    try (AccumuloClient client =
        Accumulo.newClient().from(getCluster().getClientProperties()).build()) {
      client.tableOperations().create(names[0], ntc1);
      client.tableOperations().create(names[1], ntc2);
      client.instanceOperations().waitForBalance();

      Map<String,String> tserverGroups = getTServerGroups();
      assertEquals(2, tserverGroups.size());

      Ample ample = ((ClientContext) client).getAmple();

      // Check table names[0]
      String tableId = client.tableOperations().tableIdMap().get(names[0]);
      List<TabletMetadata> locations = ample.readTablets().forTable(TableId.of(tableId))
          .fetch(TabletMetadata.ColumnType.LOCATION).build().stream().collect(Collectors.toList());

      assertEquals(26, locations.size());
      Location l1 = locations.get(0).getLocation();
      assertEquals("default", tserverGroups.get(l1.getHostAndPort().toString()));
      locations.forEach(loc -> assertEquals(l1, loc.getLocation()));

      // Check table names[1]
      tableId = client.tableOperations().tableIdMap().get(names[1]);
      locations = ample.readTablets().forTable(TableId.of(tableId))
          .fetch(TabletMetadata.ColumnType.LOCATION).build().stream().collect(Collectors.toList());

      assertEquals(26, locations.size());
      Location l2 = locations.get(0).getLocation();
      assertEquals("GROUP1", tserverGroups.get(l2.getHostAndPort().toString()));
      locations.forEach(loc -> assertEquals(l2, loc.getLocation()));

    }

  }

}

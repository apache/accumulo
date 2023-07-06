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
import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.admin.NewTableConfiguration;
import org.apache.accumulo.core.client.admin.TabletHostingGoal;
import org.apache.accumulo.core.clientImpl.ClientContext;
import org.apache.accumulo.core.clientImpl.ClientTabletCache;
import org.apache.accumulo.core.clientImpl.ClientTabletCache.LocationNeed;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Range;
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
import org.apache.accumulo.minicluster.ServerType;
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
      cfg.setProperty(Property.TSERV_MIGRATE_MAXCONCURRENT, "50");
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
      try {
        HostAndPort client = sld.orElseThrow().getAddress(ServiceLockData.ThriftService.TSERV);
        String resourceGroup = sld.orElseThrow().getGroup(ServiceLockData.ThriftService.TSERV);
        tservers.put(client.toString(), resourceGroup);
      } catch (NoSuchElementException nsee) {
        // We are starting and stopping servers, so it's possible for this to occur.
      }
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
    properties.put("table.custom.assignment.group", "GROUP1");

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

      client.tableOperations().delete(names[0]);
      client.tableOperations().delete(names[1]);
    }

  }

  @Test
  public void testResourceGroupBalanceWithNoTServers() throws Exception {

    SortedSet<Text> splits = new TreeSet<>();
    IntStream.range(97, 122).forEach(i -> splits.add(new Text(new String("" + i))));

    Map<String,String> properties = new HashMap<>();
    properties.put("table.custom.assignment.group", "GROUP2");

    NewTableConfiguration ntc1 = new NewTableConfiguration();
    ntc1.withInitialHostingGoal(TabletHostingGoal.ALWAYS);
    ntc1.withSplits(splits);
    ntc1.setProperties(properties);

    String tableName = this.getUniqueNames(1)[0];
    try (final AccumuloClient client =
        Accumulo.newClient().from(getCluster().getClientProperties()).build()) {

      client.tableOperations().create(tableName, ntc1);

      assertEquals(0, getCountOfHostedTablets(client, tableName));

      AtomicReference<Exception> error = new AtomicReference<>();
      Thread ingest = new Thread(() -> {
        try {
          ReadWriteIT.ingest(client, 1000, 1, 1, 0, tableName);
          ReadWriteIT.verify(client, 1000, 1, 1, 0, tableName);
        } catch (Exception e) {
          error.set(e);
        }
      });
      ingest.start();

      assertEquals(0, getCountOfHostedTablets(client, tableName));

      // Start TabletServer for GROUP2
      getCluster().getConfig().getClusterServerConfiguration()
          .addTabletServerResourceGroup("GROUP2", 1);
      getCluster().getClusterControl().start(ServerType.TABLET_SERVER);

      client.instanceOperations().waitForBalance();
      assertEquals(26, getCountOfHostedTablets(client, tableName));
      ingest.join();
      assertNull(error.get());

      client.tableOperations().delete(tableName);
      // Stop all tablet servers because there is no way to just stop
      // the GROUP2 server yet.
      getCluster().getClusterControl().stopAllServers(ServerType.TABLET_SERVER);
      getCluster().getConfig().getClusterServerConfiguration().clearTServerResourceGroups();
      getCluster().getConfig().getClusterServerConfiguration()
          .addTabletServerResourceGroup("GROUP1", 1);
      getCluster().getClusterControl().start(ServerType.TABLET_SERVER);

    }
  }

  @Test
  public void testResourceGroupPropertyChange() throws Exception {

    SortedSet<Text> splits = new TreeSet<>();
    IntStream.range(97, 122).forEach(i -> splits.add(new Text(new String("" + i))));

    NewTableConfiguration ntc1 = new NewTableConfiguration();
    ntc1.withInitialHostingGoal(TabletHostingGoal.ALWAYS);
    ntc1.withSplits(splits);

    String tableName = this.getUniqueNames(1)[0];
    try (final AccumuloClient client =
        Accumulo.newClient().from(getCluster().getClientProperties()).build()) {

      client.tableOperations().create(tableName, ntc1);
      client.instanceOperations().waitForBalance();

      assertEquals(26, getCountOfHostedTablets(client, tableName));

      Map<String,String> tserverGroups = getTServerGroups();
      assertEquals(2, tserverGroups.size());

      Ample ample = ((ClientContext) client).getAmple();
      String tableId = client.tableOperations().tableIdMap().get(tableName);
      List<TabletMetadata> locations = ample.readTablets().forTable(TableId.of(tableId))
          .fetch(TabletMetadata.ColumnType.LOCATION).build().stream().collect(Collectors.toList());
      assertEquals(26, locations.size());
      Location l1 = locations.get(0).getLocation();
      assertEquals("default", tserverGroups.get(l1.getHostAndPort().toString()));
      locations.forEach(loc -> assertEquals(l1, loc.getLocation()));

      client.tableOperations().setProperty(tableName, "table.custom.assignment.group", "GROUP1");
      // Wait 2x the MAC default Manager TGW interval
      Thread.sleep(10_000);
      client.instanceOperations().waitForBalance();

      assertEquals(26, getCountOfHostedTablets(client, tableName));

      locations = ample.readTablets().forTable(TableId.of(tableId))
          .fetch(TabletMetadata.ColumnType.LOCATION).build().stream().collect(Collectors.toList());
      assertEquals(26, locations.size());
      Location l2 = locations.get(0).getLocation();
      assertEquals("GROUP1", tserverGroups.get(l2.getHostAndPort().toString()));
      locations.forEach(loc -> assertEquals(l2, loc.getLocation()));

      client.tableOperations().delete(tableName);
    }

  }

  private int getCountOfHostedTablets(AccumuloClient client, String tableName) throws Exception {

    ClientTabletCache locator = ClientTabletCache.getInstance((ClientContext) client,
        TableId.of(client.tableOperations().tableIdMap().get(tableName)));
    locator.invalidateCache();
    AtomicInteger locations = new AtomicInteger(0);
    locator.findTablets((ClientContext) client, Collections.singletonList(new Range()), (ct, r) -> {
      if (ct.getTserverLocation().isPresent()) {
        locations.incrementAndGet();
      }
    }, LocationNeed.NOT_REQUIRED);
    return locations.get();
  }
}

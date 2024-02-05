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
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

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
import org.apache.accumulo.core.client.admin.TabletAvailability;
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
import org.apache.accumulo.core.metadata.AccumuloTable;
import org.apache.accumulo.core.metadata.schema.Ample;
import org.apache.accumulo.core.metadata.schema.TabletMetadata;
import org.apache.accumulo.core.metadata.schema.TabletMetadata.Location;
import org.apache.accumulo.core.metadata.schema.TabletMetadata.LocationType;
import org.apache.accumulo.core.metadata.schema.TabletsMetadata;
import org.apache.accumulo.harness.MiniClusterConfigurationCallback;
import org.apache.accumulo.harness.SharedMiniClusterBase;
import org.apache.accumulo.minicluster.ServerType;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloClusterImpl;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloConfigImpl;
import org.apache.accumulo.test.util.Wait;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.net.HostAndPort;

public class TabletResourceGroupBalanceIT extends SharedMiniClusterBase {

  private static final Logger LOG = LoggerFactory.getLogger(TabletResourceGroupBalanceIT.class);

  public static class TRGBalanceITConfig implements MiniClusterConfigurationCallback {

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
    SharedMiniClusterBase.startMiniClusterWithConfig(new TRGBalanceITConfig());
  }

  @AfterAll
  public static void afterAll() throws Exception {
    SharedMiniClusterBase.stopMiniCluster();
  }

  public static Map<String,String> getTServerGroups(MiniAccumuloClusterImpl cluster)
      throws Exception {

    Map<String,String> tservers = new HashMap<>();
    ZooCache zk = cluster.getServerContext().getZooCache();
    String zpath = cluster.getServerContext().getZooKeeperRoot() + Constants.ZTSERVERS;

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

  private static List<TabletMetadata> getLocations(Ample ample, String tableId) {
    try (TabletsMetadata tabletsMetadata = ample.readTablets().forTable(TableId.of(tableId))
        .fetch(TabletMetadata.ColumnType.LOCATION).build()) {
      return tabletsMetadata.stream().collect(Collectors.toList());
    }
  }

  @Test
  public void testBalancerWithResourceGroups() throws Exception {

    SortedSet<Text> splits = new TreeSet<>();
    IntStream.range(97, 122).forEach(i -> splits.add(new Text(new String("" + i))));

    NewTableConfiguration ntc1 = new NewTableConfiguration();
    ntc1.withInitialTabletAvailability(TabletAvailability.HOSTED);
    ntc1.withSplits(splits);

    Map<String,String> properties = new HashMap<>();
    properties.put("table.custom.assignment.group", "GROUP1");

    NewTableConfiguration ntc2 = new NewTableConfiguration();
    ntc2.withInitialTabletAvailability(TabletAvailability.HOSTED);
    ntc2.withSplits(splits);
    ntc2.setProperties(properties);

    String[] names = this.getUniqueNames(2);
    try (AccumuloClient client =
        Accumulo.newClient().from(getCluster().getClientProperties()).build()) {
      client.tableOperations().create(names[0], ntc1);
      client.tableOperations().create(names[1], ntc2);
      client.instanceOperations().waitForBalance();

      Map<String,String> tserverGroups = getTServerGroups(getCluster());
      assertEquals(2, tserverGroups.size());

      Ample ample = ((ClientContext) client).getAmple();

      // Check table names[0]
      String tableId = client.tableOperations().tableIdMap().get(names[0]);
      List<TabletMetadata> locations = getLocations(ample, tableId);

      assertEquals(26, locations.size());
      Location l1 = locations.get(0).getLocation();
      assertEquals("default", tserverGroups.get(l1.getHostAndPort().toString()));
      locations.forEach(loc -> assertEquals(l1, loc.getLocation()));

      // Check table names[1]
      tableId = client.tableOperations().tableIdMap().get(names[1]);
      locations = getLocations(ample, tableId);

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
    ntc1.withInitialTabletAvailability(TabletAvailability.HOSTED);
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
  public void testUserTablePropertyChange() throws Exception {
    SortedSet<Text> splits = new TreeSet<>();
    IntStream.range(97, 122).forEach(i -> splits.add(new Text(new String("" + i))));

    NewTableConfiguration ntc1 = new NewTableConfiguration();
    ntc1.withInitialTabletAvailability(TabletAvailability.HOSTED);
    ntc1.withSplits(splits);

    String tableName = this.getUniqueNames(1)[0];
    try (final AccumuloClient client =
        Accumulo.newClient().from(getCluster().getClientProperties()).build()) {

      client.tableOperations().create(tableName, ntc1);

      // wait for all tablets to be hosted
      Wait.waitFor(() -> 26 != getCountOfHostedTablets(client, tableName));

      client.instanceOperations().waitForBalance();

      try {
        testResourceGroupPropertyChange(client, tableName, 26);
      } finally {
        client.tableOperations().delete(tableName);
      }
    }
  }

  @Test
  public void testMetadataTablePropertyChange() throws Exception {
    try (final AccumuloClient client =
        Accumulo.newClient().from(getCluster().getClientProperties()).build()) {

      client.instanceOperations().waitForBalance();
      testResourceGroupPropertyChange(client, AccumuloTable.METADATA.tableName(),
          getCountOfHostedTablets(client, AccumuloTable.METADATA.tableName()));
    }
  }

  @Test
  public void testRootTablePropertyChange() throws Exception {
    try (final AccumuloClient client =
        Accumulo.newClient().from(getCluster().getClientProperties()).build()) {

      client.instanceOperations().waitForBalance();
      testResourceGroupPropertyChange(client, AccumuloTable.ROOT.tableName(),
          getCountOfHostedTablets(client, AccumuloTable.ROOT.tableName()));
    }
  }

  public void testResourceGroupPropertyChange(AccumuloClient client, String tableName,
      int numExpectedSplits) throws Exception {

    assertEquals(numExpectedSplits, getCountOfHostedTablets(client, tableName));

    Map<String,String> tserverGroups = getTServerGroups(getCluster());
    LOG.info("Tablet Server groups: {}", tserverGroups);

    assertEquals(2, tserverGroups.size());

    Ample ample = ((ClientContext) client).getAmple();
    String tableId = client.tableOperations().tableIdMap().get(tableName);

    // Validate that all of the tables tablets are on the same tserver and that
    // the tserver is in the default resource group
    List<TabletMetadata> locations = getLocations(ample, tableId);
    assertEquals(numExpectedSplits, locations.size());
    Location l1 = locations.get(0).getLocation();
    assertEquals("default", tserverGroups.get(l1.getHostAndPort().toString()));
    locations.forEach(loc -> assertEquals(l1, loc.getLocation()));

    // change the resource group property for the table
    client.tableOperations().setProperty(tableName, "table.custom.assignment.group", "GROUP1");

    locations = getLocations(ample, tableId);
    // wait for GROUP1 to show up in the list of locations as the current location
    while ((locations == null || locations.isEmpty() || locations.size() != numExpectedSplits
        || locations.get(0).getLocation() == null
        || locations.get(0).getLocation().getType() == LocationType.FUTURE)
        || (locations.get(0).getLocation().getType() == LocationType.CURRENT && !tserverGroups
            .get(locations.get(0).getLocation().getHostAndPort().toString()).equals("GROUP1"))) {
      locations = getLocations(ample, tableId);
    }
    Location group1Location = locations.get(0).getLocation();
    assertTrue(tserverGroups.get(group1Location.getHostAndPort().toString()).equals("GROUP1"));

    client.instanceOperations().waitForBalance();

    // validate that all tablets have the same location as the first tablet
    locations = getLocations(ample, tableId);
    while (locations == null || locations.isEmpty() || locations.size() != numExpectedSplits) {
      locations = getLocations(ample, tableId);
    }
    if (locations.stream().map(TabletMetadata::getLocation)
        .allMatch((l) -> group1Location.equals(l))) {
      LOG.info("Group1 location: {} matches all tablet locations: {}", group1Location,
          locations.stream().map(TabletMetadata::getLocation).collect(Collectors.toList()));
    } else {
      LOG.info("Group1 location: {} does not match all tablet locations: {}", group1Location,
          locations.stream().map(TabletMetadata::getLocation).collect(Collectors.toList()));
      fail();
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

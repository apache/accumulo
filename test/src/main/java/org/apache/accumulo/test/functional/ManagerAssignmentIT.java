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

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.IsolatedScanner;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.Locations;
import org.apache.accumulo.core.client.admin.NewTableConfiguration;
import org.apache.accumulo.core.client.admin.TabletAvailability;
import org.apache.accumulo.core.clientImpl.ClientContext;
import org.apache.accumulo.core.clientImpl.ClientTabletCache;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.lock.ServiceLock;
import org.apache.accumulo.core.metadata.AccumuloTable;
import org.apache.accumulo.core.metadata.schema.Ample;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection;
import org.apache.accumulo.core.metadata.schema.TabletMetadata;
import org.apache.accumulo.core.metadata.schema.TabletMetadata.LocationType;
import org.apache.accumulo.core.metadata.schema.TabletOperationId;
import org.apache.accumulo.core.metadata.schema.TabletOperationType;
import org.apache.accumulo.core.metadata.schema.TabletsMetadata;
import org.apache.accumulo.core.rpc.clients.ThriftClientTypes;
import org.apache.accumulo.core.security.TablePermission;
import org.apache.accumulo.core.spi.ondemand.DefaultOnDemandTabletUnloader;
import org.apache.accumulo.core.tabletserver.thrift.TabletStats;
import org.apache.accumulo.core.trace.TraceUtil;
import org.apache.accumulo.harness.SharedMiniClusterBase;
import org.apache.accumulo.minicluster.ServerType;
import org.apache.accumulo.test.util.Wait;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.google.common.collect.Iterables;
import com.google.common.net.HostAndPort;

public class ManagerAssignmentIT extends SharedMiniClusterBase {

  @BeforeAll
  public static void beforeAll() throws Exception {
    SharedMiniClusterBase.startMiniClusterWithConfig((cfg, core) -> {
      cfg.getClusterServerConfiguration().setNumDefaultTabletServers(1);
      cfg.setProperty(Property.TSERV_ASSIGNMENT_MAXCONCURRENT, "10");
      cfg.setProperty(Property.GENERAL_THREADPOOL_SIZE, "10");
      cfg.setProperty(Property.MANAGER_TABLET_GROUP_WATCHER_INTERVAL, "5s");
      cfg.setProperty(Property.TSERV_ONDEMAND_UNLOADER_INTERVAL, "10s");
      cfg.setProperty(DefaultOnDemandTabletUnloader.INACTIVITY_THRESHOLD, "15");
    });
  }

  @BeforeEach
  public void before() throws Exception {
    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
      Wait.waitFor(() -> countTabletsWithLocation(client, AccumuloTable.ROOT.tableId()) > 0);
      Wait.waitFor(() -> countTabletsWithLocation(client, AccumuloTable.METADATA.tableId()) > 0);
    }
  }

  @Test
  public void test() throws Exception {
    try (AccumuloClient c = Accumulo.newClient().from(getClientProps()).build()) {

      // Confirm that the root and metadata tables are hosted
      Locations rootLocations = c.tableOperations().locate(AccumuloTable.ROOT.tableName(),
          Collections.singletonList(new Range()));
      rootLocations.groupByTablet().keySet()
          .forEach(tid -> assertNotNull(rootLocations.getTabletLocation(tid)));

      Locations metadataLocations = c.tableOperations().locate(AccumuloTable.METADATA.tableName(),
          Collections.singletonList(new Range()));
      metadataLocations.groupByTablet().keySet()
          .forEach(tid -> assertNotNull(metadataLocations.getTabletLocation(tid)));

      String tableName = super.getUniqueNames(1)[0];
      c.tableOperations().create(tableName);

      String tableId = c.tableOperations().tableIdMap().get(tableName);

      // wait for the tablet to exist in the metadata table. The tablet
      // will not be hosted so the current location will be empty.
      Wait.waitFor(() -> getTabletMetadata(c, tableId, null) != null, 10000, 250);
      TabletMetadata newTablet = getTabletMetadata(c, tableId, null);
      assertNotNull(newTablet.getExtent());
      assertFalse(newTablet.hasCurrent());
      assertNull(newTablet.getLast());
      assertNull(newTablet.getLocation());
      assertEquals(TabletAvailability.ONDEMAND, newTablet.getTabletAvailability());

      // calling the batch writer will cause the tablet to be hosted
      try (BatchWriter bw = c.createBatchWriter(tableName)) {
        Mutation m = new Mutation("a");
        m.put("b", "c", "d");
        bw.addMutation(m);
      }
      // give it a last location
      c.tableOperations().flush(tableName, null, null, true);

      TabletMetadata flushed = getTabletMetadata(c, tableId, null);
      assertTrue(flushed.hasCurrent());
      assertNotNull(flushed.getLocation());
      assertEquals(flushed.getLocation().getHostPort(), flushed.getLast().getHostPort());
      assertFalse(flushed.getLocation().getType().equals(LocationType.FUTURE));
      assertEquals(TabletAvailability.ONDEMAND, flushed.getTabletAvailability());

      // take the tablet offline
      c.tableOperations().offline(tableName, true);
      TabletMetadata offline = getTabletMetadata(c, tableId, null);
      assertFalse(offline.hasCurrent());
      assertNull(offline.getLocation());
      assertEquals(flushed.getLocation().getHostPort(), offline.getLast().getHostPort());
      assertEquals(TabletAvailability.ONDEMAND, offline.getTabletAvailability());

      // put it back online
      c.tableOperations().online(tableName, true);
      TabletMetadata online = getTabletMetadata(c, tableId, null);
      assertTrue(online.hasCurrent());
      assertNotNull(online.getLocation());
      assertEquals(online.getLocation().getHostPort(), online.getLast().getHostPort());
      assertEquals(TabletAvailability.ONDEMAND, online.getTabletAvailability());

      // set the tablet availability to HOSTED
      c.tableOperations().setTabletAvailability(tableName, new Range(), TabletAvailability.HOSTED);

      Predicate<TabletMetadata> hostedOrCurrentNotNull =
          t -> (t.getTabletAvailability() == TabletAvailability.HOSTED && t.hasCurrent());

      Wait.waitFor(() -> hostedOrCurrentNotNull.test(getTabletMetadata(c, tableId, null)), 60000,
          250);

      final TabletMetadata always = getTabletMetadata(c, tableId, null);
      assertTrue(hostedOrCurrentNotNull.test(always));
      assertTrue(always.hasCurrent());
      assertEquals(flushed.getLocation().getHostPort(), always.getLast().getHostPort());
      assertEquals(TabletAvailability.HOSTED, always.getTabletAvailability());

      // set the hosting availability to never
      c.tableOperations().setTabletAvailability(tableName, new Range(),
          TabletAvailability.UNHOSTED);
      Predicate<TabletMetadata> unhostedOrCurrentNull =
          t -> (t.getTabletAvailability() == TabletAvailability.UNHOSTED && !t.hasCurrent());
      Wait.waitFor(() -> unhostedOrCurrentNull.test(getTabletMetadata(c, tableId, null)), 60000,
          250);

      final TabletMetadata unhosted = getTabletMetadata(c, tableId, null);
      assertTrue(unhostedOrCurrentNull.test(unhosted));
      assertNull(unhosted.getLocation());
      assertEquals(flushed.getLocation().getHostPort(), unhosted.getLast().getHostPort());
      assertEquals(TabletAvailability.UNHOSTED, unhosted.getTabletAvailability());

      // set the tablet availability to ONDEMAND
      c.tableOperations().setTabletAvailability(tableName, new Range(),
          TabletAvailability.ONDEMAND);
      Predicate<TabletMetadata> ondemandHosted =
          t -> t.getTabletAvailability() == TabletAvailability.ONDEMAND;
      Wait.waitFor(() -> ondemandHosted.test(getTabletMetadata(c, tableId, null)), 60000, 250);
      final TabletMetadata ondemand = getTabletMetadata(c, tableId, null);
      assertTrue(ondemandHosted.test(ondemand));
      assertNull(ondemand.getLocation());
      assertEquals(flushed.getLocation().getHostPort(), ondemand.getLast().getHostPort());
      assertEquals(TabletAvailability.ONDEMAND, ondemand.getTabletAvailability());
    }
  }

  private String prepTableForScanTest(AccumuloClient c, String tableName) throws Exception {

    TreeSet<Text> splits = new TreeSet<>();
    splits.add(new Text("f"));
    splits.add(new Text("m"));
    splits.add(new Text("t"));

    NewTableConfiguration ntc = new NewTableConfiguration();
    ntc.withSplits(splits);
    c.tableOperations().create(tableName, ntc);
    String tableId = c.tableOperations().tableIdMap().get(tableName);

    // The initial set of tablets should be unassigned
    Wait.waitFor(() -> getTabletStats(c, tableId).isEmpty(), 60000, 50);

    assertEquals(0, ClientTabletCache.getInstance((ClientContext) c, TableId.of(tableId))
        .getTabletHostingRequestCount());

    // loading data will force the tablets to be hosted
    loadDataForScan(c, tableName);

    assertTrue(ClientTabletCache.getInstance((ClientContext) c, TableId.of(tableId))
        .getTabletHostingRequestCount() > 0);

    Wait.waitFor(() -> getTabletStats(c, tableId).size() == 4, 60000, 50);

    // offline table to force unassign tablets without having to wait for the tablet unloader
    c.tableOperations().offline(tableName, true);

    c.tableOperations().clearLocatorCache(tableName);

    // online the table again, confirm still no tablets hosted
    c.tableOperations().online(tableName, true);

    Wait.waitFor(() -> getTabletStats(c, tableId).isEmpty(), 60000, 50);
    assertEquals(0, ClientTabletCache.getInstance((ClientContext) c, TableId.of(tableId))
        .getTabletHostingRequestCount());

    return tableId;

  }

  @Test
  public void testScannerAssignsOneOnDemandTablets() throws Exception {
    try (AccumuloClient c = Accumulo.newClient().from(getClientProps()).build()) {
      String tableName = super.getUniqueNames(1)[0];

      String tableId = prepTableForScanTest(c, tableName);

      Range scanRange = new Range("a", "c");
      Scanner s = c.createScanner(tableName);
      s.setRange(scanRange);
      // Should return keys for a, b, c
      assertEquals(3, Iterables.size(s));

      List<TabletStats> stats = getTabletStats(c, tableId);
      // There should be one tablet online
      assertEquals(1, stats.size());
      assertTrue(ClientTabletCache.getInstance((ClientContext) c, TableId.of(tableId))
          .getTabletHostingRequestCount() > 0);

    }
  }

  @Test
  public void testScannerAssignsMultipleOnDemandTablets() throws Exception {
    try (AccumuloClient c = Accumulo.newClient().from(getClientProps()).build()) {
      String tableName = super.getUniqueNames(1)[0];

      String tableId = prepTableForScanTest(c, tableName);

      try (Scanner s = c.createScanner(tableName)) {
        s.setRange(new Range("a", "s"));
        assertEquals(19, Iterables.size(s));
      }

      List<TabletStats> stats = getTabletStats(c, tableId);
      assertEquals(3, stats.size());
      long hostingRequestCount = ClientTabletCache
          .getInstance((ClientContext) c, TableId.of(tableId)).getTabletHostingRequestCount();
      assertTrue(hostingRequestCount > 0);

      // Run another scan, the t tablet should get loaded
      // all others should be loaded.
      try (Scanner s = c.createScanner(tableName)) {
        s.setRange(new Range("a", "t"));
        assertEquals(20, Iterables.size(s));
      }

      stats = getTabletStats(c, tableId);
      assertEquals(3, stats.size());
      // No more tablets should have been brought online
      assertEquals(hostingRequestCount, ClientTabletCache
          .getInstance((ClientContext) c, TableId.of(tableId)).getTabletHostingRequestCount());

    }
  }

  @Test
  public void testBatchScannerAssignsOneOnDemandTablets() throws Exception {
    try (AccumuloClient c = Accumulo.newClient().from(getClientProps()).build()) {
      String tableName = super.getUniqueNames(1)[0];

      String tableId = prepTableForScanTest(c, tableName);

      try (BatchScanner s = c.createBatchScanner(tableName)) {
        s.setRanges(List.of(new Range("a", "c")));
        // Should return keys for a, b, c
        assertEquals(3, Iterables.size(s));
      }

      List<TabletStats> stats = getTabletStats(c, tableId);
      // There should be one tablet online
      assertEquals(1, stats.size());
      assertTrue(ClientTabletCache.getInstance((ClientContext) c, TableId.of(tableId))
          .getTabletHostingRequestCount() > 0);

    }
  }

  @Test
  public void testBatchScannerAssignsMultipleOnDemandTablets() throws Exception {
    try (AccumuloClient c = Accumulo.newClient().from(getClientProps()).build()) {
      String tableName = super.getUniqueNames(1)[0];

      String tableId = prepTableForScanTest(c, tableName);

      try (BatchScanner s = c.createBatchScanner(tableName)) {
        s.setRanges(List.of(new Range("a", "s")));
        assertEquals(19, Iterables.size(s));
      }

      List<TabletStats> stats = getTabletStats(c, tableId);
      assertEquals(3, stats.size());
      long hostingRequestCount = ClientTabletCache
          .getInstance((ClientContext) c, TableId.of(tableId)).getTabletHostingRequestCount();
      assertTrue(hostingRequestCount > 0);

      // Run another scan, all tablets should be loaded
      try (BatchScanner s = c.createBatchScanner(tableName)) {
        s.setRanges(List.of(new Range("a", "t")));
        assertEquals(20, Iterables.size(s));
      }

      stats = getTabletStats(c, tableId);
      assertEquals(3, stats.size());
      // No more tablets should have been brought online
      assertEquals(hostingRequestCount, ClientTabletCache
          .getInstance((ClientContext) c, TableId.of(tableId)).getTabletHostingRequestCount());

    }
  }

  @Test
  public void testBatchWriterAssignsTablets() throws Exception {
    try (AccumuloClient c = Accumulo.newClient().from(getClientProps()).build()) {
      String tableName = super.getUniqueNames(1)[0];

      prepTableForScanTest(c, tableName);
    }
  }

  @Test
  public void testOpidPreventsAssignment() throws Exception {
    try (AccumuloClient c = Accumulo.newClient().from(getClientProps()).build()) {
      String tableName = super.getUniqueNames(1)[0];

      var tableId = TableId.of(prepTableForScanTest(c, tableName));
      assertEquals(0, countTabletsWithLocation(c, tableId));

      assertEquals(Set.of("f", "m", "t"), c.tableOperations().listSplits(tableName).stream()
          .map(Text::toString).collect(Collectors.toSet()));

      c.securityOperations().grantTablePermission(getPrincipal(),
          AccumuloTable.METADATA.tableName(), TablePermission.WRITE);

      // Set the OperationId on one tablet, which will cause that tablet
      // to not be assigned
      try (var writer = c.createBatchWriter(AccumuloTable.METADATA.tableName())) {
        var extent = new KeyExtent(tableId, new Text("m"), new Text("f"));
        var opid = TabletOperationId.from(TabletOperationType.SPLITTING, 42L);
        Mutation m = new Mutation(extent.toMetaRow());
        TabletsSection.ServerColumnFamily.OPID_COLUMN.put(m, new Value(opid.canonical()));
        writer.addMutation(m);
      }

      // Host all tablets.
      c.tableOperations().setTabletAvailability(tableName, new Range(), TabletAvailability.HOSTED);
      Wait.waitFor(() -> countTabletsWithLocation(c, tableId) == 3);
      var ample = ((ClientContext) c).getAmple();
      assertNull(
          ample.readTablet(new KeyExtent(tableId, new Text("m"), new Text("f"))).getLocation());

      // Delete the OperationId column, tablet should be assigned
      try (var writer = c.createBatchWriter(AccumuloTable.METADATA.tableName())) {
        var extent = new KeyExtent(tableId, new Text("m"), new Text("f"));
        Mutation m = new Mutation(extent.toMetaRow());
        TabletsSection.ServerColumnFamily.OPID_COLUMN.putDelete(m);
        writer.addMutation(m);
      }
      Wait.waitFor(() -> countTabletsWithLocation(c, tableId) == 4);

      // Set the OperationId on one tablet, which will cause that tablet
      // to be unhosted
      try (var writer = c.createBatchWriter(AccumuloTable.METADATA.tableName())) {
        var extent = new KeyExtent(tableId, new Text("m"), new Text("f"));
        var opid = TabletOperationId.from(TabletOperationType.SPLITTING, 42L);
        Mutation m = new Mutation(extent.toMetaRow());
        TabletsSection.ServerColumnFamily.OPID_COLUMN.put(m, new Value(opid.canonical()));
        writer.addMutation(m);
      }
      // there are four tablets, three should be assigned as one has a OperationId
      Wait.waitFor(() -> countTabletsWithLocation(c, tableId) == 3);
      assertNull(
          ample.readTablet(new KeyExtent(tableId, new Text("m"), new Text("f"))).getLocation());

      // Delete the OperationId column, tablet should be assigned again
      try (var writer = c.createBatchWriter(AccumuloTable.METADATA.tableName())) {
        var extent = new KeyExtent(tableId, new Text("m"), new Text("f"));
        Mutation m = new Mutation(extent.toMetaRow());
        TabletsSection.ServerColumnFamily.OPID_COLUMN.putDelete(m);
        writer.addMutation(m);
      }

      // after the operation id is deleted the tablet should be assigned
      Wait.waitFor(() -> countTabletsWithLocation(c, tableId) == 4);
    }
  }

  public static void loadDataForScan(AccumuloClient c, String tableName)
      throws MutationsRejectedException, TableNotFoundException {
    final byte[] empty = new byte[0];
    try (BatchWriter bw = c.createBatchWriter(tableName)) {
      IntStream.range(97, 122).forEach((i) -> {
        try {
          Mutation m = new Mutation(String.valueOf((char) i));
          m.put(empty, empty, empty);
          bw.addMutation(m);
        } catch (MutationsRejectedException e) {
          fail("Error inserting data", e);
        }
      });
    }
  }

  public static Ample getAmple(AccumuloClient c) {
    return ((ClientContext) c).getAmple();
  }

  public static long countTabletsWithLocation(AccumuloClient c, TableId tableId) {
    try (TabletsMetadata tabletsMetadata = getAmple(c).readTablets().forTable(tableId)
        .fetch(TabletMetadata.ColumnType.LOCATION).build()) {
      return tabletsMetadata.stream().filter(tabletMetadata -> tabletMetadata.getLocation() != null)
          .count();
    }
  }

  public static List<TabletStats> getTabletStats(AccumuloClient c, String tableId)
      throws AccumuloException, AccumuloSecurityException {
    return ThriftClientTypes.TABLET_SERVER.execute((ClientContext) c, client -> client
        .getTabletStats(TraceUtil.traceInfo(), ((ClientContext) c).rpcCreds(), tableId));
  }

  @Test
  public void testShutdownOnlyTServerWithUserTable() throws Exception {

    String tableName = getUniqueNames(1)[0];

    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {

      Wait.waitFor(() -> client.instanceOperations().getTabletServers().size() == 1,
          SECONDS.toMillis(60), SECONDS.toMillis(2));

      client.tableOperations().create(tableName);
      TableId tid = TableId.of(client.tableOperations().tableIdMap().get(tableName));

      // wait for everything to be hosted and balanced
      client.instanceOperations().waitForBalance();

      try (var writer = client.createBatchWriter(tableName)) {
        for (int i = 0; i < 1000000; i++) {
          Mutation m = new Mutation(String.format("%08d", i));
          m.put("", "", "");
          writer.addMutation(m);
        }
      }
      client.tableOperations().flush(tableName, null, null, true);

      final CountDownLatch latch = new CountDownLatch(10);

      Runnable task = () -> {
        while (true) {
          try (var scanner = new IsolatedScanner(client.createScanner(tableName))) {
            // TODO maybe do not close scanner? The following limit was placed on the stream to
            // avoid reading all the data possibly leaving a scan session active on the tserver
            AtomicInteger count = new AtomicInteger(0);
            scanner.forEach(e -> {
              // let the test thread know that this thread has read some data
              if (count.incrementAndGet() == 1_000) {
                latch.countDown();
              }
            });
          } catch (Exception e) {
            e.printStackTrace();
            break;
          }
        }
      };

      ExecutorService service = Executors.newFixedThreadPool(10);
      for (int i = 0; i < 10; i++) {
        service.execute(task);
      }

      // Wait until all threads are reading some data
      latch.await();

      // getClusterControl().stopAllServers(ServerType.TABLET_SERVER)
      // could potentially send a kill -9 to the process. Shut the tablet
      // servers down in a more graceful way.
      final Map<String,Map<KeyExtent,List<Range>>> binnedRanges = new HashMap<>();
      ClientTabletCache.getInstance((ClientContext) client, tid).binRanges((ClientContext) client,
          Collections.singletonList(TabletsSection.getRange()), binnedRanges);
      binnedRanges.keySet().forEach((location) -> {
        HostAndPort address = HostAndPort.fromString(location);
        String addressWithSession = address.toString();
        var zLockPath = ServiceLock.path(getCluster().getServerContext().getZooKeeperRoot()
            + Constants.ZTSERVERS + "/" + address);
        long sessionId =
            ServiceLock.getSessionId(getCluster().getServerContext().getZooCache(), zLockPath);
        if (sessionId != 0) {
          addressWithSession = address + "[" + Long.toHexString(sessionId) + "]";
        }

        final String finalAddress = addressWithSession;
        System.out.println("Attempting to shutdown TabletServer at: " + address);
        try {
          ThriftClientTypes.MANAGER.executeVoid((ClientContext) client,
              c -> c.shutdownTabletServer(TraceUtil.traceInfo(),
                  getCluster().getServerContext().rpcCreds(), finalAddress, false));
        } catch (AccumuloException | AccumuloSecurityException e) {
          fail("Error shutting down TabletServer", e);
        }

      });

      Wait.waitFor(() -> client.instanceOperations().getTabletServers().size() == 0);

      // restart the tablet server for the other tests. Need to call stopAllServers
      // to clear out the process list because we shutdown the TabletServer outside
      // of MAC control.
      getCluster().getClusterControl().stopAllServers(ServerType.TABLET_SERVER);
      getCluster().getClusterControl().start(ServerType.TABLET_SERVER);
      Wait.waitFor(() -> client.instanceOperations().getTabletServers().size() == 1, 60_000);
    }
  }

  @Test
  public void testShutdownOnlyTServerWithoutUserTable() throws Exception {

    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {

      Wait.waitFor(() -> client.instanceOperations().getTabletServers().size() == 1,
          SECONDS.toMillis(60), SECONDS.toMillis(2));

      client.instanceOperations().waitForBalance();

      // getClusterControl().stopAllServers(ServerType.TABLET_SERVER)
      // could potentially send a kill -9 to the process. Shut the tablet
      // servers down in a more graceful way.

      Locations locs = client.tableOperations().locate(AccumuloTable.ROOT.tableName(),
          Collections.singletonList(TabletsSection.getRange()));
      locs.groupByTablet().keySet().stream().map(locs::getTabletLocation).forEach(location -> {
        HostAndPort address = HostAndPort.fromString(location);
        String addressWithSession = address.toString();
        var zLockPath = ServiceLock.path(getCluster().getServerContext().getZooKeeperRoot()
            + Constants.ZTSERVERS + "/" + address);
        long sessionId =
            ServiceLock.getSessionId(getCluster().getServerContext().getZooCache(), zLockPath);
        if (sessionId != 0) {
          addressWithSession = address + "[" + Long.toHexString(sessionId) + "]";
        }

        final String finalAddress = addressWithSession;
        System.out.println("Attempting to shutdown TabletServer at: " + address);
        try {
          ThriftClientTypes.MANAGER.executeVoid((ClientContext) client,
              c -> c.shutdownTabletServer(TraceUtil.traceInfo(),
                  getCluster().getServerContext().rpcCreds(), finalAddress, false));
        } catch (AccumuloException | AccumuloSecurityException e) {
          fail("Error shutting down TabletServer", e);
        }

      });
      Wait.waitFor(() -> client.instanceOperations().getTabletServers().size() == 0);

      // restart the tablet server for the other tests. Need to call stopAllServers
      // to clear out the process list because we shutdown the TabletServer outside
      // of MAC control.
      getCluster().getClusterControl().stopAllServers(ServerType.TABLET_SERVER);
      getCluster().getClusterControl().start(ServerType.TABLET_SERVER);
      Wait.waitFor(() -> client.instanceOperations().getTabletServers().size() == 1, 60_000);
    }
  }

  public static TabletMetadata getTabletMetadata(AccumuloClient c, String tableId, Text endRow) {
    var ctx = (ClientContext) c;
    try (var tablets = ctx.getAmple().readTablets().forTable(TableId.of(tableId))
        .overlapping(endRow, null).build()) {
      var iter = tablets.iterator();
      if (iter.hasNext()) {
        return iter.next();
      }
    }

    return null;
  }
}

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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.TreeSet;
import java.util.function.Predicate;
import java.util.stream.IntStream;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.Locations;
import org.apache.accumulo.core.client.admin.NewTableConfiguration;
import org.apache.accumulo.core.client.admin.TabletHostingGoal;
import org.apache.accumulo.core.clientImpl.ClientContext;
import org.apache.accumulo.core.clientImpl.ClientTabletCache;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.metadata.RootTable;
import org.apache.accumulo.core.metadata.TabletLocationState;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection;
import org.apache.accumulo.core.rpc.clients.ThriftClientTypes;
import org.apache.accumulo.core.spi.ondemand.DefaultOnDemandTabletUnloader;
import org.apache.accumulo.core.tabletserver.thrift.TabletStats;
import org.apache.accumulo.core.trace.TraceUtil;
import org.apache.accumulo.harness.SharedMiniClusterBase;
import org.apache.accumulo.server.manager.state.MetaDataTableScanner;
import org.apache.accumulo.test.util.Wait;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.google.common.collect.Iterables;

public class ManagerAssignmentIT extends SharedMiniClusterBase {

  @Override
  protected Duration defaultTimeout() {
    return Duration.ofMinutes(2);
  }

  @BeforeAll
  public static void beforeAll() throws Exception {
    SharedMiniClusterBase.startMiniClusterWithConfig((cfg, core) -> {
      cfg.setNumTservers(1);
      cfg.setProperty(Property.TSERV_ASSIGNMENT_MAXCONCURRENT, "10");
      cfg.setProperty(Property.GENERAL_THREADPOOL_SIZE, "10");
      cfg.setProperty(Property.MANAGER_TABLET_GROUP_WATCHER_INTERVAL, "5s");
      cfg.setProperty(Property.TSERV_ONDEMAND_UNLOADER_INTERVAL, "10s");
      cfg.setProperty(DefaultOnDemandTabletUnloader.INACTIVITY_THRESHOLD, "15");
    });
  }

  @Test
  public void test() throws Exception {
    try (AccumuloClient c = Accumulo.newClient().from(getClientProps()).build()) {

      // Confirm that the root and metadata tables are hosted
      Locations rootLocations =
          c.tableOperations().locate(RootTable.NAME, Collections.singletonList(new Range()));
      rootLocations.groupByTablet().keySet()
          .forEach(tid -> assertNotNull(rootLocations.getTabletLocation(tid)));

      Locations metadataLocations =
          c.tableOperations().locate(MetadataTable.NAME, Collections.singletonList(new Range()));
      metadataLocations.groupByTablet().keySet()
          .forEach(tid -> assertNotNull(metadataLocations.getTabletLocation(tid)));

      String tableName = super.getUniqueNames(1)[0];
      c.tableOperations().create(tableName);

      String tableId = c.tableOperations().tableIdMap().get(tableName);

      // wait for the tablet to exist in the metadata table. The tablet
      // will not be hosted so the current location will be empty.
      Wait.waitFor(() -> getTabletLocationState(c, tableId, null).extent != null, 10000, 250);
      TabletLocationState newTablet = getTabletLocationState(c, tableId, null);
      assertNotNull(newTablet.extent);
      assertNull(newTablet.current);
      assertNull(newTablet.last);
      assertNull(newTablet.future);
      assertEquals(TabletHostingGoal.ONDEMAND, newTablet.goal);

      // calling the batch writer will cause the tablet to be hosted
      try (BatchWriter bw = c.createBatchWriter(tableName)) {
        Mutation m = new Mutation("a");
        m.put("b", "c", "d");
        bw.addMutation(m);
      }
      // give it a last location
      c.tableOperations().flush(tableName, null, null, true);

      TabletLocationState flushed = getTabletLocationState(c, tableId, null);
      assertNotNull(flushed.current);
      assertEquals(flushed.getCurrentServer(), flushed.getLastServer());
      assertNull(newTablet.future);
      assertEquals(TabletHostingGoal.ONDEMAND, flushed.goal);

      // take the tablet offline
      c.tableOperations().offline(tableName, true);
      TabletLocationState offline = getTabletLocationState(c, tableId, null);
      assertNull(offline.future);
      assertNull(offline.current);
      assertEquals(flushed.getCurrentServer(), offline.getLastServer());
      assertEquals(TabletHostingGoal.ONDEMAND, offline.goal);

      // put it back online
      c.tableOperations().online(tableName, true);
      TabletLocationState online = getTabletLocationState(c, tableId, null);
      assertNull(online.future);
      assertNotNull(online.current);
      assertEquals(online.getCurrentServer(), online.getLastServer());
      assertEquals(TabletHostingGoal.ONDEMAND, online.goal);

      // set the hosting goal to always
      c.tableOperations().setTabletHostingGoal(tableName, new Range(), TabletHostingGoal.ALWAYS);

      Predicate<TabletLocationState> alwaysHostedOrCurrentNotNull =
          t -> (t.goal == TabletHostingGoal.ALWAYS) || (t.current != null);

      assertTrue(Wait.waitFor(
          () -> alwaysHostedOrCurrentNotNull.test(getTabletLocationState(c, tableId, null)), 60000,
          250));

      final TabletLocationState always = getTabletLocationState(c, tableId, null);
      assertTrue(alwaysHostedOrCurrentNotNull.test(always));
      assertNull(always.future);
      assertEquals(flushed.getCurrentServer(), always.getLastServer());
      assertEquals(TabletHostingGoal.ALWAYS, always.goal);

      // set the hosting goal to never
      c.tableOperations().setTabletHostingGoal(tableName, new Range(), TabletHostingGoal.NEVER);
      Predicate<TabletLocationState> neverHostedOrCurrentNull =
          t -> (t.goal == TabletHostingGoal.NEVER) || (t.current == null);
      assertTrue(Wait.waitFor(
          () -> neverHostedOrCurrentNull.test(getTabletLocationState(c, tableId, null)), 60000,
          250));

      final TabletLocationState never = getTabletLocationState(c, tableId, null);
      assertTrue(neverHostedOrCurrentNull.test(never));
      assertNull(never.future);
      assertEquals(flushed.getCurrentServer(), never.getLastServer());
      assertEquals(TabletHostingGoal.NEVER, never.goal);

      // set the hosting goal to ondemand
      c.tableOperations().setTabletHostingGoal(tableName, new Range(), TabletHostingGoal.ONDEMAND);
      Predicate<TabletLocationState> ondemandHosted = t -> t.goal == TabletHostingGoal.ONDEMAND;
      assertTrue(Wait.waitFor(() -> ondemandHosted.test(getTabletLocationState(c, tableId, null)),
          60000, 250));
      final TabletLocationState ondemand = getTabletLocationState(c, tableId, null);
      assertTrue(ondemandHosted.test(ondemand));
      assertNull(ondemand.future);
      assertEquals(flushed.getCurrentServer(), ondemand.getLastServer());
      assertEquals(TabletHostingGoal.ONDEMAND, ondemand.goal);

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
    assertTrue(Wait.waitFor(() -> getTabletStats(c, tableId).isEmpty(), 60000, 50));

    assertEquals(0, ClientTabletCache.getInstance((ClientContext) c, TableId.of(tableId))
        .getTabletHostingRequestCount());

    // loading data will force the tablets to be hosted
    loadDataForScan(c, tableName);

    assertTrue(ClientTabletCache.getInstance((ClientContext) c, TableId.of(tableId))
        .getTabletHostingRequestCount() > 0);

    assertTrue(Wait.waitFor(() -> getTabletStats(c, tableId).size() == 4, 60000, 50));

    // offline table to force unassign tablets without having to wait for the tablet unloader
    c.tableOperations().offline(tableName, true);

    c.tableOperations().clearLocatorCache(tableName);

    // online the table again, confirm still no tablets hosted
    c.tableOperations().online(tableName, true);

    assertTrue(Wait.waitFor(() -> getTabletStats(c, tableId).isEmpty(), 60000, 50));
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

      // Run another scan, all tablets should be loaded
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

  public static List<TabletStats> getTabletStats(AccumuloClient c, String tableId)
      throws AccumuloException, AccumuloSecurityException {
    return ThriftClientTypes.TABLET_SERVER.execute((ClientContext) c, client -> client
        .getTabletStats(TraceUtil.traceInfo(), ((ClientContext) c).rpcCreds(), tableId));
  }

  public static TabletLocationState getTabletLocationState(AccumuloClient c, String tableId,
      Text endRow) {
    try (MetaDataTableScanner s = new MetaDataTableScanner((ClientContext) c,
        new Range(TabletsSection.encodeRow(TableId.of(tableId), endRow)), MetadataTable.NAME)) {
      return s.next();
    }
  }
}

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
import org.apache.accumulo.core.clientImpl.ClientContext;
import org.apache.accumulo.core.clientImpl.TabletLocator;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.metadata.TabletLocationState;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection;
import org.apache.accumulo.core.rpc.clients.ThriftClientTypes;
import org.apache.accumulo.core.tabletserver.thrift.TabletStats;
import org.apache.accumulo.core.trace.TraceUtil;
import org.apache.accumulo.core.util.UtilWaitThread;
import org.apache.accumulo.harness.SharedMiniClusterBase;
import org.apache.accumulo.server.manager.state.MetaDataTableScanner;
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
      cfg.setProperty(Property.MANAGER_TABLET_GROUP_WATCHER_INTERVAL, "5");
    });
  }

  @Test
  public void test() throws Exception {
    try (AccumuloClient c = Accumulo.newClient().from(getClientProps()).build()) {
      String tableName = super.getUniqueNames(1)[0];
      c.tableOperations().create(tableName);
      String tableId = c.tableOperations().tableIdMap().get(tableName);
      // wait for the table to be online
      TabletLocationState newTablet;
      do {
        UtilWaitThread.sleep(250);
        newTablet = getTabletLocationState(c, tableId);
      } while (newTablet.current == null);
      assertNull(newTablet.last);
      assertNull(newTablet.future);

      // put something in it
      try (BatchWriter bw = c.createBatchWriter(tableName)) {
        Mutation m = new Mutation("a");
        m.put("b", "c", "d");
        bw.addMutation(m);
      }
      // give it a last location
      c.tableOperations().flush(tableName, null, null, true);

      TabletLocationState flushed = getTabletLocationState(c, tableId);
      assertEquals(newTablet.current, flushed.current);
      assertEquals(flushed.current, flushed.last);
      assertNull(newTablet.future);

      // take the tablet offline
      c.tableOperations().offline(tableName, true);
      TabletLocationState offline = getTabletLocationState(c, tableId);
      assertNull(offline.future);
      assertNull(offline.current);
      assertEquals(flushed.current, offline.last);

      // put it back online
      c.tableOperations().online(tableName, true);
      TabletLocationState online = getTabletLocationState(c, tableId);
      assertNull(online.future);
      assertNotNull(online.current);
      assertEquals(online.current, online.last);

      // take the tablet offline
      c.tableOperations().offline(tableName, true);
      offline = getTabletLocationState(c, tableId);
      assertNull(offline.future);
      assertNull(offline.current);
      assertEquals(flushed.current, offline.last);

      // set the table to ondemand
      c.tableOperations().onDemand(tableName, true);
      TabletLocationState ondemand = getTabletLocationState(c, tableId);
      assertNull(ondemand.future);
      assertNull(ondemand.current);
      assertEquals(flushed.current, ondemand.last);

      // put it back online
      c.tableOperations().online(tableName, true);
      online = getTabletLocationState(c, tableId);
      assertNull(online.future);
      assertNotNull(online.current);
      assertEquals(online.current, online.last);

    }
  }

  @Test
  public void testScannerAssignsOneOnDemandTablets() throws Exception {
    try (AccumuloClient c = Accumulo.newClient().from(getClientProps()).build()) {
      String tableName = super.getUniqueNames(1)[0];
      c.tableOperations().create(tableName);
      String tableId = c.tableOperations().tableIdMap().get(tableName);
      loadDataForScan(c, tableName);

      TreeSet<Text> splits = new TreeSet<>();
      splits.add(new Text("f"));
      splits.add(new Text("m"));
      splits.add(new Text("t"));
      c.tableOperations().addSplits(tableName, splits);
      // Need to offline the table first so that the tablets
      // are unloaded.
      c.tableOperations().offline(tableName, true);
      c.tableOperations().onDemand(tableName, true);
      assertTrue(c.tableOperations().isOnDemand(tableName));

      List<TabletStats> stats = getTabletStats(c, tableId);
      // There should be no tablets online
      assertEquals(0, stats.size());
      assertEquals(0, TabletLocator.getLocator((ClientContext) c, TableId.of(tableId))
          .onDemandTabletsOnlined());

      c.tableOperations().clearLocatorCache(tableName);

      Range scanRange = new Range("a", "c");
      Scanner s = c.createScanner(tableName);
      s.setRange(scanRange);
      // Should return keys for a, b, c
      assertEquals(3, Iterables.size(s));

      stats = getTabletStats(c, tableId);
      // There should be one tablet online
      assertEquals(1, stats.size());
      assertEquals(1, TabletLocator.getLocator((ClientContext) c, TableId.of(tableId))
          .onDemandTabletsOnlined());

    }
  }

  @Test
  public void testScannerAssignsMultipleOnDemandTablets() throws Exception {
    try (AccumuloClient c = Accumulo.newClient().from(getClientProps()).build()) {
      String tableName = super.getUniqueNames(1)[0];
      c.tableOperations().create(tableName);
      String tableId = c.tableOperations().tableIdMap().get(tableName);
      loadDataForScan(c, tableName);

      TreeSet<Text> splits = new TreeSet<>();
      splits.add(new Text("f"));
      splits.add(new Text("m"));
      splits.add(new Text("t"));
      c.tableOperations().addSplits(tableName, splits);
      // Need to offline the table first so that the tablets
      // are unloaded.
      c.tableOperations().offline(tableName, true);
      c.tableOperations().onDemand(tableName, true);
      assertTrue(c.tableOperations().isOnDemand(tableName));

      List<TabletStats> stats = getTabletStats(c, tableId);
      // There should be no tablets online
      assertEquals(0, stats.size());
      assertEquals(0, TabletLocator.getLocator((ClientContext) c, TableId.of(tableId))
          .onDemandTabletsOnlined());

      c.tableOperations().clearLocatorCache(tableName);

      Range scanRange = new Range("a", "s");
      Scanner s = c.createScanner(tableName);
      s.setRange(scanRange);
      assertEquals(19, Iterables.size(s));

      stats = getTabletStats(c, tableId);
      assertEquals(3, stats.size());
      assertEquals(3, TabletLocator.getLocator((ClientContext) c, TableId.of(tableId))
          .onDemandTabletsOnlined());

      // Run another scan, all tablets should be loaded
      scanRange = new Range("a", "t");
      s = c.createScanner(tableName);
      s.setRange(scanRange);
      assertEquals(20, Iterables.size(s));

      stats = getTabletStats(c, tableId);
      assertEquals(3, stats.size());
      // No more tablets should have been brought online
      assertEquals(3, TabletLocator.getLocator((ClientContext) c, TableId.of(tableId))
          .onDemandTabletsOnlined());

    }
  }

  @Test
  public void testBatchScannerAssignsOneOnDemandTablets() throws Exception {
    try (AccumuloClient c = Accumulo.newClient().from(getClientProps()).build()) {
      String tableName = super.getUniqueNames(1)[0];
      c.tableOperations().create(tableName);
      String tableId = c.tableOperations().tableIdMap().get(tableName);
      loadDataForScan(c, tableName);

      TreeSet<Text> splits = new TreeSet<>();
      splits.add(new Text("f"));
      splits.add(new Text("m"));
      splits.add(new Text("t"));
      c.tableOperations().addSplits(tableName, splits);
      // Need to offline the table first so that the tablets
      // are unloaded.
      c.tableOperations().offline(tableName, true);
      c.tableOperations().onDemand(tableName, true);
      assertTrue(c.tableOperations().isOnDemand(tableName));

      List<TabletStats> stats = getTabletStats(c, tableId);
      // There should be no tablets online
      assertEquals(0, stats.size());
      assertEquals(0, TabletLocator.getLocator((ClientContext) c, TableId.of(tableId))
          .onDemandTabletsOnlined());

      c.tableOperations().clearLocatorCache(tableName);

      Range scanRange = new Range("a", "c");
      BatchScanner s = c.createBatchScanner(tableName);
      s.setRanges(Collections.singleton(scanRange));
      // Should return keys for a, b, c
      assertEquals(3, Iterables.size(s));

      stats = getTabletStats(c, tableId);
      // There should be one tablet online
      assertEquals(1, stats.size());
      assertEquals(1, TabletLocator.getLocator((ClientContext) c, TableId.of(tableId))
          .onDemandTabletsOnlined());

    }
  }

  @Test
  public void testBatchScannerAssignsMultipleOnDemandTablets() throws Exception {
    try (AccumuloClient c = Accumulo.newClient().from(getClientProps()).build()) {
      String tableName = super.getUniqueNames(1)[0];
      c.tableOperations().create(tableName);
      String tableId = c.tableOperations().tableIdMap().get(tableName);
      loadDataForScan(c, tableName);

      TreeSet<Text> splits = new TreeSet<>();
      splits.add(new Text("f"));
      splits.add(new Text("m"));
      splits.add(new Text("t"));
      c.tableOperations().addSplits(tableName, splits);
      // Need to offline the table first so that the tablets
      // are unloaded.
      c.tableOperations().offline(tableName, true);
      c.tableOperations().onDemand(tableName, true);
      assertTrue(c.tableOperations().isOnDemand(tableName));

      List<TabletStats> stats = getTabletStats(c, tableId);
      // There should be no tablets online
      assertEquals(0, stats.size());
      assertEquals(0, TabletLocator.getLocator((ClientContext) c, TableId.of(tableId))
          .onDemandTabletsOnlined());

      c.tableOperations().clearLocatorCache(tableName);

      Range scanRange = new Range("a", "s");
      BatchScanner s = c.createBatchScanner(tableName);
      s.setRanges(Collections.singleton(scanRange));
      assertEquals(19, Iterables.size(s));

      stats = getTabletStats(c, tableId);
      assertEquals(3, stats.size());
      assertEquals(3, TabletLocator.getLocator((ClientContext) c, TableId.of(tableId))
          .onDemandTabletsOnlined());

      // Run another scan, all tablets should be loaded
      scanRange = new Range("a", "t");
      s = c.createBatchScanner(tableName);
      s.setRanges(Collections.singleton(scanRange));
      assertEquals(20, Iterables.size(s));

      stats = getTabletStats(c, tableId);
      assertEquals(3, stats.size());
      // No more tablets should have been brought online
      assertEquals(3, TabletLocator.getLocator((ClientContext) c, TableId.of(tableId))
          .onDemandTabletsOnlined());

    }
  }

  @Test
  public void testBatchWriterAssignsTablets() throws Exception {
    try (AccumuloClient c = Accumulo.newClient().from(getClientProps()).build()) {
      String tableName = super.getUniqueNames(1)[0];
      c.tableOperations().create(tableName);
      String tableId = c.tableOperations().tableIdMap().get(tableName);
      loadDataForScan(c, tableName);

      TreeSet<Text> splits = new TreeSet<>();
      splits.add(new Text("f"));
      splits.add(new Text("m"));
      splits.add(new Text("t"));
      c.tableOperations().addSplits(tableName, splits);
      // Need to offline the table first so that the tablets
      // are unloaded.
      c.tableOperations().offline(tableName, true);
      c.tableOperations().onDemand(tableName, true);
      assertTrue(c.tableOperations().isOnDemand(tableName));

      List<TabletStats> stats = getTabletStats(c, tableId);
      // There should be no tablets online
      assertEquals(0, stats.size());
      assertEquals(0, TabletLocator.getLocator((ClientContext) c, TableId.of(tableId))
          .onDemandTabletsOnlined());

      c.tableOperations().clearLocatorCache(tableName);
      loadDataForScan(c, tableName);

      stats = getTabletStats(c, tableId);
      assertEquals(4, stats.size());
      // No more tablets should have been brought online
      assertEquals(4, TabletLocator.getLocator((ClientContext) c, TableId.of(tableId))
          .onDemandTabletsOnlined());

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

  private TabletLocationState getTabletLocationState(AccumuloClient c, String tableId) {
    try (MetaDataTableScanner s = new MetaDataTableScanner((ClientContext) c,
        new Range(TabletsSection.encodeRow(TableId.of(tableId), null)), MetadataTable.NAME)) {
      return s.next();
    }
  }
}

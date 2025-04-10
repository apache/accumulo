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
package org.apache.accumulo.test;

import static org.apache.accumulo.core.client.ScannerBase.ConsistencyLevel.IMMEDIATE;
import static org.apache.accumulo.core.metrics.Metric.SCAN_ZOMBIE_THREADS;
import static org.apache.accumulo.minicluster.ServerType.SCAN_SERVER;
import static org.apache.accumulo.minicluster.ServerType.TABLET_SERVER;
import static org.apache.accumulo.test.functional.ScannerIT.countActiveScans;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.stream.Collectors;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.ScannerBase.ConsistencyLevel;
import org.apache.accumulo.core.client.admin.ActiveScan;
import org.apache.accumulo.core.client.admin.NewTableConfiguration;
import org.apache.accumulo.core.client.admin.ScanType;
import org.apache.accumulo.core.client.admin.servers.ServerId;
import org.apache.accumulo.core.clientImpl.ClientContext;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.iterators.WrappingIterator;
import org.apache.accumulo.core.metadata.schema.TabletMetadata;
import org.apache.accumulo.core.spi.metrics.LoggingMeterRegistryFactory;
import org.apache.accumulo.minicluster.ServerType;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloConfigImpl;
import org.apache.accumulo.test.functional.ConfigurableMacBase;
import org.apache.accumulo.test.metrics.TestStatsDRegistryFactory;
import org.apache.accumulo.test.metrics.TestStatsDSink;
import org.apache.accumulo.test.util.Wait;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

public class ZombieScanIT extends ConfigurableMacBase {

  private static TestStatsDSink sink;

  @BeforeAll
  public static void before() throws Exception {
    sink = new TestStatsDSink();
  }

  @AfterAll
  public static void after() throws Exception {
    sink.close();
  }

  @Override
  protected void configure(MiniAccumuloConfigImpl cfg, Configuration hadoopCoreSite) {
    super.configure(cfg, hadoopCoreSite);

    // Make sessions time out much more quickly. This will cause a session to be classified as a
    // zombie scan much sooner.
    cfg.setProperty(Property.TSERV_SESSION_MAXIDLE, "6s");
    cfg.setProperty(Property.GENERAL_MICROMETER_ENABLED, "true");
    cfg.setProperty(Property.GENERAL_MICROMETER_JVM_METRICS_ENABLED, "true");
    cfg.setProperty("general.custom.metrics.opts.logging.step", "1s");
    String clazzList = LoggingMeterRegistryFactory.class.getName() + ","
        + TestStatsDRegistryFactory.class.getName();
    cfg.setProperty(Property.GENERAL_MICROMETER_FACTORY, clazzList);
    Map<String,String> sysProps = Map.of(TestStatsDRegistryFactory.SERVER_HOST, "127.0.0.1",
        TestStatsDRegistryFactory.SERVER_PORT, Integer.toString(sink.getPort()));
    cfg.setSystemProperties(sysProps);
    cfg.getClusterServerConfiguration().setNumDefaultTabletServers(1);
  }

  /**
   * An iterator that should get stuck forever when used
   */
  public static class ZombieIterator extends WrappingIterator {
    @Override
    public boolean hasTop() {
      // must call super.hasTop() before blocking as that will run accumulo code to setup iterator
      boolean ht = super.hasTop();
      Semaphore semaphore = new Semaphore(10);
      semaphore.acquireUninterruptibly(5);
      // this should block forever
      semaphore.acquireUninterruptibly(6);
      return ht;
    }
  }

  /**
   * An iterator that should get stuck but can be interrupted
   */
  public static class StuckIterator extends WrappingIterator {
    @Override
    public boolean hasTop() {
      try {
        // must call super.hasTop() before blocking as that will run accumulo code to setup iterator
        boolean ht = super.hasTop();
        Semaphore semaphore = new Semaphore(10);
        semaphore.acquire(5);
        // this should block forever
        semaphore.acquire(6);
        return ht;
      } catch (InterruptedException ie) {
        throw new IllegalStateException(ie);
      }
    }
  }

  /**
   * This test ensure that scans threads that run forever do not prevent tablets from unloading.
   */
  @Test
  public void testZombieScan() throws Exception {

    String table = getUniqueNames(1)[0];

    try (AccumuloClient c = Accumulo.newClient().from(getClientProperties()).build()) {

      var splits = new TreeSet<Text>();
      splits.add(new Text("3"));
      splits.add(new Text("5"));
      splits.add(new Text("7"));
      var ntc = new NewTableConfiguration().withSplits(splits);
      c.tableOperations().create(table, ntc);

      try (var writer = c.createBatchWriter(table)) {
        for (var row : List.of("2", "4", "6", "8")) {
          Mutation m = new Mutation(row);
          m.put("f", "q", "v");
          writer.addMutation(m);
        }
      }

      // Flush the data otherwise when the tablet attempts to close with an active scan reading from
      // the in memory map it will wait for 15 seconds for the scan
      c.tableOperations().flush(table, null, null, true);

      var executor = Executors.newCachedThreadPool();

      // start two zombie scans that should never return using a normal scanner
      List<Future<String>> futures = new ArrayList<>();
      for (var row : List.of("2", "4")) {
        var future = executor.submit(() -> {
          try (var scanner = c.createScanner(table)) {
            IteratorSetting iter = new IteratorSetting(100, "Z", ZombieIterator.class);
            scanner.addScanIterator(iter);
            scanner.setRange(new Range(row));
            return scanner.stream().findFirst().map(e -> e.getKey().getRowData().toString())
                .orElse("none");
          }
        });
        futures.add(future);
      }

      // start two zombie scans that should never return using a batch scanner
      for (var row : List.of("6", "8")) {
        var future = executor.submit(() -> {
          try (var scanner = c.createBatchScanner(table)) {
            IteratorSetting iter = new IteratorSetting(100, "Z", ZombieIterator.class);
            scanner.addScanIterator(iter);
            scanner.setRanges(List.of(new Range(row)));
            return scanner.stream().findFirst().map(e -> e.getKey().getRowData().toString())
                .orElse("none");
          }
        });
        futures.add(future);
      }

      // should eventually see the four zombie scans running against four tablets
      Wait.waitFor(() -> countDistinctTabletsScans(table, c) == 4);

      assertEquals(1, c.instanceOperations().getServers(ServerId.Type.TABLET_SERVER).size());

      // Start 3 new tablet servers, this should cause the table to balance and the tablets with
      // zombie scans to unload. The Zombie scans should not prevent the table from unloading. The
      // scan threads will still be running on the old tablet servers.
      getCluster().getConfig().getClusterServerConfiguration().setNumDefaultTabletServers(4);
      getCluster().getClusterControl().start(ServerType.TABLET_SERVER, Map.of(), 4);

      // Wait for all tablets servers
      Wait.waitFor(
          () -> c.instanceOperations().getServers(ServerId.Type.TABLET_SERVER).size() == 4);

      // The table should eventually balance across the 4 tablet servers
      Wait.waitFor(() -> countLocations(table, c) == 4);

      // The zombie scans should still be running
      assertTrue(futures.stream().noneMatch(Future::isDone));

      // Should be able to scan all the tablets at the new locations.
      try (var scanner = c.createScanner(table)) {
        var rows = scanner.stream().map(e -> e.getKey().getRowData().toString())
            .collect(Collectors.toSet());
        assertEquals(Set.of("2", "4", "6", "8"), rows);
      }

      try (var scanner = c.createBatchScanner(table)) {
        scanner.setRanges(List.of(new Range()));
        var rows = scanner.stream().map(e -> e.getKey().getRowData().toString())
            .collect(Collectors.toSet());
        assertEquals(Set.of("2", "4", "6", "8"), rows);
      }

      // The zombie scans should migrate with the tablets, taking up more scan threads in the
      // system.
      Set<ServerId> tabletServersWithZombieScans = new HashSet<>();
      for (ServerId tserver : c.instanceOperations().getServers(ServerId.Type.TABLET_SERVER)) {
        if (c.instanceOperations().getActiveScans(List.of(tserver)).stream()
            .flatMap(activeScan -> activeScan.getSsiList().stream())
            .anyMatch(scanIters -> scanIters.contains(ZombieIterator.class.getName()))) {
          tabletServersWithZombieScans.add(tserver);
        }
      }
      assertEquals(4, tabletServersWithZombieScans.size());

      // This check may be outside the scope of this test but works nicely for this check and is
      // simple enough to include
      assertValidScanIds(c);

      executor.shutdownNow();
    }

  }

  /**
   * Create some zombie scans and ensure metrics for them show up.
   */
  @ParameterizedTest
  @EnumSource
  public void testMetrics(ConsistencyLevel consistency) throws Exception {

    Wait.waitFor(() -> {
      var zsmc = getZombieScansMetric();
      return zsmc == -1 || zsmc == 0;
    });

    String table = getUniqueNames(1)[0];

    final ServerType serverType = consistency == IMMEDIATE ? TABLET_SERVER : SCAN_SERVER;

    try (AccumuloClient c = Accumulo.newClient().from(getClientProperties()).build()) {

      if (serverType == SCAN_SERVER) {
        // Scans will fall back to tablet servers when no scan servers are present. So wait for scan
        // servers to show up in zookeeper. Can remove this in 3.1.
        Wait.waitFor(() -> !c.instanceOperations().getServers(ServerId.Type.SCAN_SERVER).isEmpty());
      }

      c.tableOperations().create(table);

      var executor = Executors.newCachedThreadPool();

      // start four stuck scans that should never return data
      List<Future<String>> futures = new ArrayList<>();
      for (var row : List.of("2", "4")) {
        // start a scan with an iterator that gets stuck and can not be interrupted
        futures.add(startStuckScan(c, table, executor, row, false, consistency));
        // start a scan with an iterator that gets stuck and can be interrupted
        futures.add(startStuckScan(c, table, executor, row, true, consistency));
      }

      // start four stuck scans, using a batch scanner, that should never return data
      for (var row : List.of("6", "8")) {
        // start a scan with an iterator that gets stuck and can not be interrupted
        futures.add(startStuckBatchScan(c, table, executor, row, false, consistency));
        // start a scan with an iterator that gets stuck and can be interrupted
        futures.add(startStuckBatchScan(c, table, executor, row, true, consistency));
      }

      // should eventually see the eight stuck scans running
      Wait.waitFor(() -> countActiveScans(c, serverType, table) == 8);

      // Cancel the scan threads. This will cause the sessions on the server side to timeout and
      // become inactive. The stuck threads on the server side related to the timed out sessions
      // will be interrupted.
      Wait.waitFor(() -> {
        futures.forEach(future -> future.cancel(true));
        return futures.stream().allMatch(Future::isDone);
      });

      // Four of the eight running scans should respond to thread interrupts and exit
      Wait.waitFor(() -> countActiveScans(c, serverType, table) == 4);

      Wait.waitFor(() -> getZombieScansMetric() == 4);

      assertEquals(4, countActiveScans(c, serverType, table));

      // start four more stuck scans with two that will ignore interrupts
      futures.clear();
      futures.add(startStuckScan(c, table, executor, "0", false, consistency));
      futures.add(startStuckScan(c, table, executor, "0", true, consistency));
      futures.add(startStuckBatchScan(c, table, executor, "99", false, consistency));
      futures.add(startStuckBatchScan(c, table, executor, "0", true, consistency));

      Wait.waitFor(() -> countActiveScans(c, serverType, table) == 8);

      // Cancel the client side scan threads. Should cause the server side threads to be
      // interrupted.
      Wait.waitFor(() -> {
        futures.forEach(future -> future.cancel(true));
        return futures.stream().allMatch(Future::isDone);
      });

      // Two of the stuck threads should respond to interrupts on the server side and exit.
      Wait.waitFor(() -> countActiveScans(c, serverType, table) == 6);

      Wait.waitFor(() -> getZombieScansMetric() == 6);

      assertEquals(6, countActiveScans(c, serverType, table));

      executor.shutdownNow();
    }
  }

  private static long countLocations(String table, AccumuloClient client) throws Exception {
    var ctx = (ClientContext) client;
    var tableId = ctx.getTableId(table);
    return ctx.getAmple().readTablets().forTable(tableId).build().stream()
        .map(TabletMetadata::getLocation).filter(Objects::nonNull).distinct().count();
  }

  private static long countDistinctTabletsScans(String table, AccumuloClient client)
      throws Exception {
    Set<ServerId> tservers = client.instanceOperations().getServers(ServerId.Type.TABLET_SERVER);
    return client.instanceOperations().getActiveScans(tservers).stream()
        .filter(activeScan -> activeScan.getTable().equals(table)).map(ActiveScan::getTablet)
        .distinct().count();
  }

  private Future<String> startStuckScan(AccumuloClient c, String table, ExecutorService executor,
      String row, boolean canInterrupt, ConsistencyLevel consistency) {
    return executor.submit(() -> {
      try (var scanner = c.createScanner(table)) {
        String className;
        if (canInterrupt) {
          className = StuckIterator.class.getName();
        } else {
          className = ZombieIterator.class.getName();
        }
        IteratorSetting iter = new IteratorSetting(100, "Z", className);
        scanner.setConsistencyLevel(consistency);
        scanner.addScanIterator(iter);
        scanner.setRange(new Range(row));
        return scanner.stream().findFirst().map(e -> e.getKey().getRowData().toString())
            .orElse("none");
      }
    });
  }

  private Future<String> startStuckBatchScan(AccumuloClient c, String table,
      ExecutorService executor, String row, boolean canInterrupt, ConsistencyLevel consistency) {
    return executor.submit(() -> {
      try (var scanner = c.createBatchScanner(table)) {
        String className;
        if (canInterrupt) {
          className = StuckIterator.class.getName();
        } else {
          className = ZombieIterator.class.getName();
        }

        IteratorSetting iter = new IteratorSetting(100, "Z", className);
        scanner.addScanIterator(iter);
        scanner.setRanges(List.of(new Range(row)));
        scanner.setConsistencyLevel(consistency);
        return scanner.stream().findFirst().map(e -> e.getKey().getRowData().toString())
            .orElse("none");
      }
    });
  }

  private int getZombieScansMetric() {
    return sink.getLines().stream().map(TestStatsDSink::parseStatsDMetric)
        .filter(metric -> metric.getName().equals(SCAN_ZOMBIE_THREADS.getName()))
        .mapToInt(metric -> Integer.parseInt(metric.getValue())).max().orElse(-1);
  }

  /**
   * Ensure that the scan session ids are valid (should not expect 0 or -1). 0 would mean the id was
   * never set (previously existing bug). -1 previously indicated that the scan session was no
   * longer tracking the id (occurred for scans when cleanup was attempted but deferred for later)
   */
  private void assertValidScanIds(AccumuloClient c)
      throws AccumuloException, AccumuloSecurityException {
    Set<Long> scanIds = new HashSet<>();
    Set<ScanType> scanTypes = new HashSet<>();
    var tservers = c.instanceOperations().getServers(ServerId.Type.TABLET_SERVER);
    c.instanceOperations().getActiveScans(tservers).forEach(activeScan -> {
      scanIds.add(activeScan.getScanid());
      scanTypes.add(activeScan.getType());
    });
    assertNotEquals(0, scanIds.size());
    scanIds.forEach(id -> assertTrue(id != 0L && id != -1L));
    // ensure coverage of both batch and single scans
    assertEquals(scanTypes, Set.of(ScanType.SINGLE, ScanType.BATCH));
  }
}

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
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.stream.IntStream;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.ScannerBase;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.NewTableConfiguration;
import org.apache.accumulo.core.clientImpl.ClientContext;
import org.apache.accumulo.core.clientImpl.ClientTabletCache;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.metadata.schema.TabletMetadata;
import org.apache.accumulo.core.metrics.MetricsProducer;
import org.apache.accumulo.core.spi.scan.ConfigurableScanServerSelector;
import org.apache.accumulo.core.tabletserver.thrift.TabletStats;
import org.apache.accumulo.harness.SharedMiniClusterBase;
import org.apache.accumulo.minicluster.ServerType;
import org.apache.accumulo.test.metrics.TestStatsDRegistryFactory;
import org.apache.accumulo.test.metrics.TestStatsDSink;
import org.apache.accumulo.test.util.Wait;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class OnDemandTabletUnloadingIT extends SharedMiniClusterBase {

  private static final int managerTabletGroupWatcherInterval = 5;
  private static final int inactiveOnDemandTabletUnloaderInterval = 10;
  private static TestStatsDSink sink;
  private static Thread metricConsumer;
  private static Long ONDEMAND_ONLINE_COUNT = 0L;

  @BeforeAll
  public static void beforeAll() throws Exception {

    sink = new TestStatsDSink();
    metricConsumer = new Thread(() -> {
      while (!Thread.currentThread().isInterrupted()) {
        List<String> statsDMetrics = sink.getLines();
        for (String line : statsDMetrics) {
          if (Thread.currentThread().isInterrupted()) {
            break;
          }
          if (line.startsWith("accumulo")) {
            TestStatsDSink.Metric metric = TestStatsDSink.parseStatsDMetric(line);
            if (MetricsProducer.METRICS_TSERVER_TABLETS_ONLINE_ONDEMAND.equals(metric.getName())) {
              Long val = Long.parseLong(metric.getValue());
              System.out.println(val);
              ONDEMAND_ONLINE_COUNT = val;
            }
          }
        }
      }
    });
    metricConsumer.start();

    SharedMiniClusterBase.startMiniClusterWithConfig((cfg, core) -> {
      cfg.getClusterServerConfiguration().setNumDefaultScanServers(1);
      cfg.getClusterServerConfiguration().setNumDefaultTabletServers(1);
      cfg.setProperty(Property.MANAGER_TABLET_GROUP_WATCHER_INTERVAL,
          Integer.toString(managerTabletGroupWatcherInterval));
      cfg.setProperty(Property.TSERV_ONDEMAND_UNLOADER_INTERVAL,
          Integer.toString(inactiveOnDemandTabletUnloaderInterval));
      cfg.setProperty("table.custom.ondemand.unloader.inactivity.threshold.seconds", "15");

      // Tell the server processes to use a StatsDMeterRegistry that will be configured
      // to push all metrics to the sink we started.

      cfg.setProperty(Property.GENERAL_MICROMETER_ENABLED, "true");
      cfg.setProperty(Property.GENERAL_MICROMETER_FACTORY,
          TestStatsDRegistryFactory.class.getName());
      Map<String,String> sysProps = Map.of(TestStatsDRegistryFactory.SERVER_HOST, "127.0.0.1",
          TestStatsDRegistryFactory.SERVER_PORT, Integer.toString(sink.getPort()));
      cfg.setSystemProperties(sysProps);

    });
  }

  @AfterAll
  public static void after() throws Exception {
    sink.close();
    metricConsumer.interrupt();
    metricConsumer.join();
  }

  @BeforeEach
  public void before() throws Exception {
    // wait for tablets from previous test to be unloaded
    Wait.waitFor(() -> ONDEMAND_ONLINE_COUNT == 0);
  }

  @Test
  public void testTabletUnloader() throws Exception {
    try (AccumuloClient c = Accumulo.newClient().from(getClientProps()).build()) {

      String tableName = super.getUniqueNames(1)[0];

      TreeSet<Text> splits = new TreeSet<>();
      splits.add(new Text("f"));
      splits.add(new Text("m"));
      splits.add(new Text("t"));

      NewTableConfiguration ntc = new NewTableConfiguration();
      ntc.withSplits(splits);
      c.tableOperations().create(tableName, ntc);
      String tableId = c.tableOperations().tableIdMap().get(tableName);

      // There should be no tablets online
      List<TabletStats> stats = ManagerAssignmentIT.getTabletStats(c, tableId);
      assertEquals(0, stats.size());
      assertEquals(0, ClientTabletCache.getInstance((ClientContext) c, TableId.of(tableId))
          .getTabletHostingRequestCount());
      assertEquals(0, ONDEMAND_ONLINE_COUNT);

      // loading data will cause tablets to be hosted
      ManagerAssignmentIT.loadDataForScan(c, tableName);
      verifyDataForScan(c, tableName);

      // There should be four tablets online
      stats = ManagerAssignmentIT.getTabletStats(c, tableId);
      assertEquals(4, stats.size());
      assertTrue(ClientTabletCache.getInstance((ClientContext) c, TableId.of(tableId))
          .getTabletHostingRequestCount() > 0);
      Wait.waitFor(() -> ONDEMAND_ONLINE_COUNT == 4);

      // Waiting for tablets to be unloaded due to inactivity
      Wait.waitFor(() -> ONDEMAND_ONLINE_COUNT == 0);
      Wait.waitFor(() -> ManagerAssignmentIT.getTabletStats(c, tableId).size() == 0);
    }
  }

  private static void verifyDataForScan(AccumuloClient c, String tableName)
      throws TableNotFoundException, AccumuloSecurityException, AccumuloException {

    Set<Integer> rows = new HashSet<>();
    IntStream.range(97, 122).forEach(i -> rows.add(i));

    try (Scanner s = c.createScanner(tableName)) {
      s.iterator().forEachRemaining((e) -> rows.remove(e.getKey().getRow().charAt(0)));
    }
    assertEquals(0, rows.size());
  }

  public long countTabletsWithLocation(AccumuloClient client, String tableName) throws Exception {
    var ctx = (ClientContext) client;
    try (var tablets = ctx.getAmple().readTablets().forTable(ctx.getTableId(tableName)).build()) {
      return tablets.stream().map(TabletMetadata::getLocation).filter(Objects::nonNull)
          .filter(loc -> loc.getType() == TabletMetadata.LocationType.CURRENT).count();
    }
  }

  public static final String SCAN_SERVER_SELECTOR_CONFIG =
      "[{'isDefault':true,'maxBusyTimeout':'5m',"
          + "'busyTimeoutMultiplier':8, 'scanTypeActivations':[], 'timeToWaitForScanServers':'120s',"
          + "'attemptPlans':[{'servers':'3', 'busyTimeout':'33ms', 'salt':'one'},"
          + "{'servers':'13', 'busyTimeout':'33ms', 'salt':'two'},"
          + "{'servers':'100%', 'busyTimeout':'33ms'}]}]";

  @Test
  public void testScanHostedAndUnhosted() throws Exception {
    String tableName = super.getUniqueNames(1)[0];

    // create configuration that makes eventual scans wait for scan servers when there are none
    // instead of falling back to the tserver.
    var clientProps = new Properties();
    clientProps.putAll(getClientProps());
    String scanServerSelectorProfiles = "[{'isDefault':true,'maxBusyTimeout':'5m',"
        + "'busyTimeoutMultiplier':8, 'scanTypeActivations':[], 'timeToWaitForScanServers':'120s',"
        + "'attemptPlans':[{'servers':'3', 'busyTimeout':'1s'}]}]";
    clientProps.put("scan.server.selector.impl", ConfigurableScanServerSelector.class.getName());
    clientProps.put("scan.server.selector.opts.profiles",
        scanServerSelectorProfiles.replace("'", "\""));

    try (AccumuloClient c = Accumulo.newClient().from(clientProps).build()) {

      SortedSet<Text> splits = new TreeSet<>(
          List.of(new Text("005"), new Text("013"), new Text("027"), new Text("075")));
      c.tableOperations().create(tableName, new NewTableConfiguration().withSplits(splits));
      try (var writer = c.createBatchWriter(tableName)) {
        IntStream.range(0, 100).mapToObj(i -> String.format("%03d", i)).forEach(row -> {
          Mutation m = new Mutation(row);
          m.put("", "", "");
          try {
            writer.addMutation(m);
          } catch (MutationsRejectedException e) {
            throw new RuntimeException(e);
          }
        });
      }

      c.tableOperations().flush(tableName, null, null, true);

      // wait for all tablets to be unhosted
      Wait.waitFor(() -> countTabletsWithLocation(c, tableName) == 0);

      // scan a subset of the table causing some tablets to be hosted
      try (var scanner = c.createScanner(tableName)) {
        scanner.setRange(new Range("050", null));
        assertEquals(50, scanner.stream().count());
      }

      // the above scan should only cause two tablets to be hosted so check this
      assertTrue(countTabletsWithLocation(c, tableName) <= 2);

      getCluster().getClusterControl().start(ServerType.SCAN_SERVER, "localhost");

      // the cache will probably have locations for tablets, want to ensure cache can handle a mix
      // of tablets with and without locations
      c.tableOperations().clearLocatorCache(tableName);

      // Scan should only use scan servers and should scan tablets with and without locations.
      try (var scanner = c.createScanner(tableName)) {
        scanner.setConsistencyLevel(ScannerBase.ConsistencyLevel.EVENTUAL);
        assertEquals(100, scanner.stream().count());
      }

      try (var scanner = c.createBatchScanner(tableName)) {
        scanner.setConsistencyLevel(ScannerBase.ConsistencyLevel.EVENTUAL);
        scanner.setRanges(List.of(new Range()));
        assertEquals(100, scanner.stream().count());
      }

      // ensure tablets without a location were not brought online by the eventual scan
      assertTrue(countTabletsWithLocation(c, tableName) <= 2);

      // esnure an immediate scans works when the cache contains tablets w/ and w/o locations
      try (var scanner = c.createScanner(tableName)) {
        scanner.setConsistencyLevel(ScannerBase.ConsistencyLevel.IMMEDIATE);
        assertEquals(100, scanner.stream().count());
      }

      // delete table to unhost any tablets so they do not show up in counts for the next test
      c.tableOperations().delete(tableName);
    }
  }
}

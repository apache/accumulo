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

import static org.apache.accumulo.harness.AccumuloITBase.MINI_CLUSTER_ONLY;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.ScannerBase.ConsistencyLevel;
import org.apache.accumulo.core.client.TableOfflineException;
import org.apache.accumulo.core.client.TimedOutException;
import org.apache.accumulo.core.client.admin.NewTableConfiguration;
import org.apache.accumulo.core.client.admin.TabletAvailability;
import org.apache.accumulo.core.conf.ClientProperty;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.fate.zookeeper.ZooReaderWriter;
import org.apache.accumulo.core.metadata.AccumuloTable;
import org.apache.accumulo.core.metadata.schema.Ample;
import org.apache.accumulo.core.metadata.schema.Ample.TabletsMutator;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.CurrentLocationColumnFamily;
import org.apache.accumulo.core.metadata.schema.TabletOperationId;
import org.apache.accumulo.core.metadata.schema.TabletOperationType;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.util.UtilWaitThread;
import org.apache.accumulo.harness.MiniClusterConfigurationCallback;
import org.apache.accumulo.harness.SharedMiniClusterBase;
import org.apache.accumulo.minicluster.ServerType;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloConfigImpl;
import org.apache.accumulo.server.metadata.ServerAmpleImpl;
import org.apache.accumulo.test.functional.ReadWriteIT;
import org.apache.accumulo.test.functional.SlowIterator;
import org.apache.accumulo.test.util.Wait;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import com.google.common.collect.Iterables;

@Tag(MINI_CLUSTER_ONLY)
public class ScanServerIT extends SharedMiniClusterBase {

  private static class ScanServerITConfiguration implements MiniClusterConfigurationCallback {

    @Override
    public void configureMiniCluster(MiniAccumuloConfigImpl cfg,
        org.apache.hadoop.conf.Configuration coreSite) {
      cfg.getClusterServerConfiguration().setNumDefaultScanServers(1);

      // Timeout scan sessions after being idle for 3 seconds
      cfg.setProperty(Property.TSERV_SESSION_MAXIDLE, "3s");

      // Configure the scan server to only have 1 scan executor thread. This means
      // that the scan server will run scans serially, not concurrently.
      cfg.setProperty(Property.SSERV_SCAN_EXECUTORS_DEFAULT_THREADS, "1");

      cfg.setProperty(Property.MANAGER_TABLET_GROUP_WATCHER_INTERVAL, "5");
      cfg.setProperty(Property.TSERV_ONDEMAND_UNLOADER_INTERVAL, "10");
      cfg.setProperty("table.custom.ondemand.unloader.inactivity.threshold.seconds", "15");
    }
  }

  @BeforeAll
  public static void start() throws Exception {
    ScanServerITConfiguration c = new ScanServerITConfiguration();
    SharedMiniClusterBase.startMiniClusterWithConfig(c);
    SharedMiniClusterBase.getCluster().getClusterControl().start(ServerType.SCAN_SERVER,
        "localhost");

    String zooRoot = getCluster().getServerContext().getZooKeeperRoot();
    ZooReaderWriter zrw = getCluster().getServerContext().getZooReaderWriter();
    String scanServerRoot = zooRoot + Constants.ZSSERVERS;

    while (zrw.getChildren(scanServerRoot).size() == 0) {
      Thread.sleep(500);
    }
  }

  @AfterAll
  public static void stop() throws Exception {
    SharedMiniClusterBase.stopMiniCluster();
  }

  @Test
  public void testScan() throws Exception {

    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
      String tableName = getUniqueNames(1)[0];

      final int ingestedEntryCount = createTableAndIngest(client, tableName, null, 10, 10, "colf");

      try (Scanner scanner = client.createScanner(tableName, Authorizations.EMPTY)) {
        scanner.setRange(new Range());
        scanner.setConsistencyLevel(ConsistencyLevel.EVENTUAL);
        assertEquals(ingestedEntryCount, Iterables.size(scanner),
            "The scan server scanner should have seen all ingested and flushed entries");
        // if scanning against tserver would see the following, but should not on scan server
        final int additionalIngestedEntryCount =
            ingest(client, tableName, 10, 10, 10, "colf", false);
        assertEquals(ingestedEntryCount, Iterables.size(scanner),
            "The scan server scanner should have seen all ingested and flushed entries");
        scanner.setConsistencyLevel(ConsistencyLevel.IMMEDIATE);
        assertEquals(ingestedEntryCount + additionalIngestedEntryCount, Iterables.size(scanner),
            "Scanning against tserver should have resulted in seeing all ingested entries");
      } // when the scanner is closed, all open sessions should be closed
    }
  }

  @Test
  public void testBatchScan() throws Exception {

    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
      String tableName = getUniqueNames(1)[0];

      final int ingestedEntryCount = createTableAndIngest(client, tableName, null, 10, 10, "colf");

      try (BatchScanner scanner = client.createBatchScanner(tableName, Authorizations.EMPTY)) {
        scanner.setRanges(Collections.singletonList(new Range()));
        scanner.setConsistencyLevel(ConsistencyLevel.EVENTUAL);
        assertEquals(ingestedEntryCount, Iterables.size(scanner),
            "The scan server scanner should have seen all ingested and flushed entries");
        final int additionalIngestedEntryCount =
            ingest(client, tableName, 10, 10, 10, "colf", false);
        assertEquals(ingestedEntryCount, Iterables.size(scanner),
            "The scan server scanner should have seen all ingested and flushed entries");
        scanner.setConsistencyLevel(ConsistencyLevel.IMMEDIATE);
        assertEquals(ingestedEntryCount + additionalIngestedEntryCount, Iterables.size(scanner),
            "Scanning against tserver should have resulted in seeing all ingested entries");
      } // when the scanner is closed, all open sessions should be closed
    }
  }

  @Test
  public void testScanOfflineTable() throws Exception {
    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
      String tableName = getUniqueNames(1)[0];

      createTableAndIngest(client, tableName, null, 10, 10, "colf");
      client.tableOperations().offline(tableName, true);

      assertThrows(TableOfflineException.class, () -> {
        try (Scanner scanner = client.createScanner(tableName, Authorizations.EMPTY)) {
          scanner.setRange(new Range());
          scanner.setConsistencyLevel(ConsistencyLevel.EVENTUAL);
          assertEquals(100, Iterables.size(scanner));
        } // when the scanner is closed, all open sessions should be closed
      });
    }
  }

  @Test
  @Timeout(value = 20)
  public void testBatchScannerTimeout() throws Exception {
    // Configure the client to use different scan server selector property values
    Properties props = getClientProps();
    String profiles = "[{'isDefault':true,'maxBusyTimeout':'1s', 'busyTimeoutMultiplier':8, "
        + "'attemptPlans':[{'servers':'3', 'busyTimeout':'100ms'},"
        + "{'servers':'100%', 'busyTimeout':'100ms'}]}]";
    props.put(ClientProperty.SCAN_SERVER_SELECTOR_OPTS_PREFIX.getKey() + "profiles", profiles);

    String tableName = getUniqueNames(1)[0];
    try (AccumuloClient client = Accumulo.newClient().from(props).build()) {
      createTableAndIngest(client, tableName, null, 10, 10, "colf");
      try (BatchScanner bs = client.createBatchScanner(tableName)) {
        bs.setRanges(Collections.singletonList(new Range()));
        bs.setConsistencyLevel(ConsistencyLevel.EVENTUAL);
        // should not timeout
        bs.stream().forEach(entry -> assertNotNull(entry.getKey()));

        bs.setTimeout(5, TimeUnit.SECONDS);
        IteratorSetting iterSetting = new IteratorSetting(100, SlowIterator.class);
        iterSetting.addOption("sleepTime", 2000 + "");
        bs.addScanIterator(iterSetting);

        assertThrows(TimedOutException.class, () -> bs.iterator().next(),
            "batch scanner did not time out");
      }
    }
  }

  @Test
  public void testScanWithTabletAvailabilityMix() throws Exception {
    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
      String tableName = getUniqueNames(1)[0];

      final int ingestedEntryCount = setupTableWithTabletAvailabilityMix(client, tableName);

      try (Scanner scanner = client.createScanner(tableName, Authorizations.EMPTY)) {
        scanner.setRange(new Range());
        scanner.setConsistencyLevel(ConsistencyLevel.EVENTUAL);
        assertEquals(ingestedEntryCount, Iterables.size(scanner),
            "The scan server scanner should have seen all ingested and flushed entries");
        // Throws an exception because of the tablets with the UNHOSTED tablet availability
        scanner.setConsistencyLevel(ConsistencyLevel.IMMEDIATE);
        assertThrows(RuntimeException.class, () -> Iterables.size(scanner));

        // Test that hosted ranges work
        scanner.setRange(new Range(null, "row_0000000003"));
        assertEquals(40, Iterables.size(scanner));

        scanner.setRange(new Range("row_0000000008", null));
        assertEquals(20, Iterables.size(scanner));
      } // when the scanner is closed, all open sessions should be closed
    }
  }

  @Test
  public void testScanTabletsWithOperationIds() throws Exception {
    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
      String tableName = getUniqueNames(1)[0];

      setupTableWithTabletAvailabilityMix(client, tableName);

      // Unload all tablets
      TableId tid = TableId.of(client.tableOperations().tableIdMap().get(tableName));
      client.tableOperations().setTabletAvailability(tableName, new Range((Text) null, (Text) null),
          TabletAvailability.ONDEMAND);

      // Wait for the tablets to be unloaded
      Wait.waitFor(() -> ScanServerIT.getNumHostedTablets(client, tid.canonical()) == 0, 30_000,
          1_000);

      // Set operationIds on all the table's tablets so that they won't be loaded.
      TabletOperationId opid = TabletOperationId.from(TabletOperationType.SPLITTING, 1234L);
      Ample ample = getCluster().getServerContext().getAmple();
      ServerAmpleImpl sai = (ServerAmpleImpl) ample;
      try (TabletsMutator tm = sai.mutateTablets()) {
        ample.readTablets().forTable(tid).build().forEach(meta -> {
          tm.mutateTablet(meta.getExtent()).putOperation(opid).mutate();
        });
      }

      final List<Future<?>> futures = new ArrayList<>();
      final ExecutorService executor = Executors.newFixedThreadPool(4);
      try (Scanner eventualScanner = client.createScanner(tableName, Authorizations.EMPTY);
          Scanner immediateScanner = client.createScanner(tableName, Authorizations.EMPTY);
          BatchScanner eventualBScanner =
              client.createBatchScanner(tableName, Authorizations.EMPTY);
          BatchScanner immediateBScanner =
              client.createBatchScanner(tableName, Authorizations.EMPTY)) {
        eventualScanner.setRange(new Range());
        immediateScanner.setRange(new Range());

        // Confirm that the ScanServer will not complete the scan
        eventualScanner.setConsistencyLevel(ConsistencyLevel.EVENTUAL);
        futures.add(executor.submit(() -> assertTimeoutPreemptively(Duration.ofSeconds(30), () -> {
          Iterables.size(eventualScanner);
        })));

        // Confirm that the TabletServer will not complete the scan
        immediateScanner.setConsistencyLevel(ConsistencyLevel.IMMEDIATE);
        futures.add(executor.submit(() -> assertTimeoutPreemptively(Duration.ofSeconds(30), () -> {
          Iterables.size(immediateScanner);
        })));

        // Test the BatchScanner
        eventualBScanner.setRanges(Collections.singleton(new Range()));
        immediateBScanner.setRanges(Collections.singleton(new Range()));

        // Confirm that the ScanServer will not complete the scan
        eventualBScanner.setConsistencyLevel(ConsistencyLevel.EVENTUAL);
        futures.add(executor.submit(() -> assertTimeoutPreemptively(Duration.ofSeconds(30), () -> {
          Iterables.size(eventualBScanner);
        })));

        // Confirm that the TabletServer will not complete the scan
        immediateBScanner.setConsistencyLevel(ConsistencyLevel.IMMEDIATE);
        futures.add(executor.submit(() -> assertTimeoutPreemptively(Duration.ofSeconds(30), () -> {
          Iterables.size(immediateBScanner);
        })));

        UtilWaitThread.sleep(30_000);

        assertEquals(4, futures.size());
        futures.forEach(f -> {
          var e = assertThrows(ExecutionException.class, () -> f.get());
          assertTrue(e.getCause() instanceof AssertionError);
        });
      } // when the scanner is closed, all open sessions should be closed
      executor.shutdown();
    }
  }

  @Test
  public void testBatchScanWithTabletAvailabilityMix() throws Exception {
    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
      String tableName = getUniqueNames(1)[0];

      final int ingestedEntryCount = setupTableWithTabletAvailabilityMix(client, tableName);

      try (BatchScanner scanner = client.createBatchScanner(tableName, Authorizations.EMPTY)) {
        scanner.setRanges(Collections.singleton(new Range()));
        scanner.setConsistencyLevel(ConsistencyLevel.EVENTUAL);
        assertEquals(ingestedEntryCount, Iterables.size(scanner),
            "The scan server scanner should have seen all ingested and flushed entries");
        // Throws an exception because of the tablets with the UNHOSTED tablet availability
        scanner.setConsistencyLevel(ConsistencyLevel.IMMEDIATE);
        assertThrows(RuntimeException.class, () -> Iterables.size(scanner));

        // Test that hosted ranges work
        Collection<Range> ranges = new ArrayList<>();
        ranges.add(new Range(null, "row_0000000003"));
        ranges.add(new Range("row_0000000008", null));
        scanner.setRanges(ranges);
        assertEquals(60, Iterables.size(scanner));
      } // when the scanner is closed, all open sessions should be closed
    }
  }

  /**
   * Sets up a table with a mix of tablet availabilities. Specific ranges of rows are set to HOSTED,
   * UNHOSTED, and ONDEMAND availabilities. The method waits for the UNHOSTED and ONDEMAND tablets
   * to be unloaded due to inactivity before returning.
   *
   * @param client The AccumuloClient to use for the operation
   * @param tableName The name of the table to be created and set up
   * @return The count of ingested entries
   */
  protected static int setupTableWithTabletAvailabilityMix(AccumuloClient client, String tableName)
      throws Exception {
    SortedSet<Text> splits =
        IntStream.rangeClosed(1, 9).mapToObj(i -> new Text("row_000000000" + i))
            .collect(Collectors.toCollection(TreeSet::new));

    NewTableConfiguration ntc = new NewTableConfiguration();
    ntc.withSplits(splits);
    ntc.withInitialTabletAvailability(TabletAvailability.HOSTED); // speed up ingest
    final int ingestedEntryCount = createTableAndIngest(client, tableName, ntc, 10, 10, "colf");

    String tableId = client.tableOperations().tableIdMap().get(tableName);

    // row 1 -> 3 are HOSTED
    client.tableOperations().setTabletAvailability(tableName,
        new Range(null, true, "row_0000000003", true), TabletAvailability.HOSTED);
    // row 4 -> 7 are UNHOSTED
    client.tableOperations().setTabletAvailability(tableName,
        new Range("row_0000000004", true, "row_0000000007", true), TabletAvailability.UNHOSTED);
    // row 8 and 9 are ondemand
    client.tableOperations().setTabletAvailability(tableName,
        new Range("row_0000000008", true, null, true), TabletAvailability.ONDEMAND);

    // Wait for the UNHOSTED and ONDEMAND tablets to be unloaded due to inactivity
    Wait.waitFor(() -> ScanServerIT.getNumHostedTablets(client, tableId) == 3, 30_000, 1_000);

    return ingestedEntryCount;
  }

  /**
   * Create a table with the given name and the given client. Then, ingest into the table using
   * {@link #ingest(AccumuloClient, String, int, int, int, String, boolean)}
   *
   * @param client used to create the table
   * @param tableName used to create the table
   * @param ntc used to create the table. if null, a new NewTableConfiguration will replace it
   * @param rowCount number of rows to ingest
   * @param colCount number of columns to ingest
   * @param colf column family to use for ingest
   * @return the number of ingested entries
   */
  protected static int createTableAndIngest(AccumuloClient client, String tableName,
      NewTableConfiguration ntc, int rowCount, int colCount, String colf) throws Exception {

    if (Objects.isNull(ntc)) {
      ntc = new NewTableConfiguration();
    }

    client.tableOperations().create(tableName, ntc);

    return ingest(client, tableName, rowCount, colCount, 0, colf, true);
  }

  /**
   * Ingest into the table using the given parameters, then optionally flush the table
   *
   * @param client used to create the table
   * @param tableName used to create the table
   * @param rowCount number of rows to ingest
   * @param colCount number of columns to ingest
   * @param offset the offset to use for ingest
   * @param colf column family to use for ingest
   * @param shouldFlush if true, the entries will be flushed after ingest
   * @return the number of ingested entries
   */
  protected static int ingest(AccumuloClient client, String tableName, int rowCount, int colCount,
      int offset, String colf, boolean shouldFlush) throws Exception {
    ReadWriteIT.ingest(client, colCount, rowCount, 50, offset, colf, tableName);

    final int ingestedEntriesCount = colCount * rowCount;

    if (shouldFlush) {
      client.tableOperations().flush(tableName, null, null, true);
    }

    return ingestedEntriesCount;
  }

  protected static int getNumHostedTablets(AccumuloClient client, String tableId) throws Exception {
    try (Scanner scanner = client.createScanner(AccumuloTable.METADATA.tableName())) {
      scanner.setRange(new Range(tableId, tableId + "<"));
      scanner.fetchColumnFamily(CurrentLocationColumnFamily.NAME);
      return Iterables.size(scanner);
    }
  }
}

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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Objects;
import java.util.Properties;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.ScannerBase.ConsistencyLevel;
import org.apache.accumulo.core.client.TimedOutException;
import org.apache.accumulo.core.client.admin.NewTableConfiguration;
import org.apache.accumulo.core.client.admin.TabletHostingGoal;
import org.apache.accumulo.core.conf.ClientProperty;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.fate.zookeeper.ZooReaderWriter;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.CurrentLocationColumnFamily;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.harness.MiniClusterConfigurationCallback;
import org.apache.accumulo.harness.SharedMiniClusterBase;
import org.apache.accumulo.minicluster.ServerType;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloConfigImpl;
import org.apache.accumulo.test.functional.ReadWriteIT;
import org.apache.accumulo.test.functional.SlowIterator;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
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
      cfg.setNumScanServers(1);

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
  @Disabled("Scanner.setTimeout does not work, issue #2606")
  @Timeout(value = 20)
  public void testScannerTimeout() throws Exception {
    // Configure the client to use different scan server selector property values
    Properties props = getClientProps();
    String profiles = "[{'isDefault':true,'maxBusyTimeout':'1s', 'busyTimeoutMultiplier':8, "
        + "'attemptPlans':[{'servers':'3', 'busyTimeout':'100ms'},"
        + "{'servers':'100%', 'busyTimeout':'100ms'}]}]";
    props.put(ClientProperty.SCAN_SERVER_SELECTOR_OPTS_PREFIX.getKey() + "profiles", profiles);

    String tableName = getUniqueNames(1)[0];
    try (AccumuloClient client = Accumulo.newClient().from(props).build()) {
      createTableAndIngest(client, tableName, null, 10, 10, "colf");
      try (Scanner scanner = client.createScanner(tableName, Authorizations.EMPTY)) {
        IteratorSetting slow = new IteratorSetting(30, "slow", SlowIterator.class);
        SlowIterator.setSleepTime(slow, 30000);
        SlowIterator.setSeekSleepTime(slow, 30000);
        scanner.addScanIterator(slow);
        scanner.setRange(new Range());
        scanner.setConsistencyLevel(ConsistencyLevel.EVENTUAL);
        scanner.setTimeout(10, TimeUnit.SECONDS);
        assertFalse(scanner.stream().findAny().isPresent(),
            "The scanner should not see any entries");
      }
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
  public void testScanWithTabletHostingMix() throws Exception {
    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
      String tableName = getUniqueNames(1)[0];

      SortedSet<Text> splits = new TreeSet<>();
      splits.add(new Text("row_0000000001"));
      splits.add(new Text("row_0000000002"));
      splits.add(new Text("row_0000000003"));
      splits.add(new Text("row_0000000004"));
      splits.add(new Text("row_0000000005"));
      splits.add(new Text("row_0000000006"));
      splits.add(new Text("row_0000000007"));
      splits.add(new Text("row_0000000008"));
      splits.add(new Text("row_0000000009"));
      NewTableConfiguration ntc = new NewTableConfiguration();
      ntc.withSplits(splits);
      ntc.withInitialHostingGoal(TabletHostingGoal.ALWAYS); // speed up ingest
      final int ingestedEntryCount = createTableAndIngest(client, tableName, ntc, 10, 10, "colf");

      String tableId = client.tableOperations().tableIdMap().get(tableName);

      // row 1 -> 3 are always
      client.tableOperations().setTabletHostingGoal(tableName,
          new Range(null, true, "row_0000000003", true), TabletHostingGoal.ALWAYS);
      // row 4 -> 7 are never
      client.tableOperations().setTabletHostingGoal(tableName,
          new Range("row_0000000004", true, "row_0000000007", true), TabletHostingGoal.NEVER);
      // row 8 and 9 are ondemand
      client.tableOperations().setTabletHostingGoal(tableName,
          new Range("row_0000000008", true, null, true), TabletHostingGoal.ONDEMAND);

      // Wait for the NEVER and ONDEMAND tablets to be unloaded
      int hosted = getNumHostedTablets(client, tableId);
      // Waiting for tablets to be unloaded due to inactivity
      while (hosted != 3) {
        Thread.sleep(1000);
        hosted = getNumHostedTablets(client, tableId);
      }

      try (Scanner scanner = client.createScanner(tableName, Authorizations.EMPTY)) {
        scanner.setRange(new Range());
        scanner.setConsistencyLevel(ConsistencyLevel.EVENTUAL);
        assertEquals(ingestedEntryCount, Iterables.size(scanner),
            "The scan server scanner should have seen all ingested and flushed entries");
        // Throws an exception because of the tablets with the NEVER hosting goal
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
  public void testBatchScanWithTabletHostingMix() throws Exception {
    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
      String tableName = getUniqueNames(1)[0];

      SortedSet<Text> splits = new TreeSet<>();
      splits.add(new Text("row_0000000001"));
      splits.add(new Text("row_0000000002"));
      splits.add(new Text("row_0000000003"));
      splits.add(new Text("row_0000000004"));
      splits.add(new Text("row_0000000005"));
      splits.add(new Text("row_0000000006"));
      splits.add(new Text("row_0000000007"));
      splits.add(new Text("row_0000000008"));
      splits.add(new Text("row_0000000009"));
      NewTableConfiguration ntc = new NewTableConfiguration();
      ntc.withSplits(splits);
      ntc.withInitialHostingGoal(TabletHostingGoal.ALWAYS); // speed up ingest
      final int ingestedEntryCount = createTableAndIngest(client, tableName, ntc, 10, 10, "colf");

      String tableId = client.tableOperations().tableIdMap().get(tableName);

      // row 1 -> 3 are always
      client.tableOperations().setTabletHostingGoal(tableName,
          new Range(null, true, "row_0000000003", true), TabletHostingGoal.ALWAYS);
      // row 4 -> 7 are never
      client.tableOperations().setTabletHostingGoal(tableName,
          new Range("row_0000000004", true, "row_0000000007", true), TabletHostingGoal.NEVER);
      // row 8 and 9 are ondemand
      client.tableOperations().setTabletHostingGoal(tableName,
          new Range("row_0000000008", true, null, true), TabletHostingGoal.ONDEMAND);

      // Wait for the NEVER and ONDEMAND tablets to be unloaded
      int hosted = getNumHostedTablets(client, tableId);
      // Waiting for tablets to be unloaded due to inactivity
      while (hosted != 3) {
        Thread.sleep(1000);
        hosted = getNumHostedTablets(client, tableId);
      }

      try (BatchScanner scanner = client.createBatchScanner(tableName, Authorizations.EMPTY)) {
        scanner.setRanges(Collections.singleton(new Range()));
        scanner.setConsistencyLevel(ConsistencyLevel.EVENTUAL);
        assertEquals(ingestedEntryCount, Iterables.size(scanner),
            "The scan server scanner should have seen all ingested and flushed entries");
        // Throws an exception because of the tablets with the NEVER hosting goal
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
    try (Scanner scanner = client.createScanner(MetadataTable.NAME)) {
      scanner.setRange(new Range(tableId, tableId + "<"));
      scanner.fetchColumnFamily(CurrentLocationColumnFamily.NAME);
      return Iterables.size(scanner);
    }
  }
}

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
import static org.apache.accumulo.test.ScanServerIT.createTableAndIngest;
import static org.apache.accumulo.test.ScanServerIT.ingest;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.ScannerBase.ConsistencyLevel;
import org.apache.accumulo.core.client.TableOfflineException;
import org.apache.accumulo.core.client.TimedOutException;
import org.apache.accumulo.core.client.admin.NewTableConfiguration;
import org.apache.accumulo.core.client.admin.TabletHostingGoal;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.spi.scan.ConfigurableScanServerSelector;
import org.apache.accumulo.harness.MiniClusterConfigurationCallback;
import org.apache.accumulo.harness.SharedMiniClusterBase;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloConfigImpl;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import com.google.common.collect.Iterables;

@Tag(MINI_CLUSTER_ONLY)
public class ScanServerIT_NoServers extends SharedMiniClusterBase {

  // This is the same as ScanServerIT, but without any scan servers running.
  // This tests the cases where the client falls back to the Tablet Servers

  private static class ScanServerITConfiguration implements MiniClusterConfigurationCallback {

    @Override
    public void configureMiniCluster(MiniAccumuloConfigImpl cfg,
        org.apache.hadoop.conf.Configuration coreSite) {
      cfg.setNumScanServers(0);

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
            "Scanner did not see ingested and flushed entries");
        final int additionalIngestedEntryCount =
            ingest(client, tableName, 10, 10, 10, "colf", false);
        // since there are no scan servers, and we are reading from tservers, we should see update
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
            "Scanner did not see ingested and flushed entries");
        final int additionalIngestedEntryCount =
            ingest(client, tableName, 10, 10, 10, "colf", false);
        // since there are no scan servers, and we are reading from tservers, we should see update
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
  public void testScanWithNoTserverFallback() throws Exception {

    var clientProps = new Properties();
    clientProps.putAll(getClientProps());
    String scanServerSelectorProfiles = "[{'isDefault':true,'maxBusyTimeout':'5m',"
        + "'busyTimeoutMultiplier':8, 'scanTypeActivations':[], 'enableTabletServerFallback':false,"
        + "'attemptPlans':[{'servers':'3', 'busyTimeout':'1s'}]}]";
    clientProps.put("scan.server.selector.impl", ConfigurableScanServerSelector.class.getName());
    clientProps.put("scan.server.selector.opts.profiles",
        scanServerSelectorProfiles.replace("'", "\""));

    try (AccumuloClient client = Accumulo.newClient().from(clientProps).build()) {
      String tableName = getUniqueNames(1)[0];

      createTableAndIngest(client, tableName, null, 10, 10, "colf");

      assertThrows(TimedOutException.class, () -> {
        try (Scanner scanner = client.createScanner(tableName, Authorizations.EMPTY)) {
          scanner.setRange(new Range());
          scanner.setTimeout(1, TimeUnit.SECONDS);
          scanner.setConsistencyLevel(ConsistencyLevel.EVENTUAL);
          assertEquals(100, Iterables.size(scanner));
        } // when the scanner is closed, all open sessions should be closed
      });
    }
  }

  @Test
  public void testBatchScanWithNoTserverFallback() throws Exception {

    var clientProps = new Properties();
    clientProps.putAll(getClientProps());
    String scanServerSelectorProfiles = "[{'isDefault':true,'maxBusyTimeout':'5m',"
        + "'busyTimeoutMultiplier':8, 'scanTypeActivations':[], 'enableTabletServerFallback':false,"
        + "'attemptPlans':[{'servers':'3', 'busyTimeout':'1s'}]}]";
    clientProps.put("scan.server.selector.impl", ConfigurableScanServerSelector.class.getName());
    clientProps.put("scan.server.selector.opts.profiles",
        scanServerSelectorProfiles.replace("'", "\""));

    try (AccumuloClient client = Accumulo.newClient().from(clientProps).build()) {
      String tableName = getUniqueNames(1)[0];

      createTableAndIngest(client, tableName, null, 10, 10, "colf");

      assertThrows(TimedOutException.class, () -> {
        try (BatchScanner scanner = client.createBatchScanner(tableName, Authorizations.EMPTY)) {
          scanner.setRanges(List.of(new Range()));
          scanner.setTimeout(1, TimeUnit.SECONDS);
          scanner.setConsistencyLevel(ConsistencyLevel.EVENTUAL);
          assertEquals(100, Iterables.size(scanner));
        } // when the scanner is closed, all open sessions should be closed
      });
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
      @SuppressWarnings("unused")
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
      int hosted = ScanServerIT.getNumHostedTablets(client, tableId);
      // Waiting for tablets to be unloaded due to inactivity
      while (hosted != 3) {
        Thread.sleep(1000);
        hosted = ScanServerIT.getNumHostedTablets(client, tableId);
      }

      try (Scanner scanner = client.createScanner(tableName, Authorizations.EMPTY)) {
        scanner.setRange(new Range());
        scanner.setConsistencyLevel(ConsistencyLevel.EVENTUAL);
        // Throws an exception because no scan servers and falls back to tablet server with tablets
        // with the NEVER hosting goal
        assertThrows(RuntimeException.class, () -> Iterables.size(scanner));
        // Throws an exception because of the tablets with the NEVER hosting goal
        scanner.setConsistencyLevel(ConsistencyLevel.IMMEDIATE);
        assertThrows(RuntimeException.class, () -> Iterables.size(scanner));
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
      @SuppressWarnings("unused")
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
      int hosted = ScanServerIT.getNumHostedTablets(client, tableId);
      // Waiting for tablets to be unloaded due to inactivity
      while (hosted != 3) {
        Thread.sleep(1000);
        hosted = ScanServerIT.getNumHostedTablets(client, tableId);
      }

      try (BatchScanner scanner = client.createBatchScanner(tableName, Authorizations.EMPTY)) {
        scanner.setRanges(Collections.singleton(new Range()));
        scanner.setConsistencyLevel(ConsistencyLevel.EVENTUAL);
        // Throws an exception because no scan servers and falls back to tablet server with tablets
        // with the NEVER hosting goal
        assertThrows(RuntimeException.class, () -> Iterables.size(scanner));
        // Throws an exception because of the tablets with the NEVER hosting goal
        scanner.setConsistencyLevel(ConsistencyLevel.IMMEDIATE);
        assertThrows(RuntimeException.class, () -> Iterables.size(scanner));
      } // when the scanner is closed, all open sessions should be closed
    }
  }

}

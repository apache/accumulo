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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Collections;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.ScannerBase.ConsistencyLevel;
import org.apache.accumulo.core.clientImpl.ClientContext;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.fate.zookeeper.ZooReaderWriter;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.harness.MiniClusterConfigurationCallback;
import org.apache.accumulo.harness.SharedMiniClusterBase;
import org.apache.accumulo.minicluster.ServerType;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloConfigImpl;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.google.common.collect.Iterables;

public class OfflineTableExtentCacheIT extends SharedMiniClusterBase {

  private static class OfflineTableExtentCacheITConfiguration
      implements MiniClusterConfigurationCallback {

    @Override
    public void configureMiniCluster(MiniAccumuloConfigImpl cfg,
        org.apache.hadoop.conf.Configuration coreSite) {
      cfg.setNumScanServers(1);

      // Timeout scan sessions after being idle for 3 seconds
      cfg.setProperty(Property.TSERV_SESSION_MAXIDLE, "3s");

      // Configure the scan server to only have 1 scan executor thread. This means
      // that the scan server will run scans serially, not concurrently.
      cfg.setProperty(Property.SSERV_SCAN_EXECUTORS_DEFAULT_THREADS, "1");

      // Configure scan server metadata cache expiration
      cfg.setProperty(Property.SSERV_CACHED_TABLET_METADATA_EXPIRATION, "10s");
    }
  }

  @BeforeAll
  public static void start() throws Exception {
    OfflineTableExtentCacheITConfiguration c = new OfflineTableExtentCacheITConfiguration();
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
  public void testScanWithCacheInvalidatedBySplit() throws Exception {
    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
      String tableName = getUniqueNames(1)[0];

      // Create table with no splits, ingest data, offline table
      final int ingestedEntryCount =
          ScanServerIT.createTableAndIngest(client, tableName, null, 10, 10, "colf", false);
      client.tableOperations().offline(tableName, true);
      assertFalse(client.tableOperations().isOnline(tableName));

      int numInRange = 0;
      // Scan table with single range in the middle of table
      try (Scanner scanner = client.createScanner(tableName, Authorizations.EMPTY)) {
        scanner.setRange(new Range("row_0000000005", "row_0000000006"));
        scanner.setConsistencyLevel(ConsistencyLevel.EVENTUAL);
        numInRange = Iterables.size(scanner);
        assertTrue(numInRange > 0 && numInRange <= ingestedEntryCount,
            "The scan server scanner should have seen some ingested and flushed entries");
      }

      // Bring online, add splits, offline table
      client.tableOperations().online(tableName, true);
      assertTrue(client.tableOperations().isOnline(tableName));
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
      client.tableOperations().addSplits(tableName, splits);
      client.tableOperations().offline(tableName, true);
      assertFalse(client.tableOperations().isOnline(tableName));

      // Wait for SSERVER metadata cache to expire, set at 10s
      Thread.sleep(15000);

      // Re-scan
      try (Scanner scanner = client.createScanner(tableName, Authorizations.EMPTY)) {
        scanner.setRange(new Range("row_0000000005", "row_0000000006"));
        scanner.setConsistencyLevel(ConsistencyLevel.EVENTUAL);
        assertEquals(numInRange, Iterables.size(scanner),
            "The scan server scanner should have seen the same number of entries");
      }

      // Invalidate the range in the OfflineTableExtentCache and re-scan
      String tid = client.tableOperations().tableIdMap().get(tableName);
      ((ClientContext) client).getOfflineKeyExtentCache().invalidate(TableId.of(tid),
          new Range("row_0000000005", "row_0000000006"));
      // Re-scan
      try (Scanner scanner = client.createScanner(tableName, Authorizations.EMPTY)) {
        scanner.setRange(new Range("row_0000000005", "row_0000000006"));
        scanner.setConsistencyLevel(ConsistencyLevel.EVENTUAL);
        assertEquals(numInRange, Iterables.size(scanner),
            "The scan server scanner should have seen the same number of entries");
      }
    }
  }

  @Test
  public void testBatchScanWithCacheInvalidatedBySplit() throws Exception {
    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
      String tableName = getUniqueNames(1)[0];

      // Create table with no splits, ingest data, offline table
      final int ingestedEntryCount =
          ScanServerIT.createTableAndIngest(client, tableName, null, 10, 10, "colf", false);
      client.tableOperations().offline(tableName, true);
      assertFalse(client.tableOperations().isOnline(tableName));

      int numInRange = 0;
      // Scan table with single range in the middle of table
      try (BatchScanner scanner = client.createBatchScanner(tableName, Authorizations.EMPTY)) {
        scanner.setRanges(Collections.singletonList(new Range("row_0000000005", "row_0000000006")));
        scanner.setConsistencyLevel(ConsistencyLevel.EVENTUAL);
        numInRange = Iterables.size(scanner);
        assertTrue(numInRange > 0 && numInRange <= ingestedEntryCount,
            "The scan server scanner should have seen some ingested and flushed entries");
      }

      // Bring online, add splits, offline table
      client.tableOperations().online(tableName, true);
      assertTrue(client.tableOperations().isOnline(tableName));
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
      client.tableOperations().addSplits(tableName, splits);
      client.tableOperations().offline(tableName, true);
      assertFalse(client.tableOperations().isOnline(tableName));

      // Wait for SSERVER metadata cache to expire, set at 10s
      Thread.sleep(15000);

      // Re-scan
      try (BatchScanner scanner = client.createBatchScanner(tableName, Authorizations.EMPTY)) {
        scanner.setRanges(Collections.singletonList(new Range("row_0000000005", "row_0000000006")));
        scanner.setConsistencyLevel(ConsistencyLevel.EVENTUAL);
        assertEquals(numInRange, Iterables.size(scanner),
            "The scan server scanner should have seen the same number of entries");
      }

      // Invalidate the range in the OfflineTableExtentCache and re-scan
      String tid = client.tableOperations().tableIdMap().get(tableName);
      ((ClientContext) client).getOfflineKeyExtentCache().invalidate(TableId.of(tid),
          new Range("row_0000000005", "row_0000000006"));
      // Re-scan
      try (BatchScanner scanner = client.createBatchScanner(tableName, Authorizations.EMPTY)) {
        scanner.setRanges(Collections.singletonList(new Range("row_0000000005", "row_0000000006")));
        scanner.setConsistencyLevel(ConsistencyLevel.EVENTUAL);
        assertEquals(numInRange, Iterables.size(scanner),
            "The scan server scanner should have seen the same number of entries");
      }
    }

  }

}

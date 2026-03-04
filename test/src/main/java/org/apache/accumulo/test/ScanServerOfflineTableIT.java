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

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.stream.Stream;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.ScannerBase;
import org.apache.accumulo.core.client.ScannerBase.ConsistencyLevel;
import org.apache.accumulo.core.clientImpl.ClientContext;
import org.apache.accumulo.core.clientImpl.TabletLocator;
import org.apache.accumulo.core.conf.ClientProperty;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.fate.zookeeper.ZooReaderWriter;
import org.apache.accumulo.harness.MiniClusterConfigurationCallback;
import org.apache.accumulo.harness.SharedMiniClusterBase;
import org.apache.accumulo.minicluster.ServerType;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloConfigImpl;
import org.apache.accumulo.test.ScanServerAllowedTablesIT.ScannerType;
import org.apache.accumulo.test.functional.ReadWriteIT;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;
import org.junit.jupiter.params.provider.ArgumentsSource;
import org.junit.jupiter.params.provider.EnumSource;

import com.google.common.collect.Iterables;

public class ScanServerOfflineTableIT extends SharedMiniClusterBase {

  private static class ScanServerOfflineITConfiguration
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
    }
  }

  @BeforeAll
  public static void start() throws Exception {
    ScanServerOfflineITConfiguration c = new ScanServerOfflineITConfiguration();
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

  @ParameterizedTest
  @EnumSource(value = ScanServerAllowedTablesIT.ScannerType.class)
  public void testSimpleScan(ScannerType stype) throws Exception {
    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
      String tableName = getUniqueNames(1)[0] + stype.name();

      final int ingestedEntryCount =
          ScanServerIT.createTableAndIngest(client, tableName, null, 10, 10, "colf");
      client.tableOperations().offline(tableName, true);

      // This isn't necessary, but will ensure that the TabletLocator is cleared
      // Invalidate the TabletLocator for the offline table
      TabletLocator.getLocator((ClientContext) client,
          TableId.of(client.tableOperations().tableIdMap().get(tableName))).invalidateCache();

      try (
          ScannerBase scanner = ScanServerAllowedTablesIT.createScanner(client, stype, tableName)) {
        scanner.setConsistencyLevel(ConsistencyLevel.EVENTUAL);
        assertEquals(ingestedEntryCount, Iterables.size(scanner),
            "The scan server scanner should have seen all ingested and flushed entries");
      } // when the scanner is closed, all open sessions should be closed
    }
  }

  public static class ArgProvider implements ArgumentsProvider {

    @Override
    public Stream<? extends Arguments> provideArguments(ExtensionContext context) throws Exception {
      List<Arguments> args = new ArrayList<>();
      args.add(Arguments.of(ScannerType.BATCH_SCANNER, 0));
      args.add(Arguments.of(ScannerType.SCANNER, 0));
      args.add(Arguments.of(ScannerType.BATCH_SCANNER, 100));
      args.add(Arguments.of(ScannerType.SCANNER, 100));
      return args.stream();
    }

  }

  @ParameterizedTest
  @ArgumentsSource(ArgProvider.class)
  public void testScan(ScannerType stype, int maxCacheSize) throws Exception {

    final int rows = 1000;

    final Properties p = getClientProps();
    p.put(ClientProperty.OFFLINE_LOCATOR_CACHE_SIZE.getKey(), Integer.toString(maxCacheSize));

    try (AccumuloClient client = Accumulo.newClient().from(p).build()) {
      String tableName = getUniqueNames(1)[0] + stype.name() + "_" + maxCacheSize;

      final int ingestedEntryCount =
          ScanServerIT.createTableAndIngest(client, tableName, null, rows, 10, "colf");

      try (
          ScannerBase scanner = ScanServerAllowedTablesIT.createScanner(client, stype, tableName)) {
        scanner.setConsistencyLevel(ConsistencyLevel.IMMEDIATE);
        assertEquals(ingestedEntryCount, Iterables.size(scanner),
            "The tablet server scanner should have seen all ingested and flushed entries");
      } // when the scanner is closed, all open sessions should be closed
      ReadWriteIT.verify(client, rows, 10, 50, 0, tableName);

      client.tableOperations().offline(tableName, true);

      // This isn't necessary, but will ensure that the TabletLocator is cleared
      // Invalidate the TabletLocator for the offline table
      TabletLocator.getLocator((ClientContext) client,
          TableId.of(client.tableOperations().tableIdMap().get(tableName))).invalidateCache();

      try (
          ScannerBase scanner = ScanServerAllowedTablesIT.createScanner(client, stype, tableName)) {
        scanner.setConsistencyLevel(ConsistencyLevel.EVENTUAL);
        assertEquals(ingestedEntryCount, Iterables.size(scanner),
            "The scan server scanner should have seen all ingested and flushed entries");
      } // when the scanner is closed, all open sessions should be closed
      ReadWriteIT.verifyEventual(client, rows, 10, 50, 0, tableName);

      client.tableOperations().online(tableName, true);

      try (
          ScannerBase scanner = ScanServerAllowedTablesIT.createScanner(client, stype, tableName)) {
        scanner.setConsistencyLevel(ConsistencyLevel.IMMEDIATE);
        assertEquals(ingestedEntryCount, Iterables.size(scanner),
            "The tablet server scanner should have seen all ingested and flushed entries");
      } // when the scanner is closed, all open sessions should be closed
      ReadWriteIT.verify(client, rows, 10, 50, 0, tableName);

      // Add some splits to the table
      SortedSet<Text> splits = new TreeSet<>();
      for (int i = 0; i < rows; i++) {
        splits.add(new Text("row_" + String.format("%010d", i)));
      }
      client.tableOperations().addSplits(tableName, splits);
      client.instanceOperations().waitForBalance();

      try (
          ScannerBase scanner = ScanServerAllowedTablesIT.createScanner(client, stype, tableName)) {
        scanner.setConsistencyLevel(ConsistencyLevel.IMMEDIATE);
        assertEquals(ingestedEntryCount, Iterables.size(scanner),
            "The tablet server scanner should have seen all ingested and flushed entries");
      } // when the scanner is closed, all open sessions should be closed
      ReadWriteIT.verify(client, rows, 10, 50, 0, tableName);

      client.tableOperations().offline(tableName, true);

      // This isn't necessary, but will ensure that the TabletLocator is cleared
      // Invalidate the TabletLocator for the offline table
      TabletLocator.getLocator((ClientContext) client,
          TableId.of(client.tableOperations().tableIdMap().get(tableName))).invalidateCache();

      try (
          ScannerBase scanner = ScanServerAllowedTablesIT.createScanner(client, stype, tableName)) {
        scanner.setConsistencyLevel(ConsistencyLevel.EVENTUAL);
        assertEquals(ingestedEntryCount, Iterables.size(scanner),
            "The scan server scanner should have seen all ingested and flushed entries");
      } // when the scanner is closed, all open sessions should be closed
      ReadWriteIT.verifyEventual(client, rows, 10, 50, 0, tableName);

    }

  }

}

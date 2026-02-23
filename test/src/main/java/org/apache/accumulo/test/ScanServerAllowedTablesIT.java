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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Map;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.ScannerBase.ConsistencyLevel;
import org.apache.accumulo.core.clientImpl.ClientContext;
import org.apache.accumulo.core.conf.ClientProperty;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.fate.zookeeper.ZooReaderWriter;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.spi.scan.ScanServerSelector;
import org.apache.accumulo.harness.MiniClusterConfigurationCallback;
import org.apache.accumulo.harness.SharedMiniClusterBase;
import org.apache.accumulo.minicluster.ServerType;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloConfigImpl;
import org.apache.accumulo.test.util.Wait;
import org.apache.accumulo.tserver.ScanServer;
import org.apache.hadoop.conf.Configuration;
import org.apache.zookeeper.ZooKeeper;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.google.common.collect.Iterables;

public class ScanServerAllowedTablesIT extends SharedMiniClusterBase {

  // @formatter:off
  private static final String clientConfiguration =
     "["+
     " {"+
     "   \"isDefault\": true,"+
     "   \"maxBusyTimeout\": \"5m\","+
     "   \"busyTimeoutMultiplier\": 8,"+
     "   \"scanTypeActivations\": [],"+
     "   \"attemptPlans\": ["+
     "     {"+
     "       \"servers\": \"3\","+
     "       \"busyTimeout\": \"33ms\","+
     "       \"salt\": \"one\""+
     "     },"+
     "     {"+
     "       \"servers\": \"13\","+
     "       \"busyTimeout\": \"33ms\","+
     "       \"salt\": \"two\""+
     "     },"+
     "     {"+
     "       \"servers\": \"100%\","+
     "       \"busyTimeout\": \"33ms\""+
     "     }"+
     "   ]"+
     "  },"+
     " {"+
     "   \"isDefault\": false,"+
     "   \"maxBusyTimeout\": \"5m\","+
     "   \"busyTimeoutMultiplier\": 8,"+
     "   \"group\": \"GROUP1\","+
     "   \"scanTypeActivations\": [\"use_group1\"],"+
     "   \"attemptPlans\": ["+
     "     {"+
     "       \"servers\": \"3\","+
     "       \"busyTimeout\": \"33ms\","+
     "       \"salt\": \"one\""+
     "     },"+
     "     {"+
     "       \"servers\": \"13\","+
     "       \"busyTimeout\": \"33ms\","+
     "       \"salt\": \"two\""+
     "     },"+
     "     {"+
     "       \"servers\": \"100%\","+
     "       \"busyTimeout\": \"33ms\""+
     "     }"+
     "   ]"+
     "  }"+
     "]";
  // @formatter:on

  public static class SSATITConfiguration implements MiniClusterConfigurationCallback {

    @Override
    public void configureMiniCluster(MiniAccumuloConfigImpl cfg, Configuration coreSite) {

      cfg.setNumScanServers(1);

      // allow the ScanServer in the DEFAULT group to only scan tables in accumulo namespace
      cfg.setProperty(Property.SSERV_SCAN_ALLOWED_TABLES.getKey()
          + ScanServerSelector.DEFAULT_SCAN_SERVER_GROUP_NAME, "^accumulo\\..*$");
      // allow the ScanServer in the GROUP1 group to only scan tables created with the prefix 'test'
      cfg.setProperty(Property.SSERV_SCAN_ALLOWED_TABLES.getKey() + "GROUP1", "^test.*");

      cfg.setClientProperty(ClientProperty.SCAN_SERVER_SELECTOR_OPTS_PREFIX.getKey() + "profiles",
          clientConfiguration);
    }

  }

  @BeforeAll
  public static void start() throws Exception {
    SharedMiniClusterBase.startMiniClusterWithConfig(new SSATITConfiguration());
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
  public void testAllowedTables() throws Exception {

    final String zooRoot = getCluster().getServerContext().getZooKeeperRoot();
    final ZooKeeper zk = getCluster().getServerContext().getZooReaderWriter().getZooKeeper();
    final String scanServerRoot = zooRoot + Constants.ZSSERVERS;

    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {

      // Start the 2nd ScanServer
      // Bump the number of scan serves that can run to start the GROUP1 scan server
      getCluster().getConfig().setNumScanServers(2);
      getCluster()._exec(ScanServer.class, ServerType.SCAN_SERVER, Map.of(),
          new String[] {"-g", "GROUP1"});
      Wait.waitFor(() -> zk.getChildren(scanServerRoot, false).size() == 2);
      Wait.waitFor(() -> ((ClientContext) client).getScanServers().values().stream().anyMatch(
          (p) -> p.getSecond().equals(ScanServerSelector.DEFAULT_SCAN_SERVER_GROUP_NAME)) == true);
      Wait.waitFor(() -> ((ClientContext) client).getScanServers().values().stream()
          .anyMatch((p) -> p.getSecond().equals("GROUP1")) == true);

      // Create table with test prefix, load some data
      final String testTableName = "testAllowedTables";
      final int ingestedEntryCount =
          ScanServerIT.createTableAndIngest(client, testTableName, null, 10, 10, "colf");
      assertEquals(100, ingestedEntryCount);

      // Using default ScanServer should succeed, only allowed to scan system tables
      try (Scanner scanner = client.createScanner(MetadataTable.NAME, Authorizations.EMPTY)) {
        scanner.setRange(new Range());
        scanner.setConsistencyLevel(ConsistencyLevel.EVENTUAL);
        assertTrue(Iterables.size(scanner) > 0);
      }

      // Using default ScanServer should fail, only allowed to scan system tables
      try (Scanner scanner = client.createScanner(testTableName, Authorizations.EMPTY)) {
        scanner.setRange(new Range());
        scanner.setConsistencyLevel(ConsistencyLevel.EVENTUAL);
        assertThrows(RuntimeException.class, () -> Iterables.size(scanner));
      }

      // Using GROUP1 ScanServer should fail, only allowed to test tables
      try (Scanner scanner = client.createScanner(MetadataTable.NAME, Authorizations.EMPTY)) {
        scanner.setRange(new Range());
        scanner.setConsistencyLevel(ConsistencyLevel.EVENTUAL);
        scanner.setExecutionHints(Map.of("scan_type", "use_group1"));
        assertThrows(RuntimeException.class, () -> Iterables.size(scanner));
      }

      // Using GROUP1 ScanServer should succeed
      try (Scanner scanner = client.createScanner(testTableName, Authorizations.EMPTY)) {
        scanner.setRange(new Range());
        scanner.setConsistencyLevel(ConsistencyLevel.EVENTUAL);
        scanner.setExecutionHints(Map.of("scan_type", "use_group1"));
        assertEquals(100, Iterables.size(scanner));
      }

      // Change the GROUP1 property so that subsequent test tables don't work
      getCluster().getServerContext().instanceOperations()
          .setProperty(Property.SSERV_SCAN_ALLOWED_TABLES.getKey() + "GROUP1", "^foo.*");

      // Using GROUP1 ScanServer should fail, only allowed to test tables
      try (Scanner scanner = client.createScanner(MetadataTable.NAME, Authorizations.EMPTY)) {
        scanner.setRange(new Range());
        scanner.setConsistencyLevel(ConsistencyLevel.EVENTUAL);
        scanner.setExecutionHints(Map.of("scan_type", "use_group1"));
        assertThrows(RuntimeException.class, () -> Iterables.size(scanner));
      }

      // Using GROUP1 ScanServer should fail as the property was changed
      try (Scanner scanner = client.createScanner(testTableName, Authorizations.EMPTY)) {
        scanner.setRange(new Range());
        scanner.setConsistencyLevel(ConsistencyLevel.EVENTUAL);
        scanner.setExecutionHints(Map.of("scan_type", "use_group1"));
        assertThrows(RuntimeException.class, () -> Iterables.size(scanner));
      }

      // Change the GROUP1 property so that subsequent test tables don't work
      getCluster().getServerContext().instanceOperations()
          .setProperty(Property.SSERV_SCAN_ALLOWED_TABLES.getKey() + "GROUP1", "^test.*");

      // Using GROUP1 ScanServer should succeed as the property was changed back
      try (Scanner scanner = client.createScanner(testTableName, Authorizations.EMPTY)) {
        scanner.setRange(new Range());
        scanner.setConsistencyLevel(ConsistencyLevel.EVENTUAL);
        scanner.setExecutionHints(Map.of("scan_type", "use_group1"));
        assertEquals(100, Iterables.size(scanner));
      }

    }

  }

}

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
import java.util.Set;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.ScannerBase;
import org.apache.accumulo.core.client.ScannerBase.ConsistencyLevel;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.ResourceGroupOperations;
import org.apache.accumulo.core.clientImpl.ClientContext;
import org.apache.accumulo.core.conf.ClientProperty;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.ResourceGroupId;
import org.apache.accumulo.core.lock.ServiceLockPaths.AddressSelector;
import org.apache.accumulo.core.lock.ServiceLockPaths.ResourceGroupPredicate;
import org.apache.accumulo.core.metadata.SystemTables;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.harness.MiniClusterConfigurationCallback;
import org.apache.accumulo.harness.SharedMiniClusterBase;
import org.apache.accumulo.minicluster.ServerType;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloConfigImpl;
import org.apache.accumulo.test.util.Wait;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.thrift.TApplicationException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import com.google.common.collect.Iterables;

public class ScanServerAllowedTablesIT extends SharedMiniClusterBase {

  private static final String GROUP_NAME = "GROUP1";
  private static ResourceGroupId rgid = ResourceGroupId.of(GROUP_NAME);

  public static class SSATITConfiguration implements MiniClusterConfigurationCallback {

    @Override
    public void configureMiniCluster(MiniAccumuloConfigImpl cfg, Configuration coreSite) {

      cfg.getClusterServerConfiguration().setNumDefaultScanServers(1);
      cfg.getClusterServerConfiguration().addScanServerResourceGroup("GROUP1", 1);

      cfg.setClientProperty(ClientProperty.SCAN_SERVER_SELECTOR_OPTS_PREFIX.getKey() + "profiles",
          ScanServerGroupConfigurationIT.clientConfiguration);
    }

  }

  @BeforeAll
  public static void start() throws Exception {
    SharedMiniClusterBase.startMiniClusterWithConfig(new SSATITConfiguration());
    SharedMiniClusterBase.getCluster().getClusterControl().start(ServerType.SCAN_SERVER,
        "localhost");

    try (var client = Accumulo.newClient().from(getClientProps()).build()) {
      @SuppressWarnings("resource")
      final ClientContext cc = (ClientContext) client;

      Wait.waitFor(() -> cc.getServerPaths().getScanServer(
          ResourceGroupPredicate.exact(ResourceGroupId.DEFAULT), AddressSelector.all(), true).size()
          == 1);
      Wait.waitFor(() -> cc.getServerPaths()
          .getScanServer(ResourceGroupPredicate.exact(rgid), AddressSelector.all(), true).size()
          == 1);

      final ResourceGroupOperations rgOps = client.resourceGroupOperations();

      rgOps.setProperty(ResourceGroupId.DEFAULT, Property.SSERV_SCAN_ALLOWED_TABLES.getKey(),
          "^accumulo\\..*$");

      rgOps.create(rgid);
      rgOps.setProperty(rgid, Property.SSERV_SCAN_ALLOWED_TABLES.getKey(), "^test.*");
    }

  }

  @AfterAll
  public static void stop() throws Exception {
    SharedMiniClusterBase.stopMiniCluster();
  }

  public static enum ScannerType {
    BATCH_SCANNER, SCANNER;
  }

  private ScannerBase createScanner(AccumuloClient client, ScannerType stype, String tableName)
      throws TableNotFoundException {
    switch (stype) {
      case BATCH_SCANNER:
        BatchScanner batchScanner = client.createBatchScanner(tableName, Authorizations.EMPTY);
        batchScanner.setRanges(Set.of(new Range()));
        return batchScanner;
      case SCANNER:
        Scanner scanner = client.createScanner(tableName, Authorizations.EMPTY);
        scanner.setRange(new Range());
        return scanner;
      default:
        throw new IllegalArgumentException("Unknown scanner type: " + stype);
    }
  }

  @SuppressWarnings("unused")
  @ParameterizedTest
  @EnumSource(value = ScannerType.class)
  public void testAllowedTables(ScannerType stype) throws Exception {

    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {

      final ResourceGroupOperations rgOps = client.resourceGroupOperations();

      // Create table with test prefix, load some data
      final String testTableName = "testAllowedTables" + stype.name();
      final int ingestedEntryCount =
          ScanServerIT.createTableAndIngest(client, testTableName, null, 10, 10, "colf");
      assertEquals(100, ingestedEntryCount);

      // Using default ScanServer should succeed, only allowed to scan system tables
      try (ScannerBase scanner = createScanner(client, stype, SystemTables.METADATA.tableName())) {
        scanner.setConsistencyLevel(ConsistencyLevel.EVENTUAL);
        assertTrue(Iterables.size(scanner) > 0);
      }

      // Using default ScanServer should fail, only allowed to scan system tables
      try (ScannerBase scanner = createScanner(client, stype, testTableName)) {
        scanner.setConsistencyLevel(ConsistencyLevel.EVENTUAL);
        RuntimeException re = assertThrows(RuntimeException.class, () -> Iterables.size(scanner));
        Throwable root = ExceptionUtils.getRootCause(re);
        assertTrue(root instanceof TApplicationException);
        TApplicationException tae = (TApplicationException) root;
        assertEquals(TApplicationException.INTERNAL_ERROR, tae.getType());
        assertTrue(tae.getMessage().contains("disallowed by property"));
      }

      // Using GROUP1 ScanServer should fail, only allowed to test tables
      try (ScannerBase scanner = createScanner(client, stype, SystemTables.METADATA.tableName())) {
        scanner.setConsistencyLevel(ConsistencyLevel.EVENTUAL);
        scanner.setExecutionHints(Map.of("scan_type", "use_group1"));
        RuntimeException re = assertThrows(RuntimeException.class, () -> Iterables.size(scanner));
        Throwable root = ExceptionUtils.getRootCause(re);
        assertTrue(root instanceof TApplicationException);
        TApplicationException tae = (TApplicationException) root;
        assertEquals(TApplicationException.INTERNAL_ERROR, tae.getType());
        assertTrue(tae.getMessage().contains("disallowed by property"));
      }

      // Using GROUP1 ScanServer should succeed
      try (ScannerBase scanner = createScanner(client, stype, testTableName)) {
        scanner.setConsistencyLevel(ConsistencyLevel.EVENTUAL);
        scanner.setExecutionHints(Map.of("scan_type", "use_group1"));
        assertEquals(100, Iterables.size(scanner));
      }

      // Change the GROUP1 property so that subsequent test tables don't work
      rgOps.setProperty(rgid, Property.SSERV_SCAN_ALLOWED_TABLES.getKey(), "^foo.*");

      // Using GROUP1 ScanServer should fail, only allowed to test 'test*' tables
      try (ScannerBase scanner = createScanner(client, stype, SystemTables.METADATA.tableName())) {
        scanner.setConsistencyLevel(ConsistencyLevel.EVENTUAL);
        scanner.setExecutionHints(Map.of("scan_type", "use_group1"));
        RuntimeException re = assertThrows(RuntimeException.class, () -> Iterables.size(scanner));
        Throwable root = ExceptionUtils.getRootCause(re);
        assertTrue(root instanceof TApplicationException);
        TApplicationException tae = (TApplicationException) root;
        assertEquals(TApplicationException.INTERNAL_ERROR, tae.getType());
        assertTrue(tae.getMessage().contains("disallowed by property"));
      }

      // Using GROUP1 ScanServer should fail as the property was changed
      try (ScannerBase scanner = createScanner(client, stype, testTableName)) {
        scanner.setConsistencyLevel(ConsistencyLevel.EVENTUAL);
        scanner.setExecutionHints(Map.of("scan_type", "use_group1"));
        // Try multiple times waiting for the server to pick up the property change
        Wait.waitFor(() -> {
          try {
            var unused = Iterables.size(scanner);
            return false;
          } catch (RuntimeException e) {
            return true;
          }
        });
      }

      // Change the GROUP1 property so that subsequent test tables do work
      rgOps.setProperty(rgid, Property.SSERV_SCAN_ALLOWED_TABLES.getKey(), "^test.*");

      // Using GROUP1 ScanServer should succeed as the property was changed back
      try (ScannerBase scanner = createScanner(client, stype, testTableName)) {
        scanner.setConsistencyLevel(ConsistencyLevel.EVENTUAL);
        scanner.setExecutionHints(Map.of("scan_type", "use_group1"));
        // Try multiple times waiting for the server to pick up the property change
        Wait.waitFor(() -> {
          try {
            int size = Iterables.size(scanner);
            return size == 100;
          } catch (RuntimeException e) {
            return false;
          }
        });

      }

    }

  }

}

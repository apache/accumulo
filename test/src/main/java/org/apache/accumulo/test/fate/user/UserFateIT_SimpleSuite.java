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
package org.apache.accumulo.test.fate.user;

import static org.apache.accumulo.test.fate.FateTestUtil.createFateTable;
import static org.apache.accumulo.test.fate.TestLock.createDummyLockID;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.stream.StreamSupport;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.TabletAvailability;
import org.apache.accumulo.core.clientImpl.ClientContext;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.fate.AbstractFateStore.FateIdGenerator;
import org.apache.accumulo.core.fate.FateId;
import org.apache.accumulo.core.fate.ReadOnlyFateStore.TStatus;
import org.apache.accumulo.core.fate.user.UserFateStore;
import org.apache.accumulo.core.fate.user.schema.FateSchema;
import org.apache.accumulo.core.iterators.user.VersioningIterator;
import org.apache.accumulo.core.metadata.SystemTables;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.harness.SharedMiniClusterBase;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.test.fate.FateITBase;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class UserFateIT_SimpleSuite extends FateITBase {

  private String table;

  @BeforeAll
  public static void setup() throws Exception {
    SharedMiniClusterBase.startMiniCluster();
  }

  @AfterAll
  public static void teardown() {
    SharedMiniClusterBase.stopMiniCluster();
  }

  @Override
  public void executeTest(FateTestExecutor<TestEnv> testMethod, int maxDeferred,
      FateIdGenerator fateIdGenerator) throws Exception {
    table = getUniqueNames(1)[0];
    try (ClientContext client =
        (ClientContext) Accumulo.newClient().from(getClientProps()).build()) {
      createFateTable(client, table);
      testMethod.execute(new UserFateStore<>(client, table, createDummyLockID(), null, maxDeferred,
          fateIdGenerator), getCluster().getServerContext());
    }
  }

  // UserFateStore only test:
  // Test that configs related to the correctness of the FATE instance user table
  // are initialized correctly
  @Test
  public void testFateInitialConfigCorrectness() throws Exception {
    try (ClientContext client =
        (ClientContext) Accumulo.newClient().from(getClientProps()).build()) {

      // It is important here to use getTableProperties() and not getConfiguration()
      // because we want only the table properties and not a merged view
      var fateTableProps =
          client.tableOperations().getTableProperties(SystemTables.FATE.tableName());

      // Verify properties all have a table. prefix
      assertTrue(fateTableProps.keySet().stream().allMatch(key -> key.startsWith("table.")));

      // Verify properties are correctly set
      assertEquals("5", fateTableProps.get(Property.TABLE_FILE_REPLICATION.getKey()));
      assertEquals("sync", fateTableProps.get(Property.TABLE_DURABILITY.getKey()));
      assertEquals("false", fateTableProps.get(Property.TABLE_FAILURES_IGNORE.getKey()));
      assertEquals("", fateTableProps.get(Property.TABLE_DEFAULT_SCANTIME_VISIBILITY.getKey()));

      // Verify VersioningIterator related properties are correct
      var iterClass = "10," + VersioningIterator.class.getName();
      var maxVersions = "1";
      assertEquals(iterClass,
          fateTableProps.get(Property.TABLE_ITERATOR_PREFIX.getKey() + "scan.vers"));
      assertEquals(maxVersions, fateTableProps
          .get(Property.TABLE_ITERATOR_PREFIX.getKey() + "scan.vers.opt.maxVersions"));
      assertEquals(iterClass,
          fateTableProps.get(Property.TABLE_ITERATOR_PREFIX.getKey() + "minc.vers"));
      assertEquals(maxVersions, fateTableProps
          .get(Property.TABLE_ITERATOR_PREFIX.getKey() + "minc.vers.opt.maxVersions"));
      assertEquals(iterClass,
          fateTableProps.get(Property.TABLE_ITERATOR_PREFIX.getKey() + "majc.vers"));
      assertEquals(maxVersions, fateTableProps
          .get(Property.TABLE_ITERATOR_PREFIX.getKey() + "majc.vers.opt.maxVersions"));

      // Verify all tablets are HOSTED
      try (var tablets =
          client.getAmple().readTablets().forTable(SystemTables.FATE.tableId()).build()) {
        assertTrue(tablets.stream()
            .allMatch(tm -> tm.getTabletAvailability() == TabletAvailability.HOSTED));
      }
    }
  }

  @Override
  protected TStatus getTxStatus(ServerContext context, FateId fateId) {
    try (Scanner scanner = context.createScanner(table, Authorizations.EMPTY)) {
      scanner.setRange(getRow(fateId));
      FateSchema.TxAdminColumnFamily.STATUS_COLUMN.fetch(scanner);
      return StreamSupport.stream(scanner.spliterator(), false)
          .map(e -> TStatus.valueOf(e.getValue().toString())).findFirst().orElse(TStatus.UNKNOWN);
    } catch (TableNotFoundException e) {
      throw new IllegalStateException(table + " not found!", e);
    }
  }

  private static Range getRow(FateId fateId) {
    return new Range(fateId.getTxUUIDStr());
  }
}

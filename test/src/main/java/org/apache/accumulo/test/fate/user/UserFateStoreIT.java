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

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.NewTableConfiguration;
import org.apache.accumulo.core.client.admin.TabletAvailability;
import org.apache.accumulo.core.client.admin.TabletInformation;
import org.apache.accumulo.core.clientImpl.ClientContext;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.fate.FateId;
import org.apache.accumulo.core.fate.FateInstanceType;
import org.apache.accumulo.core.fate.FateStore;
import org.apache.accumulo.core.fate.ReadOnlyFateStore.TStatus;
import org.apache.accumulo.core.fate.user.UserFateStore;
import org.apache.accumulo.core.fate.user.schema.FateSchema;
import org.apache.accumulo.core.iterators.user.VersioningIterator;
import org.apache.accumulo.core.metadata.AccumuloTable;
import org.apache.accumulo.harness.SharedMiniClusterBase;
import org.apache.accumulo.test.fate.FateIT;
import org.apache.accumulo.test.fate.FateTestRunner.TestEnv;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;

import com.google.common.collect.MoreCollectors;

public class UserFateStoreIT extends SharedMiniClusterBase {

  private static final FateInstanceType fateInstanceType = FateInstanceType.USER;

  @BeforeAll
  public static void setup() throws Exception {
    SharedMiniClusterBase.startMiniCluster();
  }

  @AfterAll
  public static void teardown() {
    SharedMiniClusterBase.stopMiniCluster();
  }

  private static class TestUserFateStore extends UserFateStore<TestEnv> {
    private final Iterator<FateId> fateIdIterator;

    // use the list of fateIds to simulate collisions on fateIds
    public TestUserFateStore(ClientContext context, String tableName, List<FateId> fateIds) {
      super(context, tableName);
      this.fateIdIterator = fateIds.iterator();
    }

    @Override
    public FateId getFateId() {
      if (fateIdIterator.hasNext()) {
        return fateIdIterator.next();
      } else {
        return FateId.from(fateInstanceType, UUID.randomUUID());
      }
    }

    public TStatus getStatus(FateId fateId) {
      return _getStatus(fateId);
    }
  }

  // Test that configs related to the correctness of the FATE instance user table
  // are initialized correctly
  @Test
  public void testFateInitialConfigCorrectness() throws Exception {
    try (ClientContext client =
        (ClientContext) Accumulo.newClient().from(getClientProps()).build()) {

      // It is important here to use getTableProperties() and not getConfiguration()
      // because we want only the table properties and not a merged view
      var fateTableProps =
          client.tableOperations().getTableProperties(AccumuloTable.FATE.tableName());

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
          client.getAmple().readTablets().forTable(AccumuloTable.FATE.tableId()).build()) {
        assertTrue(tablets.stream()
            .allMatch(tm -> tm.getTabletAvailability() == TabletAvailability.HOSTED));
      }
    }
  }

  @Nested
  class TestStatusEnforcement {

    String tableName;
    ClientContext client;
    FateId fateId;
    TestUserFateStore store;
    FateStore.FateTxStore<FateIT.TestEnv> txStore;

    @BeforeEach
    public void setup() throws Exception {
      client = (ClientContext) Accumulo.newClient().from(getClientProps()).build();
      tableName = getUniqueNames(1)[0];
      createFateTable(client, tableName);
      fateId = FateId.from(fateInstanceType, UUID.randomUUID());
      store = new TestUserFateStore(client, tableName, List.of(fateId));
      store.create();
      txStore = store.reserve(fateId);
    }

    @AfterEach
    public void teardown() throws Exception {
      client.close();
    }

    private void testOperationWithStatuses(Runnable beforeOperation, Executable operation,
        Set<TStatus> acceptableStatuses) throws Exception {
      for (TStatus status : TStatus.values()) {
        // Run any needed setup for the operation before each iteration
        beforeOperation.run();

        injectStatus(client, tableName, fateId, status);
        assertEquals(status, store.getStatus(fateId));
        if (!acceptableStatuses.contains(status)) {
          assertThrows(IllegalStateException.class, operation,
              "Expected operation to fail with status " + status + " but it did not");
        } else {
          assertDoesNotThrow(operation,
              "Expected operation to succeed with status " + status + " but it did not");
        }
      }
    }

    @Test
    public void push() throws Exception {
      testOperationWithStatuses(() -> {}, // No special setup needed for push
          () -> txStore.push(new FateIT.TestRepo("testOp")),
          Set.of(TStatus.IN_PROGRESS, TStatus.NEW));
    }

    @Test
    public void pop() throws Exception {
      testOperationWithStatuses(() -> {
        // Setup for pop: Ensure there something to pop by first pushing
        try {
          injectStatus(client, tableName, fateId, TStatus.NEW);
          txStore.push(new FateIT.TestRepo("testOp"));
        } catch (Exception e) {
          throw new RuntimeException("Failed to setup for pop", e);
        }
      }, txStore::pop, Set.of(TStatus.FAILED_IN_PROGRESS, TStatus.SUCCESSFUL));
    }

    @Test
    public void delete() throws Exception {
      testOperationWithStatuses(() -> {}, // No special setup needed for delete
          txStore::delete,
          Set.of(TStatus.NEW, TStatus.SUBMITTED, TStatus.SUCCESSFUL, TStatus.FAILED));
    }
  }

  /**
   * Inject a status into the status col of the fate store table for a given transaction id.
   */
  private void injectStatus(ClientContext client, String table, FateId fateId, TStatus status)
      throws TableNotFoundException {
    try (BatchWriter writer = client.createBatchWriter(table)) {
      Mutation mutation = new Mutation(new Text(fateId.getTxUUIDStr()));
      FateSchema.TxColumnFamily.STATUS_COLUMN.put(mutation, new Value(status.name()));
      writer.addMutation(mutation);
    } catch (MutationsRejectedException e) {
      throw new RuntimeException(e);
    }
  }

  // Create the fate table with the exact configuration as the real Fate user instance table
  // including table properties and TabletAvailability
  protected static void createFateTable(ClientContext client, String table) throws Exception {
    final var fateTableProps =
        client.tableOperations().getTableProperties(AccumuloTable.FATE.tableName());

    TabletAvailability availability;
    try (var tabletStream = client.tableOperations()
        .getTabletInformation(AccumuloTable.FATE.tableName(), new Range())) {
      availability = tabletStream.map(TabletInformation::getTabletAvailability).distinct()
          .collect(MoreCollectors.onlyElement());
    }

    var newTableConf = new NewTableConfiguration().withInitialTabletAvailability(availability)
        .withoutDefaultIterators().setProperties(fateTableProps);
    client.tableOperations().create(table, newTableConf);
    var testFateTableProps = client.tableOperations().getTableProperties(table);

    // ensure that create did not set any other props
    assertEquals(fateTableProps, testFateTableProps);
  }
}

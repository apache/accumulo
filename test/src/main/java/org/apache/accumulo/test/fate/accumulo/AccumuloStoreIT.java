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
package org.apache.accumulo.test.fate.accumulo;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.clientImpl.ClientContext;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.fate.FateId;
import org.apache.accumulo.core.fate.FateInstanceType;
import org.apache.accumulo.core.fate.FateStore;
import org.apache.accumulo.core.fate.ReadOnlyFateStore.TStatus;
import org.apache.accumulo.core.fate.accumulo.AccumuloStore;
import org.apache.accumulo.core.fate.accumulo.schema.FateSchema;
import org.apache.accumulo.harness.SharedMiniClusterBase;
import org.apache.accumulo.test.fate.FateIT;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AccumuloStoreIT extends SharedMiniClusterBase {

  private static final Logger log = LoggerFactory.getLogger(AccumuloStore.class);
  private static final FateInstanceType fateInstanceType = FateInstanceType.USER;

  @BeforeAll
  public static void setup() throws Exception {
    SharedMiniClusterBase.startMiniCluster();
  }

  @AfterAll
  public static void teardown() {
    SharedMiniClusterBase.stopMiniCluster();
  }

  private static class TestAccumuloStore extends AccumuloStore<FateIT.TestEnv> {
    private final Iterator<FateId> fateIdIterator;

    // use the list of fateIds to simulate collisions on fateIds
    public TestAccumuloStore(ClientContext context, String tableName, List<FateId> fateIds) {
      super(context, tableName);
      this.fateIdIterator = fateIds.iterator();
    }

    @Override
    public FateId getFateId() {
      if (fateIdIterator.hasNext()) {
        return fateIdIterator.next();
      } else {
        return FateId.from(fateInstanceType, -1L);
      }
    }

    public TStatus getStatus(FateId fateId) {
      return _getStatus(fateId);
    }
  }

  @Test
  public void testCreateWithCollisionAndExceedRetryLimit() throws Exception {
    String table = getUniqueNames(1)[0];
    try (ClientContext client =
        (ClientContext) Accumulo.newClient().from(getClientProps()).build()) {
      client.tableOperations().create(table);

      List<Long> txids = List.of(1L, 1L, 1L, 2L, 3L, 3L, 3L, 3L, 4L, 4L, 5L, 5L, 5L, 5L, 5L, 5L);
      List<FateId> fateIds = txids.stream().map(txid -> FateId.from(fateInstanceType, txid))
          .collect(Collectors.toList());
      Set<FateId> expectedFateIds = new TreeSet<>(fateIds);
      TestAccumuloStore store = new TestAccumuloStore(client, table, fateIds);

      // call create and expect we get the unique txids
      for (FateId expectedFateId : expectedFateIds) {
        FateId fateId = store.create();
        log.info("Created fate id: " + fateId);
        assertEquals(expectedFateId, fateId, "Expected " + expectedFateId + " but got " + fateId);
      }

      // Calling create again on 5L should throw an exception since we've exceeded the max retries
      assertThrows(IllegalStateException.class, store::create);
    }
  }

  @Nested
  class TestStatusEnforcement {

    String tableName;
    ClientContext client;
    FateId fateId;
    TestAccumuloStore store;
    FateStore.FateTxStore<FateIT.TestEnv> txStore;

    @BeforeEach
    public void setup() throws Exception {
      client = (ClientContext) Accumulo.newClient().from(getClientProps()).build();
      tableName = getUniqueNames(1)[0];
      client.tableOperations().create(tableName);
      fateId = FateId.from(fateInstanceType, 1L);
      store = new TestAccumuloStore(client, tableName, List.of(fateId));
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
      Mutation mutation = new Mutation(new Text("tx_" + fateId.getHexTid()));
      FateSchema.TxColumnFamily.STATUS_COLUMN.put(mutation, new Value(status.name()));
      writer.addMutation(mutation);
    } catch (MutationsRejectedException e) {
      throw new RuntimeException(e);
    }
  }

}

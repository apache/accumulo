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
import static org.junit.jupiter.params.provider.Arguments.arguments;

import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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
import org.apache.accumulo.core.fate.ReadOnlyFateStore;
import org.apache.accumulo.core.fate.StackOverflowException;
import org.apache.accumulo.core.fate.accumulo.AccumuloStore;
import org.apache.accumulo.core.fate.accumulo.schema.FateSchema;
import org.apache.accumulo.harness.SharedMiniClusterBase;
import org.apache.accumulo.test.fate.FateIT;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
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

  /**
   * Test that an operation only works with the expected statuses present for the transaction.
   */
  @ParameterizedTest(name = "{index} => Operation: {0}")
  @MethodSource("operationStatusPairs")
  public void testStatuses(String description,
      Consumer<FateStore.FateTxStore<FateIT.TestEnv>> operationConsumer,
      Set<ReadOnlyFateStore.TStatus> acceptableStatuses) throws Exception {
    try (ClientContext client =
        (ClientContext) Accumulo.newClient().from(getClientProps()).build()) {
      final String tableName = getUniqueNames(1)[0];
      try {
        client.tableOperations().create(tableName);
        FateId fateId = FateId.from(fateInstanceType, 1L);
        TestAccumuloStore store = new TestAccumuloStore(client, tableName, List.of(fateId));
        store.create();
        FateStore.FateTxStore<FateIT.TestEnv> txStore = store.reserve(fateId);

        // for each TStatus, set the status and verify that the operation only works if it is in the
        // expected set of statuses
        for (ReadOnlyFateStore.TStatus status : ReadOnlyFateStore.TStatus.values()) {
          injectStatus(client, tableName, fateId, status);
          assertEquals(status, store.getStatus(fateId));
          Executable testOp = () -> operationConsumer.accept(txStore);
          if (!acceptableStatuses.contains(status)) {
            assertThrows(IllegalStateException.class, testOp,
                "Expected " + description + " to fail with status " + status + " but it did not");
          } else {
            assertDoesNotThrow(testOp, "Expected " + description + " to succeed with status "
                + status + " but it did not");
          }
        }
      } finally {
        try {
          client.tableOperations().delete(tableName);
        } catch (TableNotFoundException e) {
          // ignore
        }
      }
    }
  }

  public static Stream<Arguments> operationStatusPairs() {
    return Stream
        .of(arguments("push()", (Consumer<FateStore.FateTxStore<FateIT.TestEnv>>) txStore -> {
          try {
            txStore.push(new FateIT.TestRepo("testOp"));
          } catch (StackOverflowException e) {
            throw new RuntimeException(e);
          }
        }, Set.of(ReadOnlyFateStore.TStatus.IN_PROGRESS, ReadOnlyFateStore.TStatus.NEW)),
            arguments("pop()",
                (Consumer<FateStore.FateTxStore<FateIT.TestEnv>>) FateStore.FateTxStore::pop,
                Set.of(ReadOnlyFateStore.TStatus.FAILED_IN_PROGRESS)),
            arguments("delete()",
                (Consumer<FateStore.FateTxStore<FateIT.TestEnv>>) FateStore.FateTxStore::delete,
                Set.of(ReadOnlyFateStore.TStatus.NEW, ReadOnlyFateStore.TStatus.SUBMITTED,
                    ReadOnlyFateStore.TStatus.SUCCESSFUL, ReadOnlyFateStore.TStatus.FAILED)));
  }

  /**
   * Inject a status into the status col of the fate store table for a given transaction id.
   */
  private void injectStatus(ClientContext client, String table, FateId fateId,
      ReadOnlyFateStore.TStatus status) throws TableNotFoundException {
    try (BatchWriter writer = client.createBatchWriter(table)) {
      Mutation mutation = new Mutation(new Text("tx_" + fateId.getHexTid()));
      FateSchema.TxColumnFamily.STATUS_COLUMN.put(mutation, new Value(status.name()));
      writer.addMutation(mutation);
    } catch (MutationsRejectedException e) {
      throw new RuntimeException(e);
    }
  }
}

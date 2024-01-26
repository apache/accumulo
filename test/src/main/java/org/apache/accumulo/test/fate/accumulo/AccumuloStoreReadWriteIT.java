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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.clientImpl.ClientContext;
import org.apache.accumulo.core.fate.Fate.TxInfo;
import org.apache.accumulo.core.fate.FateStore.FateTxStore;
import org.apache.accumulo.core.fate.ReadOnlyFateStore.TStatus;
import org.apache.accumulo.core.fate.ReadOnlyRepo;
import org.apache.accumulo.core.fate.accumulo.AccumuloStore;
import org.apache.accumulo.harness.SharedMiniClusterBase;
import org.apache.accumulo.test.fate.FateIT.TestEnv;
import org.apache.accumulo.test.fate.FateIT.TestRepo;
import org.apache.accumulo.test.util.Wait;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class AccumuloStoreReadWriteIT extends SharedMiniClusterBase {

  @BeforeAll
  public static void setup() throws Exception {
    SharedMiniClusterBase.startMiniCluster();
  }

  @AfterAll
  public static void teardown() {
    SharedMiniClusterBase.stopMiniCluster();
  }

  @Override
  protected Duration defaultTimeout() {
    return Duration.ofMinutes(1);
  }

  @Test
  public void testReadWrite() throws Exception {
    final String table = getUniqueNames(1)[0];
    try (ClientContext client =
        (ClientContext) Accumulo.newClient().from(getClientProps()).build()) {
      client.tableOperations().create(table);

      AccumuloStore<TestEnv> store = new AccumuloStore<>(client, table);
      // Verify no transactions
      assertEquals(0, store.list().count());

      // Create a new transaction and get the store for it
      long tid = store.create();
      FateTxStore<TestEnv> txStore = store.reserve(tid);
      assertTrue(txStore.timeCreated() > 0);
      assertEquals(1, store.list().count());

      // Push a test FATE op and verify we can read it back
      txStore.push(new TestRepo("testOp"));
      TestRepo op = (TestRepo) txStore.top();
      assertNotNull(op);

      // Test status
      txStore.setStatus(TStatus.SUBMITTED);
      assertEquals(TStatus.SUBMITTED, txStore.getStatus());

      // Set a name to test setTransactionInfo()
      txStore.setTransactionInfo(TxInfo.TX_NAME, "name");
      assertEquals("name", txStore.getTransactionInfo(TxInfo.TX_NAME));

      // Try setting a second test op to test getStack()
      // when listing or popping TestOperation2 should be first
      assertEquals(1, txStore.getStack().size());
      txStore.push(new TestOperation2());
      // test top returns TestOperation2
      ReadOnlyRepo<TestEnv> top = txStore.top();
      assertInstanceOf(TestOperation2.class, top);

      // test get stack
      List<ReadOnlyRepo<TestEnv>> ops = txStore.getStack();
      assertEquals(2, ops.size());
      assertInstanceOf(TestOperation2.class, ops.get(0));
      assertEquals(TestRepo.class, ops.get(1).getClass());

      // test pop, TestOperation should be left
      txStore.pop();
      ops = txStore.getStack();
      assertEquals(1, ops.size());
      assertEquals(TestRepo.class, ops.get(0).getClass());

      // create second
      FateTxStore<TestEnv> txStore2 = store.reserve(store.create());
      assertEquals(2, store.list().count());

      // test delete
      txStore.delete();
      assertEquals(1, store.list().count());
      txStore2.delete();
      assertEquals(0, store.list().count());
    }
  }

  @Test
  public void testReadWriteTxInfo() throws Exception {
    final String table = getUniqueNames(1)[0];
    try (ClientContext client =
        (ClientContext) Accumulo.newClient().from(getClientProps()).build()) {
      client.tableOperations().create(table);

      AccumuloStore<TestEnv> store = new AccumuloStore<>(client, table);

      long tid = store.create();
      FateTxStore<TestEnv> txStore = store.reserve(tid);

      try {
        // Go through all enum values to verify each TxInfo type will be properly
        // written and read from the store
        for (TxInfo txInfo : TxInfo.values()) {
          assertNull(txStore.getTransactionInfo(txInfo));
          txStore.setTransactionInfo(txInfo, "value: " + txInfo.name());
          assertEquals("value: " + txInfo.name(), txStore.getTransactionInfo(txInfo));
        }
      } finally {
        txStore.delete();
      }
    }
  }

  @Test
  public void testDeferredOverflow() throws Exception {
    final String table = getUniqueNames(1)[0];
    try (ClientContext client =
        (ClientContext) Accumulo.newClient().from(getClientProps()).build()) {
      client.tableOperations().create(table);

      AccumuloStore<TestEnv> store = new AccumuloStore<>(client, table, 10);
      // Verify no transactions
      assertEquals(0, store.list().count());
      assertFalse(store.isDeferredOverflow());

      // Store 10 transactions that are all deferred
      final Set<Long> transactions = new HashSet<>();
      for (int i = 0; i < 10; i++) {
        long tid = store.create();
        transactions.add(tid);
        FateTxStore<TestEnv> txStore = store.reserve(tid);
        txStore.setStatus(TStatus.SUBMITTED);
        assertTrue(txStore.timeCreated() > 0);
        txStore.unreserve(10, TimeUnit.SECONDS);
      }

      // Verify we have 10 transactions and all are deferred
      assertEquals(10, store.list().count());
      assertEquals(10, store.getDeferredCount());

      // Should still be false as we are at thet max but not over yet
      assertFalse(store.isDeferredOverflow());

      var executor = Executors.newCachedThreadPool();
      Future<?> future;
      AtomicBoolean keepRunning = new AtomicBoolean(true);
      try {
        // Run and verify all 10 transactions still exist and were not
        // run because of the deferral time of all the transactions
        future = executor.submit(() -> store.runnable(keepRunning, transactions::remove));
        Thread.sleep(2000);
        assertEquals(10, transactions.size());
        // Setting this flag to false should terminate the task if sleeping
        keepRunning.set(false);
        // wait for the future to finish to verify the task finished
        future.get();

        // Store one more that should go over the max deferred of 10
        // and should clear the map and set the overflow flag
        long tid = store.create();
        transactions.add(tid);
        FateTxStore<TestEnv> txStore = store.reserve(tid);
        txStore.setStatus(TStatus.SUBMITTED);
        txStore.unreserve(30, TimeUnit.SECONDS);

        // Verify we have 11 transactions stored and none
        // deferred anymore because of the overflow
        assertEquals(11, store.list().count());
        assertEquals(0, store.getDeferredCount());
        assertTrue(store.isDeferredOverflow());

        // Run and verify all 11 transactions were processed
        // and removed from the store
        keepRunning.set(true);
        future = executor.submit(() -> store.runnable(keepRunning, transactions::remove));
        Wait.waitFor(transactions::isEmpty);
        // Setting this flag to false should terminate the task if sleeping
        keepRunning.set(false);
        // wait for the future to finish to verify the task finished
        future.get();

        // Overflow should now be reset to false so adding another deferred
        // transaction should now go back into the deferral map and flag should
        // still be false as we are under the limit
        assertFalse(store.isDeferredOverflow());
        txStore = store.reserve(store.create());
        txStore.unreserve(30, TimeUnit.SECONDS);
        assertEquals(1, store.getDeferredCount());
        assertFalse(store.isDeferredOverflow());
      } finally {
        executor.shutdownNow();
      }
    }
  }

  private static class TestOperation2 extends TestRepo {

    private static final long serialVersionUID = 1L;

    public TestOperation2() {
      super("testOperation2");
    }
  }

}

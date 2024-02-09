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
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.fate.AbstractFateStore;
import org.apache.accumulo.core.fate.Fate.TxInfo;
import org.apache.accumulo.core.fate.FateId;
import org.apache.accumulo.core.fate.FateKey;
import org.apache.accumulo.core.fate.FateStore;
import org.apache.accumulo.core.fate.FateStore.FateTxStore;
import org.apache.accumulo.core.fate.ReadOnlyFateStore.TStatus;
import org.apache.accumulo.core.fate.ReadOnlyRepo;
import org.apache.accumulo.core.fate.StackOverflowException;
import org.apache.accumulo.core.metadata.schema.ExternalCompactionId;
import org.apache.accumulo.harness.SharedMiniClusterBase;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.test.fate.FateIT.TestRepo;
import org.apache.accumulo.test.fate.FateTestRunner;
import org.apache.accumulo.test.fate.FateTestRunner.TestEnv;
import org.apache.accumulo.test.util.Wait;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.Test;

public abstract class FateStoreIT extends SharedMiniClusterBase implements FateTestRunner<TestEnv> {

  @Override
  protected Duration defaultTimeout() {
    return Duration.ofMinutes(1);
  }

  @Test
  public void testReadWrite() throws Exception {
    executeTest(this::testReadWrite);
  }

  protected void testReadWrite(FateStore<TestEnv> store, ServerContext sctx)
      throws StackOverflowException {
    // Verify no transactions
    assertEquals(0, store.list().count());

    // Create a new transaction and get the store for it
    FateId fateId = store.create();
    FateTxStore<TestEnv> txStore = store.reserve(fateId);
    assertTrue(txStore.timeCreated() > 0);
    assertFalse(txStore.getKey().isPresent());
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

  @Test
  public void testReadWriteTxInfo() throws Exception {
    executeTest(this::testReadWriteTxInfo);
  }

  protected void testReadWriteTxInfo(FateStore<TestEnv> store, ServerContext sctx) {
    FateId fateId = store.create();
    FateTxStore<TestEnv> txStore = store.reserve(fateId);

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

  @Test
  public void testDeferredOverflow() throws Exception {
    executeTest(this::testDeferredOverflow, 10, AbstractFateStore.DEFAULT_FATE_ID_GENERATOR);
  }

  protected void testDeferredOverflow(FateStore<TestEnv> store, ServerContext sctx)
      throws Exception {
    // Verify no transactions
    assertEquals(0, store.list().count());
    assertFalse(store.isDeferredOverflow());

    // Store 10 transactions that are all deferred
    final Set<FateId> transactions = new HashSet<>();
    for (int i = 0; i < 10; i++) {
      FateId fateId = store.create();
      transactions.add(fateId);
      FateTxStore<TestEnv> txStore = store.reserve(fateId);
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
      FateId fateId = store.create();
      transactions.add(fateId);
      FateTxStore<TestEnv> txStore = store.reserve(fateId);
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
      // Cleanup so we don't interfere with other tests
      // All stores should already be unreserved
      store.list().forEach(
          fateIdStatus -> store.tryReserve(fateIdStatus.getFateId()).orElseThrow().delete());
    }
  }

  @Test
  public void testCreateWithKey() throws Exception {
    executeTest(this::testCreateWithKey);
  }

  protected void testCreateWithKey(FateStore<TestEnv> store, ServerContext sctx) {
    KeyExtent ke1 =
        new KeyExtent(TableId.of(getUniqueNames(1)[0]), new Text("zzz"), new Text("aaa"));

    FateKey fateKey1 = FateKey.forSplit(ke1);
    FateId fateId1 = store.create(fateKey1);

    FateKey fateKey2 =
        FateKey.forCompactionCommit(ExternalCompactionId.generate(UUID.randomUUID()));
    FateId fateId2 = store.create(fateKey2);
    assertNotEquals(fateId1, fateId2);

    FateTxStore<TestEnv> txStore1 = store.reserve(fateId1);
    FateTxStore<TestEnv> txStore2 = store.reserve(fateId2);
    try {
      assertTrue(txStore1.timeCreated() > 0);
      assertEquals(TStatus.NEW, txStore1.getStatus());
      assertEquals(fateKey1, txStore1.getKey().orElseThrow());

      assertTrue(txStore2.timeCreated() > 0);
      assertEquals(TStatus.NEW, txStore2.getStatus());
      assertEquals(fateKey2, txStore2.getKey().orElseThrow());

      assertEquals(2, store.list().count());
    } finally {
      txStore1.delete();
      txStore2.delete();
      txStore1.unreserve(0, TimeUnit.SECONDS);
      txStore2.unreserve(0, TimeUnit.SECONDS);
    }
  }

  @Test
  public void testCreateWithKeyDuplicate() throws Exception {
    executeTest(this::testCreateWithKeyDuplicate);
  }

  protected void testCreateWithKeyDuplicate(FateStore<TestEnv> store, ServerContext sctx) {
    KeyExtent ke =
        new KeyExtent(TableId.of(getUniqueNames(1)[0]), new Text("zzz"), new Text("aaa"));

    // Creating with the same key should be fine if the status is NEW
    // It should just return the same id and allow us to continue reserving
    FateKey fateKey = FateKey.forSplit(ke);
    FateId fateId1 = store.create(fateKey);
    FateId fateId2 = store.create(fateKey);
    assertEquals(fateId1, fateId2);

    FateTxStore<TestEnv> txStore = store.reserve(fateId1);
    try {
      assertTrue(txStore.timeCreated() > 0);
      assertEquals(TStatus.NEW, txStore.getStatus());
      assertEquals(fateKey, txStore.getKey().orElseThrow());
      assertEquals(1, store.list().count());
    } finally {
      txStore.delete();
      txStore.unreserve(0, TimeUnit.SECONDS);
    }
  }

  @Test
  public void testCreateWithKeyInProgress() throws Exception {
    executeTest(this::testCreateWithKeyInProgress);
  }

  protected void testCreateWithKeyInProgress(FateStore<TestEnv> store, ServerContext sctx) {
    KeyExtent ke =
        new KeyExtent(TableId.of(getUniqueNames(1)[0]), new Text("zzz"), new Text("aaa"));

    FateKey fateKey = FateKey.forSplit(ke);
    FateId fateId = store.create(fateKey);

    FateTxStore<TestEnv> txStore = store.reserve(fateId);
    try {
      assertTrue(txStore.timeCreated() > 0);
      txStore.setStatus(TStatus.IN_PROGRESS);

      // We have an existing transaction with the same key in progress
      // so should not be allowed
      assertThrows(IllegalStateException.class, () -> store.create(fateKey));
      assertEquals(TStatus.IN_PROGRESS, txStore.getStatus());
    } finally {
      txStore.delete();
      txStore.unreserve(0, TimeUnit.SECONDS);
    }

    try {
      // After deletion, make sure we can create again with the same key
      fateId = store.create(fateKey);
      txStore = store.reserve(fateId);
      assertTrue(txStore.timeCreated() > 0);
      assertEquals(TStatus.NEW, txStore.getStatus());
    } finally {
      txStore.delete();
      txStore.unreserve(0, TimeUnit.SECONDS);
    }

  }

  @Test
  public void testCreateWithKeyCollision() throws Exception {
    // Replace the default hasing algorithm with one that always returns the same tid so
    // we can check duplicate detection with different keys
    executeTest(this::testCreateWithKeyCollision, AbstractFateStore.DEFAULT_MAX_DEFERRED,
        (instanceType, fateKey) -> FateId.from(instanceType, 1000));
  }

  protected void testCreateWithKeyCollision(FateStore<TestEnv> store, ServerContext sctx) {
    String[] tables = getUniqueNames(2);
    KeyExtent ke1 = new KeyExtent(TableId.of(tables[0]), new Text("zzz"), new Text("aaa"));
    KeyExtent ke2 = new KeyExtent(TableId.of(tables[1]), new Text("ddd"), new Text("bbb"));

    FateKey fateKey1 = FateKey.forSplit(ke1);
    FateKey fateKey2 = FateKey.forSplit(ke2);
    FateId fateId1 = store.create(fateKey1);

    FateTxStore<TestEnv> txStore = store.reserve(fateId1);
    try {
      var e = assertThrows(IllegalStateException.class, () -> store.create(fateKey2));
      assertEquals("Collision detected for tid 1000", e.getMessage());
      assertEquals(fateKey1, txStore.getKey().orElseThrow());
    } finally {
      txStore.delete();
      txStore.unreserve(0, TimeUnit.SECONDS);
    }

  }

  @Test
  public void testCollisionWithRandomFateId() throws Exception {
    executeTest(this::testCollisionWithRandomFateId);
  }

  protected void testCollisionWithRandomFateId(FateStore<TestEnv> store, ServerContext sctx) {
    KeyExtent ke =
        new KeyExtent(TableId.of(getUniqueNames(1)[0]), new Text("zzz"), new Text("aaa"));

    FateKey fateKey = FateKey.forSplit(ke);
    FateId fateId = store.create(fateKey);

    // After create a fate transaction using a key we can simulate a collision with
    // a random FateId by deleting the key out of Fate and calling create again to verify
    // it detects the key is missing. Then we can continue and see if we can still reserve
    // and use the existing transaction, which we should.
    deleteKey(fateId, sctx);
    var e = assertThrows(IllegalStateException.class, () -> store.create(fateKey));
    assertEquals("Tx key column is missing", e.getMessage());

    // We should still be able to reserve and continue when not using a key
    FateTxStore<TestEnv> txStore = store.reserve(fateId);
    try {
      assertTrue(txStore.timeCreated() > 0);
      assertEquals(TStatus.NEW, txStore.getStatus());
    } finally {
      txStore.delete();
      txStore.unreserve(0, TimeUnit.SECONDS);
    }

  }

  protected abstract void deleteKey(FateId fateId, ServerContext sctx);

  private static class TestOperation2 extends TestRepo {

    private static final long serialVersionUID = 1L;

    public TestOperation2() {
      super("testOperation2");
    }
  }

}

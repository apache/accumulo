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

import java.lang.reflect.Method;
import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
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

import com.google.common.base.Throwables;

public abstract class FateStoreIT extends SharedMiniClusterBase implements FateTestRunner<TestEnv> {

  private static final Method fsCreateByKeyMethod;

  static {
    try {
      // Private method, need to capture for testing
      fsCreateByKeyMethod = AbstractFateStore.class.getDeclaredMethod("create", FateKey.class);
      fsCreateByKeyMethod.setAccessible(true);
    } catch (NoSuchMethodException e) {
      throw new RuntimeException(e);
    }
  }

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

    long existing = store.list().count();
    FateKey fateKey1 = FateKey.forSplit(ke1);
    FateKey fateKey2 =
        FateKey.forCompactionCommit(ExternalCompactionId.generate(UUID.randomUUID()));

    FateTxStore<TestEnv> txStore1 = store.createAndReserve(fateKey1).orElseThrow();
    FateTxStore<TestEnv> txStore2 = store.createAndReserve(fateKey2).orElseThrow();

    assertNotEquals(txStore1.getID(), txStore2.getID());

    try {
      assertTrue(txStore1.timeCreated() > 0);
      assertEquals(TStatus.NEW, txStore1.getStatus());
      assertEquals(fateKey1, txStore1.getKey().orElseThrow());

      assertTrue(txStore2.timeCreated() > 0);
      assertEquals(TStatus.NEW, txStore2.getStatus());
      assertEquals(fateKey2, txStore2.getKey().orElseThrow());

      assertEquals(existing + 2, store.list().count());
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
    // A second call to createAndReserve() should just return an empty optional
    // since it's already in reserved and in progress
    FateKey fateKey = FateKey.forSplit(ke);
    FateTxStore<TestEnv> txStore = store.createAndReserve(fateKey).orElseThrow();

    // second call is empty
    assertTrue(store.createAndReserve(fateKey).isEmpty());

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

  protected void testCreateWithKeyInProgress(FateStore<TestEnv> store, ServerContext sctx)
      throws Exception {
    KeyExtent ke =
        new KeyExtent(TableId.of(getUniqueNames(1)[0]), new Text("zzz"), new Text("aaa"));
    FateKey fateKey = FateKey.forSplit(ke);

    FateTxStore<TestEnv> txStore = store.createAndReserve(fateKey).orElseThrow();
    ;
    try {
      assertTrue(txStore.timeCreated() > 0);
      txStore.setStatus(TStatus.IN_PROGRESS);

      // We have an existing transaction with the same key in progress
      // so should return an empty Optional
      assertTrue(create(store, fateKey).isEmpty());
      assertEquals(TStatus.IN_PROGRESS, txStore.getStatus());
    } finally {
      txStore.delete();
      txStore.unreserve(0, TimeUnit.SECONDS);
    }

    try {
      // After deletion, make sure we can create again with the same key
      txStore = store.createAndReserve(fateKey).orElseThrow();
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

    FateTxStore<TestEnv> txStore = store.createAndReserve(fateKey1).orElseThrow();
    try {
      var e = assertThrows(IllegalStateException.class, () -> create(store, fateKey2));
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

  protected void testCollisionWithRandomFateId(FateStore<TestEnv> store, ServerContext sctx)
      throws Exception {
    KeyExtent ke =
        new KeyExtent(TableId.of(getUniqueNames(1)[0]), new Text("zzz"), new Text("aaa"));

    FateKey fateKey = FateKey.forSplit(ke);
    FateId fateId = create(store, fateKey).orElseThrow();

    // After create a fate transaction using a key we can simulate a collision with
    // a random FateId by deleting the key out of Fate and calling create again to verify
    // it detects the key is missing. Then we can continue and see if we can still reserve
    // and use the existing transaction, which we should.
    deleteKey(fateId, sctx);
    var e = assertThrows(IllegalStateException.class, () -> store.createAndReserve(fateKey));
    assertEquals("Tx Key is missing from tid " + fateId.getTid(), e.getMessage());

    // We should still be able to reserve and continue when not using a key
    // just like a normal transaction
    FateTxStore<TestEnv> txStore = store.reserve(fateId);
    try {
      assertTrue(txStore.timeCreated() > 0);
      assertEquals(TStatus.NEW, txStore.getStatus());
    } finally {
      txStore.delete();
      txStore.unreserve(0, TimeUnit.SECONDS);
    }

  }

  // create(fateKey) method is private so expose for testing to check error states
  @SuppressWarnings("unchecked")
  protected Optional<FateId> create(FateStore<TestEnv> store, FateKey fateKey) throws Exception {
    try {
      return (Optional<FateId>) fsCreateByKeyMethod.invoke(store, fateKey);
    } catch (Exception e) {
      Exception rootCause = (Exception) Throwables.getRootCause(e);
      throw rootCause;
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

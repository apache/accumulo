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
package org.apache.accumulo.test.fate;

import static org.apache.accumulo.core.fate.ReadOnlyFateStore.TStatus.FAILED;
import static org.apache.accumulo.core.fate.ReadOnlyFateStore.TStatus.FAILED_IN_PROGRESS;
import static org.apache.accumulo.core.fate.ReadOnlyFateStore.TStatus.IN_PROGRESS;
import static org.apache.accumulo.core.fate.ReadOnlyFateStore.TStatus.NEW;
import static org.apache.accumulo.core.fate.ReadOnlyFateStore.TStatus.SUBMITTED;
import static org.apache.accumulo.core.fate.ReadOnlyFateStore.TStatus.UNKNOWN;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.accumulo.core.conf.ConfigurationCopy;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.fate.Fate;
import org.apache.accumulo.core.fate.FateStore;
import org.apache.accumulo.core.fate.FateTxId;
import org.apache.accumulo.core.fate.ReadOnlyFateStore.TStatus;
import org.apache.accumulo.core.fate.Repo;
import org.apache.accumulo.harness.SharedMiniClusterBase;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.test.util.Wait;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class FateIT extends SharedMiniClusterBase {

  private static final Logger LOG = LoggerFactory.getLogger(FateIT.class);

  private static CountDownLatch callStarted;
  private static CountDownLatch finishCall;

  public static class TestEnv {

  }

  public static class TestRepo implements Repo<TestEnv> {
    private static final long serialVersionUID = 1L;

    private final String data;

    public TestRepo(String data) {
      this.data = data;
    }

    @Override
    public long isReady(long tid, TestEnv environment) throws Exception {
      return 0;
    }

    @Override
    public String getName() {
      return "TestRepo_" + data;
    }

    @Override
    public Repo<TestEnv> call(long tid, TestEnv environment) throws Exception {
      LOG.debug("Entering call {}", FateTxId.formatTid(tid));
      try {
        FateIT.inCall();
        return null;
      } finally {
        LOG.debug("Leaving call {}", FateTxId.formatTid(tid));
      }
    }

    @Override
    public void undo(long tid, TestEnv environment) throws Exception {

    }

    @Override
    public String getReturn() {
      return data + "_ret";
    }
  }

  /**
   * Test Repo that allows configuring a delay time to be returned in isReady().
   */
  public static class DeferredTestRepo implements Repo<TestEnv> {
    private static final long serialVersionUID = 1L;

    private final String data;

    // These are static as we don't want to serialize them and they should
    // be shared across all instances during the test
    private static final AtomicInteger executedCalls = new AtomicInteger();
    private static final AtomicLong delay = new AtomicLong();
    private static final CountDownLatch callLatch = new CountDownLatch(1);

    public DeferredTestRepo(String data) {
      this.data = data;
    }

    @Override
    public long isReady(long tid, TestEnv environment) {
      LOG.debug("Fate {} delayed {}", tid, delay.get());
      return delay.get();
    }

    @Override
    public String getName() {
      return "TestRepo_" + data;
    }

    @Override
    public Repo<TestEnv> call(long tid, TestEnv environment) throws Exception {
      callLatch.await();
      LOG.debug("Executing call {}, total executed {}", FateTxId.formatTid(tid),
          executedCalls.incrementAndGet());
      return null;
    }

    @Override
    public void undo(long tid, TestEnv environment) {

    }

    @Override
    public String getReturn() {
      return data + "_ret";
    }
  }

  @Test
  @Timeout(30)
  public void testTransactionStatus() throws Exception {
    executeTest(this::testTransactionStatus);
  }

  protected void testTransactionStatus(FateStore<TestEnv> store, ServerContext sctx)
      throws Exception {
    Fate<TestEnv> fate = initializeFate(store);
    try {

      // Wait for the transaction runner to be scheduled.
      Thread.sleep(3000);

      callStarted = new CountDownLatch(1);
      finishCall = new CountDownLatch(1);

      long txid = fate.startTransaction();
      assertEquals(TStatus.NEW, getTxStatus(sctx, txid));
      fate.seedTransaction("TestOperation", txid, new TestRepo("testTransactionStatus"), true,
          "Test Op");
      assertEquals(TStatus.SUBMITTED, getTxStatus(sctx, txid));
      // wait for call() to be called
      callStarted.await();
      assertEquals(IN_PROGRESS, getTxStatus(sctx, txid));
      // tell the op to exit the method
      finishCall.countDown();

      Wait.waitFor(() -> getTxStatus(sctx, txid) == UNKNOWN);
    } finally {
      fate.shutdown();
    }
  }

  @Test
  public void testCancelWhileNew() throws Exception {
    executeTest(this::testCancelWhileNew);
  }

  protected void testCancelWhileNew(FateStore<TestEnv> store, ServerContext sctx) throws Exception {
    Fate<TestEnv> fate = initializeFate(store);
    try {

      // Wait for the transaction runner to be scheduled.
      Thread.sleep(3000);

      callStarted = new CountDownLatch(1);
      finishCall = new CountDownLatch(1);

      long txid = fate.startTransaction();
      LOG.debug("Starting test testCancelWhileNew with {}", FateTxId.formatTid(txid));
      assertEquals(NEW, getTxStatus(sctx, txid));
      // cancel the transaction
      assertTrue(fate.cancel(txid));
      assertTrue(
          FAILED_IN_PROGRESS == getTxStatus(sctx, txid) || FAILED == getTxStatus(sctx, txid));
      fate.seedTransaction("TestOperation", txid, new TestRepo("testCancelWhileNew"), true,
          "Test Op");
      Wait.waitFor(() -> FAILED == getTxStatus(sctx, txid));
      // nothing should have run
      assertEquals(1, callStarted.getCount());
      fate.delete(txid);
      assertEquals(UNKNOWN, getTxStatus(sctx, txid));
    } finally {
      fate.shutdown();
    }
  }

  @Test
  public void testCancelWhileSubmittedAndRunning() throws Exception {
    executeTest(this::testCancelWhileSubmittedAndRunning);
  }

  protected void testCancelWhileSubmittedAndRunning(FateStore<TestEnv> store, ServerContext sctx)
      throws Exception {
    Fate<TestEnv> fate = initializeFate(store);
    try {

      // Wait for the transaction runner to be scheduled.
      Thread.sleep(3000);

      callStarted = new CountDownLatch(1);
      finishCall = new CountDownLatch(1);

      long txid = fate.startTransaction();
      LOG.debug("Starting test testCancelWhileSubmitted with {}", FateTxId.formatTid(txid));
      assertEquals(NEW, getTxStatus(sctx, txid));
      fate.seedTransaction("TestOperation", txid,
          new TestRepo("testCancelWhileSubmittedAndRunning"), false, "Test Op");
      Wait.waitFor(() -> IN_PROGRESS == getTxStatus(sctx, txid));
      // This is false because the transaction runner has reserved the FaTe
      // transaction.
      assertFalse(fate.cancel(txid));
      callStarted.await();
      finishCall.countDown();
      Wait.waitFor(() -> IN_PROGRESS != getTxStatus(sctx, txid));
      fate.delete(txid);
      assertEquals(UNKNOWN, getTxStatus(sctx, txid));
    } finally {
      fate.shutdown();
    }
  }

  @Test
  public void testCancelWhileInCall() throws Exception {
    executeTest(this::testCancelWhileInCall);
  }

  protected void testCancelWhileInCall(FateStore<TestEnv> store, ServerContext sctx)
      throws Exception {
    Fate<TestEnv> fate = initializeFate(store);
    try {

      // Wait for the transaction runner to be scheduled.
      Thread.sleep(3000);

      callStarted = new CountDownLatch(1);
      finishCall = new CountDownLatch(1);

      long txid = fate.startTransaction();
      LOG.debug("Starting test testCancelWhileInCall with {}", FateTxId.formatTid(txid));
      assertEquals(NEW, getTxStatus(sctx, txid));
      fate.seedTransaction("TestOperation", txid, new TestRepo("testCancelWhileInCall"), true,
          "Test Op");
      assertEquals(SUBMITTED, getTxStatus(sctx, txid));
      // wait for call() to be called
      callStarted.await();
      // cancel the transaction
      assertFalse(fate.cancel(txid));
    } finally {
      fate.shutdown();
    }

  }

  @Test
  @Timeout(30)
  public void testDeferredOverflow() throws Exception {
    // Set a maximum deferred map size of 10 transactions so that when the 11th
    // is seen the Fate store should clear the deferred map and mark
    // the flag as overflow so that all the deferred transactions will be run
    executeTest(this::testDeferredOverflow, 10);
  }

  protected void testDeferredOverflow(FateStore<TestEnv> store, ServerContext sctx)
      throws Exception {
    Fate<TestEnv> fate = initializeFate(store);
    try {

      // Wait for the transaction runner to be scheduled.
      Thread.sleep(3000);

      DeferredTestRepo.executedCalls.set(0);
      // Initialize the repo to have a delay of 30 seconds
      // so it will be deferred when submitted
      DeferredTestRepo.delay.set(30000);

      Set<Long> transactions = new HashSet<>();

      // Start by creating 10 transactions that are all deferred which should
      // fill up the deferred map with all 10 as we set the max deferred limit
      // to only allow 10 transactions
      for (int i = 0; i < 10; i++) {
        submitDeferred(fate, sctx, transactions);
      }

      // Verify all 10 are deferred in the map and each will
      // We should not be in an overflow state yet
      Wait.waitFor(() -> store.getDeferredCount() == 10);
      assertFalse(store.isDeferredOverflow());

      // After verifying all 10 are deferred, submit another 10
      // which should trigger an overflow. We are blocking in the
      // call method of DeferredTestRepo at this point using a countdown
      // latch to prevent fate executor from running early and clearing
      // the deferred overflow flag before we can check it below
      for (int i = 0; i < 10; i++) {
        submitDeferred(fate, sctx, transactions);
      }
      // Verify deferred overflow is true and map is now empty
      Wait.waitFor(() -> store.getDeferredCount() == 0);
      Wait.waitFor(store::isDeferredOverflow);

      // Set the delay to 0 and countdown so we will process the
      // call method in the repos. We need to change the delay because
      // due to the async nature of Fate it's possible some of the submitted
      // repos previously wouldn't be processed in the first batch until
      // after the flag was cleared which would trigger a long delay again
      DeferredTestRepo.delay.set(0);
      DeferredTestRepo.callLatch.countDown();

      // Verify the flag was cleared and everything ran
      Wait.waitFor(() -> !store.isDeferredOverflow());
      Wait.waitFor(() -> DeferredTestRepo.executedCalls.get() == 20);

      // Verify all 20 unique transactions finished
      Wait.waitFor(() -> {
        transactions.removeIf(txid -> getTxStatus(sctx, txid) == UNKNOWN);
        return transactions.isEmpty();
      });

    } finally {
      fate.shutdown();
    }
  }

  private void submitDeferred(Fate<TestEnv> fate, ServerContext sctx, Set<Long> transactions) {
    long txid = fate.startTransaction();
    transactions.add(txid);
    assertEquals(TStatus.NEW, getTxStatus(sctx, txid));
    fate.seedTransaction("TestOperation", txid, new DeferredTestRepo("testDeferredOverflow"), true,
        "Test Op");
    assertEquals(TStatus.SUBMITTED, getTxStatus(sctx, txid));
  }

  protected Fate<TestEnv> initializeFate(FateStore<TestEnv> store) {
    ConfigurationCopy config = new ConfigurationCopy();
    config.set(Property.GENERAL_THREADPOOL_SIZE, "2");
    config.set(Property.MANAGER_FATE_THREADPOOL_SIZE, "1");
    return new Fate<>(new TestEnv(), store, r -> r + "", config);
  }

  protected abstract TStatus getTxStatus(ServerContext sctx, long txid);

  protected abstract void executeTest(FateTestExecutor testMethod) throws Exception;

  protected abstract void executeTest(FateTestExecutor testMethod, int maxDeferred)
      throws Exception;

  protected interface FateTestExecutor {
    void execute(FateStore<TestEnv> store, ServerContext sctx) throws Exception;
  }

  private static void inCall() throws InterruptedException {
    // signal that call started
    callStarted.countDown();
    // wait for the signal to exit the method
    finishCall.await();
  }
}

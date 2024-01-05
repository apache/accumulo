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

import java.util.concurrent.CountDownLatch;

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

  @Test
  @Timeout(30)
  public void testTransactionStatus() throws Exception {
    executeTest(this::testTransactionStatus);
  }

  protected void testTransactionStatus(FateStore<TestEnv> store, ServerContext sctx)
      throws Exception {
    ConfigurationCopy config = new ConfigurationCopy();
    config.set(Property.GENERAL_THREADPOOL_SIZE, "2");
    config.set(Property.MANAGER_FATE_THREADPOOL_SIZE, "1");
    TestEnv testEnv = new TestEnv();
    Fate<TestEnv> fate = new Fate<>(testEnv, store, r -> r + "", config);
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
    ConfigurationCopy config = new ConfigurationCopy();
    config.set(Property.GENERAL_THREADPOOL_SIZE, "2");
    config.set(Property.MANAGER_FATE_THREADPOOL_SIZE, "1");
    TestEnv testEnv = new TestEnv();
    Fate<TestEnv> fate = new Fate<>(testEnv, store, r -> r + "", config);
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
    ConfigurationCopy config = new ConfigurationCopy();
    config.set(Property.GENERAL_THREADPOOL_SIZE, "2");
    config.set(Property.MANAGER_FATE_THREADPOOL_SIZE, "1");
    TestEnv testEnv = new TestEnv();
    Fate<TestEnv> fate = new Fate<>(testEnv, store, r -> r + "", config);
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
    ConfigurationCopy config = new ConfigurationCopy();
    config.set(Property.GENERAL_THREADPOOL_SIZE, "2");
    config.set(Property.MANAGER_FATE_THREADPOOL_SIZE, "1");
    TestEnv testEnv = new TestEnv();
    Fate<TestEnv> fate = new Fate<>(testEnv, store, r -> r + "", config);
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

  protected abstract TStatus getTxStatus(ServerContext sctx, long txid) throws Exception;

  protected abstract void executeTest(FateTestExecutor testMethod) throws Exception;

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

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
package org.apache.accumulo.test.fate.zookeeper;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.accumulo.core.fate.ReadOnlyTStore.TStatus.FAILED;
import static org.apache.accumulo.core.fate.ReadOnlyTStore.TStatus.FAILED_IN_PROGRESS;
import static org.apache.accumulo.core.fate.ReadOnlyTStore.TStatus.IN_PROGRESS;
import static org.apache.accumulo.core.fate.ReadOnlyTStore.TStatus.NEW;
import static org.apache.accumulo.core.fate.ReadOnlyTStore.TStatus.SUBMITTED;
import static org.apache.accumulo.harness.AccumuloITBase.ZOOKEEPER_TESTING_SERVER;
import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.clientImpl.thrift.TableOperation;
import org.apache.accumulo.core.conf.ConfigurationCopy;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.InstanceId;
import org.apache.accumulo.core.data.NamespaceId;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.fate.AgeOffStore;
import org.apache.accumulo.core.fate.Fate;
import org.apache.accumulo.core.fate.FateTxId;
import org.apache.accumulo.core.fate.ReadOnlyTStore.TStatus;
import org.apache.accumulo.core.fate.Repo;
import org.apache.accumulo.core.fate.ZooStore;
import org.apache.accumulo.core.fate.zookeeper.DistributedReadWriteLock.LockType;
import org.apache.accumulo.core.fate.zookeeper.ZooReaderWriter;
import org.apache.accumulo.core.zookeeper.ZooSession;
import org.apache.accumulo.manager.Manager;
import org.apache.accumulo.manager.tableOps.ManagerRepo;
import org.apache.accumulo.manager.tableOps.TraceRepo;
import org.apache.accumulo.manager.tableOps.Utils;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.test.util.Wait;
import org.apache.accumulo.test.zookeeper.ZooKeeperTestingServer;
import org.apache.zookeeper.KeeperException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Tag(ZOOKEEPER_TESTING_SERVER)
public class FateIT {

  public static class TestOperation extends ManagerRepo {

    private static final Logger LOG = LoggerFactory.getLogger(TestOperation.class);

    private static final long serialVersionUID = 1L;

    private final TableId tableId;
    private final NamespaceId namespaceId;

    public TestOperation(NamespaceId namespaceId, TableId tableId) {
      this.namespaceId = namespaceId;
      this.tableId = tableId;
    }

    @Override
    public long isReady(long tid, Manager manager) throws Exception {
      return Utils.reserveNamespace(manager, namespaceId, tid, LockType.READ, true,
          TableOperation.RENAME)
          + Utils.reserveTable(manager, tableId, tid, LockType.WRITE, true, TableOperation.RENAME);
    }

    @Override
    public void undo(long tid, Manager manager) throws Exception {
      Utils.unreserveNamespace(manager, namespaceId, tid, LockType.READ);
      Utils.unreserveTable(manager, tableId, tid, LockType.WRITE);
    }

    @Override
    public Repo<Manager> call(long tid, Manager manager) throws Exception {
      LOG.debug("Entering call {}", FateTxId.formatTid(tid));
      try {
        FateIT.inCall();
        return null;
      } finally {
        Utils.unreserveNamespace(manager, namespaceId, tid, LockType.READ);
        Utils.unreserveTable(manager, tableId, tid, LockType.WRITE);
        LOG.debug("Leaving call {}", FateTxId.formatTid(tid));
      }

    }

  }

  public static class TestOperationFails extends ManagerRepo {
    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(TestOperationFails.class);
    private static List<String> undoOrder = new ArrayList<>();
    private static final int TOTAL_NUM_OPS = 3;
    private int opNum;
    private final String opName;
    private final ExceptionLocation location;

    public TestOperationFails(int opNum, ExceptionLocation location) {
      this.opNum = opNum;
      this.opName = "OP" + opNum;
      this.location = location;
    }

    @Override
    public long isReady(long tid, Manager environment) throws Exception {
      LOG.debug("{} {} Entered isReady()", opName, FateTxId.formatTid(tid));
      if (location == ExceptionLocation.IS_READY) {
        if (opNum < TOTAL_NUM_OPS) {
          return 0;
        } else {
          throw new Exception(
              opName + " " + FateTxId.formatTid(tid) + " isReady() failed - this is expected");
        }
      } else {
        return 0;
      }
    }

    @Override
    public void undo(long tid, Manager environment) throws Exception {
      LOG.debug("{} {} Entered undo()", opName, FateTxId.formatTid(tid));
      undoOrder.add(opName);
      undoLatch.countDown();
    }

    @Override
    public Repo<Manager> call(long tid, Manager environment) throws Exception {
      LOG.debug("{} {} Entered call()", opName, FateTxId.formatTid(tid));
      if (location == ExceptionLocation.CALL) {
        if (opNum < TOTAL_NUM_OPS) {
          return new TestOperationFails(++opNum, location);
        } else {
          throw new Exception(
              opName + " " + FateTxId.formatTid(tid) + " call() failed - this is expected");
        }
      } else {
        return new TestOperationFails(++opNum, location);
      }
    }
  }

  private static final Logger LOG = LoggerFactory.getLogger(FateIT.class);

  @TempDir
  private static File tempDir;

  private static ZooKeeperTestingServer testZk = null;
  private static ZooSession zk = null;
  private static ZooReaderWriter zrw = null;
  private static final InstanceId IID = InstanceId.of(UUID.randomUUID());
  private static final NamespaceId NS = NamespaceId.of("testNameSpace");
  private static final TableId TID = TableId.of("testTable");

  private static CountDownLatch callStarted;
  private static CountDownLatch finishCall;
  private static CountDownLatch undoLatch;

  private enum ExceptionLocation {
    CALL, IS_READY
  }

  @BeforeAll
  public static void setup() throws Exception {
    testZk = new ZooKeeperTestingServer(tempDir);
    zk = testZk.newClient();
    zrw = zk.asReaderWriter();
    zrw.mkdirs(Constants.ZFATE);
    zrw.mkdirs(Constants.ZTABLE_LOCKS);
    zrw.mkdirs(Constants.ZNAMESPACES + "/" + NS.canonical());
    zrw.mkdirs(Constants.ZTABLE_STATE + "/" + TID.canonical());
    zrw.mkdirs(Constants.ZTABLES + "/" + TID.canonical());
  }

  @AfterAll
  public static void teardown() throws Exception {
    try {
      zk.close();
    } finally {
      testZk.close();
    }
  }

  @Test
  @Timeout(30)
  public void testTransactionStatus() throws Exception {

    final ZooStore<Manager> zooStore = new ZooStore<>(Constants.ZFATE, zk);
    final AgeOffStore<Manager> store = new AgeOffStore<>(zooStore, 3000, System::currentTimeMillis);

    Manager manager = createMock(Manager.class);
    ServerContext sctx = createMock(ServerContext.class);
    expect(manager.getContext()).andReturn(sctx).anyTimes();
    expect(sctx.getZooSession()).andReturn(zk).anyTimes();
    replay(manager, sctx);

    ConfigurationCopy config = new ConfigurationCopy();
    config.set(Property.GENERAL_THREADPOOL_SIZE, "2");
    config.set(Property.MANAGER_FATE_THREADPOOL_SIZE, "1");
    Fate<Manager> fate = new Fate<>(manager, store, TraceRepo::toLogString, config);
    try {

      callStarted = new CountDownLatch(1);
      finishCall = new CountDownLatch(1);

      long txid = fate.startTransaction();
      assertEquals(TStatus.NEW, getTxStatus(zrw, txid));
      fate.seedTransaction("TestOperation", txid, new TestOperation(NS, TID), true, "Test Op");
      assertEquals(TStatus.SUBMITTED, getTxStatus(zrw, txid));

      // Wait for the transaction runner to be scheduled.
      Thread.sleep(3000);

      // wait for call() to be called
      callStarted.await();
      assertEquals(IN_PROGRESS, getTxStatus(zrw, txid));
      // tell the op to exit the method
      finishCall.countDown();
      // Check that it transitions to SUCCESSFUL and gets removed
      final var sawSuccess = new AtomicBoolean(false);
      Wait.waitFor(() -> {
        TStatus s;
        try {
          switch (s = getTxStatus(zrw, txid)) {
            case IN_PROGRESS:
              if (sawSuccess.get()) {
                fail("Should never see IN_PROGRESS after seeing SUCCESSFUL");
              }
              break;
            case SUCCESSFUL:
              // expected, but might be too quick to be detected
              if (sawSuccess.compareAndSet(false, true)) {
                LOG.debug("Saw expected transaction status change to SUCCESSFUL");
              }
              break;
            default:
              fail("Saw unexpected status: " + s);
          }
        } catch (KeeperException e) {
          if (e.code() == KeeperException.Code.NONODE) {
            if (!sawSuccess.get()) {
              LOG.debug("Never saw transaction status change to SUCCESSFUL, but that's okay");
            }
            return true;
          } else {
            fail("Unexpected error thrown: " + e.getMessage());
          }
        }
        // keep waiting for NoNode
        return false;
      }, SECONDS.toMillis(30), 10);
    } finally {
      fate.shutdown();
    }
  }

  @Test
  public void testCancelWhileNew() throws Exception {
    final ZooStore<Manager> zooStore = new ZooStore<>(Constants.ZFATE, zk);
    final AgeOffStore<Manager> store = new AgeOffStore<>(zooStore, 3000, System::currentTimeMillis);

    Manager manager = createMock(Manager.class);
    ServerContext sctx = createMock(ServerContext.class);
    expect(manager.getContext()).andReturn(sctx).anyTimes();
    expect(sctx.getZooSession()).andReturn(zk).anyTimes();
    replay(manager, sctx);

    ConfigurationCopy config = new ConfigurationCopy();
    config.set(Property.GENERAL_THREADPOOL_SIZE, "2");
    config.set(Property.MANAGER_FATE_THREADPOOL_SIZE, "1");
    Fate<Manager> fate = new Fate<>(manager, store, TraceRepo::toLogString, config);
    try {
      // Wait for the transaction runner to be scheduled.
      Thread.sleep(3000);

      callStarted = new CountDownLatch(1);
      finishCall = new CountDownLatch(1);

      long txid = fate.startTransaction();
      LOG.debug("Starting test testCancelWhileNew with {}", FateTxId.formatTid(txid));
      assertEquals(NEW, getTxStatus(zrw, txid));
      // cancel the transaction
      assertTrue(fate.cancel(txid));
      assertTrue(FAILED_IN_PROGRESS == getTxStatus(zrw, txid) || FAILED == getTxStatus(zrw, txid));
      fate.seedTransaction("TestOperation", txid, new TestOperation(NS, TID), true, "Test Op");
      Wait.waitFor(() -> FAILED == getTxStatus(zrw, txid));
      // nothing should have run
      assertEquals(1, callStarted.getCount());
      fate.delete(txid);
      assertThrows(KeeperException.NoNodeException.class, () -> getTxStatus(zrw, txid));
    } finally {
      fate.shutdown();
    }
  }

  @Test
  public void testCancelWhileSubmittedAndRunning() throws Exception {
    final ZooStore<Manager> zooStore = new ZooStore<>(Constants.ZFATE, zk);
    final AgeOffStore<Manager> store = new AgeOffStore<>(zooStore, 3000, System::currentTimeMillis);

    Manager manager = createMock(Manager.class);
    ServerContext sctx = createMock(ServerContext.class);
    expect(manager.getContext()).andReturn(sctx).anyTimes();
    expect(sctx.getZooSession()).andReturn(zk).anyTimes();
    replay(manager, sctx);

    ConfigurationCopy config = new ConfigurationCopy();
    config.set(Property.GENERAL_THREADPOOL_SIZE, "2");
    config.set(Property.MANAGER_FATE_THREADPOOL_SIZE, "1");
    Fate<Manager> fate = new Fate<>(manager, store, TraceRepo::toLogString, config);
    try {

      // Wait for the transaction runner to be scheduled.
      Thread.sleep(3000);

      callStarted = new CountDownLatch(1);
      finishCall = new CountDownLatch(1);

      long txid = fate.startTransaction();
      LOG.debug("Starting test testCancelWhileSubmitted with {}", FateTxId.formatTid(txid));
      assertEquals(NEW, getTxStatus(zrw, txid));
      fate.seedTransaction("TestOperation", txid, new TestOperation(NS, TID), false, "Test Op");
      Wait.waitFor(() -> IN_PROGRESS == getTxStatus(zrw, txid));
      // This is false because the transaction runner has reserved the FaTe
      // transaction.
      assertFalse(fate.cancel(txid));
      callStarted.await();
      finishCall.countDown();
      Wait.waitFor(() -> IN_PROGRESS != getTxStatus(zrw, txid));
      fate.delete(txid);
      assertThrows(KeeperException.NoNodeException.class, () -> getTxStatus(zrw, txid));
    } finally {
      fate.shutdown();
    }
  }

  @Test
  public void testCancelWhileInCall() throws Exception {
    final ZooStore<Manager> zooStore = new ZooStore<>(Constants.ZFATE, zk);
    final AgeOffStore<Manager> store = new AgeOffStore<>(zooStore, 3000, System::currentTimeMillis);

    Manager manager = createMock(Manager.class);
    ServerContext sctx = createMock(ServerContext.class);
    expect(manager.getContext()).andReturn(sctx).anyTimes();
    expect(sctx.getZooSession()).andReturn(zk).anyTimes();
    replay(manager, sctx);

    ConfigurationCopy config = new ConfigurationCopy();
    config.set(Property.GENERAL_THREADPOOL_SIZE, "2");
    config.set(Property.MANAGER_FATE_THREADPOOL_SIZE, "1");
    Fate<Manager> fate = new Fate<>(manager, store, TraceRepo::toLogString, config);
    try {

      // Wait for the transaction runner to be scheduled.
      Thread.sleep(3000);

      callStarted = new CountDownLatch(1);
      finishCall = new CountDownLatch(1);

      long txid = fate.startTransaction();
      LOG.debug("Starting test testCancelWhileInCall with {}", FateTxId.formatTid(txid));
      assertEquals(NEW, getTxStatus(zrw, txid));
      fate.seedTransaction("TestOperation", txid, new TestOperation(NS, TID), true, "Test Op");
      assertEquals(SUBMITTED, getTxStatus(zrw, txid));

      // wait for call() to be called
      callStarted.await();
      // cancel the transaction
      assertFalse(fate.cancel(txid));
    } finally {
      fate.shutdown();
    }

  }

  @Test
  public void testRepoFails() throws Exception {
    /*
     * This test ensures that when an exception occurs in a Repo's call() or isReady() methods, that
     * undo() will be called back up the chain of Repo's and in the correct order. The test works as
     * follows: 1) Repo1 is called and returns Repo2, 2) Repo2 is called and returns Repo3, 3) Repo3
     * is called and throws an exception (in call() or isReady()). It is then expected that: 1)
     * undo() is called on Repo3, 2) undo() is called on Repo2, 3) undo() is called on Repo1
     */
    final ZooStore<Manager> zooStore = new ZooStore<>(Constants.ZFATE, zk);
    final AgeOffStore<Manager> store = new AgeOffStore<>(zooStore, 3000, System::currentTimeMillis);

    Manager manager = createMock(Manager.class);
    ServerContext sctx = createMock(ServerContext.class);
    expect(manager.getContext()).andReturn(sctx).anyTimes();
    expect(sctx.getZooSession()).andReturn(zk).anyTimes();
    replay(manager, sctx);

    ConfigurationCopy config = new ConfigurationCopy();
    config.set(Property.GENERAL_THREADPOOL_SIZE, "2");
    config.set(Property.MANAGER_FATE_THREADPOOL_SIZE, "1");
    Fate<Manager> fate = new Fate<>(manager, store, TraceRepo::toLogString, config);
    try {

      // Wait for the transaction runner to be scheduled.
      Thread.sleep(3000);

      List<String> expectedUndoOrder = List.of("OP3", "OP2", "OP1");
      /*
       * Test exception in call()
       */
      undoLatch = new CountDownLatch(TestOperationFails.TOTAL_NUM_OPS);
      long txid = fate.startTransaction();
      assertEquals(NEW, getTxStatus(zrw, txid));
      fate.seedTransaction("TestOperationFails", txid,
          new TestOperationFails(1, ExceptionLocation.CALL), false, "Test Op Fails");
      // Wait for all the undo() calls to complete
      undoLatch.await();
      assertEquals(expectedUndoOrder, TestOperationFails.undoOrder);
      assertEquals(FAILED, fate.waitForCompletion(txid));
      assertTrue(fate.getException(txid).getMessage().contains("call() failed"));
      /*
       * Test exception in isReady()
       */
      TestOperationFails.undoOrder = new ArrayList<>();
      undoLatch = new CountDownLatch(TestOperationFails.TOTAL_NUM_OPS);
      txid = fate.startTransaction();
      assertEquals(NEW, getTxStatus(zrw, txid));
      fate.seedTransaction("TestOperationFails", txid,
          new TestOperationFails(1, ExceptionLocation.IS_READY), false, "Test Op Fails");
      // Wait for all the undo() calls to complete
      undoLatch.await();
      assertEquals(expectedUndoOrder, TestOperationFails.undoOrder);
      assertEquals(FAILED, fate.waitForCompletion(txid));
      assertTrue(fate.getException(txid).getMessage().contains("isReady() failed"));
    } finally {
      fate.shutdown();
    }
  }

  private static void inCall() throws InterruptedException {
    // signal that call started
    callStarted.countDown();
    // wait for the signal to exit the method
    finishCall.await();
  }

  /*
   * Get the status of the TX from ZK directly. Unable to call ZooStore.getStatus because this test
   * thread does not have the reservation (the FaTE thread does)
   */
  private static TStatus getTxStatus(ZooReaderWriter zrw, long txid)
      throws KeeperException, InterruptedException {
    zrw.sync("/");
    String txdir = String.format("%s/tx_%016x", Constants.ZFATE, txid);
    return TStatus.valueOf(new String(zrw.getData(txdir), UTF_8));
  }

}

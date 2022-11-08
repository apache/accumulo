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
import static org.apache.accumulo.core.fate.ReadOnlyTStore.TStatus.FAILED;
import static org.apache.accumulo.core.fate.ReadOnlyTStore.TStatus.FAILED_IN_PROGRESS;
import static org.apache.accumulo.core.fate.ReadOnlyTStore.TStatus.IN_PROGRESS;
import static org.apache.accumulo.core.fate.ReadOnlyTStore.TStatus.NEW;
import static org.apache.accumulo.core.fate.ReadOnlyTStore.TStatus.SUBMITTED;
import static org.apache.accumulo.core.fate.ReadOnlyTStore.TStatus.SUCCESSFUL;
import static org.apache.accumulo.harness.AccumuloITBase.ZOOKEEPER_TESTING_SERVER;
import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.File;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.clientImpl.thrift.TableOperation;
import org.apache.accumulo.core.conf.ConfigurationCopy;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.NamespaceId;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.fate.AgeOffStore;
import org.apache.accumulo.core.fate.Fate;
import org.apache.accumulo.core.fate.FateTxId;
import org.apache.accumulo.core.fate.ReadOnlyTStore.TStatus;
import org.apache.accumulo.core.fate.Repo;
import org.apache.accumulo.core.fate.ZooStore;
import org.apache.accumulo.core.fate.zookeeper.ZooReaderWriter;
import org.apache.accumulo.core.util.UtilWaitThread;
import org.apache.accumulo.manager.Manager;
import org.apache.accumulo.manager.tableOps.ManagerRepo;
import org.apache.accumulo.manager.tableOps.TraceRepo;
import org.apache.accumulo.manager.tableOps.Utils;
import org.apache.accumulo.server.ServerContext;
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
      return Utils.reserveNamespace(manager, namespaceId, tid, false, true, TableOperation.RENAME)
          + Utils.reserveTable(manager, tableId, tid, true, true, TableOperation.RENAME);
    }

    @Override
    public void undo(long tid, Manager manager) throws Exception {
      Utils.unreserveNamespace(manager, namespaceId, tid, false);
      Utils.unreserveTable(manager, tableId, tid, true);
    }

    @Override
    public Repo<Manager> call(long tid, Manager manager) throws Exception {
      LOG.debug("Entering call {}", FateTxId.formatTid(tid));
      try {
        FateIT.inCall();
        return null;
      } finally {
        Utils.unreserveNamespace(manager, namespaceId, tid, false);
        Utils.unreserveTable(manager, tableId, tid, true);
        LOG.debug("Leaving call {}", FateTxId.formatTid(tid));
      }

    }

  }

  private static final Logger LOG = LoggerFactory.getLogger(FateIT.class);

  @TempDir
  private static File tempDir;

  private static ZooKeeperTestingServer szk = null;
  private static ZooReaderWriter zk = null;
  private static final String ZK_ROOT = "/accumulo/" + UUID.randomUUID().toString();
  private static final NamespaceId NS = NamespaceId.of("testNameSpace");
  private static final TableId TID = TableId.of("testTable");

  private static CountDownLatch callStarted;
  private static CountDownLatch finishCall;

  @BeforeAll
  public static void setup() throws Exception {
    szk = new ZooKeeperTestingServer(tempDir);
    zk = szk.getZooReaderWriter();
    zk.mkdirs(ZK_ROOT + Constants.ZFATE);
    zk.mkdirs(ZK_ROOT + Constants.ZTABLE_LOCKS);
    zk.mkdirs(ZK_ROOT + Constants.ZNAMESPACES + "/" + NS.canonical());
    zk.mkdirs(ZK_ROOT + Constants.ZTABLE_STATE + "/" + TID.canonical());
    zk.mkdirs(ZK_ROOT + Constants.ZTABLES + "/" + TID.canonical());
  }

  @AfterAll
  public static void teardown() throws Exception {
    szk.close();
  }

  @Test
  @Timeout(30)
  public void testTransactionStatus() throws Exception {

    final ZooStore<Manager> zooStore = new ZooStore<Manager>(ZK_ROOT + Constants.ZFATE, zk);
    final AgeOffStore<Manager> store =
        new AgeOffStore<Manager>(zooStore, 3000, System::currentTimeMillis);

    Manager manager = createMock(Manager.class);
    ServerContext sctx = createMock(ServerContext.class);
    expect(manager.getContext()).andReturn(sctx).anyTimes();
    expect(sctx.getZooKeeperRoot()).andReturn(ZK_ROOT).anyTimes();
    expect(sctx.getZooReaderWriter()).andReturn(zk).anyTimes();
    replay(manager, sctx);

    Fate<Manager> fate = new Fate<Manager>(manager, store, TraceRepo::toLogString);
    try {
      ConfigurationCopy config = new ConfigurationCopy();
      config.set(Property.GENERAL_THREADPOOL_SIZE, "2");
      config.set(Property.MANAGER_FATE_THREADPOOL_SIZE, "1");
      fate.startTransactionRunners(config);

      // Wait for the transaction runner to be scheduled.
      UtilWaitThread.sleep(3000);

      callStarted = new CountDownLatch(1);
      finishCall = new CountDownLatch(1);

      long txid = fate.startTransaction();
      assertEquals(TStatus.NEW, getTxStatus(zk, txid));
      fate.seedTransaction("TestOperation", txid, new TestOperation(NS, TID), true, "Test Op");
      assertEquals(TStatus.SUBMITTED, getTxStatus(zk, txid));
      // wait for call() to be called
      callStarted.await();
      assertEquals(IN_PROGRESS, getTxStatus(zk, txid));
      // tell the op to exit the method
      finishCall.countDown();
      // Check that it transitions to SUCCESSFUL
      TStatus s = getTxStatus(zk, txid);
      while (s != SUCCESSFUL) {
        s = getTxStatus(zk, txid);
        Thread.sleep(10);
      }
      // Check that it gets removed
      boolean errorSeen = false;
      while (!errorSeen) {
        try {
          s = getTxStatus(zk, txid);
          Thread.sleep(10);
        } catch (KeeperException e) {
          if (e.code() == KeeperException.Code.NONODE) {
            errorSeen = true;
          } else {
            fail("Unexpected error thrown: " + e.getMessage());
          }
        }
      }

    } finally {
      fate.shutdown();
    }
  }

  @Test
  public void testCancelWhileNew() throws Exception {
    final ZooStore<Manager> zooStore = new ZooStore<Manager>(ZK_ROOT + Constants.ZFATE, zk);
    final AgeOffStore<Manager> store =
        new AgeOffStore<Manager>(zooStore, 3000, System::currentTimeMillis);

    Manager manager = createMock(Manager.class);
    ServerContext sctx = createMock(ServerContext.class);
    expect(manager.getContext()).andReturn(sctx).anyTimes();
    expect(sctx.getZooKeeperRoot()).andReturn(ZK_ROOT).anyTimes();
    expect(sctx.getZooReaderWriter()).andReturn(zk).anyTimes();
    replay(manager, sctx);

    Fate<Manager> fate = new Fate<Manager>(manager, store, TraceRepo::toLogString);
    try {
      ConfigurationCopy config = new ConfigurationCopy();
      config.set(Property.GENERAL_THREADPOOL_SIZE, "2");
      config.set(Property.MANAGER_FATE_THREADPOOL_SIZE, "1");
      fate.startTransactionRunners(config);

      // Wait for the transaction runner to be scheduled.
      UtilWaitThread.sleep(3000);

      callStarted = new CountDownLatch(1);
      finishCall = new CountDownLatch(1);

      long txid = fate.startTransaction();
      LOG.debug("Starting test testCancelWhileNew with {}", FateTxId.formatTid(txid));
      assertEquals(NEW, getTxStatus(zk, txid));
      // cancel the transaction
      assertTrue(fate.cancel(txid));
      assertTrue(FAILED_IN_PROGRESS == getTxStatus(zk, txid) || FAILED == getTxStatus(zk, txid));
      fate.seedTransaction("TestOperation", txid, new TestOperation(NS, TID), true, "Test Op");
      assertTrue(FAILED_IN_PROGRESS == getTxStatus(zk, txid) || FAILED == getTxStatus(zk, txid));
      fate.delete(txid);
    } finally {
      fate.shutdown();
    }
  }

  @Test
  public void testCancelWhileSubmittedNotRunning() throws Exception {
    final ZooStore<Manager> zooStore = new ZooStore<Manager>(ZK_ROOT + Constants.ZFATE, zk);
    final AgeOffStore<Manager> store =
        new AgeOffStore<Manager>(zooStore, 3000, System::currentTimeMillis);

    Manager manager = createMock(Manager.class);
    ServerContext sctx = createMock(ServerContext.class);
    expect(manager.getContext()).andReturn(sctx).anyTimes();
    expect(sctx.getZooKeeperRoot()).andReturn(ZK_ROOT).anyTimes();
    expect(sctx.getZooReaderWriter()).andReturn(zk).anyTimes();
    replay(manager, sctx);

    Fate<Manager> fate = new Fate<Manager>(manager, store, TraceRepo::toLogString);
    ConfigurationCopy config = new ConfigurationCopy();
    config.set(Property.GENERAL_THREADPOOL_SIZE, "2");

    // Notice that we did not start the transaction runners

    // Wait for the transaction runner to be scheduled.
    UtilWaitThread.sleep(3000);

    callStarted = new CountDownLatch(1);
    finishCall = new CountDownLatch(1);

    long txid = fate.startTransaction();
    LOG.debug("Starting test testCancelWhileSubmitted with {}", FateTxId.formatTid(txid));
    assertEquals(NEW, getTxStatus(zk, txid));
    fate.seedTransaction("TestOperation", txid, new TestOperation(NS, TID), true, "Test Op");
    assertEquals(SUBMITTED, getTxStatus(zk, txid));
    assertTrue(fate.cancel(txid));
  }

  @Test
  public void testCancelWhileSubmittedAndRunning() throws Exception {
    final ZooStore<Manager> zooStore = new ZooStore<Manager>(ZK_ROOT + Constants.ZFATE, zk);
    final AgeOffStore<Manager> store =
        new AgeOffStore<Manager>(zooStore, 3000, System::currentTimeMillis);

    Manager manager = createMock(Manager.class);
    ServerContext sctx = createMock(ServerContext.class);
    expect(manager.getContext()).andReturn(sctx).anyTimes();
    expect(sctx.getZooKeeperRoot()).andReturn(ZK_ROOT).anyTimes();
    expect(sctx.getZooReaderWriter()).andReturn(zk).anyTimes();
    replay(manager, sctx);

    Fate<Manager> fate = new Fate<Manager>(manager, store, TraceRepo::toLogString);
    try {
      ConfigurationCopy config = new ConfigurationCopy();
      config.set(Property.GENERAL_THREADPOOL_SIZE, "2");
      config.set(Property.MANAGER_FATE_THREADPOOL_SIZE, "1");
      fate.startTransactionRunners(config);

      // Wait for the transaction runner to be scheduled.
      UtilWaitThread.sleep(3000);

      callStarted = new CountDownLatch(1);
      finishCall = new CountDownLatch(1);

      long txid = fate.startTransaction();
      LOG.debug("Starting test testCancelWhileSubmitted with {}", FateTxId.formatTid(txid));
      assertEquals(NEW, getTxStatus(zk, txid));
      fate.seedTransaction("TestOperation", txid, new TestOperation(NS, TID), true, "Test Op");
      assertEquals(SUBMITTED, getTxStatus(zk, txid));
      // This is false because the transaction runner has reserved the FaTe
      // transaction.
      assertFalse(fate.cancel(txid));
      callStarted.await();
      finishCall.countDown();
      fate.delete(txid);
    } finally {
      fate.shutdown();
    }
  }

  @Test
  public void testCancelWhileInCall() throws Exception {
    final ZooStore<Manager> zooStore = new ZooStore<Manager>(ZK_ROOT + Constants.ZFATE, zk);
    final AgeOffStore<Manager> store =
        new AgeOffStore<Manager>(zooStore, 3000, System::currentTimeMillis);

    Manager manager = createMock(Manager.class);
    ServerContext sctx = createMock(ServerContext.class);
    expect(manager.getContext()).andReturn(sctx).anyTimes();
    expect(sctx.getZooKeeperRoot()).andReturn(ZK_ROOT).anyTimes();
    expect(sctx.getZooReaderWriter()).andReturn(zk).anyTimes();
    replay(manager, sctx);

    Fate<Manager> fate = new Fate<Manager>(manager, store, TraceRepo::toLogString);
    try {
      ConfigurationCopy config = new ConfigurationCopy();
      config.set(Property.GENERAL_THREADPOOL_SIZE, "2");
      config.set(Property.MANAGER_FATE_THREADPOOL_SIZE, "1");
      fate.startTransactionRunners(config);

      // Wait for the transaction runner to be scheduled.
      UtilWaitThread.sleep(3000);

      callStarted = new CountDownLatch(1);
      finishCall = new CountDownLatch(1);

      long txid = fate.startTransaction();
      LOG.debug("Starting test testCancelWhileInCall with {}", FateTxId.formatTid(txid));
      assertEquals(NEW, getTxStatus(zk, txid));
      fate.seedTransaction("TestOperation", txid, new TestOperation(NS, TID), true, "Test Op");
      assertEquals(SUBMITTED, getTxStatus(zk, txid));
      // wait for call() to be called
      callStarted.await();
      // cancel the transaction
      assertFalse(fate.cancel(txid));
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
    zrw.sync(ZK_ROOT);
    String txdir = String.format("%s%s/tx_%016x", ZK_ROOT, Constants.ZFATE, txid);
    return TStatus.valueOf(new String(zrw.getData(txdir), UTF_8));
  }

}

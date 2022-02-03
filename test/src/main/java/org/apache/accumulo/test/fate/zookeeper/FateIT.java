/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
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
import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.util.UUID;
import java.util.concurrent.CountDownLatch;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.clientImpl.thrift.TableOperation;
import org.apache.accumulo.core.conf.ConfigurationCopy;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.NamespaceId;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.fate.AgeOffStore;
import org.apache.accumulo.fate.Fate;
import org.apache.accumulo.fate.ReadOnlyTStore.TStatus;
import org.apache.accumulo.fate.Repo;
import org.apache.accumulo.fate.ZooStore;
import org.apache.accumulo.fate.util.UtilWaitThread;
import org.apache.accumulo.fate.zookeeper.ZooReaderWriter;
import org.apache.accumulo.manager.Manager;
import org.apache.accumulo.manager.tableOps.ManagerRepo;
import org.apache.accumulo.manager.tableOps.TraceRepo;
import org.apache.accumulo.manager.tableOps.Utils;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.test.categories.ZooKeeperTestingServerTests;
import org.apache.accumulo.test.zookeeper.ZooKeeperTestingServer;
import org.apache.zookeeper.KeeperException;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ZooKeeperTestingServerTests.class})
public class FateIT {

  public static class TestOperation extends ManagerRepo {

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
    public Repo<Manager> call(long tid, Manager environment) throws Exception {
      FateIT.inCall();
      return null;
    }

  }

  private static ZooKeeperTestingServer szk = null;
  private static final String ZK_ROOT = "/accumulo/" + UUID.randomUUID().toString();
  private static final NamespaceId NS = NamespaceId.of("testNameSpace");
  private static final TableId TID = TableId.of("testTable");

  private static CountDownLatch callStarted;
  private static CountDownLatch finishCall;

  @BeforeClass
  public static void setup() throws Exception {
    szk = new ZooKeeperTestingServer();
  }

  @AfterClass
  public static void teardown() throws Exception {
    szk.close();
  }

  @Test(timeout = 30000)
  public void testTransactionStatus() throws Exception {
    ZooReaderWriter zk = new ZooReaderWriter(szk.getConn(), 30000, "secret");

    zk.mkdirs(ZK_ROOT + Constants.ZFATE);
    zk.mkdirs(ZK_ROOT + Constants.ZTABLE_LOCKS);
    zk.mkdirs(ZK_ROOT + Constants.ZNAMESPACES + "/" + NS.canonical());
    zk.mkdirs(ZK_ROOT + Constants.ZTABLE_STATE + "/" + TID.canonical());
    zk.mkdirs(ZK_ROOT + Constants.ZTABLES + "/" + TID.canonical());

    ZooStore<Manager> zooStore = new ZooStore<Manager>(ZK_ROOT + Constants.ZFATE, zk);
    final AgeOffStore<Manager> store = new AgeOffStore<Manager>(zooStore, 1000 * 60 * 60 * 8);

    Manager manager = createMock(Manager.class);
    ServerContext sctx = createMock(ServerContext.class);
    expect(manager.getContext()).andReturn(sctx).anyTimes();
    expect(sctx.getZooKeeperRoot()).andReturn(ZK_ROOT).anyTimes();
    expect(sctx.getZooReaderWriter()).andReturn(zk).anyTimes();
    replay(manager, sctx);

    Fate<Manager> fate = new Fate<Manager>(manager, store, TraceRepo::toLogString);
    try {
      ConfigurationCopy config = new ConfigurationCopy();
      config.set(Property.GENERAL_SIMPLETIMER_THREADPOOL_SIZE, "2");
      config.set(Property.MANAGER_FATE_THREADPOOL_SIZE, "1");
      fate.startTransactionRunners(config);

      // Wait for the transaction runner to be scheduled.
      UtilWaitThread.sleep(3000);

      callStarted = new CountDownLatch(1);
      finishCall = new CountDownLatch(1);

      long txid = fate.startTransaction();
      assertEquals(TStatus.NEW, getTxStatus(zk, txid));
      fate.seedTransaction(txid, new TestOperation(NS, TID), true);
      assertEquals(TStatus.SUBMITTED, getTxStatus(zk, txid));
      // wait for call() to be called
      callStarted.await();
      assertEquals(TStatus.IN_PROGRESS, getTxStatus(zk, txid));
      // tell the op to exit the method
      finishCall.countDown();
      // Check that it transitions to SUCCESSFUL
      TStatus s = getTxStatus(zk, txid);
      while (!s.equals(TStatus.SUCCESSFUL)) {
        s = getTxStatus(zk, txid);
      }
      // Check that it gets removed
      boolean errorSeen = false;
      while (!errorSeen) {
        try {
          s = getTxStatus(zk, txid);
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

  public static void inCall() throws InterruptedException {
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
    String txdir = String.format("%s/tx_%016x", ZK_ROOT + Constants.ZFATE, txid);
    return TStatus.valueOf(new String(zrw.getData(txdir), UTF_8));
  }

}

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

import java.util.UUID;

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

  public static Boolean DELETE_NOW_OP_CALLED = false;

  public static class LongRenameOp extends ManagerRepo {

    private static final long serialVersionUID = 1L;

    private final TableId tableId;
    private final NamespaceId namespaceId;

    public LongRenameOp(NamespaceId namespaceId, TableId tableId) {
      this.namespaceId = namespaceId;
      this.tableId = tableId;
    }

    @Override
    public long isReady(long tid, Manager manager) throws Exception {
      return Utils.reserveNamespace(manager, namespaceId, tid, false, true, TableOperation.RENAME)
          + Utils.reserveTable(manager, tableId, tid, true, true, TableOperation.RENAME, false);
    }

    @Override
    public void undo(long tid, Manager manager) throws Exception {
      Utils.unreserveNamespace(manager, namespaceId, tid, false);
      Utils.unreserveTable(manager, tableId, tid, true);
    }

    @Override
    public Repo<Manager> call(long tid, Manager environment) throws Exception {
      Thread.sleep(3 * 60 * 1000);
      return null;
    }

  }

  public static class DeleteTableNowOp extends ManagerRepo {

    private static final long serialVersionUID = 1L;

    private final TableId tableId;
    private final NamespaceId namespaceId;

    public DeleteTableNowOp(NamespaceId namespaceId, TableId tableId) {
      this.namespaceId = namespaceId;
      this.tableId = tableId;
    }

    @Override
    public long isReady(long tid, Manager manager) throws Exception {
      return Utils.reserveNamespace(manager, namespaceId, tid, false, true, TableOperation.RENAME)
          + Utils.reserveTable(manager, tableId, tid, true, true, TableOperation.RENAME, true);
    }

    @Override
    public void undo(long tid, Manager manager) throws Exception {
      Utils.unreserveNamespace(manager, namespaceId, tid, false);
      Utils.unreserveTable(manager, tableId, tid, true);
    }

    @Override
    public Repo<Manager> call(long tid, Manager environment) throws Exception {
      FateIT.DELETE_NOW_OP_CALLED = true;
      return null;
    }

  }

  private static ZooKeeperTestingServer szk = null;
  private static final String ZK_ROOT = "/accumulo/" + UUID.randomUUID().toString();
  private static final NamespaceId NS = NamespaceId.of("testNameSpace");
  private static final TableId TID = TableId.of("testTable");

  @BeforeClass
  public static void setup() throws Exception {
    szk = new ZooKeeperTestingServer();
  }

  @AfterClass
  public static void teardown() throws Exception {
    szk.close();
  }

  @Test
  public void testPreemption() throws Exception {
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
    expect(manager.getZooStore()).andReturn(zooStore).anyTimes();
    replay(manager, sctx);

    Fate<Manager> fate = new Fate<Manager>(manager, store, TraceRepo::toLogString);
    try {
      ConfigurationCopy config = new ConfigurationCopy();
      config.set(Property.GENERAL_SIMPLETIMER_THREADPOOL_SIZE, "2");
      config.set(Property.MANAGER_FATE_THREADPOOL_SIZE, "2");
      fate.startTransactionRunners(config);

      // Wait for the transaction runner to be scheduled.
      UtilWaitThread.sleep(3000);

      long txid = fate.startTransaction();
      fate.seedTransaction(txid, new LongRenameOp(NS, TID), true);
      Thread.sleep(100);
      assertEquals(TStatus.IN_PROGRESS, getTxStatus(zk, txid));

      long txid2 = fate.startTransaction();
      fate.seedTransaction(txid2, new DeleteTableNowOp(NS, TID), true);

      while (!FateIT.DELETE_NOW_OP_CALLED) {
        Thread.sleep(100);
      }
    } finally {
      fate.shutdown();
    }
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

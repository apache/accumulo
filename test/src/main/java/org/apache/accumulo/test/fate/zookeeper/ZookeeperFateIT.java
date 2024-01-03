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
import static org.apache.accumulo.harness.AccumuloITBase.ZOOKEEPER_TESTING_SERVER;
import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.File;
import java.util.UUID;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.fate.AgeOffStore;
import org.apache.accumulo.core.fate.ReadOnlyFateStore.TStatus;
import org.apache.accumulo.core.fate.ZooStore;
import org.apache.accumulo.core.fate.zookeeper.ZooReaderWriter;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.test.fate.FateIT;
import org.apache.accumulo.test.zookeeper.ZooKeeperTestingServer;
import org.apache.zookeeper.KeeperException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.io.TempDir;

@Tag(ZOOKEEPER_TESTING_SERVER)
public class ZookeeperFateIT extends FateIT {

  private static ZooKeeperTestingServer szk = null;
  private static ZooReaderWriter zk = null;
  private static final String ZK_ROOT = "/accumulo/" + UUID.randomUUID();

  @TempDir
  private static File tempDir;

  @BeforeAll
  public static void setup() throws Exception {
    szk = new ZooKeeperTestingServer(tempDir);
    zk = szk.getZooReaderWriter();
    zk.mkdirs(ZK_ROOT + Constants.ZFATE);
    zk.mkdirs(ZK_ROOT + Constants.ZTABLE_LOCKS);
  }

  @AfterAll
  public static void teardown() throws Exception {
    szk.close();
  }

  @Override
  protected void executeTest(FateTestExecutor testMethod) throws Exception {
    final ZooStore<TestEnv> zooStore = new ZooStore<>(ZK_ROOT + Constants.ZFATE, zk);
    final AgeOffStore<TestEnv> store = new AgeOffStore<>(zooStore, 3000, System::currentTimeMillis);

    ServerContext sctx = createMock(ServerContext.class);
    expect(sctx.getZooKeeperRoot()).andReturn(ZK_ROOT).anyTimes();
    expect(sctx.getZooReaderWriter()).andReturn(zk).anyTimes();
    replay(sctx);

    testMethod.execute(store, sctx);
  }

  @Override
  protected Class<? extends Exception> getNoTxExistsException() {
    return KeeperException.NoNodeException.class;
  }

  @Override
  protected TStatus getTxStatus(ServerContext sctx, long txid)
      throws InterruptedException, KeeperException {
    return getTxStatus(sctx.getZooReaderWriter(), txid);
  }

  @Override
  protected boolean verifyRemoved(ServerContext sctx, long txid) {
    try {
      getTxStatus(sctx, txid);
    } catch (KeeperException e) {
      if (e.code() == KeeperException.Code.NONODE) {
        return true;
      } else {
        fail("Unexpected error thrown: " + e.getMessage());
      }
    } catch (InterruptedException e) {
      throw new IllegalStateException(e);
    }
    return false;
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

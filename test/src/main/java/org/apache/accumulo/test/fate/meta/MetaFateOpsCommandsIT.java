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
package org.apache.accumulo.test.fate.meta;

import java.util.function.Predicate;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.fate.AbstractFateStore;
import org.apache.accumulo.core.fate.FateStore;
import org.apache.accumulo.core.fate.zookeeper.MetaFateStore;
import org.apache.accumulo.core.fate.zookeeper.ZooReaderWriter;
import org.apache.accumulo.core.fate.zookeeper.ZooUtil;
import org.apache.accumulo.core.lock.ServiceLock;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.test.fate.FateOpsCommandsIT;
import org.apache.accumulo.test.fate.MultipleStoresIT.LatchTestEnv;
import org.apache.accumulo.test.fate.TestLock;

public class MetaFateOpsCommandsIT extends FateOpsCommandsIT {
  /**
   * This should be used for tests that will not seed a txn with work/reserve a txn. Note that this
   * should be used in conjunction with
   * {@link FateOpsCommandsIT#initFateNoDeadResCleaner(FateStore)}
   */
  @Override
  public void executeTest(FateTestExecutor<LatchTestEnv> testMethod, int maxDeferred,
      AbstractFateStore.FateIdGenerator fateIdGenerator) throws Exception {
    ServerContext context = getCluster().getServerContext();
    String path = context.getZooKeeperRoot() + Constants.ZFATE;
    ZooReaderWriter zk = context.getZooReaderWriter();
    // test should not be reserving txns or checking reservations, so null lockID and isLockHeld
    testMethod.execute(new MetaFateStore<>(path, zk, null, null), context);
  }

  /**
   * This should be used for tests that will seed a txn with work/reserve a txn. Note that this
   * should be used in conjunction with
   * {@link FateOpsCommandsIT#initFateWithDeadResCleaner(FateStore, LatchTestEnv)}
   */
  @Override
  public void stopManagerAndExecuteTest(FateTestExecutor<LatchTestEnv> testMethod)
      throws Exception {
    stopManager();
    ServerContext context = getCluster().getServerContext();
    String path = context.getZooKeeperRoot() + Constants.ZFATE;
    ZooReaderWriter zk = context.getZooReaderWriter();
    ServiceLock testLock = null;
    try {
      testLock = new TestLock().createTestLock(context);
      ZooUtil.LockID lockID = testLock.getLockID();
      Predicate<ZooUtil.LockID> isLockHeld =
          lock -> ServiceLock.isLockHeld(context.getZooCache(), lock);
      testMethod.execute(new MetaFateStore<>(path, zk, lockID, isLockHeld), context);
    } finally {
      if (testLock != null) {
        testLock.unlock();
      }
    }
  }
}

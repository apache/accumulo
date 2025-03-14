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

import static org.apache.accumulo.test.fate.TestLock.createDummyLockID;

import java.util.UUID;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.data.InstanceId;
import org.apache.accumulo.core.fate.AbstractFateStore;
import org.apache.accumulo.core.fate.zookeeper.MetaFateStore;
import org.apache.accumulo.core.fate.zookeeper.ZooUtil;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.test.fate.FateExecutionOrderIT;

public class MetaFateExecutionOrderIT extends FateExecutionOrderIT {

  // put the fate data for the test in a different location than what accumulo is using
  private static final InstanceId IID = InstanceId.of(UUID.randomUUID());
  private static final String ZK_ROOT = ZooUtil.getRoot(IID);

  @Override
  public void executeTest(FateTestExecutor<FeoTestEnv> testMethod, int maxDeferred,
      AbstractFateStore.FateIdGenerator fateIdGenerator) throws Exception {
    ServerContext sctx = getCluster().getServerContext();
    String path = ZK_ROOT + Constants.ZFATE;
    var zk = sctx.getZooSession();
    zk.asReaderWriter().mkdirs(ZK_ROOT);
    testMethod.execute(new MetaFateStore<>(path, zk, createDummyLockID(), null), sctx);
  }
}

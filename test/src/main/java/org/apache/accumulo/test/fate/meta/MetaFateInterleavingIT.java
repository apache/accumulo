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

import java.util.UUID;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.fate.AbstractFateStore;
import org.apache.accumulo.core.fate.MetaFateStore;
import org.apache.accumulo.core.fate.zookeeper.ZooReaderWriter;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.test.fate.FateInterleavingIT;

public class MetaFateInterleavingIT extends FateInterleavingIT {

  // put the fate data for the test in a different location than what accumulo is using
  private static final String ZK_ROOT = "/accumulo/" + UUID.randomUUID();

  @Override
  public void executeTest(FateTestExecutor<FilTestEnv> testMethod, int maxDeferred,
      AbstractFateStore.FateIdGenerator fateIdGenerator) throws Exception {
    ServerContext sctx = getCluster().getServerContext();
    String path = ZK_ROOT + Constants.ZFATE;
    ZooReaderWriter zk = sctx.getZooReaderWriter();
    zk.mkdirs(ZK_ROOT);
    testMethod.execute(new MetaFateStore<>(path, zk), sctx);
  }
}

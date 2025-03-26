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
import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;

import java.io.File;

import org.apache.accumulo.core.fate.AbstractFateStore;
import org.apache.accumulo.core.fate.zookeeper.MetaFateStore;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.test.fate.FatePoolsWatcherIT;
import org.apache.accumulo.test.fate.FateTestUtil;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.io.TempDir;

public class MetaFatePoolsWatcherIT extends FatePoolsWatcherIT {
  @TempDir
  private static File tempDir;

  @BeforeAll
  public static void setup() throws Exception {
    FateTestUtil.MetaFateZKSetup.setup(tempDir);
  }

  @AfterAll
  public static void teardown() throws Exception {
    FateTestUtil.MetaFateZKSetup.teardown();
  }

  @Override
  public void executeTest(FateTestExecutor<PoolResizeTestEnv> testMethod, int maxDeferred,
      AbstractFateStore.FateIdGenerator fateIdGenerator) throws Exception {
    var zk = FateTestUtil.MetaFateZKSetup.getZk();
    ServerContext sctx = createMock(ServerContext.class);
    expect(sctx.getZooSession()).andReturn(zk).anyTimes();
    replay(sctx);

    testMethod.execute(
        new MetaFateStore<>(zk, createDummyLockID(), null, maxDeferred, fateIdGenerator), sctx);
  }
}

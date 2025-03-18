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

import java.io.File;
import java.util.function.Predicate;

import org.apache.accumulo.core.fate.FateStore;
import org.apache.accumulo.core.fate.zookeeper.MetaFateStore;
import org.apache.accumulo.core.fate.zookeeper.ZooUtil;
import org.apache.accumulo.test.fate.FateStoreUtil;
import org.apache.accumulo.test.fate.MultipleStoresIT;
import org.apache.zookeeper.KeeperException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.io.TempDir;

public class MetaMultipleStoresIT extends MultipleStoresIT {
  @TempDir
  private static File tempDir;

  @BeforeAll
  public static void setup() throws Exception {
    FateStoreUtil.MetaFateZKSetup.setup(tempDir);
  }

  @AfterAll
  public static void teardown() throws Exception {
    FateStoreUtil.MetaFateZKSetup.teardown();
  }

  @Override
  protected void executeSleepingEnvTest(MultipleStoresTestExecutor<SleepingTestEnv> testMethod)
      throws Exception {
    testMethod.execute(new SleepingEnvMetaStoreFactory());
  }

  @Override
  protected void executeLatchEnvTest(MultipleStoresTestExecutor<LatchTestEnv> testMethod)
      throws Exception {
    testMethod.execute(new LatchEnvMetaStoreFactory());
  }

  static class SleepingEnvMetaStoreFactory implements TestStoreFactory<SleepingTestEnv> {
    @Override
    public FateStore<SleepingTestEnv> create(ZooUtil.LockID lockID,
        Predicate<ZooUtil.LockID> isLockHeld) throws InterruptedException, KeeperException {
      return new MetaFateStore<>(FateStoreUtil.MetaFateZKSetup.getZk(), lockID, isLockHeld);
    }
  }

  static class LatchEnvMetaStoreFactory implements TestStoreFactory<LatchTestEnv> {
    @Override
    public FateStore<LatchTestEnv> create(ZooUtil.LockID lockID,
        Predicate<ZooUtil.LockID> isLockHeld) throws InterruptedException, KeeperException {
      return new MetaFateStore<>(FateStoreUtil.MetaFateZKSetup.getZk(), lockID, isLockHeld);
    }
  }
}

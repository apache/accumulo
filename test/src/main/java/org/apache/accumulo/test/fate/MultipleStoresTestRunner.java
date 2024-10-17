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
package org.apache.accumulo.test.fate;

import java.util.function.Predicate;

import org.apache.accumulo.core.fate.FateStore;
import org.apache.accumulo.core.fate.zookeeper.ZooUtil;
import org.apache.zookeeper.KeeperException;

public interface MultipleStoresTestRunner {

  void executeSleepingEnvTest(
      MultipleStoresTestExecutor<MultipleStoresIT.SleepingTestEnv> testMethod) throws Exception;

  void executeLatchEnvTest(MultipleStoresTestExecutor<MultipleStoresIT.LatchTestEnv> testMethod)
      throws Exception;

  interface TestStoreFactory<T extends MultipleStoresTestEnv> {
    FateStore<T> create(ZooUtil.LockID lockID, Predicate<ZooUtil.LockID> isLockHeld)
        throws InterruptedException, KeeperException;
  }

  @FunctionalInterface
  interface MultipleStoresTestExecutor<T extends MultipleStoresTestEnv> {
    void execute(TestStoreFactory<T> fateStoreFactory) throws Exception;
  }

  class MultipleStoresTestEnv {}
}

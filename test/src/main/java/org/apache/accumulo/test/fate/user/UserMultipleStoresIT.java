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
package org.apache.accumulo.test.fate.user;

import static org.apache.accumulo.test.fate.user.UserFateStoreIT.createFateTable;

import java.util.function.Predicate;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.clientImpl.ClientContext;
import org.apache.accumulo.core.fate.FateStore;
import org.apache.accumulo.core.fate.user.UserFateStore;
import org.apache.accumulo.core.fate.zookeeper.ZooUtil;
import org.apache.accumulo.harness.SharedMiniClusterBase;
import org.apache.accumulo.test.fate.MultipleStoresIT;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;

public class UserMultipleStoresIT extends MultipleStoresIT {
  private static ClientContext CLIENT;
  private static String TABLE_NAME;

  @BeforeAll
  public static void beforeAllSetup() throws Exception {
    SharedMiniClusterBase.startMiniCluster();
  }

  @BeforeEach
  public void beforeEachSetup() throws Exception {
    TABLE_NAME = getUniqueNames(1)[0];
    CLIENT = (ClientContext) Accumulo.newClient().from(getClientProps()).build();
    createFateTable(CLIENT, TABLE_NAME);
  }

  @AfterAll
  public static void afterAllTeardown() {
    SharedMiniClusterBase.stopMiniCluster();
  }

  @AfterEach
  public void afterEachTeardown()
      throws AccumuloException, TableNotFoundException, AccumuloSecurityException {
    CLIENT.tableOperations().delete(TABLE_NAME);
    CLIENT.close();
  }

  @Override
  public void executeSleepingEnvTest(MultipleStoresTestExecutor<SleepingTestEnv> testMethod)
      throws Exception {
    testMethod.execute(new SleepingEnvUserStoreFactory());
  }

  @Override
  public void executeLatchEnvTest(MultipleStoresTestExecutor<LatchTestEnv> testMethod)
      throws Exception {
    testMethod.execute(new LatchEnvUserStoreFactory());
  }

  static class SleepingEnvUserStoreFactory implements TestStoreFactory<SleepingTestEnv> {
    @Override
    public FateStore<SleepingTestEnv> create(ZooUtil.LockID lockID,
        Predicate<ZooUtil.LockID> isLockHeld) {
      return new UserFateStore<>(CLIENT, TABLE_NAME, lockID, isLockHeld);
    }
  }

  static class LatchEnvUserStoreFactory implements TestStoreFactory<LatchTestEnv> {
    @Override
    public FateStore<LatchTestEnv> create(ZooUtil.LockID lockID,
        Predicate<ZooUtil.LockID> isLockHeld) {
      return new UserFateStore<>(CLIENT, TABLE_NAME, lockID, isLockHeld);
    }
  }
}

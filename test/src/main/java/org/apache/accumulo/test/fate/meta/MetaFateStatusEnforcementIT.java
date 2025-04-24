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

import java.io.File;

import org.apache.accumulo.core.fate.zookeeper.MetaFateStore;
import org.apache.accumulo.test.fate.FateStatusEnforcementIT;
import org.apache.accumulo.test.fate.FateTestUtil;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.io.TempDir;

public class MetaFateStatusEnforcementIT extends FateStatusEnforcementIT {
  @TempDir
  private static File tempDir;

  @BeforeAll
  public static void beforeAllSetup() throws Exception {
    FateTestUtil.MetaFateZKSetup.setup(tempDir);
  }

  @AfterAll
  public static void afterAllTeardown() throws Exception {
    FateTestUtil.MetaFateZKSetup.teardown();
  }

  @BeforeEach
  public void beforeEachSetup() throws Exception {
    store = new MetaFateStore<>(FateTestUtil.MetaFateZKSetup.getZk(), createDummyLockID(), null);
    fateId = store.create();
    txStore = store.reserve(fateId);
  }
}

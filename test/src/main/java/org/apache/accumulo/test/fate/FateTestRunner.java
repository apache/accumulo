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

import org.apache.accumulo.core.fate.AbstractFateStore;
import org.apache.accumulo.core.fate.AbstractFateStore.FateIdGenerator;
import org.apache.accumulo.core.fate.FateStore;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.test.fate.FateTestRunner.TestEnv;

public interface FateTestRunner<T extends TestEnv> {

  void executeTest(FateTestExecutor<T> testMethod, int maxDeferred, FateIdGenerator fateIdGenerator)
      throws Exception;

  default void executeTest(FateTestExecutor<T> testMethod) throws Exception {
    executeTest(testMethod, AbstractFateStore.DEFAULT_MAX_DEFERRED,
        AbstractFateStore.DEFAULT_FATE_ID_GENERATOR);
  }

  interface FateTestExecutor<T> {
    void execute(FateStore<T> store, ServerContext sctx) throws Exception;
  }

  class TestEnv {

  }
}

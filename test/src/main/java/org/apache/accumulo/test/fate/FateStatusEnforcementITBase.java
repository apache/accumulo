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

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Set;

import org.apache.accumulo.core.fate.AbstractFateStore;
import org.apache.accumulo.core.fate.FateId;
import org.apache.accumulo.core.fate.FateStore;
import org.apache.accumulo.core.fate.ReadOnlyFateStore;
import org.apache.accumulo.harness.SharedMiniClusterBase;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;

public abstract class FateStatusEnforcementITBase extends SharedMiniClusterBase {

  protected FateId fateId;
  protected FateStore<FateTestRunner.TestEnv> store;
  protected FateStore.FateTxStore<FateITBase.TestEnv> txStore;

  @Test
  public void push() throws Exception {
    testOperationWithStatuses(() -> {}, // No special setup needed for push
        () -> txStore.push(new FateITBase.TestRepo("testOp")), AbstractFateStore.REQ_PUSH_STATUS);
  }

  @Test
  public void pop() throws Exception {
    testOperationWithStatuses(() -> {
      // Setup for pop: Ensure there something to pop by first pushing
      try {
        txStore.setStatus(ReadOnlyFateStore.TStatus.NEW);
        txStore.push(new FateITBase.TestRepo("testOp"));
      } catch (Exception e) {
        throw new RuntimeException("Failed to setup for pop", e);
      }
    }, txStore::pop, AbstractFateStore.REQ_POP_STATUS);
  }

  @Test
  public void delete() throws Exception {
    testOperationWithStatuses(() -> {
      // Setup for delete: Create a new txStore before each delete since delete cannot be called
      // on the same txStore more than once
      fateId = store.create();
      txStore = store.reserve(fateId);
    }, () -> txStore.delete(), AbstractFateStore.REQ_DELETE_STATUS);
  }

  @Test
  public void forceDelete() throws Exception {
    testOperationWithStatuses(() -> {
      // Setup for forceDelete: same as delete
      fateId = store.create();
      txStore = store.reserve(fateId);
    }, () -> txStore.forceDelete(), AbstractFateStore.REQ_FORCE_DELETE_STATUS);
  }

  protected void testOperationWithStatuses(Runnable beforeOperation, Executable operation,
      Set<ReadOnlyFateStore.TStatus> acceptableStatuses) throws Exception {
    for (ReadOnlyFateStore.TStatus status : ReadOnlyFateStore.TStatus.values()) {
      // Run any needed setup for the operation before each iteration
      beforeOperation.run();

      txStore.setStatus(status);
      var fateIdStatus = store.list().filter(statusEntry -> statusEntry.getFateId().equals(fateId))
          .findFirst().orElseThrow();
      assertEquals(status, fateIdStatus.getStatus());
      if (!acceptableStatuses.contains(status)) {
        assertThrows(IllegalStateException.class, operation,
            "Expected operation to fail with status " + status + " but it did not");
      } else {
        assertDoesNotThrow(operation,
            "Expected operation to succeed with status " + status + " but it did not");
      }
    }
  }
}

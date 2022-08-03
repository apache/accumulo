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
package org.apache.accumulo.core.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Function;
import java.util.function.Predicate;

import org.junit.jupiter.api.Test;

public class CompletableFutureUtilTest {
  @Test
  public void testMerge() throws Exception {
    ExecutorService es = Executors.newFixedThreadPool(3);
    try {
      for (int n : new int[] {1, 2, 3, 997, 1000}) {
        List<CompletableFuture<Integer>> futures = new ArrayList<>();
        for (int i = 1; i <= n; i++) {
          final int num = i;
          futures.add(CompletableFuture.supplyAsync(() -> num, es));
        }

        CompletableFuture<Integer> mergedFutures =
            CompletableFutureUtil.merge(futures, Integer::sum, () -> 0);
        assertEquals(n * (n + 1) / 2, mergedFutures.get().intValue());
      }

      // test zero
      CompletableFuture<Integer> mergedFutures =
          CompletableFutureUtil.merge(Collections.emptyList(), Integer::sum, () -> 0);
      assertEquals(0, mergedFutures.get().intValue());
    } finally {
      es.shutdown();
    }
  }

  @Test
  public void testIterateUntil() throws Exception {
    ExecutorService es = Executors.newFixedThreadPool(1);
    Function<Integer,CompletableFuture<Integer>> step =
        n -> CompletableFuture.supplyAsync(() -> n - 1, es);
    Predicate<Integer> isDone = n -> n == 0;
    // The call stack should overflow before 10,000 calls, so this
    // effectively tests whether iterateUntil avoids stack overflows
    // when given async futures.
    for (int n : new int[] {0, 1, 2, 3, 100, 10_000}) {
      assertEquals(0, CompletableFutureUtil.iterateUntil(step, isDone, n).get());
    }
    // Test throwing an exception in the step function.
    {
      Function<Integer,CompletableFuture<Integer>> badStep = n -> {
        throw new RuntimeException();
      };
      assertThrows(ExecutionException.class,
          () -> CompletableFutureUtil.iterateUntil(badStep, isDone, 100).get());
    }
    // Test throwing an exception in the future returned by the step
    // function.
    {
      Function<Integer,CompletableFuture<Integer>> badStep =
          n -> CompletableFuture.supplyAsync(() -> {
            throw new RuntimeException();
          }, es);
      assertThrows(ExecutionException.class,
          () -> CompletableFutureUtil.iterateUntil(badStep, isDone, 100).get());
    }
    // Test throwing an exception in the predicate.
    {
      Predicate<Integer> badIsDone = n -> {
        throw new RuntimeException();
      };
      assertThrows(ExecutionException.class,
          () -> CompletableFutureUtil.iterateUntil(step, badIsDone, 100).get());
    }
  }
}

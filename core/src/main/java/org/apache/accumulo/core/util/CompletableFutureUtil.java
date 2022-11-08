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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;

public class CompletableFutureUtil {

  // create a binary tree of completable future operations, where each node in the tree merges the
  // results of their children when complete
  public static <T> CompletableFuture<T> merge(List<CompletableFuture<T>> futures,
      BiFunction<T,T,T> mergeFunc, Supplier<T> nothing) {
    if (futures.isEmpty()) {
      return CompletableFuture.completedFuture(nothing.get());
    }
    while (futures.size() > 1) {
      ArrayList<CompletableFuture<T>> mergedFutures = new ArrayList<>(futures.size() / 2);
      for (int i = 0; i < futures.size(); i += 2) {
        if (i + 1 == futures.size()) {
          mergedFutures.add(futures.get(i));
        } else {
          mergedFutures.add(futures.get(i).thenCombine(futures.get(i + 1), mergeFunc));
        }
      }

      futures = mergedFutures;
    }

    return futures.get(0);
  }

  /**
   * Iterate some function until a given condition is met.
   *
   * The step function should always return an asynchronous {@code
   * CompletableFuture} in order to avoid stack overflows.
   */
  public static <T> CompletableFuture<T> iterateUntil(Function<T,CompletableFuture<T>> step,
      Predicate<T> isDone, T init) {
    // We'd like to use a lambda here, but lambdas don't have
    // `this`, so we would have to use some clumsy indirection to
    // achieve self-reference.
    Function<T,CompletableFuture<T>> go = new Function<>() {
      @Override
      public CompletableFuture<T> apply(T x) {
        if (isDone.test(x)) {
          return CompletableFuture.completedFuture(x);
        }
        return step.apply(x).thenCompose(this);
      }
    };
    return CompletableFuture.completedFuture(init).thenCompose(go);
  }
}

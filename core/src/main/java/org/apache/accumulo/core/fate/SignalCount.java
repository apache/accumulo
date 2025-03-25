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
package org.apache.accumulo.core.fate;

import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import java.util.function.LongPredicate;

import com.google.common.base.Preconditions;

class SignalCount {
  private long count = 0;

  synchronized void increment() {
    count++;
    this.notifyAll();
  }

  synchronized void decrement() {
    Preconditions.checkState(count > 0);
    count--;
    this.notifyAll();
  }

  synchronized long getCount() {
    return count;
  }

  synchronized boolean waitFor(LongPredicate predicate, BooleanSupplier keepWaiting) {
    return waitFor(predicate, Long.MAX_VALUE, keepWaiting);
  }

  synchronized boolean waitFor(LongPredicate predicate, long maxWait, BooleanSupplier keepWaiting) {
    Preconditions.checkArgument(maxWait >= 0);

    if (maxWait == 0) {
      return predicate.test(count);
    }

    long start = System.nanoTime();

    while (!predicate.test(count) && keepWaiting.getAsBoolean()
        && TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start) < maxWait) {
      try {
        wait(100);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new IllegalStateException(e);
      }
    }

    return predicate.test(count);
  }
}

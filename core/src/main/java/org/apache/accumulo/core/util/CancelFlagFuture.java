/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.accumulo.core.util;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A simple future wrapper that will set an atomic boolean to true if a future is successfully canceled
 */
public class CancelFlagFuture<T> implements Future<T> {

  private Future<T> wrappedFuture;
  private AtomicBoolean cancelFlag;

  public CancelFlagFuture(Future<T> wrappedFuture, AtomicBoolean cancelFlag) {
    this.wrappedFuture = wrappedFuture;
    this.cancelFlag = cancelFlag;
  }

  @Override
  public boolean cancel(boolean mayInterruptIfRunning) {
    boolean ret = wrappedFuture.cancel(mayInterruptIfRunning);
    if (ret) {
      cancelFlag.set(true);
    }
    return ret;
  }

  @Override
  public boolean isCancelled() {
    return wrappedFuture.isCancelled();
  }

  @Override
  public boolean isDone() {
    return wrappedFuture.isDone();
  }

  @Override
  public T get() throws InterruptedException, ExecutionException {
    return wrappedFuture.get();
  }

  @Override
  public T get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
    return wrappedFuture.get(timeout, unit);
  }
}

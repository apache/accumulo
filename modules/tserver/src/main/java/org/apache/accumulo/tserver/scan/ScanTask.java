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
package org.apache.accumulo.tserver.scan;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.RunnableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.accumulo.tserver.TabletServer;

public abstract class ScanTask<T> implements RunnableFuture<T> {

  protected final TabletServer server;
  protected AtomicBoolean interruptFlag;
  protected ArrayBlockingQueue<Object> resultQueue;
  protected AtomicInteger state;
  protected AtomicReference<ScanRunState> runState;

  private static final int INITIAL = 1;
  private static final int ADDED = 2;
  private static final int CANCELED = 3;

  ScanTask(TabletServer server) {
    this.server = server;
    interruptFlag = new AtomicBoolean(false);
    runState = new AtomicReference<>(ScanRunState.QUEUED);
    state = new AtomicInteger(INITIAL);
    resultQueue = new ArrayBlockingQueue<>(1);
  }

  protected void addResult(Object o) {
    if (state.compareAndSet(INITIAL, ADDED))
      resultQueue.add(o);
    else if (state.get() == ADDED)
      throw new IllegalStateException("Tried to add more than one result");
  }

  @Override
  public boolean cancel(boolean mayInterruptIfRunning) {
    if (!mayInterruptIfRunning)
      throw new IllegalArgumentException("Cancel will always attempt to interupt running next batch task");

    if (state.get() == CANCELED)
      return true;

    if (state.compareAndSet(INITIAL, CANCELED)) {
      interruptFlag.set(true);
      resultQueue = null;
      return true;
    }

    return false;
  }

  @Override
  public T get() throws InterruptedException, ExecutionException {
    throw new UnsupportedOperationException();
  }

  @Override
  public T get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {

    ArrayBlockingQueue<Object> localRQ = resultQueue;

    if (isCancelled())
      throw new CancellationException();

    if (localRQ == null) {
      int st = state.get();
      String stateStr;
      switch (st) {
        case ADDED:
          stateStr = "ADDED";
          break;
        case CANCELED:
          stateStr = "CANCELED";
          break;
        case INITIAL:
          stateStr = "INITIAL";
          break;
        default:
          stateStr = "UNKNOWN";
          break;
      }
      throw new IllegalStateException("Tried to get result twice [state=" + stateStr + "(" + st + ")]");
    }

    Object r = localRQ.poll(timeout, unit);

    // could have been canceled while waiting
    if (isCancelled()) {
      if (r != null)
        throw new IllegalStateException("Nothing should have been added when in canceled state");

      throw new CancellationException();
    }

    if (r == null)
      throw new TimeoutException();

    // make this method stop working now that something is being
    // returned
    resultQueue = null;

    if (r instanceof Throwable)
      throw new ExecutionException((Throwable) r);

    @SuppressWarnings("unchecked")
    T rAsT = (T) r;
    return rAsT;
  }

  @Override
  public boolean isCancelled() {
    return state.get() == CANCELED;
  }

  @Override
  public boolean isDone() {
    return runState.get().equals(ScanRunState.FINISHED);
  }

  public ScanRunState getScanRunState() {
    return runState.get();
  }

}

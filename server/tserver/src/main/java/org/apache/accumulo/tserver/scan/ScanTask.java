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
package org.apache.accumulo.tserver.scan;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.accumulo.tserver.TabletHostingServer;

public abstract class ScanTask<T> implements Runnable {

  protected final TabletHostingServer server;
  protected AtomicBoolean interruptFlag;
  protected ArrayBlockingQueue<Object> resultQueue;
  protected AtomicInteger state;
  protected AtomicReference<ScanRunState> runState;

  private static final int INITIAL = 1;
  private static final int ADDED = 2;
  private static final int CANCELED = 3;

  ScanTask(TabletHostingServer server) {
    this.server = server;
    interruptFlag = new AtomicBoolean(false);
    runState = new AtomicReference<>(ScanRunState.QUEUED);
    state = new AtomicInteger(INITIAL);
    resultQueue = new ArrayBlockingQueue<>(1);
  }

  protected boolean transitionToRunning() {
    return runState.compareAndSet(ScanRunState.QUEUED, ScanRunState.RUNNING);
  }

  protected void addResult(Object o) {
    if (state.compareAndSet(INITIAL, ADDED)) {
      resultQueue.add(o);
    } else if (state.get() == ADDED) {
      throw new IllegalStateException("Tried to add more than one result");
    }
  }

  public boolean cancel(boolean mayInterruptIfRunning) {
    if (!mayInterruptIfRunning) {
      throw new IllegalArgumentException(
          "Cancel will always attempt to interrupt running next batch task");
    }

    if (state.get() == CANCELED) {
      return true;
    }

    if (state.compareAndSet(INITIAL, CANCELED)) {
      interruptFlag.set(true);
      resultQueue = null;
      return true;
    }

    return false;
  }

  private String stateString(int state) {
    String stateStr;
    switch (state) {
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
    return stateStr;
  }

  /**
   * @param busyTimeout when this less than 0 it has no impact. When its greater than 0 and the task
   *        is queued, then get() will sleep for the specified busyTimeout and if after sleeping its
   *        still queued it will cancel the task. This behavior allows a scan to queue a scan task
   *        and give it a short period to either start running or be canceled.
   */
  public T get(long busyTimeout, long timeout, TimeUnit unit)
      throws InterruptedException, ExecutionException, TimeoutException {
    ArrayBlockingQueue<Object> localRQ = resultQueue;

    if (isCancelled()) {
      throw new CancellationException();
    }

    if (localRQ == null) {
      int st = state.get();
      throw new IllegalStateException(
          "Tried to get result twice [state=" + stateString(st) + "(" + st + ")]");
    }

    Object r;
    if (busyTimeout > 0 && runState.get() == ScanRunState.QUEUED) {
      r = localRQ.poll(busyTimeout, unit);
      if (r == null) {
        // we did not get anything during the busy timeout, if the task has not started lets try to
        // keep it from ever starting
        if (runState.compareAndSet(ScanRunState.QUEUED, ScanRunState.FINISHED)) {
          // the task was queued and we prevented it from running so lets mark it canceled
          state.compareAndSet(INITIAL, CANCELED);
          if (state.get() != CANCELED) {
            throw new IllegalStateException(
                "Scan task is in unexpected state " + stateString(state.get()));
          }
        } else {
          // the task is either running or finished so lets try to get the result
          long waitTime = Math.max(0, timeout - busyTimeout);
          r = localRQ.poll(waitTime, unit);
        }
      }
    } else {
      r = localRQ.poll(timeout, unit);
    }

    // could have been canceled while waiting
    if (isCancelled()) {
      if (r != null) {
        throw new IllegalStateException("Nothing should have been added when in canceled state");
      }

      throw new CancellationException();
    }

    if (r == null) {
      throw new TimeoutException();
    }

    // make this method stop working now that something is being
    // returned
    resultQueue = null;

    if (r instanceof Throwable) {
      throw new ExecutionException((Throwable) r);
    }

    @SuppressWarnings("unchecked")
    T rAsT = (T) r;
    return rAsT;
  }

  public boolean isCancelled() {
    return state.get() == CANCELED;
  }

  public ScanRunState getScanRunState() {
    return runState.get();
  }

}

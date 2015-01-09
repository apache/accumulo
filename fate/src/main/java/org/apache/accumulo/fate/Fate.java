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
package org.apache.accumulo.fate;

import java.util.EnumSet;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.accumulo.fate.ReadOnlyTStore.TStatus;
import org.apache.accumulo.fate.util.Daemon;
import org.apache.accumulo.fate.util.LoggingRunnable;
import org.apache.log4j.Logger;

/**
 * Fault tolerant executor
 *
 *
 */

public class Fate<T> {

  private static final String DEBUG_PROP = "debug";
  private static final String AUTO_CLEAN_PROP = "autoClean";
  private static final String EXCEPTION_PROP = "exception";
  private static final String RETURN_PROP = "return";

  final private static Logger log = Logger.getLogger(Fate.class);

  private TStore<T> store;
  private T environment;

  private static final EnumSet<TStatus> FINISHED_STATES = EnumSet.of(TStatus.FAILED, TStatus.SUCCESSFUL, TStatus.UNKNOWN);

  private AtomicBoolean keepRunning = new AtomicBoolean(true);

  private class TransactionRunner implements Runnable {

    @Override
    public void run() {
      while (keepRunning.get()) {
        long deferTime = 0;
        long tid = store.reserve();
        try {
          TStatus status = store.getStatus(tid);
          Repo<T> op = store.top(tid);
          if (status == TStatus.FAILED_IN_PROGRESS) {
            processFailed(tid, op);
          } else {
            Repo<T> prevOp = null;
            try {
              deferTime = op.isReady(tid, environment);
              if (deferTime == 0) {
                prevOp = op;
                op = op.call(tid, environment);
              } else
                continue;

            } catch (Exception e) {
              transitionToFailed(tid, op, e);
              continue;
            }

            if (op == null) {
              // transaction is finished
              String ret = prevOp.getReturn();
              if (ret != null)
                store.setProperty(tid, RETURN_PROP, ret);
              store.setStatus(tid, TStatus.SUCCESSFUL);
              doCleanUp(tid);
            } else {
              try {
                store.push(tid, op);
              } catch (StackOverflowException e) {
                // the op that failed to push onto the stack was never executed, so no need to undo it
                // just transition to failed and undo the ops that executed
                transitionToFailed(tid, op, e);
                continue;
              }
            }
          }
        } finally {
          store.unreserve(tid, deferTime);
        }

      }
    }

    private void transitionToFailed(long tid, Repo<T> op, Exception e) {
      String tidStr = String.format("%016x", tid);
      log.warn("Failed to execute Repo, tid=" + tidStr, e);
      store.setProperty(tid, EXCEPTION_PROP, e);
      store.setStatus(tid, TStatus.FAILED_IN_PROGRESS);
      log.info("Updated status for Repo with tid=" + tidStr + " to FAILED_IN_PROGRESS");
    }

    private void processFailed(long tid, Repo<T> op) {
      while (op != null) {
        undo(tid, op);

        store.pop(tid);
        op = store.top(tid);
      }

      store.setStatus(tid, TStatus.FAILED);
      doCleanUp(tid);
    }

    private void doCleanUp(long tid) {
      Boolean autoClean = (Boolean) store.getProperty(tid, AUTO_CLEAN_PROP);
      if (autoClean != null && autoClean) {
        store.delete(tid);
      } else {
        // no longer need persisted operations, so delete them to save space in case
        // TX is never cleaned up...
        while (store.top(tid) != null)
          store.pop(tid);
      }
    }

    private void undo(long tid, Repo<T> op) {
      try {
        op.undo(tid, environment);
      } catch (Exception e) {
        log.warn("Failed to undo Repo, tid=" + String.format("%016x", tid), e);
      }
    }

  }

  // TODO: move thread creation to another method ACCUMULO-2136
  public Fate(T environment, TStore<T> store, int numThreads) {
    this.store = store;
    this.environment = environment;

    for (int i = 0; i < numThreads; i++) {
      // TODO: use an ExecutorService, maybe a utility to do these steps throughout the server packages - ACCUMULO-1311
      Thread thread = new Daemon(new LoggingRunnable(log, new TransactionRunner()), "Repo runner " + i);
      thread.start();
    }
  }

  // get a transaction id back to the requester before doing any work
  public long startTransaction() {
    long dir = store.create();
    return dir;
  }

  // start work in the transaction.. it is safe to call this
  // multiple times for a transaction... but it will only seed once
  public void seedTransaction(long tid, Repo<T> repo, boolean autoCleanUp) {
    store.reserve(tid);
    try {
      if (store.getStatus(tid) == TStatus.NEW) {
        if (store.top(tid) == null) {
          try {
            store.push(tid, repo);
          } catch (StackOverflowException e) {
            // this should not happen
            throw new RuntimeException(e);
          }
        }

        if (autoCleanUp)
          store.setProperty(tid, AUTO_CLEAN_PROP, Boolean.valueOf(autoCleanUp));

        store.setProperty(tid, DEBUG_PROP, repo.getDescription());

        store.setStatus(tid, TStatus.IN_PROGRESS);
      }
    } finally {
      store.unreserve(tid, 0);
    }

  }

  // check on the transaction
  public TStatus waitForCompletion(long tid) {
    return store.waitForStatusChange(tid, FINISHED_STATES);
  }

  // resource cleanup
  public void delete(long tid) {
    store.reserve(tid);
    try {
      switch (store.getStatus(tid)) {
        case NEW:
        case FAILED:
        case SUCCESSFUL:
          store.delete(tid);
          break;
        case FAILED_IN_PROGRESS:
        case IN_PROGRESS:
          throw new IllegalStateException("Can not delete in progress transaction " + String.format("%016x", tid));
        case UNKNOWN:
          // nothing to do, it does not exist
          break;
      }
    } finally {
      store.unreserve(tid, 0);
    }
  }

  public String getReturn(long tid) {
    store.reserve(tid);
    try {
      if (store.getStatus(tid) != TStatus.SUCCESSFUL)
        throw new IllegalStateException("Tried to get exception when transaction " + String.format("%016x", tid) + " not in successful state");
      return (String) store.getProperty(tid, RETURN_PROP);
    } finally {
      store.unreserve(tid, 0);
    }
  }

  // get reportable failures
  public Exception getException(long tid) {
    store.reserve(tid);
    try {
      if (store.getStatus(tid) != TStatus.FAILED)
        throw new IllegalStateException("Tried to get exception when transaction " + String.format("%016x", tid) + " not in failed state");
      return (Exception) store.getProperty(tid, EXCEPTION_PROP);
    } finally {
      store.unreserve(tid, 0);
    }
  }

  /**
   * Flags that FATE threadpool to clear out and end. Does not actively stop running FATE processes.
   */
  public void shutdown() {
    keepRunning.set(false);
  }

}

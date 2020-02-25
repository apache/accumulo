/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.accumulo.fate;

import java.io.IOException;
import java.util.EnumSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import org.apache.accumulo.core.logging.FateLogger;
import org.apache.accumulo.core.util.ShutdownUtil;
import org.apache.accumulo.fate.ReadOnlyTStore.TStatus;
import org.apache.accumulo.fate.util.LoggingRunnable;
import org.apache.accumulo.fate.util.UtilWaitThread;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Fault tolerant executor
 */
public class Fate<T> {

  private static final String DEBUG_PROP = "debug";
  private static final String AUTO_CLEAN_PROP = "autoClean";
  private static final String EXCEPTION_PROP = "exception";
  private static final String RETURN_PROP = "return";

  private static final Logger log = LoggerFactory.getLogger(Fate.class);
  private final Logger runnerLog = LoggerFactory.getLogger(TransactionRunner.class);

  private TStore<T> store;
  private T environment;
  private ExecutorService executor;

  private static final EnumSet<TStatus> FINISHED_STATES =
      EnumSet.of(TStatus.FAILED, TStatus.SUCCESSFUL, TStatus.UNKNOWN);

  private AtomicBoolean keepRunning = new AtomicBoolean(true);

  private class TransactionRunner implements Runnable {

    @Override
    public void run() {
      while (keepRunning.get()) {
        long deferTime = 0;
        Long tid = null;
        try {
          tid = store.reserve();
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
              blockIfHadoopShutdown(tid, e);
              transitionToFailed(tid, e);
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
                // the op that failed to push onto the stack was never executed, so no need to undo
                // it
                // just transition to failed and undo the ops that executed
                transitionToFailed(tid, e);
                continue;
              }
            }
          }
        } catch (Exception e) {
          runnerLog.error("Uncaught exception in FATE runner thread.", e);
        } finally {
          if (tid != null) {
            store.unreserve(tid, deferTime);
          }
        }

      }
    }

    private boolean isIOException(Throwable e) {
      if (e == null)
        return false;

      if (e instanceof IOException)
        return true;

      for (Throwable suppressed : e.getSuppressed())
        if (isIOException(suppressed))
          return true;

      return isIOException(e.getCause());
    }

    /**
     * The Hadoop Filesystem registers a java shutdown hook that closes the file system. This can
     * cause threads to get spurious IOException. If this happens, instead of failing a FATE
     * transaction just wait for process to die. When the master start elsewhere the FATE
     * transaction can resume.
     */
    private void blockIfHadoopShutdown(long tid, Exception e) {
      if (ShutdownUtil.isShutdownInProgress()) {
        String tidStr = FateTxId.formatTid(tid);

        if (e instanceof AcceptableException) {
          log.debug("Ignoring exception possibly caused by Hadoop Shutdown hook. {} ", tidStr, e);
        } else if (isIOException(e)) {
          log.info("Ignoring exception likely caused by Hadoop Shutdown hook. {} ", tidStr, e);
        } else {
          // sometimes code will catch an IOException caused by the hadoop shutdown hook and throw
          // another exception without setting the cause.
          log.warn("Ignoring exception possibly caused by Hadoop Shutdown hook. {} ", tidStr, e);
        }

        while (true) {
          // Nothing is going to work well at this point, so why even try. Just wait for the end,
          // preventing this FATE thread from processing further work and likely failing.
          UtilWaitThread.sleepUninterruptibly(1, TimeUnit.MINUTES);
        }
      }
    }

    private void transitionToFailed(long tid, Exception e) {
      String tidStr = FateTxId.formatTid(tid);
      final String msg = "Failed to execute Repo, " + tidStr;
      // Certain FATE ops that throw exceptions don't need to be propagated up to the Monitor
      // as a warning. They're a normal, handled failure condition.
      if (e instanceof AcceptableException) {
        log.debug(msg, e.getCause());
      } else {
        log.warn(msg, e);
      }
      store.setProperty(tid, EXCEPTION_PROP, e);
      store.setStatus(tid, TStatus.FAILED_IN_PROGRESS);
      log.info("Updated status for Repo with {} to FAILED_IN_PROGRESS", tidStr);
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
        log.warn("Failed to undo Repo, " + FateTxId.formatTid(tid), e);
      }
    }

  }

  /**
   * Creates a Fault-tolerant executor.
   * <p>
   * Note: Users of this class should call {@link #startTransactionRunners(int)} to launch the
   * worker threads after creating a Fate object.
   *
   * @param toLogStrFunc
   *          A function that converts Repo to Strings that are suitable for logging
   */
  public Fate(T environment, TStore<T> store, Function<Repo<T>,String> toLogStrFunc) {
    this.store = FateLogger.wrap(store, toLogStrFunc);
    this.environment = environment;
  }

  /**
   * Launches the specified number of worker threads.
   */
  public void startTransactionRunners(int numThreads) {
    final AtomicInteger runnerCount = new AtomicInteger(0);
    executor = Executors.newFixedThreadPool(numThreads, r -> {
      Thread t =
          new Thread(new LoggingRunnable(log, r), "Repo runner " + runnerCount.getAndIncrement());
      t.setDaemon(true);
      return t;
    });
    for (int i = 0; i < numThreads; i++) {
      executor.execute(new TransactionRunner());
    }
  }

  // get a transaction id back to the requester before doing any work
  public long startTransaction() {
    return store.create();
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
          store.setProperty(tid, AUTO_CLEAN_PROP, autoCleanUp);

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
          throw new IllegalStateException(
              "Can not delete in progress transaction " + FateTxId.formatTid(tid));
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
        throw new IllegalStateException("Tried to get exception when transaction "
            + FateTxId.formatTid(tid) + " not in successful state");
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
        throw new IllegalStateException("Tried to get exception when transaction "
            + FateTxId.formatTid(tid) + " not in failed state");
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
    executor.shutdown();
  }

}

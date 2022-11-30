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

import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.accumulo.core.fate.ReadOnlyTStore.TStatus.FAILED;
import static org.apache.accumulo.core.fate.ReadOnlyTStore.TStatus.FAILED_IN_PROGRESS;
import static org.apache.accumulo.core.fate.ReadOnlyTStore.TStatus.IN_PROGRESS;
import static org.apache.accumulo.core.fate.ReadOnlyTStore.TStatus.NEW;
import static org.apache.accumulo.core.fate.ReadOnlyTStore.TStatus.SUBMITTED;
import static org.apache.accumulo.core.fate.ReadOnlyTStore.TStatus.SUCCESSFUL;
import static org.apache.accumulo.core.fate.ReadOnlyTStore.TStatus.UNKNOWN;
import static org.apache.accumulo.core.util.ShutdownUtil.isIOException;

import java.util.EnumSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

import org.apache.accumulo.core.clientImpl.AcceptableThriftTableOperationException;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.fate.ReadOnlyTStore.TStatus;
import org.apache.accumulo.core.logging.FateLogger;
import org.apache.accumulo.core.util.ShutdownUtil;
import org.apache.accumulo.core.util.UtilWaitThread;
import org.apache.accumulo.core.util.threads.ThreadPools;
import org.apache.thrift.TApplicationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Fault tolerant executor
 */
public class Fate<T> {

  private static final Logger log = LoggerFactory.getLogger(Fate.class);
  private final Logger runnerLog = LoggerFactory.getLogger(TransactionRunner.class);

  private final TStore<T> store;
  private final T environment;
  private ScheduledThreadPoolExecutor fatePoolWatcher;
  private ExecutorService executor;

  private static final EnumSet<TStatus> FINISHED_STATES = EnumSet.of(FAILED, SUCCESSFUL, UNKNOWN);

  private final AtomicBoolean keepRunning = new AtomicBoolean(true);

  public enum TxInfo {
    TX_NAME, AUTO_CLEAN, EXCEPTION, RETURN_VALUE
  }

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
          if (status == FAILED_IN_PROGRESS) {
            processFailed(tid, op);
          } else {
            Repo<T> prevOp = null;
            try {
              deferTime = op.isReady(tid, environment);

              // Here, deferTime is only used to determine success (zero) or failure (non-zero),
              // proceeding on success and returning to the while loop on failure.
              // The value of deferTime is only used as a wait time in ZooStore.unreserve
              if (deferTime == 0) {
                prevOp = op;
                if (status == SUBMITTED) {
                  store.setStatus(tid, IN_PROGRESS);
                }
                op = op.call(tid, environment);
              } else {
                continue;
              }

            } catch (Exception e) {
              blockIfHadoopShutdown(tid, e);
              transitionToFailed(tid, e);
              continue;
            }

            if (op == null) {
              // transaction is finished
              String ret = prevOp.getReturn();
              if (ret != null) {
                store.setTransactionInfo(tid, TxInfo.RETURN_VALUE, ret);
              }
              store.setStatus(tid, SUCCESSFUL);
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

    /**
     * The Hadoop Filesystem registers a java shutdown hook that closes the file system. This can
     * cause threads to get spurious IOException. If this happens, instead of failing a FATE
     * transaction just wait for process to die. When the manager start elsewhere the FATE
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
          UtilWaitThread.sleepUninterruptibly(1, MINUTES);
        }
      }
    }

    private void transitionToFailed(long tid, Exception e) {
      String tidStr = FateTxId.formatTid(tid);
      final String msg = "Failed to execute Repo " + tidStr;
      // Certain FATE ops that throw exceptions don't need to be propagated up to the Monitor
      // as a warning. They're a normal, handled failure condition.
      if (e instanceof AcceptableException) {
        var tableOpEx = (AcceptableThriftTableOperationException) e;
        log.debug(msg + " for {}({}) {}", tableOpEx.getTableName(), tableOpEx.getTableId(),
            tableOpEx.getDescription());
      } else {
        log.warn(msg, e);
      }
      store.setTransactionInfo(tid, TxInfo.EXCEPTION, e);
      store.setStatus(tid, FAILED_IN_PROGRESS);
      log.info("Updated status for Repo with {} to FAILED_IN_PROGRESS", tidStr);
    }

    private void processFailed(long tid, Repo<T> op) {
      while (op != null) {
        undo(tid, op);

        store.pop(tid);
        op = store.top(tid);
      }

      store.setStatus(tid, FAILED);
      doCleanUp(tid);
    }

    private void doCleanUp(long tid) {
      Boolean autoClean = (Boolean) store.getTransactionInfo(tid, TxInfo.AUTO_CLEAN);
      if (autoClean != null && autoClean) {
        store.delete(tid);
      } else {
        // no longer need persisted operations, so delete them to save space in case
        // TX is never cleaned up...
        while (store.top(tid) != null) {
          store.pop(tid);
        }
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
   * Note: Users of this class should call {@link #startTransactionRunners(AccumuloConfiguration)}
   * to launch the worker threads after creating a Fate object.
   *
   * @param toLogStrFunc A function that converts Repo to Strings that are suitable for logging
   */
  public Fate(T environment, TStore<T> store, Function<Repo<T>,String> toLogStrFunc) {
    this.store = FateLogger.wrap(store, toLogStrFunc);
    this.environment = environment;
  }

  /**
   * Launches the specified number of worker threads.
   */
  public void startTransactionRunners(AccumuloConfiguration conf) {
    final ThreadPoolExecutor pool = ThreadPools.getServerThreadPools().createExecutorService(conf,
        Property.MANAGER_FATE_THREADPOOL_SIZE, true);
    fatePoolWatcher =
        ThreadPools.getServerThreadPools().createGeneralScheduledExecutorService(conf);
    ThreadPools.watchCriticalScheduledTask(fatePoolWatcher.schedule(() -> {
      // resize the pool if the property changed
      ThreadPools.resizePool(pool, conf, Property.MANAGER_FATE_THREADPOOL_SIZE);
      // If the pool grew, then ensure that there is a TransactionRunner for each thread
      int needed = conf.getCount(Property.MANAGER_FATE_THREADPOOL_SIZE) - pool.getQueue().size();
      if (needed > 0) {
        for (int i = 0; i < needed; i++) {
          try {
            pool.execute(new TransactionRunner());
          } catch (RejectedExecutionException e) {
            // RejectedExecutionException could be shutting down
            if (pool.isShutdown()) {
              // The exception is expected in this case, no need to spam the logs.
              log.trace("Error adding transaction runner to FaTE executor pool.", e);
            } else {
              // This is bad, FaTE may no longer work!
              log.error("Error adding transaction runner to FaTE executor pool.", e);
            }
            break;
          }
        }
      }
    }, 3, SECONDS));
    executor = pool;
  }

  // get a transaction id back to the requester before doing any work
  public long startTransaction() {
    return store.create();
  }

  // start work in the transaction.. it is safe to call this
  // multiple times for a transaction... but it will only seed once
  public void seedTransaction(String txName, long tid, Repo<T> repo, boolean autoCleanUp,
      String goalMessage) {
    store.reserve(tid);
    try {
      if (store.getStatus(tid) == NEW) {
        if (store.top(tid) == null) {
          try {
            log.info("Seeding {} {}", FateTxId.formatTid(tid), goalMessage);
            store.push(tid, repo);
          } catch (StackOverflowException e) {
            // this should not happen
            throw new RuntimeException(e);
          }
        }

        if (autoCleanUp) {
          store.setTransactionInfo(tid, TxInfo.AUTO_CLEAN, autoCleanUp);
        }

        store.setTransactionInfo(tid, TxInfo.TX_NAME, txName);

        store.setStatus(tid, SUBMITTED);
      }
    } finally {
      store.unreserve(tid, 0);
    }

  }

  // check on the transaction
  public TStatus waitForCompletion(long tid) {
    return store.waitForStatusChange(tid, FINISHED_STATES);
  }

  /**
   * Attempts to cancel a running Fate transaction
   *
   * @param tid transaction id
   * @return true if transaction transitioned to a failed state or already in a completed state,
   *         false otherwise
   */
  public boolean cancel(long tid) {
    String tidStr = FateTxId.formatTid(tid);
    for (int retries = 0; retries < 5; retries++) {
      if (store.tryReserve(tid)) {
        try {
          TStatus status = store.getStatus(tid);
          log.info("status is: {}", status);
          if (status == NEW || status == SUBMITTED) {
            store.setTransactionInfo(tid, TxInfo.EXCEPTION, new TApplicationException(
                TApplicationException.INTERNAL_ERROR, "Fate transaction cancelled by user"));
            store.setStatus(tid, FAILED_IN_PROGRESS);
            log.info("Updated status for {} to FAILED_IN_PROGRESS because it was cancelled by user",
                tidStr);
            return true;
          } else {
            log.info("{} cancelled by user but already in progress or finished state", tidStr);
            return false;
          }
        } finally {
          store.unreserve(tid, 0);
        }
      } else {
        // reserved, lets retry.
        UtilWaitThread.sleep(500);
      }
    }
    log.info("Unable to reserve transaction {} to cancel it", tid);
    return false;
  }

  // resource cleanup
  public void delete(long tid) {
    store.reserve(tid);
    try {
      switch (store.getStatus(tid)) {
        case NEW:
        case SUBMITTED:
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
      if (store.getStatus(tid) != SUCCESSFUL) {
        throw new IllegalStateException("Tried to get exception when transaction "
            + FateTxId.formatTid(tid) + " not in successful state");
      }
      return (String) store.getTransactionInfo(tid, TxInfo.RETURN_VALUE);
    } finally {
      store.unreserve(tid, 0);
    }
  }

  // get reportable failures
  public Exception getException(long tid) {
    store.reserve(tid);
    try {
      if (store.getStatus(tid) != FAILED) {
        throw new IllegalStateException("Tried to get exception when transaction "
            + FateTxId.formatTid(tid) + " not in failed state");
      }
      return (Exception) store.getTransactionInfo(tid, TxInfo.EXCEPTION);
    } finally {
      store.unreserve(tid, 0);
    }
  }

  /**
   * Flags that FATE threadpool to clear out and end. Does not actively stop running FATE processes.
   */
  public void shutdown() {
    keepRunning.set(false);
    fatePoolWatcher.shutdown();
    executor.shutdown();
  }

}

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

import static com.google.common.util.concurrent.Uninterruptibles.sleepUninterruptibly;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.accumulo.core.fate.ReadOnlyFateStore.TStatus.FAILED;
import static org.apache.accumulo.core.fate.ReadOnlyFateStore.TStatus.FAILED_IN_PROGRESS;
import static org.apache.accumulo.core.fate.ReadOnlyFateStore.TStatus.IN_PROGRESS;
import static org.apache.accumulo.core.fate.ReadOnlyFateStore.TStatus.NEW;
import static org.apache.accumulo.core.fate.ReadOnlyFateStore.TStatus.SUBMITTED;
import static org.apache.accumulo.core.fate.ReadOnlyFateStore.TStatus.SUCCESSFUL;
import static org.apache.accumulo.core.fate.ReadOnlyFateStore.TStatus.UNKNOWN;
import static org.apache.accumulo.core.util.ShutdownUtil.isIOException;

import java.util.Collections;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.function.Supplier;

import org.apache.accumulo.core.clientImpl.AcceptableThriftTableOperationException;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.fate.FateStore.FateTxStore;
import org.apache.accumulo.core.fate.ReadOnlyFateStore.TStatus;
import org.apache.accumulo.core.logging.FateLogger;
import org.apache.accumulo.core.manager.PartitionData;
import org.apache.accumulo.core.util.Retry;
import org.apache.accumulo.core.util.ShutdownUtil;
import org.apache.accumulo.core.util.UtilWaitThread;
import org.apache.accumulo.core.util.threads.ThreadPools;
import org.apache.accumulo.core.util.threads.Threads;
import org.apache.thrift.TApplicationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Fault tolerant executor
 */
public class Fate<T> {

  private static final Logger log = LoggerFactory.getLogger(Fate.class);
  private final Logger runnerLog = LoggerFactory.getLogger(TransactionRunner.class);

  private final FateStore<T> store;
  private final T environment;
  private final ScheduledThreadPoolExecutor fatePoolWatcher;
  private final ExecutorService executor;

  private static final EnumSet<TStatus> FINISHED_STATES = EnumSet.of(FAILED, SUCCESSFUL, UNKNOWN);

  private final AtomicBoolean keepRunning = new AtomicBoolean(true);
  private final Supplier<PartitionData> partitionDataSupplier;
  private final BlockingQueue<Long> workQueue;
  private final Set<Long> queuedWork;
  private final Thread workFinder;

  public enum TxInfo {
    TX_NAME, AUTO_CLEAN, EXCEPTION, RETURN_VALUE
  }

  // TODO add a task that periodically looks for fate task reserved by dead instances, only run in
  // partition zero
  private class LockCleaner implements Runnable {

    @Override
    public void run() {
      while (keepRunning.get()) {
        var partitionData = partitionDataSupplier.get();
        if (partitionData.shouldRun(PartitionData.SingletonManagerService.FATE_LOCK_CLEANUP)) {
          // run cleanup
        }
        // sleep a long time
      }
    }
  }

  /**
   * A single thread that finds transactions to work on and queues them up. Do not want each worker
   * thread going to the store and looking for work as it would place more load on the store.
   */
  private class WorkFinder implements Runnable {

    private Retry newRetry() {
      // ELASTICITY_TODO the max time to retry may be store depependent, depends on how expensive
      // the read is in the impl. The more expensive the read is the high we may want to make the
      // max. Can figure this out after we have an Accumulo table store.
      return Retry.builder().infiniteRetries().retryAfter(25, MILLISECONDS)
          .incrementBy(25, MILLISECONDS).maxWait(10, SECONDS).backOffFactor(1.5)
          .logInterval(3, MINUTES).createRetry();
    }

    @Override
    public void run() {

      try {
        Retry retry = newRetry();
        PartitionData lastPartitionData = partitionDataSupplier.get();

        while (keepRunning.get()) {
          var partitionData = partitionDataSupplier.get();
          if (!partitionData.equals(lastPartitionData)) {
            // Partition data changed, so lets clear anything queued to avoid unnecessary
            // contention.
            // The queue does not have to be cleared here for overall correctness, this is just an
            // attempt to avoid some unnecessary work caused by multiple processes trying to reserve
            // the same fate transaction.
            workQueue.clear();
            queuedWork.clear();
          }
          lastPartitionData = partitionData;

          if (workQueue.isEmpty()) {
            queuedWork.clear();
          }

          var iter = store.runnable(partitionData);

          int added = 0;
          while (iter.hasNext() && keepRunning.get()) {
            Long txid = iter.next();
            try {
              if (queuedWork.contains(txid)) {
                // Avoid adding the same id to the queue multiple times as this will cause
                // unnecessary work when multiple threads try to work on the same id.
                continue;
              }
              added++;
              queuedWork.add(txid);
              while (keepRunning.get()) {
                if (workQueue.offer(txid, 100, TimeUnit.MILLISECONDS)) {
                  break;
                }
              }
            } catch (InterruptedException e) {
              Thread.currentThread().interrupt();
              throw new IllegalStateException(e);
            }
          }

          if (added == 0) {
            // When nothing was added to the queue, then start doing exponential backoff before
            // reading from the store again in order to avoid overwhelming the store.
            try {
              retry.waitForNextAttempt(log,
                  "Find work for fate partition " + partitionData.toString());
            } catch (InterruptedException e) {
              Thread.currentThread().interrupt();
              throw new IllegalStateException(e);
            }
          } else {
            // reset the retry object
            retry.logCompletion(log, "Find work for fate partition " + partitionData.toString());
            retry = newRetry();
          }
        }
      } catch (Exception e) {
        if (keepRunning.get()) {
          log.warn("Failure while attempting to find work for fate", e);
        } else {
          log.debug("Failure while attempting to find work for fate", e);
        }

        queuedWork.clear();
        workQueue.clear();
      }
    }
  }

  private class TransactionRunner implements Runnable {

    @Override
    public void run() {
      while (keepRunning.get()) {
        long deferTime = 0;
        FateStore.FateTxStore<T> txStore = null;
        try {
          var unreservedTid = workQueue.poll(100, MILLISECONDS);
          if (unreservedTid == null) {
            continue;
          }
          queuedWork.remove(unreservedTid);
          var optionalopStore = store.tryReserve(unreservedTid);
          if (optionalopStore.isPresent()) {
            txStore = optionalopStore.orElseThrow();
          } else {
            continue;
          }
          TStatus status = txStore.getStatus();
          Repo<T> op = txStore.top();
          if (status == FAILED_IN_PROGRESS) {
            processFailed(txStore, op);
          } else if (status == SUBMITTED || status == IN_PROGRESS) {
            Repo<T> prevOp = null;
            try {
              deferTime = op.isReady(txStore.getID(), environment);

              // Here, deferTime is only used to determine success (zero) or failure (non-zero),
              // proceeding on success and returning to the while loop on failure.
              // The value of deferTime is only used as a wait time in ZooStore.unreserve
              if (deferTime == 0) {
                prevOp = op;
                if (status == SUBMITTED) {
                  txStore.setStatus(IN_PROGRESS);
                }
                op = op.call(txStore.getID(), environment);
              } else {
                continue;
              }

            } catch (Exception e) {
              blockIfHadoopShutdown(txStore.getID(), e);
              transitionToFailed(txStore, e);
              continue;
            }

            if (op == null) {
              // transaction is finished
              String ret = prevOp.getReturn();
              if (ret != null) {
                txStore.setTransactionInfo(TxInfo.RETURN_VALUE, ret);
              }
              txStore.setStatus(SUCCESSFUL);
              doCleanUp(txStore);
            } else {
              try {
                txStore.push(op);
              } catch (StackOverflowException e) {
                // the op that failed to push onto the stack was never executed, so no need to undo
                // it
                // just transition to failed and undo the ops that executed
                transitionToFailed(txStore, e);
                continue;
              }
            }
          }
        } catch (Exception e) {
          runnerLog.error("Uncaught exception in FATE runner thread.", e);
        } finally {
          if (txStore != null) {
            txStore.unreserve(deferTime);
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
          sleepUninterruptibly(1, MINUTES);
        }
      }
    }

    private void transitionToFailed(FateTxStore<T> txStore, Exception e) {
      String tidStr = FateTxId.formatTid(txStore.getID());
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
      txStore.setTransactionInfo(TxInfo.EXCEPTION, e);
      txStore.setStatus(FAILED_IN_PROGRESS);
      log.info("Updated status for Repo with {} to FAILED_IN_PROGRESS", tidStr);
    }

    private void processFailed(FateTxStore<T> txStore, Repo<T> op) {
      while (op != null) {
        undo(txStore.getID(), op);

        txStore.pop();
        op = txStore.top();
      }

      txStore.setStatus(FAILED);
      doCleanUp(txStore);
    }

    private void doCleanUp(FateTxStore<T> txStore) {
      Boolean autoClean = (Boolean) txStore.getTransactionInfo(TxInfo.AUTO_CLEAN);
      if (autoClean != null && autoClean) {
        txStore.delete();
      } else {
        // no longer need persisted operations, so delete them to save space in case
        // TX is never cleaned up...
        while (txStore.top() != null) {
          txStore.pop();
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
   *
   * @param toLogStrFunc A function that converts Repo to Strings that are suitable for logging
   */
  public Fate(T environment, FateStore<T> store, Function<Repo<T>,String> toLogStrFunc,
      Supplier<PartitionData> partitionDataSupplier, AccumuloConfiguration conf) {
    this.store = FateLogger.wrap(store, toLogStrFunc);
    this.environment = environment;
    this.partitionDataSupplier = partitionDataSupplier;
    final ThreadPoolExecutor pool = ThreadPools.getServerThreadPools().createExecutorService(conf,
        Property.MANAGER_FATE_THREADPOOL_SIZE, true);
    // TODO this queue does not resize when config changes
    this.workQueue =
        new ArrayBlockingQueue<>(conf.getCount(Property.MANAGER_FATE_THREADPOOL_SIZE) * 4);
    this.queuedWork = Collections.synchronizedSet(new HashSet<>());
    this.fatePoolWatcher =
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
    this.executor = pool;

    this.workFinder = Threads.createThread("Fate work finder", new WorkFinder());
    this.workFinder.start();
  }

  // get a transaction id back to the requester before doing any work
  public long startTransaction() {
    return store.create();
  }

  // start work in the transaction.. it is safe to call this
  // multiple times for a transaction... but it will only seed once
  public void seedTransaction(String txName, long tid, Repo<T> repo, boolean autoCleanUp,
      String goalMessage) {
    FateTxStore<T> txStore = store.reserve(tid);
    try {
      if (txStore.getStatus() == NEW) {
        if (txStore.top() == null) {
          try {
            log.info("Seeding {} {}", FateTxId.formatTid(tid), goalMessage);
            txStore.push(repo);
          } catch (StackOverflowException e) {
            // this should not happen
            throw new IllegalStateException(e);
          }
        }

        if (autoCleanUp) {
          txStore.setTransactionInfo(TxInfo.AUTO_CLEAN, autoCleanUp);
        }

        txStore.setTransactionInfo(TxInfo.TX_NAME, txName);

        txStore.setStatus(SUBMITTED);
      }
    } finally {
      txStore.unreserve(0);
    }

  }

  // check on the transaction
  public TStatus waitForCompletion(long tid) {
    return store.read(tid).waitForStatusChange(FINISHED_STATES);
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
      Optional<FateTxStore<T>> optionalTxStore = store.tryReserve(tid);
      if (optionalTxStore.isPresent()) {
        var txStore = optionalTxStore.orElseThrow();
        try {
          TStatus status = txStore.getStatus();
          log.info("status is: {}", status);
          if (status == NEW || status == SUBMITTED) {
            txStore.setTransactionInfo(TxInfo.EXCEPTION, new TApplicationException(
                TApplicationException.INTERNAL_ERROR, "Fate transaction cancelled by user"));
            txStore.setStatus(FAILED_IN_PROGRESS);
            log.info("Updated status for {} to FAILED_IN_PROGRESS because it was cancelled by user",
                tidStr);
            return true;
          } else {
            log.info("{} cancelled by user but already in progress or finished state", tidStr);
            return false;
          }
        } finally {
          txStore.unreserve(0);
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
    // TODO need to handle case of not existing
    FateTxStore<T> txStore = store.reserve(tid);
    try {
      switch (txStore.getStatus()) {
        case NEW:
        case SUBMITTED:
        case FAILED:
        case SUCCESSFUL:
          txStore.delete();
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
      txStore.unreserve(0);
    }
  }

  public String getReturn(long tid) {
    FateTxStore<T> txStore = store.reserve(tid);
    try {
      if (txStore.getStatus() != SUCCESSFUL) {
        throw new IllegalStateException("Tried to get exception when transaction "
            + FateTxId.formatTid(tid) + " not in successful state");
      }
      return (String) txStore.getTransactionInfo(TxInfo.RETURN_VALUE);
    } finally {
      txStore.unreserve(0);
    }
  }

  // get reportable failures
  public Exception getException(long tid) {
    FateTxStore<T> txStore = store.reserve(tid);
    try {
      if (txStore.getStatus() != FAILED) {
        throw new IllegalStateException("Tried to get exception when transaction "
            + FateTxId.formatTid(tid) + " not in failed state");
      }
      return (Exception) txStore.getTransactionInfo(TxInfo.EXCEPTION);
    } finally {
      txStore.unreserve(0);
    }
  }

  /**
   * Flags that FATE threadpool to clear out and end. Does not actively stop running FATE processes.
   */
  public void shutdown() {
    keepRunning.set(false);
    fatePoolWatcher.shutdown();
    if (executor != null) {
      executor.shutdown();
    }
    workFinder.interrupt();
  }

}

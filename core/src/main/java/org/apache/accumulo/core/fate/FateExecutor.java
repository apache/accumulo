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
import static org.apache.accumulo.core.fate.ReadOnlyFateStore.TStatus.SUBMITTED;
import static org.apache.accumulo.core.fate.ReadOnlyFateStore.TStatus.SUCCESSFUL;
import static org.apache.accumulo.core.util.ShutdownUtil.isIOException;

import java.time.Duration;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedTransferQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TransferQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.accumulo.core.clientImpl.AcceptableThriftTableOperationException;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.fate.Fate.TxInfo;
import org.apache.accumulo.core.fate.FateStore.FateTxStore;
import org.apache.accumulo.core.fate.ReadOnlyFateStore.TStatus;
import org.apache.accumulo.core.util.ShutdownUtil;
import org.apache.accumulo.core.util.Timer;
import org.apache.accumulo.core.util.threads.ThreadPoolNames;
import org.apache.accumulo.core.util.threads.ThreadPools;
import org.apache.accumulo.core.util.threads.Threads;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

/**
 * Handles finding and working on FATE work. Only finds/works on fate operations that it is assigned
 * to work on defined by 'fateOps'. These executors may be stopped and new ones started throughout
 * FATEs life, depending on changes to {@link Property#MANAGER_FATE_USER_CONFIG} and
 * {@link Property#MANAGER_FATE_META_CONFIG}.
 */
public class FateExecutor<T> {
  private static final Logger log = LoggerFactory.getLogger(FateExecutor.class);
  private final Logger runnerLog = LoggerFactory.getLogger(TransactionRunner.class);

  private final T environment;
  private final Fate<T> fate;
  private final Thread workFinder;
  private final TransferQueue<FateId> workQueue;
  private final AtomicInteger idleWorkerCount;
  private final String name;
  private final String poolName;
  private final ThreadPoolExecutor transactionExecutor;
  private final Set<TransactionRunner> runningTxRunners;
  private final Set<Fate.FateOperation> fateOps;
  private final ConcurrentLinkedQueue<Integer> idleCountHistory = new ConcurrentLinkedQueue<>();
  private final FateExecutorMetrics<T> fateExecutorMetrics;

  public FateExecutor(Fate<T> fate, T environment, Set<Fate.FateOperation> fateOps, int poolSize,
      String name) {
    final FateInstanceType type = fate.getStore().type();
    final String typeStr = type.name().toLowerCase();
    final String poolName =
        ThreadPoolNames.MANAGER_FATE_POOL_PREFIX.poolName + typeStr + "." + name;
    final String workFinderThreadName = "fate.work.finder." + typeStr + "." + name;

    this.fate = fate;
    this.environment = environment;
    this.fateOps = Collections.unmodifiableSet(fateOps);
    this.workQueue = new LinkedTransferQueue<>();
    this.runningTxRunners = Collections.synchronizedSet(new HashSet<>());
    this.name = name;
    this.poolName = poolName;
    this.transactionExecutor = ThreadPools.getServerThreadPools().getPoolBuilder(poolName)
        .numCoreThreads(poolSize).build();
    this.idleWorkerCount = new AtomicInteger(0);
    this.fateExecutorMetrics =
        new FateExecutorMetrics<T>(type, poolName, runningTxRunners, idleWorkerCount);

    this.workFinder = Threads.createCriticalThread(workFinderThreadName, new WorkFinder());
    this.workFinder.start();
  }

  /**
   * resize the pool to match the config as necessary and submit new TransactionRunners if the pool
   * grew, stop TransactionRunners if the pool shrunk, and potentially suggest resizing the pool if
   * the load is consistently high.
   */
  protected void resizeFateExecutor(
      Map<Set<Fate.FateOperation>,Map.Entry<String,Integer>> poolConfigs,
      long idleCheckIntervalMillis) {
    final int configured = poolConfigs.get(fateOps).getValue();
    ThreadPools.resizePool(transactionExecutor, () -> configured, poolName);
    synchronized (runningTxRunners) {
      final int running = runningTxRunners.size();
      final int needed = configured - running;
      log.trace("resizing pools configured:{} running:{} needed:{} fateOps:{}", configured, running,
          needed, fateOps);
      if (needed > 0) {
        // If the pool grew, then ensure that there is a TransactionRunner for each thread
        for (int i = 0; i < needed; i++) {
          final TransactionRunner tr = new TransactionRunner();
          try {
            runningTxRunners.add(tr);
            transactionExecutor.execute(tr);
          } catch (RejectedExecutionException e) {
            runningTxRunners.remove(tr);
            // RejectedExecutionException could be shutting down
            if (transactionExecutor.isShutdown()) {
              // The exception is expected in this case, no need to spam the logs.
              log.trace("Expected error adding transaction runner to FaTE executor pool. "
                  + "The pool is shutdown.", e);
            } else {
              // This is bad, FaTE may no longer work!
              log.error("Unexpected error adding transaction runner to FaTE executor pool.", e);
            }
            break;
          }
        }
        idleCountHistory.clear();
      } else if (needed < 0) {
        // If we need the pool to shrink, then ensure excess TransactionRunners are safely
        // stopped.
        // Flag the necessary number of TransactionRunners to safely stop when they are done
        // work on a transaction.
        int numFlagged = (int) runningTxRunners.stream()
            .filter(FateExecutor.TransactionRunner::isFlaggedToStop).count();
        int numToStop = -1 * (numFlagged + needed);
        for (var runner : runningTxRunners) {
          if (numToStop <= 0) {
            break;
          }
          if (runner.flagStop()) {
            log.trace("Flagging a TransactionRunner to stop...");
            numToStop--;
          }
        }
      } else {
        // The pool size did not change, but should it based on idle Fate threads? Maintain
        // count of the last X minutes of idle Fate threads. If zero 95% of the time, then
        // suggest that the pool size be increased or the fate ops assigned to that pool be
        // split into separate pools.
        final long interval =
            Math.min(60, TimeUnit.MILLISECONDS.toMinutes(idleCheckIntervalMillis));
        var fateConfigProp = Fate.getFateConfigProp(fate.getStore().type());

        if (interval == 0) {
          idleCountHistory.clear();
        } else {
          if (idleCountHistory.size() >= interval * 2) { // this task runs every 30s
            int zeroFateThreadsIdleCount = 0;
            for (Integer idleConsumerCount : idleCountHistory) {
              if (idleConsumerCount == 0) {
                zeroFateThreadsIdleCount++;
              }
            }
            boolean needMoreThreads =
                (zeroFateThreadsIdleCount / (double) idleCountHistory.size()) >= 0.95;
            if (needMoreThreads) {
              fate.getNeedMoreThreadsWarnCount().incrementAndGet();
              log.warn(
                  "All {} Fate threads working on the fate ops {} appear to be busy for "
                      + "the last {} minutes. Consider increasing the value for the "
                      + "entry in the property {} or splitting the fate ops across "
                      + "multiple entries/pools.",
                  fate.getStore().type(), fateOps, interval, fateConfigProp.getKey());
              // Clear the history so that we don't log for interval minutes.
              idleCountHistory.clear();
            } else {
              while (idleCountHistory.size() >= interval * 2) {
                idleCountHistory.remove();
              }
            }
          }
          idleCountHistory.add(getIdleWorkerCount());
        }
      }
    }
  }

  protected String getName() {
    return name;
  }

  private int getIdleWorkerCount() {
    // This could call workQueue.getWaitingConsumerCount() if other code use poll with timeout
    return idleWorkerCount.get();
  }

  /**
   * @return the number of currently running transaction runners
   */
  protected int getNumRunningTxRunners() {
    return runningTxRunners.size();
  }

  protected Set<Fate.FateOperation> getFateOps() {
    return fateOps;
  }

  public FateExecutorMetrics<T> getFateExecutorMetrics() {
    return fateExecutorMetrics;
  }

  /**
   * Initiates the shutdown of this FateExecutor. This means the pool executing TransactionRunners
   * will no longer accept new TransactionRunners, the currently running TransactionRunners will
   * terminate after they are done with their current transaction, if applicable, the work finder is
   * shutdown, and the metrics created for this FateExecutor are removed from the registry (if
   * metrics were enabled). {@link #isShutdown()} returns true after this is called.
   */
  protected void initiateShutdown() {
    log.debug("Initiated shutdown {}", fateOps);
    transactionExecutor.shutdown();
    synchronized (runningTxRunners) {
      runningTxRunners.forEach(TransactionRunner::flagStop);
    }
    fateExecutorMetrics.clearMetrics();
    // work finder will terminate since this.isShutdown() is true
  }

  /**
   * @return true if {@link #initiateShutdown()} has previously been called on this FateExecutor.
   *         The FateExecutor may or may not still have running threads. To check that, see
   *         {@link #isAlive()}
   */
  protected boolean isShutdown() {
    return transactionExecutor.isShutdown();
  }

  protected void shutdownNow() {
    transactionExecutor.shutdownNow();
  }

  protected void waitForShutdown(long start, long timeout, TimeUnit timeUnit)
      throws InterruptedException {
    if (timeout > 0) {
      while (((System.nanoTime() - start) < timeUnit.toNanos(timeout)) && isAlive()) {
        if (!transactionExecutor.awaitTermination(1, SECONDS)) {
          log.debug("Fate {} is waiting for {} worker threads for fate ops {} to terminate",
              fate.getStore().type(), runningTxRunners.size(), fateOps);
          continue;
        }

        workFinder.join(1_000);
        if (workFinder.isAlive()) {
          log.debug("Fate {} is waiting for work finder thread for fate ops {} to terminate",
              fate.getStore().type(), fateOps);
          workFinder.interrupt();
        }
      }

      if (isAlive()) {
        log.warn(
            "Waited for {}ms for the {} fate executor operating on fate ops {} to stop, but it"
                + " is still running. Summary of run state of its threads: work finder:{}"
                + " transaction executor:{}",
            TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start), fate.getStore().type(),
            fateOps, workFinder.isAlive(), !transactionExecutor.isTerminated());
      }
    }
  }

  /**
   * This fate executor is defined as being alive if any of its threads are running
   */
  protected boolean isAlive() {
    return !transactionExecutor.isTerminated() || workFinder.isAlive();
  }

  protected ConcurrentLinkedQueue<Integer> getIdleCountHistory() {
    return idleCountHistory;
  }

  /**
   * A single thread that finds transactions to work on and queues them up. Do not want each worker
   * thread going to the store and looking for work as it would place more load on the store.
   */
  private class WorkFinder implements Runnable {

    @Override
    public void run() {
      while (fate.getKeepRunning().get() && !isShutdown()) {
        try {
          fate.getStore().runnable(() -> fate.getKeepRunning().get(), fateIdStatus -> {
            // The FateId with the fate operation 'fateOp' is workable by this FateExecutor if
            // 1) This FateExecutor is assigned to work on 'fateOp' ('fateOp' is in 'fateOps')
            // 2) The transaction was cancelled while NEW. This is an edge case that needs to be
            // handled since this is the only case where we will have a runnable transaction
            // that doesn't have a name. We allow any FateExecutor to work on this since this case
            // should be rare and won't put much load on any one FateExecutor
            var status = fateIdStatus.getStatus();
            var fateOp = fateIdStatus.getFateOperation().orElse(null);
            if ((fateOp != null && fateOps.contains(fateOp))
                || txCancelledWhileNew(status, fateOp)) {
              while (fate.getKeepRunning().get() && !isShutdown()) {
                try {
                  // The reason for calling transfer instead of queueing is avoid rescanning the
                  // storage layer and adding the same thing over and over. For example if all
                  // threads were busy, the queue size was 100, and there are three runnable things
                  // in the store. Do not want to keep scanning the store adding those same 3
                  // runnable things until the queue is full.
                  if (workQueue.tryTransfer(fateIdStatus.getFateId(), 100, MILLISECONDS)) {
                    break;
                  }
                } catch (InterruptedException e) {
                  throw new IllegalStateException(e);
                }
              }
            }
          });
        } catch (Exception e) {
          log.warn("Unexpected failure while attempting to find work for fate", e);
          workQueue.clear();
        }
      }

      log.debug(
          "FATE work finder for ops {} is gracefully exiting: either FATE is "
              + "being shutdown ({}) and therefore all FATE threads are being shutdown or the "
              + "FATE threads for the specific ops are being shutdown (due to FATE shutdown, "
              + "or due to FATE config changes) ({})",
          fateOps, !fate.getKeepRunning().get(), isShutdown());
    }

    private boolean txCancelledWhileNew(TStatus status, Fate.FateOperation fateOp) {
      if (fateOp == null) {
        // The only time a transaction should be runnable and not have a fate operation is if
        // it was cancelled while NEW (and now is FAILED_IN_PROGRESS)
        Preconditions.checkState(status == FAILED_IN_PROGRESS);
        return true;
      }
      return false;
    }
  }

  protected class TransactionRunner implements Runnable {

    // used to signal a TransactionRunner to stop in the case where there are too many running
    // i.e.,
    // 1. the property for the pool size decreased so we have to stop excess TransactionRunners
    // or
    // 2. this FateExecutor is no longer valid from config changes so we need to shutdown this
    // FateExecutor
    private final AtomicBoolean stop = new AtomicBoolean(false);
    private volatile Long threadId = null;

    private Optional<FateTxStore<T>> reserveFateTx() throws InterruptedException {
      idleWorkerCount.getAndIncrement();
      try {
        while (fate.getKeepRunning().get() && !stop.get()) {
          // Because of JDK-8301341 can not use poll w/ timeout until JDK 21+
          FateId unreservedFateId = workQueue.poll();

          if (unreservedFateId == null) {
            Thread.sleep(1);
            continue;
          }
          var optionalopStore = fate.getStore().tryReserve(unreservedFateId);
          if (optionalopStore.isPresent()) {
            return optionalopStore;
          }
        }
      } finally {
        idleWorkerCount.decrementAndGet();
      }

      return Optional.empty();
    }

    private boolean isInterruptedException(Throwable e) {
      if (e == null) {
        return false;
      }

      if (e instanceof InterruptedException) {
        return true;
      }

      for (Throwable suppressed : e.getSuppressed()) {
        if (isInterruptedException(suppressed)) {
          return true;
        }
      }

      return isInterruptedException(e.getCause());
    }

    @Override
    public void run() {
      runnerLog.trace("A TransactionRunner is starting for {} {} ", fate.getStore().type(),
          fateOps);
      threadId = Thread.currentThread().getId();
      try {
        while (fate.getKeepRunning().get() && !isShutdown() && !stop.get()) {
          FateTxStore<T> txStore = null;
          ExecutionState state = new ExecutionState();
          try {
            var optionalopStore = reserveFateTx();
            if (optionalopStore.isPresent()) {
              txStore = optionalopStore.orElseThrow();
            } else {
              continue;
            }
            state.status = txStore.getStatus();
            state.op = txStore.top();
            runnerLog.trace("Processing FATE transaction {} id: {} status: {}",
                state.op == null ? null : state.op.getName(), txStore.getID(), state.status);
            if (state.status == FAILED_IN_PROGRESS) {
              processFailed(txStore, state.op);
            } else if (state.status == SUBMITTED || state.status == IN_PROGRESS) {
              try {
                execute(txStore, state);
                // It's possible that a Fate operation impl
                // may not do the right thing with an
                // InterruptedException.
                if (Thread.currentThread().isInterrupted()) {
                  throw new InterruptedException("Fate Transaction Runner thread interrupted");
                }
                if (state.op != null && state.deferTime != 0) {
                  // The current op is not ready to execute
                  continue;
                }
              } catch (StackOverflowException e) {
                // the op that failed to push onto the stack was never executed, so no need to undo
                // it just transition to failed and undo the ops that executed
                transitionToFailed(txStore, e);
                continue;
              } catch (Exception e) {
                if (!isInterruptedException(e)) {
                  blockIfHadoopShutdown(txStore.getID(), e);
                  transitionToFailed(txStore, e);
                  continue;
                } else {
                  if (fate.getKeepRunning().get()) {
                    throw e;
                  } else {
                    // If we are shutting down then Fate.shutdown was called
                    // and ExecutorService.shutdownNow was called resulting
                    // in this exception. We will exit at the top of the loop.
                    Thread.interrupted();
                    continue;
                  }
                }
              }

              if (state.op == null) {
                // transaction is finished
                String ret = state.prevOp.getReturn();
                if (ret != null) {
                  txStore.setTransactionInfo(TxInfo.RETURN_VALUE, ret);
                }
                txStore.setStatus(SUCCESSFUL);
                doCleanUp(txStore);
              }
            }
          } catch (Exception e) {
            String name = state.op == null ? null : state.op.getName();
            FateId txid = txStore == null ? null : txStore.getID();
            if (isInterruptedException(e)) {
              if (fate.getKeepRunning().get()) {
                runnerLog.error(
                    "Uncaught InterruptedException in FATE runner thread processing {} id: {} status: {}",
                    name, txid, state.status, e);
              } else {
                // If we are shutting down then Fate.shutdown was called
                // and ExecutorService.shutdownNow was called resulting
                // in this exception. We will exit at the top of the loop,
                // so continue this loop iteration normally.
                Thread.interrupted();
              }
            } else {
              runnerLog.error(
                  "Uncaught exception in FATE runner thread processing {} id: {} status: {}", name,
                  txid, state.status, e);
            }
          } finally {
            if (txStore != null) {
              if (runnerLog.isTraceEnabled()) {
                String name = state.op == null ? null : state.op.getName();
                runnerLog.trace("Completed FATE transaction {} id: {} status: {}", name,
                    txStore.getID(), state.status);
              }
              txStore.unreserve(Duration.ofMillis(state.deferTime));
            }
          }
        }
      } finally {
        log.trace("A TransactionRunner is exiting for {} {}", fate.getStore().type(), fateOps);
        Preconditions.checkState(runningTxRunners.remove(this));
        threadId = null;
      }
    }

    private class ExecutionState {
      Repo<T> prevOp = null;
      Repo<T> op = null;
      long deferTime = 0;
      TStatus status;
    }

    // Executes as many steps of a fate operation as possible
    private void execute(final FateTxStore<T> txStore, final ExecutionState state)
        throws Exception {
      while (state.op != null && state.deferTime == 0) {
        state.deferTime = executeIsReady(txStore.getID(), state.op);

        if (state.deferTime == 0) {
          if (state.status == SUBMITTED) {
            txStore.setStatus(IN_PROGRESS);
            state.status = IN_PROGRESS;
          }

          state.prevOp = state.op;
          state.op = executeCall(txStore.getID(), state.op);

          if (state.op != null) {
            // persist the completion of this step before starting to run the next so in the case of
            // process death the completed steps are not rerun
            txStore.push(state.op);
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
    private void blockIfHadoopShutdown(FateId fateId, Exception e) {
      if (ShutdownUtil.isShutdownInProgress()) {

        if (e instanceof AcceptableException) {
          log.debug("Ignoring exception possibly caused by Hadoop Shutdown hook. {} ", fateId, e);
        } else if (isIOException(e)) {
          log.info("Ignoring exception likely caused by Hadoop Shutdown hook. {} ", fateId, e);
        } else {
          // sometimes code will catch an IOException caused by the hadoop shutdown hook and throw
          // another exception without setting the cause.
          log.warn("Ignoring exception possibly caused by Hadoop Shutdown hook. {} ", fateId, e);
        }

        while (true) {
          // Nothing is going to work well at this point, so why even try. Just wait for the end,
          // preventing this FATE thread from processing further work and likely failing.
          sleepUninterruptibly(1, MINUTES);
        }
      }
    }

    private void transitionToFailed(FateTxStore<T> txStore, Exception e) {
      final String msg = "Failed to execute Repo " + txStore.getID();
      // Certain FATE ops that throw exceptions don't need to be propagated up to the Monitor
      // as a warning. They're a normal, handled failure condition.
      if (e instanceof AcceptableException) {
        var tableOpEx = (AcceptableThriftTableOperationException) e;
        log.info("{} for table:{}({}) saw acceptable exception: {}", msg, tableOpEx.getTableName(),
            tableOpEx.getTableId(), tableOpEx.getDescription());
      } else {
        log.warn(msg, e);
      }
      txStore.setTransactionInfo(TxInfo.EXCEPTION, e);
      txStore.setStatus(FAILED_IN_PROGRESS);
      log.info("Updated status for Repo with {} to FAILED_IN_PROGRESS", txStore.getID());
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

    private void undo(FateId fateId, Repo<T> op) {
      try {
        op.undo(fateId, environment);
      } catch (Exception e) {
        log.warn("Failed to undo Repo, " + fateId, e);
      }
    }

    protected boolean flagStop() {
      boolean setStop = stop.compareAndSet(false, true);
      if (setStop) {
        runnerLog.trace("set stop for {}", threadId);
      }
      return setStop;
    }

    protected boolean isFlaggedToStop() {
      return stop.get();
    }

    @Override
    public String toString() {
      return "threadId:" + threadId + " stop:" + stop.get();
    }

  }

  protected long executeIsReady(FateId fateId, Repo<T> op) throws Exception {
    var startTime = Timer.startNew();
    var deferTime = op.isReady(fateId, environment);
    log.debug("Running {}.isReady() {} took {} ms and returned {}", op.getName(), fateId,
        startTime.elapsed(MILLISECONDS), deferTime);
    return deferTime;
  }

  protected Repo<T> executeCall(FateId fateId, Repo<T> op) throws Exception {
    var startTime = Timer.startNew();
    var next = op.call(fateId, environment);
    log.debug("Running {}.call() {} took {} ms and returned {}", op.getName(), fateId,
        startTime.elapsed(MILLISECONDS), next == null ? "null" : next.getName());

    return next;
  }

  @Override
  public String toString() {
    return String.format("FateExecutor:{FateOps=%s,Name=%s,PoolSize:%s,TransactionRunners:%s}",
        fateOps, name, runningTxRunners.size(), runningTxRunners);
  }
}

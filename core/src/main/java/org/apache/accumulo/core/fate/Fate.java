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

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.accumulo.core.fate.ReadOnlyFateStore.TStatus.FAILED;
import static org.apache.accumulo.core.fate.ReadOnlyFateStore.TStatus.FAILED_IN_PROGRESS;
import static org.apache.accumulo.core.fate.ReadOnlyFateStore.TStatus.NEW;
import static org.apache.accumulo.core.fate.ReadOnlyFateStore.TStatus.SUBMITTED;
import static org.apache.accumulo.core.fate.ReadOnlyFateStore.TStatus.SUCCESSFUL;
import static org.apache.accumulo.core.fate.ReadOnlyFateStore.TStatus.UNKNOWN;
import static org.apache.accumulo.core.util.threads.ThreadPoolNames.META_DEAD_RESERVATION_CLEANER_POOL;
import static org.apache.accumulo.core.util.threads.ThreadPoolNames.USER_DEAD_RESERVATION_CLEANER_POOL;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.fate.FateStore.FateTxStore;
import org.apache.accumulo.core.fate.ReadOnlyFateStore.TStatus;
import org.apache.accumulo.core.logging.FateLogger;
import org.apache.accumulo.core.manager.thrift.TFateOperation;
import org.apache.accumulo.core.util.UtilWaitThread;
import org.apache.accumulo.core.util.threads.ThreadPools;
import org.apache.thrift.TApplicationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.gson.JsonParser;

/**
 * Fault tolerant executor
 */
public class Fate<T> {

  private static final Logger log = LoggerFactory.getLogger(Fate.class);

  private final FateStore<T> store;
  private final ScheduledThreadPoolExecutor fatePoolsWatcher;
  private final AtomicInteger needMoreThreadsWarnCount = new AtomicInteger(0);
  private final ExecutorService deadResCleanerExecutor;

  private static final EnumSet<TStatus> FINISHED_STATES = EnumSet.of(FAILED, SUCCESSFUL, UNKNOWN);
  public static final Duration INITIAL_DELAY = Duration.ofSeconds(3);
  private static final Duration DEAD_RES_CLEANUP_DELAY = Duration.ofMinutes(3);
  private static final Duration POOL_WATCHER_DELAY = Duration.ofSeconds(30);

  private final AtomicBoolean keepRunning = new AtomicBoolean(true);
  private final Set<FateExecutor<T>> fateExecutors = new HashSet<>();

  public enum TxInfo {
    TX_NAME, AUTO_CLEAN, EXCEPTION, TX_AGEOFF, RETURN_VALUE
  }

  public enum FateOperation {
    COMMIT_COMPACTION(null),
    NAMESPACE_CREATE(TFateOperation.NAMESPACE_CREATE),
    NAMESPACE_DELETE(TFateOperation.NAMESPACE_DELETE),
    NAMESPACE_RENAME(TFateOperation.NAMESPACE_RENAME),
    SHUTDOWN_TSERVER(null),
    SYSTEM_SPLIT(null),
    TABLE_BULK_IMPORT2(TFateOperation.TABLE_BULK_IMPORT2),
    TABLE_CANCEL_COMPACT(TFateOperation.TABLE_CANCEL_COMPACT),
    TABLE_CLONE(TFateOperation.TABLE_CLONE),
    TABLE_COMPACT(TFateOperation.TABLE_COMPACT),
    TABLE_CREATE(TFateOperation.TABLE_CREATE),
    TABLE_DELETE(TFateOperation.TABLE_DELETE),
    TABLE_DELETE_RANGE(TFateOperation.TABLE_DELETE_RANGE),
    TABLE_EXPORT(TFateOperation.TABLE_EXPORT),
    TABLE_IMPORT(TFateOperation.TABLE_IMPORT),
    TABLE_MERGE(TFateOperation.TABLE_MERGE),
    TABLE_OFFLINE(TFateOperation.TABLE_OFFLINE),
    TABLE_ONLINE(TFateOperation.TABLE_ONLINE),
    TABLE_RENAME(TFateOperation.TABLE_RENAME),
    TABLE_SPLIT(TFateOperation.TABLE_SPLIT),
    TABLE_TABLET_AVAILABILITY(TFateOperation.TABLE_TABLET_AVAILABILITY);

    private final TFateOperation top;
    private static final Set<FateOperation> nonThriftOps =
        Collections.unmodifiableSet(EnumSet.of(COMMIT_COMPACTION, SHUTDOWN_TSERVER, SYSTEM_SPLIT));
    private static final Set<FateOperation> allUserFateOps =
        Collections.unmodifiableSet(EnumSet.allOf(FateOperation.class));
    private static final Set<FateOperation> allMetaFateOps =
        Collections.unmodifiableSet(EnumSet.of(Fate.FateOperation.TABLE_COMPACT,
            Fate.FateOperation.TABLE_CANCEL_COMPACT, Fate.FateOperation.COMMIT_COMPACTION,
            Fate.FateOperation.TABLE_MERGE, Fate.FateOperation.TABLE_DELETE_RANGE,
            Fate.FateOperation.TABLE_SPLIT, Fate.FateOperation.SYSTEM_SPLIT));

    FateOperation(TFateOperation top) {
      this.top = top;
    }

    public static FateOperation fromThrift(TFateOperation top) {
      return FateOperation.valueOf(top.name());
    }

    public static Set<FateOperation> getNonThriftOps() {
      return nonThriftOps;
    }

    public TFateOperation toThrift() {
      if (top == null) {
        throw new IllegalStateException(this + " does not have an equivalent thrift form");
      }
      return top;
    }

    public static Set<FateOperation> getAllUserFateOps() {
      return allUserFateOps;
    }

    public static Set<FateOperation> getAllMetaFateOps() {
      return allMetaFateOps;
    }
  }

  // The fate pools watcher:
  // - Maintains a TransactionRunner per available thread per pool/FateExecutor. Does so by
  // periodically checking the pools for an inactive thread (i.e., a thread running a
  // TransactionRunner died or the pool size was increased in the property), resizing the pool and
  // submitting new runners as needed. Also safely stops the necessary number of TransactionRunners
  // if the pool size in the property was decreased.
  // - Warns the user to consider increasing the pool size (or splitting the fate ops assigned to
  // that pool into separate pools) for any pool that does not often have any idle threads.
  private class FatePoolsWatcher implements Runnable {
    private final T environment;
    private final AccumuloConfiguration conf;

    private FatePoolsWatcher(T environment, AccumuloConfiguration conf) {
      this.environment = environment;
      this.conf = conf;
    }

    @Override
    public void run() {
      // Read from the config here and here only. Must avoid reading the same property from the
      // config more than once since it can change at any point in this execution
      var poolConfigs = getPoolConfigurations(conf);
      var idleCheckIntervalMillis = conf.getTimeInMillis(Property.MANAGER_FATE_IDLE_CHECK_INTERVAL);

      // shutdown task: shutdown fate executors whose set of fate operations are no longer present
      // in the config
      synchronized (fateExecutors) {
        final var fateExecutorsIter = fateExecutors.iterator();
        while (fateExecutorsIter.hasNext()) {
          var fateExecutor = fateExecutorsIter.next();

          // if this fate executors set of fate ops is no longer present in the config...
          if (!poolConfigs.containsKey(fateExecutor.getFateOps())) {
            if (!fateExecutor.isShutdown()) {
              log.debug("The config for {} has changed invalidating {}. Gracefully shutting down "
                  + "the FateExecutor.", getFateConfigProp(), fateExecutor);
              fateExecutor.initiateShutdown();
            } else if (fateExecutor.isShutdown() && fateExecutor.isAlive()) {
              log.debug("{} has been shutdown, but is still actively working on transactions.",
                  fateExecutor);
            } else if (fateExecutor.isShutdown() && !fateExecutor.isAlive()) {
              log.debug("{} has been shutdown and all threads have safely terminated.",
                  fateExecutor);
              fateExecutorsIter.remove();
            }
          }
        }
      }

      // replacement task: at this point, the existing FateExecutors that were invalidated by the
      // config changes have started shutdown or finished shutdown. Now create any new replacement
      // FateExecutors needed
      for (var poolConfig : poolConfigs.entrySet()) {
        var configFateOps = poolConfig.getKey();
        var configPoolSize = poolConfig.getValue();
        synchronized (fateExecutors) {
          if (fateExecutors.stream().map(FateExecutor::getFateOps)
              .noneMatch(fo -> fo.equals(configFateOps))) {
            fateExecutors
                .add(new FateExecutor<>(Fate.this, environment, configFateOps, configPoolSize));
          }
        }
      }

      // resize task: For each fate executor, resize the pool to match the config as necessary and
      // submit new TransactionRunners if the pool grew, stop TransactionRunners if the pool
      // shrunk, and potentially suggest resizing the pool if the load is consistently high.
      synchronized (fateExecutors) {
        for (var fateExecutor : fateExecutors) {
          if (fateExecutor.isShutdown()) {
            continue;
          }
          final var pool = fateExecutor.getTransactionExecutor();
          final var poolName = fateExecutor.getPoolName();
          final var runningTxRunners = fateExecutor.getRunningTxRunners();
          final int configured = poolConfigs.get(fateExecutor.getFateOps());
          ThreadPools.resizePool(pool, () -> configured, poolName);
          final int needed = configured - runningTxRunners.size();
          if (needed > 0) {
            // If the pool grew, then ensure that there is a TransactionRunner for each thread
            for (int i = 0; i < needed; i++) {
              try {
                pool.execute(fateExecutor.new TransactionRunner());
              } catch (RejectedExecutionException e) {
                // RejectedExecutionException could be shutting down
                if (pool.isShutdown()) {
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
            fateExecutor.getIdleCountHistory().clear();
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
            var idleCountHistory = fateExecutor.getIdleCountHistory();
            var workQueue = fateExecutor.getWorkQueue();
            var fateConfigProp = getFateConfigProp();

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
                  needMoreThreadsWarnCount.incrementAndGet();
                  log.warn(
                      "All {} Fate threads working on the fate ops {} appear to be busy for "
                          + "the last {} minutes. Consider increasing the value for the "
                          + "entry in the property {} or splitting the fate ops across "
                          + "multiple entries/pools.",
                      store.type(), fateExecutor.getFateOps(), interval, fateConfigProp.getKey());
                  // Clear the history so that we don't log for interval minutes.
                  idleCountHistory.clear();
                } else {
                  while (idleCountHistory.size() >= interval * 2) {
                    idleCountHistory.remove();
                  }
                }
              }
              idleCountHistory.add(workQueue.getWaitingConsumerCount());
            }
          }
        }
      }
    }
  }

  /**
   * A thread that finds reservations held by dead processes and unreserves them
   */
  private class DeadReservationCleaner implements Runnable {
    @Override
    public void run() {
      if (keepRunning.get()) {
        store.deleteDeadReservations();
      }
    }
  }

  /**
   * Creates a Fault-tolerant executor for the given store type.
   *
   * @param runDeadResCleaner Whether this FATE should run a dead reservation cleaner. The real
   *        FATEs need have a cleaner, but may be undesirable in testing.
   * @param toLogStrFunc A function that converts Repo to Strings that are suitable for logging
   */
  public Fate(T environment, FateStore<T> store, boolean runDeadResCleaner,
      Function<Repo<T>,String> toLogStrFunc, AccumuloConfiguration conf) {
    this.store = FateLogger.wrap(store, toLogStrFunc, false);

    this.fatePoolsWatcher =
        ThreadPools.getServerThreadPools().createGeneralScheduledExecutorService(conf);
    ThreadPools.watchCriticalScheduledTask(
        fatePoolsWatcher.scheduleWithFixedDelay(new FatePoolsWatcher(environment, conf),
            INITIAL_DELAY.toSeconds(), getPoolWatcherDelay().toSeconds(), SECONDS));

    ScheduledExecutorService deadResCleanerExecutor = null;
    if (runDeadResCleaner) {
      // Create a dead reservation cleaner for this store that will periodically clean up
      // reservations held by dead processes, if they exist.
      deadResCleanerExecutor = ThreadPools.getServerThreadPools().createScheduledExecutorService(1,
          store.type() == FateInstanceType.USER ? USER_DEAD_RESERVATION_CLEANER_POOL.poolName
              : META_DEAD_RESERVATION_CLEANER_POOL.poolName);
      ScheduledFuture<?> deadReservationCleaner =
          deadResCleanerExecutor.scheduleWithFixedDelay(new DeadReservationCleaner(),
              INITIAL_DELAY.toSeconds(), getDeadResCleanupDelay().toSeconds(), SECONDS);
      ThreadPools.watchCriticalScheduledTask(deadReservationCleaner);
    }
    this.deadResCleanerExecutor = deadResCleanerExecutor;

    startFateExecutors(environment, conf, fateExecutors);
  }

  protected void startFateExecutors(T environment, AccumuloConfiguration conf,
      Set<FateExecutor<T>> fateExecutors) {
    for (var poolConf : getPoolConfigurations(conf).entrySet()) {
      // no fate threads are running at this point; fine not to synchronize
      fateExecutors
          .add(new FateExecutor<>(this, environment, poolConf.getKey(), poolConf.getValue()));
    }
  }

  /**
   * Returns a map of the current pool configurations as set in the given config. Each key is a set
   * of fate operations and each value is an integer for the number of threads assigned to work
   * those fate operations.
   */
  protected Map<Set<FateOperation>,Integer> getPoolConfigurations(AccumuloConfiguration conf) {
    Map<Set<FateOperation>,Integer> poolConfigs = new HashMap<>();
    final var json = JsonParser.parseString(conf.get(getFateConfigProp())).getAsJsonObject();

    for (var entry : json.entrySet()) {
      var key = entry.getKey();
      var val = entry.getValue().getAsInt();
      var fateOpsStrArr = key.split(",");
      Set<FateOperation> fateOpsSet = Arrays.stream(fateOpsStrArr).map(FateOperation::valueOf)
          .collect(Collectors.toCollection(TreeSet::new));

      poolConfigs.put(fateOpsSet, val);
    }

    return poolConfigs;
  }

  protected AtomicBoolean getKeepRunning() {
    return keepRunning;
  }

  protected FateStore<T> getStore() {
    return store;
  }

  protected Property getFateConfigProp() {
    return this.store.type() == FateInstanceType.USER ? Property.MANAGER_FATE_USER_CONFIG
        : Property.MANAGER_FATE_META_CONFIG;
  }

  public Duration getDeadResCleanupDelay() {
    return DEAD_RES_CLEANUP_DELAY;
  }

  public Duration getPoolWatcherDelay() {
    return POOL_WATCHER_DELAY;
  }

  /**
   * Returns the number of TransactionRunners active for the FateExecutor assigned to work on the
   * given set of fate operations. "Active" meaning it is waiting for a transaction or working on
   * one. Returns 0 if no such FateExecutor exists. This should only be used for testing
   */
  @VisibleForTesting
  public int getTxRunnersActive(Set<FateOperation> fateOps) {
    synchronized (fateExecutors) {
      for (var fateExecutor : fateExecutors) {
        if (fateExecutor.getFateOps().equals(fateOps)) {
          return fateExecutor.getRunningTxRunners().size();
        }
      }
    }
    return 0;
  }

  /**
   * Returns the total number of TransactionRunners active across all FateExecutors. This should
   * only be used for testing
   */
  @VisibleForTesting
  public int getTotalTxRunnersActive() {
    synchronized (fateExecutors) {
      return fateExecutors.stream().mapToInt(fe -> fe.getRunningTxRunners().size()).sum();
    }
  }

  /**
   * Returns how many times it is warned that a pool size should be increased. This should only be
   * used for testing
   */
  @VisibleForTesting
  public int getNeedMoreThreadsWarnCount() {
    return needMoreThreadsWarnCount.get();
  }

  // get a transaction id back to the requester before doing any work
  public FateId startTransaction() {
    return store.create();
  }

  public void seedTransaction(FateOperation txName, FateKey fateKey, Repo<T> repo,
      boolean autoCleanUp) {
    store.seedTransaction(txName, fateKey, repo, autoCleanUp);
  }

  // start work in the transaction.. it is safe to call this
  // multiple times for a transaction... but it will only seed once
  public void seedTransaction(FateOperation txName, FateId fateId, Repo<T> repo,
      boolean autoCleanUp, String goalMessage) {
    log.info("Seeding {} {}", fateId, goalMessage);
    store.seedTransaction(txName, fateId, repo, autoCleanUp);
  }

  // check on the transaction
  public TStatus waitForCompletion(FateId fateId) {
    return store.read(fateId).waitForStatusChange(FINISHED_STATES);
  }

  /**
   * Attempts to cancel a running Fate transaction
   *
   * @param fateId fate transaction id
   * @return true if transaction transitioned to a failed state or already in a completed state,
   *         false otherwise
   */
  public boolean cancel(FateId fateId) {
    for (int retries = 0; retries < 5; retries++) {
      Optional<FateTxStore<T>> optionalTxStore = store.tryReserve(fateId);
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
                fateId);
            return true;
          } else {
            log.info("{} cancelled by user but already in progress or finished state", fateId);
            return false;
          }
        } finally {
          txStore.unreserve(Duration.ZERO);
        }
      } else {
        // reserved, lets retry.
        UtilWaitThread.sleep(500);
      }
    }
    log.info("Unable to reserve transaction {} to cancel it", fateId);
    return false;
  }

  // resource cleanup
  public void delete(FateId fateId) {
    FateTxStore<T> txStore = store.reserve(fateId);
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
          throw new IllegalStateException("Can not delete in progress transaction " + fateId);
        case UNKNOWN:
          // nothing to do, it does not exist
          break;
      }
    } finally {
      txStore.unreserve(Duration.ZERO);
    }
  }

  public String getReturn(FateId fateId) {
    FateTxStore<T> txStore = store.reserve(fateId);
    try {
      if (txStore.getStatus() != SUCCESSFUL) {
        throw new IllegalStateException(
            "Tried to get exception when transaction " + fateId + " not in successful state");
      }
      return (String) txStore.getTransactionInfo(TxInfo.RETURN_VALUE);
    } finally {
      txStore.unreserve(Duration.ZERO);
    }
  }

  // get reportable failures
  public Exception getException(FateId fateId) {
    FateTxStore<T> txStore = store.reserve(fateId);
    try {
      if (txStore.getStatus() != FAILED) {
        throw new IllegalStateException(
            "Tried to get exception when transaction " + fateId + " not in failed state");
      }
      return (Exception) txStore.getTransactionInfo(TxInfo.EXCEPTION);
    } finally {
      txStore.unreserve(Duration.ZERO);
    }
  }

  /**
   * Lists transctions for a given fate key type.
   */
  public Stream<FateKey> list(FateKey.FateKeyType type) {
    return store.list(type);
  }

  /**
   * Initiates shutdown of background threads and optionally waits on them.
   */
  public void shutdown(long timeout, TimeUnit timeUnit) {
    log.info("Shutting down {} FATE", store.type());

    if (keepRunning.compareAndSet(true, false)) {
      synchronized (fateExecutors) {
        for (var fateExecutor : fateExecutors) {
          fateExecutor.initiateShutdown();
        }
      }
      if (deadResCleanerExecutor != null) {
        deadResCleanerExecutor.shutdown();
      }
      fatePoolsWatcher.shutdown();
    }

    if (timeout > 0) {
      long start = System.nanoTime();
      try {
        waitForAllFateExecShutdown(start, timeout, timeUnit);
        waitForDeadResCleanerShutdown(start, timeout, timeUnit);
        waitForFatePoolsWatcherShutdown(start, timeout, timeUnit);

        if (anyFateExecutorIsAlive() || deadResCleanerIsAlive() || fatePoolsWatcherIsAlive()) {
          log.warn(
              "Waited for {}ms for all fate {} background threads to stop, but some are still running. "
                  + "fate executor threads:{} dead reservation cleaner thread:{} "
                  + "fate pools watcher thread:{}",
              TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start), store.type(),
              anyFateExecutorIsAlive(), deadResCleanerIsAlive(), fatePoolsWatcherIsAlive());
        }
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }

    // interrupt the background threads
    synchronized (fateExecutors) {
      for (var fateExecutor : fateExecutors) {
        fateExecutor.shutdownNow();
        fateExecutor.getIdleCountHistory().clear();
      }
    }
    if (deadResCleanerExecutor != null) {
      deadResCleanerExecutor.shutdownNow();
    }
    fatePoolsWatcher.shutdownNow();
  }

  private boolean anyFateExecutorIsAlive() {
    synchronized (fateExecutors) {
      return fateExecutors.stream().anyMatch(FateExecutor::isAlive);
    }
  }

  private boolean deadResCleanerIsAlive() {
    return deadResCleanerExecutor != null && !deadResCleanerExecutor.isTerminated();
  }

  private boolean fatePoolsWatcherIsAlive() {
    return !fatePoolsWatcher.isTerminated();
  }

  private void waitForAllFateExecShutdown(long start, long timeout, TimeUnit timeUnit)
      throws InterruptedException {
    synchronized (fateExecutors) {
      for (var fateExecutor : fateExecutors) {
        fateExecutor.waitForShutdown(start, timeout, timeUnit);
      }
    }
  }

  private void waitForDeadResCleanerShutdown(long start, long timeout, TimeUnit timeUnit)
      throws InterruptedException {
    while (((System.nanoTime() - start) < timeUnit.toNanos(timeout)) && deadResCleanerIsAlive()) {
      if (deadResCleanerExecutor != null && !deadResCleanerExecutor.awaitTermination(1, SECONDS)) {
        log.debug("Fate {} is waiting for dead reservation cleaner thread to terminate",
            store.type());
      }
    }
  }

  private void waitForFatePoolsWatcherShutdown(long start, long timeout, TimeUnit timeUnit)
      throws InterruptedException {
    while (((System.nanoTime() - start) < timeUnit.toNanos(timeout)) && fatePoolsWatcherIsAlive()) {
      if (!fatePoolsWatcher.awaitTermination(1, SECONDS)) {
        log.debug("Fate {} is waiting for fate pools watcher thread to terminate", store.type());
      }
    }
  }
}

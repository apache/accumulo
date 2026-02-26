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
import static org.apache.accumulo.core.util.threads.ThreadPoolNames.META_DEAD_RESERVATION_CLEANER_POOL;
import static org.apache.accumulo.core.util.threads.ThreadPoolNames.USER_DEAD_RESERVATION_CLEANER_POOL;

import java.time.Duration;
import java.util.AbstractMap;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.logging.FateLogger;
import org.apache.accumulo.core.manager.thrift.TFateOperation;
import org.apache.accumulo.core.util.threads.ThreadPools;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.gson.JsonParser;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * Fault tolerant executor
 */
@SuppressFBWarnings(value = "CT_CONSTRUCTOR_THROW",
    justification = "Constructor validation is required for proper initialization")
public class Fate<T> extends FateClient<T> {

  static final Logger log = LoggerFactory.getLogger(Fate.class);

  private final FateStore<T> store;
  private final ScheduledFuture<?> fatePoolsWatcherFuture;
  private final AtomicInteger needMoreThreadsWarnCount = new AtomicInteger(0);
  private final ExecutorService deadResCleanerExecutor;

  public static final Duration INITIAL_DELAY = Duration.ofSeconds(3);
  private static final Duration DEAD_RES_CLEANUP_DELAY = Duration.ofMinutes(3);
  public static final Duration POOL_WATCHER_DELAY = Duration.ofSeconds(30);

  private final AtomicBoolean keepRunning = new AtomicBoolean(true);
  // Visible for FlakyFate test object
  protected final Set<FateExecutor<T>> fateExecutors = new HashSet<>();

  public enum TxInfo {
    FATE_OP, AUTO_CLEAN, EXCEPTION, TX_AGEOFF, RETURN_VALUE
  }

  public enum FateOperation {
    COMMIT_COMPACTION(null),
    NAMESPACE_CREATE(TFateOperation.NAMESPACE_CREATE),
    NAMESPACE_DELETE(TFateOperation.NAMESPACE_DELETE),
    NAMESPACE_RENAME(TFateOperation.NAMESPACE_RENAME),
    SHUTDOWN_TSERVER(null),
    SYSTEM_SPLIT(null),
    SYSTEM_MERGE(null),
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
    private static final Set<FateOperation> nonThriftOps = Arrays.stream(FateOperation.values())
        .filter(fateOp -> fateOp.top == null).collect(Collectors.toUnmodifiableSet());
    private static final Set<FateOperation> allUserFateOps =
        Collections.unmodifiableSet(EnumSet.allOf(FateOperation.class));
    private static final Set<FateOperation> allMetaFateOps =
        Collections.unmodifiableSet(EnumSet.allOf(FateOperation.class));

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
      final var poolConfigs = getPoolConfigurations(conf, store.type());
      final var idleCheckIntervalMillis =
          conf.getTimeInMillis(Property.MANAGER_FATE_IDLE_CHECK_INTERVAL);

      // shutdown task: shutdown fate executors whose set of fate operations are no longer present
      // in the config
      synchronized (fateExecutors) {
        final var fateExecutorsIter = fateExecutors.iterator();
        while (fateExecutorsIter.hasNext()) {
          var fateExecutor = fateExecutorsIter.next();

          // if this fate executors set of fate ops is no longer present in the config OR
          // this fate executor was renamed in the config
          if (!poolConfigs.containsKey(fateExecutor.getFateOps()) || !poolConfigs
              .get(fateExecutor.getFateOps()).getKey().equals(fateExecutor.getName())) {
            if (!fateExecutor.isShutdown()) {
              log.debug(
                  "[{}] The config for {} has changed invalidating {}. Gracefully shutting down "
                      + "the FateExecutor.",
                  store.type(), getFateConfigProp(store.type()), fateExecutor);
              fateExecutor.initiateShutdown();
            } else if (fateExecutor.isShutdown() && fateExecutor.isAlive()) {
              log.debug("[{}] {} has been shutdown, but is still actively working on transactions.",
                  store.type(), fateExecutor);
            } else if (fateExecutor.isShutdown() && !fateExecutor.isAlive()) {
              log.debug("[{}] {} has been shutdown and all threads have safely terminated.",
                  store.type(), fateExecutor);
              fateExecutorsIter.remove();
            }
          }
        }
      }

      // replacement task: at this point, the existing FateExecutors that were invalidated by the
      // config changes have started shutdown or finished shutdown. Now create any new replacement
      // FateExecutors needed
      for (var poolConfig : poolConfigs.entrySet()) {
        Set<FateOperation> fateOps = poolConfig.getKey();
        Map.Entry<String,Integer> fateExecNameAndPoolSize = poolConfig.getValue();
        String fateExecutorName = fateExecNameAndPoolSize.getKey();
        int poolSize = fateExecNameAndPoolSize.getValue();
        synchronized (fateExecutors) {
          if (fateExecutors.stream().noneMatch(
              fe -> fe.getFateOps().equals(fateOps) && fe.getName().equals(fateExecutorName))) {
            log.debug("[{}] Adding FateExecutor for {} with {} threads", store.type(), fateOps,
                poolSize);
            fateExecutors.add(
                new FateExecutor<>(Fate.this, environment, fateOps, poolSize, fateExecutorName));
          }
        }
      }

      // resize task: see description for FateExecutor.resizeFateExecutor
      synchronized (fateExecutors) {
        for (var fateExecutor : fateExecutors) {
          if (fateExecutor.isShutdown()) {
            continue;
          }
          fateExecutor.resizeFateExecutor(poolConfigs, idleCheckIntervalMillis);
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
      Function<Repo<T>,String> toLogStrFunc, AccumuloConfiguration conf,
      ScheduledThreadPoolExecutor genSchedExecutor) {
    super(store, toLogStrFunc);
    this.store = FateLogger.wrap(store, toLogStrFunc, false);

    fatePoolsWatcherFuture =
        genSchedExecutor.scheduleWithFixedDelay(new FatePoolsWatcher(environment, conf),
            INITIAL_DELAY.toSeconds(), getPoolWatcherDelay().toSeconds(), SECONDS);
    ThreadPools.watchCriticalScheduledTask(fatePoolsWatcherFuture);

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
  }

  /**
   * Returns a map of the current pool configurations as set in the given config. Each key is a set
   * of fate operations and each value is a map entry with key = fate executor name and value = pool
   * size
   */
  @VisibleForTesting
  public static Map<Set<FateOperation>,Map.Entry<String,Integer>>
      getPoolConfigurations(AccumuloConfiguration conf, FateInstanceType type) {
    Map<Set<FateOperation>,Map.Entry<String,Integer>> poolConfigs = new HashMap<>();
    final var json = JsonParser.parseString(conf.get(getFateConfigProp(type))).getAsJsonObject();

    for (var entry : json.entrySet()) {
      String fateExecutorName = entry.getKey();
      var poolConfig = entry.getValue().getAsJsonObject().entrySet().iterator().next();
      String fateOpsStr = poolConfig.getKey();
      int poolSize = poolConfig.getValue().getAsInt();
      String[] fateOpsStrArr = fateOpsStr.split(",");
      Set<FateOperation> fateOpsSet = Arrays.stream(fateOpsStrArr).map(FateOperation::valueOf)
          .collect(Collectors.toCollection(TreeSet::new));

      poolConfigs.put(fateOpsSet,
          new AbstractMap.SimpleImmutableEntry<>(fateExecutorName, poolSize));
    }

    return poolConfigs;
  }

  protected AtomicBoolean getKeepRunning() {
    return keepRunning;
  }

  protected FateStore<T> getStore() {
    return store;
  }

  protected static Property getFateConfigProp(FateInstanceType type) {
    return type == FateInstanceType.USER ? Property.MANAGER_FATE_USER_CONFIG
        : Property.MANAGER_FATE_META_CONFIG;
  }

  /**
   * Exists for overrides in test code. Internal access to this field needs to be through this
   * getter
   */
  public Duration getDeadResCleanupDelay() {
    return DEAD_RES_CLEANUP_DELAY;
  }

  /**
   * Exists for overrides in test code. Internal access to this field needs to be through this
   * getter
   */
  public Duration getPoolWatcherDelay() {
    return POOL_WATCHER_DELAY;
  }

  public Set<FateExecutor<T>> getFateExecutors() {
    return fateExecutors;
  }

  /**
   * Returns the number of TransactionRunners active for the FateExecutor assigned to work on the
   * given set of fate operations. "Active" meaning it is waiting for a transaction to work on or
   * actively working on one. Returns 0 if no such FateExecutor exists. This should only be used for
   * testing
   */
  @VisibleForTesting
  public int getTxRunnersActive(Set<FateOperation> fateOps) {
    synchronized (fateExecutors) {
      for (var fateExecutor : fateExecutors) {
        if (fateExecutor.getFateOps().equals(fateOps)) {
          return fateExecutor.getNumRunningTxRunners();
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
      return fateExecutors.stream().mapToInt(FateExecutor::getNumRunningTxRunners).sum();
    }
  }

  /**
   * Returns how many times it is warned that a pool size should be increased. Should only be used
   * for testing or in {@link FateExecutor}
   */
  @VisibleForTesting
  public AtomicInteger getNeedMoreThreadsWarnCount() {
    return needMoreThreadsWarnCount;
  }

  /**
   * Initiates shutdown of background threads that run fate operations and cleanup fate data and
   * optionally waits on them. Leaves the fate object in a state where it can still update and read
   * fate data, like add a new fate operation or get the status of an existing fate operation.
   */
  public void shutdown(long timeout, TimeUnit timeUnit) {
    log.info("Shutting down {} FATE, waiting: {} seconds", store.type(),
        TimeUnit.SECONDS.convert(timeout, timeUnit));
    // important this is set before shutdownNow is called as the background
    // threads will check this to see if shutdown related errors should be ignored.
    if (keepRunning.compareAndSet(true, false)) {
      synchronized (fateExecutors) {
        for (var fateExecutor : fateExecutors) {
          fateExecutor.initiateShutdown();
        }
      }
      if (deadResCleanerExecutor != null) {
        deadResCleanerExecutor.shutdown();
      }
      fatePoolsWatcherFuture.cancel(false);
    }

    if (timeout > 0) {
      long start = System.nanoTime();
      try {
        waitForAllFateExecShutdown(start, timeout, timeUnit);
        waitForDeadResCleanerShutdown(start, timeout, timeUnit);

        if (anyFateExecutorIsAlive() || deadResCleanerIsAlive()) {
          log.warn(
              "Waited for {}ms for all fate {} background threads to stop, but some are still running. "
                  + "fate executor threads:{} dead reservation cleaner thread:{} ",
              TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start), store.type(),
              anyFateExecutorIsAlive(), deadResCleanerIsAlive());
        }
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }

    // interrupt the background threads
    synchronized (fateExecutors) {
      var fateExecutorsIter = fateExecutors.iterator();
      while (fateExecutorsIter.hasNext()) {
        var fateExecutor = fateExecutorsIter.next();
        fateExecutor.shutdownNow();
        fateExecutor.getIdleCountHistory().clear();
        fateExecutorsIter.remove();
      }
    }
    if (deadResCleanerExecutor != null) {
      deadResCleanerExecutor.shutdownNow();
    }
  }

  /**
   * Initiates shutdown of all fate threads and prevents reads and updates of fates persisted data.
   */
  public void close() {
    shutdown(0, SECONDS);
    store.close();
  }

  private boolean anyFateExecutorIsAlive() {
    synchronized (fateExecutors) {
      return fateExecutors.stream().anyMatch(FateExecutor::isAlive);
    }
  }

  private boolean deadResCleanerIsAlive() {
    return deadResCleanerExecutor != null && !deadResCleanerExecutor.isTerminated();
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
}

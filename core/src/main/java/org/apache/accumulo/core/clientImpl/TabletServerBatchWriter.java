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
package org.apache.accumulo.core.clientImpl;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;

import java.io.IOException;
import java.lang.management.CompilationMXBean;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Durability;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.TableDeletedException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.TableOfflineException;
import org.apache.accumulo.core.client.TimedOutException;
import org.apache.accumulo.core.clientImpl.TabletLocator.TabletServerMutations;
import org.apache.accumulo.core.clientImpl.thrift.SecurityErrorCode;
import org.apache.accumulo.core.clientImpl.thrift.ThriftSecurityException;
import org.apache.accumulo.core.constraints.Violations;
import org.apache.accumulo.core.data.ConstraintViolationSummary;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.data.TabletId;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.dataImpl.TabletIdImpl;
import org.apache.accumulo.core.dataImpl.thrift.TMutation;
import org.apache.accumulo.core.dataImpl.thrift.UpdateErrors;
import org.apache.accumulo.core.rpc.ThriftUtil;
import org.apache.accumulo.core.rpc.clients.ThriftClientTypes;
import org.apache.accumulo.core.tabletserver.thrift.ConstraintViolationException;
import org.apache.accumulo.core.tabletserver.thrift.NotServingTabletException;
import org.apache.accumulo.core.tabletserver.thrift.TabletClientService;
import org.apache.accumulo.core.trace.TraceUtil;
import org.apache.accumulo.core.trace.thrift.TInfo;
import org.apache.accumulo.core.util.HostAndPort;
import org.apache.accumulo.core.util.threads.ThreadPools;
import org.apache.accumulo.core.util.threads.Threads;
import org.apache.thrift.TApplicationException;
import org.apache.thrift.TException;
import org.apache.thrift.TServiceClient;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Joiner;

import io.opentelemetry.api.trace.Span;
import io.opentelemetry.context.Scope;

/*
 * Differences from previous TabletServerBatchWriter
 *   + As background threads finish sending mutations to tablet servers they decrement memory usage
 *   + Once the queue of unprocessed mutations reaches 50% it is always pushed
 *     to the background threads, even if they are currently processing... new
 *     mutations are merged with mutations currently processing in the background
 *   + Failed mutations are held for 1000ms and then re-added to the unprocessed queue
 *   + Flush holds adding of new mutations so it does not wait indefinitely
 *
 * Considerations
 *   + All background threads must catch and note Exception
 *   + mutations for a single tablet server are only processed by one thread
 *     concurrently (if new mutations come in for a tablet server while one
 *     thread is processing mutations for it, no other thread should
 *     start processing those mutations)
 *
 * Memory accounting
 *   + when a mutation enters the system memory is incremented
 *   + when a mutation successfully leaves the system memory is decremented
 */
public class TabletServerBatchWriter implements AutoCloseable {

  private static final Logger log = LoggerFactory.getLogger(TabletServerBatchWriter.class);

  // basic configuration
  private final ClientContext context;
  private final long maxMem;
  private final long maxLatency;
  private final long timeout;
  private final Durability durability;

  // state
  private boolean flushing;
  private boolean closed;
  private MutationSet mutations;

  // background writer
  private final MutationWriter writer;

  // latency timers
  private final ScheduledThreadPoolExecutor executor;
  private ScheduledFuture<?> latencyTimerFuture;
  private final Map<String,TimeoutTracker> timeoutTrackers =
      Collections.synchronizedMap(new HashMap<>());

  // stats
  private long totalMemUsed = 0;
  private long lastProcessingStartTime;

  private long totalAdded = 0;
  private final AtomicLong totalSent = new AtomicLong(0);
  private final AtomicLong totalBinned = new AtomicLong(0);
  private final AtomicLong totalBinTime = new AtomicLong(0);
  private final AtomicLong totalSendTime = new AtomicLong(0);
  private long startTime = 0;
  private long initialGCTimes;
  private long initialCompileTimes;
  private double initialSystemLoad;

  private AtomicInteger tabletServersBatchSum = new AtomicInteger(0);
  private AtomicInteger tabletBatchSum = new AtomicInteger(0);
  private AtomicInteger numBatches = new AtomicInteger(0);
  private AtomicInteger maxTabletBatch = new AtomicInteger(Integer.MIN_VALUE);
  private AtomicInteger minTabletBatch = new AtomicInteger(Integer.MAX_VALUE);
  private AtomicInteger minTabletServersBatch = new AtomicInteger(Integer.MAX_VALUE);
  private AtomicInteger maxTabletServersBatch = new AtomicInteger(Integer.MIN_VALUE);

  // error handling
  private final Violations violations = new Violations();
  private final Map<KeyExtent,Set<SecurityErrorCode>> authorizationFailures = new HashMap<>();
  private final HashSet<String> serverSideErrors = new HashSet<>();
  private final FailedMutations failedMutations;
  private int unknownErrors = 0;
  private boolean somethingFailed = false;
  private Exception lastUnknownError = null;

  private static class TimeoutTracker {

    final String server;
    final long timeOut;
    long activityTime;
    Long firstErrorTime = null;

    TimeoutTracker(String server, long timeOut) {
      this.timeOut = timeOut;
      this.server = server;
    }

    void startingWrite() {
      activityTime = System.currentTimeMillis();
    }

    void madeProgress() {
      activityTime = System.currentTimeMillis();
      firstErrorTime = null;
    }

    void wroteNothing() {
      if (firstErrorTime == null) {
        firstErrorTime = activityTime;
      } else if (System.currentTimeMillis() - firstErrorTime > timeOut) {
        throw new TimedOutException(Collections.singleton(server));
      }
    }

    void errorOccured() {
      wroteNothing();
    }

    public long getTimeOut() {
      return timeOut;
    }
  }

  public TabletServerBatchWriter(ClientContext context, BatchWriterConfig config) {
    this.context = context;
    this.executor = context.threadPools()
        .createGeneralScheduledExecutorService(this.context.getConfiguration());
    this.failedMutations = new FailedMutations();
    this.maxMem = config.getMaxMemory();
    this.maxLatency = config.getMaxLatency(MILLISECONDS) <= 0 ? Long.MAX_VALUE
        : config.getMaxLatency(MILLISECONDS);
    this.timeout = config.getTimeout(MILLISECONDS);
    this.mutations = new MutationSet();
    this.lastProcessingStartTime = System.currentTimeMillis();
    this.durability = config.getDurability();

    this.writer = new MutationWriter(config.getMaxWriteThreads());

    if (this.maxLatency != Long.MAX_VALUE) {
      latencyTimerFuture = executor
          .scheduleWithFixedDelay(Threads.createNamedRunnable("BatchWriterLatencyTimer", () -> {
            try {
              synchronized (TabletServerBatchWriter.this) {
                if ((System.currentTimeMillis() - lastProcessingStartTime)
                    > TabletServerBatchWriter.this.maxLatency) {
                  startProcessing();
                }
              }
            } catch (Exception e) {
              updateUnknownErrors("Max latency task failed " + e.getMessage(), e);
            }
          }), 0, this.maxLatency / 4, MILLISECONDS);
    }
  }

  private synchronized void startProcessing() {
    if (mutations.getMemoryUsed() == 0) {
      return;
    }
    lastProcessingStartTime = System.currentTimeMillis();
    writer.queueMutations(mutations);
    mutations = new MutationSet();
  }

  private synchronized void decrementMemUsed(long amount) {
    totalMemUsed -= amount;
    this.notifyAll();
  }

  public synchronized void addMutation(TableId table, Mutation m)
      throws MutationsRejectedException {

    if (closed) {
      throw new IllegalStateException("Closed");
    }
    if (m.size() == 0) {
      throw new IllegalArgumentException("Can not add empty mutations");
    }
    if (this.latencyTimerFuture != null) {
      ThreadPools.ensureRunning(this.latencyTimerFuture,
          "Latency timer thread has exited, cannot guarantee latency target");
    }

    checkForFailures();

    waitRTE(() -> (totalMemUsed > maxMem || flushing) && !somethingFailed);

    // do checks again since things could have changed while waiting and not holding lock
    if (closed) {
      throw new IllegalStateException("Closed");
    }
    checkForFailures();

    if (startTime == 0) {
      startTime = System.currentTimeMillis();

      List<GarbageCollectorMXBean> gcmBeans = ManagementFactory.getGarbageCollectorMXBeans();
      for (GarbageCollectorMXBean garbageCollectorMXBean : gcmBeans) {
        initialGCTimes += garbageCollectorMXBean.getCollectionTime();
      }

      CompilationMXBean compMxBean = ManagementFactory.getCompilationMXBean();
      if (compMxBean.isCompilationTimeMonitoringSupported()) {
        initialCompileTimes = compMxBean.getTotalCompilationTime();
      }

      initialSystemLoad = ManagementFactory.getOperatingSystemMXBean().getSystemLoadAverage();
    }

    // create a copy of mutation so that after this method returns the user
    // is free to reuse the mutation object, like calling readFields... this
    // is important for the case where a mutation is passed from map to reduce
    // to batch writer... the map reduce code will keep passing the same mutation
    // object into the reduce method
    m = new Mutation(m);

    totalMemUsed += m.estimatedMemoryUsed();
    mutations.addMutation(table, m);
    totalAdded++;

    if (mutations.getMemoryUsed() >= maxMem / 2) {
      startProcessing();
      checkForFailures();
    }
  }

  public void addMutation(TableId table, Iterator<Mutation> iterator)
      throws MutationsRejectedException {
    while (iterator.hasNext()) {
      addMutation(table, iterator.next());
    }
  }

  public synchronized void flush() throws MutationsRejectedException {

    if (closed) {
      throw new IllegalStateException("Closed");
    }

    Span span = TraceUtil.startSpan(this.getClass(), "flush");
    try (Scope scope = span.makeCurrent()) {
      checkForFailures();

      if (flushing) {
        // some other thread is currently flushing, so wait
        waitRTE(() -> flushing && !somethingFailed);

        checkForFailures();

        return;
      }

      flushing = true;

      startProcessing();
      checkForFailures();

      waitRTE(() -> totalMemUsed > 0 && !somethingFailed);

      flushing = false;
      this.notifyAll();

      checkForFailures();
    } catch (Exception e) {
      TraceUtil.setException(span, e, true);
      throw e;
    } finally {
      span.end();
    }
  }

  @Override
  public synchronized void close() throws MutationsRejectedException {

    if (closed) {
      return;
    }

    Span span = TraceUtil.startSpan(this.getClass(), "close");
    try (Scope scope = span.makeCurrent()) {
      closed = true;

      startProcessing();

      waitRTE(() -> totalMemUsed > 0 && !somethingFailed);

      logStats();

      checkForFailures();
    } catch (Exception e) {
      TraceUtil.setException(span, e, true);
      throw e;
    } finally {
      span.end();
      // make a best effort to release these resources
      writer.binningThreadPool.shutdownNow();
      writer.sendThreadPool.shutdownNow();
      executor.shutdownNow();
    }
  }

  private void logStats() {
    if (log.isTraceEnabled()) {
      long finishTime = System.currentTimeMillis();

      long finalGCTimes = 0;
      List<GarbageCollectorMXBean> gcmBeans = ManagementFactory.getGarbageCollectorMXBeans();
      for (GarbageCollectorMXBean garbageCollectorMXBean : gcmBeans) {
        finalGCTimes += garbageCollectorMXBean.getCollectionTime();
      }

      CompilationMXBean compMxBean = ManagementFactory.getCompilationMXBean();
      long finalCompileTimes = 0;
      if (compMxBean.isCompilationTimeMonitoringSupported()) {
        finalCompileTimes = compMxBean.getTotalCompilationTime();
      }

      double averageRate = totalSent.get() / (totalSendTime.get() / 1000.0);
      double overallRate = totalAdded / ((finishTime - startTime) / 1000.0);

      double finalSystemLoad = ManagementFactory.getOperatingSystemMXBean().getSystemLoadAverage();

      log.trace("");
      log.trace("TABLET SERVER BATCH WRITER STATISTICS");
      log.trace(String.format("Added                : %,10d mutations", totalAdded));
      log.trace(String.format("Sent                 : %,10d mutations", totalSent.get()));
      log.trace(String.format("Resent percentage   : %10.2f%s",
          (totalSent.get() - totalAdded) / (double) totalAdded * 100.0, "%"));
      log.trace(
          String.format("Overall time         : %,10.2f secs", (finishTime - startTime) / 1000.0));
      log.trace(String.format("Overall send rate    : %,10.2f mutations/sec", overallRate));
      log.trace(
          String.format("Send efficiency      : %10.2f%s", overallRate / averageRate * 100.0, "%"));
      log.trace("");
      log.trace("BACKGROUND WRITER PROCESS STATISTICS");
      log.trace(
          String.format("Total send time      : %,10.2f secs %6.2f%s", totalSendTime.get() / 1000.0,
              100.0 * totalSendTime.get() / (finishTime - startTime), "%"));
      log.trace(String.format("Average send rate    : %,10.2f mutations/sec", averageRate));
      log.trace(String.format("Total bin time       : %,10.2f secs %6.2f%s",
          totalBinTime.get() / 1000.0, 100.0 * totalBinTime.get() / (finishTime - startTime), "%"));
      log.trace(String.format("Average bin rate     : %,10.2f mutations/sec",
          totalBinned.get() / (totalBinTime.get() / 1000.0)));
      log.trace(String.format("tservers per batch   : %,8.2f avg  %,6d min %,6d max",
          (float) (numBatches.get() != 0 ? (tabletServersBatchSum.get() / numBatches.get()) : 0),
          minTabletServersBatch.get(), maxTabletServersBatch.get()));
      log.trace(String.format("tablets per batch    : %,8.2f avg  %,6d min %,6d max",
          (float) (numBatches.get() != 0 ? (tabletBatchSum.get() / numBatches.get()) : 0),
          minTabletBatch.get(), maxTabletBatch.get()));
      log.trace("");
      log.trace("SYSTEM STATISTICS");
      log.trace(String.format("JVM GC Time          : %,10.2f secs",
          ((finalGCTimes - initialGCTimes) / 1000.0)));
      if (compMxBean.isCompilationTimeMonitoringSupported()) {
        log.trace(String.format("JVM Compile Time     : %,10.2f secs",
            (finalCompileTimes - initialCompileTimes) / 1000.0));
      }
      log.trace(String.format("System load average : initial=%6.2f final=%6.2f", initialSystemLoad,
          finalSystemLoad));
    }
  }

  private void updateSendStats(long count, long time) {
    totalSent.addAndGet(count);
    totalSendTime.addAndGet(time);
  }

  public void updateBinningStats(int count, long time,
      Map<String,TabletServerMutations<Mutation>> binnedMutations) {
    if (log.isTraceEnabled()) {
      totalBinTime.addAndGet(time);
      totalBinned.addAndGet(count);
      updateBatchStats(binnedMutations);
    }
  }

  private static void computeMin(AtomicInteger stat, int update) {
    int old = stat.get();
    while (!stat.compareAndSet(old, Math.min(old, update))) {
      old = stat.get();
    }
  }

  private static void computeMax(AtomicInteger stat, int update) {
    int old = stat.get();
    while (!stat.compareAndSet(old, Math.max(old, update))) {
      old = stat.get();
    }
  }

  private void updateBatchStats(Map<String,TabletServerMutations<Mutation>> binnedMutations) {
    tabletServersBatchSum.addAndGet(binnedMutations.size());

    computeMin(minTabletServersBatch, binnedMutations.size());
    computeMax(maxTabletServersBatch, binnedMutations.size());

    int numTablets = 0;

    for (Entry<String,TabletServerMutations<Mutation>> entry : binnedMutations.entrySet()) {
      TabletServerMutations<Mutation> tsm = entry.getValue();
      numTablets += tsm.getMutations().size();
    }

    tabletBatchSum.addAndGet(numTablets);

    computeMin(minTabletBatch, numTablets);
    computeMax(maxTabletBatch, numTablets);

    numBatches.incrementAndGet();
  }

  private interface WaitCondition {
    boolean shouldWait();
  }

  private void waitRTE(WaitCondition condition) {
    try {
      while (condition.shouldWait()) {
        wait();
      }
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  // BEGIN code for handling unrecoverable errors

  private void updatedConstraintViolations(List<ConstraintViolationSummary> cvsList) {
    if (!cvsList.isEmpty()) {
      synchronized (this) {
        somethingFailed = true;
        violations.add(cvsList);
        this.notifyAll();
      }
    }
  }

  private void updateAuthorizationFailures(Map<KeyExtent,SecurityErrorCode> authorizationFailures) {
    if (!authorizationFailures.isEmpty()) {

      // was a table deleted?
      context.clearTableListCache();
      authorizationFailures.keySet().stream().map(KeyExtent::tableId)
          .forEach(context::requireNotDeleted);

      synchronized (this) {
        somethingFailed = true;
        // add these authorizationFailures to those collected by this batch writer
        authorizationFailures.forEach((ke, code) -> this.authorizationFailures
            .computeIfAbsent(ke, k -> new HashSet<>()).add(code));
        this.notifyAll();
      }
    }
  }

  private synchronized void updateServerErrors(String server, Exception e) {
    somethingFailed = true;
    this.serverSideErrors.add(server);
    this.notifyAll();
    log.error("Server side error on {}", server, e);
  }

  private synchronized void updateUnknownErrors(String msg, Exception t) {
    somethingFailed = true;
    unknownErrors++;
    this.lastUnknownError = t;
    this.notifyAll();
    if (t instanceof TableDeletedException || t instanceof TableOfflineException
        || t instanceof TimedOutException) {
      log.debug("{}", msg, t); // this is not unknown
    } else {
      log.error("{}", msg, t);
    }
  }

  private void checkForFailures() throws MutationsRejectedException {
    if (somethingFailed) {
      List<ConstraintViolationSummary> cvsList = violations.asList();
      HashMap<TabletId,Set<org.apache.accumulo.core.client.security.SecurityErrorCode>> af =
          new HashMap<>();
      for (Entry<KeyExtent,Set<SecurityErrorCode>> entry : authorizationFailures.entrySet()) {
        HashSet<org.apache.accumulo.core.client.security.SecurityErrorCode> codes = new HashSet<>();

        for (SecurityErrorCode sce : entry.getValue()) {
          codes.add(org.apache.accumulo.core.client.security.SecurityErrorCode.valueOf(sce.name()));
        }

        af.put(new TabletIdImpl(entry.getKey()), codes);
      }

      throw new MutationsRejectedException(context, cvsList, af, serverSideErrors, unknownErrors,
          lastUnknownError);
    }
  }

  // END code for handling unrecoverable errors

  // BEGIN code for handling failed mutations

  /**
   * Add mutations that previously failed back into the mix
   */
  private synchronized void addFailedMutations(MutationSet failedMutations) {
    mutations.addAll(failedMutations);
    if (mutations.getMemoryUsed() >= maxMem / 2 || closed || flushing) {
      startProcessing();
    }
  }

  private class FailedMutations {

    private MutationSet recentFailures = null;
    private long initTime;
    private final Runnable task;
    private final ScheduledFuture<?> future;

    FailedMutations() {
      task =
          Threads.createNamedRunnable("failed mutationBatchWriterLatencyTimers handler", this::run);
      future = executor.scheduleWithFixedDelay(task, 0, 500, MILLISECONDS);
    }

    private MutationSet init() {
      ThreadPools.ensureRunning(future,
          "Background task that re-queues failed mutations has exited.");
      if (recentFailures == null) {
        recentFailures = new MutationSet();
        initTime = System.currentTimeMillis();
      }
      return recentFailures;
    }

    synchronized void add(TableId table, ArrayList<Mutation> tableFailures) {
      init().addAll(table, tableFailures);
    }

    synchronized void add(MutationSet failures) {
      init().addAll(failures);
    }

    synchronized void add(TabletServerMutations<Mutation> tsm) {
      init();
      tsm.getMutations().forEach((ke, muts) -> recentFailures.addAll(ke.tableId(), muts));
    }

    public void run() {
      try {
        MutationSet rf = null;

        synchronized (this) {
          if (recentFailures != null && System.currentTimeMillis() - initTime > 1000) {
            rf = recentFailures;
            recentFailures = null;
          }
        }

        if (rf != null) {
          if (log.isTraceEnabled()) {
            log.trace("tid={}  Requeuing {} failed mutations", Thread.currentThread().getId(),
                rf.size());
          }
          addFailedMutations(rf);
        }
      } catch (Exception t) {
        updateUnknownErrors("tid=" + Thread.currentThread().getId()
            + "  Failed to requeue failed mutations " + t.getMessage(), t);
        executor.remove(task);
      }
    }
  }

  // END code for handling failed mutations

  // BEGIN code for sending mutations to tablet servers using background threads

  private class MutationWriter {

    private static final int MUTATION_BATCH_SIZE = 1 << 17;
    private final ThreadPoolExecutor sendThreadPool;
    private final ThreadPoolExecutor binningThreadPool;
    private final Map<String,TabletServerMutations<Mutation>> serversMutations;
    private final Set<String> queued;
    private final Map<TableId,TabletLocator> locators;

    public MutationWriter(int numSendThreads) {
      serversMutations = new HashMap<>();
      queued = new HashSet<>();
      sendThreadPool = context.threadPools().createFixedThreadPool(numSendThreads,
          this.getClass().getName(), false);
      locators = new HashMap<>();
      binningThreadPool = context.threadPools().createFixedThreadPool(1, "BinMutations",
          new SynchronousQueue<>(), false);
      binningThreadPool.setRejectedExecutionHandler(new ThreadPoolExecutor.CallerRunsPolicy());
    }

    private synchronized TabletLocator getLocator(TableId tableId) {
      TabletLocator ret = locators.get(tableId);
      if (ret == null) {
        ret = new TimeoutTabletLocator(timeout, context, tableId);
        locators.put(tableId, ret);
      }

      return ret;
    }

    private void binMutations(MutationSet mutationsToProcess,
        Map<String,TabletServerMutations<Mutation>> binnedMutations) {
      TableId tableId = null;
      try {
        Set<Entry<TableId,List<Mutation>>> es = mutationsToProcess.getMutations().entrySet();
        for (Entry<TableId,List<Mutation>> entry : es) {
          tableId = entry.getKey();
          TabletLocator locator = getLocator(tableId);
          List<Mutation> tableMutations = entry.getValue();

          if (tableMutations != null) {
            ArrayList<Mutation> tableFailures = new ArrayList<>();
            locator.binMutations(context, tableMutations, binnedMutations, tableFailures);

            if (!tableFailures.isEmpty()) {
              failedMutations.add(tableId, tableFailures);

              if (tableFailures.size() == tableMutations.size()) {
                context.requireNotDeleted(tableId);
                context.requireNotOffline(tableId, null);
              }
            }
          }

        }
        return;
      } catch (AccumuloServerException ase) {
        updateServerErrors(ase.getServer(), ase);
      } catch (AccumuloException ae) {
        // assume an IOError communicating with metadata tablet
        failedMutations.add(mutationsToProcess);
      } catch (AccumuloSecurityException e) {
        updateAuthorizationFailures(Collections.singletonMap(new KeyExtent(tableId, null, null),
            SecurityErrorCode.valueOf(e.getSecurityErrorCode().name())));
      } catch (TableDeletedException | TableNotFoundException | TableOfflineException e) {
        updateUnknownErrors(e.getMessage(), e);
      }

      // an error occurred
      binnedMutations.clear();

    }

    void queueMutations(final MutationSet mutationsToSend) {
      if (mutationsToSend == null) {
        return;
      }
      binningThreadPool.execute(() -> {
        try {
          log.trace("{} - binning {} mutations", Thread.currentThread().getName(),
              mutationsToSend.size());
          addMutations(mutationsToSend);
        } catch (Exception e) {
          updateUnknownErrors("Error processing mutation set", e);
        }
      });
    }

    private void addMutations(MutationSet mutationsToSend) {
      Map<String,TabletServerMutations<Mutation>> binnedMutations = new HashMap<>();
      Span span = TraceUtil.startSpan(this.getClass(), "binMutations");
      try (Scope scope = span.makeCurrent()) {
        long t1 = System.currentTimeMillis();
        binMutations(mutationsToSend, binnedMutations);
        long t2 = System.currentTimeMillis();
        updateBinningStats(mutationsToSend.size(), (t2 - t1), binnedMutations);
      } catch (Exception e) {
        TraceUtil.setException(span, e, true);
        throw e;
      } finally {
        span.end();
      }
      addMutations(binnedMutations);
    }

    private synchronized void
        addMutations(Map<String,TabletServerMutations<Mutation>> binnedMutations) {

      int count = 0;

      // merge mutations into existing mutations for a tablet server
      for (Entry<String,TabletServerMutations<Mutation>> entry : binnedMutations.entrySet()) {
        String server = entry.getKey();

        TabletServerMutations<Mutation> currentMutations = serversMutations.get(server);

        if (currentMutations == null) {
          serversMutations.put(server, entry.getValue());
        } else {
          for (Entry<KeyExtent,List<Mutation>> entry2 : entry.getValue().getMutations()
              .entrySet()) {
            for (Mutation m : entry2.getValue()) {
              currentMutations.addMutation(entry2.getKey(), m);
            }
          }
        }

        if (log.isTraceEnabled()) {
          for (Entry<KeyExtent,List<Mutation>> entry2 : entry.getValue().getMutations()
              .entrySet()) {
            count += entry2.getValue().size();
          }
        }

      }

      if (count > 0 && log.isTraceEnabled()) {
        log.trace(String.format("Started sending %,d mutations to %,d tablet servers", count,
            binnedMutations.keySet().size()));
      }

      // randomize order of servers
      ArrayList<String> servers = new ArrayList<>(binnedMutations.keySet());
      Collections.shuffle(servers);

      for (String server : servers) {
        if (!queued.contains(server)) {
          sendThreadPool.execute(new SendTask(server));
          queued.add(server);
        }
      }
    }

    private synchronized TabletServerMutations<Mutation> getMutationsToSend(String server) {
      TabletServerMutations<Mutation> tsmuts = serversMutations.remove(server);
      if (tsmuts == null) {
        queued.remove(server);
      }

      return tsmuts;
    }

    class SendTask implements Runnable {

      private final String location;

      SendTask(String server) {
        this.location = server;
      }

      @Override
      public void run() {
        try {
          TabletServerMutations<Mutation> tsmuts = getMutationsToSend(location);

          while (tsmuts != null) {
            send(tsmuts);
            tsmuts = getMutationsToSend(location);
          }
        } catch (Exception t) {
          updateUnknownErrors(
              "Failed to send tablet server " + location + " its batch : " + t.getMessage(), t);
        }
      }

      public void send(TabletServerMutations<Mutation> tsm)
          throws AccumuloServerException, AccumuloSecurityException {

        MutationSet failures = null;

        String oldName = Thread.currentThread().getName();

        Map<KeyExtent,List<Mutation>> mutationBatch = tsm.getMutations();
        try {

          long count = 0;

          Set<TableId> tableIds = new TreeSet<>();
          for (Map.Entry<KeyExtent,List<Mutation>> entry : mutationBatch.entrySet()) {
            count += entry.getValue().size();
            tableIds.add(entry.getKey().tableId());
          }

          String msg = "sending " + String.format("%,d", count) + " mutations to "
              + String.format("%,d", mutationBatch.size()) + " tablets at " + location + " tids: ["
              + Joiner.on(',').join(tableIds) + ']';
          Thread.currentThread().setName(msg);

          Span span = TraceUtil.startSpan(this.getClass(), "sendMutations");
          try (Scope scope = span.makeCurrent()) {

            TimeoutTracker timeoutTracker = timeoutTrackers.get(location);
            if (timeoutTracker == null) {
              timeoutTracker = new TimeoutTracker(location, timeout);
              timeoutTrackers.put(location, timeoutTracker);
            }

            long st1 = System.currentTimeMillis();
            failures = sendMutationsToTabletServer(location, mutationBatch, timeoutTracker);
            long st2 = System.currentTimeMillis();
            if (log.isTraceEnabled()) {
              log.trace("sent " + String.format("%,d", count) + " mutations to " + location + " in "
                  + String.format("%.2f secs (%,.2f mutations/sec) with %,d failures",
                      (st2 - st1) / 1000.0, count / ((st2 - st1) / 1000.0), failures.size()));
            }

            long successBytes = 0;
            for (Entry<KeyExtent,List<Mutation>> entry : mutationBatch.entrySet()) {
              for (Mutation mutation : entry.getValue()) {
                successBytes += mutation.estimatedMemoryUsed();
              }
            }

            if (failures.size() > 0) {
              failedMutations.add(failures);
              successBytes -= failures.getMemoryUsed();
            }

            updateSendStats(count, st2 - st1);
            decrementMemUsed(successBytes);

          } catch (Exception e) {
            TraceUtil.setException(span, e, true);
            throw e;
          } finally {
            span.end();
          }
        } catch (IOException e) {
          if (log.isTraceEnabled()) {
            log.trace("failed to send mutations to {} : {}", location, e.getMessage());
          }

          HashSet<TableId> tables = new HashSet<>();
          for (KeyExtent ke : mutationBatch.keySet()) {
            tables.add(ke.tableId());
          }

          for (TableId table : tables) {
            getLocator(table).invalidateCache(context, location);
          }

          failedMutations.add(tsm);
        } finally {
          Thread.currentThread().setName(oldName);
        }
      }
    }

    private MutationSet sendMutationsToTabletServer(String location,
        Map<KeyExtent,List<Mutation>> tabMuts, TimeoutTracker timeoutTracker)
        throws IOException, AccumuloSecurityException, AccumuloServerException {
      if (tabMuts.isEmpty()) {
        return new MutationSet();
      }
      TInfo tinfo = TraceUtil.traceInfo();

      timeoutTracker.startingWrite();

      try {
        final HostAndPort parsedServer = HostAndPort.fromString(location);
        final TabletClientService.Iface client;

        if (timeoutTracker.getTimeOut() < context.getClientTimeoutInMillis()) {
          client = ThriftUtil.getClient(ThriftClientTypes.TABLET_SERVER, parsedServer, context,
              timeoutTracker.getTimeOut());
        } else {
          client = ThriftUtil.getClient(ThriftClientTypes.TABLET_SERVER, parsedServer, context);
        }

        try {
          MutationSet allFailures = new MutationSet();

          if (tabMuts.size() == 1 && tabMuts.values().iterator().next().size() == 1) {
            Entry<KeyExtent,List<Mutation>> entry = tabMuts.entrySet().iterator().next();

            try {
              client.update(tinfo, context.rpcCreds(), entry.getKey().toThrift(),
                  entry.getValue().get(0).toThrift(), DurabilityImpl.toThrift(durability));
            } catch (NotServingTabletException e) {
              allFailures.addAll(entry.getKey().tableId(), entry.getValue());
              getLocator(entry.getKey().tableId()).invalidateCache(entry.getKey());
            } catch (ConstraintViolationException e) {
              updatedConstraintViolations(e.violationSummaries.stream()
                  .map(ConstraintViolationSummary::new).collect(toList()));
            }
            timeoutTracker.madeProgress();
          } else {

            long usid =
                client.startUpdate(tinfo, context.rpcCreds(), DurabilityImpl.toThrift(durability));

            List<TMutation> updates = new ArrayList<>();
            for (Entry<KeyExtent,List<Mutation>> entry : tabMuts.entrySet()) {
              long size = 0;
              Iterator<Mutation> iter = entry.getValue().iterator();
              while (iter.hasNext()) {
                while (size < MUTATION_BATCH_SIZE && iter.hasNext()) {
                  Mutation mutation = iter.next();
                  updates.add(mutation.toThrift());
                  size += mutation.numBytes();
                }

                client.applyUpdates(tinfo, usid, entry.getKey().toThrift(), updates);
                updates.clear();
                size = 0;
              }
            }

            UpdateErrors updateErrors = client.closeUpdate(tinfo, usid);

            // @formatter:off
            Map<KeyExtent,Long> failures = updateErrors.failedExtents.entrySet().stream().collect(toMap(
                            entry -> KeyExtent.fromThrift(entry.getKey()),
                            Entry::getValue
            ));
            // @formatter:on
            updatedConstraintViolations(updateErrors.violationSummaries.stream()
                .map(ConstraintViolationSummary::new).collect(toList()));
            // @formatter:off
            updateAuthorizationFailures(updateErrors.authorizationFailures.entrySet().stream().collect(toMap(
                            entry -> KeyExtent.fromThrift(entry.getKey()),
                            Entry::getValue
            )));
            // @formatter:on
            long totalCommitted = 0;

            for (Entry<KeyExtent,Long> entry : failures.entrySet()) {
              KeyExtent failedExtent = entry.getKey();
              int numCommitted = (int) (long) entry.getValue();
              totalCommitted += numCommitted;

              TableId tableId = failedExtent.tableId();

              getLocator(tableId).invalidateCache(failedExtent);

              List<Mutation> mutations = tabMuts.get(failedExtent);
              allFailures.addAll(tableId, mutations.subList(numCommitted, mutations.size()));
            }

            if (failures.keySet().containsAll(tabMuts.keySet()) && totalCommitted == 0) {
              // nothing was successfully written
              timeoutTracker.wroteNothing();
            } else {
              // successfully wrote something to tablet server
              timeoutTracker.madeProgress();
            }
          }
          return allFailures;
        } finally {
          ThriftUtil.returnClient((TServiceClient) client, context);
        }
      } catch (TTransportException e) {
        timeoutTracker.errorOccured();
        throw new IOException(e);
      } catch (TApplicationException tae) {
        updateServerErrors(location, tae);
        throw new AccumuloServerException(location, tae);
      } catch (ThriftSecurityException e) {
        updateAuthorizationFailures(
            tabMuts.keySet().stream().collect(toMap(identity(), ke -> e.code)));
        throw new AccumuloSecurityException(e.user, e.code, e);
      } catch (TException e) {
        throw new IOException(e);
      }
    }
  }

  // END code for sending mutations to tablet servers using background threads

  private static class MutationSet {

    private final HashMap<TableId,List<Mutation>> mutations;
    private long memoryUsed = 0;

    MutationSet() {
      mutations = new HashMap<>();
    }

    void addMutation(TableId table, Mutation mutation) {
      mutations.computeIfAbsent(table, k -> new ArrayList<>()).add(mutation);
      memoryUsed += mutation.estimatedMemoryUsed();
    }

    Map<TableId,List<Mutation>> getMutations() {
      return mutations;
    }

    int size() {
      int result = 0;
      for (List<Mutation> perTable : mutations.values()) {
        result += perTable.size();
      }
      return result;
    }

    public void addAll(MutationSet failures) {
      Set<Entry<TableId,List<Mutation>>> es = failures.getMutations().entrySet();

      for (Entry<TableId,List<Mutation>> entry : es) {
        TableId table = entry.getKey();

        for (Mutation mutation : entry.getValue()) {
          addMutation(table, mutation);
        }
      }
    }

    public void addAll(TableId table, List<Mutation> mutations) {
      for (Mutation mutation : mutations) {
        addMutation(table, mutation);
      }
    }

    public long getMemoryUsed() {
      return memoryUsed;
    }

  }
}

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
package org.apache.accumulo.tserver;

import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.toUnmodifiableMap;
import static org.apache.accumulo.core.util.UtilWaitThread.sleepUninterruptibly;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.function.IntSupplier;
import java.util.function.Supplier;

import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.AccumuloConfiguration.ScanExecutorConfig;
import org.apache.accumulo.core.conf.ConfigurationTypeHelper;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.file.blockfile.cache.impl.BlockCacheManagerFactory;
import org.apache.accumulo.core.file.blockfile.impl.ScanCacheProvider;
import org.apache.accumulo.core.spi.cache.BlockCache;
import org.apache.accumulo.core.spi.cache.BlockCacheManager;
import org.apache.accumulo.core.spi.cache.CacheType;
import org.apache.accumulo.core.spi.common.ServiceEnvironment;
import org.apache.accumulo.core.spi.scan.ScanDispatch;
import org.apache.accumulo.core.spi.scan.ScanDispatcher;
import org.apache.accumulo.core.spi.scan.ScanDispatcher.DispatchParameters;
import org.apache.accumulo.core.spi.scan.ScanExecutor;
import org.apache.accumulo.core.spi.scan.ScanInfo;
import org.apache.accumulo.core.spi.scan.ScanPrioritizer;
import org.apache.accumulo.core.spi.scan.SimpleScanDispatcher;
import org.apache.accumulo.core.trace.TraceUtil;
import org.apache.accumulo.core.util.threads.ThreadPools;
import org.apache.accumulo.core.util.threads.Threads;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.ServiceEnvironmentImpl;
import org.apache.accumulo.server.fs.FileManager;
import org.apache.accumulo.server.fs.FileManager.ScanFileManager;
import org.apache.accumulo.tserver.memory.LargestFirstMemoryManager;
import org.apache.accumulo.tserver.memory.NativeMapLoader;
import org.apache.accumulo.tserver.memory.TabletMemoryReport;
import org.apache.accumulo.tserver.session.ScanSession;
import org.apache.accumulo.tserver.tablet.Tablet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Suppliers;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * ResourceManager is responsible for managing the resources of all tablets within a tablet server.
 */
public class TabletServerResourceManager {

  private static final Logger log = LoggerFactory.getLogger(TabletServerResourceManager.class);

  private final ThreadPoolExecutor minorCompactionThreadPool;
  private final ThreadPoolExecutor splitThreadPool;
  private final ThreadPoolExecutor defaultSplitThreadPool;
  private final ThreadPoolExecutor defaultMigrationPool;
  private final ThreadPoolExecutor migrationPool;
  private final ThreadPoolExecutor assignmentPool;
  private final ThreadPoolExecutor assignMetaDataPool;
  private final ThreadPoolExecutor summaryRetrievalPool;
  private final ThreadPoolExecutor summaryPartitionPool;
  private final ThreadPoolExecutor summaryRemotePool;

  private final Map<String,ThreadPoolExecutor> scanExecutors;
  private final Map<String,ScanExecutor> scanExecutorChoices;

  private final ConcurrentHashMap<KeyExtent,RunnableStartedAt> activeAssignments;

  private final FileManager fileManager;

  private final LargestFirstMemoryManager memoryManager;

  private final MemoryManagementFramework memMgmt;

  private final BlockCacheManager cacheManager;
  private final BlockCache _dCache;
  private final BlockCache _iCache;
  private final BlockCache _sCache;
  private final ServerContext context;

  private Cache<String,Long> fileLenCache;

  /**
   * This method creates a task that changes the number of core and maximum threads on the thread
   * pool executor
   *
   * @param maxThreads max threads
   * @param name name of thread pool
   * @param tp executor
   */
  private void modifyThreadPoolSizesAtRuntime(IntSupplier maxThreads, String name,
      final ThreadPoolExecutor tp) {
    ThreadPools.watchCriticalScheduledTask(context.getScheduledExecutor().scheduleWithFixedDelay(
        () -> ThreadPools.resizePool(tp, maxThreads, name), 1, 10, SECONDS));
  }

  private ThreadPoolExecutor createPriorityExecutor(ScanExecutorConfig sec,
      Map<String,Queue<Runnable>> scanExecQueues) {

    BlockingQueue<Runnable> queue;

    if (sec.prioritizerClass.orElse("").isEmpty()) {
      queue = new LinkedBlockingQueue<>();
    } else {
      ScanPrioritizer factory = null;
      try {
        factory = ConfigurationTypeHelper.getClassInstance(null, sec.prioritizerClass.orElseThrow(),
            ScanPrioritizer.class);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }

      if (factory == null) {
        queue = new LinkedBlockingQueue<>();
      } else {
        Comparator<ScanInfo> comparator =
            factory.createComparator(new ScanPrioritizer.CreateParameters() {

              private final ServiceEnvironment senv = new ServiceEnvironmentImpl(context);

              @Override
              public Map<String,String> getOptions() {
                return sec.prioritizerOpts;
              }

              @Override
              public ServiceEnvironment getServiceEnv() {
                return senv;
              }
            });

        // function to extract scan session from runnable
        Function<Runnable,ScanInfo> extractor =
            r -> ((ScanSession.ScanMeasurer) TraceUtil.unwrap(r)).getScanInfo();

        queue = new PriorityBlockingQueue<>(sec.maxThreads,
            Comparator.comparing(extractor, comparator));
      }
    }

    scanExecQueues.put(sec.name, queue);

    ThreadPoolExecutor es = ThreadPools.getServerThreadPools().getPoolBuilder("scan-" + sec.name)
        .numCoreThreads(sec.getCurrentMaxThreads()).numMaxThreads(sec.getCurrentMaxThreads())
        .withTimeOut(0L, MILLISECONDS).withQueue(queue).atPriority(sec.priority)
        .enableThreadPoolMetrics().build();
    modifyThreadPoolSizesAtRuntime(sec::getCurrentMaxThreads, "scan-" + sec.name, es);
    return es;

  }

  private static class ScanExecutorImpl implements ScanExecutor {

    private static class ConfigImpl implements ScanExecutor.Config {

      final ScanExecutorConfig cfg;

      public ConfigImpl(ScanExecutorConfig sec) {
        this.cfg = sec;
      }

      @Override
      public String getName() {
        return cfg.name;
      }

      @Override
      public int getMaxThreads() {
        return cfg.maxThreads;
      }

      @Override
      public Optional<String> getPrioritizerClass() {
        return cfg.prioritizerClass;
      }

      @Override
      public Map<String,String> getPrioritizerOptions() {
        return cfg.prioritizerOpts;
      }

    }

    private final ConfigImpl config;
    private final Queue<?> queue;

    ScanExecutorImpl(ScanExecutorConfig sec, Queue<?> q) {
      this.config = new ConfigImpl(sec);
      this.queue = q;
    }

    @Override
    public int getQueued() {
      return queue.size();
    }

    @Override
    public Config getConfig() {
      return config;
    }

  }

  @SuppressFBWarnings(value = "DM_GC",
      justification = "GC is run to get a good estimate of memory availability")
  public TabletServerResourceManager(ServerContext context, TabletHostingServer tserver) {
    this.context = context;
    final AccumuloConfiguration acuConf = context.getConfiguration();
    long maxMemory = acuConf.getAsBytes(Property.TSERV_MAXMEM);
    boolean usingNativeMap = acuConf.getBoolean(Property.TSERV_NATIVEMAP_ENABLED);
    if (usingNativeMap) {
      NativeMapLoader.load();
    }

    long totalQueueSize = acuConf.getAsBytes(Property.TSERV_TOTAL_MUTATION_QUEUE_MAX);

    try {
      cacheManager = BlockCacheManagerFactory.getInstance(acuConf);
    } catch (Exception e) {
      throw new RuntimeException("Error creating BlockCacheManager", e);
    }

    cacheManager.start(tserver.getBlockCacheConfiguration(acuConf));

    _iCache = cacheManager.getBlockCache(CacheType.INDEX);
    _dCache = cacheManager.getBlockCache(CacheType.DATA);
    _sCache = cacheManager.getBlockCache(CacheType.SUMMARY);

    long dCacheSize = _dCache.getMaxHeapSize();
    long iCacheSize = _iCache.getMaxHeapSize();
    long sCacheSize = _sCache.getMaxHeapSize();

    Runtime runtime = Runtime.getRuntime();
    if (usingNativeMap) {
      // Still check block cache sizes when using native maps.
      if (dCacheSize + iCacheSize + sCacheSize + totalQueueSize > runtime.maxMemory()) {
        throw new IllegalArgumentException(String.format(
            "Block cache sizes %,d and mutation queue size %,d is too large for this JVM"
                + " configuration %,d",
            dCacheSize + iCacheSize + sCacheSize, totalQueueSize, runtime.maxMemory()));
      }
    } else if (maxMemory + dCacheSize + iCacheSize + sCacheSize + totalQueueSize
        > runtime.maxMemory()) {
      throw new IllegalArgumentException(String.format(
          "Maximum tablet server"
              + " map memory %,d block cache sizes %,d and mutation queue size %,d is"
              + " too large for this JVM configuration %,d",
          maxMemory, dCacheSize + iCacheSize + sCacheSize, totalQueueSize, runtime.maxMemory()));
    }
    runtime.gc();

    // totalMemory - freeMemory = memory in use
    // maxMemory - memory in use = max available memory
    if (!usingNativeMap
        && maxMemory > runtime.maxMemory() - (runtime.totalMemory() - runtime.freeMemory())) {
      log.warn("In-memory map may not fit into local memory space.");
    }

    minorCompactionThreadPool = ThreadPools.getServerThreadPools().createExecutorService(acuConf,
        Property.TSERV_MINC_MAXCONCURRENT, true);
    modifyThreadPoolSizesAtRuntime(
        () -> context.getConfiguration().getCount(Property.TSERV_MINC_MAXCONCURRENT),
        "minor compactor", minorCompactionThreadPool);

    splitThreadPool =
        ThreadPools.getServerThreadPools().getPoolBuilder("splitter").numCoreThreads(0)
            .numMaxThreads(1).withTimeOut(1, SECONDS).enableThreadPoolMetrics().build();

    defaultSplitThreadPool =
        ThreadPools.getServerThreadPools().getPoolBuilder("md splitter").numCoreThreads(0)
            .numMaxThreads(1).withTimeOut(60, SECONDS).enableThreadPoolMetrics().build();

    defaultMigrationPool = ThreadPools.getServerThreadPools()
        .getPoolBuilder("metadata tablet migration").numCoreThreads(0).numMaxThreads(1)
        .withTimeOut(60, SECONDS).enableThreadPoolMetrics().build();

    migrationPool = ThreadPools.getServerThreadPools().createExecutorService(acuConf,
        Property.TSERV_MIGRATE_MAXCONCURRENT, true);
    modifyThreadPoolSizesAtRuntime(
        () -> context.getConfiguration().getCount(Property.TSERV_MIGRATE_MAXCONCURRENT),
        "tablet migration", migrationPool);

    // not sure if concurrent assignments can run safely... even if they could there is probably no
    // benefit at startup because
    // individual tablet servers are already running assignments concurrently... having each
    // individual tablet server run
    // concurrent assignments would put more load on the metadata table at startup
    assignmentPool = ThreadPools.getServerThreadPools().createExecutorService(acuConf,
        Property.TSERV_ASSIGNMENT_MAXCONCURRENT, true);
    modifyThreadPoolSizesAtRuntime(
        () -> context.getConfiguration().getCount(Property.TSERV_ASSIGNMENT_MAXCONCURRENT),
        "tablet assignment", assignmentPool);

    assignMetaDataPool = ThreadPools.getServerThreadPools()
        .getPoolBuilder("metadata tablet assignment").numCoreThreads(0).numMaxThreads(1)
        .withTimeOut(60, SECONDS).enableThreadPoolMetrics().build();

    activeAssignments = new ConcurrentHashMap<>();

    summaryRetrievalPool = ThreadPools.getServerThreadPools().createExecutorService(acuConf,
        Property.TSERV_SUMMARY_RETRIEVAL_THREADS, true);
    modifyThreadPoolSizesAtRuntime(
        () -> context.getConfiguration().getCount(Property.TSERV_SUMMARY_RETRIEVAL_THREADS),
        "summary file retriever", summaryRetrievalPool);

    summaryRemotePool = ThreadPools.getServerThreadPools().createExecutorService(acuConf,
        Property.TSERV_SUMMARY_REMOTE_THREADS, true);
    modifyThreadPoolSizesAtRuntime(
        () -> context.getConfiguration().getCount(Property.TSERV_SUMMARY_REMOTE_THREADS),
        "summary remote", summaryRemotePool);

    summaryPartitionPool = ThreadPools.getServerThreadPools().createExecutorService(acuConf,
        Property.TSERV_SUMMARY_PARTITION_THREADS, true);
    modifyThreadPoolSizesAtRuntime(
        () -> context.getConfiguration().getCount(Property.TSERV_SUMMARY_PARTITION_THREADS),
        "summary partition", summaryPartitionPool);

    boolean isScanServer = (tserver instanceof ScanServer);

    Collection<ScanExecutorConfig> scanExecCfg = acuConf.getScanExecutors(isScanServer);
    Map<String,Queue<Runnable>> scanExecQueues = new HashMap<>();
    scanExecutors = scanExecCfg.stream().collect(
        toUnmodifiableMap(cfg -> cfg.name, cfg -> createPriorityExecutor(cfg, scanExecQueues)));
    scanExecutorChoices = scanExecCfg.stream().collect(toUnmodifiableMap(cfg -> cfg.name,
        cfg -> new ScanExecutorImpl(cfg, scanExecQueues.get(cfg.name))));

    int maxOpenFiles = acuConf.getCount(Property.TSERV_SCAN_MAX_OPENFILES);

    fileLenCache =
        CacheBuilder.newBuilder().maximumSize(Math.min(maxOpenFiles * 1000L, 100_000)).build();

    fileManager = new FileManager(context, maxOpenFiles, fileLenCache);

    memoryManager = new LargestFirstMemoryManager();
    memoryManager.init(context);
    memMgmt = new MemoryManagementFramework();
    memMgmt.startThreads();

    // We can use the same map for both metadata and normal assignments since the keyspace (extent)
    // is guaranteed to be unique. Schedule the task once, the task will reschedule itself.
    ThreadPools.watchCriticalScheduledTask(context.getScheduledExecutor().schedule(
        new AssignmentWatcher(acuConf, context, activeAssignments), 5000, TimeUnit.MILLISECONDS));
  }

  /**
   * Accepts some map which is tracking active assignment task(s) (running) and monitors them to
   * ensure that the time the assignment(s) have been running don't exceed a threshold. If the time
   * is exceeded a warning is printed and a stack trace is logged for the running assignment.
   */
  public static class AssignmentWatcher implements Runnable {
    private static final Logger log = LoggerFactory.getLogger(AssignmentWatcher.class);

    private static long longAssignments = 0;

    private final Map<KeyExtent,RunnableStartedAt> activeAssignments;
    private final AccumuloConfiguration conf;
    private final ServerContext context;

    public static long getLongAssignments() {
      return longAssignments;
    }

    public AssignmentWatcher(AccumuloConfiguration conf, ServerContext context,
        Map<KeyExtent,RunnableStartedAt> activeAssignments) {
      this.conf = conf;
      this.context = context;
      this.activeAssignments = activeAssignments;
    }

    @Override
    public void run() {
      final long millisBeforeWarning =
          this.conf.getTimeInMillis(Property.TSERV_ASSIGNMENT_DURATION_WARNING);
      try {
        long now = System.currentTimeMillis();
        KeyExtent extent;
        RunnableStartedAt runnable;
        long warnings = 0;
        for (Entry<KeyExtent,RunnableStartedAt> entry : activeAssignments.entrySet()) {
          extent = entry.getKey();
          runnable = entry.getValue();
          final long duration = now - runnable.getStartTime();

          // Print a warning if an assignment has been running for over the configured time length
          if (duration > millisBeforeWarning) {
            warnings++;
            log.warn("Assignment for {} has been running for at least {}ms", extent, duration,
                runnable.getTask().getException());
          } else if (log.isTraceEnabled()) {
            log.trace("Assignment for {} only running for {}ms", extent, duration);
          }
        }
        longAssignments = warnings;
      } catch (Exception e) {
        log.warn("Caught exception checking active assignments", e);
      } finally {
        // Don't run more often than every 5s
        long delay = Math.max((long) (millisBeforeWarning * 0.5), 5000L);
        if (log.isTraceEnabled()) {
          log.trace("Rescheduling assignment watcher to run in {}ms", delay);
        }
        ThreadPools.watchCriticalScheduledTask(
            context.getScheduledExecutor().schedule(this, delay, TimeUnit.MILLISECONDS));
      }
    }
  }

  private class MemoryManagementFramework {
    private final Map<KeyExtent,TabletMemoryReport> tabletReports;
    private final LinkedBlockingQueue<TabletMemoryReport> memUsageReports;
    private long lastMemCheckTime = System.currentTimeMillis();
    private long maxMem;
    private long lastMemTotal = 0;
    private final Thread memoryGuardThread;
    private final Thread minorCompactionInitiatorThread;

    MemoryManagementFramework() {
      tabletReports = Collections.synchronizedMap(new HashMap<>());
      memUsageReports = new LinkedBlockingQueue<>();
      maxMem = context.getConfiguration().getAsBytes(Property.TSERV_MAXMEM);
      memoryGuardThread = Threads.createThread("Accumulo Memory Guard",
          OptionalInt.of(Thread.NORM_PRIORITY + 1), this::processTabletMemStats);
      minorCompactionInitiatorThread =
          Threads.createThread("Accumulo Minor Compaction Initiator", this::manageMemory);
    }

    void startThreads() {
      memoryGuardThread.start();
      minorCompactionInitiatorThread.start();
    }

    private void processTabletMemStats() {
      while (true) {
        try {

          TabletMemoryReport report = memUsageReports.take();

          while (report != null) {
            tabletReports.put(report.getExtent(), report);
            report = memUsageReports.poll();
          }

          long delta = System.currentTimeMillis() - lastMemCheckTime;
          if (holdCommits || delta > 50 || lastMemTotal > 0.90 * maxMem) {
            lastMemCheckTime = System.currentTimeMillis();

            long totalMemUsed = 0;

            synchronized (tabletReports) {
              for (TabletMemoryReport tsi : tabletReports.values()) {
                totalMemUsed += tsi.getMemTableSize();
                totalMemUsed += tsi.getMinorCompactingMemTableSize();
              }
            }

            if (totalMemUsed > 0.95 * maxMem) {
              holdAllCommits(true);
            } else {
              holdAllCommits(false);
            }

            lastMemTotal = totalMemUsed;
          }

        } catch (InterruptedException e) {
          log.warn("Interrupted processing tablet memory statistics", e);
        }
      }
    }

    private void manageMemory() {
      while (true) {
        List<KeyExtent> tabletsToMinorCompact = null;

        Map<KeyExtent,TabletMemoryReport> tabletReportsCopy = null;
        try {
          synchronized (tabletReports) {
            tabletReportsCopy = new HashMap<>(tabletReports);
          }
          ArrayList<TabletMemoryReport> tabletStates = new ArrayList<>(tabletReportsCopy.values());
          tabletsToMinorCompact = memoryManager.tabletsToMinorCompact(tabletStates);

        } catch (Exception t) {
          log.error("Memory manager failed {}", t.getMessage(), t);
        }

        try {
          if (tabletsToMinorCompact != null && !tabletsToMinorCompact.isEmpty()) {
            for (KeyExtent keyExtent : tabletsToMinorCompact) {
              TabletMemoryReport tabletReport = tabletReportsCopy.get(keyExtent);

              if (tabletReport == null) {
                log.warn("Memory manager asked to compact nonexistent tablet"
                    + " {}; manager implementation might be misbehaving", keyExtent);
                continue;
              }
              Tablet tablet = tabletReport.getTablet();
              if (!tablet.initiateMinorCompaction(MinorCompactionReason.SYSTEM)) {
                if (tablet.isClosed() || tablet.isBeingDeleted()) {
                  // attempt to remove it from the current reports if still there
                  synchronized (tabletReports) {
                    TabletMemoryReport latestReport = tabletReports.remove(keyExtent);
                    if (latestReport != null) {
                      if (latestReport.getTablet() == tablet) {
                        log.debug("Cleaned up report for closed/deleted tablet {}", keyExtent);
                      } else {
                        // different tablet instance => put it back
                        tabletReports.put(keyExtent, latestReport);
                      }
                    }
                  }
                  log.debug("Ignoring memory manager recommendation: not minor"
                      + " compacting closed tablet {}", keyExtent);
                } else {
                  log.info("Ignoring memory manager recommendation: not minor compacting {}",
                      keyExtent);
                }
              }
            }

            // log.debug("mma.tabletsToMinorCompact = "+mma.tabletsToMinorCompact);
          }
        } catch (Exception t) {
          log.error("Minor compactions for memory management failed", t);
        }

        sleepUninterruptibly(250, TimeUnit.MILLISECONDS);
      }
    }

    public void updateMemoryUsageStats(Tablet tablet, long size, long lastCommitTime,
        long mincSize) {
      memUsageReports.add(new TabletMemoryReport(tablet, lastCommitTime, size, mincSize));
    }

    public void tabletClosed(KeyExtent extent) {
      tabletReports.remove(extent);
    }
  }

  private final Object commitHold = new Object();
  private volatile boolean holdCommits = false;
  private long holdStartTime;

  protected void holdAllCommits(boolean holdAllCommits) {
    synchronized (commitHold) {
      if (holdCommits != holdAllCommits) {
        holdCommits = holdAllCommits;

        if (holdCommits) {
          holdStartTime = System.currentTimeMillis();
        }

        if (!holdCommits) {
          log.debug(String.format("Commits held for %6.2f secs",
              (System.currentTimeMillis() - holdStartTime) / 1000.0));
          commitHold.notifyAll();
        }
      }
    }

  }

  void waitUntilCommitsAreEnabled() {
    if (holdCommits) {
      long timeout = System.currentTimeMillis()
          + context.getConfiguration().getTimeInMillis(Property.GENERAL_RPC_TIMEOUT);
      synchronized (commitHold) {
        while (holdCommits) {
          try {
            if (System.currentTimeMillis() > timeout) {
              throw new HoldTimeoutException("Commits are held");
            }
            commitHold.wait(1000);
          } catch (InterruptedException e) {}
        }
      }
    }
  }

  public long holdTime() {
    if (!holdCommits) {
      return 0;
    }
    synchronized (commitHold) {
      return System.currentTimeMillis() - holdStartTime;
    }
  }

  public TabletResourceManager createTabletResourceManager(KeyExtent extent,
      AccumuloConfiguration conf) {
    return new TabletResourceManager(extent, conf);
  }

  public class TabletResourceManager {

    private volatile boolean openFilesReserved = false;

    private volatile boolean closed = false;

    private final KeyExtent extent;

    private final AccumuloConfiguration tableConf;

    TabletResourceManager(KeyExtent extent, AccumuloConfiguration tableConf) {
      requireNonNull(extent, "extent is null");
      requireNonNull(tableConf, "tableConf is null");
      this.extent = extent;
      this.tableConf = tableConf;
    }

    @VisibleForTesting
    KeyExtent getExtent() {
      return extent;
    }

    @VisibleForTesting
    AccumuloConfiguration getTableConfiguration() {
      return tableConf;
    }

    // BEGIN methods that Tablets call to manage their set of open map files

    public void importedMapFiles() {
      lastReportedCommitTime = System.currentTimeMillis();
    }

    public synchronized ScanFileManager newScanFileManager(ScanDispatch scanDispatch) {
      if (closed) {
        throw new IllegalStateException("closed");
      }

      return fileManager.newScanFileManager(extent,
          new ScanCacheProvider(tableConf, scanDispatch, _iCache, _dCache));
    }

    // END methods that Tablets call to manage their set of open map files

    // BEGIN methods that Tablets call to manage memory

    private final AtomicLong lastReportedSize = new AtomicLong();
    private final AtomicLong lastReportedMincSize = new AtomicLong();
    private volatile long lastReportedCommitTime = 0;

    public void updateMemoryUsageStats(Tablet tablet, long size, long mincSize) {

      // do not want to update stats for every little change,
      // so only do it under certain circumstances... the reason
      // for this is that reporting stats acquires a lock, do
      // not want all tablets locking on the same lock for every
      // commit
      long totalSize = size + mincSize;
      long lrs = lastReportedSize.get();
      long delta = totalSize - lrs;
      long lrms = lastReportedMincSize.get();
      boolean report = false;
      // the atomic longs are considered independently, when one is set
      // the other is not set intentionally because this method is not
      // synchronized... therefore there are not transactional semantics
      // for reading and writing two variables
      if ((lrms > 0 && mincSize == 0 || lrms == 0 && mincSize > 0)
          && lastReportedMincSize.compareAndSet(lrms, mincSize)) {
        report = true;
      }

      long currentTime = System.currentTimeMillis();
      if ((delta > 32000 || delta < 0 || (currentTime - lastReportedCommitTime > 1000))
          && lastReportedSize.compareAndSet(lrs, totalSize)) {
        if (delta > 0) {
          lastReportedCommitTime = currentTime;
        }
        report = true;
      }

      if (report) {
        memMgmt.updateMemoryUsageStats(tablet, size, lastReportedCommitTime, mincSize);
      }
    }

    // END methods that Tablets call to manage memory

    // tablets call this method to run minor compactions,
    // this allows us to control how many minor compactions
    // run concurrently in a tablet server
    public void executeMinorCompaction(final Runnable r) {
      minorCompactionThreadPool.execute(r);
    }

    public void close() throws IOException {
      // always obtain locks in same order to avoid deadlock
      synchronized (TabletServerResourceManager.this) {
        synchronized (this) {
          if (closed) {
            throw new IOException("closed");
          }
          if (openFilesReserved) {
            throw new IOException("tired to close files while open files reserved");
          }

          memMgmt.tabletClosed(extent);

          closed = true;
        }
      }
    }

    public TabletServerResourceManager getTabletServerResourceManager() {
      return TabletServerResourceManager.this;
    }
  }

  public void executeSplit(KeyExtent tablet, Runnable splitTask) {
    if (tablet.isMeta()) {
      if (tablet.isRootTablet()) {
        log.warn("Saw request to split root tablet, ignoring");
        return;
      }
      defaultSplitThreadPool.execute(splitTask);
    } else {
      splitThreadPool.execute(splitTask);
    }
  }

  @SuppressWarnings("deprecation")
  private static abstract class DispatchParamsImpl implements DispatchParameters,
      org.apache.accumulo.core.spi.scan.ScanDispatcher.DispatchParmaters {

  }

  public void executeReadAhead(KeyExtent tablet, ScanDispatcher dispatcher, ScanSession scanInfo,
      Runnable task) {

    task = ScanSession.wrap(scanInfo, task);

    if (tablet.isRootTablet()) {
      // TODO make meta dispatch??
      scanInfo.scanParams.setScanDispatch(ScanDispatch.builder().build());
      task.run();
    } else if (tablet.isMeta()) {
      // TODO make meta dispatch??
      scanInfo.scanParams.setScanDispatch(ScanDispatch.builder().build());
      scanExecutors.get("meta").execute(task);
    } else {
      DispatchParameters params = new DispatchParamsImpl() {

        // in scan critical path so only create ServiceEnv if needed
        private final Supplier<ServiceEnvironment> senvSupplier =
            Suppliers.memoize(() -> new ServiceEnvironmentImpl(context));

        @Override
        public ScanInfo getScanInfo() {
          return scanInfo;
        }

        @Override
        public Map<String,ScanExecutor> getScanExecutors() {
          return scanExecutorChoices;
        }

        @Override
        public ServiceEnvironment getServiceEnv() {
          return senvSupplier.get();
        }
      };

      ScanDispatch prefs = dispatcher.dispatch(params);
      scanInfo.scanParams.setScanDispatch(prefs);

      ThreadPoolExecutor executor = scanExecutors.get(prefs.getExecutorName());
      if (executor == null) {
        log.warn(
            "For table id {}, {} dispatched to non-existent executor {} Using default executor.",
            tablet.tableId(), dispatcher.getClass().getName(), prefs.getExecutorName());
        executor = scanExecutors.get(SimpleScanDispatcher.DEFAULT_SCAN_EXECUTOR_NAME);
      } else if ("meta".equals(prefs.getExecutorName())) {
        log.warn("For table id {}, {} dispatched to meta executor. Using default executor.",
            tablet.tableId(), dispatcher.getClass().getName());
        executor = scanExecutors.get(SimpleScanDispatcher.DEFAULT_SCAN_EXECUTOR_NAME);
      }
      executor.execute(task);
    }
  }

  public void addAssignment(KeyExtent extent, Logger log, AssignmentHandler assignmentHandler) {
    assignmentPool
        .execute(new ActiveAssignmentRunnable(activeAssignments, extent, assignmentHandler));
  }

  public void addMetaDataAssignment(KeyExtent extent, Logger log,
      AssignmentHandler assignmentHandler) {
    assignMetaDataPool
        .execute(new ActiveAssignmentRunnable(activeAssignments, extent, assignmentHandler));
  }

  public void addMigration(KeyExtent tablet, Runnable migrationHandler) {
    if (tablet.isRootTablet()) {
      migrationHandler.run();
    } else if (tablet.isMeta()) {
      defaultMigrationPool.execute(migrationHandler);
    } else {
      migrationPool.execute(migrationHandler);
    }
  }

  public BlockCache getIndexCache() {
    return _iCache;
  }

  public BlockCache getDataCache() {
    return _dCache;
  }

  public BlockCache getSummaryCache() {
    return _sCache;
  }

  public Cache<String,Long> getFileLenCache() {
    return fileLenCache;
  }

  public ExecutorService getSummaryRetrievalExecutor() {
    return summaryRetrievalPool;
  }

  public ExecutorService getSummaryPartitionExecutor() {
    return summaryPartitionPool;
  }

  public ExecutorService getSummaryRemoteExecutor() {
    return summaryRemotePool;
  }
}

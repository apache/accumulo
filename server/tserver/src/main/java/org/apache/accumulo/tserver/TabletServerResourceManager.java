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
package org.apache.accumulo.tserver;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.KeyExtent;
import org.apache.accumulo.core.file.blockfile.cache.LruBlockCache;
import org.apache.accumulo.core.metadata.schema.DataFileValue;
import org.apache.accumulo.core.util.Daemon;
import org.apache.accumulo.core.util.LoggingRunnable;
import org.apache.accumulo.core.util.NamingThreadFactory;
import org.apache.accumulo.core.util.UtilWaitThread;
import org.apache.accumulo.server.conf.ServerConfiguration;
import org.apache.accumulo.server.fs.FileRef;
import org.apache.accumulo.server.fs.VolumeManager;
import org.apache.accumulo.server.tabletserver.LargestFirstMemoryManager;
import org.apache.accumulo.server.tabletserver.MemoryManagementActions;
import org.apache.accumulo.server.tabletserver.MemoryManager;
import org.apache.accumulo.server.tabletserver.TabletState;
import org.apache.accumulo.server.util.time.SimpleTimer;
import org.apache.accumulo.trace.instrument.TraceExecutorService;
import org.apache.accumulo.tserver.FileManager.ScanFileManager;
import org.apache.accumulo.tserver.Tablet.MinorCompactionReason;
import org.apache.accumulo.tserver.TabletServer.AssignmentHandler;
import org.apache.accumulo.tserver.compaction.CompactionStrategy;
import org.apache.accumulo.tserver.compaction.DefaultCompactionStrategy;
import org.apache.accumulo.tserver.compaction.MajorCompactionReason;
import org.apache.accumulo.tserver.compaction.MajorCompactionRequest;
import org.apache.log4j.Logger;

/**
 * ResourceManager is responsible for managing the resources of all tablets within a tablet server.
 *
 *
 *
 */
public class TabletServerResourceManager {

  private ExecutorService minorCompactionThreadPool;
  private ExecutorService majorCompactionThreadPool;
  private ExecutorService rootMajorCompactionThreadPool;
  private ExecutorService defaultMajorCompactionThreadPool;
  private ExecutorService splitThreadPool;
  private ExecutorService defaultSplitThreadPool;
  private ExecutorService defaultMigrationPool;
  private ExecutorService migrationPool;
  private ExecutorService assignmentPool;
  private ExecutorService assignMetaDataPool;
  private ExecutorService readAheadThreadPool;
  private ExecutorService defaultReadAheadThreadPool;
  private Map<String,ExecutorService> threadPools = new TreeMap<String,ExecutorService>();

  private final ConcurrentHashMap<KeyExtent,RunnableStartedAt> activeAssignments;

  private HashSet<TabletResourceManager> tabletResources;

  private final VolumeManager fs;

  private FileManager fileManager;

  private MemoryManager memoryManager;

  private MemoryManagementFramework memMgmt;

  private final LruBlockCache _dCache;
  private final LruBlockCache _iCache;
  private final ServerConfiguration conf;

  private static final Logger log = Logger.getLogger(TabletServerResourceManager.class);

  private ExecutorService addEs(String name, ExecutorService tp) {
    if (threadPools.containsKey(name)) {
      throw new IllegalArgumentException("Cannot create two executor services with same name " + name);
    }
    tp = new TraceExecutorService(tp);
    threadPools.put(name, tp);
    return tp;
  }

  private ExecutorService addEs(final Property maxThreads, String name, final ThreadPoolExecutor tp) {
    ExecutorService result = addEs(name, tp);
    SimpleTimer.getInstance().schedule(new Runnable() {
      @Override
      public void run() {
        try {
          int max = conf.getConfiguration().getCount(maxThreads);
          if (tp.getMaximumPoolSize() != max) {
            log.info("Changing " + maxThreads.getKey() + " to " + max);
            tp.setCorePoolSize(max);
            tp.setMaximumPoolSize(max);
          }
        } catch (Throwable t) {
          log.error(t, t);
        }
      }

    }, 1000, 10 * 1000);
    return result;
  }

  private ExecutorService createEs(int max, String name) {
    return addEs(name, Executors.newFixedThreadPool(max, new NamingThreadFactory(name)));
  }

  private ExecutorService createEs(Property max, String name) {
    return createEs(max, name, new LinkedBlockingQueue<Runnable>());
  }

  private ExecutorService createEs(Property max, String name, BlockingQueue<Runnable> queue) {
    int maxThreads = conf.getConfiguration().getCount(max);
    ThreadPoolExecutor tp = new ThreadPoolExecutor(maxThreads, maxThreads, 0L, TimeUnit.MILLISECONDS, queue, new NamingThreadFactory(name));
    return addEs(max, name, tp);
  }

  private ExecutorService createEs(int min, int max, int timeout, String name) {
    return addEs(name, new ThreadPoolExecutor(min, max, timeout, TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>(), new NamingThreadFactory(name)));
  }

  public TabletServerResourceManager(Instance instance, VolumeManager fs) {
    this.conf = new ServerConfiguration(instance);
    this.fs = fs;
    final AccumuloConfiguration acuConf = conf.getConfiguration();

    long maxMemory = acuConf.getMemoryInBytes(Property.TSERV_MAXMEM);
    boolean usingNativeMap = acuConf.getBoolean(Property.TSERV_NATIVEMAP_ENABLED) && NativeMap.isLoaded();

    long blockSize = acuConf.getMemoryInBytes(Property.TSERV_DEFAULT_BLOCKSIZE);
    long dCacheSize = acuConf.getMemoryInBytes(Property.TSERV_DATACACHE_SIZE);
    long iCacheSize = acuConf.getMemoryInBytes(Property.TSERV_INDEXCACHE_SIZE);

    _iCache = new LruBlockCache(iCacheSize, blockSize);
    _dCache = new LruBlockCache(dCacheSize, blockSize);

    Runtime runtime = Runtime.getRuntime();
    if (!usingNativeMap && maxMemory + dCacheSize + iCacheSize > runtime.maxMemory()) {
      throw new IllegalArgumentException(String.format(
          "Maximum tablet server map memory %,d and block cache sizes %,d is too large for this JVM configuration %,d", maxMemory, dCacheSize + iCacheSize,
          runtime.maxMemory()));
    }
    runtime.gc();

    // totalMemory - freeMemory = memory in use
    // maxMemory - memory in use = max available memory
    if (!usingNativeMap && maxMemory > runtime.maxMemory() - (runtime.totalMemory() - runtime.freeMemory())) {
      log.warn("In-memory map may not fit into local memory space.");
    }

    minorCompactionThreadPool = createEs(Property.TSERV_MINC_MAXCONCURRENT, "minor compactor");

    // make this thread pool have a priority queue... and execute tablets with the most
    // files first!
    majorCompactionThreadPool = createEs(Property.TSERV_MAJC_MAXCONCURRENT, "major compactor", new CompactionQueue());
    rootMajorCompactionThreadPool = createEs(0, 1, 300, "md root major compactor");
    defaultMajorCompactionThreadPool = createEs(0, 1, 300, "md major compactor");

    splitThreadPool = createEs(1, "splitter");
    defaultSplitThreadPool = createEs(0, 1, 60, "md splitter");

    defaultMigrationPool = createEs(0, 1, 60, "metadata tablet migration");
    migrationPool = createEs(Property.TSERV_MIGRATE_MAXCONCURRENT, "tablet migration");

    // not sure if concurrent assignments can run safely... even if they could there is probably no benefit at startup because
    // individual tablet servers are already running assignments concurrently... having each individual tablet server run
    // concurrent assignments would put more load on the metadata table at startup
    assignmentPool = createEs(1, "tablet assignment");

    assignMetaDataPool = createEs(0, 1, 60, "metadata tablet assignment");

    activeAssignments = new ConcurrentHashMap<KeyExtent,RunnableStartedAt>();

    readAheadThreadPool = createEs(Property.TSERV_READ_AHEAD_MAXCONCURRENT, "tablet read ahead");
    defaultReadAheadThreadPool = createEs(Property.TSERV_METADATA_READ_AHEAD_MAXCONCURRENT, "metadata tablets read ahead");

    tabletResources = new HashSet<TabletResourceManager>();

    int maxOpenFiles = acuConf.getCount(Property.TSERV_SCAN_MAX_OPENFILES);

    fileManager = new FileManager(conf, fs, maxOpenFiles, _dCache, _iCache);

    memoryManager = Property.createInstanceFromPropertyName(acuConf, Property.TSERV_MEM_MGMT, MemoryManager.class, new LargestFirstMemoryManager());
    memoryManager.init(conf);
    memMgmt = new MemoryManagementFramework();
    memMgmt.startThreads();

    SimpleTimer timer = SimpleTimer.getInstance();

    // We can use the same map for both metadata and normal assignments since the keyspace (extent)
    // is guaranteed to be unique. Schedule the task once, the task will reschedule itself.
    timer.schedule(new AssignmentWatcher(acuConf, activeAssignments, timer), 5000);
  }

  /**
   * Accepts some map which is tracking active assignment task(s) (running) and monitors them to ensure that the time the assignment(s) have been running don't
   * exceed a threshold. If the time is exceeded a warning is printed and a stack trace is logged for the running assignment.
   */
  protected static class AssignmentWatcher implements Runnable {
    private static final Logger log = Logger.getLogger(AssignmentWatcher.class);

    private final Map<KeyExtent,RunnableStartedAt> activeAssignments;
    private final AccumuloConfiguration conf;
    private final SimpleTimer timer;

    public AssignmentWatcher(AccumuloConfiguration conf, Map<KeyExtent,RunnableStartedAt> activeAssignments, SimpleTimer timer) {
      this.conf = conf;
      this.activeAssignments = activeAssignments;
      this.timer = timer;
    }

    @Override
    public void run() {
      final long millisBeforeWarning = conf.getTimeInMillis(Property.TSERV_ASSIGNMENT_DURATION_WARNING);
      try {
        long now = System.currentTimeMillis();
        KeyExtent extent;
        RunnableStartedAt runnable;
        for (Entry<KeyExtent,RunnableStartedAt> entry : activeAssignments.entrySet()) {
          extent = entry.getKey();
          runnable = entry.getValue();
          final long duration = now - runnable.getStartTime();

          // Print a warning if an assignment has been running for over the configured time length
          if (duration > millisBeforeWarning) {
            log.warn("Assignment for " + extent + " has been running for at least " + duration + "ms", runnable.getTask().getException());
          } else if (log.isTraceEnabled()) {
            log.trace("Assignment for " + extent + " only running for " + duration + "ms");
          }
        }
      } catch (Exception e) {
        log.warn("Caught exception checking active assignments", e);
      } finally {
        // Don't run more often than every 5s
        long delay = Math.max((long) (millisBeforeWarning * 0.5), 5000l);
        if (log.isTraceEnabled()) {
          log.trace("Rescheduling assignment watcher to run in " + delay + "ms");
        }
        timer.schedule(this, delay);
      }
    }
  }

  private static class TabletStateImpl implements TabletState, Cloneable {

    private long lct;
    private Tablet tablet;
    private long mts;
    private long mcmts;

    public TabletStateImpl(Tablet t, long mts, long lct, long mcmts) {
      this.tablet = t;
      this.mts = mts;
      this.lct = lct;
      this.mcmts = mcmts;
    }

    @Override
    public KeyExtent getExtent() {
      return tablet.getExtent();
    }

    Tablet getTablet() {
      return tablet;
    }

    @Override
    public long getLastCommitTime() {
      return lct;
    }

    @Override
    public long getMemTableSize() {
      return mts;
    }

    @Override
    public long getMinorCompactingMemTableSize() {
      return mcmts;
    }
  }

  private class MemoryManagementFramework {
    private final Map<KeyExtent,TabletStateImpl> tabletReports;
    private LinkedBlockingQueue<TabletStateImpl> memUsageReports;
    private long lastMemCheckTime = System.currentTimeMillis();
    private long maxMem;
    private Thread memoryGuardThread;
    private Thread minorCompactionInitiatorThread;

    MemoryManagementFramework() {
      tabletReports = Collections.synchronizedMap(new HashMap<KeyExtent,TabletStateImpl>());
      memUsageReports = new LinkedBlockingQueue<TabletStateImpl>();
      maxMem = conf.getConfiguration().getMemoryInBytes(Property.TSERV_MAXMEM);

      Runnable r1 = new Runnable() {
        @Override
        public void run() {
          processTabletMemStats();
        }
      };

      memoryGuardThread = new Daemon(new LoggingRunnable(log, r1));
      memoryGuardThread.setPriority(Thread.NORM_PRIORITY + 1);
      memoryGuardThread.setName("Accumulo Memory Guard");

      Runnable r2 = new Runnable() {
        @Override
        public void run() {
          manageMemory();
        }
      };

      minorCompactionInitiatorThread = new Daemon(new LoggingRunnable(log, r2));
      minorCompactionInitiatorThread.setName("Accumulo Minor Compaction Initiator");
    }

    void startThreads() {
      memoryGuardThread.start();
      minorCompactionInitiatorThread.start();
    }

    private long lastMemTotal = 0;

    private void processTabletMemStats() {
      while (true) {
        try {

          TabletStateImpl report = memUsageReports.take();

          while (report != null) {
            tabletReports.put(report.getExtent(), report);
            report = memUsageReports.poll();
          }

          long delta = System.currentTimeMillis() - lastMemCheckTime;
          if (holdCommits || delta > 50 || lastMemTotal > 0.90 * maxMem) {
            lastMemCheckTime = System.currentTimeMillis();

            long totalMemUsed = 0;

            synchronized (tabletReports) {
              for (TabletStateImpl tsi : tabletReports.values()) {
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
          log.warn(e, e);
        }
      }
    }

    private void manageMemory() {
      while (true) {
        MemoryManagementActions mma = null;

        try {
          ArrayList<TabletState> tablets;
          synchronized (tabletReports) {
            tablets = new ArrayList<TabletState>(tabletReports.values());
          }
          mma = memoryManager.getMemoryManagementActions(tablets);

        } catch (Throwable t) {
          log.error("Memory manager failed " + t.getMessage(), t);
        }

        try {
          if (mma != null && mma.tabletsToMinorCompact != null && mma.tabletsToMinorCompact.size() > 0) {
            for (KeyExtent keyExtent : mma.tabletsToMinorCompact) {
              TabletStateImpl tabletReport = tabletReports.get(keyExtent);

              if (tabletReport == null) {
                log.warn("Memory manager asked to compact nonexistant tablet " + keyExtent);
                continue;
              }

              if (!tabletReport.getTablet().initiateMinorCompaction(MinorCompactionReason.SYSTEM)) {
                if (tabletReport.getTablet().isClosed()) {
                  tabletReports.remove(tabletReport.getExtent());
                  log.debug("Ignoring memory manager recommendation: not minor compacting closed tablet " + keyExtent);
                } else {
                  log.info("Ignoring memory manager recommendation: not minor compacting " + keyExtent);
                }
              }
            }

            // log.debug("mma.tabletsToMinorCompact = "+mma.tabletsToMinorCompact);
          }
        } catch (Throwable t) {
          log.error("Minor compactions for memory managment failed", t);
        }

        UtilWaitThread.sleep(250);
      }
    }

    public void updateMemoryUsageStats(Tablet tablet, long size, long lastCommitTime, long mincSize) {
      memUsageReports.add(new TabletStateImpl(tablet, size, lastCommitTime, mincSize));
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
          log.debug(String.format("Commits held for %6.2f secs", (System.currentTimeMillis() - holdStartTime) / 1000.0));
          commitHold.notifyAll();
        }
      }
    }

  }

  void waitUntilCommitsAreEnabled() {
    if (holdCommits) {
      long timeout = System.currentTimeMillis() + conf.getConfiguration().getTimeInMillis(Property.GENERAL_RPC_TIMEOUT);
      synchronized (commitHold) {
        while (holdCommits) {
          try {
            if (System.currentTimeMillis() > timeout)
              throw new HoldTimeoutException("Commits are held");
            commitHold.wait(1000);
          } catch (InterruptedException e) {}
        }
      }
    }
  }

  public long holdTime() {
    if (!holdCommits)
      return 0;
    synchronized (commitHold) {
      return System.currentTimeMillis() - holdStartTime;
    }
  }

  public void close() {
    for (ExecutorService executorService : threadPools.values()) {
      executorService.shutdown();
    }

    for (Entry<String,ExecutorService> entry : threadPools.entrySet()) {
      while (true) {
        try {
          if (entry.getValue().awaitTermination(60, TimeUnit.SECONDS))
            break;
          log.info("Waiting for thread pool " + entry.getKey() + " to shutdown");
        } catch (InterruptedException e) {
          log.warn(e);
        }
      }
    }
  }

  public synchronized TabletResourceManager createTabletResourceManager() {
    TabletResourceManager trm = new TabletResourceManager();
    return trm;
  }

  synchronized private void addTabletResource(TabletResourceManager tr) {
    tabletResources.add(tr);
  }

  synchronized private void removeTabletResource(TabletResourceManager tr) {
    tabletResources.remove(tr);
  }

  public class TabletResourceManager {

    private final long creationTime = System.currentTimeMillis();

    private volatile boolean openFilesReserved = false;

    private volatile boolean closed = false;

    private Tablet tablet;

    private AccumuloConfiguration tableConf;

    TabletResourceManager() {}

    void setTablet(Tablet tablet, AccumuloConfiguration tableConf) {
      this.tablet = tablet;
      this.tableConf = tableConf;
      // TabletResourceManager is not really initialized until this
      // function is called.... so do not make it publicly available
      // until now

      addTabletResource(this);
    }

    // BEGIN methods that Tablets call to manage their set of open map files

    public void importedMapFiles() {
      lastReportedCommitTime = System.currentTimeMillis();
    }

    synchronized ScanFileManager newScanFileManager() {
      if (closed)
        throw new IllegalStateException("closed");
      return fileManager.newScanFileManager(tablet.getExtent());
    }

    // END methods that Tablets call to manage their set of open map files

    // BEGIN methods that Tablets call to manage memory

    private AtomicLong lastReportedSize = new AtomicLong();
    private AtomicLong lastReportedMincSize = new AtomicLong();
    private volatile long lastReportedCommitTime = 0;

    public void updateMemoryUsageStats(long size, long mincSize) {

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
      if ((lrms > 0 && mincSize == 0 || lrms == 0 && mincSize > 0) && lastReportedMincSize.compareAndSet(lrms, mincSize)) {
        report = true;
      }

      long currentTime = System.currentTimeMillis();
      if ((delta > 32000 || delta < 0 || (currentTime - lastReportedCommitTime > 1000)) && lastReportedSize.compareAndSet(lrs, totalSize)) {
        if (delta > 0)
          lastReportedCommitTime = currentTime;
        report = true;
      }

      if (report)
        memMgmt.updateMemoryUsageStats(tablet, size, lastReportedCommitTime, mincSize);
    }

    // END methods that Tablets call to manage memory

    // BEGIN methods that Tablets call to make decisions about major compaction
    // when too many files are open, we may want tablets to compact down
    // to one map file
    boolean needsMajorCompaction(SortedMap<FileRef,DataFileValue> tabletFiles, MajorCompactionReason reason) {
      if (closed)
        return false;// throw new IOException("closed");

      // int threshold;

      if (reason == MajorCompactionReason.USER)
        return true;

      if (reason == MajorCompactionReason.IDLE) {
        // threshold = 1;
        long idleTime;
        if (lastReportedCommitTime == 0) {
          // no commits, so compute how long the tablet has been assigned to the
          // tablet server
          idleTime = System.currentTimeMillis() - creationTime;
        } else {
          idleTime = System.currentTimeMillis() - lastReportedCommitTime;
        }

        if (idleTime < tableConf.getTimeInMillis(Property.TABLE_MAJC_COMPACTALL_IDLETIME)) {
          return false;
        }
      }
      CompactionStrategy strategy = Property.createTableInstanceFromPropertyName(tableConf, Property.TABLE_COMPACTION_STRATEGY, CompactionStrategy.class,
          new DefaultCompactionStrategy());
      strategy.init(Property.getCompactionStrategyOptions(tableConf));
      MajorCompactionRequest request = new MajorCompactionRequest(tablet.getExtent(), reason, TabletServerResourceManager.this.fs, tableConf);
      request.setFiles(tabletFiles);
      try {
        return strategy.shouldCompact(request);
      } catch (IOException ex) {
        return false;
      }
    }

    // END methods that Tablets call to make decisions about major compaction

    // tablets call this method to run minor compactions,
    // this allows us to control how many minor compactions
    // run concurrently in a tablet server
    void executeMinorCompaction(final Runnable r) {
      minorCompactionThreadPool.execute(new LoggingRunnable(log, r));
    }

    void close() throws IOException {
      // always obtain locks in same order to avoid deadlock
      synchronized (TabletServerResourceManager.this) {
        synchronized (this) {
          if (closed)
            throw new IOException("closed");
          if (openFilesReserved)
            throw new IOException("tired to close files while open files reserved");

          TabletServerResourceManager.this.removeTabletResource(this);

          memMgmt.tabletClosed(tablet.getExtent());
          memoryManager.tabletClosed(tablet.getExtent());

          closed = true;
        }
      }
    }

    public TabletServerResourceManager getTabletServerResourceManager() {
      return TabletServerResourceManager.this;
    }

    public void executeMajorCompaction(KeyExtent tablet, Runnable compactionTask) {
      TabletServerResourceManager.this.executeMajorCompaction(tablet, compactionTask);
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

  public void executeMajorCompaction(KeyExtent tablet, Runnable compactionTask) {
    if (tablet.isRootTablet()) {
      rootMajorCompactionThreadPool.execute(compactionTask);
    } else if (tablet.isMeta()) {
      defaultMajorCompactionThreadPool.execute(compactionTask);
    } else {
      majorCompactionThreadPool.execute(compactionTask);
    }
  }

  public void executeReadAhead(KeyExtent tablet, Runnable task) {
    if (tablet.isRootTablet()) {
      task.run();
    } else if (tablet.isMeta()) {
      defaultReadAheadThreadPool.execute(task);
    } else {
      readAheadThreadPool.execute(task);
    }
  }

  public void addAssignment(KeyExtent extent, Logger log, AssignmentHandler assignmentHandler) {
    assignmentPool.execute(new ActiveAssignmentRunnable(activeAssignments, extent, new LoggingRunnable(log, assignmentHandler)));
  }

  public void addMetaDataAssignment(KeyExtent extent, Logger log, AssignmentHandler assignmentHandler) {
    assignMetaDataPool.execute(new ActiveAssignmentRunnable(activeAssignments, extent, new LoggingRunnable(log, assignmentHandler)));
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

  public void stopSplits() {
    splitThreadPool.shutdown();
    defaultSplitThreadPool.shutdown();
    while (true) {
      try {
        while (!splitThreadPool.awaitTermination(1, TimeUnit.MINUTES)) {
          log.info("Waiting for metadata split thread pool to stop");
        }
        while (!defaultSplitThreadPool.awaitTermination(1, TimeUnit.MINUTES)) {
          log.info("Waiting for split thread pool to stop");
        }
        break;
      } catch (InterruptedException ex) {
        log.info(ex, ex);
      }
    }
  }

  public void stopNormalAssignments() {
    assignmentPool.shutdown();
    while (true) {
      try {
        while (!assignmentPool.awaitTermination(1, TimeUnit.MINUTES)) {
          log.info("Waiting for assignment thread pool to stop");
        }
        break;
      } catch (InterruptedException ex) {
        log.info(ex, ex);
      }
    }
  }

  public void stopMetadataAssignments() {
    assignMetaDataPool.shutdown();
    while (true) {
      try {
        while (!assignMetaDataPool.awaitTermination(1, TimeUnit.MINUTES)) {
          log.info("Waiting for metadata assignment thread pool to stop");
        }
        break;
      } catch (InterruptedException ex) {
        log.info(ex, ex);
      }
    }
  }

  public LruBlockCache getIndexCache() {
    return _iCache;
  }

  public LruBlockCache getDataCache() {
    return _dCache;
  }

}

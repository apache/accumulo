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
package org.apache.accumulo.server.tabletserver;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.SortedMap;
import java.util.TimerTask;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.accumulo.cloudtrace.instrument.TraceExecutorService;
import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.KeyExtent;
import org.apache.accumulo.core.file.blockfile.cache.LruBlockCache;
import org.apache.accumulo.core.util.Daemon;
import org.apache.accumulo.core.util.LoggingRunnable;
import org.apache.accumulo.core.util.MetadataTable.DataFileValue;
import org.apache.accumulo.core.util.UtilWaitThread;
import org.apache.accumulo.server.conf.ServerConfiguration;
import org.apache.accumulo.server.tabletserver.FileManager.ScanFileManager;
import org.apache.accumulo.server.tabletserver.Tablet.MajorCompactionReason;
import org.apache.accumulo.server.util.NamingThreadFactory;
import org.apache.accumulo.server.util.time.SimpleTimer;
import org.apache.accumulo.start.classloader.AccumuloClassLoader;
import org.apache.hadoop.fs.FileSystem;
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
  
  private HashSet<TabletResourceManager> tabletResources;
  
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
    SimpleTimer.getInstance().schedule(new TimerTask() {
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
  
  public TabletServerResourceManager(Instance instance, FileSystem fs) {
    this.conf = new ServerConfiguration(instance);
    final AccumuloConfiguration acuConf = conf.getConfiguration();
    
    long maxMemory = acuConf.getMemoryInBytes(Property.TSERV_MAXMEM);
    boolean usingNativeMap = acuConf.getBoolean(Property.TSERV_NATIVEMAP_ENABLED) && NativeMap.loadedNativeLibraries();
    
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
    
    readAheadThreadPool = createEs(Property.TSERV_READ_AHEAD_MAXCONCURRENT, "tablet read ahead");
    defaultReadAheadThreadPool = createEs(Property.TSERV_METADATA_READ_AHEAD_MAXCONCURRENT, "metadata tablets read ahead");
    
    tabletResources = new HashSet<TabletResourceManager>();
    
    int maxOpenFiles = acuConf.getCount(Property.TSERV_SCAN_MAX_OPENFILES);
    
    fileManager = new FileManager(conf, fs, maxOpenFiles, _dCache, _iCache);
    
    try {
      Class<? extends MemoryManager> clazz = AccumuloClassLoader.loadClass(acuConf.get(Property.TSERV_MEM_MGMT), MemoryManager.class);
      memoryManager = clazz.newInstance();
      memoryManager.init(conf);
      log.debug("Loaded memory manager : " + memoryManager.getClass().getName());
    } catch (Exception e) {
      log.error("Failed to find memory manger in config, using default", e);
    }
    
    if (memoryManager == null) {
      memoryManager = new LargestFirstMemoryManager();
    }
    
    memMgmt = new MemoryManagementFramework();
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
    
    public KeyExtent getExtent() {
      return tablet.getExtent();
    }
    
    Tablet getTablet() {
      return tablet;
    }
    
    public long getLastCommitTime() {
      return lct;
    }
    
    public long getMemTableSize() {
      return mts;
    }
    
    public long getMinorCompactingMemTableSize() {
      return mcmts;
    }
  }
  
  private class MemoryManagementFramework {
    private Map<KeyExtent,TabletStateImpl> tabletReports;
    private LinkedBlockingQueue<TabletStateImpl> memUsageReports;
    private long lastMemCheckTime = System.currentTimeMillis();
    private long maxMem;
    
    MemoryManagementFramework() {
      tabletReports = Collections.synchronizedMap(new HashMap<KeyExtent,TabletStateImpl>());
      memUsageReports = new LinkedBlockingQueue<TabletStateImpl>();
      maxMem = conf.getConfiguration().getMemoryInBytes(Property.TSERV_MAXMEM);
      
      Runnable r1 = new Runnable() {
        public void run() {
          processTabletMemStats();
        }
      };
      
      Thread t1 = new Daemon(new LoggingRunnable(log, r1));
      t1.setPriority(Thread.NORM_PRIORITY + 1);
      t1.setName("Accumulo Memory Guard");
      t1.start();
      
      Runnable r2 = new Runnable() {
        public void run() {
          manageMemory();
        }
      };
      
      Thread t2 = new Daemon(new LoggingRunnable(log, r2));
      t2.setName("Accumulo Minor Compaction Initiator");
      t2.start();
      
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
              
              if (!tabletReport.getTablet().initiateMinorCompaction()) {
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
  
  private Object commitHold = new String("");
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
  
  private class MapFileInfo {
    private final String path;
    private final long size;
    
    MapFileInfo(String path, long size) {
      this.path = path;
      this.size = size;
    }
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
    Map<String,Long> findMapFilesToCompact(SortedMap<String,DataFileValue> tabletFiles, MajorCompactionReason reason) {
      if (reason == MajorCompactionReason.ALL) {
        Map<String,Long> files = new HashMap<String,Long>();
        for (Entry<String,DataFileValue> entry : tabletFiles.entrySet()) {
          files.put(entry.getKey(), entry.getValue().getSize());
        }
        return files;
      }
      
      if (tabletFiles.size() <= 1)
        return null;
      TreeSet<MapFileInfo> candidateFiles = new TreeSet<MapFileInfo>(new Comparator<MapFileInfo>() {
        @Override
        public int compare(MapFileInfo o1, MapFileInfo o2) {
          if (o1 == o2)
            return 0;
          if (o1.size < o2.size)
            return -1;
          if (o1.size > o2.size)
            return 1;
          return o1.path.compareTo(o2.path);
        }
      });
      
      double ratio = tableConf.getFraction(Property.TABLE_MAJC_RATIO);
      int maxFilesToCompact = tableConf.getCount(Property.TSERV_MAJC_THREAD_MAXOPEN);
      int maxFilesPerTablet = tableConf.getMaxFilesPerTablet();
      
      for (Entry<String,DataFileValue> entry : tabletFiles.entrySet()) {
        candidateFiles.add(new MapFileInfo(entry.getKey(), entry.getValue().getSize()));
      }
      
      long totalSize = 0;
      for (MapFileInfo mfi : candidateFiles) {
        totalSize += mfi.size;
      }
      
      Map<String,Long> files = new HashMap<String,Long>();
      
      while (candidateFiles.size() > 1) {
        MapFileInfo max = candidateFiles.last();
        if (max.size * ratio <= totalSize) {
          files.clear();
          for (MapFileInfo mfi : candidateFiles) {
            files.put(mfi.path, mfi.size);
            if (files.size() >= maxFilesToCompact)
              break;
          }
          
          break;
        }
        totalSize -= max.size;
        candidateFiles.remove(max);
      }
      
      int totalFilesToCompact = 0;
      if (tabletFiles.size() > maxFilesPerTablet)
        totalFilesToCompact = tabletFiles.size() - maxFilesPerTablet + 1;
      
      totalFilesToCompact = Math.min(totalFilesToCompact, maxFilesToCompact);
      
      if (files.size() < totalFilesToCompact) {
        
        TreeMap<String,DataFileValue> tfc = new TreeMap<String,DataFileValue>(tabletFiles);
        tfc.keySet().removeAll(files.keySet());
        
        // put data in candidateFiles to sort it
        candidateFiles.clear();
        for (Entry<String,DataFileValue> entry : tfc.entrySet())
          candidateFiles.add(new MapFileInfo(entry.getKey(), entry.getValue().getSize()));
        
        for (MapFileInfo mfi : candidateFiles) {
          files.put(mfi.path, mfi.size);
          if (files.size() >= totalFilesToCompact)
            break;
        }
      }
      
      if (files.size() == 0)
        return null;
      
      return files;
    }
    
    boolean needsMajorCompaction(SortedMap<String,DataFileValue> tabletFiles, MajorCompactionReason reason) {
      if (closed)
        return false;// throw new IOException("closed");
        
      // int threshold;
      
      if (reason == MajorCompactionReason.ALL)
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
      }/*
        * else{ threshold = tableConf.getCount(Property.TABLE_MAJC_THRESHOLD); }
        */
      
      return findMapFilesToCompact(tabletFiles, reason) != null;
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
    if (tablet.equals(Constants.ROOT_TABLET_EXTENT)) {
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
  
  public void addAssignment(Runnable assignmentHandler) {
    assignmentPool.execute(assignmentHandler);
  }
  
  public void addMetaDataAssignment(Runnable assignmentHandler) {
    assignMetaDataPool.execute(assignmentHandler);
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

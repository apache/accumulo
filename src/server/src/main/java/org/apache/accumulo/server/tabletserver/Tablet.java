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

/*
 * We need to be able to have the master tell a tabletServer to
 * close this file, and the tablet server to handle all pending client reads
 * before closing
 * 
 */

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.impl.HdfsZooInstance;
import org.apache.accumulo.core.client.impl.ScannerImpl;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.ConfigurationObserver;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.conf.TableConfiguration;
import org.apache.accumulo.core.constraints.Violations;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Column;
import org.apache.accumulo.core.data.ColumnUpdate;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.KeyExtent;
import org.apache.accumulo.core.data.KeyValue;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.data.thrift.IterInfo;
import org.apache.accumulo.core.data.thrift.MapFileInfo;
import org.apache.accumulo.core.file.FileOperations;
import org.apache.accumulo.core.file.FileSKVIterator;
import org.apache.accumulo.core.file.FileSKVWriter;
import org.apache.accumulo.core.file.FileUtil;
import org.apache.accumulo.core.file.map.MyMapFile;
import org.apache.accumulo.core.iterators.CountingIterator;
import org.apache.accumulo.core.iterators.DeletingIterator;
import org.apache.accumulo.core.iterators.InterruptibleIterator;
import org.apache.accumulo.core.iterators.IterationInterruptedException;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.IteratorUtil;
import org.apache.accumulo.core.iterators.IteratorUtil.IteratorScope;
import org.apache.accumulo.core.iterators.MultiIterator;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.SourceSwitchingIterator;
import org.apache.accumulo.core.iterators.SourceSwitchingIterator.DataSource;
import org.apache.accumulo.core.iterators.SystemScanIterator;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.accumulo.core.security.thrift.AuthInfo;
import org.apache.accumulo.core.util.CachedConfiguration;
import org.apache.accumulo.core.util.LocalityGroupUtil;
import org.apache.accumulo.core.util.LocalityGroupUtil.LocalityGroupConfigurationError;
import org.apache.accumulo.core.util.MetadataTable.DataFileValue;
import org.apache.accumulo.core.util.Pair;
import org.apache.accumulo.core.util.TabletOperations;
import org.apache.accumulo.core.util.UtilWaitThread;
import org.apache.accumulo.core.zookeeper.ZooLock;
import org.apache.accumulo.core.zookeeper.ZooSession;
import org.apache.accumulo.server.constraints.ConstraintChecker;
import org.apache.accumulo.server.constraints.ConstraintLoader;
import org.apache.accumulo.server.constraints.UnsatisfiableConstraint;
import org.apache.accumulo.server.master.state.TServerInstance;
import org.apache.accumulo.server.problems.ProblemReport;
import org.apache.accumulo.server.problems.ProblemReportingIterator;
import org.apache.accumulo.server.problems.ProblemReports;
import org.apache.accumulo.server.problems.ProblemType;
import org.apache.accumulo.server.security.SecurityConstants;
import org.apache.accumulo.server.tabletserver.FileManager.ScanFileManager;
import org.apache.accumulo.server.tabletserver.InMemoryMap.MemoryIterator;
import org.apache.accumulo.server.tabletserver.TabletServerResourceManager.TabletResourceManager;
import org.apache.accumulo.server.tabletserver.TabletStatsKeeper.Operation;
import org.apache.accumulo.server.tabletserver.log.MutationReceiver;
import org.apache.accumulo.server.tabletserver.log.RemoteLogger;
import org.apache.accumulo.server.tabletserver.metrics.TabletServerMinCMetrics;
import org.apache.accumulo.server.util.MapCounter;
import org.apache.accumulo.server.util.MetadataTable;
import org.apache.accumulo.server.util.MetadataTable.LogEntry;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

import cloudtrace.instrument.Span;
import cloudtrace.instrument.Trace;

/**
 * 
 * this class just provides an interface to read from a MapFile mostly takes care of reporting start and end keys
 * 
 * need this because a single row extent can have multiple columns this manages all the columns (each handled by a store) for a single row-extent
 * 
 * 
 */

@SuppressWarnings("deprecation")
public class Tablet {
  
  public class CommitSession {
    
    private int seq;
    private InMemoryMap memTable;
    private int commitsInProgress;
    private long maxCommittedTime = Long.MIN_VALUE;
    
    private CommitSession(int seq, InMemoryMap imm) {
      this.seq = seq;
      this.memTable = imm;
      commitsInProgress = 0;
    }
    
    public int getWALogSeq() {
      return seq;
    }
    
    private void decrementCommitsInProgress() {
      if (commitsInProgress < 1)
        throw new IllegalStateException("commitsInProgress = " + commitsInProgress);
      
      commitsInProgress--;
      if (commitsInProgress == 0)
        Tablet.this.notifyAll();
    }
    
    private void incrementCommitsInProgress() {
      if (commitsInProgress < 0)
        throw new IllegalStateException("commitsInProgress = " + commitsInProgress);
      
      commitsInProgress++;
    }
    
    private void waitForCommitsToFinish() {
      while (commitsInProgress > 0) {
        try {
          Tablet.this.wait(50);
        } catch (InterruptedException e) {
          log.warn(e, e);
        }
      }
    }
    
    public void abortCommit(List<Mutation> value) {
      Tablet.this.abortCommit(this, value);
    }
    
    public void commit(List<Mutation> mutations) {
      Tablet.this.commit(this, mutations);
    }
    
    public Tablet getTablet() {
      return Tablet.this;
    }
    
    public boolean beginUpdatingLogsUsed(ArrayList<RemoteLogger> copy, boolean mincFinish) {
      return Tablet.this.beginUpdatingLogsUsed(memTable, copy, mincFinish);
    }
    
    public void finishUpdatingLogsUsed() {
      Tablet.this.finishUpdatingLogsUsed();
    }
    
    public int getLogId() {
      return logId;
    }
    
    public KeyExtent getExtent() {
      return extent;
    }
    
    private void updateMaxCommittedTime(long time) {
      maxCommittedTime = Math.max(time, maxCommittedTime);
    }
    
    private long getMaxCommittedTime() {
      if (maxCommittedTime == Long.MIN_VALUE)
        throw new IllegalStateException("Tried to read max committed time when it was never set");
      return maxCommittedTime;
    }
    
  }
  
  private class TabletMemory {
    private InMemoryMap memTable;
    private InMemoryMap otherMemTable;
    private InMemoryMap deletingMemTable;
    private int nextSeq = 1;
    private CommitSession commitSession;
    
    TabletMemory() {
      memTable = new InMemoryMap();
      commitSession = new CommitSession(nextSeq, memTable);
      nextSeq += 2;
    }
    
    InMemoryMap getMemTable() {
      return memTable;
    }
    
    InMemoryMap getMinCMemTable() {
      return otherMemTable;
    }
    
    CommitSession prepareForMinC() {
      if (otherMemTable != null) {
        throw new IllegalStateException();
      }
      
      if (deletingMemTable != null) {
        throw new IllegalStateException();
      }
      
      otherMemTable = memTable;
      memTable = new InMemoryMap();
      
      CommitSession oldCommitSession = commitSession;
      commitSession = new CommitSession(nextSeq, memTable);
      nextSeq += 2;
      
      tabletResources.updateMemoryUsageStats(memTable.estimatedSizeInBytes(), otherMemTable.estimatedSizeInBytes());
      
      return oldCommitSession;
    }
    
    void finishedMinC() {
      
      if (otherMemTable == null) {
        throw new IllegalStateException();
      }
      
      if (deletingMemTable != null) {
        throw new IllegalStateException();
      }
      
      deletingMemTable = otherMemTable;
      
      otherMemTable = null;
      Tablet.this.notifyAll();
    }
    
    void finalizeMinC() {
      try {
        deletingMemTable.delete(15000);
      } finally {
        synchronized (Tablet.this) {
          if (otherMemTable != null) {
            throw new IllegalStateException();
          }
          
          if (deletingMemTable == null) {
            throw new IllegalStateException();
          }
          
          deletingMemTable = null;
          
          tabletResources.updateMemoryUsageStats(memTable.estimatedSizeInBytes(), 0);
        }
      }
    }
    
    boolean memoryReservedForMinC() {
      return otherMemTable != null || deletingMemTable != null;
    }
    
    void waitForMinC() {
      while (otherMemTable != null || deletingMemTable != null) {
        try {
          Tablet.this.wait(50);
        } catch (InterruptedException e) {
          log.warn(e, e);
        }
      }
    }
    
    void mutate(CommitSession cm, List<Mutation> mutations) {
      cm.memTable.mutate(mutations);
    }
    
    void updateMemoryUsageStats() {
      long other = 0;
      if (otherMemTable != null)
        other = otherMemTable.estimatedSizeInBytes();
      else if (deletingMemTable != null)
        other = deletingMemTable.estimatedSizeInBytes();
      
      tabletResources.updateMemoryUsageStats(memTable.estimatedSizeInBytes(), other);
    }
    
    List<MemoryIterator> getIterators() {
      List<MemoryIterator> toReturn = new ArrayList<MemoryIterator>(2);
      toReturn.add(memTable.skvIterator());
      if (otherMemTable != null)
        toReturn.add(otherMemTable.skvIterator());
      return toReturn;
    }
    
    void returnIterators(List<MemoryIterator> iters) {
      for (MemoryIterator iter : iters) {
        iter.close();
      }
    }
    
    public long getNumEntries() {
      if (otherMemTable != null)
        return memTable.getNumEntries() + otherMemTable.getNumEntries();
      return memTable.getNumEntries();
    }
    
    CommitSession getCommitSession() {
      return commitSession;
    }
  }
  
  private TabletMemory tabletMemory;
  private TabletTime tabletTime;
  
  private Path location; // absolute path of this tablets dir
  private TServerInstance lastLocation;
  
  private Configuration conf;
  private FileSystem fs;
  
  private TableConfiguration acuTableConf;
  
  private boolean generationInitialized = false;
  private int mapfileSequence = 0;
  private int generation = 0;
  
  private AtomicLong dataSourceDeletions = new AtomicLong(0);
  private Set<ScanDataSource> activeScans = new HashSet<ScanDataSource>();
  
  private volatile boolean closing = false;
  private boolean closed = false;
  private boolean closeComplete = false;
  
  private KeyExtent extent;
  
  private TabletResourceManager tabletResources;
  private DatafileManager datafileManager;
  private volatile boolean majorCompactionInProgress = false;
  private volatile boolean majorCompactionWaitingToStart = false;
  private volatile boolean majorCompactionQueued = false;
  private volatile boolean minorCompactionInProgress = false;
  private volatile boolean minorCompactionWaitingToStart = false;
  
  private AtomicReference<ConstraintChecker> constraintChecker = new AtomicReference<ConstraintChecker>();
  
  private String tabletDirectory;
  
  private int writesInProgress = 0;
  
  private static final Logger log = Logger.getLogger(Tablet.class);
  public TabletStatsKeeper timer;
  
  private Rate queryRate = new Rate(0.2);
  private long queryCount = 0;
  
  private Rate queryByteRate = new Rate(0.2);
  private long queryBytes = 0;
  
  private Rate ingestRate = new Rate(0.2);
  private long ingestCount = 0;
  
  private Rate ingestByteRate = new Rate(0.2);
  private long ingestBytes = 0;
  
  private byte[] defaultSecurityLabel = new byte[0];
  
  private long lastMinorCompactionFinishTime;
  private long lastMapFileImportTime;
  
  private volatile long numEntries;
  private volatile long numEntriesInMemory;
  private ConfigurationObserver configObserver;
  
  private TabletServer tabletServer;
  
  private final int logId;
  
  public int getLogId() {
    return logId;
  }
  
  public static class TabletClosedException extends RuntimeException {
    public TabletClosedException(Exception e) {
      super(e);
    }
    
    public TabletClosedException() {
      super();
    }
    
    private static final long serialVersionUID = 1L;
  }
  
  private void incrementGeneration() throws IOException {
    initGeneration();
    generation++;
    mapfileSequence = 0;
  }
  
  private String getMapFilename(int gen, int seq) {
    String extension = FileOperations.getNewFileExtension(AccumuloConfiguration.getTableConfiguration(HdfsZooInstance.getInstance().getInstanceID(), extent
        .getTableId().toString()));
    return location.toString() + "/" + String.format("%05d", gen) + "_" + String.format("%05d", seq) + "." + extension;
  }
  
  String getNextMapFilename() throws IOException {
    initGeneration();
    return getMapFilename(generation, mapfileSequence++);
  }
  
  private void initGeneration() throws IOException {
    if (!generationInitialized) {
      initGeneration(this.location, datafileManager.getDatafileSizes().keySet());
      generationInitialized = true;
    }
  }
  
  private void initGeneration(Path tabletDir, Set<String> mapFileNames) throws IOException {
    
    // create a copy of the set since this method adds to it
    mapFileNames = new TreeSet<String>(mapFileNames);
    
    // add all files in tablet dir, as they may not be listed in
    // metadata table... do not want to try and create a map file
    // using an existing file name
    FileStatus[] files = fs.listStatus(tabletDir);
    
    if (files == null) {
      log.warn("Tablet " + extent + " had no dir, creating " + tabletDir);
      fs.mkdirs(tabletDir);
    } else {
      for (FileStatus fileStatus : files) {
        mapFileNames.add(fileStatus.getPath().getName());
      }
    }
    
    int maxGen = 0;
    
    // find largest gen num
    for (String file : mapFileNames) {
      int genIndex = 0;
      
      String name = (new Path(file)).getName();
      
      if (name.startsWith(MyMapFile.EXTENSION + "_"))
        genIndex = 1;
      
      int gen = Integer.parseInt(name.split("[_.]")[genIndex]);
      if (gen > maxGen) {
        maxGen = gen;
      }
    }
    
    generation = maxGen;
    
    int maxSeq = -1;
    
    // find largest Seq
    for (String file : mapFileNames) {
      
      int genIndex = 0;
      
      String name = (new Path(file)).getName();
      
      if (name.startsWith(MyMapFile.EXTENSION + "_"))
        genIndex = 1;
      
      String split[] = name.split("[_.]");
      
      int gen = Integer.parseInt(split[genIndex]);
      int seq = Integer.parseInt(split[genIndex + 1]);
      
      if (gen == maxGen && seq > maxSeq) {
        maxSeq = seq;
      }
    }
    
    mapfileSequence = maxSeq + 1;
  }
  
  public boolean isMetadataTablet() {
    return extent.getTableId().toString().equals(Constants.METADATA_TABLE_ID);
  }
  
  private static String rel2abs(String relPath, KeyExtent extent) {
    return Constants.getTablesDir() + "/" + extent.getTableId() + relPath;
  }
  
  class DatafileManager {
    private TreeMap<String,DataFileValue> datafileSizes;
    private SortedMap<String,DataFileValue> unModMap;
    
    DatafileManager(SortedMap<String,DataFileValue> datafileSizes) {
      this.datafileSizes = new TreeMap<String,DataFileValue>(datafileSizes);
      unModMap = Collections.unmodifiableSortedMap(this.datafileSizes);
      
      for (Entry<String,DataFileValue> datafiles : datafileSizes.entrySet()) {
        try {
          tabletResources.addMapFile(rel2abs(datafiles.getKey(), extent), datafiles.getValue().getSize());
        } catch (IOException e) {
          log.error("ioexception trying to open " + datafiles);
          continue;
        }
      }
    }
    
    Set<String> filesToDeleteAfterScan = new HashSet<String>();
    Map<Long,Set<String>> scanFileReservations = new HashMap<Long,Set<String>>();
    MapCounter<String> fileScanReferenceCounts = new MapCounter<String>();
    long nextScanReservationId = 0;
    boolean reservationsBlocked = false;
    
    Pair<Long,Set<String>> reserveFilesForScan() {
      synchronized (Tablet.this) {
        
        while (reservationsBlocked) {
          try {
            Tablet.this.wait(50);
          } catch (InterruptedException e) {
            log.warn(e, e);
          }
        }
        
        Set<String> absFilePaths;
        try {
          absFilePaths = tabletResources.getCopyOfMapFilePaths();
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
        
        long rid = nextScanReservationId++;
        
        scanFileReservations.put(rid, absFilePaths);
        
        for (String path : absFilePaths) {
          fileScanReferenceCounts.increment(path, 1);
        }
        
        return new Pair<Long,Set<String>>(rid, absFilePaths);
      }
    }
    
    void returnFilesForScan(Long reservationId) {
      
      final Set<String> filesToDelete = new HashSet<String>();
      
      synchronized (Tablet.this) {
        Set<String> absFilePaths = scanFileReservations.remove(reservationId);
        
        if (absFilePaths == null)
          throw new IllegalArgumentException("Unknown scan reservation id " + reservationId);
        
        boolean notify = false;
        for (String path : absFilePaths) {
          long refCount = fileScanReferenceCounts.decrement(path, 1);
          if (refCount == 0) {
            if (filesToDeleteAfterScan.remove(path))
              filesToDelete.add(path);
            notify = true;
          } else if (refCount < 0)
            throw new IllegalStateException("Scan ref count for " + path + " is " + refCount);
        }
        
        if (notify)
          Tablet.this.notifyAll();
      }
      
      if (filesToDelete.size() > 0) {
        log.debug("Removing scan refs from metadata " + extent + " " + abs2rel(filesToDelete));
        MetadataTable.removeScanFiles(extent, abs2rel(filesToDelete), SecurityConstants.systemCredentials, tabletServer.getLock());
      }
    }
    
    void removeFilesAfterScan(Set<String> scanFiles) {
      if (scanFiles.size() == 0)
        return;
      
      Set<String> filesToDelete = new HashSet<String>();
      
      synchronized (Tablet.this) {
        for (String path : scanFiles) {
          if (fileScanReferenceCounts.get(path) == 0)
            filesToDelete.add(path);
          else
            filesToDeleteAfterScan.add(path);
        }
      }
      
      if (filesToDelete.size() > 0) {
        log.debug("Removing scan refs from metadata " + extent + " " + abs2rel(filesToDelete));
        MetadataTable.removeScanFiles(extent, abs2rel(filesToDelete), SecurityConstants.systemCredentials, tabletServer.getLock());
      }
    }
    
    TreeSet<String> waitForScansToFinish(Set<String> pathsToWaitFor, boolean blockNewScans, long maxWaitTime) {
      long startTime = System.currentTimeMillis();
      TreeSet<String> inUse = new TreeSet<String>();
      
      Span waitForScans = Trace.start("waitForScans");
      synchronized (Tablet.this) {
        if (blockNewScans) {
          if (reservationsBlocked)
            throw new IllegalStateException();
          
          reservationsBlocked = true;
        }
        
        for (String path : pathsToWaitFor) {
          while (fileScanReferenceCounts.get(path) > 0 && System.currentTimeMillis() - startTime < maxWaitTime) {
            try {
              Tablet.this.wait(100);
            } catch (InterruptedException e) {
              log.warn(e, e);
            }
          }
        }
        
        for (String path : pathsToWaitFor) {
          if (fileScanReferenceCounts.get(path) > 0)
            inUse.add(path);
        }
        
        if (blockNewScans) {
          reservationsBlocked = false;
          Tablet.this.notifyAll();
        }
        
      }
      waitForScans.stop();
      return inUse;
    }
    
    public void importMapFiles(Map<String,DataFileValue> paths) throws IOException {
      
      Map<String,String> relPaths = new HashMap<String,String>(paths.size());
      Map<String,DataFileValue> relSizes = new HashMap<String,DataFileValue>(paths.size());
      
      String bulkDir = null;
      
      for (String tpath : paths.keySet()) {
        Path tmpPath = new Path(tpath);
        
        if (!tmpPath.getParent().getParent().equals(new Path(Constants.getTablesDir() + "/" + extent.getTableId()))) {
          throw new IOException("Map file " + tpath + " not in table dir " + Constants.getTablesDir() + "/" + extent.getTableId());
        }
        
        String path = "/" + tmpPath.getParent().getName() + "/" + tmpPath.getName();
        relPaths.put(tpath, path);
        relSizes.put(path, paths.get(tpath));
        
        if (bulkDir == null)
          bulkDir = tmpPath.getParent().toString();
        else if (!bulkDir.equals(tmpPath.getParent().toString()))
          throw new IllegalArgumentException("bulk files in different dirs " + bulkDir + " " + tmpPath);
        
      }
      
      if (extent.equals(Constants.ROOT_TABLET_EXTENT)) {
        throw new IllegalArgumentException("Can not import files to root tablet");
      }
      
      MetadataTable.updateTabletDataFile(extent, relSizes, SecurityConstants.systemCredentials, tabletServer.getLock());
      
      if (fs.globStatus(new Path(bulkDir + "/processing_proc_*")).length == 0) {
        DataFileValue zero = new DataFileValue(0, 0);
        MetadataTable.replaceDatafiles(extent, relSizes.keySet(), new HashSet<String>(), "junk", zero, SecurityConstants.systemCredentials,
            tabletServer.getClientAddressString(), lastLocation, tabletServer.getLock(), false);
        throw new IOException("Processing file does not exist, aborting bulk import " + extent + " " + bulkDir);
      }
      
      synchronized (Tablet.this) {
        for (Entry<String,DataFileValue> tpath : paths.entrySet()) {
          String path = relPaths.get(tpath.getKey());
          DataFileValue estSize = paths.get(tpath.getKey());
          
          if (datafileSizes.containsKey(path)) {
            log.error("Adding file that is already in set " + path);
          }
          datafileSizes.put(path, estSize);
          tabletResources.importMapFile(tpath.getKey(), tpath.getValue().getSize());
        }
        
        computeNumEntries();
      }
      
      for (String tpath : paths.keySet()) {
        String path = relPaths.get(tpath);
        log.log(TLevel.TABLET_HIST, extent + " import " + path + " " + paths.get(tpath));
      }
    }
    
    void bringMinorCompactionOnline(String tmpDatafile, String newDatafile, DataFileValue dfv, CommitSession commitSession) {
      Path newDatafilePath = new Path(newDatafile);
      String relativePath = tabletDirectory + "/" + newDatafilePath.getName();
      
      if (extent.equals(Constants.ROOT_TABLET_EXTENT)) {
        try {
          if (!ZooLock.isLockHeld(ZooSession.getSession(), tabletServer.getLock().getLockID())) {
            throw new IllegalStateException();
          }
        } catch (Exception e) {
          throw new IllegalStateException("Can not bring major compaction online, lock not held", e);
        }
      }
      
      // rename before putting in metadata table, so files in metadata table should
      // always exist
      do {
        try {
          if (dfv.getNumEntries() == 0) {
            fs.delete(new Path(tmpDatafile), true);
          } else {
            if (fs.exists(newDatafilePath)) {
              log.warn("Target map file already exist " + newDatafile);
              fs.delete(newDatafilePath, true);
            }
            
            if (!fs.rename(new Path(tmpDatafile), newDatafilePath)) {
              throw new IOException("rename fails");
            }
          }
          break;
        } catch (IOException ioe) {
          log.warn("Tablet " + extent + " failed to rename " + relativePath + " after MinC, will retry in 60 secs...", ioe);
          UtilWaitThread.sleep(60 * 1000);
        }
      } while (true);
      
      long t1, t2;
      
      String time = tabletTime.getMetadataValue(commitSession.getMaxCommittedTime());
      
      Set<String> unusedWalLogs = beginClearingUnusedLogs();
      try {
        // the order of writing to !METADATA and walog is important in the face of machine/process failures
        // need to write to !METADATA before writing to walog, when things are done in the reverse order
        // data could be lost
        AuthInfo creds = SecurityConstants.systemCredentials;
        MetadataTable.updateTabletDataFile(extent, relativePath, dfv, time, creds, tabletServer.getClientAddressString(), tabletServer.getLock(),
            unusedWalLogs, lastLocation);
      } finally {
        finishClearingUnusedLogs();
      }
      
      do {
        try {
          // the purpose of making this update use the new commit session, instead of the old one passed in,
          // is because the new one will reference the logs used by current memory...
          
          tabletServer.minorCompactionFinished(tabletMemory.getCommitSession(), newDatafile, commitSession.getWALogSeq() + 2);
          break;
        } catch (IOException e) {
          log.error("Failed to write to write-ahead log " + e.getMessage() + " will retry", e);
          UtilWaitThread.sleep(1 * 1000);
        }
      } while (true);
      
      synchronized (Tablet.this) {
        lastLocation = null;
        
        t1 = System.currentTimeMillis();
        if (datafileSizes.containsKey(relativePath)) {
          log.error("Adding file that is already in set " + relativePath);
        }
        
        if (dfv.getNumEntries() > 0) {
          datafileSizes.put(relativePath, dfv);
        }
        
        boolean addMFFailed;
        do {
          addMFFailed = false;
          try {
            if (dfv.getNumEntries() > 0) {
              tabletResources.addMapFile(newDatafile, dfv.getSize());
            }
          } catch (IOException e) {
            log.warn("Tablet " + extent + " failed to add map file " + relativePath + " after MinC, will retry in 60 secs...", e);
            try {
              Tablet.this.wait(60 * 1000);
            } catch (InterruptedException e1) {
              e1.printStackTrace();
              log.error(e1.getMessage(), e1);
            }
            addMFFailed = true;
          }
        } while (addMFFailed);
        
        dataSourceDeletions.incrementAndGet();
        tabletMemory.finishedMinC();
        
        computeNumEntries();
        t2 = System.currentTimeMillis();
      }
      
      log.log(TLevel.TABLET_HIST, extent + " MinC " + relativePath);
      log.debug(String.format("MinC finish lock %.2f secs %s", (t2 - t1) / 1000.0, getExtent().toString()));
      if (dfv.getSize() > acuTableConf.getMemoryInBytes(Property.TABLE_SPLIT_THRESHOLD)) {
        log.debug(String.format("Minor Compaction wrote out file larger than split threshold.  split threshold = %,d  file size = %,d",
            acuTableConf.getMemoryInBytes(Property.TABLE_SPLIT_THRESHOLD), dfv.getSize()));
      }
      
    }
    
    private Set<String> abs2rel(Set<String> absPaths) {
      Set<String> relativePaths = new TreeSet<String>();
      for (String absPath : absPaths) {
        Path path = new Path(absPath);
        String relPath = "/" + path.getParent().getName() + "/" + path.getName();
        relativePaths.add(relPath);
      }
      
      return relativePaths;
    }
    
    synchronized void bringMajorCompactionOnline(Set<String> oldDatafiles, String tmpDatafile, String newDatafile, DataFileValue dfv) throws IOException {
      long t1, t2;
      
      Set<String> datafilesToDelete = abs2rel(oldDatafiles);
      
      String shortPath = tabletDirectory + "/" + new Path(newDatafile).getName();
      
      if (!extent.equals(Constants.ROOT_TABLET_EXTENT)) {
        
        if (fs.exists(new Path(newDatafile))) {
          log.error("Target map file already exist " + newDatafile, new Exception());
          throw new IllegalStateException("Target map file already exist " + newDatafile);
        }
        
        // rename before putting in metadata table, so files in metadata table should
        // always exist
        if (!fs.rename(new Path(tmpDatafile), new Path(newDatafile)))
          log.warn("Rename of " + tmpDatafile + " to " + newDatafile + " returned false");
        
        if (dfv.getNumEntries() == 0) {
          fs.delete(new Path(newDatafile), true);
        }
      }
      
      TServerInstance lastLocation = null;
      synchronized (Tablet.this) {
        
        t1 = System.currentTimeMillis();
        
        dataSourceDeletions.incrementAndGet();
        
        if (extent.equals(Constants.ROOT_TABLET_EXTENT)) {
          
          waitForScansToFinish(oldDatafiles, true, Long.MAX_VALUE);
          
          try {
            if (!ZooLock.isLockHeld(ZooSession.getSession(), tabletServer.getLock().getLockID())) {
              throw new IllegalStateException();
            }
          } catch (Exception e) {
            throw new IllegalStateException("Can not bring major compaction online, lock not held", e);
          }
          
          // mark files as ready for deletion, but
          // do not delete them until we successfully
          // rename the compacted map file, in case
          // the system goes down
          
          String compactName = new Path(newDatafile).getName();
          
          for (String file : oldDatafiles) {
            Path path = new Path(file);
            fs.rename(path, new Path(location + "/delete+" + compactName + "+" + path.getName()));
          }
          
          if (fs.exists(new Path(newDatafile))) {
            log.error("Target map file already exist " + newDatafile, new Exception());
            throw new IllegalStateException("Target map file already exist " + newDatafile);
          }
          
          if (!fs.rename(new Path(tmpDatafile), new Path(newDatafile)))
            log.warn("Rename of " + tmpDatafile + " to " + newDatafile + " returned false");
          
          // start deleting files, if we do not finish they will be cleaned
          // up later
          for (String file : oldDatafiles) {
            Path path = new Path(file);
            fs.delete(new Path(location + "/delete+" + compactName + "+" + path.getName()), true);
          }
        }
        
        // atomically remove old files and add new file
        for (String oldDatafile : datafilesToDelete) {
          if (!datafileSizes.containsKey(oldDatafile)) {
            log.error("file does not exist in set " + oldDatafile);
          }
          datafileSizes.remove(oldDatafile);
        }
        
        tabletResources.removeMapFiles(oldDatafiles);
        
        if (datafileSizes.containsKey(shortPath)) {
          log.error("Adding file that is already in set " + shortPath);
        }
        
        if (dfv.getNumEntries() > 0) {
          tabletResources.addMapFile(newDatafile, dfv.getSize());
          datafileSizes.put(shortPath, dfv);
        }
        checkConsistency();
        computeNumEntries();
        
        lastLocation = Tablet.this.lastLocation;
        Tablet.this.lastLocation = null;
        
        t2 = System.currentTimeMillis();
      }
      
      if (!extent.equals(Constants.ROOT_TABLET_EXTENT)) {
        Set<String> filesInUseByScans = waitForScansToFinish(oldDatafiles, false, 10000);
        if (filesInUseByScans.size() > 0)
          log.debug("Adding scan refs to metadata " + extent + " " + abs2rel(filesInUseByScans));
        MetadataTable.replaceDatafiles(extent, datafilesToDelete, abs2rel(filesInUseByScans), shortPath, dfv, SecurityConstants.systemCredentials,
            tabletServer.getClientAddressString(), lastLocation, tabletServer.getLock());
        removeFilesAfterScan(filesInUseByScans);
      }
      
      log.debug(String.format("MajC finish lock %.2f secs", (t2 - t1) / 1000.0));
      log.log(TLevel.TABLET_HIST, extent + " MajC " + datafilesToDelete + " --> " + shortPath);
    }
    
    private void checkConsistency() throws IOException {
      synchronized (Tablet.this) {
        
        // do a consistency check
        SortedSet<String> paths = tabletResources.getCopyOfMapFilePaths();
        
        boolean inconsistent = false;
        
        for (String dfp : datafileSizes.keySet()) {
          String longPath = Constants.getTablesDir() + "/" + extent.getTableId() + dfp;
          if (!paths.contains(longPath)) {
            log.error(dfp + " not in tabletResources");
            inconsistent = true;
          }
        }
        
        for (String path : paths) {
          Path p = new Path(path);
          String dfp = "/" + p.getParent().getName() + "/" + p.getName();
          if (!datafileSizes.containsKey(dfp)) {
            log.error(dfp + " not in datafileSizes");
            inconsistent = true;
          }
        }
        
        if (inconsistent)
          log.error("data file sets inconsistent " + paths + " " + datafileSizes.keySet());
        
      }
      
    }
    
    public SortedMap<String,DataFileValue> getDatafileSizes() {
      return unModMap;
    }
    
  }
  
  public Tablet(TabletServer tabletServer, Text location, KeyExtent extent, TabletResourceManager trm, SortedMap<Key,Value> tabletsKeyValues)
      throws IOException {
    this(tabletServer, location, extent, trm, CachedConfiguration.getInstance(), tabletsKeyValues);
    splitCreationTime = 0;
  }
  
  public Tablet(TabletServer tabletServer, Text location, KeyExtent extent, TabletResourceManager trm, SortedMap<String,DataFileValue> datafiles, String time)
      throws IOException {
    this(tabletServer, location, extent, trm, CachedConfiguration.getInstance(), datafiles, time);
    splitCreationTime = System.currentTimeMillis();
  }
  
  private Tablet(TabletServer tabletServer, Text location, KeyExtent extent, TabletResourceManager trm, Configuration conf,
      SortedMap<Key,Value> tabletsKeyValues) throws IOException {
    this(tabletServer, location, extent, trm, conf, FileSystem.get(conf), tabletsKeyValues);
  }
  
  static private final List<LogEntry> EMPTY = Collections.emptyList();
  
  private Tablet(TabletServer tabletServer, Text location, KeyExtent extent, TabletResourceManager trm, Configuration conf,
      SortedMap<String,DataFileValue> datafiles, String time) throws IOException {
    this(tabletServer, location, extent, trm, conf, FileSystem.get(conf), EMPTY, datafiles, time, null, new HashSet<String>());
  }
  
  private static String lookupTime(KeyExtent extent, SortedMap<Key,Value> tabletsKeyValues) {
    SortedMap<Key,Value> entries;
    
    if (extent.equals(Constants.ROOT_TABLET_EXTENT)) {
      return null;
    } else if (extent.getTableId().toString().equals(Constants.METADATA_TABLE_ID)) {
      SortedSet<Column> columns = new TreeSet<Column>();
      columns.add(Constants.METADATA_TIME_COLUMN.toColumn());
      entries = MetadataTable.getRootMetadataDataEntries(extent, columns, SecurityConstants.systemCredentials);
    } else {
      entries = new TreeMap<Key,Value>();
      Text rowName = extent.getMetadataEntry();
      for (Entry<Key,Value> entry : tabletsKeyValues.entrySet()) {
        if (entry.getKey().compareRow(rowName) == 0 && Constants.METADATA_TIME_COLUMN.hasColumns(entry.getKey())) {
          entries.put(new Key(entry.getKey()), new Value(entry.getValue()));
        }
      }
    }
    
    // log.debug("extent : "+extent+"   entries : "+entries);
    
    if (entries.size() == 1)
      return entries.values().iterator().next().toString();
    return null;
  }
  
  private static SortedMap<String,DataFileValue> lookupDatafiles(Text locText, FileSystem fs, KeyExtent extent, SortedMap<Key,Value> tabletsKeyValues)
      throws IOException {
    Path location = new Path(Constants.getTablesDir() + "/" + extent.getTableId().toString() + locText.toString());
    
    TreeMap<String,DataFileValue> datafiles = new TreeMap<String,DataFileValue>();
    
    if (extent.equals(Constants.ROOT_TABLET_EXTENT)) { // the meta0 tablet
      // cleanUpFiles() has special handling for delete. files
      FileStatus[] files = fs.listStatus(location);
      Path[] paths = new Path[files.length];
      for (int i = 0; i < files.length; i++) {
        paths[i] = files[i].getPath();
      }
      log.debug("fs " + fs + " files: " + Arrays.toString(paths) + " location: " + location);
      Collection<String> goodPaths = cleanUpFiles(fs, files, location, true);
      for (String path : goodPaths) {
        String filename = new Path(path).getName();
        DataFileValue dfv = new DataFileValue(0, 0);
        datafiles.put(locText.toString() + "/" + filename, dfv);
      }
    } else {
      
      SortedMap<Key,Value> datafilesMetadata;
      
      if (extent.getTableId().toString().equals(Constants.METADATA_TABLE_ID)) {
        datafilesMetadata = MetadataTable.getRootMetadataDataFileEntries(extent, SecurityConstants.systemCredentials);
      } else {
        
        Text rowName = extent.getMetadataEntry();
        
        if (tabletsKeyValues != null && tabletsKeyValues.size() > 0) {
          datafilesMetadata = new TreeMap<Key,Value>();
          for (Entry<Key,Value> entry : tabletsKeyValues.entrySet()) {
            if (entry.getKey().compareRow(rowName) == 0 && entry.getKey().compareColumnFamily(Constants.METADATA_DATAFILE_COLUMN_FAMILY) == 0) {
              datafilesMetadata.put(new Key(entry.getKey()), new Value(entry.getValue()));
            }
          }
        } else {
          
          ScannerImpl mdScanner = new ScannerImpl(HdfsZooInstance.getInstance(), SecurityConstants.systemCredentials, Constants.METADATA_TABLE_ID,
              Constants.NO_AUTHS);
          
          // Commented out because when no data file is present, each tablet will scan through metadata table and return nothing
          // reduced batch size to improve performance
          // changed here after endKeys were implemented from 10 to 1000
          mdScanner.setBatchSize(1000);
          
          // leave these in, again, now using endKey for safety
          mdScanner.fetchColumnFamily(Constants.METADATA_DATAFILE_COLUMN_FAMILY);
          
          mdScanner.setRange(new Range(rowName));
          
          datafilesMetadata = new TreeMap<Key,Value>();
          
          for (Entry<Key,Value> entry : mdScanner) {
            
            if (entry.getKey().compareRow(rowName) != 0) {
              break;
            }
            
            datafilesMetadata.put(new Key(entry.getKey()), new Value(entry.getValue()));
          }
        }
      }
      
      Iterator<Entry<Key,Value>> dfmdIter = datafilesMetadata.entrySet().iterator();
      
      while (dfmdIter.hasNext()) {
        Entry<Key,Value> entry = dfmdIter.next();
        
        datafiles.put(entry.getKey().getColumnQualifier().toString(), new DataFileValue(entry.getValue().get()));
      }
    }
    
    return datafiles;
  }
  
  private static Set<RemoteLogger> getCurrentLoggers(List<LogEntry> entries) {
    Set<RemoteLogger> result = new HashSet<RemoteLogger>();
    for (LogEntry logEntry : entries) {
      for (String log : logEntry.logSet) {
        String[] parts = log.split("/", 2);
        result.add(new RemoteLogger(parts[0], parts[1], null));
      }
    }
    return result;
  }
  
  private static List<LogEntry> lookupLogEntries(KeyExtent ke, SortedMap<Key,Value> tabletsKeyValues) {
    List<LogEntry> logEntries = new ArrayList<LogEntry>();
    
    if (ke.getTableId().toString().equals(Constants.METADATA_TABLE_ID)) {
      try {
        logEntries = MetadataTable.getLogEntries(SecurityConstants.systemCredentials, ke);
      } catch (Exception ex) {
        throw new RuntimeException("Unable to read tablet log entries", ex);
      }
    } else {
      log.debug("Looking at metadata " + tabletsKeyValues);
      Text row = ke.getMetadataEntry();
      for (Entry<Key,Value> entry : tabletsKeyValues.entrySet()) {
        Key key = entry.getKey();
        if (key.getRow().equals(row)) {
          if (key.getColumnFamily().equals(Constants.METADATA_LOG_COLUMN_FAMILY)) {
            logEntries.add(MetadataTable.entryFromKeyValue(key, entry.getValue()));
          }
        }
      }
    }
    
    log.debug("got " + logEntries + " for logs for " + ke);
    return logEntries;
  }
  
  private static Set<String> lookupScanFiles(KeyExtent extent, SortedMap<Key,Value> tabletsKeyValues) {
    HashSet<String> scanFiles = new HashSet<String>();
    
    Text row = extent.getMetadataEntry();
    for (Entry<Key,Value> entry : tabletsKeyValues.entrySet()) {
      Key key = entry.getKey();
      if (key.getRow().equals(row) && key.getColumnFamily().equals(Constants.METADATA_SCANFILE_COLUMN_FAMILY)) {
        scanFiles.add(key.getColumnQualifier().toString());
      }
    }
    
    return scanFiles;
  }
  
  private Tablet(TabletServer tabletServer, Text location, KeyExtent extent, TabletResourceManager trm, Configuration conf, FileSystem fs,
      SortedMap<Key,Value> tabletsKeyValues) throws IOException {
    this(tabletServer, location, extent, trm, conf, fs, lookupLogEntries(extent, tabletsKeyValues), lookupDatafiles(location, fs, extent, tabletsKeyValues),
        lookupTime(extent, tabletsKeyValues), lookupLastServer(extent, tabletsKeyValues), lookupScanFiles(extent, tabletsKeyValues));
  }
  
  private static TServerInstance lookupLastServer(KeyExtent extent, SortedMap<Key,Value> tabletsKeyValues) {
    for (Entry<Key,Value> entry : tabletsKeyValues.entrySet()) {
      if (entry.getKey().getColumnFamily().compareTo(Constants.METADATA_LAST_LOCATION_COLUMN_FAMILY) == 0) {
        return new TServerInstance(entry.getValue(), entry.getKey().getColumnQualifier());
      }
    }
    return null;
  }
  
  /**
   * yet another constructor - this one allows us to avoid costly lookups into the Metadata table if we already know the files we need - as at split time
   */
  private Tablet(final TabletServer tabletServer, final Text location, final KeyExtent extent, final TabletResourceManager trm, final Configuration conf,
      final FileSystem fs, final List<LogEntry> logEntries, final SortedMap<String,DataFileValue> datafiles, String time, final TServerInstance lastLocation,
      Set<String> scanFiles) throws IOException {
    this.location = new Path(Constants.getTablesDir() + "/" + extent.getTableId().toString() + location.toString());
    this.lastLocation = lastLocation;
    this.tabletDirectory = location.toString();
    this.conf = conf;
    this.acuTableConf = AccumuloConfiguration.getTableConfiguration(HdfsZooInstance.getInstance().getInstanceID(), extent.getTableId().toString());
    
    this.fs = fs;
    this.extent = extent;
    this.tabletResources = trm;
    
    if (extent.equals(Constants.ROOT_TABLET_EXTENT)) {
      
      long rtime = Long.MIN_VALUE;
      for (String path : datafiles.keySet()) {
        String filename = new Path(path).getName();
        
        FileSKVIterator reader = FileOperations.getInstance().openReader(this.location + "/" + filename, true, fs, fs.getConf(),
            AccumuloConfiguration.getTableConfiguration(HdfsZooInstance.getInstance().getInstanceID(), Constants.METADATA_TABLE_ID));
        long maxTime = -1;
        try {
          
          while (reader.hasTop()) {
            maxTime = Math.max(maxTime, reader.getTopKey().getTimestamp());
            reader.next();
          }
          
        } finally {
          reader.close();
        }
        
        if (maxTime > rtime) {
          time = TabletTime.LOGICAL_TIME_ID + "" + maxTime;
          rtime = maxTime;
        }
      }
    }
    
    this.tabletServer = tabletServer;
    this.logId = tabletServer.createLogId(extent);
    
    this.timer = new TabletStatsKeeper();
    
    setupDefaultSecurityLabels(extent);
    
    tabletMemory = new TabletMemory();
    tabletTime = TabletTime.getInstance(time);
    
    constraintChecker.set(ConstraintLoader.load(extent.getTableId().toString()));
    
    acuTableConf.addObserver(configObserver = new ConfigurationObserver() {
      
      private void reloadConstraints() {
        
        ConstraintChecker cc = null;
        
        try {
          log.debug("Reloading constraints");
          cc = ConstraintLoader.load(extent.getTableId().toString());
        } catch (IOException e) {
          log.error("Failed to reload constraints for " + extent, e);
          cc = new ConstraintChecker();
          cc.addConstraint(new UnsatisfiableConstraint((short) -1, "Failed to reload constraints, not accepting mutations."));
        }
        
        constraintChecker.set(cc);
      }
      
      public void propertiesChanged() {
        reloadConstraints();
        
        try {
          setupDefaultSecurityLabels(extent);
        } catch (Exception e) {
          log.error("Failed to reload default security labels for extent: " + extent.toString());
        }
      }
      
      public void propertyChanged(String prop) {
        if (prop.startsWith(Property.TABLE_CONSTRAINT_PREFIX.getKey()))
          reloadConstraints();
        else if (prop.equals(Property.TABLE_DEFAULT_SCANTIME_VISIBILITY.getKey())) {
          try {
            log.info("Default security labels changed for extent: " + extent.toString());
            setupDefaultSecurityLabels(extent);
          } catch (Exception e) {
            log.error("Failed to reload default security labels for extent: " + extent.toString());
          }
        }
        
      }
      
      public void sessionExpired() {
        log.debug("Session expired, no longer updating per table props...");
      }
      
    });
    // Force a load of any per-table properties
    configObserver.propertiesChanged();
    
    tabletResources.setTablet(this, acuTableConf);
    
    if (!logEntries.isEmpty()) {
      log.info("Starting Write-Ahead Log recovery for " + this.extent);
      final long[] count = new long[2];
      final CommitSession commitSession = tabletMemory.getCommitSession();
      count[1] = Long.MIN_VALUE;
      try {
        Set<String> absPaths = new HashSet<String>();
        for (String relPath : datafiles.keySet())
          absPaths.add(rel2abs(relPath, extent));
        
        tabletServer.recover(this, logEntries, absPaths, new MutationReceiver() {
          public void receive(Mutation m) {
            // LogReader.printMutation(m);
            Collection<ColumnUpdate> muts = m.getUpdates();
            for (ColumnUpdate columnUpdate : muts) {
              if (!columnUpdate.hasTimestamp()) {
                // if it is not a user set timestamp, it must have been set
                // by the system
                count[1] = Math.max(count[1], columnUpdate.getTimestamp());
              }
            }
            tabletMemory.mutate(commitSession, Collections.singletonList(m));
            count[0]++;
          }
        });
        
        if (count[1] != Long.MIN_VALUE) {
          tabletTime.useMaxTimeFromWALog(count[1]);
          commitSession.updateMaxCommittedTime(count[1]);
        } else {
          commitSession.updateMaxCommittedTime(tabletTime.getTime());
        }
        
        tabletMemory.updateMemoryUsageStats();
        
        if (count[0] == 0) {
          MetadataTable.removeUnusedWALEntries(extent, logEntries, tabletServer.getLock());
          logEntries.clear();
        }
        
      } catch (Throwable t) {
        if (acuTableConf.getBoolean(Property.TABLE_FAILURES_IGNORE)) {
          log.warn("Error recovering from log files: ", t);
        } else {
          throw new RuntimeException(t);
        }
      }
      currentLogs = getCurrentLoggers(logEntries);
      log.info("Write-Ahead Log recovery complete for " + this.extent + " (" + count[0] + " mutations applied, " + tabletMemory.getNumEntries()
          + " entries created)");
    }
    
    // do this last after tablet is completely setup because it
    // could cause major compaction to start
    datafileManager = new DatafileManager(datafiles);
    
    computeNumEntries();
    
    datafileManager.removeFilesAfterScan(scanFiles);
    
    log.log(TLevel.TABLET_HIST, extent + " opened ");
  }
  
  private void setupDefaultSecurityLabels(KeyExtent extent) {
    if (extent.getTableId().toString().equals(Constants.METADATA_TABLE_ID)) {
      defaultSecurityLabel = new byte[0];
    } else {
      try {
        ColumnVisibility cv = new ColumnVisibility(acuTableConf.get(Property.TABLE_DEFAULT_SCANTIME_VISIBILITY));
        this.defaultSecurityLabel = cv.getExpression();
      } catch (Exception e) {
        log.error(e, e);
        this.defaultSecurityLabel = new byte[0];
      }
    }
  }
  
  private static Collection<String> cleanUpFiles(FileSystem fs, FileStatus[] files, Path location, boolean deleteTmp) throws IOException {
    /*
     * called in constructor and before major compactions
     */
    Collection<String> goodFiles = new ArrayList<String>(files.length);
    
    for (FileStatus file : files) {
      
      String path = file.getPath().toString();
      String filename = file.getPath().getName();
      
      // check for incomplete major compaction, this should only occur
      // for root tablet
      if (filename.startsWith("delete+")) {
        String expectedCompactedFile = location.toString() + "/" + filename.split("\\+")[1];
        if (fs.exists(new Path(expectedCompactedFile))) {
          // compaction finished, but did not finish deleting compacted files.. so delete it
          if (!fs.delete(file.getPath(), true))
            log.warn("Delete of file: " + file.getPath().toString() + " return false");
          continue;
        }
        // compaction did not finish, so put files back
        
        // reset path and filename for rest of loop
        filename = filename.split("\\+", 3)[2];
        path = location + "/" + filename;
        
        if (!fs.rename(file.getPath(), new Path(path)))
          log.warn("Rename of " + file.getPath().toString() + " to " + path + " returned false");
      }
      
      if (filename.endsWith("_tmp")) {
        if (deleteTmp) {
          log.warn("cleaning up old tmp file: " + path);
          if (!fs.delete(file.getPath(), true))
            log.warn("Delete of tmp file: " + file.getPath().toString() + " return false");
          
        }
        continue;
      }
      
      if (!filename.startsWith(MyMapFile.EXTENSION + "_") && !FileOperations.getValidExtensions().contains(filename.split("\\.")[1])) {
        log.error("unknown file in tablet" + path);
        continue;
      }
      
      goodFiles.add(path);
    }
    
    return goodFiles;
  }
  
  public static class KVEntry extends KeyValue {
    
    public KVEntry(Key k, Value v) {
      super(new Key(k), Arrays.copyOf(v.get(), v.get().length));
    }
    
    public String toString() {
      return key.toString() + "=" + getValue();
    }
    
    int numBytes() {
      return key.getSize() + getValue().get().length;
    }
    
    int estimateMemoryUsed() {
      return key.getSize() + getValue().get().length + (9 * 32); // overhead is 32 per object
    }
  }
  
  private LookupResult lookup(SortedKeyValueIterator<Key,Value> mmfi, List<Range> ranges, HashSet<Column> columnSet, ArrayList<KVEntry> results,
      long maxResultsSize) throws IOException {
    
    LookupResult lookupResult = new LookupResult();
    
    boolean exceededMemoryUsage = false;
    boolean tabletClosed = false;
    
    Set<ByteSequence> cfset = null;
    if (columnSet.size() > 0)
      cfset = LocalityGroupUtil.families(columnSet);
    
    for (Range range : ranges) {
      
      if (exceededMemoryUsage || tabletClosed) {
        lookupResult.unfinishedRanges.add(range);
        continue;
      }
      
      int entriesAdded = 0;
      
      try {
        if (cfset != null)
          mmfi.seek(range, cfset, true);
        else
          mmfi.seek(range, LocalityGroupUtil.EMPTY_CF_SET, false);
        
        while (mmfi.hasTop()) {
          Key key = mmfi.getTopKey();
          
          KVEntry kve = new KVEntry(key, mmfi.getTopValue());
          results.add(kve);
          entriesAdded++;
          lookupResult.bytesAdded += kve.estimateMemoryUsed();
          
          exceededMemoryUsage = lookupResult.bytesAdded > maxResultsSize;
          
          if (exceededMemoryUsage) {
            addUnfinishedRange(lookupResult, range, key, false);
            break;
          }
          
          mmfi.next();
        }
        
      } catch (TooManyFilesException tmfe) {
        // treat this as a closed tablet, and let the client retry
        log.warn("Tablet " + getExtent() + " has too many files, batch lookup can not run");
        handleTabletClosedDuringScan(results, lookupResult, exceededMemoryUsage, range, entriesAdded);
        tabletClosed = true;
      } catch (IOException ioe) {
        if (shutdownInProgress()) {
          // assume HDFS shutdown hook caused this exception
          log.debug("IOException while shutdown in progress ", ioe);
          handleTabletClosedDuringScan(results, lookupResult, exceededMemoryUsage, range, entriesAdded);
          tabletClosed = true;
        } else {
          throw ioe;
        }
      } catch (IterationInterruptedException iie) {
        if (isClosed()) {
          handleTabletClosedDuringScan(results, lookupResult, exceededMemoryUsage, range, entriesAdded);
          tabletClosed = true;
        } else {
          throw iie;
        }
      } catch (TabletClosedException tce) {
        handleTabletClosedDuringScan(results, lookupResult, exceededMemoryUsage, range, entriesAdded);
        tabletClosed = true;
      }
      
    }
    
    return lookupResult;
  }
  
  private void handleTabletClosedDuringScan(ArrayList<KVEntry> results, LookupResult lookupResult, boolean exceededMemoryUsage, Range range, int entriesAdded) {
    if (exceededMemoryUsage)
      throw new IllegalStateException("tablet should not exceed memory usage or close, not both");
    
    if (entriesAdded > 0)
      addUnfinishedRange(lookupResult, range, results.get(results.size() - 1).key, false);
    else
      lookupResult.unfinishedRanges.add(range);
    
    lookupResult.closed = true;
  }
  
  private void addUnfinishedRange(LookupResult lookupResult, Range range, Key key, boolean inclusiveStartKey) {
    if (range.getEndKey() == null || key.compareTo(range.getEndKey()) < 0) {
      Range nlur = new Range(new Key(key), inclusiveStartKey, range.getEndKey(), range.isEndKeyInclusive());
      lookupResult.unfinishedRanges.add(nlur);
    }
  }
  
  public static interface KVReceiver {
    void receive(List<KVEntry> matches) throws IOException;
  }
  
  class LookupResult {
    List<Range> unfinishedRanges = new ArrayList<Range>();
    long bytesAdded = 0;
    boolean closed = false;
  }
  
  public LookupResult lookup(List<Range> ranges, HashSet<Column> columns, Authorizations authorizations, ArrayList<KVEntry> results, long maxResultSize,
      List<IterInfo> ssiList, Map<String,Map<String,String>> ssio, AtomicBoolean interruptFlag) throws IOException {
    
    if (ranges.size() == 0) {
      return new LookupResult();
    }
    
    ranges = Range.mergeOverlapping(ranges);
    Collections.sort(ranges);
    
    Range tabletRange = extent.toDataRange();
    for (Range range : ranges) {
      // do a test to see if this range falls within the tablet, if it does not
      // then clip will throw an exception
      tabletRange.clip(range);
    }
    
    ScanDataSource dataSource = new ScanDataSource(authorizations, this.defaultSecurityLabel, columns, ssiList, ssio, interruptFlag);
    
    try {
      SortedKeyValueIterator<Key,Value> iter = new SourceSwitchingIterator(dataSource);
      return lookup(iter, ranges, columns, results, maxResultSize);
    } catch (IOException ioe) {
      dataSource.close(true);
      throw ioe;
    } finally {
      // code in finally block because always want
      // to return mapfiles, even when exception is thrown
      dataSource.close(false);
      
      synchronized (this) {
        queryCount += results.size();
      }
    }
  }
  
  private Batch nextBatch(SortedKeyValueIterator<Key,Value> iter, Range range, int num, HashSet<Column> columns) throws IOException {
    
    // log.info("In nextBatch..");
    
    List<KVEntry> results = new ArrayList<KVEntry>();
    Key key = null;
    
    Value value;
    long resultSize = 0L;
    long resultBytes = 0L;
    
    long maxResultsSize = acuTableConf.getMemoryInBytes(Property.TABLE_SCAN_MAXMEM);
    
    if (columns.size() == 0) {
      iter.seek(range, LocalityGroupUtil.EMPTY_CF_SET, false);
    } else {
      iter.seek(range, LocalityGroupUtil.families(columns), true);
    }
    
    Key continueKey = null;
    boolean skipContinueKey = false;
    
    boolean endOfTabletReached = false;
    while (iter.hasTop()) {
      
      value = (Value) iter.getTopValue();
      key = iter.getTopKey();
      
      KVEntry kvEntry = new KVEntry(key, value); // copies key and value
      results.add(kvEntry);
      resultSize += kvEntry.estimateMemoryUsed();
      resultBytes += kvEntry.numBytes();
      
      if (resultSize >= maxResultsSize || results.size() >= num) {
        continueKey = new Key(key);
        skipContinueKey = true;
        break;
      }
      
      iter.next();
    }
    
    if (iter.hasTop() == false) {
      endOfTabletReached = true;
    }
    
    Batch retBatch = new Batch();
    retBatch.numBytes = resultBytes;
    
    if (!endOfTabletReached) {
      retBatch.continueKey = continueKey;
      retBatch.skipContinueKey = skipContinueKey;
    } else {
      retBatch.continueKey = null;
    }
    
    if (endOfTabletReached && results.size() == 0)
      retBatch.results = null;
    else
      retBatch.results = results;
    
    return retBatch;
  }
  
  /**
   * Determine if a JVM shutdown is in progress.
   * 
   */
  private boolean shutdownInProgress() {
    try {
      Runtime.getRuntime().removeShutdownHook(new Thread(new Runnable() {
        @Override
        public void run() {}
      }));
    } catch (IllegalStateException ise) {
      return true;
    }
    
    return false;
  }
  
  private class Batch {
    public boolean skipContinueKey;
    public List<KVEntry> results;
    public Key continueKey;
    public long numBytes;
  }
  
  Scanner createScanner(Range range, int num, HashSet<Column> columns, Authorizations authorizations, List<IterInfo> ssiList,
      Map<String,Map<String,String>> ssio, boolean isolated, AtomicBoolean interruptFlag) {
    // do a test to see if this range falls within the tablet, if it does not
    // then clip will throw an exception
    extent.toDataRange().clip(range);
    
    ScanOptions opts = new ScanOptions(num, authorizations, this.defaultSecurityLabel, columns, ssiList, ssio, interruptFlag, isolated);
    return new Scanner(range, opts);
  }
  
  class ScanBatch {
    boolean more;
    List<KVEntry> results;
    
    ScanBatch(List<KVEntry> results, boolean more) {
      this.results = results;
      this.more = more;
    }
  }
  
  class Scanner {
    
    private ScanOptions options;
    private Range range;
    private SortedKeyValueIterator<Key,Value> isolatedIter;
    private ScanDataSource isolatedDataSource;
    private boolean sawException = false;
    private boolean scanClosed = false;
    
    Scanner(Range range, ScanOptions options) {
      this.range = range;
      this.options = options;
    }
    
    synchronized ScanBatch read() throws IOException, TabletClosedException {
      
      if (sawException)
        throw new IllegalStateException("Tried to use scanner after exception occurred.");
      
      if (scanClosed)
        throw new IllegalStateException("Tried to use scanner after it was closed.");
      
      Batch results = null;
      
      ScanDataSource dataSource;
      
      if (options.isolated) {
        if (isolatedDataSource == null)
          isolatedDataSource = new ScanDataSource(options);
        dataSource = isolatedDataSource;
      } else {
        dataSource = new ScanDataSource(options);
      }
      
      try {
        
        SortedKeyValueIterator<Key,Value> iter;
        
        if (options.isolated) {
          if (isolatedIter == null)
            isolatedIter = new SourceSwitchingIterator(dataSource, true);
          else
            isolatedDataSource.fileManager.reattach();
          iter = isolatedIter;
        } else {
          iter = new SourceSwitchingIterator(dataSource, false);
        }
        
        results = nextBatch(iter, range, options.num, options.columnSet);
        
        if (results.results == null) {
          range = null;
          return new ScanBatch(new ArrayList<Tablet.KVEntry>(), false);
        } else if (results.continueKey == null) {
          return new ScanBatch(results.results, false);
        } else {
          range = new Range(results.continueKey, !results.skipContinueKey, range.getEndKey(), range.isEndKeyInclusive());
          return new ScanBatch(results.results, true);
        }
        
      } catch (IterationInterruptedException iie) {
        sawException = true;
        if (isClosed())
          throw new TabletClosedException(iie);
        else
          throw iie;
      } catch (IOException ioe) {
        if (shutdownInProgress()) {
          log.debug("IOException while shutdown in progress ", ioe);
          throw new TabletClosedException(ioe); // assume IOException was caused by execution of HDFS shutdown hook
        }
        
        sawException = true;
        dataSource.close(true);
        throw ioe;
      } catch (RuntimeException re) {
        sawException = true;
        throw re;
      } finally {
        // code in finally block because always want
        // to return mapfiles, even when exception is thrown
        if (!options.isolated)
          dataSource.close(false);
        else if (dataSource.fileManager != null)
          dataSource.fileManager.detach();
        
        synchronized (Tablet.this) {
          if (results != null && results.results != null) {
            long more = results.results.size();
            queryCount += more;
            queryBytes += results.numBytes;
          }
        }
      }
    }
    
    // close and read are synchronized because can not call close on the data source while it is in use
    // this cloud lead to the case where file iterators that are in use by a thread are returned
    // to the pool... this would be bad
    void close() {
      options.interruptFlag.set(true);
      synchronized (this) {
        scanClosed = true;
        if (isolatedDataSource != null)
          isolatedDataSource.close(false);
      }
    }
  }
  
  static class ScanOptions {
    
    // scan options
    Authorizations authorizations;
    byte[] defaultLabels;
    HashSet<Column> columnSet;
    List<IterInfo> ssiList;
    Map<String,Map<String,String>> ssio;
    AtomicBoolean interruptFlag;
    int num;
    boolean isolated;
    
    ScanOptions(int num, Authorizations authorizations, byte[] defaultLabels, HashSet<Column> columnSet, List<IterInfo> ssiList,
        Map<String,Map<String,String>> ssio, AtomicBoolean interruptFlag, boolean isolated) {
      this.num = num;
      this.authorizations = authorizations;
      this.defaultLabels = defaultLabels;
      this.columnSet = columnSet;
      this.ssiList = ssiList;
      this.ssio = ssio;
      this.interruptFlag = interruptFlag;
      this.isolated = isolated;
    }
    
  }
  
  class ScanDataSource implements DataSource {
    
    // data source state
    private ScanFileManager fileManager;
    private SortedKeyValueIterator<Key,Value> iter;
    private long expectedDeletionCount;
    private List<MemoryIterator> memIters = null;
    private long fileReservationId;
    private AtomicBoolean interruptFlag;
    
    ScanOptions options;
    
    ScanDataSource(Authorizations authorizations, byte[] defaultLabels, HashSet<Column> columnSet, List<IterInfo> ssiList, Map<String,Map<String,String>> ssio,
        AtomicBoolean interruptFlag) {
      expectedDeletionCount = dataSourceDeletions.get();
      this.options = new ScanOptions(-1, authorizations, defaultLabels, columnSet, ssiList, ssio, interruptFlag, false);
      this.interruptFlag = interruptFlag;
    }
    
    ScanDataSource(ScanOptions options) {
      expectedDeletionCount = dataSourceDeletions.get();
      this.options = options;
      this.interruptFlag = options.interruptFlag;
    }
    
    @Override
    public DataSource getNewDataSource() {
      if (!isCurrent()) {
        // log.debug("Switching data sources during a scan");
        if (memIters != null) {
          tabletMemory.returnIterators(memIters);
          memIters = null;
          datafileManager.returnFilesForScan(fileReservationId);
          fileReservationId = -1;
        }
        
        if (fileManager != null)
          fileManager.releaseOpenFiles(false);
        
        expectedDeletionCount = dataSourceDeletions.get();
        iter = null;
        
        return this;
      } else
        return this;
    }
    
    @Override
    public boolean isCurrent() {
      return expectedDeletionCount == dataSourceDeletions.get();
    }
    
    @Override
    public SortedKeyValueIterator<Key,Value> iterator() throws IOException {
      if (iter == null)
        iter = createIterator();
      return iter;
    }
    
    private SortedKeyValueIterator<Key,Value> createIterator() throws IOException {
      
      Set<String> files;
      
      synchronized (Tablet.this) {
        
        if (memIters != null)
          throw new IllegalStateException("Tried to create new scan iterator w/o releasing memory");
        
        if (Tablet.this.closed)
          throw new TabletClosedException();
        
        if (interruptFlag.get())
          throw new IterationInterruptedException(extent.toString() + " " + interruptFlag.hashCode());
        
        // only acquire the file manager when we know the tablet is open
        if (fileManager == null) {
          fileManager = tabletResources.newScanFileManager();
          activeScans.add(this);
        }
        
        if (fileManager.getNumOpenFiles() != 0)
          throw new IllegalStateException("Tried to create new scan iterator w/o releasing files");
        
        // set this before trying to get iterators in case
        // getIterators() throws an exception
        expectedDeletionCount = dataSourceDeletions.get();
        
        memIters = tabletMemory.getIterators();
        Pair<Long,Set<String>> reservation = datafileManager.reserveFilesForScan();
        fileReservationId = reservation.getFirst();
        files = reservation.getSecond();
      }
      
      Collection<InterruptibleIterator> mapfiles = fileManager.openFiles(files, options.isolated);
      
      List<SortedKeyValueIterator<Key,Value>> iters = new ArrayList<SortedKeyValueIterator<Key,Value>>(mapfiles.size() + memIters.size());
      
      iters.addAll(mapfiles);
      iters.addAll(memIters);
      
      for (SortedKeyValueIterator<Key,Value> skvi : iters)
        ((InterruptibleIterator) skvi).setInterruptFlag(interruptFlag);
      
      MultiIterator multiIter = new MultiIterator(iters, extent);
      
      IteratorEnvironment iterEnv = new TabletIteratorEnvironment(IteratorScope.scan, acuTableConf, fileManager);
      
      DeletingIterator delIter = new DeletingIterator(multiIter, false);
      
      SystemScanIterator systemIter = new SystemScanIterator(delIter, options.authorizations, options.defaultLabels, options.columnSet);
      
      return IteratorUtil.loadIterators(IteratorScope.scan, systemIter, extent, acuTableConf, options.ssiList, options.ssio, iterEnv);
    }
    
    private void close(boolean sawErrors) {
      
      if (memIters != null) {
        tabletMemory.returnIterators(memIters);
        memIters = null;
        datafileManager.returnFilesForScan(fileReservationId);
        fileReservationId = -1;
      }
      
      synchronized (Tablet.this) {
        activeScans.remove(this);
        if (activeScans.size() == 0)
          Tablet.this.notifyAll();
      }
      
      if (fileManager != null) {
        fileManager.releaseOpenFiles(sawErrors);
        fileManager = null;
      }
      
    }
    
    public void interrupt() {
      interruptFlag.set(true);
    }
    
    @Override
    public DataSource getDeepCopyDataSource(IteratorEnvironment env) {
      throw new UnsupportedOperationException();
    }
    
  }
  
  private DataFileValue minorCompact(Configuration conf, FileSystem fs, InMemoryMap memTable, String tmpDatafile, String newDatafile, boolean hasQueueTime,
      long queued, CommitSession commitSession) {
    boolean failed = false;
    long start = System.currentTimeMillis();
    timer.incrementStatusMinor();
    
    long count = 0;
    
    try {
      Span span = Trace.start("write");
      count = memTable.getNumEntries();
      DataFileValue stats = memTable.minorCompact(conf, fs, tmpDatafile, extent);
      span.stop();
      span = Trace.start("bringOnline");
      datafileManager.bringMinorCompactionOnline(tmpDatafile, newDatafile, stats, commitSession);
      span.stop();
      return stats;
    } catch (RuntimeException E) {
      failed = true;
      throw E;
    } catch (Error E) {
      // Weird errors like "OutOfMemoryError" when trying to create the thread for the compaction
      failed = true;
      throw new RuntimeException(E);
    } finally {
      try {
        tabletMemory.finalizeMinC();
      } catch (Throwable t) {
        log.error("Failed to free tablet memory", t);
      }
      
      if (!failed) {
        lastMinorCompactionFinishTime = System.currentTimeMillis();
      }
      if (tabletServer.mincMetrics.isEnabled())
        tabletServer.mincMetrics.add(TabletServerMinCMetrics.minc, (lastMinorCompactionFinishTime - start));
      if (hasQueueTime) {
        timer.updateTime(Operation.MINOR, queued, start, count, failed);
        if (tabletServer.mincMetrics.isEnabled())
          tabletServer.mincMetrics.add(TabletServerMinCMetrics.queue, (start - queued));
      } else
        timer.updateTime(Operation.MINOR, start, count, failed);
    }
  }
  
  private class MinorCompactionTask implements Runnable {
    
    private long queued;
    private String newMapfileLocation;
    private CommitSession commitSession;
    private DataFileValue stats;
    
    MinorCompactionTask(String newMapfileLocation, CommitSession commitSession) {
      this.newMapfileLocation = newMapfileLocation;
      queued = System.currentTimeMillis();
      minorCompactionWaitingToStart = true;
      this.commitSession = commitSession;
    }
    
    public void run() {
      minorCompactionWaitingToStart = false;
      minorCompactionInProgress = true;
      Span minorCompaction = Trace.on("minorCompaction");
      try {
        Span span = Trace.start("waitForCommits");
        synchronized (Tablet.this) {
          commitSession.waitForCommitsToFinish();
        }
        span.stop();
        span = Trace.start("start");
        while (true) {
          try {
            tabletServer.minorCompactionStarted(commitSession, commitSession.getWALogSeq() + 1, newMapfileLocation);
            break;
          } catch (IOException e) {
            log.warn("Failed to write to write ahead log " + e.getMessage(), e);
          }
        }
        span.stop();
        span = Trace.start("compact");
        this.stats = minorCompact(conf, fs, tabletMemory.getMinCMemTable(), newMapfileLocation + "_tmp", newMapfileLocation, true, queued, commitSession);
        span.stop();
      } catch (Throwable t) {
        log.error("Unknown error during minor compaction for extent: " + getExtent(), t);
        throw new RuntimeException(t);
      } finally {
        minorCompactionInProgress = false;
        minorCompaction.data("extent", extent.toString());
        minorCompaction.data("numEntries", Long.toString(this.stats.getNumEntries()));
        minorCompaction.data("size", Long.toString(this.stats.getSize()));
        minorCompaction.stop();
      }
    }
  }
  
  private synchronized MinorCompactionTask prepareForMinC() {
    String newMapfileLocation;
    
    try {
      newMapfileLocation = getNextMapFilename();
    } catch (IOException e1) {
      log.error("Failed to get filename for minor compaction " + e1.getMessage(), e1);
      throw new RuntimeException(e1);
    }
    
    CommitSession oldCommitSession = tabletMemory.prepareForMinC();
    otherLogs = currentLogs;
    currentLogs = new HashSet<RemoteLogger>();
    
    return new MinorCompactionTask(newMapfileLocation, oldCommitSession);
    
  }
  
  synchronized boolean initiateMinorCompaction() {
    
    MinorCompactionTask mct;
    long t1, t2;
    
    StringBuilder logMessage = null;
    
    try {
      synchronized (this) {
        t1 = System.currentTimeMillis();
        
        if (closing || closed || majorCompactionWaitingToStart || tabletMemory.memoryReservedForMinC() || tabletMemory.getMemTable().getNumEntries() == 0) {
          
          logMessage = new StringBuilder();
          
          logMessage.append(extent.toString());
          logMessage.append(" closing " + closing);
          logMessage.append(" closed " + closed);
          logMessage.append(" majorCompactionWaitingToStart " + majorCompactionWaitingToStart);
          if (tabletMemory != null)
            logMessage.append(" tabletMemory.memoryReservedForMinC() " + tabletMemory.memoryReservedForMinC());
          if (tabletMemory != null && tabletMemory.getMemTable() != null)
            logMessage.append(" tabletMemory.getMemTable().getNumEntries() " + tabletMemory.getMemTable().getNumEntries());
          
          return false;
        }
        // We're still recovering log entries
        if (datafileManager == null) {
          logMessage = new StringBuilder();
          logMessage.append(extent.toString());
          logMessage.append(" datafileManager " + datafileManager);
          return false;
        }
        
        mct = prepareForMinC();
        t2 = System.currentTimeMillis();
      }
    } finally {
      // log outside of sync block
      if (logMessage != null && log.isDebugEnabled())
        log.debug(logMessage);
    }
    
    tabletResources.executeMinorCompaction(mct);
    
    log.debug(String.format("MinC initiate lock %.2f secs", (t2 - t1) / 1000.0));
    
    return true;
  }
  
  public synchronized void waitForMinC() {
    tabletMemory.waitForMinC();
  }
  
  static class TConstraintViolationException extends Exception {
    private static final long serialVersionUID = 1L;
    private Violations violations;
    private List<Mutation> violators;
    private List<Mutation> nonViolators;
    private CommitSession commitSession;
    
    TConstraintViolationException(Violations violations, List<Mutation> violators, List<Mutation> nonViolators, CommitSession commitSession) {
      this.violations = violations;
      this.violators = violators;
      this.nonViolators = nonViolators;
      this.commitSession = commitSession;
    }
    
    Violations getViolations() {
      return violations;
    }
    
    List<Mutation> getViolators() {
      return violators;
    }
    
    List<Mutation> getNonViolators() {
      return nonViolators;
    }
    
    CommitSession getCommitSession() {
      return commitSession;
    }
  }
  
  private synchronized CommitSession finishPreparingMutations(long time) {
    if (writesInProgress < 0) {
      throw new IllegalStateException("waitingForLogs < 0 " + writesInProgress);
    }
    
    if (closed || tabletMemory == null) {
      // log.debug("tablet closed, can't commit");
      return null;
    }
    
    writesInProgress++;
    CommitSession commitSession = tabletMemory.getCommitSession();
    commitSession.incrementCommitsInProgress();
    commitSession.updateMaxCommittedTime(time);
    return commitSession;
  }
  
  public CommitSession prepareMutationsForCommit(List<Mutation> mutations) throws TConstraintViolationException {
    
    ConstraintChecker cc = constraintChecker.get();
    
    List<Mutation> violators = null;
    Violations violations = new Violations();
    for (Mutation mutation : mutations) {
      Violations more = cc.check(extent, mutation);
      if (more != null) {
        violations.add(more);
        if (violators == null)
          violators = new ArrayList<Mutation>();
        violators.add(mutation);
      }
    }
    
    long time = tabletTime.setUpdateTimes(mutations);
    
    if (!violations.isEmpty()) {
      
      HashSet<Mutation> violatorsSet = new HashSet<Mutation>(violators);
      ArrayList<Mutation> nonViolators = new ArrayList<Mutation>();
      
      for (Mutation mutation : mutations) {
        if (!violatorsSet.contains(mutation)) {
          nonViolators.add(mutation);
        }
      }
      
      CommitSession commitSession = null;
      
      if (nonViolators.size() > 0) {
        // if everything is a violation, then it is expected that
        // code calling this will not log or commit
        commitSession = finishPreparingMutations(time);
        if (commitSession == null)
          return null;
      }
      
      throw new TConstraintViolationException(violations, violators, nonViolators, commitSession);
    }
    
    return finishPreparingMutations(time);
  }
  
  public synchronized void abortCommit(CommitSession commitSession, List<Mutation> value) {
    if (writesInProgress <= 0) {
      throw new IllegalStateException("waitingForLogs <= 0 " + writesInProgress);
    }
    
    if (closeComplete || tabletMemory == null) {
      throw new IllegalStateException("aborting commit when tablet is closed");
    }
    
    commitSession.decrementCommitsInProgress();
    writesInProgress--;
    if (writesInProgress == 0)
      this.notifyAll();
  }
  
  public void commit(CommitSession commitSession, List<Mutation> mutations) {
    
    int totalCount = 0;
    long totalBytes = 0;
    
    // write the mutation to the in memory table
    for (Mutation mutation : mutations) {
      totalCount += mutation.size();
      totalBytes += mutation.numBytes();
    }
    
    tabletMemory.mutate(commitSession, mutations);
    
    synchronized (this) {
      if (writesInProgress < 1) {
        throw new IllegalStateException("commiting mutations after logging, but not waiting for any log messages");
      }
      
      if (closed && closeComplete) {
        throw new IllegalStateException("tablet closed with outstanding messages to the logger");
      }
      
      tabletMemory.updateMemoryUsageStats();
      
      // decrement here in case an exception is thrown below
      writesInProgress--;
      if (writesInProgress == 0)
        this.notifyAll();
      
      commitSession.decrementCommitsInProgress();
      
      numEntries += totalCount;
      numEntriesInMemory += totalCount;
      ingestCount += totalCount;
      ingestBytes += totalBytes;
    }
  }
  
  /**
   * Closes the mapfiles associated with a Tablet. If saveState is true, a minor compaction is performed.
   */
  public void close(boolean saveState) throws IOException {
    initiateClose(saveState, false, false);
    completeClose(saveState, true);
  }
  
  void initiateClose(boolean saveState, boolean queueMinC, boolean disableWrites) {
    
    if (!saveState && queueMinC) {
      throw new IllegalArgumentException("Not saving state on close and requesting minor compactions queue does not make sense");
    }
    
    log.debug("initiateClose(saveState=" + saveState + " queueMinC=" + queueMinC + " disableWrites=" + disableWrites + ") " + getExtent());
    
    MinorCompactionTask mct = null;
    
    synchronized (this) {
      if (closed || closing || closeComplete) {
        String msg = "Tablet " + getExtent() + " already";
        if (closed)
          msg += " closed";
        if (closing)
          msg += " closing";
        if (closeComplete)
          msg += " closeComplete";
        throw new IllegalStateException(msg);
      }
      
      // enter the closing state, no splits, minor, or major compactions can start
      // should cause running major compactions to stop
      closing = true;
      this.notifyAll();
      
      // determines if inserts and queries can still continue while minor compacting
      closed = disableWrites;
      
      // wait for major compactions to finish, setting closing to
      // true should cause any running major compactions to abort
      while (majorCompactionInProgress) {
        try {
          this.wait(50);
        } catch (InterruptedException e) {
          log.error(e.toString());
        }
      }
      
      if (!saveState || tabletMemory.getMemTable().getNumEntries() == 0) {
        return;
      }
      
      tabletMemory.waitForMinC();
      
      mct = prepareForMinC();
      
      if (queueMinC) {
        tabletResources.executeMinorCompaction(mct);
        return;
      }
      
    }
    
    // do minor compaction outside of synch block so that tablet can be read and written to while
    // compaction runs
    mct.run();
  }
  
  private boolean closeCompleting = false;
  
  synchronized void completeClose(boolean saveState, boolean completeClose) throws IOException {
    
    if (!closing || closeComplete || closeCompleting) {
      throw new IllegalStateException("closing = " + closing + " closed = " + closed + " closeComplete = " + closeComplete + " closeCompleting = "
          + closeCompleting);
    }
    
    log.debug("completeClose(saveState=" + saveState + " completeClose=" + completeClose + ") " + getExtent());
    
    // ensure this method is only called once, also guards against multiple
    // threads entering the method at the same time
    closeCompleting = true;
    closed = true;
    
    // modify dataSourceDeletions so scans will try to switch data sources and fail because the tablet is closed
    dataSourceDeletions.incrementAndGet();
    
    for (ScanDataSource activeScan : activeScans) {
      activeScan.interrupt();
    }
    
    // wait for reads and writes to complete
    while (writesInProgress > 0 || activeScans.size() > 0) {
      try {
        this.wait(50);
      } catch (InterruptedException e) {
        log.error(e.toString());
      }
    }
    
    tabletMemory.waitForMinC();
    
    if (saveState && tabletMemory.getMemTable().getNumEntries() > 0) {
      prepareForMinC().run();
    }
    
    if (saveState) {
      // at this point all tablet data is flushed, so do a consistency check
      RuntimeException err = null;
      for (int i = 0; i < 5; i++) {
        try {
          closeConsistencyCheck();
          err = null;
        } catch (RuntimeException t) {
          err = t;
          log.error("Consistency check fails, retrying " + t);
          UtilWaitThread.sleep(500);
        }
      }
      if (err != null)
        throw err;
    }
    
    try {
      tabletMemory.getMemTable().delete(0);
    } catch (Throwable t) {
      log.error("Failed to delete mem table : " + t.getMessage(), t);
    }
    
    tabletMemory = null;
    
    // close map files
    tabletResources.close();
    
    log.log(TLevel.TABLET_HIST, extent + " closed");
    
    acuTableConf.removeObserver(configObserver);
    
    closeComplete = completeClose;
  }
  
  private void closeConsistencyCheck() {
    
    if (tabletMemory.getMemTable().getNumEntries() != 0) {
      String msg = "Closed tablet " + extent + " has " + tabletMemory.getMemTable().getNumEntries() + " entries in memory";
      log.error(msg);
      throw new RuntimeException(msg);
    }
    
    if (tabletMemory.memoryReservedForMinC()) {
      String msg = "Closed tablet " + extent + " has minor compacting memory";
      log.error(msg);
      throw new RuntimeException(msg);
    }
    
    try {
      Pair<List<LogEntry>,SortedMap<String,DataFileValue>> fileLog = MetadataTable.getFileAndLogEntries(SecurityConstants.systemCredentials, extent);
      
      if (fileLog.getFirst().size() != 0) {
        String msg = "Closed tablet " + extent + " has walog entries in !METADATA " + fileLog.getFirst();
        log.error(msg);
        throw new RuntimeException(msg);
      }
      
      if (extent.equals(Constants.ROOT_TABLET_EXTENT)) {
        if (!fileLog.getSecond().keySet().equals(datafileManager.datafileSizes.keySet())) {
          String msg = "Data file in !METADATA differ from in memory data " + extent + "  " + fileLog.getSecond().keySet() + "  "
              + datafileManager.datafileSizes.keySet();
          log.error(msg);
          throw new RuntimeException(msg);
        }
      } else {
        if (!fileLog.getSecond().equals(datafileManager.datafileSizes)) {
          String msg = "Data file in !METADATA differ from in memory data " + extent + "  " + fileLog.getSecond() + "  " + datafileManager.datafileSizes;
          log.error(msg);
          throw new RuntimeException(msg);
        }
      }
      
    } catch (Exception e) {
      String msg = "Failed to do close consistency check for tablet " + extent;
      log.error(msg, e);
      throw new RuntimeException(msg, e);
      
    }
    
    if (otherLogs.size() != 0 || currentLogs.size() != 0) {
      String msg = "Closed tablet " + extent + " has walog entries in memory currentLogs = " + currentLogs + "  otherLogs = " + otherLogs;
      log.error(msg);
      throw new RuntimeException(msg);
    }
  }
  
  /**
   * Returns a Path object representing the tablet's location on the DFS.
   * 
   * @return location
   */
  public Path getLocation() {
    return location;
  }
  
  private class CompactionRunner implements Runnable, Comparable<CompactionRunner> {
    
    long queued;
    long start;
    boolean failed = false;
    private boolean idleCompaction;
    
    public CompactionRunner(boolean idleCompactions) {
      queued = System.currentTimeMillis();
      this.idleCompaction = idleCompactions;
      
    }
    
    public void run() {
      MajorCompactionStats majCStats = null;
      
      if (tabletServer.isMajorCompactionDisabled()) {
        // this will make compaction task that were queued when shutdown was
        // initiated exit
        majorCompactionQueued = false;
        return;
      }
      
      try {
        timer.incrementStatusMajor();
        start = System.currentTimeMillis();
        majCStats = majorCompact(idleCompaction);
      } catch (RuntimeException E) {
        failed = true;
      } finally {
        long count = 0;
        if (majCStats != null) {
          count = majCStats.getEntriesRead();
        }
        
        timer.updateTime(Operation.MAJOR, queued, start, count, failed);
      }
    }
    
    private int getNumFiles() {
      synchronized (Tablet.this) {
        return datafileManager.getDatafileSizes().size();
      }
    }
    
    @Override
    public int compareTo(CompactionRunner o) {
      return o.getNumFiles() - this.getNumFiles();
    }
  }
  
  synchronized boolean initiateMajorCompaction(boolean idleCompaction) {
    
    if (closing || closed || !needsMajorCompaction(idleCompaction) || majorCompactionInProgress || majorCompactionQueued) {
      return false;
    }
    
    majorCompactionQueued = true;
    
    tabletResources.executeMajorCompaction(getExtent(), new CompactionRunner(idleCompaction));
    
    return false;
  }
  
  /**
   * Returns true if a major compaction should be performed on the tablet.
   * 
   */
  public boolean needsMajorCompaction(boolean idleCompaction) {
    if (majorCompactionInProgress)
      return false;
    return tabletResources.needsMajorCompaction(idleCompaction);
  }
  
  private class CompactionTuple {
    private Map<String,Long> filesToCompact;
    private boolean compactAll;
    
    public CompactionTuple(Map<String,Long> filesToCompact, boolean doAll) {
      this.filesToCompact = filesToCompact;
      compactAll = doAll;
    }
    
    public Map<String,Long> getFilesToCompact() {
      return filesToCompact;
    }
    
    public boolean getCompactAll() {
      return compactAll;
    }
  }
  
  /**
   * Returns list of files that need to be compacted by major compactor
   */
  
  private CompactionTuple getFilesToCompact(boolean idleCompaction) {
    Map<String,Long> toCompact = tabletResources.findMapFilesToCompact();
    return new CompactionTuple(toCompact, toCompact.size() == datafileManager.getDatafileSizes().size());
  }
  
  /**
   * Returns an int representing the total block size of the mapfiles served by this tablet.
   * 
   * @return size
   */
  // this is the size of just the mapfiles
  public long estimateTabletSize() {
    long size = 0L;
    
    for (DataFileValue sz : datafileManager.getDatafileSizes().values())
      size += sz.getSize();
    
    return size;
  }
  
  private boolean sawBigRow = false;
  private long timeOfLastMinCWhenBigFreakinRowWasSeen = 0;
  private long timeOfLastImportWhenBigFreakinRowWasSeen = 0;
  private long splitCreationTime;
  
  private static class SplitRowSpec {
    double splitRatio;
    Text row;
    
    SplitRowSpec(double splitRatio, Text row) {
      this.splitRatio = splitRatio;
      this.row = row;
    }
  }
  
  private SplitRowSpec findSplitRow() {
    
    // never split the root tablet
    // check if we already decided that we can never split
    // check to see if we're big enough to split
    
    long splitThreshold = acuTableConf.getMemoryInBytes(Property.TABLE_SPLIT_THRESHOLD);
    if (location.toString().equals(Constants.getRootTabletDir()) || estimateTabletSize() <= splitThreshold) {
      return null;
    }
    
    // have seen a big row before, do not bother checking unless a minor compaction or map file import has occurred.
    if (sawBigRow) {
      if (timeOfLastMinCWhenBigFreakinRowWasSeen != lastMinorCompactionFinishTime || timeOfLastImportWhenBigFreakinRowWasSeen != lastMapFileImportTime) {
        // a minor compaction or map file import has occurred... check again
        sawBigRow = false;
      } else {
        // nothing changed, do not split
        return null;
      }
    }
    
    SortedMap<Double,Key> keys = null;
    
    try {
      // we should make .25 below configurable
      keys = FileUtil.findMidPoint(extent.getPrevEndRow(), extent.getEndRow(), tabletResources.getCopyOfMapFilePaths(), .25);
    } catch (IOException e) {
      log.error("Failed to find midpoint " + e.getMessage());
      return null;
    }
    
    // check to see if one row takes up most of the tablet, in which case we can not split
    try {
      
      Text lastRow;
      if (extent.getEndRow() == null) {
        Key lastKey = (Key) FileUtil.findLastKey(tabletResources.getCopyOfMapFilePaths());
        lastRow = lastKey.getRow();
      } else {
        lastRow = extent.getEndRow();
      }
      
      // check to see that the midPoint is not equal to the end key
      if (keys.get(.5).compareRow(lastRow) == 0) {
        if (keys.firstKey() < .5) {
          Key candidate = keys.get(keys.firstKey());
          if (candidate.compareRow(lastRow) != 0) {
            // we should use this ratio in split size estimations
            if (log.isTraceEnabled())
              log.trace(String.format("Splitting at %6.2f instead of .5, row at .5 is same as end row\n", keys.firstKey()));
            return new SplitRowSpec(keys.firstKey(), candidate.getRow());
          }
          
        }
        
        log.warn("Cannot split tablet " + extent + " it contains a big row : " + lastRow);
        
        sawBigRow = true;
        timeOfLastMinCWhenBigFreakinRowWasSeen = lastMinorCompactionFinishTime;
        timeOfLastImportWhenBigFreakinRowWasSeen = lastMapFileImportTime;
        
        return null;
      }
      Key mid = keys.get(.5);
      Text text = (mid == null) ? null : mid.getRow();
      SortedMap<Double,Key> firstHalf = keys.headMap(.5);
      if (firstHalf.size() > 0) {
        Text beforeMid = firstHalf.get(firstHalf.lastKey()).getRow();
        Text shorter = new Text();
        int trunc = longestCommonLength(text, beforeMid);
        shorter.set(text.getBytes(), 0, Math.min(text.getLength(), trunc + 1));
        text = shorter;
      }
      return new SplitRowSpec(.5, text);
    } catch (IOException e) {
      // don't split now, but check again later
      log.error("Failed to find lastkey " + e.getMessage());
      return null;
    }
  }
  
  private static int longestCommonLength(Text text, Text beforeMid) {
    int common = 0;
    while (common < text.getLength() && common < beforeMid.getLength() && text.getBytes()[common] == beforeMid.getBytes()[common]) {
      common++;
    }
    return common;
  }
  
  /**
   * Returns true if this tablet needs to be split
   * 
   */
  public synchronized boolean needsSplit() {
    boolean ret;
    
    if (closing || closed)
      ret = false;
    else
      ret = findSplitRow() != null;
    
    return ret;
  }
  
  public static class MajorCompactionStats {
    private long entriesRead;
    private long entriesWritten;
    
    MajorCompactionStats(long er, long ew) {
      this.setEntriesRead(er);
      this.setEntriesWritten(ew);
    }
    
    public MajorCompactionStats() {}
    
    private void setEntriesRead(long entriesRead) {
      this.entriesRead = entriesRead;
    }
    
    public long getEntriesRead() {
      return entriesRead;
    }
    
    private void setEntriesWritten(long entriesWritten) {
      this.entriesWritten = entriesWritten;
    }
    
    public long getEntriesWritten() {
      return entriesWritten;
    }
    
    public void add(MajorCompactionStats mcs) {
      this.entriesRead += mcs.entriesRead;
      this.entriesWritten += mcs.entriesWritten;
    }
    
  }
  
  // BEGIN PRIVATE METHODS RELATED TO MAJOR COMPACTION
  
  private static class MajorCompactionCanceledException extends Exception {
    private static final long serialVersionUID = 1L;
  }
  
  private boolean isCompactionEnabled() {
    return !closing && !tabletServer.isMajorCompactionDisabled();
  }
  
  private List<SortedKeyValueIterator<Key,Value>> openMapDataFiles(String lgName, Configuration conf, FileSystem fs, Set<String> mapFiles, KeyExtent extent,
      ArrayList<FileSKVIterator> readers) throws IOException {
    
    List<SortedKeyValueIterator<Key,Value>> iters = new ArrayList<SortedKeyValueIterator<Key,Value>>(mapFiles.size());
    
    for (String mapFile : mapFiles) {
      try {
        
        FileOperations fileFactory = FileOperations.getInstance();
        
        FileSKVIterator reader;
        
        AccumuloConfiguration tableConf = AccumuloConfiguration.getTableConfiguration(HdfsZooInstance.getInstance().getInstanceID(), extent.getTableId()
            .toString());
        
        reader = fileFactory.openReader(mapFile, false, fs, conf, tableConf);
        
        readers.add(reader);
        iters.add(new ProblemReportingIterator(extent.getTableId().toString(), mapFile, false, reader));
        
      } catch (Throwable e) {
        
        ProblemReports.getInstance().report(new ProblemReport(extent.getTableId().toString(), ProblemType.FILE_READ, mapFile, e));
        
        log.warn("Some problem opening map file " + mapFile + " " + e.getMessage(), e);
        // failed to open some map file... close the ones that were opened
        for (FileSKVIterator reader : readers) {
          try {
            reader.close();
          } catch (Throwable e2) {
            log.warn("Failed to close map file", e2);
          }
        }
        
        readers.clear();
        
        if (e instanceof IOException)
          throw (IOException) e;
        throw new IOException("Failed to open map data files", e);
      }
    }
    
    return iters;
  }
  
  private void compactLocalityGroup(String lgName, Set<ByteSequence> columnFamilies, boolean inclusive, Configuration conf, FileSystem fs,
      Set<String> filesToCompact, boolean propogateDeletes, KeyExtent extent, String compactTmpName, FileSKVWriter mfw, MajorCompactionStats majCStats)
      throws IOException, MajorCompactionCanceledException {
    ArrayList<FileSKVIterator> readers = new ArrayList<FileSKVIterator>(filesToCompact.size());
    Span span = Trace.start("compact");
    try {
      long entriesCompacted = 0;
      List<SortedKeyValueIterator<Key,Value>> iters = openMapDataFiles(lgName, conf, fs, filesToCompact, extent, readers);
      CountingIterator citr = new CountingIterator(new MultiIterator(iters, extent.toDataRange()));
      DeletingIterator delIter = new DeletingIterator(citr, propogateDeletes);
      
      IteratorEnvironment iterEnv = new TabletIteratorEnvironment(IteratorScope.majc, !propogateDeletes, acuTableConf);
      
      SortedKeyValueIterator<Key,Value> itr = IteratorUtil.loadIterators(IteratorScope.majc, delIter, extent, acuTableConf, iterEnv);
      
      itr.seek(extent.toDataRange(), columnFamilies, inclusive);
      
      if (!inclusive) {
        mfw.startDefaultLocalityGroup();
      } else {
        mfw.startNewLocalityGroup(lgName, columnFamilies);
      }
      
      Span write = Trace.start("write");
      try {
        while (itr.hasTop() && isCompactionEnabled()) {
          mfw.append(itr.getTopKey(), itr.getTopValue());
          itr.next();
          entriesCompacted++;
        }
        
        if (itr.hasTop() && !isCompactionEnabled()) {
          // cancel major compaction operation
          try {
            try {
              mfw.close();
            } catch (IOException e) {
              log.error(e, e);
            }
            fs.delete(new Path(compactTmpName), true);
          } catch (Exception e) {
            log.warn("Failed to delete Canceled major compaction output file " + compactTmpName, e);
          }
          throw new MajorCompactionCanceledException();
        }
        
      } finally {
        MajorCompactionStats lgMajcStats = new MajorCompactionStats(citr.getCount(), entriesCompacted);
        majCStats.add(lgMajcStats);
        write.stop();
      }
      
    } finally {
      // close sequence files opened
      for (FileSKVIterator reader : readers) {
        try {
          reader.close();
        } catch (Throwable e) {
          log.warn("Failed to close map file", e);
        }
      }
      span.stop();
    }
  }
  
  private MajorCompactionStats compactFiles(Configuration conf, FileSystem fs, Set<String> filesToCompact, boolean propogateDeletes, KeyExtent extent,
      String compactTmpName) throws IOException, MajorCompactionCanceledException {
    
    FileSKVWriter mfw = null;
    
    MajorCompactionStats majCStats = new MajorCompactionStats();
    
    try {
      FileOperations fileFactory = FileOperations.getInstance();
      AccumuloConfiguration tableConf = AccumuloConfiguration.getTableConfiguration(HdfsZooInstance.getInstance().getInstanceID(), extent.getTableId()
          .toString());
      mfw = fileFactory.openWriter(compactTmpName, fs, conf, tableConf);
      
      Map<String,Set<ByteSequence>> lGroups;
      try {
        lGroups = LocalityGroupUtil.getLocalityGroups(tableConf);
      } catch (LocalityGroupConfigurationError e) {
        throw new IOException(e);
      }
      
      long t1 = System.currentTimeMillis();
      
      HashSet<ByteSequence> allColumnFamilies = new HashSet<ByteSequence>();
      
      if (mfw.supportsLocalityGroups()) {
        for (Entry<String,Set<ByteSequence>> entry : lGroups.entrySet()) {
          compactLocalityGroup(entry.getKey(), entry.getValue(), true, conf, fs, filesToCompact, propogateDeletes, extent, compactTmpName, mfw, majCStats);
          allColumnFamilies.addAll(entry.getValue());
        }
      }
      
      compactLocalityGroup(null, allColumnFamilies, false, conf, fs, filesToCompact, propogateDeletes, extent, compactTmpName, mfw, majCStats);
      
      long t2 = System.currentTimeMillis();
      
      FileSKVWriter mfwTmp = mfw;
      mfw = null; // set this to null so we do not try to close it again in finally if the close fails
      mfwTmp.close(); // if the close fails it will cause the compaction to fail
      
      // Verify the file, since hadoop 0.20.2 sometimes lies about the success of close()
      try {
        FileSKVIterator openReader = fileFactory.openReader(compactTmpName, false, fs, conf, tableConf);
        openReader.close();
      } catch (IOException ex) {
        log.error("Verification of successful major compaction fails!!!", ex);
        throw ex;
      }
      
      log.debug(String.format("MajC %,d read | %,d written | %,6d entries/sec | %6.3f secs", majCStats.getEntriesRead(), majCStats.getEntriesWritten(),
          (int) (majCStats.getEntriesRead() / ((t2 - t1) / 1000.0)), (t2 - t1) / 1000.0));
      
      return majCStats;
    } catch (IOException e) {
      log.error(e, e);
      throw e;
    } catch (MajorCompactionCanceledException e) {
      log.info("Major compaction Canceled");
      throw e;
    } catch (RuntimeException e) {
      log.error(e, e);
      throw e;
    } finally {
      try {
        if (mfw != null) {
          // compaction must not have finished successfully, so close its output file
          try {
            mfw.close();
          } finally {
            Path path = new Path(compactTmpName);
            if (!fs.delete(path, true))
              if (fs.exists(path))
                log.error("Unable to delete " + compactTmpName);
          }
        }
      } catch (IOException e) {
        log.warn(e, e);
      }
    }
    
  }
  
  private MajorCompactionStats _majorCompact(boolean idleCompaction) throws IOException, MajorCompactionCanceledException {
    
    boolean propogateDeletes;
    
    long t1, t2, t3;
    
    Map<String,Long> filesToCompact;
    List<String> fileNames = new LinkedList<String>();
    
    int maxFilesToCompact = acuTableConf.getCount(Property.TSERV_MAJC_MAXOPEN) / acuTableConf.getCount(Property.TSERV_MAJC_MAXCONCURRENT);
    synchronized (this) {
      // plan all that work that needs to be done in the sync block... then do the actual work
      // outside the sync block
      
      t1 = System.currentTimeMillis();
      
      majorCompactionWaitingToStart = true;
      
      tabletMemory.waitForMinC();
      
      t2 = System.currentTimeMillis();
      
      majorCompactionWaitingToStart = false;
      notifyAll();
      
      if (extent.equals(Constants.ROOT_TABLET_EXTENT)) {
        // very important that we call this before doing major compaction,
        // otherwise deleted compacted files could possible be brought back
        // at some point if the file they were compacted to was legitimately
        // removed by a major compaction
        cleanUpFiles(fs, fs.listStatus(this.location), this.location, false);
      }
      
      // getFilesToCompact() and cleanUpFiles() both
      // do dir listings, which means two calls to the namenode
      // we should refactor so that there is only one call
      CompactionTuple ret = getFilesToCompact(idleCompaction);
      if (ret == null) {
        // nothing to compact
        return null;
      }
      filesToCompact = ret.getFilesToCompact();
      
      if (!ret.getCompactAll()) {
        // since not all files are being compacted, we want to propagate delete entries
        propogateDeletes = true;
      } else {
        propogateDeletes = false;
      }
      
      // if(tabletMemory.getMemTable().getNumEntries() > 0)
      // minorCompactionTask = prepareForMinC();
      
      // increment generation after choosing minor compact filename
      incrementGeneration();
      
      int numToCompact = filesToCompact.size();
      
      while (numToCompact > 0) {
        fileNames.add(getNextMapFilename());
        numToCompact -= maxFilesToCompact;
        if (numToCompact > 0)
          numToCompact++;
      }
      
      t3 = System.currentTimeMillis();
    }
    
    log.debug(String.format("MajC initiate lock %.2f secs, wait %.2f secs", (t3 - t2) / 1000.0, (t2 - t1) / 1000.0));
    
    MajorCompactionStats majCStats = new MajorCompactionStats();
    
    // need to handle case where only one file is being major compacted
    while (filesToCompact.size() > 0) {
      
      int numToCompact = maxFilesToCompact;
      
      if (filesToCompact.size() > maxFilesToCompact && filesToCompact.size() < 2 * maxFilesToCompact) {
        // on the second to last compaction pass, compact the minimum amount of files possible
        numToCompact = filesToCompact.size() - maxFilesToCompact + 1;
      }
      
      Set<String> smallestFiles = removeSmallest(filesToCompact, numToCompact);
      
      String fileName = fileNames.remove(0);
      String compactTmpName = fileName + "_tmp";
      
      Span span = Trace.start("compactFiles");
      try {
        MajorCompactionStats mcs = compactFiles(conf, fs, smallestFiles, filesToCompact.size() == 0 ? propogateDeletes : true, // always propagate
                                                                                                                               // deletes,
                                                                                                                               // unless last batch
            extent, compactTmpName);
        span.data("files", "" + smallestFiles.size());
        span.data("read", "" + mcs.entriesRead);
        span.data("written", "" + mcs.entriesWritten);
        majCStats.add(mcs);
        
        long size = FileOperations.getInstance().getFileSize(compactTmpName, fs, conf,
            AccumuloConfiguration.getTableConfiguration(HdfsZooInstance.getInstance().getInstanceID(), extent.getTableId().toString()));
        
        datafileManager.bringMajorCompactionOnline(smallestFiles, compactTmpName, fileName, new DataFileValue(size, mcs.entriesWritten));
        
        // when major compaction produces a file w/ zero entries, it will be deleted... do not want
        // to add the deleted file
        if (filesToCompact.size() > 0 && mcs.entriesWritten > 0) {
          filesToCompact.put(fileName, size);
        }
      } finally {
        span.stop();
      }
      
    }
    
    return majCStats;
  }
  
  private Set<String> removeSmallest(Map<String,Long> filesToCompact, int maxFilesToCompact) {
    // ensure this method works properly when multiple files have the same size
    
    PriorityQueue<Pair<String,Long>> fileHeap = new PriorityQueue<Pair<String,Long>>(filesToCompact.size(), new Comparator<Pair<String,Long>>() {
      @Override
      public int compare(Pair<String,Long> o1, Pair<String,Long> o2) {
        if (o1.getSecond() == o2.getSecond())
          return o1.getFirst().compareTo(o2.getFirst());
        if (o1.getSecond() < o2.getSecond())
          return -1;
        return 1;
      }
    });
    
    for (Iterator<Entry<String,Long>> iterator = filesToCompact.entrySet().iterator(); iterator.hasNext();) {
      Entry<String,Long> entry = (Entry<String,Long>) iterator.next();
      fileHeap.add(new Pair<String,Long>(entry.getKey(), entry.getValue()));
    }
    
    Set<String> smallestFiles = new HashSet<String>();
    while (smallestFiles.size() < maxFilesToCompact && fileHeap.size() > 0) {
      Pair<String,Long> pair = fileHeap.remove();
      filesToCompact.remove(pair.getFirst());
      smallestFiles.add(pair.getFirst());
    }
    
    return smallestFiles;
  }
  
  // END PRIVATE METHODS RELATED TO MAJOR COMPACTION
  
  /**
   * Performs a major compaction on the tablet. If needsSplit() returns true, the tablet is split and a reference to the new tablet is returned.
   */
  
  private MajorCompactionStats majorCompact(boolean idleCompaction) {
    
    MajorCompactionStats majCStats = null;
    
    // Always trace majC
    Span span = Trace.on("majorCompaction");
    try {
      synchronized (this) {
        // check that compaction is still needed - defer to splitting
        majorCompactionQueued = false;
        
        if (closing || closed || !needsMajorCompaction(idleCompaction) || majorCompactionInProgress || needsSplit()) {
          return null;
        }
        
        majorCompactionInProgress = true;
      }
      
      majCStats = _majorCompact(idleCompaction);
    } catch (MajorCompactionCanceledException mcce) {
      log.debug("Major compaction canceled, extent = " + getExtent());
      throw new RuntimeException(mcce);
    } catch (Throwable t) {
      log.error("MajC Failed, extent = " + getExtent());
      log.error("MajC Failed, message = " + (t.getMessage() == null ? t.getClass().getName() : t.getMessage()), t);
      throw new RuntimeException(t);
    } finally {
      // ensure we always reset boolean, even
      // when an exception is thrown
      synchronized (this) {
        majorCompactionInProgress = false;
        this.notifyAll();
      }
      Span curr = Trace.currentTrace();
      curr.data("extent", "" + getExtent());
      curr.data("read", "" + majCStats.entriesRead);
      curr.data("written", "" + majCStats.entriesWritten);
      span.stop();
    }
    
    return majCStats;
  }
  
  /**
   * Returns a KeyExtent object representing this tablet's key range.
   * 
   * @return extent
   */
  public KeyExtent getExtent() {
    return extent;
  }
  
  private synchronized void computeNumEntries() {
    Collection<DataFileValue> vals = datafileManager.getDatafileSizes().values();
    
    long numEntries = 0;
    
    for (DataFileValue tableValue : vals) {
      numEntries += tableValue.getNumEntries();
    }
    
    this.numEntriesInMemory = tabletMemory.getNumEntries();
    numEntries += tabletMemory.getNumEntries();
    
    this.numEntries = numEntries;
  }
  
  public long getNumEntries() {
    return numEntries;
  }
  
  public long getNumEntriesInMemory() {
    return numEntriesInMemory;
  }
  
  /*
   * public void dump() throws IOException {
   * 
   * MyMapFile.Reader[] a = null;
   * 
   * try{ a = tabletResources.reserveMapFileReaders();
   * 
   * MMFAndMemoryIterator mmfi = new MMFAndMemoryIterator(new MultipleMapFileIterator(a,extent.getPrevEndRow(), null,extent.getEndRow(), false, 1), memTable,
   * otherMemTable);
   * 
   * while(mmfi.hasTop()) { log.info("TABLET DUMP ("+location+"): "+mmfi.getTopKey() + " " + mmfi.getTopValue()); mmfi.next(); }
   * 
   * }finally{ if(a != null){ tabletResources.returnMapFileReaders(); } }
   * 
   * 
   * }
   */
  
  public synchronized boolean isClosing() {
    return closing;
  }
  
  public synchronized boolean isClosed() {
    return closed;
  }
  
  public synchronized boolean isCloseComplete() {
    return closeComplete;
  }
  
  public boolean majorCompactionRunning() {
    return this.majorCompactionInProgress;
  }
  
  public boolean minorCompactionQueued() {
    return minorCompactionWaitingToStart;
  }
  
  public boolean minorCompactionRunning() {
    return minorCompactionInProgress;
  }
  
  public boolean majorCompactionQueued() {
    return majorCompactionQueued;
  }
  
  /**
   * operations are disallowed while we split which is ok since splitting is fast
   * 
   * a minor compaction should have taken place before calling this so there should be relatively little left to compact
   * 
   * we just need to make sure major compactions aren't occurring if we have the major compactor thread decide who needs splitting we can avoid synchronization
   * issues with major compactions
   * 
   */
  
  static class SplitInfo {
    String dir;
    SortedMap<String,DataFileValue> datafiles;
    String time;
    
    SplitInfo(String d, SortedMap<String,DataFileValue> dfv, String time) {
      this.dir = d;
      this.datafiles = dfv;
      this.time = time;
    }
    
  }
  
  public TreeMap<KeyExtent,SplitInfo> split(byte[] sp) throws IOException {
    
    if (sp != null && extent.getEndRow() != null && extent.getEndRow().equals(new Text(sp))) {
      throw new IllegalArgumentException();
    }
    
    if (extent.equals(Constants.ROOT_TABLET_EXTENT)) {
      String msg = "Cannot split root tablet";
      log.warn(msg);
      throw new RuntimeException(msg);
    }
    
    try {
      initiateClose(true, false, false);
    } catch (IllegalStateException ise) {
      log.debug("File " + extent + " not splitting : " + ise.getMessage());
      return null;
    }
    
    // obtain this info outside of synch block since it will involve opening
    // the map files... it is ok if the set of map files changes, because
    // this info is used for optimization... it is ok if map files are missing
    // from the set... can still query and insert into the tablet while this
    // map file operation is happening
    Map<String,org.apache.accumulo.core.file.FileUtil.FileInfo> firstAndLastRowsAbs = FileUtil
        .tryToGetFirstAndLastRows(tabletResources.getCopyOfMapFilePaths());
    
    // convert absolute paths to relative paths
    Map<String,org.apache.accumulo.core.file.FileUtil.FileInfo> firstAndLastRows = new HashMap<String,org.apache.accumulo.core.file.FileUtil.FileInfo>();
    
    for (Entry<String,org.apache.accumulo.core.file.FileUtil.FileInfo> entry : firstAndLastRowsAbs.entrySet()) {
      String tdir = Constants.getTablesDir() + "/" + extent.getTableId();
      
      if (entry.getKey().startsWith(tdir)) {
        firstAndLastRows.put(entry.getKey().substring(tdir.length()), entry.getValue());
      } else {
        log.warn("Absolute path not in table dir " + entry.getKey() + " (" + tdir + ")");
      }
    }
    
    synchronized (this) {
      // java needs tuples ...
      TreeMap<KeyExtent,SplitInfo> newTablets = new TreeMap<KeyExtent,SplitInfo>();
      
      long t1 = System.currentTimeMillis();
      
      // choose a split point
      SplitRowSpec splitPoint;
      if (sp == null)
        splitPoint = findSplitRow();
      else {
        Text tsp = new Text(sp);
        splitPoint = new SplitRowSpec(FileUtil.estimatePercentageLTE(extent.getPrevEndRow(), extent.getEndRow(), tabletResources.getCopyOfMapFilePaths(), tsp),
            tsp);
      }
      
      if (splitPoint == null || splitPoint.row == null) {
        log.info("had to abort split because splitRow was null");
        closing = false;
        return null;
      }
      
      closed = true;
      completeClose(true, false);
      
      Text midRow = splitPoint.row;
      double splitRatio = splitPoint.splitRatio;
      
      KeyExtent low = new KeyExtent(extent.getTableId(), midRow, extent.getPrevEndRow());
      KeyExtent high = new KeyExtent(extent.getTableId(), extent.getEndRow(), midRow);
      
      String lowDirectory = TabletOperations.createTabletDirectory(fs, location.getParent().toString(), midRow);
      
      // write new tablet information to MetadataTable
      SortedMap<String,DataFileValue> lowDatafileSizes = new TreeMap<String,DataFileValue>();
      SortedMap<String,DataFileValue> highDatafileSizes = new TreeMap<String,DataFileValue>();
      List<String> highDatafilesToRemove = new ArrayList<String>();
      
      MetadataTable.splitDatafiles(extent.getTableId(), midRow, splitRatio, firstAndLastRows, datafileManager.datafileSizes, lowDatafileSizes,
          highDatafileSizes, highDatafilesToRemove);
      
      log.debug("Files for low split " + low + "  " + lowDatafileSizes.keySet());
      log.debug("Files for high split " + high + "  " + highDatafileSizes.keySet());
      
      String time = tabletTime.getMetadataValue();
      
      MetadataTable.splitTablet(high, extent.getPrevEndRow(), splitRatio, SecurityConstants.systemCredentials, tabletServer.getLock());
      MetadataTable.addNewTablet(low, lowDirectory, tabletServer.getTabletSession(), lowDatafileSizes, SecurityConstants.systemCredentials, time,
          tabletServer.getLock());
      MetadataTable.finishSplit(high, highDatafileSizes, highDatafilesToRemove, SecurityConstants.systemCredentials, tabletServer.getLock());
      
      log.log(TLevel.TABLET_HIST, extent + " split " + low + " " + high);
      
      newTablets.put(high, new SplitInfo(tabletDirectory, highDatafileSizes, time));
      newTablets.put(low, new SplitInfo(lowDirectory, lowDatafileSizes, time));
      
      long t2 = System.currentTimeMillis();
      
      log.debug(String.format("offline split time : %6.2f secs", (t2 - t1) / 1000.0));
      
      closeComplete = true;
      
      return newTablets;
    }
  }
  
  public SortedMap<String,DataFileValue> getDatafiles() {
    return datafileManager.getDatafileSizes();
  }
  
  public double queryRate() {
    return queryRate.rate();
  }
  
  public double queryByteRate() {
    return queryByteRate.rate();
  }
  
  public double ingestRate() {
    return ingestRate.rate();
  }
  
  public double ingestByteRate() {
    return ingestByteRate.rate();
  }
  
  public long totalQueries() {
    return this.queryCount;
  }
  
  public long totalIngest() {
    return this.ingestCount;
  }
  
  // synchronized?
  public void updateRates(long now) {
    queryRate.update(now, queryCount);
    queryByteRate.update(now, queryBytes);
    ingestRate.update(now, ingestCount);
    ingestByteRate.update(now, ingestBytes);
  }
  
  public long getSplitCreationTime() {
    return splitCreationTime;
  }
  
  public void importMapFiles(Map<String,MapFileInfo> fileMap) throws IOException {
    Map<String,DataFileValue> entries = new HashMap<String,DataFileValue>(fileMap.size());
    
    for (String path : fileMap.keySet()) {
      MapFileInfo mfi = fileMap.get(path);
      entries.put(path, new DataFileValue(mfi.estimatedSize, 0l));
    }
    
    // Clients timeout and will think that this operation failed.
    // Don't do it if we spent too long waiting for the lock
    long now = System.currentTimeMillis();
    synchronized (this) {
      if (closed) {
        throw new IOException("tablet " + extent + " is closed");
      }
      
      long lockWait = System.currentTimeMillis() - now;
      if (lockWait > AccumuloConfiguration.getSystemConfiguration().getTimeInMillis(Property.GENERAL_RPC_TIMEOUT)) {
        throw new IOException("Timeout waiting " + (lockWait / 1000.) + " seconds to get tablet lock");
      }
      
      if (writesInProgress < 0)
        throw new IllegalStateException("writesInProgress < 0 " + writesInProgress);
      
      writesInProgress++;
    }
    
    try {
      datafileManager.importMapFiles(entries);
      lastMapFileImportTime = System.currentTimeMillis();
    } finally {
      synchronized (this) {
        if (writesInProgress < 1)
          throw new IllegalStateException("writesInProgress < 1 " + writesInProgress);
        
        writesInProgress--;
        if (writesInProgress == 0)
          this.notifyAll();
      }
    }
  }
  
  private Set<RemoteLogger> currentLogs = new HashSet<RemoteLogger>();
  
  private Set<String> beginClearingUnusedLogs() {
    Set<String> doomed = new HashSet<String>();
    
    ArrayList<String> otherLogsCopy = new ArrayList<String>();
    ArrayList<String> currentLogsCopy = new ArrayList<String>();
    
    // do not hold tablet lock while acquiring the log lock
    logLock.lock();
    
    synchronized (this) {
      if (removingLogs)
        throw new IllegalStateException("Attempted to clear logs when removal of logs in progress");
      
      for (RemoteLogger logger : otherLogs) {
        otherLogsCopy.add(logger.toString());
        doomed.add(logger.toString());
      }
      
      for (RemoteLogger logger : currentLogs) {
        currentLogsCopy.add(logger.toString());
        doomed.remove(logger.toString());
      }
      
      otherLogs = Collections.emptySet();
      
      if (doomed.size() > 0)
        removingLogs = true;
    }
    
    // do debug logging outside tablet lock
    for (String logger : otherLogsCopy) {
      log.debug("Logs for memory compacted: " + getExtent() + " " + logger.toString());
    }
    
    for (String logger : currentLogsCopy) {
      log.debug("Logs for current memory: " + getExtent() + " " + logger);
    }
    
    return doomed;
  }
  
  private synchronized void finishClearingUnusedLogs() {
    removingLogs = false;
    logLock.unlock();
  }
  
  private Set<RemoteLogger> otherLogs = Collections.emptySet();
  private boolean removingLogs = false;
  
  // this lock is basically used to synchronize writing of log info to !METADATA
  private ReentrantLock logLock = new ReentrantLock();
  
  public synchronized int getLogCount() {
    return currentLogs.size();
  }
  
  private boolean beginUpdatingLogsUsed(InMemoryMap memTable, Collection<RemoteLogger> more, boolean mincFinish) {
    
    boolean releaseLock = true;
    
    // do not hold tablet lock while acquiring the log lock
    logLock.lock();
    
    try {
      synchronized (this) {
        
        if (closed && closeComplete) {
          throw new IllegalStateException("Can not update logs of closed tablet " + extent);
        }
        
        boolean addToOther;
        
        if (memTable == tabletMemory.otherMemTable)
          addToOther = true;
        else if (memTable == tabletMemory.memTable)
          addToOther = false;
        else
          throw new IllegalArgumentException("passed in memtable that is not in use");
        
        if (mincFinish) {
          if (addToOther)
            throw new IllegalStateException("Adding to other logs for mincFinish");
          if (otherLogs.size() != 0)
            throw new IllegalStateException("Expect other logs to be 0 when min finish, but its " + otherLogs);
          
          // when writing a minc finish event, there is no need to add the log to !METADATA
          // if nothing has been logged for the tablet since the minor compaction started
          if (currentLogs.size() == 0)
            return false;
        }
        
        int numAdded = 0;
        int numContained = 0;
        for (RemoteLogger logger : more) {
          if (addToOther) {
            if (otherLogs.add(logger))
              numAdded++;
            
            if (currentLogs.contains(logger))
              numContained++;
          } else {
            if (currentLogs.add(logger))
              numAdded++;
            
            if (otherLogs.contains(logger))
              numContained++;
          }
        }
        
        if (numAdded > 0 && numAdded != more.size()) {
          // expect to add all or none
          throw new IllegalArgumentException("Added subset of logs " + extent + " " + more + " " + currentLogs);
        }
        
        if (numContained > 0 && numContained != more.size()) {
          // expect to contain all or none
          throw new IllegalArgumentException("Other logs contained subset of logs " + extent + " " + more + " " + otherLogs);
        }
        
        if (numAdded > 0 && numContained == 0) {
          releaseLock = false;
          return true;
        }
        
        return false;
      }
    } finally {
      if (releaseLock)
        logLock.unlock();
    }
  }
  
  private void finishUpdatingLogsUsed() {
    logLock.unlock();
  }
  
}

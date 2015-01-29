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

import static com.google.common.base.Charsets.UTF_8;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.impl.ScannerImpl;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.ConfigurationCopy;
import org.apache.accumulo.core.conf.ConfigurationObserver;
import org.apache.accumulo.core.conf.Property;
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
import org.apache.accumulo.core.iterators.IterationInterruptedException;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.IteratorUtil;
import org.apache.accumulo.core.iterators.IteratorUtil.IteratorScope;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.system.ColumnFamilySkippingIterator;
import org.apache.accumulo.core.iterators.system.ColumnQualifierFilter;
import org.apache.accumulo.core.iterators.system.DeletingIterator;
import org.apache.accumulo.core.iterators.system.InterruptibleIterator;
import org.apache.accumulo.core.iterators.system.MultiIterator;
import org.apache.accumulo.core.iterators.system.SourceSwitchingIterator;
import org.apache.accumulo.core.iterators.system.SourceSwitchingIterator.DataSource;
import org.apache.accumulo.core.iterators.system.StatsIterator;
import org.apache.accumulo.core.iterators.system.VisibilityFilter;
import org.apache.accumulo.core.master.thrift.TabletLoadState;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.metadata.RootTable;
import org.apache.accumulo.core.metadata.schema.DataFileValue;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.DataFileColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.LogColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.ScanFileColumnFamily;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.accumulo.core.security.Credentials;
import org.apache.accumulo.core.tabletserver.log.LogEntry;
import org.apache.accumulo.core.util.CachedConfiguration;
import org.apache.accumulo.core.util.LocalityGroupUtil;
import org.apache.accumulo.core.util.LocalityGroupUtil.LocalityGroupConfigurationError;
import org.apache.accumulo.core.util.MapCounter;
import org.apache.accumulo.core.util.Pair;
import org.apache.accumulo.core.util.UtilWaitThread;
import org.apache.accumulo.fate.zookeeper.IZooReaderWriter;
import org.apache.accumulo.server.ServerConstants;
import org.apache.accumulo.server.client.HdfsZooInstance;
import org.apache.accumulo.server.conf.TableConfiguration;
import org.apache.accumulo.server.fs.FileRef;
import org.apache.accumulo.server.fs.VolumeManager;
import org.apache.accumulo.server.fs.VolumeManager.FileType;
import org.apache.accumulo.server.fs.VolumeManagerImpl;
import org.apache.accumulo.server.fs.VolumeUtil;
import org.apache.accumulo.server.fs.VolumeUtil.TabletFiles;
import org.apache.accumulo.server.master.state.TServerInstance;
import org.apache.accumulo.server.master.tableOps.CompactionIterators;
import org.apache.accumulo.server.problems.ProblemReport;
import org.apache.accumulo.server.problems.ProblemReports;
import org.apache.accumulo.server.problems.ProblemType;
import org.apache.accumulo.server.security.SystemCredentials;
import org.apache.accumulo.server.tablets.TabletTime;
import org.apache.accumulo.server.tablets.UniqueNameAllocator;
import org.apache.accumulo.server.util.FileUtil;
import org.apache.accumulo.server.util.MasterMetadataUtil;
import org.apache.accumulo.server.util.MetadataTableUtil;
import org.apache.accumulo.server.util.TabletOperations;
import org.apache.accumulo.server.zookeeper.ZooReaderWriter;
import org.apache.accumulo.start.classloader.vfs.AccumuloVFSClassLoader;
import org.apache.accumulo.trace.instrument.Span;
import org.apache.accumulo.trace.instrument.Trace;
import org.apache.accumulo.tserver.Compactor.CompactionCanceledException;
import org.apache.accumulo.tserver.Compactor.CompactionEnv;
import org.apache.accumulo.tserver.FileManager.ScanFileManager;
import org.apache.accumulo.tserver.InMemoryMap.MemoryIterator;
import org.apache.accumulo.tserver.TabletServer.TservConstraintEnv;
import org.apache.accumulo.tserver.TabletServerResourceManager.TabletResourceManager;
import org.apache.accumulo.tserver.TabletStatsKeeper.Operation;
import org.apache.accumulo.tserver.compaction.CompactionPlan;
import org.apache.accumulo.tserver.compaction.CompactionStrategy;
import org.apache.accumulo.tserver.compaction.DefaultCompactionStrategy;
import org.apache.accumulo.tserver.compaction.MajorCompactionReason;
import org.apache.accumulo.tserver.compaction.MajorCompactionRequest;
import org.apache.accumulo.tserver.compaction.WriteParameters;
import org.apache.accumulo.tserver.constraints.ConstraintChecker;
import org.apache.accumulo.tserver.log.DfsLogger;
import org.apache.accumulo.tserver.log.MutationReceiver;
import org.apache.accumulo.tserver.mastermessage.TabletStatusMessage;
import org.apache.accumulo.tserver.metrics.TabletServerMinCMetrics;
import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.NoNodeException;

import com.google.common.annotations.VisibleForTesting;

/*
 * We need to be able to have the master tell a tabletServer to
 * close this file, and the tablet server to handle all pending client reads
 * before closing
 *
 */

/**
 *
 * this class just provides an interface to read from a MapFile mostly takes care of reporting start and end keys
 *
 * need this because a single row extent can have multiple columns this manages all the columns (each handled by a store) for a single row-extent
 *
 *
 */

public class Tablet {

  enum MinorCompactionReason {
    USER, SYSTEM, CLOSE, RECOVERY
  }

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

    public boolean beginUpdatingLogsUsed(ArrayList<DfsLogger> copy, boolean mincFinish) {
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
      try {
        memTable = new InMemoryMap(acuTableConf);
      } catch (LocalityGroupConfigurationError e) {
        throw new RuntimeException(e);
      }
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
      try {
        memTable = new InMemoryMap(acuTableConf);
      } catch (LocalityGroupConfigurationError e) {
        throw new RuntimeException(e);
      }

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

  private final TabletTime tabletTime;
  private long persistedTime;
  private final Object timeLock = new Object();

  private final Path location; // absolute path of this tablets dir
  private TServerInstance lastLocation;

  private Configuration conf;
  private VolumeManager fs;

  private TableConfiguration acuTableConf;

  private volatile boolean tableDirChecked = false;

  private AtomicLong dataSourceDeletions = new AtomicLong(0);
  private Set<ScanDataSource> activeScans = new HashSet<ScanDataSource>();

  private volatile boolean closing = false;
  private boolean closed = false;
  private boolean closeComplete = false;

  private long lastFlushID = -1;
  private long lastCompactID = -1;

  private static class CompactionWaitInfo {
    long flushID = -1;
    long compactionID = -1;
  }

  // stores info about user initiated major compaction that is waiting on a minor compaction to finish
  private CompactionWaitInfo compactionWaitInfo = new CompactionWaitInfo();

  private KeyExtent extent;

  private TabletResourceManager tabletResources;
  final private DatafileManager datafileManager;
  private volatile boolean majorCompactionInProgress = false;
  private volatile boolean majorCompactionWaitingToStart = false;
  private Set<MajorCompactionReason> majorCompactionQueued = Collections.synchronizedSet(EnumSet.noneOf(MajorCompactionReason.class));
  private volatile boolean minorCompactionInProgress = false;
  private volatile boolean minorCompactionWaitingToStart = false;

  private boolean updatingFlushID = false;

  private AtomicReference<ConstraintChecker> constraintChecker = new AtomicReference<ConstraintChecker>();

  private final String tabletDirectory;

  private int writesInProgress = 0;

  private static final Logger log = Logger.getLogger(Tablet.class);
  public TabletStatsKeeper timer;

  private Rate queryRate = new Rate(0.95);
  private long queryCount = 0;

  private Rate queryByteRate = new Rate(0.95);
  private long queryBytes = 0;

  private Rate ingestRate = new Rate(0.95);
  private long ingestCount = 0;

  private Rate ingestByteRate = new Rate(0.95);
  private long ingestBytes = 0;

  private byte[] defaultSecurityLabel = new byte[0];

  private long lastMinorCompactionFinishTime;
  private long lastMapFileImportTime;

  private volatile long numEntries;
  private volatile long numEntriesInMemory;

  // a count of the amount of data read by the iterators
  private AtomicLong scannedCount = new AtomicLong(0);
  private Rate scannedRate = new Rate(0.95);

  private ConfigurationObserver configObserver;

  private TabletServer tabletServer;

  private final int logId;
  // ensure we only have one reader/writer of our bulk file notes at at time
  public final Object bulkFileImportLock = new Object();

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

  FileRef getNextMapFilename(String prefix) throws IOException {
    String extension = FileOperations.getNewFileExtension(tabletServer.getTableConfiguration(extent));
    checkTabletDir();
    return new FileRef(location.toString() + "/" + prefix + UniqueNameAllocator.getInstance().getNextName() + "." + extension);
  }

  private void checkTabletDir() throws IOException {
    if (!tableDirChecked) {
      checkTabletDir(this.location);
      tableDirChecked = true;
    }
  }

  private void checkTabletDir(Path tabletDir) throws IOException {

    FileStatus[] files = null;
    try {
      files = fs.listStatus(tabletDir);
    } catch (FileNotFoundException ex) {
      // ignored
    }

    if (files == null) {
      if (tabletDir.getName().startsWith(Constants.CLONE_PREFIX))
        log.debug("Tablet " + extent + " had no dir, creating " + tabletDir); // its a clone dir...
      else
        log.warn("Tablet " + extent + " had no dir, creating " + tabletDir);

      fs.mkdirs(tabletDir);
    }
  }

  class DatafileManager {
    // access to datafilesizes needs to be synchronized: see CompactionRunner#getNumFiles
    final private Map<FileRef,DataFileValue> datafileSizes = Collections.synchronizedMap(new TreeMap<FileRef,DataFileValue>());

    DatafileManager(SortedMap<FileRef,DataFileValue> datafileSizes) {
      for (Entry<FileRef,DataFileValue> datafiles : datafileSizes.entrySet())
        this.datafileSizes.put(datafiles.getKey(), datafiles.getValue());
    }

    FileRef mergingMinorCompactionFile = null;
    Set<FileRef> filesToDeleteAfterScan = new HashSet<FileRef>();
    Map<Long,Set<FileRef>> scanFileReservations = new HashMap<Long,Set<FileRef>>();
    MapCounter<FileRef> fileScanReferenceCounts = new MapCounter<FileRef>();
    long nextScanReservationId = 0;
    boolean reservationsBlocked = false;

    Set<FileRef> majorCompactingFiles = new HashSet<FileRef>();

    Pair<Long,Map<FileRef,DataFileValue>> reserveFilesForScan() {
      synchronized (Tablet.this) {

        while (reservationsBlocked) {
          try {
            Tablet.this.wait(50);
          } catch (InterruptedException e) {
            log.warn(e, e);
          }
        }

        Set<FileRef> absFilePaths = new HashSet<FileRef>(datafileSizes.keySet());

        long rid = nextScanReservationId++;

        scanFileReservations.put(rid, absFilePaths);

        Map<FileRef,DataFileValue> ret = new HashMap<FileRef,DataFileValue>();

        for (FileRef path : absFilePaths) {
          fileScanReferenceCounts.increment(path, 1);
          ret.put(path, datafileSizes.get(path));
        }

        return new Pair<Long,Map<FileRef,DataFileValue>>(rid, ret);
      }
    }

    void returnFilesForScan(Long reservationId) {

      final Set<FileRef> filesToDelete = new HashSet<FileRef>();

      synchronized (Tablet.this) {
        Set<FileRef> absFilePaths = scanFileReservations.remove(reservationId);

        if (absFilePaths == null)
          throw new IllegalArgumentException("Unknown scan reservation id " + reservationId);

        boolean notify = false;
        for (FileRef path : absFilePaths) {
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
        log.debug("Removing scan refs from metadata " + extent + " " + filesToDelete);
        MetadataTableUtil.removeScanFiles(extent, filesToDelete, SystemCredentials.get(), tabletServer.getLock());
      }
    }

    private void removeFilesAfterScan(Set<FileRef> scanFiles) {
      if (scanFiles.size() == 0)
        return;

      Set<FileRef> filesToDelete = new HashSet<FileRef>();

      synchronized (Tablet.this) {
        for (FileRef path : scanFiles) {
          if (fileScanReferenceCounts.get(path) == 0)
            filesToDelete.add(path);
          else
            filesToDeleteAfterScan.add(path);
        }
      }

      if (filesToDelete.size() > 0) {
        log.debug("Removing scan refs from metadata " + extent + " " + filesToDelete);
        MetadataTableUtil.removeScanFiles(extent, filesToDelete, SystemCredentials.get(), tabletServer.getLock());
      }
    }

    private TreeSet<FileRef> waitForScansToFinish(Set<FileRef> pathsToWaitFor, boolean blockNewScans, long maxWaitTime) {
      long startTime = System.currentTimeMillis();
      TreeSet<FileRef> inUse = new TreeSet<FileRef>();

      Span waitForScans = Trace.start("waitForScans");
      try {
        synchronized (Tablet.this) {
          if (blockNewScans) {
            if (reservationsBlocked)
              throw new IllegalStateException();

            reservationsBlocked = true;
          }

          for (FileRef path : pathsToWaitFor) {
            while (fileScanReferenceCounts.get(path) > 0 && System.currentTimeMillis() - startTime < maxWaitTime) {
              try {
                Tablet.this.wait(100);
              } catch (InterruptedException e) {
                log.warn(e, e);
              }
            }
          }

          for (FileRef path : pathsToWaitFor) {
            if (fileScanReferenceCounts.get(path) > 0)
              inUse.add(path);
          }

          if (blockNewScans) {
            reservationsBlocked = false;
            Tablet.this.notifyAll();
          }

        }
      } finally {
        waitForScans.stop();
      }
      return inUse;
    }

    public void importMapFiles(long tid, Map<FileRef,DataFileValue> pathsString, boolean setTime) throws IOException {

      String bulkDir = null;

      Map<FileRef,DataFileValue> paths = new HashMap<FileRef,DataFileValue>();
      for (Entry<FileRef,DataFileValue> entry : pathsString.entrySet())
        paths.put(entry.getKey(), entry.getValue());

      for (FileRef tpath : paths.keySet()) {

        boolean inTheRightDirectory = false;
        Path parent = tpath.path().getParent().getParent();
        for (String tablesDir : ServerConstants.getTablesDirs()) {
          if (parent.equals(new Path(tablesDir, extent.getTableId().toString()))) {
            inTheRightDirectory = true;
            break;
          }
        }
        if (!inTheRightDirectory) {
          throw new IOException("Data file " + tpath + " not in table dirs");
        }

        if (bulkDir == null)
          bulkDir = tpath.path().getParent().toString();
        else if (!bulkDir.equals(tpath.path().getParent().toString()))
          throw new IllegalArgumentException("bulk files in different dirs " + bulkDir + " " + tpath);

      }

      if (extent.isRootTablet()) {
        throw new IllegalArgumentException("Can not import files to root tablet");
      }

      synchronized (bulkFileImportLock) {
        Credentials creds = SystemCredentials.get();
        Connector conn;
        try {
          conn = HdfsZooInstance.getInstance().getConnector(creds.getPrincipal(), creds.getToken());
        } catch (Exception ex) {
          throw new IOException(ex);
        }
        // Remove any bulk files we've previously loaded and compacted away
        List<FileRef> files = MetadataTableUtil.getBulkFilesLoaded(conn, extent, tid);

        for (FileRef file : files)
          if (paths.keySet().remove(file))
            log.debug("Ignoring request to re-import a file already imported: " + extent + ": " + file);

        if (paths.size() > 0) {
          long bulkTime = Long.MIN_VALUE;
          if (setTime) {
            for (DataFileValue dfv : paths.values()) {
              long nextTime = tabletTime.getAndUpdateTime();
              if (nextTime < bulkTime)
                throw new IllegalStateException("Time went backwards unexpectedly " + nextTime + " " + bulkTime);
              bulkTime = nextTime;
              dfv.setTime(bulkTime);
            }
          }

          synchronized (timeLock) {
            if (bulkTime > persistedTime)
              persistedTime = bulkTime;

            MetadataTableUtil.updateTabletDataFile(tid, extent, paths, tabletTime.getMetadataValue(persistedTime), creds, tabletServer.getLock());
          }
        }
      }

      synchronized (Tablet.this) {
        for (Entry<FileRef,DataFileValue> tpath : paths.entrySet()) {
          if (datafileSizes.containsKey(tpath.getKey())) {
            log.error("Adding file that is already in set " + tpath.getKey());
          }
          datafileSizes.put(tpath.getKey(), tpath.getValue());

        }

        tabletResources.importedMapFiles();

        computeNumEntries();
      }

      for (Entry<FileRef,DataFileValue> entry : paths.entrySet()) {
        log.log(TLevel.TABLET_HIST, extent + " import " + entry.getKey() + " " + entry.getValue());
      }
    }

    FileRef reserveMergingMinorCompactionFile() {
      if (mergingMinorCompactionFile != null)
        throw new IllegalStateException("Tried to reserve merging minor compaction file when already reserved  : " + mergingMinorCompactionFile);

      if (extent.isRootTablet())
        return null;

      int maxFiles = acuTableConf.getMaxFilesPerTablet();

      // when a major compaction is running and we are at max files, write out
      // one extra file... want to avoid the case where major compaction is
      // compacting everything except for the largest file, and therefore the
      // largest file is returned for merging.. the following check mostly
      // avoids this case, except for the case where major compactions fail or
      // are canceled
      if (majorCompactingFiles.size() > 0 && datafileSizes.size() == maxFiles)
        return null;

      if (datafileSizes.size() >= maxFiles) {
        // find the smallest file

        long min = Long.MAX_VALUE;
        FileRef minName = null;

        for (Entry<FileRef,DataFileValue> entry : datafileSizes.entrySet()) {
          if (entry.getValue().getSize() < min && !majorCompactingFiles.contains(entry.getKey())) {
            min = entry.getValue().getSize();
            minName = entry.getKey();
          }
        }

        if (minName == null)
          return null;

        mergingMinorCompactionFile = minName;
        return minName;
      }

      return null;
    }

    void unreserveMergingMinorCompactionFile(FileRef file) {
      if ((file == null && mergingMinorCompactionFile != null) || (file != null && mergingMinorCompactionFile == null)
          || (file != null && mergingMinorCompactionFile != null && !file.equals(mergingMinorCompactionFile)))
        throw new IllegalStateException("Disagreement " + file + " " + mergingMinorCompactionFile);

      mergingMinorCompactionFile = null;
    }

    void bringMinorCompactionOnline(FileRef tmpDatafile, FileRef newDatafile, FileRef absMergeFile, DataFileValue dfv, CommitSession commitSession, long flushId)
        throws IOException {

      IZooReaderWriter zoo = ZooReaderWriter.getInstance();
      if (extent.isRootTablet()) {
        try {
          if (!zoo.isLockHeld(tabletServer.getLock().getLockID())) {
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
            fs.deleteRecursively(tmpDatafile.path());
          } else {
            if (fs.exists(newDatafile.path())) {
              log.warn("Target map file already exist " + newDatafile);
              fs.deleteRecursively(newDatafile.path());
            }

            rename(fs, tmpDatafile.path(), newDatafile.path());
          }
          break;
        } catch (IOException ioe) {
          log.warn("Tablet " + extent + " failed to rename " + newDatafile + " after MinC, will retry in 60 secs...", ioe);
          UtilWaitThread.sleep(60 * 1000);
        }
      } while (true);

      long t1, t2;

      // the code below always assumes merged files are in use by scans... this must be done
      // because the in memory list of files is not updated until after the metadata table
      // therefore the file is available to scans until memory is updated, but want to ensure
      // the file is not available for garbage collection... if memory were updated
      // before this point (like major compactions do), then the following code could wait
      // for scans to finish like major compactions do.... used to wait for scans to finish
      // here, but that was incorrect because a scan could start after waiting but before
      // memory was updated... assuming the file is always in use by scans leads to
      // one uneeded metadata update when it was not actually in use
      Set<FileRef> filesInUseByScans = Collections.emptySet();
      if (absMergeFile != null)
        filesInUseByScans = Collections.singleton(absMergeFile);

      // very important to write delete entries outside of log lock, because
      // this metadata write does not go up... it goes sideways or to itself
      if (absMergeFile != null)
        MetadataTableUtil.addDeleteEntries(extent, Collections.singleton(absMergeFile), SystemCredentials.get());

      Set<String> unusedWalLogs = beginClearingUnusedLogs();
      try {
        // the order of writing to metadata and walog is important in the face of machine/process failures
        // need to write to metadata before writing to walog, when things are done in the reverse order
        // data could be lost... the minor compaction start even should be written before the following metadata
        // write is made

        synchronized (timeLock) {
          if (commitSession.getMaxCommittedTime() > persistedTime)
            persistedTime = commitSession.getMaxCommittedTime();

          String time = tabletTime.getMetadataValue(persistedTime);
          MasterMetadataUtil.updateTabletDataFile(extent, newDatafile, absMergeFile, dfv, time, SystemCredentials.get(), filesInUseByScans,
              tabletServer.getClientAddressString(), tabletServer.getLock(), unusedWalLogs, lastLocation, flushId);
        }

      } finally {
        finishClearingUnusedLogs();
      }

      do {
        try {
          // the purpose of making this update use the new commit session, instead of the old one passed in,
          // is because the new one will reference the logs used by current memory...

          tabletServer.minorCompactionFinished(tabletMemory.getCommitSession(), newDatafile.toString(), commitSession.getWALogSeq() + 2);
          break;
        } catch (IOException e) {
          log.error("Failed to write to write-ahead log " + e.getMessage() + " will retry", e);
          UtilWaitThread.sleep(1 * 1000);
        }
      } while (true);

      synchronized (Tablet.this) {
        lastLocation = null;

        t1 = System.currentTimeMillis();
        if (datafileSizes.containsKey(newDatafile)) {
          log.error("Adding file that is already in set " + newDatafile);
        }

        if (dfv.getNumEntries() > 0) {
          datafileSizes.put(newDatafile, dfv);
        }

        if (absMergeFile != null) {
          datafileSizes.remove(absMergeFile);
        }

        unreserveMergingMinorCompactionFile(absMergeFile);

        dataSourceDeletions.incrementAndGet();
        tabletMemory.finishedMinC();

        lastFlushID = flushId;

        computeNumEntries();
        t2 = System.currentTimeMillis();
      }

      // must do this after list of files in memory is updated above
      removeFilesAfterScan(filesInUseByScans);

      if (absMergeFile != null)
        log.log(TLevel.TABLET_HIST, extent + " MinC [" + absMergeFile + ",memory] -> " + newDatafile);
      else
        log.log(TLevel.TABLET_HIST, extent + " MinC [memory] -> " + newDatafile);
      log.debug(String.format("MinC finish lock %.2f secs %s", (t2 - t1) / 1000.0, getExtent().toString()));
      if (dfv.getSize() > acuTableConf.getMemoryInBytes(Property.TABLE_SPLIT_THRESHOLD)) {
        log.debug(String.format("Minor Compaction wrote out file larger than split threshold.  split threshold = %,d  file size = %,d",
            acuTableConf.getMemoryInBytes(Property.TABLE_SPLIT_THRESHOLD), dfv.getSize()));
      }

    }

    public void reserveMajorCompactingFiles(Collection<FileRef> files) {
      if (majorCompactingFiles.size() != 0)
        throw new IllegalStateException("Major compacting files not empty " + majorCompactingFiles);

      if (mergingMinorCompactionFile != null && files.contains(mergingMinorCompactionFile))
        throw new IllegalStateException("Major compaction tried to resrve file in use by minor compaction " + mergingMinorCompactionFile);

      majorCompactingFiles.addAll(files);
    }

    public void clearMajorCompactingFile() {
      majorCompactingFiles.clear();
    }

    void bringMajorCompactionOnline(Set<FileRef> oldDatafiles, FileRef tmpDatafile, FileRef newDatafile, Long compactionId, DataFileValue dfv)
        throws IOException {
      long t1, t2;

      if (!extent.isRootTablet()) {

        if (fs.exists(newDatafile.path())) {
          log.error("Target map file already exist " + newDatafile, new Exception());
          throw new IllegalStateException("Target map file already exist " + newDatafile);
        }

        // rename before putting in metadata table, so files in metadata table should
        // always exist
        rename(fs, tmpDatafile.path(), newDatafile.path());

        if (dfv.getNumEntries() == 0) {
          fs.deleteRecursively(newDatafile.path());
        }
      }

      TServerInstance lastLocation = null;
      synchronized (Tablet.this) {

        t1 = System.currentTimeMillis();

        IZooReaderWriter zoo = ZooReaderWriter.getInstance();

        dataSourceDeletions.incrementAndGet();

        if (extent.isRootTablet()) {

          waitForScansToFinish(oldDatafiles, true, Long.MAX_VALUE);

          try {
            if (!zoo.isLockHeld(tabletServer.getLock().getLockID())) {
              throw new IllegalStateException();
            }
          } catch (Exception e) {
            throw new IllegalStateException("Can not bring major compaction online, lock not held", e);
          }

          // mark files as ready for deletion, but
          // do not delete them until we successfully
          // rename the compacted map file, in case
          // the system goes down

          RootFiles.replaceFiles(acuTableConf, fs, location, oldDatafiles, tmpDatafile, newDatafile);
        }

        // atomically remove old files and add new file
        for (FileRef oldDatafile : oldDatafiles) {
          if (!datafileSizes.containsKey(oldDatafile)) {
            log.error("file does not exist in set " + oldDatafile);
          }
          datafileSizes.remove(oldDatafile);
          majorCompactingFiles.remove(oldDatafile);
        }

        if (datafileSizes.containsKey(newDatafile)) {
          log.error("Adding file that is already in set " + newDatafile);
        }

        if (dfv.getNumEntries() > 0) {
          datafileSizes.put(newDatafile, dfv);
        }

        // could be used by a follow on compaction in a multipass compaction
        majorCompactingFiles.add(newDatafile);

        computeNumEntries();

        lastLocation = Tablet.this.lastLocation;
        Tablet.this.lastLocation = null;

        if (compactionId != null)
          lastCompactID = compactionId;

        t2 = System.currentTimeMillis();
      }

      if (!extent.isRootTablet()) {
        Set<FileRef> filesInUseByScans = waitForScansToFinish(oldDatafiles, false, 10000);
        if (filesInUseByScans.size() > 0)
          log.debug("Adding scan refs to metadata " + extent + " " + filesInUseByScans);
        MasterMetadataUtil.replaceDatafiles(extent, oldDatafiles, filesInUseByScans, newDatafile, compactionId, dfv, SystemCredentials.get(),
            tabletServer.getClientAddressString(), lastLocation, tabletServer.getLock());
        removeFilesAfterScan(filesInUseByScans);
      }

      log.debug(String.format("MajC finish lock %.2f secs", (t2 - t1) / 1000.0));
      log.log(TLevel.TABLET_HIST, extent + " MajC " + oldDatafiles + " --> " + newDatafile);
    }

    public SortedMap<FileRef,DataFileValue> getDatafileSizes() {
      synchronized (Tablet.this) {
        TreeMap<FileRef,DataFileValue> copy = new TreeMap<FileRef,DataFileValue>(datafileSizes);
        return Collections.unmodifiableSortedMap(copy);
      }
    }

    public Set<FileRef> getFiles() {
      synchronized (Tablet.this) {
        HashSet<FileRef> files = new HashSet<FileRef>(datafileSizes.keySet());
        return Collections.unmodifiableSet(files);
      }
    }

  }

  public Tablet(TabletServer tabletServer, Text location, KeyExtent extent, TabletResourceManager trm, SortedMap<Key,Value> tabletsKeyValues)
      throws IOException {
    this(tabletServer, location, extent, trm, CachedConfiguration.getInstance(), tabletsKeyValues);
    splitCreationTime = 0;
  }

  public Tablet(KeyExtent extent, TabletServer tabletServer, TabletResourceManager trm, SplitInfo info) throws IOException {
    this(tabletServer, new Text(info.dir), extent, trm, CachedConfiguration.getInstance(), info.datafiles, info.time, info.initFlushID, info.initCompactID,
        info.lastLocation);
    splitCreationTime = System.currentTimeMillis();
  }

  /**
   * Only visibile for testing
   */
  @VisibleForTesting
  protected Tablet(TabletTime tabletTime, String tabletDirectory, int logId, Path location, DatafileManager datafileManager) {
    this.tabletTime = tabletTime;
    this.tabletDirectory = tabletDirectory;
    this.logId = logId;
    this.location = location;
    this.datafileManager = datafileManager;
  }

  private Tablet(TabletServer tabletServer, Text location, KeyExtent extent, TabletResourceManager trm, Configuration conf,
      SortedMap<Key,Value> tabletsKeyValues) throws IOException {
    this(tabletServer, location, extent, trm, conf, VolumeManagerImpl.get(), tabletsKeyValues);
  }

  static private final List<LogEntry> EMPTY = Collections.emptyList();

  private Tablet(TabletServer tabletServer, Text location, KeyExtent extent, TabletResourceManager trm, Configuration conf,
      SortedMap<FileRef,DataFileValue> datafiles, String time, long initFlushID, long initCompactID, TServerInstance last) throws IOException {
    this(tabletServer, location, extent, trm, conf, VolumeManagerImpl.get(), EMPTY, datafiles, time, last, new HashSet<FileRef>(), initFlushID, initCompactID);
  }

  private static String lookupTime(AccumuloConfiguration conf, KeyExtent extent, SortedMap<Key,Value> tabletsKeyValues) {
    SortedMap<Key,Value> entries;

    if (extent.isRootTablet()) {
      return null;
    } else {
      entries = new TreeMap<Key,Value>();
      Text rowName = extent.getMetadataEntry();
      for (Entry<Key,Value> entry : tabletsKeyValues.entrySet()) {
        if (entry.getKey().compareRow(rowName) == 0 && TabletsSection.ServerColumnFamily.TIME_COLUMN.hasColumns(entry.getKey())) {
          entries.put(new Key(entry.getKey()), new Value(entry.getValue()));
        }
      }
    }

    // log.debug("extent : "+extent+"   entries : "+entries);

    if (entries.size() == 1)
      return entries.values().iterator().next().toString();
    return null;
  }

  private static SortedMap<FileRef,DataFileValue> lookupDatafiles(AccumuloConfiguration conf, VolumeManager fs, KeyExtent extent,
      SortedMap<Key,Value> tabletsKeyValues) throws IOException {

    TreeMap<FileRef,DataFileValue> datafiles = new TreeMap<FileRef,DataFileValue>();

    if (extent.isRootTablet()) { // the meta0 tablet
      Path location = new Path(MetadataTableUtil.getRootTabletDir());

      // cleanUpFiles() has special handling for delete. files
      FileStatus[] files = fs.listStatus(location);
      Collection<String> goodPaths = RootFiles.cleanupReplacement(fs, files, true);
      for (String good : goodPaths) {
        Path path = new Path(good);
        String filename = path.getName();
        FileRef ref = new FileRef(location.toString() + "/" + filename, path);
        DataFileValue dfv = new DataFileValue(0, 0);
        datafiles.put(ref, dfv);
      }
    } else {

      Text rowName = extent.getMetadataEntry();

      String tableId = extent.isMeta() ? RootTable.ID : MetadataTable.ID;
      ScannerImpl mdScanner = new ScannerImpl(HdfsZooInstance.getInstance(), SystemCredentials.get(), tableId, Authorizations.EMPTY);

      // Commented out because when no data file is present, each tablet will scan through metadata table and return nothing
      // reduced batch size to improve performance
      // changed here after endKeys were implemented from 10 to 1000
      mdScanner.setBatchSize(1000);

      // leave these in, again, now using endKey for safety
      mdScanner.fetchColumnFamily(DataFileColumnFamily.NAME);

      mdScanner.setRange(new Range(rowName));

      for (Entry<Key,Value> entry : mdScanner) {

        if (entry.getKey().compareRow(rowName) != 0) {
          break;
        }

        FileRef ref = new FileRef(fs, entry.getKey());
        datafiles.put(ref, new DataFileValue(entry.getValue().get()));
      }
    }
    return datafiles;
  }

  private static List<LogEntry> lookupLogEntries(KeyExtent ke, SortedMap<Key,Value> tabletsKeyValues) {
    List<LogEntry> logEntries = new ArrayList<LogEntry>();

    if (ke.isMeta()) {
      try {
        logEntries = MetadataTableUtil.getLogEntries(SystemCredentials.get(), ke);
      } catch (Exception ex) {
        throw new RuntimeException("Unable to read tablet log entries", ex);
      }
    } else {
      log.debug("Looking at metadata " + tabletsKeyValues);
      Text row = ke.getMetadataEntry();
      for (Entry<Key,Value> entry : tabletsKeyValues.entrySet()) {
        Key key = entry.getKey();
        if (key.getRow().equals(row)) {
          if (key.getColumnFamily().equals(LogColumnFamily.NAME)) {
            logEntries.add(LogEntry.fromKeyValue(key, entry.getValue()));
          }
        }
      }
    }

    log.debug("got " + logEntries + " for logs for " + ke);
    return logEntries;
  }

  private static Set<FileRef> lookupScanFiles(KeyExtent extent, SortedMap<Key,Value> tabletsKeyValues, VolumeManager fs) throws IOException {
    HashSet<FileRef> scanFiles = new HashSet<FileRef>();

    Text row = extent.getMetadataEntry();
    for (Entry<Key,Value> entry : tabletsKeyValues.entrySet()) {
      Key key = entry.getKey();
      if (key.getRow().equals(row) && key.getColumnFamily().equals(ScanFileColumnFamily.NAME)) {
        scanFiles.add(new FileRef(fs, key));
      }
    }

    return scanFiles;
  }

  private static long lookupFlushID(KeyExtent extent, SortedMap<Key,Value> tabletsKeyValues) {
    Text row = extent.getMetadataEntry();
    for (Entry<Key,Value> entry : tabletsKeyValues.entrySet()) {
      Key key = entry.getKey();
      if (key.getRow().equals(row) && TabletsSection.ServerColumnFamily.FLUSH_COLUMN.equals(key.getColumnFamily(), key.getColumnQualifier()))
        return Long.parseLong(entry.getValue().toString());
    }

    return -1;
  }

  private static long lookupCompactID(KeyExtent extent, SortedMap<Key,Value> tabletsKeyValues) {
    Text row = extent.getMetadataEntry();
    for (Entry<Key,Value> entry : tabletsKeyValues.entrySet()) {
      Key key = entry.getKey();
      if (key.getRow().equals(row) && TabletsSection.ServerColumnFamily.COMPACT_COLUMN.equals(key.getColumnFamily(), key.getColumnQualifier()))
        return Long.parseLong(entry.getValue().toString());
    }

    return -1;
  }

  private Tablet(TabletServer tabletServer, Text location, KeyExtent extent, TabletResourceManager trm, Configuration conf, VolumeManager fs,
      SortedMap<Key,Value> tabletsKeyValues) throws IOException {
    this(tabletServer, location, extent, trm, conf, fs, lookupLogEntries(extent, tabletsKeyValues), lookupDatafiles(tabletServer.getSystemConfiguration(), fs,
        extent, tabletsKeyValues), lookupTime(tabletServer.getSystemConfiguration(), extent, tabletsKeyValues), lookupLastServer(extent, tabletsKeyValues),
        lookupScanFiles(extent, tabletsKeyValues, fs), lookupFlushID(extent, tabletsKeyValues), lookupCompactID(extent, tabletsKeyValues));
  }

  private static TServerInstance lookupLastServer(KeyExtent extent, SortedMap<Key,Value> tabletsKeyValues) {
    for (Entry<Key,Value> entry : tabletsKeyValues.entrySet()) {
      if (entry.getKey().getColumnFamily().compareTo(TabletsSection.LastLocationColumnFamily.NAME) == 0) {
        return new TServerInstance(entry.getValue(), entry.getKey().getColumnQualifier());
      }
    }
    return null;
  }

  /**
   * yet another constructor - this one allows us to avoid costly lookups into the Metadata table if we already know the files we need - as at split time
   */
  private Tablet(final TabletServer tabletServer, final Text location, final KeyExtent extent, final TabletResourceManager trm, final Configuration conf,
      final VolumeManager fs, final List<LogEntry> rawLogEntries, final SortedMap<FileRef,DataFileValue> rawDatafiles, String time,
      final TServerInstance lastLocation, Set<FileRef> scanFiles, long initFlushID, long initCompactID) throws IOException {

    TabletFiles tabletPaths = VolumeUtil.updateTabletVolumes(tabletServer.getLock(), fs, extent, new TabletFiles(location.toString(), rawLogEntries,
        rawDatafiles));

    Path locationPath;

    if (tabletPaths.dir.contains(":")) {
      locationPath = new Path(tabletPaths.dir.toString());
    } else {
      locationPath = fs.getFullPath(FileType.TABLE, extent.getTableId().toString() + tabletPaths.dir.toString());
    }

    final List<LogEntry> logEntries = tabletPaths.logEntries;
    final SortedMap<FileRef,DataFileValue> datafiles = tabletPaths.datafiles;

    this.location = locationPath;
    this.lastLocation = lastLocation;
    this.tabletDirectory = tabletPaths.dir;
    this.conf = conf;
    this.acuTableConf = tabletServer.getTableConfiguration(extent);

    this.fs = fs;
    this.extent = extent;
    this.tabletResources = trm;

    this.lastFlushID = initFlushID;
    this.lastCompactID = initCompactID;

    if (extent.isRootTablet()) {
      long rtime = Long.MIN_VALUE;
      for (FileRef ref : datafiles.keySet()) {
        Path path = ref.path();
        FileSystem ns = fs.getVolumeByPath(path).getFileSystem();
        FileSKVIterator reader = FileOperations.getInstance().openReader(path.toString(), true, ns, ns.getConf(), tabletServer.getTableConfiguration(extent));
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
    if (time == null && datafiles.isEmpty() && extent.equals(RootTable.OLD_EXTENT)) {
      // recovery... old root tablet has no data, so time doesn't matter:
      time = TabletTime.LOGICAL_TIME_ID + "" + Long.MIN_VALUE;
    }

    this.tabletServer = tabletServer;
    this.logId = tabletServer.createLogId(extent);

    this.timer = new TabletStatsKeeper();

    setupDefaultSecurityLabels(extent);

    tabletMemory = new TabletMemory();
    tabletTime = TabletTime.getInstance(time);
    persistedTime = tabletTime.getTime();

    acuTableConf.addObserver(configObserver = new ConfigurationObserver() {

      private void reloadConstraints() {
        constraintChecker.set(new ConstraintChecker(acuTableConf));
      }

      @Override
      public void propertiesChanged() {
        reloadConstraints();

        try {
          setupDefaultSecurityLabels(extent);
        } catch (Exception e) {
          log.error("Failed to reload default security labels for extent: " + extent.toString());
        }
      }

      @Override
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

      @Override
      public void sessionExpired() {
        log.debug("Session expired, no longer updating per table props...");
      }

    });

    acuTableConf.getNamespaceConfiguration().addObserver(configObserver);

    // Force a load of any per-table properties
    configObserver.propertiesChanged();

    tabletResources.setTablet(this, acuTableConf);
    if (!logEntries.isEmpty()) {
      log.info("Starting Write-Ahead Log recovery for " + this.extent);
      // count[0] = entries used on tablet
      // count[1] = track max time from walog entries wihtout timestamps
      final long[] count = new long[2];
      final CommitSession commitSession = tabletMemory.getCommitSession();
      count[1] = Long.MIN_VALUE;
      try {
        Set<String> absPaths = new HashSet<String>();
        for (FileRef ref : datafiles.keySet())
          absPaths.add(ref.path().toString());

        tabletServer.recover(this.tabletServer.getFileSystem(), this, logEntries, absPaths, new MutationReceiver() {
          @Override
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
        }
        commitSession.updateMaxCommittedTime(tabletTime.getTime());

        if (count[0] == 0) {
          log.debug("No replayed mutations applied, removing unused entries for " + extent);
          MetadataTableUtil.removeUnusedWALEntries(extent, logEntries, tabletServer.getLock());
          logEntries.clear();
        }

      } catch (Throwable t) {
        if (acuTableConf.getBoolean(Property.TABLE_FAILURES_IGNORE)) {
          log.warn("Error recovering from log files: ", t);
        } else {
          throw new RuntimeException(t);
        }
      }
      // make some closed references that represent the recovered logs
      currentLogs = new HashSet<DfsLogger>();
      for (LogEntry logEntry : logEntries) {
        for (String log : logEntry.logSet) {
          currentLogs.add(new DfsLogger(tabletServer.getServerConfig(), log, logEntry.getColumnQualifier().toString()));
        }
      }

      log.info("Write-Ahead Log recovery complete for " + this.extent + " (" + count[0] + " mutations applied, " + tabletMemory.getNumEntries()
          + " entries created)");
    }

    String contextName = acuTableConf.get(Property.TABLE_CLASSPATH);
    if (contextName != null && !contextName.equals("")) {
      // initialize context classloader, instead of possibly waiting for it to initialize for a scan
      // TODO this could hang, causing other tablets to fail to load - ACCUMULO-1292
      AccumuloVFSClassLoader.getContextManager().getClassLoader(contextName);
    }

    // do this last after tablet is completely setup because it
    // could cause major compaction to start
    datafileManager = new DatafileManager(datafiles);

    computeNumEntries();

    datafileManager.removeFilesAfterScan(scanFiles);

    // look for hints of a failure on the previous tablet server
    if (!logEntries.isEmpty() || needsMajorCompaction(MajorCompactionReason.NORMAL)) {
      // look for any temp files hanging around
      removeOldTemporaryFiles();
    }

    log.log(TLevel.TABLET_HIST, extent + " opened");
  }

  private void removeOldTemporaryFiles() {
    // remove any temporary files created by a previous tablet server
    try {
      for (FileStatus tmp : fs.globStatus(new Path(location, "*_tmp"))) {
        try {
          log.debug("Removing old temp file " + tmp.getPath());
          fs.delete(tmp.getPath());
        } catch (IOException ex) {
          log.error("Unable to remove old temp file " + tmp.getPath() + ": " + ex);
        }
      }
    } catch (IOException ex) {
      log.error("Error scanning for old temp files in " + location);
    }
  }

  private void setupDefaultSecurityLabels(KeyExtent extent) {
    if (extent.isMeta()) {
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

  public static class KVEntry extends KeyValue {
    private static final long serialVersionUID = 1L;

    public KVEntry(Key k, Value v) {
      super(new Key(k), Arrays.copyOf(v.get(), v.get().length));
    }

    int numBytes() {
      return getKey().getSize() + getValue().get().length;
    }

    int estimateMemoryUsed() {
      return getKey().getSize() + getValue().get().length + (9 * 32); // overhead is 32 per object
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
          lookupResult.dataSize += kve.numBytes();

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
      addUnfinishedRange(lookupResult, range, results.get(results.size() - 1).getKey(), false);
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
    long dataSize = 0;
    boolean closed = false;
  }

  public LookupResult lookup(List<Range> ranges, HashSet<Column> columns, Authorizations authorizations, ArrayList<KVEntry> results, long maxResultSize,
      List<IterInfo> ssiList, Map<String,Map<String,String>> ssio, AtomicBoolean interruptFlag) throws IOException {

    if (ranges.size() == 0) {
      return new LookupResult();
    }

    ranges = Range.mergeOverlapping(ranges);
    if (ranges.size() > 1) {
      Collections.sort(ranges);
    }

    Range tabletRange = extent.toDataRange();
    for (Range range : ranges) {
      // do a test to see if this range falls within the tablet, if it does not
      // then clip will throw an exception
      tabletRange.clip(range);
    }

    ScanDataSource dataSource = new ScanDataSource(authorizations, this.defaultSecurityLabel, columns, ssiList, ssio, interruptFlag);

    LookupResult result = null;

    try {
      SortedKeyValueIterator<Key,Value> iter = new SourceSwitchingIterator(dataSource);
      result = lookup(iter, ranges, columns, results, maxResultSize);
      return result;
    } catch (IOException ioe) {
      dataSource.close(true);
      throw ioe;
    } finally {
      // code in finally block because always want
      // to return mapfiles, even when exception is thrown
      dataSource.close(false);

      synchronized (this) {
        queryCount += results.size();
        if (result != null)
          queryBytes += result.dataSize;
      }
    }
  }

  private Batch nextBatch(SortedKeyValueIterator<Key,Value> iter, Range range, int num, Set<Column> columns) throws IOException {

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

      value = iter.getTopValue();
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

  Scanner createScanner(Range range, int num, Set<Column> columns, Authorizations authorizations, List<IterInfo> ssiList, Map<String,Map<String,String>> ssio,
      boolean isolated, AtomicBoolean interruptFlag) {
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
    Set<Column> columnSet;
    List<IterInfo> ssiList;
    Map<String,Map<String,String>> ssio;
    AtomicBoolean interruptFlag;
    int num;
    boolean isolated;

    ScanOptions(int num, Authorizations authorizations, byte[] defaultLabels, Set<Column> columnSet, List<IterInfo> ssiList,
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
    private StatsIterator statsIterator;

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

      Map<FileRef,DataFileValue> files;

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
        Pair<Long,Map<FileRef,DataFileValue>> reservation = datafileManager.reserveFilesForScan();
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

      TabletIteratorEnvironment iterEnv = new TabletIteratorEnvironment(IteratorScope.scan, acuTableConf, fileManager, files);

      statsIterator = new StatsIterator(multiIter, TabletServer.seekCount, scannedCount);

      DeletingIterator delIter = new DeletingIterator(statsIterator, false);

      ColumnFamilySkippingIterator cfsi = new ColumnFamilySkippingIterator(delIter);

      ColumnQualifierFilter colFilter = new ColumnQualifierFilter(cfsi, options.columnSet);

      VisibilityFilter visFilter = new VisibilityFilter(colFilter, options.authorizations, options.defaultLabels);

      return iterEnv.getTopLevelIterator(IteratorUtil
          .loadIterators(IteratorScope.scan, visFilter, extent, acuTableConf, options.ssiList, options.ssio, iterEnv));
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

      if (statsIterator != null) {
        statsIterator.report();
      }

    }

    public void interrupt() {
      interruptFlag.set(true);
    }

    @Override
    public DataSource getDeepCopyDataSource(IteratorEnvironment env) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void setInterruptFlag(AtomicBoolean flag) {
      throw new UnsupportedOperationException();
    }

  }

  private DataFileValue minorCompact(Configuration conf, VolumeManager fs, InMemoryMap memTable, FileRef tmpDatafile, FileRef newDatafile, FileRef mergeFile,
      boolean hasQueueTime, long queued, CommitSession commitSession, long flushId, MinorCompactionReason mincReason) {
    boolean failed = false;
    long start = System.currentTimeMillis();
    timer.incrementStatusMinor();

    long count = 0;

    try {
      Span span = Trace.start("write");
      CompactionStats stats;
      try {
        count = memTable.getNumEntries();

        DataFileValue dfv = null;
        if (mergeFile != null)
          dfv = datafileManager.getDatafileSizes().get(mergeFile);

        MinorCompactor compactor = new MinorCompactor(conf, fs, memTable, mergeFile, dfv, tmpDatafile, acuTableConf, extent, mincReason);
        stats = compactor.call();
      } finally {
        span.stop();
      }
      span = Trace.start("bringOnline");
      try {
        datafileManager.bringMinorCompactionOnline(tmpDatafile, newDatafile, mergeFile, new DataFileValue(stats.getFileSize(), stats.getEntriesWritten()),
            commitSession, flushId);
      } finally {
        span.stop();
      }
      return new DataFileValue(stats.getFileSize(), stats.getEntriesWritten());
    } catch (Exception E) {
      failed = true;
      throw new RuntimeException(E);
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
    private CommitSession commitSession;
    private DataFileValue stats;
    private FileRef mergeFile;
    private long flushId;
    private MinorCompactionReason mincReason;

    MinorCompactionTask(FileRef mergeFile, CommitSession commitSession, long flushId, MinorCompactionReason mincReason) {
      queued = System.currentTimeMillis();
      minorCompactionWaitingToStart = true;
      this.commitSession = commitSession;
      this.mergeFile = mergeFile;
      this.flushId = flushId;
      this.mincReason = mincReason;
    }

    @Override
    public void run() {
      minorCompactionWaitingToStart = false;
      minorCompactionInProgress = true;
      Span minorCompaction = Trace.on("minorCompaction");
      try {
        FileRef newMapfileLocation = getNextMapFilename(mergeFile == null ? "F" : "M");
        FileRef tmpFileRef = new FileRef(newMapfileLocation.path() + "_tmp");
        Span span = Trace.start("waitForCommits");
        synchronized (Tablet.this) {
          commitSession.waitForCommitsToFinish();
        }
        span.stop();
        span = Trace.start("start");
        while (true) {
          try {
            // the purpose of the minor compaction start event is to keep track of the filename... in the case
            // where the metadata table write for the minor compaction finishes and the process dies before
            // writing the minor compaction finish event, then the start event+filename in metadata table will
            // prevent recovery of duplicate data... the minor compaction start event could be written at any time
            // before the metadata write for the minor compaction
            tabletServer.minorCompactionStarted(commitSession, commitSession.getWALogSeq() + 1, newMapfileLocation.path().toString());
            break;
          } catch (IOException e) {
            log.warn("Failed to write to write ahead log " + e.getMessage(), e);
          }
        }
        span.stop();
        span = Trace.start("compact");
        this.stats = minorCompact(conf, fs, tabletMemory.getMinCMemTable(), tmpFileRef, newMapfileLocation, mergeFile, true, queued, commitSession, flushId,
            mincReason);
        span.stop();

        if (needsSplit()) {
          tabletServer.executeSplit(Tablet.this);
        } else {
          initiateMajorCompaction(MajorCompactionReason.NORMAL);
        }
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

  private synchronized MinorCompactionTask prepareForMinC(long flushId, MinorCompactionReason mincReason) {
    CommitSession oldCommitSession = tabletMemory.prepareForMinC();
    otherLogs = currentLogs;
    currentLogs = new HashSet<DfsLogger>();

    FileRef mergeFile = null;
    if (mincReason != MinorCompactionReason.RECOVERY) {
      mergeFile = datafileManager.reserveMergingMinorCompactionFile();
    }

    return new MinorCompactionTask(mergeFile, oldCommitSession, flushId, mincReason);

  }

  void flush(long tableFlushID) {
    boolean updateMetadata = false;
    boolean initiateMinor = false;

    try {

      synchronized (this) {

        // only want one thing at a time to update flush ID to ensure that metadata table and tablet in memory state are consistent
        if (updatingFlushID)
          return;

        if (lastFlushID >= tableFlushID)
          return;

        if (closing || closed || tabletMemory.memoryReservedForMinC())
          return;

        if (tabletMemory.getMemTable().getNumEntries() == 0) {
          lastFlushID = tableFlushID;
          updatingFlushID = true;
          updateMetadata = true;
        } else
          initiateMinor = true;
      }

      if (updateMetadata) {
        Credentials creds = SystemCredentials.get();
        // if multiple threads were allowed to update this outside of a sync block, then it would be
        // a race condition
        MetadataTableUtil.updateTabletFlushID(extent, tableFlushID, creds, tabletServer.getLock());
      } else if (initiateMinor)
        initiateMinorCompaction(tableFlushID, MinorCompactionReason.USER);

    } finally {
      if (updateMetadata) {
        synchronized (this) {
          updatingFlushID = false;
          this.notifyAll();
        }
      }
    }

  }

  boolean initiateMinorCompaction(MinorCompactionReason mincReason) {
    if (isClosed()) {
      // don't bother trying to get flush id if closed... could be closed after this check but that is ok... just trying to cut down on uneeded log messages....
      return false;
    }

    // get the flush id before the new memmap is made available for write
    long flushId;
    try {
      flushId = getFlushID();
    } catch (NoNodeException e) {
      log.info("Asked to initiate MinC when there was no flush id " + getExtent() + " " + e.getMessage());
      return false;
    }
    return initiateMinorCompaction(flushId, mincReason);
  }

  boolean minorCompactNow(MinorCompactionReason mincReason) {
    long flushId;
    try {
      flushId = getFlushID();
    } catch (NoNodeException e) {
      log.info("Asked to initiate MinC when there was no flush id " + getExtent() + " " + e.getMessage());
      return false;
    }
    MinorCompactionTask mct = createMinorCompactionTask(flushId, mincReason);
    if (mct == null)
      return false;
    mct.run();
    return true;
  }

  boolean initiateMinorCompaction(long flushId, MinorCompactionReason mincReason) {
    MinorCompactionTask mct = createMinorCompactionTask(flushId, mincReason);
    if (mct == null)
      return false;
    tabletResources.executeMinorCompaction(mct);
    return true;
  }

  private MinorCompactionTask createMinorCompactionTask(long flushId, MinorCompactionReason mincReason) {
    MinorCompactionTask mct;
    long t1, t2;

    StringBuilder logMessage = null;

    try {
      synchronized (this) {
        t1 = System.currentTimeMillis();

        if (closing || closed || majorCompactionWaitingToStart || tabletMemory.memoryReservedForMinC() || tabletMemory.getMemTable().getNumEntries() == 0
            || updatingFlushID) {

          logMessage = new StringBuilder();

          logMessage.append(extent.toString());
          logMessage.append(" closing " + closing);
          logMessage.append(" closed " + closed);
          logMessage.append(" majorCompactionWaitingToStart " + majorCompactionWaitingToStart);
          if (tabletMemory != null)
            logMessage.append(" tabletMemory.memoryReservedForMinC() " + tabletMemory.memoryReservedForMinC());
          if (tabletMemory != null && tabletMemory.getMemTable() != null)
            logMessage.append(" tabletMemory.getMemTable().getNumEntries() " + tabletMemory.getMemTable().getNumEntries());
          logMessage.append(" updatingFlushID " + updatingFlushID);

          return null;
        }

        mct = prepareForMinC(flushId, mincReason);
        t2 = System.currentTimeMillis();
      }
    } finally {
      // log outside of sync block
      if (logMessage != null && log.isDebugEnabled())
        log.debug(logMessage);
    }

    log.debug(String.format("MinC initiate lock %.2f secs", (t2 - t1) / 1000.0));
    return mct;
  }

  long getFlushID() throws NoNodeException {
    try {
      String zTablePath = Constants.ZROOT + "/" + HdfsZooInstance.getInstance().getInstanceID() + Constants.ZTABLES + "/" + extent.getTableId()
          + Constants.ZTABLE_FLUSH_ID;
      return Long.parseLong(new String(ZooReaderWriter.getInstance().getData(zTablePath, null), UTF_8));
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    } catch (NumberFormatException nfe) {
      throw new RuntimeException(nfe);
    } catch (KeeperException ke) {
      if (ke instanceof NoNodeException) {
        throw (NoNodeException) ke;
      } else {
        throw new RuntimeException(ke);
      }
    }
  }

  long getCompactionCancelID() {
    String zTablePath = Constants.ZROOT + "/" + HdfsZooInstance.getInstance().getInstanceID() + Constants.ZTABLES + "/" + extent.getTableId()
        + Constants.ZTABLE_COMPACT_CANCEL_ID;

    try {
      return Long.parseLong(new String(ZooReaderWriter.getInstance().getData(zTablePath, null), UTF_8));
    } catch (KeeperException e) {
      throw new RuntimeException(e);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  Pair<Long,List<IteratorSetting>> getCompactionID() throws NoNodeException {
    try {
      String zTablePath = Constants.ZROOT + "/" + HdfsZooInstance.getInstance().getInstanceID() + Constants.ZTABLES + "/" + extent.getTableId()
          + Constants.ZTABLE_COMPACT_ID;

      String[] tokens = new String(ZooReaderWriter.getInstance().getData(zTablePath, null), UTF_8).split(",");
      long compactID = Long.parseLong(tokens[0]);

      CompactionIterators iters = new CompactionIterators();

      if (tokens.length > 1) {
        Hex hex = new Hex();
        ByteArrayInputStream bais = new ByteArrayInputStream(hex.decode(tokens[1].split("=")[1].getBytes(UTF_8)));
        DataInputStream dis = new DataInputStream(bais);

        try {
          iters.readFields(dis);
        } catch (IOException e) {
          throw new RuntimeException(e);
        }

        KeyExtent ke = new KeyExtent(extent.getTableId(), iters.getEndRow(), iters.getStartRow());

        if (!ke.overlaps(extent)) {
          // only use iterators if compaction range overlaps
          iters = new CompactionIterators();
        }
      }

      return new Pair<Long,List<IteratorSetting>>(compactID, iters.getIterators());
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    } catch (NumberFormatException nfe) {
      throw new RuntimeException(nfe);
    } catch (KeeperException ke) {
      if (ke instanceof NoNodeException) {
        throw (NoNodeException) ke;
      } else {
        throw new RuntimeException(ke);
      }
    } catch (DecoderException e) {
      throw new RuntimeException(e);
    }
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

  public void checkConstraints() {
    ConstraintChecker cc = constraintChecker.get();

    if (cc.classLoaderChanged()) {
      ConstraintChecker ncc = new ConstraintChecker(acuTableConf);
      constraintChecker.compareAndSet(cc, ncc);
    }
  }

  public CommitSession prepareMutationsForCommit(TservConstraintEnv cenv, List<Mutation> mutations) throws TConstraintViolationException {

    ConstraintChecker cc = constraintChecker.get();

    List<Mutation> violators = null;
    Violations violations = new Violations();
    cenv.setExtent(extent);
    for (Mutation mutation : mutations) {
      Violations more = cc.check(cenv, mutation);
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

      while (updatingFlushID) {
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

      try {
        mct = prepareForMinC(getFlushID(), MinorCompactionReason.CLOSE);
      } catch (NoNodeException e) {
        throw new RuntimeException(e);
      }

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
      try {
        prepareForMinC(getFlushID(), MinorCompactionReason.CLOSE).run();
      } catch (NoNodeException e) {
        throw new RuntimeException(e);
      }
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
      if (err != null) {
        ProblemReports.getInstance().report(new ProblemReport(extent.getTableId().toString(), ProblemType.TABLET_LOAD, this.extent.toString(), err));
        log.error("Tablet closed consistency check has failed for " + this.extent + " giving up and closing");
      }
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

    acuTableConf.getNamespaceConfiguration().removeObserver(configObserver);
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
      Pair<List<LogEntry>,SortedMap<FileRef,DataFileValue>> fileLog = MetadataTableUtil.getFileAndLogEntries(SystemCredentials.get(), extent);

      if (fileLog.getFirst().size() != 0) {
        String msg = "Closed tablet " + extent + " has walog entries in " + MetadataTable.NAME + " " + fileLog.getFirst();
        log.error(msg);
        throw new RuntimeException(msg);
      }

      if (extent.isRootTablet()) {
        if (!fileLog.getSecond().keySet().equals(datafileManager.getDatafileSizes().keySet())) {
          String msg = "Data file in " + RootTable.NAME + " differ from in memory data " + extent + "  " + fileLog.getSecond().keySet() + "  "
              + datafileManager.getDatafileSizes().keySet();
          log.error(msg);
          throw new RuntimeException(msg);
        }
      } else {
        if (!fileLog.getSecond().equals(datafileManager.getDatafileSizes())) {
          String msg = "Data file in " + MetadataTable.NAME + " differ from in memory data " + extent + "  " + fileLog.getSecond() + "  "
              + datafileManager.getDatafileSizes();
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

    // TODO check lastFlushID and lostCompactID - ACCUMULO-1290
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
    private MajorCompactionReason reason;

    public CompactionRunner(MajorCompactionReason reason) {
      queued = System.currentTimeMillis();
      this.reason = reason;
    }

    @Override
    public void run() {
      CompactionStats majCStats = null;

      if (tabletServer.isMajorCompactionDisabled()) {
        // this will make compaction task that were queued when shutdown was
        // initiated exit
        majorCompactionQueued.remove(reason);
        return;
      }

      try {
        timer.incrementStatusMajor();
        start = System.currentTimeMillis();
        majCStats = majorCompact(reason);

        // if there is more work to be done, queue another major compaction
        synchronized (Tablet.this) {
          if (reason == MajorCompactionReason.NORMAL && needsMajorCompaction(reason))
            initiateMajorCompaction(reason);
        }
      } catch (CompactionCanceledException cce) {
        log.debug("Major compaction canceled, extent = " + getExtent());
        failed = true;
      } catch (IOException ioe) {
        log.error("MajC Failed, extent = " + getExtent(), ioe);
        failed = true;
      } catch (RuntimeException e) {
        log.error("MajC Unexpected exception, extent = " + getExtent(), e);
        failed = true;
      } finally {
        long count = 0;
        if (majCStats != null) {
          count = majCStats.getEntriesRead();
        }

        timer.updateTime(Operation.MAJOR, queued, start, count, failed);
      }
    }

    // We used to synchronize on the Tablet before fetching this information,
    // but this method is called by the compaction queue thread to re-order the compactions.
    // The compaction queue holds a lock during this sort.
    // A tablet lock can be held while putting itself on the queue, so we can't lock the tablet
    // while pulling information used to sort the tablets in the queue, or we may get deadlocked.
    // See ACCUMULO-1110.
    private int getNumFiles() {
      return datafileManager.datafileSizes.size();
    }

    @Override
    public int compareTo(CompactionRunner o) {
      int cmp = reason.compareTo(o.reason);
      if (cmp != 0)
        return cmp;

      if (reason == MajorCompactionReason.USER || reason == MajorCompactionReason.CHOP) {
        // for these types of compactions want to do the oldest first
        cmp = (int) (queued - o.queued);
        if (cmp != 0)
          return cmp;
      }

      return o.getNumFiles() - this.getNumFiles();
    }
  }

  synchronized boolean initiateMajorCompaction(MajorCompactionReason reason) {

    if (closing || closed || !needsMajorCompaction(reason) || majorCompactionInProgress || majorCompactionQueued.contains(reason)) {
      return false;
    }

    majorCompactionQueued.add(reason);

    tabletResources.executeMajorCompaction(getExtent(), new CompactionRunner(reason));

    return false;
  }

  /**
   * Returns true if a major compaction should be performed on the tablet.
   *
   */
  public boolean needsMajorCompaction(MajorCompactionReason reason) {
    if (majorCompactionInProgress)
      return false;
    if (reason == MajorCompactionReason.CHOP || reason == MajorCompactionReason.USER)
      return true;
    return tabletResources.needsMajorCompaction(datafileManager.getDatafileSizes(), reason);
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

  private SplitRowSpec findSplitRow(Collection<FileRef> files) {

    // never split the root tablet
    // check if we already decided that we can never split
    // check to see if we're big enough to split

    long splitThreshold = acuTableConf.getMemoryInBytes(Property.TABLE_SPLIT_THRESHOLD);
    if (extent.isRootTablet() || estimateTabletSize() <= splitThreshold) {
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
      keys = FileUtil.findMidPoint(fs, tabletServer.getSystemConfiguration(), extent.getPrevEndRow(), extent.getEndRow(), FileUtil.toPathStrings(files), .25);
    } catch (IOException e) {
      log.error("Failed to find midpoint " + e.getMessage());
      return null;
    }

    // check to see if one row takes up most of the tablet, in which case we can not split
    try {

      Text lastRow;
      if (extent.getEndRow() == null) {
        Key lastKey = (Key) FileUtil.findLastKey(fs, tabletServer.getSystemConfiguration(), files);
        lastRow = lastKey.getRow();
      } else {
        lastRow = extent.getEndRow();
      }

      // We expect to get a midPoint for this set of files. If we don't get one, we have a problem.
      final Key mid = keys.get(.5);
      if (null == mid) {
        throw new IllegalStateException("Could not determine midpoint for files");
      }

      // check to see that the midPoint is not equal to the end key
      if (mid.compareRow(lastRow) == 0) {
        if (keys.firstKey() < .5) {
          Key candidate = keys.get(keys.firstKey());
          if (candidate.compareRow(lastRow) != 0) {
            // we should use this ratio in split size estimations
            if (log.isTraceEnabled())
              log.trace(String.format("Splitting at %6.2f instead of .5, row at .5 is same as end row%n", keys.firstKey()));
            return new SplitRowSpec(keys.firstKey(), candidate.getRow());
          }

        }

        log.warn("Cannot split tablet " + extent + " it contains a big row : " + lastRow);

        sawBigRow = true;
        timeOfLastMinCWhenBigFreakinRowWasSeen = lastMinorCompactionFinishTime;
        timeOfLastImportWhenBigFreakinRowWasSeen = lastMapFileImportTime;

        return null;
      }
      Text text = mid.getRow();
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

  private Map<FileRef,Pair<Key,Key>> getFirstAndLastKeys(SortedMap<FileRef,DataFileValue> allFiles) throws IOException {
    Map<FileRef,Pair<Key,Key>> result = new HashMap<FileRef,Pair<Key,Key>>();
    FileOperations fileFactory = FileOperations.getInstance();
    for (Entry<FileRef,DataFileValue> entry : allFiles.entrySet()) {
      FileRef file = entry.getKey();
      FileSystem ns = fs.getVolumeByPath(file.path()).getFileSystem();
      FileSKVIterator openReader = fileFactory.openReader(file.path().toString(), true, ns, ns.getConf(), this.getTableConfiguration());
      try {
        Key first = openReader.getFirstKey();
        Key last = openReader.getLastKey();
        result.put(file, new Pair<Key,Key>(first, last));
      } finally {
        openReader.close();
      }
    }
    return result;
  }

  List<FileRef> findChopFiles(KeyExtent extent, Map<FileRef,Pair<Key,Key>> firstAndLastKeys, Collection<FileRef> allFiles) throws IOException {
    List<FileRef> result = new ArrayList<FileRef>();
    if (firstAndLastKeys == null) {
      result.addAll(allFiles);
      return result;
    }

    for (FileRef file : allFiles) {
      Pair<Key,Key> pair = firstAndLastKeys.get(file);
      if (pair == null) {
        // file was created or imported after we obtained the first and last keys... there
        // are a few options here... throw an exception which will cause the compaction to
        // retry and also cause ugly error message that the admin has to ignore... could
        // go get the first and last key, but this code is called while the tablet lock
        // is held... or just compact the file....
        result.add(file);
      } else {
        Key first = pair.getFirst();
        Key last = pair.getSecond();
        // If first and last are null, it's an empty file. Add it to the compact set so it goes away.
        if ((first == null && last == null) || !extent.contains(first.getRow()) || !extent.contains(last.getRow())) {
          result.add(file);
        }
      }
    }
    return result;
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
      ret = findSplitRow(datafileManager.getFiles()) != null;

    return ret;
  }

  // BEGIN PRIVATE METHODS RELATED TO MAJOR COMPACTION

  private boolean isCompactionEnabled() {
    return !closing && !tabletServer.isMajorCompactionDisabled();
  }

  private CompactionStats _majorCompact(MajorCompactionReason reason) throws IOException, CompactionCanceledException {

    long t1, t2, t3;

    // acquire file info outside of tablet lock
    CompactionStrategy strategy = Property.createTableInstanceFromPropertyName(acuTableConf, Property.TABLE_COMPACTION_STRATEGY, CompactionStrategy.class,
        new DefaultCompactionStrategy());
    strategy.init(Property.getCompactionStrategyOptions(acuTableConf));

    Map<FileRef,Pair<Key,Key>> firstAndLastKeys = null;
    if (reason == MajorCompactionReason.CHOP) {
      firstAndLastKeys = getFirstAndLastKeys(datafileManager.getDatafileSizes());
    } else if (reason != MajorCompactionReason.USER) {
      MajorCompactionRequest request = new MajorCompactionRequest(extent, reason, fs, acuTableConf);
      request.setFiles(datafileManager.getDatafileSizes());
      strategy.gatherInformation(request);
    }

    Map<FileRef,DataFileValue> filesToCompact;

    int maxFilesToCompact = acuTableConf.getCount(Property.TSERV_MAJC_THREAD_MAXOPEN);

    CompactionStats majCStats = new CompactionStats();
    CompactionPlan plan = null;

    boolean propogateDeletes = false;

    synchronized (this) {
      // plan all that work that needs to be done in the sync block... then do the actual work
      // outside the sync block

      t1 = System.currentTimeMillis();

      majorCompactionWaitingToStart = true;

      tabletMemory.waitForMinC();

      t2 = System.currentTimeMillis();

      majorCompactionWaitingToStart = false;
      notifyAll();

      if (extent.isRootTablet()) {
        // very important that we call this before doing major compaction,
        // otherwise deleted compacted files could possible be brought back
        // at some point if the file they were compacted to was legitimately
        // removed by a major compaction
        RootFiles.cleanupReplacement(fs, fs.listStatus(this.location), false);
      }
      SortedMap<FileRef,DataFileValue> allFiles = datafileManager.getDatafileSizes();
      List<FileRef> inputFiles = new ArrayList<FileRef>();
      if (reason == MajorCompactionReason.CHOP) {
        // enforce rules: files with keys outside our range need to be compacted
        inputFiles.addAll(findChopFiles(extent, firstAndLastKeys, allFiles.keySet()));
      } else if (reason == MajorCompactionReason.USER) {
        inputFiles.addAll(allFiles.keySet());
      } else {
        MajorCompactionRequest request = new MajorCompactionRequest(extent, reason, fs, acuTableConf);
        request.setFiles(allFiles);
        plan = strategy.getCompactionPlan(request);
        if (plan != null) {
          plan.validate(allFiles.keySet());
          inputFiles.addAll(plan.inputFiles);
        }
      }

      if (inputFiles.isEmpty()) {
        return majCStats;
      }
      // If no original files will exist at the end of the compaction, we do not have to propogate deletes
      Set<FileRef> droppedFiles = new HashSet<FileRef>();
      droppedFiles.addAll(inputFiles);
      if (plan != null)
        droppedFiles.addAll(plan.deleteFiles);
      propogateDeletes = !(droppedFiles.equals(allFiles.keySet()));
      log.debug("Major compaction plan: " + plan + " propogate deletes : " + propogateDeletes);
      filesToCompact = new HashMap<FileRef,DataFileValue>(allFiles);
      filesToCompact.keySet().retainAll(inputFiles);

      t3 = System.currentTimeMillis();

      datafileManager.reserveMajorCompactingFiles(filesToCompact.keySet());
    }

    try {

      log.debug(String.format("MajC initiate lock %.2f secs, wait %.2f secs", (t3 - t2) / 1000.0, (t2 - t1) / 1000.0));

      Pair<Long,List<IteratorSetting>> compactionId = null;
      if (!propogateDeletes) {
        // compacting everything, so update the compaction id in metadata
        try {
          compactionId = getCompactionID();
        } catch (NoNodeException e) {
          throw new RuntimeException(e);
        }
      }

      List<IteratorSetting> compactionIterators = new ArrayList<IteratorSetting>();
      if (compactionId != null) {
        if (reason == MajorCompactionReason.USER) {
          if (getCompactionCancelID() >= compactionId.getFirst()) {
            // compaction was canceled
            return majCStats;
          }

          synchronized (this) {
            if (lastCompactID >= compactionId.getFirst())
              // already compacted
              return majCStats;
          }
        }

        compactionIterators = compactionId.getSecond();
      }

      // need to handle case where only one file is being major compacted
      while (filesToCompact.size() > 0) {

        int numToCompact = maxFilesToCompact;

        if (filesToCompact.size() > maxFilesToCompact && filesToCompact.size() < 2 * maxFilesToCompact) {
          // on the second to last compaction pass, compact the minimum amount of files possible
          numToCompact = filesToCompact.size() - maxFilesToCompact + 1;
        }

        Set<FileRef> smallestFiles = removeSmallest(filesToCompact, numToCompact);

        FileRef fileName = getNextMapFilename((filesToCompact.size() == 0 && !propogateDeletes) ? "A" : "C");
        FileRef compactTmpName = new FileRef(fileName.path().toString() + "_tmp");

        AccumuloConfiguration tableConf = createTableConfiguration(acuTableConf, plan);

        Span span = Trace.start("compactFiles");
        try {

          CompactionEnv cenv = new CompactionEnv() {
            @Override
            public boolean isCompactionEnabled() {
              return Tablet.this.isCompactionEnabled();
            }

            @Override
            public IteratorScope getIteratorScope() {
              return IteratorScope.majc;
            }
          };

          HashMap<FileRef,DataFileValue> copy = new HashMap<FileRef,DataFileValue>(datafileManager.getDatafileSizes());
          if (!copy.keySet().containsAll(smallestFiles))
            throw new IllegalStateException("Cannot find data file values for " + smallestFiles);

          copy.keySet().retainAll(smallestFiles);

          log.debug("Starting MajC " + extent + " (" + reason + ") " + copy.keySet() + " --> " + compactTmpName + "  " + compactionIterators);

          // always propagate deletes, unless last batch
          boolean lastBatch = filesToCompact.isEmpty();
          Compactor compactor = new Compactor(conf, fs, copy, null, compactTmpName, lastBatch ? propogateDeletes : true, tableConf, extent, cenv,
              compactionIterators, reason);

          CompactionStats mcs = compactor.call();

          span.data("files", "" + smallestFiles.size());
          span.data("read", "" + mcs.getEntriesRead());
          span.data("written", "" + mcs.getEntriesWritten());
          majCStats.add(mcs);

          if (lastBatch && plan != null && plan.deleteFiles != null) {
            smallestFiles.addAll(plan.deleteFiles);
          }
          datafileManager.bringMajorCompactionOnline(smallestFiles, compactTmpName, fileName,
              filesToCompact.size() == 0 && compactionId != null ? compactionId.getFirst() : null,
              new DataFileValue(mcs.getFileSize(), mcs.getEntriesWritten()));

          // when major compaction produces a file w/ zero entries, it will be deleted... do not want
          // to add the deleted file
          if (filesToCompact.size() > 0 && mcs.getEntriesWritten() > 0) {
            filesToCompact.put(fileName, new DataFileValue(mcs.getFileSize(), mcs.getEntriesWritten()));
          }
        } finally {
          span.stop();
        }

      }
      return majCStats;
    } finally {
      synchronized (Tablet.this) {
        datafileManager.clearMajorCompactingFile();
      }
    }
  }

  protected AccumuloConfiguration createTableConfiguration(TableConfiguration base, CompactionPlan plan) {
    if (plan == null || plan.writeParameters == null)
      return base;
    WriteParameters p = plan.writeParameters;
    ConfigurationCopy result = new ConfigurationCopy(base);
    if (p.getHdfsBlockSize() > 0)
      result.set(Property.TABLE_FILE_BLOCK_SIZE, "" + p.getHdfsBlockSize());
    if (p.getBlockSize() > 0)
      result.set(Property.TABLE_FILE_COMPRESSED_BLOCK_SIZE, "" + p.getBlockSize());
    if (p.getIndexBlockSize() > 0)
      result.set(Property.TABLE_FILE_COMPRESSED_BLOCK_SIZE_INDEX, "" + p.getIndexBlockSize());
    if (p.getCompressType() != null)
      result.set(Property.TABLE_FILE_COMPRESSION_TYPE, p.getCompressType());
    if (p.getReplication() != 0)
      result.set(Property.TABLE_FILE_REPLICATION, "" + p.getReplication());
    return result;
  }

  private Set<FileRef> removeSmallest(Map<FileRef,DataFileValue> filesToCompact, int maxFilesToCompact) {
    // ensure this method works properly when multiple files have the same size

    PriorityQueue<Pair<FileRef,Long>> fileHeap = new PriorityQueue<Pair<FileRef,Long>>(filesToCompact.size(), new Comparator<Pair<FileRef,Long>>() {
      @Override
      public int compare(Pair<FileRef,Long> o1, Pair<FileRef,Long> o2) {
        if (o1.getSecond() == o2.getSecond())
          return o1.getFirst().compareTo(o2.getFirst());
        if (o1.getSecond() < o2.getSecond())
          return -1;
        return 1;
      }
    });

    for (Iterator<Entry<FileRef,DataFileValue>> iterator = filesToCompact.entrySet().iterator(); iterator.hasNext();) {
      Entry<FileRef,DataFileValue> entry = iterator.next();
      fileHeap.add(new Pair<FileRef,Long>(entry.getKey(), entry.getValue().getSize()));
    }

    Set<FileRef> smallestFiles = new HashSet<FileRef>();
    while (smallestFiles.size() < maxFilesToCompact && fileHeap.size() > 0) {
      Pair<FileRef,Long> pair = fileHeap.remove();
      filesToCompact.remove(pair.getFirst());
      smallestFiles.add(pair.getFirst());
    }

    return smallestFiles;
  }

  static void rename(VolumeManager fs, Path src, Path dst) throws IOException {
    if (!fs.rename(src, dst)) {
      throw new IOException("Rename " + src + " to " + dst + " returned false ");
    }
  }

  // END PRIVATE METHODS RELATED TO MAJOR COMPACTION

  /**
   * Performs a major compaction on the tablet. If needsSplit() returns true, the tablet is split and a reference to the new tablet is returned.
   */

  private CompactionStats majorCompact(MajorCompactionReason reason) throws IOException, CompactionCanceledException {
    CompactionStats majCStats = null;

    synchronized (this) {
      // check that compaction is still needed - defer to splitting
      majorCompactionQueued.remove(reason);

      if (closing || closed || !needsMajorCompaction(reason) || majorCompactionInProgress || needsSplit()) {
        return null;
      }

      majorCompactionInProgress = true;
    }

    Span span = null;

    try {
      // Always trace majC
      span = Trace.on("majorCompaction");

      majCStats = _majorCompact(reason);
      if (reason == MajorCompactionReason.CHOP) {
        MetadataTableUtil.chopped(getExtent(), this.tabletServer.getLock());
        tabletServer.enqueueMasterMessage(new TabletStatusMessage(TabletLoadState.CHOPPED, extent));
      }
    } finally {
      // ensure we always reset boolean, even
      // when an exception is thrown
      synchronized (this) {
        majorCompactionInProgress = false;
        this.notifyAll();
      }

      if(span != null){
        span.data("extent", "" + getExtent());
        if (majCStats != null) {
          span.data("read", "" + majCStats.getEntriesRead());
          span.data("written", "" + majCStats.getEntriesWritten());
        }
        span.stop();
      }
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
    return majorCompactionQueued.size() > 0;
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
    SortedMap<FileRef,DataFileValue> datafiles;
    String time;
    long initFlushID;
    long initCompactID;
    TServerInstance lastLocation;

    SplitInfo(String d, SortedMap<FileRef,DataFileValue> dfv, String time, long initFlushID, long initCompactID, TServerInstance lastLocation) {
      this.dir = d;
      this.datafiles = dfv;
      this.time = time;
      this.initFlushID = initFlushID;
      this.initCompactID = initCompactID;
      this.lastLocation = lastLocation;
    }

  }

  public TreeMap<KeyExtent,SplitInfo> split(byte[] sp) throws IOException {

    if (sp != null && extent.getEndRow() != null && extent.getEndRow().equals(new Text(sp))) {
      throw new IllegalArgumentException();
    }

    if (extent.isRootTablet()) {
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
    Map<FileRef,FileUtil.FileInfo> firstAndLastRows = FileUtil.tryToGetFirstAndLastRows(fs, tabletServer.getSystemConfiguration(), datafileManager.getFiles());

    synchronized (this) {
      // java needs tuples ...
      TreeMap<KeyExtent,SplitInfo> newTablets = new TreeMap<KeyExtent,SplitInfo>();

      long t1 = System.currentTimeMillis();

      // choose a split point
      SplitRowSpec splitPoint;
      if (sp == null)
        splitPoint = findSplitRow(datafileManager.getFiles());
      else {
        Text tsp = new Text(sp);
        splitPoint = new SplitRowSpec(FileUtil.estimatePercentageLTE(fs, tabletServer.getSystemConfiguration(), extent.getPrevEndRow(), extent.getEndRow(),
            FileUtil.toPathStrings(datafileManager.getFiles()), tsp), tsp);
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

      String lowDirectory = TabletOperations.createTabletDirectory(fs, extent.getTableId().toString(), midRow);

      // write new tablet information to MetadataTable
      SortedMap<FileRef,DataFileValue> lowDatafileSizes = new TreeMap<FileRef,DataFileValue>();
      SortedMap<FileRef,DataFileValue> highDatafileSizes = new TreeMap<FileRef,DataFileValue>();
      List<FileRef> highDatafilesToRemove = new ArrayList<FileRef>();

      MetadataTableUtil.splitDatafiles(extent.getTableId(), midRow, splitRatio, firstAndLastRows, datafileManager.getDatafileSizes(), lowDatafileSizes,
          highDatafileSizes, highDatafilesToRemove);

      log.debug("Files for low split " + low + "  " + lowDatafileSizes.keySet());
      log.debug("Files for high split " + high + "  " + highDatafileSizes.keySet());

      String time = tabletTime.getMetadataValue();

      // it is possible that some of the bulk loading flags will be deleted after being read below because the bulk load
      // finishes.... therefore split could propagate load flags for a finished bulk load... there is a special iterator
      // on the metadata table to clean up this type of garbage
      Map<FileRef,Long> bulkLoadedFiles = MetadataTableUtil.getBulkFilesLoaded(SystemCredentials.get(), extent);

      MetadataTableUtil.splitTablet(high, extent.getPrevEndRow(), splitRatio, SystemCredentials.get(), tabletServer.getLock());
      MasterMetadataUtil.addNewTablet(low, lowDirectory, tabletServer.getTabletSession(), lowDatafileSizes, bulkLoadedFiles, SystemCredentials.get(), time,
          lastFlushID, lastCompactID, tabletServer.getLock());
      MetadataTableUtil.finishSplit(high, highDatafileSizes, highDatafilesToRemove, SystemCredentials.get(), tabletServer.getLock());

      log.log(TLevel.TABLET_HIST, extent + " split " + low + " " + high);

      newTablets.put(high, new SplitInfo(tabletDirectory, highDatafileSizes, time, lastFlushID, lastCompactID, this.lastLocation));
      newTablets.put(low, new SplitInfo(lowDirectory, lowDatafileSizes, time, lastFlushID, lastCompactID, this.lastLocation));

      long t2 = System.currentTimeMillis();

      log.debug(String.format("offline split time : %6.2f secs", (t2 - t1) / 1000.0));

      closeComplete = true;

      return newTablets;
    }
  }

  public SortedMap<FileRef,DataFileValue> getDatafiles() {
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

  public double scanRate() {
    return scannedRate.rate();
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
    scannedRate.update(now, scannedCount.get());
  }

  public long getSplitCreationTime() {
    return splitCreationTime;
  }

  public void importMapFiles(long tid, Map<FileRef,MapFileInfo> fileMap, boolean setTime) throws IOException {
    Map<FileRef,DataFileValue> entries = new HashMap<FileRef,DataFileValue>(fileMap.size());

    for (Entry<FileRef,MapFileInfo> entry : fileMap.entrySet()) {
      entries.put(entry.getKey(), new DataFileValue(entry.getValue().estimatedSize, 0l));
    }

    // Clients timeout and will think that this operation failed.
    // Don't do it if we spent too long waiting for the lock
    long now = System.currentTimeMillis();
    synchronized (this) {
      if (closed) {
        throw new IOException("tablet " + extent + " is closed");
      }

      // TODO check seems uneeded now - ACCUMULO-1291
      long lockWait = System.currentTimeMillis() - now;
      if (lockWait > tabletServer.getSystemConfiguration().getTimeInMillis(Property.GENERAL_RPC_TIMEOUT)) {
        throw new IOException("Timeout waiting " + (lockWait / 1000.) + " seconds to get tablet lock");
      }

      if (writesInProgress < 0)
        throw new IllegalStateException("writesInProgress < 0 " + writesInProgress);

      writesInProgress++;
    }

    try {
      datafileManager.importMapFiles(tid, entries, setTime);
      lastMapFileImportTime = System.currentTimeMillis();

      if (needsSplit()) {
        tabletServer.executeSplit(this);
      } else {
        initiateMajorCompaction(MajorCompactionReason.NORMAL);
      }
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

  private Set<DfsLogger> currentLogs = new HashSet<DfsLogger>();

  public Set<String> getCurrentLogFiles() {
    Set<String> result = new HashSet<String>();
    synchronized (currentLogs) {
      for (DfsLogger log : currentLogs) {
        result.add(log.getFileName());
      }
    }
    return result;
  }

  private Set<String> beginClearingUnusedLogs() {
    Set<String> doomed = new HashSet<String>();

    ArrayList<String> otherLogsCopy = new ArrayList<String>();
    ArrayList<String> currentLogsCopy = new ArrayList<String>();

    // do not hold tablet lock while acquiring the log lock
    logLock.lock();

    synchronized (this) {
      if (removingLogs)
        throw new IllegalStateException("Attempted to clear logs when removal of logs in progress");

      for (DfsLogger logger : otherLogs) {
        otherLogsCopy.add(logger.toString());
        doomed.add(logger.getMeta());
      }

      for (DfsLogger logger : currentLogs) {
        currentLogsCopy.add(logger.toString());
        doomed.remove(logger.getMeta());
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

    for (String logger : doomed) {
      log.debug("Logs to be destroyed: " + getExtent() + " " + logger);
    }

    return doomed;
  }

  private synchronized void finishClearingUnusedLogs() {
    removingLogs = false;
    logLock.unlock();
  }

  private Set<DfsLogger> otherLogs = Collections.emptySet();
  private boolean removingLogs = false;

  // this lock is basically used to synchronize writing of log info to metadata
  private final ReentrantLock logLock = new ReentrantLock();

  public synchronized int getLogCount() {
    return currentLogs.size();
  }

  private boolean beginUpdatingLogsUsed(InMemoryMap memTable, Collection<DfsLogger> more, boolean mincFinish) {

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

          // when writing a minc finish event, there is no need to add the log to metadata
          // if nothing has been logged for the tablet since the minor compaction started
          if (currentLogs.size() == 0)
            return false;
        }

        int numAdded = 0;
        int numContained = 0;
        for (DfsLogger logger : more) {
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

  synchronized public void chopFiles() {
    initiateMajorCompaction(MajorCompactionReason.CHOP);
  }

  public void compactAll(long compactionId) {
    boolean updateMetadata = false;

    synchronized (this) {
      if (lastCompactID >= compactionId)
        return;

      if (minorCompactionInProgress) {
        // want to wait for running minc to finish before starting majc, see ACCUMULO-3041
        if (compactionWaitInfo.compactionID == compactionId) {
          if (lastFlushID == compactionWaitInfo.flushID)
            return;
        } else {
          compactionWaitInfo.compactionID = compactionId;
          compactionWaitInfo.flushID = lastFlushID;
          return;
        }
      }

      if (closing || closed || majorCompactionQueued.contains(MajorCompactionReason.USER) || majorCompactionInProgress)
        return;

      if (datafileManager.getDatafileSizes().size() == 0) {
        // no files, so jsut update the metadata table
        majorCompactionInProgress = true;
        updateMetadata = true;
        lastCompactID = compactionId;
      } else
        initiateMajorCompaction(MajorCompactionReason.USER);
    }

    if (updateMetadata) {
      try {
        // if multiple threads were allowed to update this outside of a sync block, then it would be
        // a race condition
        MetadataTableUtil.updateTabletCompactID(extent, compactionId, SystemCredentials.get(), tabletServer.getLock());
      } finally {
        synchronized (this) {
          majorCompactionInProgress = false;
          this.notifyAll();
        }
      }
    }
  }

  public TableConfiguration getTableConfiguration() {
    return acuTableConf;
  }
}

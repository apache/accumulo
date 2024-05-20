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
package org.apache.accumulo.tserver.tablet;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.stream.Collectors.toList;
import static org.apache.accumulo.core.util.UtilWaitThread.sleepUninterruptibly;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.ref.SoftReference;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.Durability;
import org.apache.accumulo.core.client.admin.CompactionConfig;
import org.apache.accumulo.core.clientImpl.DurabilityImpl;
import org.apache.accumulo.core.clientImpl.UserCompactionUtils;
import org.apache.accumulo.core.conf.AccumuloConfiguration.Deriver;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.constraints.Violations;
import org.apache.accumulo.core.data.ColumnUpdate;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.dataImpl.thrift.MapFileInfo;
import org.apache.accumulo.core.file.FileOperations;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iteratorsImpl.system.SourceSwitchingIterator;
import org.apache.accumulo.core.logging.TabletLogger;
import org.apache.accumulo.core.manager.state.tables.TableState;
import org.apache.accumulo.core.master.thrift.BulkImportState;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.metadata.StoredTabletFile;
import org.apache.accumulo.core.metadata.TabletFile;
import org.apache.accumulo.core.metadata.schema.DataFileValue;
import org.apache.accumulo.core.metadata.schema.ExternalCompactionId;
import org.apache.accumulo.core.metadata.schema.ExternalCompactionMetadata;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.ServerColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataTime;
import org.apache.accumulo.core.metadata.schema.TabletMetadata;
import org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType;
import org.apache.accumulo.core.metadata.schema.TabletMetadata.Location;
import org.apache.accumulo.core.protobuf.ProtobufUtil;
import org.apache.accumulo.core.sample.impl.SamplerConfigurationImpl;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.spi.fs.VolumeChooserEnvironment;
import org.apache.accumulo.core.spi.scan.ScanDispatch;
import org.apache.accumulo.core.tabletserver.log.LogEntry;
import org.apache.accumulo.core.tabletserver.thrift.TabletStats;
import org.apache.accumulo.core.trace.TraceUtil;
import org.apache.accumulo.core.util.Pair;
import org.apache.accumulo.core.volume.Volume;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.compaction.CompactionStats;
import org.apache.accumulo.server.fs.VolumeChooserEnvironmentImpl;
import org.apache.accumulo.server.fs.VolumeUtil;
import org.apache.accumulo.server.fs.VolumeUtil.TabletFiles;
import org.apache.accumulo.server.problems.ProblemReport;
import org.apache.accumulo.server.problems.ProblemReports;
import org.apache.accumulo.server.problems.ProblemType;
import org.apache.accumulo.server.replication.proto.Replication.Status;
import org.apache.accumulo.server.tablets.TabletTime;
import org.apache.accumulo.server.tablets.UniqueNameAllocator;
import org.apache.accumulo.server.util.FileUtil;
import org.apache.accumulo.server.util.ManagerMetadataUtil;
import org.apache.accumulo.server.util.MetadataTableUtil;
import org.apache.accumulo.server.util.ReplicationTableUtil;
import org.apache.accumulo.tserver.ConditionCheckerContext.ConditionChecker;
import org.apache.accumulo.tserver.InMemoryMap;
import org.apache.accumulo.tserver.MinorCompactionReason;
import org.apache.accumulo.tserver.TabletServer;
import org.apache.accumulo.tserver.TabletServerResourceManager.TabletResourceManager;
import org.apache.accumulo.tserver.TabletStatsKeeper;
import org.apache.accumulo.tserver.TabletStatsKeeper.Operation;
import org.apache.accumulo.tserver.TservConstraintEnv;
import org.apache.accumulo.tserver.compactions.Compactable;
import org.apache.accumulo.tserver.constraints.ConstraintChecker;
import org.apache.accumulo.tserver.log.DfsLogger;
import org.apache.accumulo.tserver.metrics.TabletServerMinCMetrics;
import org.apache.accumulo.tserver.metrics.TabletServerScanMetrics;
import org.apache.accumulo.tserver.scan.ScanParameters;
import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.context.Scope;

/**
 * Provide access to a single row range in a living TabletServer.
 */
public class Tablet extends TabletBase {
  private static final Logger log = LoggerFactory.getLogger(Tablet.class);

  private final TabletServer tabletServer;
  private final TabletResourceManager tabletResources;
  private final DatafileManager datafileManager;
  private final String dirName;

  private final TabletMemory tabletMemory;

  private final TabletTime tabletTime;
  private final Object timeLock = new Object();
  private long persistedTime;

  private Location lastLocation = null;
  private volatile Set<Path> checkedTabletDirs = new ConcurrentSkipListSet<>();

  private final AtomicLong dataSourceDeletions = new AtomicLong(0);

  @Override
  public long getDataSourceDeletions() {
    return dataSourceDeletions.get();
  }

  private enum CloseState {
    OPEN, CLOSING, CLOSED, COMPLETE
  }

  private volatile CloseState closeState = CloseState.OPEN;

  private boolean updatingFlushID = false;

  private AtomicLong lastFlushID = new AtomicLong(-1);
  private AtomicLong lastCompactID = new AtomicLong(-1);

  public long getLastCompactId() {
    return lastCompactID.get();
  }

  private static class CompactionWaitInfo {
    long flushID = -1;
    long compactionID = -1;
  }

  // stores info about user initiated major compaction that is waiting on a minor compaction to
  // finish
  private final CompactionWaitInfo compactionWaitInfo = new CompactionWaitInfo();

  enum CompactionState {
    WAITING_TO_START, IN_PROGRESS
  }

  private CompactableImpl compactable;

  private volatile CompactionState minorCompactionState = null;

  private final Deriver<ConstraintChecker> constraintChecker;

  private int writesInProgress = 0;

  private final TabletStatsKeeper timer = new TabletStatsKeeper();

  /**
   * Counts are maintained in this object and reported out with the Micrometer metrics via
   * TabletServerMetricsUtil
   */
  private long ingestCount = 0;
  private long ingestBytes = 0;

  /**
   * Rates are calculated here in the Tablet for use in the Monitor but we do not emit them as
   * metrics. Rates can be calculated from the "Count" metrics above by downstream systems.
   */
  private final Rate queryRate = new Rate(0.95);
  private final Rate queryByteRate = new Rate(0.95);
  private final Rate ingestRate = new Rate(0.95);
  private final Rate ingestByteRate = new Rate(0.95);
  private final Rate scannedRate = new Rate(0.95);

  private long lastMinorCompactionFinishTime = 0;
  private long lastMapFileImportTime = 0;

  private volatile long numEntries = 0;
  private volatile long numEntriesInMemory = 0;

  // Files that are currently in the process of bulk importing. Access to this is protected by the
  // tablet lock.
  private final Set<TabletFile> bulkImporting = new HashSet<>();

  // Files that were successfully bulk imported. Using a concurrent map supports non-locking
  // operations on the key set which is useful for the periodic task that cleans up completed bulk
  // imports for all tablets. However the values of this map are ArrayList which do not support
  // concurrency. This is ok because all operations on the values are done while the tablet lock is
  // held.
  private final ConcurrentHashMap<Long,List<TabletFile>> bulkImported = new ConcurrentHashMap<>();

  private final int logId;

  public int getLogId() {
    return logId;
  }

  public static class LookupResult {
    public List<Range> unfinishedRanges = new ArrayList<>();
    public long bytesAdded = 0;
    public long dataSize = 0;
    public boolean closed = false;
  }

  private String chooseTabletDir() throws IOException {
    VolumeChooserEnvironment chooserEnv =
        new VolumeChooserEnvironmentImpl(extent.tableId(), extent.endRow(), context);
    String dirUri = tabletServer.getVolumeManager().choose(chooserEnv, context.getBaseUris())
        + Constants.HDFS_TABLES_DIR + Path.SEPARATOR + extent.tableId() + Path.SEPARATOR + dirName;
    checkTabletDir(new Path(dirUri));
    return dirUri;
  }

  TabletFile getNextMapFilename(String prefix) throws IOException {
    String extension = FileOperations.getNewFileExtension(tableConfiguration);
    return new TabletFile(new Path(chooseTabletDir() + "/" + prefix
        + context.getUniqueNameAllocator().getNextName() + "." + extension));
  }

  TabletFile getNextMapFilenameForMajc(boolean propagateDeletes) throws IOException {
    String tmpFileName = getNextMapFilename(!propagateDeletes ? "A" : "C").getMetaInsert() + "_tmp";
    return new TabletFile(new Path(tmpFileName));
  }

  private void checkTabletDir(Path path) throws IOException {
    if (!checkedTabletDirs.contains(path)) {
      FileStatus[] files = null;
      try {
        files = getTabletServer().getVolumeManager().listStatus(path);
      } catch (FileNotFoundException ex) {
        // ignored
      }

      if (files == null) {
        log.debug("Tablet {} had no dir, creating {}", extent, path);

        getTabletServer().getVolumeManager().mkdirs(path);
      }
      checkedTabletDirs.add(path);
    }
  }

  public Tablet(final TabletServer tabletServer, final KeyExtent extent,
      final TabletResourceManager trm, TabletData data)
      throws IOException, IllegalArgumentException {

    super(tabletServer, extent);

    this.tabletServer = tabletServer;
    this.tabletResources = trm;
    this.lastLocation = data.getLastLocation();
    this.lastFlushID.set(data.getFlushID());
    this.lastCompactID.set(data.getCompactID());
    this.splitCreationTime = data.getSplitTime();
    this.tabletTime = TabletTime.getInstance(data.getTime());
    this.persistedTime = tabletTime.getTime();
    this.logId = tabletServer.createLogId();

    // translate any volume changes
    @SuppressWarnings("deprecation")
    boolean replicationEnabled = org.apache.accumulo.core.replication.ReplicationConfigurationUtil
        .isEnabled(extent, this.tableConfiguration);
    TabletFiles tabletPaths =
        new TabletFiles(data.getDirectoryName(), data.getLogEntries(), data.getDataFiles());
    tabletPaths = VolumeUtil.updateTabletVolumes(tabletServer.getContext(), tabletServer.getLock(),
        extent, tabletPaths, replicationEnabled);

    this.dirName = data.getDirectoryName();

    for (Entry<Long,List<TabletFile>> entry : data.getBulkImported().entrySet()) {
      this.bulkImported.put(entry.getKey(), new ArrayList<>(entry.getValue()));
    }

    final List<LogEntry> logEntries = tabletPaths.logEntries;
    final SortedMap<StoredTabletFile,DataFileValue> datafiles = tabletPaths.datafiles;

    constraintChecker = tableConfiguration.newDeriver(ConstraintChecker::new);

    tabletMemory = new TabletMemory(this);

    // don't bother examining WALs for recovery if Table is being deleted
    if (!logEntries.isEmpty() && !isBeingDeleted()) {
      TabletLogger.recovering(extent, logEntries);
      final AtomicLong entriesUsedOnTablet = new AtomicLong(0);
      // track max time from walog entries without timestamps
      final AtomicLong maxTime = new AtomicLong(Long.MIN_VALUE);
      final CommitSession commitSession = getTabletMemory().getCommitSession();
      try {
        Set<String> absPaths = new HashSet<>();
        for (TabletFile ref : datafiles.keySet()) {
          absPaths.add(ref.getPathStr());
        }

        tabletServer.recover(this.getTabletServer().getVolumeManager(), extent, logEntries,
            absPaths, m -> {
              Collection<ColumnUpdate> muts = m.getUpdates();
              for (ColumnUpdate columnUpdate : muts) {
                if (!columnUpdate.hasTimestamp()) {
                  // if it is not a user set timestamp, it must have been set
                  // by the system
                  maxTime.set(Math.max(maxTime.get(), columnUpdate.getTimestamp()));
                }
              }
              getTabletMemory().mutate(commitSession, Collections.singletonList(m), 1);
              entriesUsedOnTablet.incrementAndGet();
            });

        if (maxTime.get() != Long.MIN_VALUE) {
          tabletTime.useMaxTimeFromWALog(maxTime.get());
        }
        commitSession.updateMaxCommittedTime(tabletTime.getTime());

        @SuppressWarnings("deprecation")
        boolean replicationEnabledForTable =
            org.apache.accumulo.core.replication.ReplicationConfigurationUtil.isEnabled(extent,
                tabletServer.getTableConfiguration(extent));
        if (entriesUsedOnTablet.get() == 0) {
          log.debug("No replayed mutations applied, removing unused entries for {}", extent);
          MetadataTableUtil.removeUnusedWALEntries(getTabletServer().getContext(), extent,
              logEntries, tabletServer.getLock());
          logEntries.clear();
        } else if (replicationEnabledForTable) {
          // record that logs may have data for this extent
          @SuppressWarnings("deprecation")
          Status status = org.apache.accumulo.server.replication.StatusUtil.openWithUnknownLength();
          for (LogEntry logEntry : logEntries) {
            log.debug("Writing updated status to metadata table for {} {}", logEntry.filename,
                ProtobufUtil.toString(status));
            ReplicationTableUtil.updateFiles(tabletServer.getContext(), extent, logEntry.filename,
                status);
          }
        }

      } catch (Exception t) {
        String msg = "Error recovering tablet " + extent + " from log files";
        if (tableConfiguration.getBoolean(Property.TABLE_FAILURES_IGNORE)) {
          log.warn(msg, t);
        } else {
          throw new RuntimeException(msg, t);
        }
      }
      // make some closed references that represent the recovered logs
      currentLogs = new HashSet<>();
      for (LogEntry logEntry : logEntries) {
        currentLogs.add(new DfsLogger(tabletServer.getContext(), tabletServer.getServerConfig(),
            logEntry.filename, logEntry.getColumnQualifier().toString()));
      }

      rebuildReferencedLogs();

      TabletLogger.recovered(extent, logEntries, entriesUsedOnTablet.get(),
          getTabletMemory().getNumEntries());
    }

    // do this last after tablet is completely setup because it
    // could cause major compaction to start
    datafileManager = new DatafileManager(this, datafiles);

    computeNumEntries();

    getDatafileManager().removeFilesAfterScan(data.getScanFiles());

    // look for hints of a failure on the previous tablet server
    if (!logEntries.isEmpty()) {
      // look for any temp files hanging around
      removeOldTemporaryFiles(data.getExternalCompactions());
    }

    this.compactable = new CompactableImpl(this, tabletServer.getCompactionManager(),
        data.getExternalCompactions());
  }

  private void removeOldTemporaryFiles(
      Map<ExternalCompactionId,ExternalCompactionMetadata> externalCompactions) {
    // remove any temporary files created by a previous tablet server
    try {

      var extCompactionFiles = externalCompactions.values().stream()
          .map(ecMeta -> ecMeta.getCompactTmpName().getPath()).collect(Collectors.toSet());

      for (Volume volume : getTabletServer().getVolumeManager().getVolumes()) {
        String dirUri = volume.getBasePath() + Constants.HDFS_TABLES_DIR + Path.SEPARATOR
            + extent.tableId() + Path.SEPARATOR + dirName;

        for (FileStatus tmp : volume.getFileSystem().globStatus(new Path(dirUri, "*_tmp"))) {

          if (extCompactionFiles.contains(tmp.getPath())) {
            continue;
          }

          try {
            log.debug("Removing old temp file {}", tmp.getPath());
            volume.getFileSystem().delete(tmp.getPath(), false);
          } catch (IOException ex) {
            log.error("Unable to remove old temp file " + tmp.getPath() + ": " + ex);
          }
        }
      }
    } catch (IOException ex) {
      log.error("Error scanning for old temp files", ex);
    }
  }

  public void checkConditions(ConditionChecker checker, Authorizations authorizations,
      AtomicBoolean iFlag) throws IOException {

    ScanParameters scanParams = new ScanParameters(-1, authorizations, Collections.emptySet(), null,
        null, false, null, -1, null);
    scanParams.setScanDispatch(ScanDispatch.builder().build());

    ScanDataSource dataSource = createDataSource(scanParams, false, iFlag);

    boolean sawException = false;
    try {
      SortedKeyValueIterator<Key,Value> iter = new SourceSwitchingIterator(dataSource);
      checker.check(iter);
    } catch (IOException | RuntimeException e) {
      sawException = true;
      throw e;
    } finally {
      // code in finally block because always want
      // to return mapfiles, even when exception is thrown
      dataSource.close(sawException);
    }
  }

  DataFileValue minorCompact(InMemoryMap memTable, TabletFile tmpDatafile, TabletFile newDatafile,
      long queued, CommitSession commitSession, long flushId, MinorCompactionReason mincReason) {
    boolean failed = false;
    long start = System.currentTimeMillis();
    timer.incrementStatusMinor();

    long count = 0;

    String oldName = Thread.currentThread().getName();
    try {
      Thread.currentThread().setName("Minor compacting " + this.extent);
      CompactionStats stats;
      Span span = TraceUtil.startSpan(this.getClass(), "minorCompact::write");
      try (Scope scope = span.makeCurrent()) {
        count = memTable.getNumEntries();

        MinorCompactor compactor = new MinorCompactor(tabletServer, this, memTable, tmpDatafile,
            mincReason, tableConfiguration);
        stats = compactor.call();
      } catch (Exception e) {
        TraceUtil.setException(span, e, true);
        throw e;
      } finally {
        span.end();
      }

      Span span2 = TraceUtil.startSpan(this.getClass(), "minorCompact::bringOnline");
      try (Scope scope = span2.makeCurrent()) {
        var storedFile = getDatafileManager().bringMinorCompactionOnline(tmpDatafile, newDatafile,
            new DataFileValue(stats.getFileSize(), stats.getEntriesWritten()), commitSession,
            flushId);
        storedFile.ifPresent(stf -> compactable.filesAdded(true, List.of(stf)));
      } catch (Exception e) {
        TraceUtil.setException(span2, e, true);
        throw e;
      } finally {
        span2.end();
      }

      return new DataFileValue(stats.getFileSize(), stats.getEntriesWritten());
    } catch (Exception | Error e) {
      failed = true;
      throw new RuntimeException("Exception occurred during minor compaction on " + extent, e);
    } finally {
      Thread.currentThread().setName(oldName);
      try {
        getTabletMemory().finalizeMinC();
      } catch (Exception t) {
        log.error("Failed to free tablet memory on {}", extent, t);
      }

      if (!failed) {
        lastMinorCompactionFinishTime = System.currentTimeMillis();
      }
      TabletServerMinCMetrics minCMetrics = getTabletServer().getMinCMetrics();
      minCMetrics.addActive(lastMinorCompactionFinishTime - start);
      timer.updateTime(Operation.MINOR, queued, start, count, failed);
      minCMetrics.addQueued(start - queued);
    }
  }

  private synchronized MinorCompactionTask prepareForMinC(long flushId,
      MinorCompactionReason mincReason) {
    Preconditions.checkState(otherLogs.isEmpty());
    Preconditions.checkState(referencedLogs.equals(currentLogs));
    CommitSession oldCommitSession = getTabletMemory().prepareForMinC();
    otherLogs = currentLogs;
    currentLogs = new HashSet<>();

    return new MinorCompactionTask(this, oldCommitSession, flushId, mincReason);

  }

  public void flush(long tableFlushID) {
    boolean updateMetadata = false;
    boolean initiateMinor = false;

    try {

      synchronized (this) {

        // only want one thing at a time to update flush ID to ensure that metadata table and tablet
        // in memory state are consistent
        if (updatingFlushID) {
          return;
        }

        if (lastFlushID.get() >= tableFlushID) {
          return;
        }

        if (isClosing() || isClosed() || isBeingDeleted()
            || getTabletMemory().memoryReservedForMinC()) {
          return;
        }

        if (getTabletMemory().getMemTable().getNumEntries() == 0) {
          lastFlushID.set(tableFlushID);
          updatingFlushID = true;
          updateMetadata = true;
        } else {
          initiateMinor = true;
        }
      }

      if (updateMetadata) {
        // if multiple threads were allowed to update this outside of a sync block, then it would be
        // a race condition
        MetadataTableUtil.updateTabletFlushID(extent, tableFlushID, context,
            getTabletServer().getLock());
      } else if (initiateMinor) {
        initiateMinorCompaction(tableFlushID, MinorCompactionReason.USER);
      }

    } finally {
      if (updateMetadata) {
        synchronized (this) {
          updatingFlushID = false;
          this.notifyAll();
        }
      }
    }

  }

  public boolean initiateMinorCompaction(MinorCompactionReason mincReason) {
    if (isClosed()) {
      return false;
    }
    if (isBeingDeleted()) {
      log.debug("Table {} is being deleted so don't flush {}", extent.tableId(), extent);
      return false;
    }

    // get the flush id before the new memmap is made available for write
    long flushId;
    try {
      flushId = getFlushID();
    } catch (NoNodeException e) {
      log.info("Asked to initiate MinC when there was no flush id {} {}", getExtent(),
          e.getMessage());
      return false;
    }
    return initiateMinorCompaction(flushId, mincReason);
  }

  public boolean minorCompactNow(MinorCompactionReason mincReason) {
    long flushId;
    try {
      flushId = getFlushID();
    } catch (NoNodeException e) {
      log.info("Asked to initiate MinC when there was no flush id {} {}", getExtent(),
          e.getMessage());
      return false;
    }
    MinorCompactionTask mct = createMinorCompactionTask(flushId, mincReason);
    if (mct == null) {
      return false;
    }
    mct.run();
    return true;
  }

  boolean initiateMinorCompaction(long flushId, MinorCompactionReason mincReason) {
    MinorCompactionTask mct = createMinorCompactionTask(flushId, mincReason);
    if (mct == null) {
      return false;
    }
    getTabletResources().executeMinorCompaction(mct);
    return true;
  }

  private MinorCompactionTask createMinorCompactionTask(long flushId,
      MinorCompactionReason mincReason) {
    MinorCompactionTask mct;
    long t1, t2;

    StringBuilder logMessage = null;

    try {
      synchronized (this) {
        t1 = System.currentTimeMillis();

        if (isClosing() || isClosed() || getTabletMemory().memoryReservedForMinC()
            || getTabletMemory().getMemTable().getNumEntries() == 0 || updatingFlushID) {

          logMessage = new StringBuilder();

          logMessage.append(extent);
          logMessage.append(" closeState " + closeState);
          if (getTabletMemory() != null) {
            logMessage.append(" tabletMemory.memoryReservedForMinC() "
                + getTabletMemory().memoryReservedForMinC());
          }
          if (getTabletMemory() != null && getTabletMemory().getMemTable() != null) {
            logMessage.append(" tabletMemory.getMemTable().getNumEntries() "
                + getTabletMemory().getMemTable().getNumEntries());
          }
          logMessage.append(" updatingFlushID " + updatingFlushID);

          return null;
        }

        mct = prepareForMinC(flushId, mincReason);
        t2 = System.currentTimeMillis();
      }
    } finally {
      // log outside of sync block
      if (logMessage != null && log.isDebugEnabled()) {
        log.debug("{}", logMessage);
      }
    }

    log.debug(String.format("MinC initiate lock %.2f secs", (t2 - t1) / 1000.0));
    return mct;
  }

  public long getFlushID() throws NoNodeException {
    try {
      String zTablePath = Constants.ZROOT + "/" + tabletServer.getInstanceID() + Constants.ZTABLES
          + "/" + extent.tableId() + Constants.ZTABLE_FLUSH_ID;
      String id = new String(context.getZooReaderWriter().getData(zTablePath), UTF_8);
      return Long.parseLong(id);
    } catch (InterruptedException | NumberFormatException e) {
      throw new RuntimeException("Exception on " + extent + " getting flush ID", e);
    } catch (KeeperException ke) {
      if (ke instanceof NoNodeException) {
        throw (NoNodeException) ke;
      } else {
        throw new RuntimeException("Exception on " + extent + " getting flush ID", ke);
      }
    }
  }

  long getCompactionCancelID() {
    String zTablePath = Constants.ZROOT + "/" + tabletServer.getInstanceID() + Constants.ZTABLES
        + "/" + extent.tableId() + Constants.ZTABLE_COMPACT_CANCEL_ID;
    String id = new String(context.getZooCache().get(zTablePath), UTF_8);
    return Long.parseLong(id);
  }

  public Pair<Long,CompactionConfig> getCompactionID() throws NoNodeException {
    try {
      String zTablePath = Constants.ZROOT + "/" + tabletServer.getInstanceID() + Constants.ZTABLES
          + "/" + extent.tableId() + Constants.ZTABLE_COMPACT_ID;

      String[] tokens =
          new String(context.getZooReaderWriter().getData(zTablePath), UTF_8).split(",");
      long compactID = Long.parseLong(tokens[0]);

      CompactionConfig overlappingConfig = null;

      if (tokens.length > 1) {
        Hex hex = new Hex();
        ByteArrayInputStream bais =
            new ByteArrayInputStream(hex.decode(tokens[1].split("=")[1].getBytes(UTF_8)));
        DataInputStream dis = new DataInputStream(bais);

        var compactionConfig = UserCompactionUtils.decodeCompactionConfig(dis);

        KeyExtent ke = new KeyExtent(extent.tableId(), compactionConfig.getEndRow(),
            compactionConfig.getStartRow());

        if (ke.overlaps(extent)) {
          overlappingConfig = compactionConfig;
        }
      }

      if (overlappingConfig == null) {
        overlappingConfig = new CompactionConfig(); // no config present, set to default
      }

      return new Pair<>(compactID, overlappingConfig);
    } catch (InterruptedException | DecoderException | NumberFormatException e) {
      throw new RuntimeException("Exception on " + extent + " getting compaction ID", e);
    } catch (KeeperException ke) {
      if (ke instanceof NoNodeException) {
        throw (NoNodeException) ke;
      } else {
        throw new RuntimeException("Exception on " + extent + " getting compaction ID", ke);
      }
    }
  }

  private synchronized CommitSession finishPreparingMutations(long time) {
    if (isClosed() || getTabletMemory() == null) {
      return null;
    }

    CommitSession commitSession = getTabletMemory().getCommitSession();
    incrementWritesInProgress(commitSession);

    commitSession.updateMaxCommittedTime(time);
    return commitSession;
  }

  public PreparedMutations prepareMutationsForCommit(final TservConstraintEnv cenv,
      final List<Mutation> mutations) {
    cenv.setExtent(extent);
    final ConstraintChecker constraints = constraintChecker.derive();

    // Check each mutation for any constraint violations.
    Violations violations = null;
    Set<Mutation> violators = null;
    List<Mutation> nonViolators = null;

    for (Mutation mutation : mutations) {
      Violations mutationViolations = constraints.check(cenv, mutation);
      if (mutationViolations != null) {
        if (violations == null) {
          violations = new Violations();
          violators = new HashSet<>();
        }

        violations.add(mutationViolations);
        violators.add(mutation);
      }
    }

    if (violations == null) {
      // If there are no violations, use the original list for non-violators.
      nonViolators = mutations;
      violators = Collections.emptySet();
      violations = Violations.EMPTY;
    } else if (violators.size() != mutations.size()) {
      // Otherwise, find all non-violators.
      nonViolators = new ArrayList<>((mutations.size() - violators.size()));
      for (Mutation mutation : mutations) {
        if (!violators.contains(mutation)) {
          nonViolators.add(mutation);
        }
      }
    } else {
      // all mutations violated a constraint
      nonViolators = Collections.emptyList();
    }

    // If there are any mutations that do not violate the constraints, attempt to prepare the tablet
    // and retrieve the commit session.
    CommitSession cs = null;
    if (!nonViolators.isEmpty()) {
      long time = tabletTime.setUpdateTimes(nonViolators);
      cs = finishPreparingMutations(time);
      if (cs == null) {
        // tablet is closed
        return new PreparedMutations();
      }
    }

    return new PreparedMutations(cs, nonViolators, violations, violators);
  }

  private synchronized void incrementWritesInProgress(CommitSession cs) {
    incrementWritesInProgress();
    cs.incrementCommitsInProgress();
  }

  private synchronized void incrementWritesInProgress() {
    if (writesInProgress < 0) {
      throw new IllegalStateException("FATAL: Something really bad went wrong. Attempted to "
          + "increment a negative number of writes in progress " + writesInProgress + "on tablet "
          + extent);
    }
    writesInProgress++;
  }

  private synchronized void decrementWritesInProgress(CommitSession cs) {
    decrementWritesInProgress();
    cs.decrementCommitsInProgress();
  }

  private synchronized void decrementWritesInProgress() {
    if (writesInProgress <= 0) {
      throw new IllegalStateException("FATAL: Something really bad went wrong. Attempted to "
          + "decrement the number of writes in progress " + writesInProgress + " to < 0 on tablet "
          + extent);
    }
    writesInProgress--;
    if (writesInProgress == 0) {
      this.notifyAll();
    }
  }

  public synchronized void abortCommit(CommitSession commitSession) {
    if (isCloseComplete() || getTabletMemory() == null) {
      throw new IllegalStateException("Aborting commit when tablet " + extent + " is closed");
    }

    decrementWritesInProgress(commitSession);
  }

  public void commit(CommitSession commitSession, List<Mutation> mutations) {

    int totalCount = 0;
    long totalBytes = 0;

    // write the mutation to the in memory table
    for (Mutation mutation : mutations) {
      totalCount += mutation.size();
      totalBytes += mutation.numBytes();
    }

    getTabletMemory().mutate(commitSession, mutations, totalCount);

    synchronized (this) {
      if (isCloseComplete()) {
        throw new IllegalStateException(
            "Tablet " + extent + " closed with outstanding messages to the logger");
      }
      // decrement here in case an exception is thrown below
      decrementWritesInProgress(commitSession);

      getTabletMemory().updateMemoryUsageStats();

      numEntries += totalCount;
      numEntriesInMemory += totalCount;
      ingestCount += totalCount;
      ingestBytes += totalBytes;
    }
  }

  /**
   * Closes the mapfiles associated with a Tablet. If saveState is true, a minor compaction is
   * performed.
   */
  @Override
  public void close(boolean saveState) throws IOException {
    initiateClose(saveState);
    completeClose(saveState, true);
    log.info("Tablet {} closed.", this.extent);
  }

  void initiateClose(boolean saveState) {
    log.trace("initiateClose(saveState={}) {}", saveState, getExtent());

    MinorCompactionTask mct = null;
    if (saveState) {
      try {
        synchronized (this) {
          // Wait for any running minor compaction before trying to start another. This is done for
          // the case where the current in memory map has a lot of data. So wait for the running
          // minor compaction and then start compacting the current in memory map before closing.
          getTabletMemory().waitForMinC();
        }
        mct = createMinorCompactionTask(getFlushID(), MinorCompactionReason.CLOSE);
      } catch (NoNodeException e) {
        throw new IllegalStateException("Exception on " + extent + " during prep for MinC", e);
      }
    }

    if (mct != null) {
      // Do an initial minor compaction that flushes any data in memory before marking that tablet
      // as closed. Another minor compaction will be done once the tablet is marked as closed. There
      // are two goals for this initial minor compaction.
      //
      // 1. Make the 2nd minor compaction that occurs after closing faster because it has less
      // data. That is important because after the tablet is closed it can not be read or written
      // to, so hopefully the 2nd compaction has little if any data because of this minor compaction
      // that occurred before close.
      //
      // 2. Its possible a minor compaction may hang because of bad config or DFS problems. Taking
      // this action before close can be less disruptive if it does hang. Also in the case where
      // there is a bug that causes minor compaction to fail it will leave the tablet in a bad
      // state. If that happens here before starting to close then it could leave the tablet in a
      // more usable state than a failure that happens after the tablet starts to close.
      //
      // If 'mct' was null it means either a minor compaction was running, there was no data to
      // minor compact, or the flush id was updating. In the case of flush id was updating, ideally
      // this code would wait for flush id updates and then minor compact if needed, but that can
      // not be done without setting the close state to closing to prevent flush id updates from
      // starting. So if there is a flush id update going on it could cause no minor compaction
      // here. There will still be a minor compaction after close.
      //
      // Its important to run the following minor compaction outside of any sync blocks as this
      // could needlessly block scans. The resources needed for the minor compaction have already
      // been reserved in a sync block.
      mct.run();
    }

    synchronized (this) {

      if (saveState) {
        // Wait for any running minc to finish before we start shutting things down in the tablet.
        // It is possible that this function was unable to initiate a minor compaction above and
        // something else did because of race conditions (because everything above happens before
        // marking anything closed so normal actions could still start minor compactions). If
        // something did start lets wait on it before marking things closed.
        getTabletMemory().waitForMinC();
      }

      // This check is intentionally done later in the method because the check and change of the
      // closeState variable need to be atomic, so both are done in the same sync block.
      if (isClosed() || isClosing()) {
        String msg = "Tablet " + getExtent() + " already " + closeState;
        throw new IllegalStateException(msg);
      }

      // enter the closing state, no splits or minor compactions can start
      closeState = CloseState.CLOSING;
      this.notifyAll();
    }

    // Cancel any running compactions and prevent future ones from starting. This is very important
    // because background compactions may update the metadata table. These metadata updates can not
    // be allowed after a tablet closes. Compactable has its own lock and calls tablet code, so do
    // not hold tablet lock while calling it.
    compactable.close();

    synchronized (this) {
      Preconditions.checkState(closeState == CloseState.CLOSING);

      while (updatingFlushID) {
        try {
          this.wait(50);
        } catch (InterruptedException e) {
          log.error(e.toString());
        }
      }

      // calling this.wait() releases the lock, ensure things are as expected when the lock is
      // obtained again
      Preconditions.checkState(closeState == CloseState.CLOSING);
    }
  }

  private boolean closeCompleting = false;

  synchronized void completeClose(boolean saveState, boolean completeClose) throws IOException {

    if (!isClosing() || isCloseComplete() || closeCompleting) {
      throw new IllegalStateException("Bad close state " + closeState + " on tablet " + extent);
    }

    log.trace("completeClose(saveState={} completeClose={}) {}", saveState, completeClose, extent);

    // ensure this method is only called once, also guards against multiple
    // threads entering the method at the same time
    closeCompleting = true;
    closeState = CloseState.CLOSED;

    // modify dataSourceDeletions so scans will try to switch data sources and fail because the
    // tablet is closed
    dataSourceDeletions.incrementAndGet();

    for (ScanDataSource activeScan : activeScans) {
      activeScan.interrupt();
    }

    long lastLogTime = System.nanoTime();

    // wait for reads and writes to complete
    while (writesInProgress > 0 || !activeScans.isEmpty()) {

      if (log.isDebugEnabled() && System.nanoTime() - lastLogTime > TimeUnit.SECONDS.toNanos(60)) {
        for (ScanDataSource activeScan : activeScans) {
          log.debug("Waiting on scan in completeClose {} {}", extent, activeScan);
        }

        lastLogTime = System.nanoTime();
      }

      try {
        log.debug("Waiting to completeClose for {}. {} writes {} scans", extent, writesInProgress,
            activeScans.size());
        this.wait(50);
      } catch (InterruptedException e) {
        log.error("Interrupted waiting to completeClose for extent {}", extent, e);
      }
    }

    getTabletMemory().waitForMinC();

    if (saveState && getTabletMemory().getMemTable().getNumEntries() > 0) {
      try {
        prepareForMinC(getFlushID(), MinorCompactionReason.CLOSE).run();
      } catch (NoNodeException e) {
        throw new RuntimeException("Exception on " + extent + " during prep for MinC", e);
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
          log.error("Consistency check fails, retrying", t);
          sleepUninterruptibly(500, TimeUnit.MILLISECONDS);
        }
      }
      if (err != null) {
        ProblemReports.getInstance(context).report(new ProblemReport(extent.tableId(),
            ProblemType.TABLET_LOAD, this.extent.toString(), err));
        log.error("Tablet closed consistency check has failed for {} giving up and closing",
            this.extent);
      }
    }

    try {
      getTabletMemory().getMemTable().delete(0);
    } catch (Exception t) {
      log.error("Failed to delete mem table : " + t.getMessage() + " for tablet " + extent, t);
    }

    getTabletMemory().close();

    // close map files
    getTabletResources().close();

    if (completeClose) {
      closeState = CloseState.COMPLETE;
    }
  }

  private void closeConsistencyCheck() {

    long num = tabletMemory.getMemTable().getNumEntries();
    if (num != 0) {
      String msg = "Closed tablet " + extent + " has " + num + " entries in memory";
      log.error(msg);
      throw new RuntimeException(msg);
    }

    if (tabletMemory.memoryReservedForMinC()) {
      String msg = "Closed tablet " + extent + " has minor compacting memory";
      log.error(msg);
      throw new RuntimeException(msg);
    }

    try {
      var tabletMeta = context.getAmple().readTablet(extent, ColumnType.FILES, ColumnType.LOGS,
          ColumnType.ECOMP, ColumnType.PREV_ROW, ColumnType.FLUSH_ID, ColumnType.COMPACT_ID);

      if (tabletMeta == null) {
        String msg = "Closed tablet " + extent + " not found in metadata";
        log.error(msg);
        throw new RuntimeException(msg);
      }

      HashSet<ExternalCompactionId> ecids = new HashSet<>();
      compactable.getExternalCompactionIds(ecids::add);
      if (!tabletMeta.getExternalCompactions().keySet().equals(ecids)) {
        String msg = "Closed tablet " + extent + " external compaction ids differ " + ecids + " != "
            + tabletMeta.getExternalCompactions().keySet();
        log.error(msg);
        throw new RuntimeException(msg);
      }

      if (!tabletMeta.getLogs().isEmpty()) {
        String msg = "Closed tablet " + extent + " has walog entries in " + MetadataTable.NAME + " "
            + tabletMeta.getLogs();
        log.error(msg);
        throw new RuntimeException(msg);
      }

      tabletMeta.getFlushId().ifPresent(flushId -> {
        if (flushId != lastFlushID.get()) {
          String msg = "Closed tablet " + extent + " lastFlushID is inconsistent with metadata : "
              + flushId + " != " + lastFlushID;
          log.error(msg);
          throw new RuntimeException(msg);
        }
      });

      tabletMeta.getCompactId().ifPresent(compactId -> {
        if (compactId != lastCompactID.get()) {
          String msg = "Closed tablet " + extent + " lastCompactID is inconsistent with metadata : "
              + compactId + " != " + lastCompactID;
          log.error(msg);
          throw new RuntimeException(msg);
        }
      });

      if (!tabletMeta.getFilesMap().equals(getDatafileManager().getDatafileSizes())) {
        String msg = "Data files in " + extent + " differ from in-memory data "
            + tabletMeta.getFilesMap() + " " + getDatafileManager().getDatafileSizes();
        log.error(msg);
      }
    } catch (Exception e) {
      String msg = "Failed to do close consistency check for tablet " + extent;
      log.error(msg, e);
      throw new RuntimeException(msg, e);

    }

    if (!otherLogs.isEmpty() || !currentLogs.isEmpty() || !referencedLogs.isEmpty()) {
      String msg = "Closed tablet " + extent + " has walog entries in memory currentLogs = "
          + currentLogs + "  otherLogs = " + otherLogs + " referencedLogs = " + referencedLogs;
      log.error(msg);
      throw new RuntimeException(msg);
    }
  }

  private boolean loggedErrorForTabletComparison = false;

  /**
   * Checks that tablet metadata from the metadata table matches what this tablet has in memory. The
   * caller of this method must acquire the updateCounter parameter before acquiring the
   * tabletMetadata.
   *
   * @param updateCounter used to check for conucurrent updates in which case this check is a no-op.
   *        See {@link #getUpdateCount()}
   * @param tabletMetadata the metadata for this tablet that was acquired from the metadata table.
   */
  public synchronized void compareTabletInfo(MetadataUpdateCount updateCounter,
      TabletMetadata tabletMetadata) {

    // verify the given counter is for this tablet, if this check fail it indicates a bug in the
    // calling code
    Preconditions.checkArgument(updateCounter.getExtent().equals(getExtent()),
        "Counter had unexpected extent %s != %s", updateCounter.getExtent(), getExtent());

    // verify the given tablet metadata is for this tablet, if this check fail it indicates a bug in
    // the calling code
    Preconditions.checkArgument(tabletMetadata.getExtent().equals(getExtent()),
        "Tablet metadata had unexpected extent %s != %s", tabletMetadata.getExtent(), getExtent());

    // All of the log messages in this method have the AMCC acronym which means Accumulo Metadata
    // Consistency Check. AMCC was added to the log messages to make grep/search for all log
    // message from this method easy to find.

    if (isClosed() || isClosing()) {
      log.trace("AMCC Tablet {} was closed, so skipping check", tabletMetadata.getExtent());
      return;
    }

    var dataFileSizes = getDatafileManager().getDatafileSizes();

    if (!tabletMetadata.getFilesMap().equals(dataFileSizes)) {
      // The counters are modified outside of locks before and after tablet metadata operations and
      // data file updates so, it's very important to acquire the 2nd counts after doing the
      // equality check above. If the counts are the same (as the ones acquired before reading
      // metadata table) after the equality check above then we know the tablet did not do any
      // metadata updates while we were reading metadata and then comparing.
      var latestCount = this.getUpdateCount();
      if (updateCounter.overlapsUpdate() || !updateCounter.equals(latestCount)) {
        log.trace(
            "AMCC Tablet {} may have been updating its metadata while it was being read for "
                + "check, so skipping check {} {}",
            tabletMetadata.getExtent(), updateCounter, latestCount);
      } else {
        log.error("Data files in {} differ from in-memory data {} {} {} {}", extent,
            tabletMetadata.getFilesMap(), dataFileSizes, updateCounter, latestCount);
        loggedErrorForTabletComparison = true;
      }
    } else {
      if (loggedErrorForTabletComparison) {
        log.info("AMCC Tablet {} files in memory are now same as in metadata table {}",
            tabletMetadata.getExtent(), updateCounter);
        loggedErrorForTabletComparison = false;
      } else {
        log.trace("AMCC Tablet {} files in memory are same as in metadata table {}",
            tabletMetadata.getExtent(), updateCounter);
      }
    }
  }

  /**
   * Returns an int representing the total block size of the files served by this tablet.
   *
   * @return size
   */
  // this is the size of just the files
  public long estimateTabletSize() {
    long size = 0L;

    for (DataFileValue sz : getDatafileManager().getDatafileSizes().values()) {
      size += sz.getSize();
    }

    return size;
  }

  private final long splitCreationTime;

  private boolean isSplitPossible() {

    // never split the root tablet
    // check if we already decided that we can never split
    // check to see if we're big enough to split

    long splitThreshold = tableConfiguration.getAsBytes(Property.TABLE_SPLIT_THRESHOLD);

    if (extent.isRootTablet() || isFindSplitsSuppressed()
        || estimateTabletSize() <= splitThreshold) {
      return false;
    }

    return true;
  }

  private synchronized SplitRowSpec findSplitRow(Optional<SplitComputations> splitComputations) {

    // never split the root tablet
    // check if we already decided that we can never split
    // check to see if we're big enough to split

    long maxEndRow = tableConfiguration.getAsBytes(Property.TABLE_MAX_END_ROW_SIZE);

    if (!isSplitPossible()) {
      return null;
    }

    if (!splitComputations.isPresent()) {
      // information needed to compute a split point is out of date or does not exists, try again
      // later
      return null;
    }

    SortedMap<Double,Key> keys = splitComputations.orElseThrow().midPoint;

    if (keys.isEmpty()) {
      log.info("Cannot split tablet " + extent + ", files contain no data for tablet.");
      suppressFindSplits();
      return null;
    }

    // check to see if one row takes up most of the tablet, in which case we can not split
    Text lastRow;
    if (extent.endRow() == null) {
      lastRow = splitComputations.orElseThrow().lastRowForDefaultTablet;
    } else {
      lastRow = extent.endRow();
    }

    // We expect to get a midPoint for this set of files. If we don't get one, we have a problem.
    final Key mid = keys.get(.5);
    if (mid == null) {
      throw new IllegalStateException("Could not determine midpoint for files on " + extent);
    }

    // check to see that the midPoint is not equal to the end key
    if (mid.compareRow(lastRow) == 0) {
      if (keys.firstKey() < .5) {
        Key candidate = keys.get(keys.firstKey());
        if (candidate.getLength() > maxEndRow) {
          log.warn("Cannot split tablet {}, selected split point too long.  Length :  {}", extent,
              candidate.getLength());

          suppressFindSplits();

          return null;
        }
        if (candidate.compareRow(lastRow) != 0) {
          // we should use this ratio in split size estimations
          if (log.isTraceEnabled()) {
            log.trace(
                String.format("Splitting at %6.2f instead of .5, row at .5 is same as end row%n",
                    keys.firstKey()));
          }
          return new SplitRowSpec(keys.firstKey(), candidate.getRow());
        }

      }

      log.warn("Cannot split tablet {} it contains a big row : {}", extent, lastRow);
      suppressFindSplits();

      return null;
    }

    Text text = mid.getRow();
    SortedMap<Double,Key> firstHalf = keys.headMap(.5);
    if (!firstHalf.isEmpty()) {
      Text beforeMid = firstHalf.get(firstHalf.lastKey()).getRow();
      Text shorter = new Text();
      int trunc = longestCommonLength(text, beforeMid);
      shorter.set(text.getBytes(), 0, Math.min(text.getLength(), trunc + 1));
      text = shorter;
    }

    if (text.getLength() > maxEndRow) {
      log.warn("Cannot split tablet {}, selected split point too long.  Length :  {}", extent,
          text.getLength());

      suppressFindSplits();

      return null;
    }

    return new SplitRowSpec(.5, text);
  }

  private boolean supressFindSplits = false;
  private long timeOfLastMinCWhenFindSplitsWasSupressed = 0;
  private long timeOfLastImportWhenFindSplitsWasSupressed = 0;

  /**
   * Check if the the current files were found to be unsplittable. If so, then do not want to spend
   * resources on examining the files again until the files change.
   */
  private boolean isFindSplitsSuppressed() {
    if (supressFindSplits) {
      if (timeOfLastMinCWhenFindSplitsWasSupressed != lastMinorCompactionFinishTime
          || timeOfLastImportWhenFindSplitsWasSupressed != lastMapFileImportTime) {
        // a minor compaction or map file import has occurred... check again
        supressFindSplits = false;
      } else {
        // nothing changed, do not split
        return true;
      }
    }

    return false;
  }

  /**
   * Remember that the current set of files are unsplittable.
   */
  private void suppressFindSplits() {
    supressFindSplits = true;
    timeOfLastMinCWhenFindSplitsWasSupressed = lastMinorCompactionFinishTime;
    timeOfLastImportWhenFindSplitsWasSupressed = lastMapFileImportTime;
  }

  private static int longestCommonLength(Text text, Text beforeMid) {
    int common = 0;
    while (common < text.getLength() && common < beforeMid.getLength()
        && text.getBytes()[common] == beforeMid.getBytes()[common]) {
      common++;
    }
    return common;
  }

  // encapsulates results of computations needed to make determinations about splits
  private static class SplitComputations {
    final Set<TabletFile> inputFiles;

    // cached result of calling FileUtil.findMidpoint
    final SortedMap<Double,Key> midPoint;

    // the last row seen in the files, only set for the default tablet
    final Text lastRowForDefaultTablet;

    private SplitComputations(Set<TabletFile> inputFiles, SortedMap<Double,Key> midPoint,
        Text lastRowForDefaultTablet) {
      this.inputFiles = inputFiles;
      this.midPoint = midPoint;
      this.lastRowForDefaultTablet = lastRowForDefaultTablet;
    }
  }

  // The following caches keys from users files needed to compute a tablets split point. This cached
  // data could potentially be large and is therefore stored using a soft refence so the Java GC can
  // release it if needed. If the cached information is not there it can always be recomputed.
  private volatile SoftReference<SplitComputations> lastSplitComputation =
      new SoftReference<>(null);
  private final Lock splitComputationLock = new ReentrantLock();

  /**
   * Computes split point information from files when a tablets set of files changes. Do not call
   * this method when holding the tablet lock.
   */
  public Optional<SplitComputations> getSplitComputations() {

    if (!isSplitPossible() || isClosing() || isClosed()) {
      // do not want to bother doing any computations when a split is not possible
      return Optional.empty();
    }

    Set<TabletFile> files = getDatafileManager().getFiles();
    SplitComputations lastComputation = lastSplitComputation.get();
    if (lastComputation != null && lastComputation.inputFiles.equals(files)) {
      // the last computation is still relevant
      return Optional.of(lastComputation);
    }

    if (Thread.holdsLock(this)) {
      log.warn(
          "Thread holding tablet lock is doing split computation, this is unexpected and needs "
              + "investigation. Please open an Accumulo issue with the stack trace because this can "
              + "cause performance problems for scans.",
          new RuntimeException());
    }

    SplitComputations newComputation;

    // Only want one thread doing this computation at time for a tablet.
    if (splitComputationLock.tryLock()) {
      try {
        SortedMap<Double,Key> midpoint =
            FileUtil.findMidPoint(context, tableConfiguration, chooseTabletDir(),
                extent.prevEndRow(), extent.endRow(), FileUtil.toPathStrings(files), .25, true);

        Text lastRow = null;

        if (extent.endRow() == null) {
          Key lastKey = (Key) FileUtil.findLastKey(context, tableConfiguration, files);
          lastRow = lastKey.getRow();
        }

        newComputation = new SplitComputations(files, midpoint, lastRow);

        lastSplitComputation = new SoftReference<>(newComputation);
      } catch (IOException e) {
        lastSplitComputation.clear();
        log.error("Failed to compute split information from files " + e.getMessage());
        return Optional.empty();
      } finally {
        splitComputationLock.unlock();
      }

      return Optional.of(newComputation);
    } else {
      // some other thread seems to be working on split, let the other thread work on it
      return Optional.empty();
    }
  }

  /**
   * Returns true if this tablet needs to be split
   *
   */
  public synchronized boolean needsSplit(Optional<SplitComputations> splitComputations) {
    if (isClosing() || isClosed()) {
      return false;
    }
    return findSplitRow(splitComputations) != null;
  }

  synchronized void computeNumEntries() {
    Collection<DataFileValue> vals = getDatafileManager().getDatafileSizes().values();

    long numEntries = 0;

    for (DataFileValue tableValue : vals) {
      numEntries += tableValue.getNumEntries();
    }

    this.numEntriesInMemory = getTabletMemory().getNumEntries();
    numEntries += getTabletMemory().getNumEntries();

    this.numEntries = numEntries;
  }

  public long getNumEntries() {
    return numEntries;
  }

  public long getNumEntriesInMemory() {
    return numEntriesInMemory;
  }

  // Do not synchronize this method, it is called frequently by compactions
  public boolean isClosing() {
    return closeState == CloseState.CLOSING;
  }

  @Override
  public boolean isClosed() {
    // Assign to a local var to avoid race conditions since closeState is volatile and two
    // comparisons are done.
    CloseState localCS = closeState;
    return localCS == CloseState.CLOSED || localCS == CloseState.COMPLETE;
  }

  public boolean isBeingDeleted() {
    return context.getTableManager().getTableState(extent.tableId()) == TableState.DELETING;
  }

  public boolean isCloseComplete() {
    return closeState == CloseState.COMPLETE;
  }

  public boolean isMajorCompactionRunning() {
    return compactable.isMajorCompactionRunning();
  }

  public boolean isMajorCompactionQueued() {
    return compactable.isMajorCompactionQueued();
  }

  public boolean isMinorCompactionQueued() {
    return minorCompactionState == CompactionState.WAITING_TO_START;
  }

  public boolean isMinorCompactionRunning() {
    return minorCompactionState == CompactionState.IN_PROGRESS;
  }

  public TreeMap<KeyExtent,TabletData> split(byte[] sp) throws IOException {

    if (sp != null && extent.endRow() != null && extent.endRow().equals(new Text(sp))) {
      throw new IllegalArgumentException(
          "Attempting to split on EndRow " + extent.endRow() + " for " + extent);
    }

    if (sp != null && sp.length > tableConfiguration.getAsBytes(Property.TABLE_MAX_END_ROW_SIZE)) {
      String msg = "Cannot split tablet " + extent + ", selected split point too long.  Length :  "
          + sp.length;
      log.warn(msg);
      throw new IOException(msg);
    }

    if (extent.isRootTablet()) {
      String msg = "Cannot split root tablet";
      log.warn(msg);
      throw new RuntimeException(msg);
    }

    SplitRowSpec splitPoint = null;
    if (sp == null) {
      // call this outside of sync block
      var splitComputations = getSplitComputations();
      splitPoint = findSplitRow(splitComputations);
      if (splitPoint == null || splitPoint.row == null) {
        // no reason to log anything here, findSplitRow will log reasons when it returns null
        return null;
      }
    } else {
      Text tsp = new Text(sp);
      var fileStrings = FileUtil.toPathStrings(getDatafileManager().getFiles());
      // This ratio is calculated before that tablet is closed and outside of a lock, so new files
      // could arrive before the tablet is closed and locked. That is okay as the ratio is an
      // estimate.
      var ratio = FileUtil.estimatePercentageLTE(context, tableConfiguration, chooseTabletDir(),
          extent.prevEndRow(), extent.endRow(), fileStrings, tsp);
      splitPoint = new SplitRowSpec(ratio, tsp);
    }

    try {
      initiateClose(true);
    } catch (IllegalStateException ise) {
      log.debug("File {} not splitting : {}", extent, ise.getMessage());
      return null;
    } catch (RuntimeException re) {
      log.debug("File {} not splitting : {}", extent, re.getMessage());
      throw re;
    }

    // obtain this info outside of synch block since it will involve opening
    // the map files... it is ok if the set of map files changes, because
    // this info is used for optimization... it is ok if map files are missing
    // from the set... can still query and insert into the tablet while this
    // map file operation is happening
    Map<TabletFile,FileUtil.FileInfo> firstAndLastRows = FileUtil.tryToGetFirstAndLastRows(context,
        tableConfiguration, getDatafileManager().getFiles());

    synchronized (this) {
      // java needs tuples ...
      TreeMap<KeyExtent,TabletData> newTablets = new TreeMap<>();

      long t1 = System.currentTimeMillis();

      closeState = CloseState.CLOSING;
      completeClose(true, false);

      Text midRow = splitPoint.row;
      double splitRatio = splitPoint.splitRatio;

      KeyExtent low = new KeyExtent(extent.tableId(), midRow, extent.prevEndRow());
      KeyExtent high = new KeyExtent(extent.tableId(), extent.endRow(), midRow);

      String lowDirectoryName = createTabletDirectoryName(context, midRow);

      // write new tablet information to MetadataTable
      SortedMap<StoredTabletFile,DataFileValue> lowDatafileSizes = new TreeMap<>();
      SortedMap<StoredTabletFile,DataFileValue> highDatafileSizes = new TreeMap<>();
      List<StoredTabletFile> highDatafilesToRemove = new ArrayList<>();

      MetadataTableUtil.splitDatafiles(midRow, splitRatio, firstAndLastRows,
          getDatafileManager().getDatafileSizes(), lowDatafileSizes, highDatafileSizes,
          highDatafilesToRemove);

      log.debug("Files for low split {} {}", low, lowDatafileSizes.keySet());
      log.debug("Files for high split {} {}", high, highDatafileSizes.keySet());

      MetadataTime time = tabletTime.getMetadataTime();

      HashSet<ExternalCompactionId> ecids = new HashSet<>();
      compactable.getExternalCompactionIds(ecids::add);

      MetadataTableUtil.splitTablet(high, extent.prevEndRow(), splitRatio,
          getTabletServer().getContext(), getTabletServer().getLock(), ecids);
      ManagerMetadataUtil.addNewTablet(getTabletServer().getContext(), low, lowDirectoryName,
          getTabletServer().getTabletSession(), lowDatafileSizes, bulkImported, time,
          lastFlushID.get(), lastCompactID.get(), getTabletServer().getLock());
      MetadataTableUtil.finishSplit(high, highDatafileSizes, highDatafilesToRemove,
          getTabletServer().getContext(), getTabletServer().getLock());

      TabletLogger.split(extent, low, high, getTabletServer().getTabletSession());

      newTablets.put(high, new TabletData(dirName, highDatafileSizes, time, lastFlushID.get(),
          lastCompactID.get(), lastLocation, bulkImported));
      newTablets.put(low, new TabletData(lowDirectoryName, lowDatafileSizes, time,
          lastFlushID.get(), lastCompactID.get(), lastLocation, bulkImported));

      long t2 = System.currentTimeMillis();

      log.debug(String.format("offline split time : %6.2f secs", (t2 - t1) / 1000.0));

      closeState = CloseState.COMPLETE;
      return newTablets;
    }
  }

  @Override
  public SortedMap<StoredTabletFile,DataFileValue> getDatafiles() {
    return getDatafileManager().getDatafileSizes();
  }

  @Override
  public void addToYieldMetric(int i) {
    getTabletServer().getScanMetrics().addYield(i);
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

  public long totalQueriesResults() {
    return this.queryResultCount.get();
  }

  public long totalIngest() {
    return this.ingestCount;
  }

  public long totalIngestBytes() {
    return this.ingestBytes;
  }

  public long totalQueryResultsBytes() {
    return this.queryResultBytes.get();
  }

  public long totalScannedCount() {
    return this.scannedCount.get();
  }

  public long totalLookupCount() {
    return this.lookupCount.get();
  }

  // synchronized?
  public void updateRates(long now) {
    queryRate.update(now, this.queryResultCount.get());
    queryByteRate.update(now, this.queryResultBytes.get());
    ingestRate.update(now, ingestCount);
    ingestByteRate.update(now, ingestBytes);
    scannedRate.update(now, this.scannedCount.get());
  }

  public long getSplitCreationTime() {
    return splitCreationTime;
  }

  public void importMapFiles(long tid, Map<TabletFile,MapFileInfo> fileMap, boolean setTime)
      throws IOException {
    Map<TabletFile,DataFileValue> entries = new HashMap<>(fileMap.size());
    List<String> files = new ArrayList<>();

    for (Entry<TabletFile,MapFileInfo> entry : fileMap.entrySet()) {
      entries.put(entry.getKey(), new DataFileValue(entry.getValue().estimatedSize, 0L));
      files.add(entry.getKey().getPathStr());
    }

    // Clients timeout and will think that this operation failed.
    // Don't do it if we spent too long waiting for the lock
    long now = System.nanoTime();
    synchronized (this) {
      if (isClosed()) {
        throw new IOException("tablet " + extent + " is closed");
      }

      long rpcTimeoutNanos = TimeUnit.MILLISECONDS.toNanos(
          (long) (getTabletServer().getConfiguration().getTimeInMillis(Property.GENERAL_RPC_TIMEOUT)
              * 1.1));

      // wait for any files that are bulk importing up to the RPC timeout limit
      while (!Collections.disjoint(bulkImporting, fileMap.keySet())) {
        try {
          wait(1_000);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new IllegalStateException(e);
        }

        long lockWait = System.nanoTime() - now;
        if (lockWait > rpcTimeoutNanos) {
          throw new IOException("Timeout waiting " + TimeUnit.NANOSECONDS.toSeconds(lockWait)
              + " seconds to get tablet lock for " + extent + " " + tid);
        }
      }

      // need to check this again because when wait is called above the lock is released.
      if (isClosed()) {
        throw new IOException("tablet " + extent + " is closed");
      }

      long lockWait = System.nanoTime() - now;
      if (lockWait > rpcTimeoutNanos) {
        throw new IOException("Timeout waiting " + TimeUnit.NANOSECONDS.toSeconds(lockWait)
            + " seconds to get tablet lock for " + extent + " " + tid);
      }

      List<TabletFile> alreadyImported = bulkImported.get(tid);
      if (alreadyImported != null) {
        for (TabletFile entry : alreadyImported) {
          if (fileMap.remove(entry) != null) {
            log.trace("Ignoring import of bulk file already imported: {}", entry);
          }
        }
      }

      if (fileMap.isEmpty()) {
        return;
      }

      incrementWritesInProgress();

      // prevent other threads from processing this file while its added to the metadata table.
      bulkImporting.addAll(fileMap.keySet());
    }
    try {
      tabletServer.updateBulkImportState(files, BulkImportState.LOADING);

      var storedTabletFile = getDatafileManager().importMapFiles(tid, entries, setTime);
      lastMapFileImportTime = System.currentTimeMillis();

      synchronized (this) {
        // only mark the bulk import a success if no exception was thrown
        bulkImported.computeIfAbsent(tid, k -> new ArrayList<>()).addAll(fileMap.keySet());
      }

      if (isSplitPossible()) {
        getTabletServer().executeSplit(this);
      } else {
        compactable.filesAdded(false, storedTabletFile);
      }
    } finally {
      synchronized (this) {
        decrementWritesInProgress();

        if (!bulkImporting.removeAll(fileMap.keySet())) {
          throw new AssertionError(
              "Likely bug in code, always expect to remove something.  Please open an Accumulo issue.");
        }

        tabletServer.removeBulkImportState(files);
      }
    }
  }

  private Set<DfsLogger> currentLogs = new HashSet<>();
  private Set<DfsLogger> otherLogs = Collections.emptySet();

  // An immutable copy of currentLogs + otherLogs. This exists so that removeInUseLogs() does not
  // have to get the tablet lock. See #558
  private volatile Set<DfsLogger> referencedLogs = Collections.emptySet();

  private synchronized void rebuildReferencedLogs() {
    /*
     * Each tablet has the following sets of WALogs. While a WALog exists in one set, garbage
     * collection must be avoided.
     *
     * 1. WALogs for the active in memory map
     *
     * 2. WAlogs for the minor compacting in memory map
     *
     * 3. WAlogs for a newly minor compacted file that is being added to the metadata table.
     *
     * Set 1 is currentLogs. Set 2 is otherLogs. Set 3 only exist in referenced logs as a side
     * effect of not calling this method in beginClearingUnusedLogs() when otherLogs is cleared.
     *
     * Ensuring referencedLogs accurately tracks these sets ensures in use walogs are not GCed.
     */

    var prev = referencedLogs;

    referencedLogs = Stream.concat(currentLogs.stream(), otherLogs.stream())
        .collect(Collectors.toUnmodifiableSet());

    if (TabletLogger.isWalRefLoggingEnabled() && !prev.equals(referencedLogs)) {
      TabletLogger.walRefsChanged(extent,
          referencedLogs.stream().map(DfsLogger::getPath).map(Path::getName).collect(toList()));
    }

  }

  public void removeInUseLogs(Set<DfsLogger> candidates) {
    candidates.removeAll(referencedLogs);
  }

  public void checkIfMinorCompactionNeededForLogs(List<DfsLogger> closedLogs) {

    // grab this outside of tablet lock.
    @SuppressWarnings("deprecation")
    Property prop = tableConfiguration.resolve(Property.TSERV_WAL_MAX_REFERENCED,
        Property.TSERV_WALOG_MAX_REFERENCED, Property.TABLE_MINC_LOGS_MAX);
    int maxLogs = tableConfiguration.getCount(prop);

    String reason = null;
    synchronized (this) {
      if (currentLogs.size() >= maxLogs) {
        reason = "referenced " + currentLogs.size() + " write ahead logs";
      } else if (maxLogs < closedLogs.size()) {
        // If many tablets reference a single WAL, but each tablet references a different WAL then
        // this could result in the tablet server referencing many WALs. For recovery that would
        // mean each tablet had to process lots of WAL. This check looks for a single use of an
        // older WAL and compacts if one is found. The following check assumes the most recent WALs
        // are at the end of the list and ignores these.
        List<DfsLogger> oldClosed = closedLogs.subList(0, closedLogs.size() - maxLogs);
        for (DfsLogger closedLog : oldClosed) {
          if (currentLogs.contains(closedLog)) {
            reason = "referenced at least one old write ahead log " + closedLog.getFileName();
            break;
          }
        }
      }
    }

    if (reason != null) {
      // initiate and log outside of tablet lock
      log.debug("Initiating minor compaction for {} because {}", getExtent(), reason);
      initiateMinorCompaction(MinorCompactionReason.SYSTEM);
    }
  }

  ReentrantLock getLogLock() {
    return logLock;
  }

  Set<String> beginClearingUnusedLogs() {
    Preconditions.checkState(logLock.isHeldByCurrentThread());
    Set<String> unusedLogs = new HashSet<>();

    ArrayList<String> otherLogsCopy = new ArrayList<>();
    ArrayList<String> currentLogsCopy = new ArrayList<>();

    synchronized (this) {
      if (removingLogs) {
        throw new IllegalStateException(
            "Attempted to clear logs when removal of logs in progress on " + extent);
      }

      for (DfsLogger logger : otherLogs) {
        otherLogsCopy.add(logger.toString());
        unusedLogs.add(logger.getMeta());
      }

      for (DfsLogger logger : currentLogs) {
        currentLogsCopy.add(logger.toString());
        unusedLogs.remove(logger.getMeta());
      }

      if (!unusedLogs.isEmpty()) {
        removingLogs = true;
      }
    }

    // do debug logging outside tablet lock
    for (String logger : otherLogsCopy) {
      log.trace("Logs for memory compacted: {} {}", getExtent(), logger);
    }

    for (String logger : currentLogsCopy) {
      log.trace("Logs for current memory: {} {}", getExtent(), logger);
    }

    for (String logger : unusedLogs) {
      log.trace("Logs to be destroyed: {} {}", getExtent(), logger);
    }

    return unusedLogs;
  }

  synchronized void finishClearingUnusedLogs() {
    Preconditions.checkState(logLock.isHeldByCurrentThread());
    removingLogs = false;
    otherLogs = Collections.emptySet();
    rebuildReferencedLogs();
  }

  private boolean removingLogs = false;

  // this lock is basically used to synchronize writing of log info to metadata
  private final ReentrantLock logLock = new ReentrantLock();

  // don't release the lock if this method returns true for success; instead, the caller should
  // clean up by calling finishUpdatingLogsUsed()
  @SuppressFBWarnings(value = "UL_UNRELEASED_LOCK",
      justification = "lock is released by caller calling finishedUpdatingLogsUsed method")
  public boolean beginUpdatingLogsUsed(InMemoryMap memTable, DfsLogger more, boolean mincFinish) {

    boolean releaseLock = true;

    // Should not hold the tablet lock while trying to acquire the log lock because this could lead
    // to deadlock. However there is a path in the code that does this. See #3759
    logLock.lock();

    try {
      synchronized (this) {

        if (isCloseComplete()) {
          throw new IllegalStateException("Can not update logs of closed tablet " + extent);
        }

        boolean addToOther;

        if (memTable == getTabletMemory().getMinCMemTable()) {
          addToOther = true;
        } else if (memTable == getTabletMemory().getMemTable()) {
          addToOther = false;
        } else {
          throw new IllegalArgumentException("Passed in memtable that is not in use for " + extent);
        }

        if (mincFinish) {
          if (addToOther) {
            throw new IllegalStateException("Adding to other logs for mincFinish on " + extent);
          }
          if (!otherLogs.isEmpty()) {
            throw new IllegalStateException("Expect other logs to be 0 when minC finish, but its "
                + otherLogs + " for " + extent);
          }

          // when writing a minc finish event, there is no need to add the log to metadata
          // if nothing has been logged for the tablet since the minor compaction started
          if (currentLogs.isEmpty()) {
            return !releaseLock;
          }
        }

        boolean added;
        boolean contained;
        if (addToOther) {
          added = otherLogs.add(more);
          contained = currentLogs.contains(more);
        } else {
          added = currentLogs.add(more);
          contained = otherLogs.contains(more);
        }

        if (added) {
          rebuildReferencedLogs();
        }

        if (added && !contained) {
          releaseLock = false;
        }

        return !releaseLock;
      }
    } finally {
      if (releaseLock) {
        logLock.unlock();
      }
    }
  }

  public void finishUpdatingLogsUsed() {
    logLock.unlock();
  }

  public void chopFiles() {
    compactable.initiateChop();
  }

  public void compactAll(long compactionId, CompactionConfig compactionConfig) {

    synchronized (this) {
      // This check will quickly ignore stale request from the manager, however its not sufficient
      // for correctness. This same check is done again later at a point when no compactions could
      // be running concurrently to avoid race conditions. If the check passes here, it possible a
      // concurrent compaction could change lastCompactID after the check succeeds.
      if (lastCompactID.get() >= compactionId) {
        return;
      }

      if (isMinorCompactionRunning()) {
        // want to wait for running minc to finish before starting majc, see ACCUMULO-3041
        if (compactionWaitInfo.compactionID == compactionId) {
          if (lastFlushID.get() == compactionWaitInfo.flushID) {
            return;
          }
        } else {
          compactionWaitInfo.compactionID = compactionId;
          compactionWaitInfo.flushID = lastFlushID.get();
          return;
        }
      }

      if (isClosing() || isClosed() || isBeingDeleted()) {
        return;
      }
    }

    // passed all verification checks so initiate compaction
    compactable.initiateUserCompaction(compactionId, compactionConfig);
  }

  public Durability getDurability() {
    return DurabilityImpl.fromString(getTableConfiguration().get(Property.TABLE_DURABILITY));
  }

  public void updateMemoryUsageStats(long size, long mincSize) {
    getTabletResources().updateMemoryUsageStats(this, size, mincSize);
  }

  public long incrementDataSourceDeletions() {
    return dataSourceDeletions.incrementAndGet();
  }

  public void updateTimer(Operation operation, long queued, long start, long count,
      boolean failed) {
    timer.updateTime(operation, queued, start, count, failed);
  }

  public void incrementStatusMajor() {
    timer.incrementStatusMajor();
  }

  TabletServer getTabletServer() {
    return tabletServer;
  }

  public Map<StoredTabletFile,DataFileValue> updatePersistedTime(long bulkTime,
      Map<TabletFile,DataFileValue> paths, long tid) {
    synchronized (timeLock) {
      if (bulkTime > persistedTime) {
        persistedTime = bulkTime;
      }

      return MetadataTableUtil.updateTabletDataFile(tid, extent, paths,
          tabletTime.getMetadataTime(persistedTime), getTabletServer().getContext(),
          getTabletServer().getLock());
    }

  }

  /**
   * Update tablet file data from flush. Returns a StoredTabletFile if there are data entries.
   */
  public Optional<StoredTabletFile> updateTabletDataFile(long maxCommittedTime,
      TabletFile newDatafile, DataFileValue dfv, Set<String> unusedWalLogs, long flushId) {
    synchronized (timeLock) {
      if (maxCommittedTime > persistedTime) {
        persistedTime = maxCommittedTime;
      }

      return ManagerMetadataUtil.updateTabletDataFile(getTabletServer().getContext(), extent,
          newDatafile, dfv, tabletTime.getMetadataTime(persistedTime),
          tabletServer.getTabletSession(), tabletServer.getLock(), unusedWalLogs, lastLocation,
          flushId);
    }

  }

  @Override
  TabletResourceManager getTabletResources() {
    return tabletResources;
  }

  @Override
  public TabletServerScanMetrics getScanMetrics() {
    return getTabletServer().getScanMetrics();
  }

  DatafileManager getDatafileManager() {
    return datafileManager;
  }

  @Override
  public Pair<Long,Map<TabletFile,DataFileValue>> reserveFilesForScan() {
    return getDatafileManager().reserveFilesForScan();
  }

  @Override
  public void returnFilesForScan(long scanId) {
    getDatafileManager().returnFilesForScan(scanId);
  }

  public MetadataUpdateCount getUpdateCount() {
    return getDatafileManager().getUpdateCount();
  }

  TabletMemory getTabletMemory() {
    return tabletMemory;
  }

  @Override
  public List<InMemoryMap.MemoryIterator> getMemIterators(SamplerConfigurationImpl samplerConfig) {
    return getTabletMemory().getIterators(samplerConfig);
  }

  @Override
  public void returnMemIterators(List<InMemoryMap.MemoryIterator> iters) {
    getTabletMemory().returnIterators(iters);
  }

  public long getAndUpdateTime() {
    return tabletTime.getAndUpdateTime();
  }

  public void flushComplete(long flushId) {
    lastLocation = null;
    dataSourceDeletions.incrementAndGet();
    tabletMemory.finishedMinC();
    lastFlushID.set(flushId);
    computeNumEntries();
  }

  public Location resetLastLocation() {
    Location result = lastLocation;
    lastLocation = null;
    return result;
  }

  public synchronized void setLastCompactionID(Long compactionId) {
    if (compactionId != null) {
      this.lastCompactID.set(compactionId);
    }
  }

  public void minorCompactionWaitingToStart() {
    minorCompactionState = CompactionState.WAITING_TO_START;
  }

  public void minorCompactionStarted() {
    minorCompactionState = CompactionState.IN_PROGRESS;
  }

  public void minorCompactionComplete() {
    minorCompactionState = null;
  }

  public TabletStats getTabletStats() {
    return timer.getTabletStats();
  }

  private static String createTabletDirectoryName(ServerContext context, Text endRow) {
    if (endRow == null) {
      return ServerColumnFamily.DEFAULT_TABLET_DIR_NAME;
    } else {
      UniqueNameAllocator namer = context.getUniqueNameAllocator();
      return Constants.GENERATED_TABLET_DIRECTORY_PREFIX + namer.getNextName();
    }
  }

  public Set<Long> getBulkIngestedTxIds() {
    return bulkImported.keySet();
  }

  public void cleanupBulkLoadedFiles(Set<Long> tids) {
    bulkImported.keySet().removeAll(tids);
  }

  public String getDirName() {
    return dirName;
  }

  public Compactable asCompactable() {
    return compactable;
  }
}

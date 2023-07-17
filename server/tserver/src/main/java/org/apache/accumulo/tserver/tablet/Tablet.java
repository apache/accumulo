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

import static com.google.common.util.concurrent.Uninterruptibles.sleepUninterruptibly;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.stream.Collectors.toList;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.Durability;
import org.apache.accumulo.core.client.admin.TabletHostingGoal;
import org.apache.accumulo.core.clientImpl.DurabilityImpl;
import org.apache.accumulo.core.conf.AccumuloConfiguration.Deriver;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.constraints.Violations;
import org.apache.accumulo.core.data.ColumnUpdate;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.file.FilePrefix;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iteratorsImpl.system.SourceSwitchingIterator;
import org.apache.accumulo.core.logging.TabletLogger;
import org.apache.accumulo.core.manager.state.tables.TableState;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.metadata.ReferencedTabletFile;
import org.apache.accumulo.core.metadata.StoredTabletFile;
import org.apache.accumulo.core.metadata.schema.DataFileValue;
import org.apache.accumulo.core.metadata.schema.TabletMetadata;
import org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType;
import org.apache.accumulo.core.metadata.schema.TabletMetadata.Location;
import org.apache.accumulo.core.sample.impl.SamplerConfigurationImpl;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.spi.scan.ScanDispatch;
import org.apache.accumulo.core.tabletserver.log.LogEntry;
import org.apache.accumulo.core.tabletserver.thrift.TabletStats;
import org.apache.accumulo.core.trace.TraceUtil;
import org.apache.accumulo.core.util.Pair;
import org.apache.accumulo.server.compaction.CompactionStats;
import org.apache.accumulo.server.fs.VolumeUtil;
import org.apache.accumulo.server.fs.VolumeUtil.TabletFiles;
import org.apache.accumulo.server.problems.ProblemReport;
import org.apache.accumulo.server.problems.ProblemReports;
import org.apache.accumulo.server.problems.ProblemType;
import org.apache.accumulo.server.tablets.ConditionCheckerContext.ConditionChecker;
import org.apache.accumulo.server.tablets.TabletNameGenerator;
import org.apache.accumulo.server.tablets.TabletTime;
import org.apache.accumulo.server.util.ManagerMetadataUtil;
import org.apache.accumulo.server.util.MetadataTableUtil;
import org.apache.accumulo.tserver.InMemoryMap;
import org.apache.accumulo.tserver.MinorCompactionReason;
import org.apache.accumulo.tserver.TabletServer;
import org.apache.accumulo.tserver.TabletServerResourceManager.TabletResourceManager;
import org.apache.accumulo.tserver.TabletStatsKeeper;
import org.apache.accumulo.tserver.TabletStatsKeeper.Operation;
import org.apache.accumulo.tserver.TservConstraintEnv;
import org.apache.accumulo.tserver.constraints.ConstraintChecker;
import org.apache.accumulo.tserver.log.DfsLogger;
import org.apache.accumulo.tserver.metrics.TabletServerMinCMetrics;
import org.apache.accumulo.tserver.metrics.TabletServerScanMetrics;
import org.apache.accumulo.tserver.scan.ScanParameters;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
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
  private final Set<Path> checkedTabletDirs = new ConcurrentSkipListSet<>();

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

  private final AtomicLong lastFlushID = new AtomicLong(-1);

  enum CompactionState {
    WAITING_TO_START, IN_PROGRESS
  }

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

  private volatile long numEntries = 0;
  private volatile long numEntriesInMemory = 0;

  private final int logId;

  // TODO: User can change this, how does it get updated?
  private final TabletHostingGoal goal;

  public int getLogId() {
    return logId;
  }

  public static class LookupResult {
    public List<Range> unfinishedRanges = new ArrayList<>();
    public long bytesAdded = 0;
    public long dataSize = 0;
    public boolean closed = false;
  }

  ReferencedTabletFile getNextDataFilename(FilePrefix prefix) throws IOException {
    return TabletNameGenerator.getNextDataFilename(prefix, context, extent, dirName,
        dir -> checkTabletDir(new Path(dir)));
  }

  private void checkTabletDir(Path path) {
    try {
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
    } catch (IOException e) {
      throw new UncheckedIOException(e);
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
    this.tabletTime = TabletTime.getInstance(data.getTime());
    this.persistedTime = tabletTime.getTime();
    this.logId = tabletServer.createLogId();
    this.goal = data.getHostingGoal();

    // translate any volume changes
    TabletFiles tabletPaths =
        new TabletFiles(data.getDirectoryName(), data.getLogEntries(), data.getDataFiles());
    tabletPaths = VolumeUtil.updateTabletVolumes(tabletServer.getContext(), tabletServer.getLock(),
        extent, tabletPaths);

    this.dirName = data.getDirectoryName();

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
        for (StoredTabletFile ref : datafiles.keySet()) {
          absPaths.add(ref.getNormalizedPathStr());
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

        if (entriesUsedOnTablet.get() == 0) {
          log.debug("No replayed mutations applied, removing unused entries for {}", extent);
          MetadataTableUtil.removeUnusedWALEntries(getTabletServer().getContext(), extent,
              logEntries, tabletServer.getLock());
          logEntries.clear();
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
  }

  public void checkConditions(ConditionChecker checker, Authorizations authorizations,
      AtomicBoolean iFlag) throws IOException {

    ScanParameters scanParams = new ScanParameters(-1, authorizations, Collections.emptySet(), null,
        null, false, null, -1, null);
    scanParams.setScanDispatch(ScanDispatch.builder().build());

    ScanDataSource dataSource = createDataSource(scanParams, false, iFlag);

    try {
      SortedKeyValueIterator<Key,Value> iter = new SourceSwitchingIterator(dataSource);
      checker.check(iter);
    } catch (IOException ioe) {
      dataSource.close(true);
      throw ioe;
    } finally {
      // code in finally block because always want
      // to return data files, even when exception is thrown
      dataSource.close(false);
    }
  }

  DataFileValue minorCompact(InMemoryMap memTable, ReferencedTabletFile tmpDatafile,
      ReferencedTabletFile newDatafile, long queued, CommitSession commitSession, long flushId,
      MinorCompactionReason mincReason) {
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
        getDatafileManager().bringMinorCompactionOnline(tmpDatafile, newDatafile,
            new DataFileValue(stats.getFileSize(), stats.getEntriesWritten()), commitSession,
            flushId);
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
   * Closes the data files associated with a Tablet. If saveState is true, a minor compaction is
   * performed.
   */
  @Override
  public void close(boolean saveState) throws IOException {
    initiateClose(saveState);
    completeClose(saveState);
    log.info("Tablet {} closed.", this.extent);
  }

  void initiateClose(boolean saveState) {
    log.trace("initiateClose(saveState={}) {}", saveState, getExtent());

    MinorCompactionTask mct = null;
    synchronized (this) {
      if (isClosed() || isClosing()) {
        String msg = "Tablet " + getExtent() + " already " + closeState;
        throw new IllegalStateException(msg);
      }

      // enter the closing state, no splits or minor compactions can start
      closeState = CloseState.CLOSING;
      this.notifyAll();
    }

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

      if (!saveState || getTabletMemory().getMemTable().getNumEntries() == 0) {
        return;
      }

      getTabletMemory().waitForMinC();

      // calling this.wait() in waitForMinC() releases the lock, ensure things are as expected when
      // the lock is obtained again
      Preconditions.checkState(closeState == CloseState.CLOSING);

      try {
        mct = prepareForMinC(getFlushID(), MinorCompactionReason.CLOSE);
      } catch (NoNodeException e) {
        throw new RuntimeException("Exception on " + extent + " during prep for MinC", e);
      }
    }

    // do minor compaction outside of synch block so that tablet can be read and written to while
    // compaction runs
    mct.run();
  }

  private boolean closeCompleting = false;

  synchronized void completeClose(boolean saveState) throws IOException {

    if (!isClosing() || isCloseComplete() || closeCompleting) {
      throw new IllegalStateException("Bad close state " + closeState + " on tablet " + extent);
    }

    log.trace("completeClose(saveState={}) {}", saveState, extent);

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

    // close data files
    getTabletResources().close();

    closeState = CloseState.COMPLETE;
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

  public boolean isMinorCompactionQueued() {
    return minorCompactionState == CompactionState.WAITING_TO_START;
  }

  public boolean isMinorCompactionRunning() {
    return minorCompactionState == CompactionState.IN_PROGRESS;
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

  public synchronized void updateRates(long now) {
    queryRate.update(now, this.queryResultCount.get());
    queryByteRate.update(now, this.queryResultBytes.get());
    ingestRate.update(now, ingestCount);
    ingestByteRate.update(now, ingestBytes);
    scannedRate.update(now, this.scannedCount.get());
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
    int maxLogs = tableConfiguration.getCount(Property.TSERV_WAL_MAX_REFERENCED);

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

  Set<String> beginClearingUnusedLogs() {
    Set<String> unusedLogs = new HashSet<>();

    ArrayList<String> otherLogsCopy = new ArrayList<>();
    ArrayList<String> currentLogsCopy = new ArrayList<>();

    // do not hold tablet lock while acquiring the log lock
    logLock.lock();

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

      otherLogs = Collections.emptySet();
      // Intentionally NOT calling rebuildReferencedLogs() here as that could cause GC of in use
      // walogs(see #539). The clearing of otherLogs is reflected in ReferencedLogs when
      // finishClearingUnusedLogs() calls rebuildReferencedLogs(). See the comments in
      // rebuildReferencedLogs() for more info.

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
    removingLogs = false;
    rebuildReferencedLogs();
    logLock.unlock();
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

    // do not hold tablet lock while acquiring the log lock
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

  public Durability getDurability() {
    return DurabilityImpl.fromString(getTableConfiguration().get(Property.TABLE_DURABILITY));
  }

  public void updateMemoryUsageStats(long size, long mincSize) {
    getTabletResources().updateMemoryUsageStats(this, size, mincSize);
  }

  TabletServer getTabletServer() {
    return tabletServer;
  }

  /**
   * Update tablet file data from flush. Returns a StoredTabletFile if there are data entries.
   */
  public Optional<StoredTabletFile> updateTabletDataFile(long maxCommittedTime,
      ReferencedTabletFile newDatafile, DataFileValue dfv, Set<String> unusedWalLogs,
      long flushId) {
    synchronized (timeLock) {
      if (maxCommittedTime > persistedTime) {
        persistedTime = maxCommittedTime;
      }

      return ManagerMetadataUtil.updateTabletDataFile(getTabletServer().getContext(), extent,
          newDatafile, dfv, tabletTime.getMetadataTime(persistedTime),
          tabletServer.getClientAddressString(), tabletServer.getLock(), unusedWalLogs,
          lastLocation, flushId);
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
  public Pair<Long,Map<StoredTabletFile,DataFileValue>> reserveFilesForScan() {
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

  public void flushComplete(long flushId) {
    lastLocation = null;
    dataSourceDeletions.incrementAndGet();
    tabletMemory.finishedMinC();
    lastFlushID.set(flushId);
    computeNumEntries();
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

  public boolean isOnDemand() {
    return goal == TabletHostingGoal.ONDEMAND;
  }

  public void refresh(List<StoredTabletFile> scanEntries) {
    if (isClosing() || isClosed()) {
      // TODO this is just a best effort could close after this check, its a race condition.
      // Intentionally not being handled ATM.
      return;
    }

    // ELASTICITY_TODO instead of reading the tablet metadata in this method, could just invalidate
    // it forcing next scan to read it.

    // ELASTICITY_TODO this entire method is a hack at the moment with race conditions. Want to
    // move towards the tablet just using a cached TabletMetadata object and have a central orderly
    // thread safe way to update it within the tablet in response to external refresh request and
    // internal events like minor compactions. Would probably be easiest to implement this after
    // removing bulk import, split, and compactions from the tablet server. For now just leave it
    // as a hack instead of trying to make it work correctly with the current tablet code.
    TabletMetadata tabletMetadata =
        getContext().getAmple().readTablet(getExtent(), ColumnType.FILES);

    // TODO this could have race conditions with minor compactions. Intentionally
    // not being handled ATM.
    getDatafileManager().setFilesHack(tabletMetadata.getFilesMap());

    // ELASTICITY_TODO this was in the code that brought a major compaction online. Adding it here
    // w/o looking into the larger context too much.
    dataSourceDeletions.incrementAndGet();

    // ELASTICITY_TODO this was in the code that brought a major compaction online. Adding it here
    // w/o looking into the larger context too much.
    computeNumEntries();

    if (!scanEntries.isEmpty()) {
      // ELASTICITY_TODO this is a temporary hack. Should not always remove scan entries added by a
      // compaction, need to check and see if they are actually in use by a scan. If in use need to
      // add to a set to remove later. Also should use a conditional mutation to update, did not
      // bother using conditional mutation as this is temporary.
      var tabletMutator = getContext().getAmple().mutateTablet(extent);
      scanEntries.forEach(tabletMutator::deleteScan);
      tabletMutator.mutate();
    }
  }
}

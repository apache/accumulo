/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
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
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;
import static org.apache.accumulo.fate.util.UtilWaitThread.sleepUninterruptibly;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.Durability;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.admin.CompactionStrategyConfig;
import org.apache.accumulo.core.clientImpl.DurabilityImpl;
import org.apache.accumulo.core.clientImpl.Tables;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.AccumuloConfiguration.Deriver;
import org.apache.accumulo.core.conf.ConfigurationCopy;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.constraints.Violations;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.ColumnUpdate;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.dataImpl.thrift.MapFileInfo;
import org.apache.accumulo.core.file.FileOperations;
import org.apache.accumulo.core.file.FileSKVIterator;
import org.apache.accumulo.core.iterators.IterationInterruptedException;
import org.apache.accumulo.core.iterators.IteratorUtil.IteratorScope;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.YieldCallback;
import org.apache.accumulo.core.iteratorsImpl.system.SourceSwitchingIterator;
import org.apache.accumulo.core.logging.TabletLogger;
import org.apache.accumulo.core.master.thrift.BulkImportState;
import org.apache.accumulo.core.master.thrift.TabletLoadState;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.metadata.StoredTabletFile;
import org.apache.accumulo.core.metadata.TabletFile;
import org.apache.accumulo.core.metadata.schema.DataFileValue;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.ServerColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataTime;
import org.apache.accumulo.core.protobuf.ProtobufUtil;
import org.apache.accumulo.core.replication.ReplicationConfigurationUtil;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.accumulo.core.spi.cache.BlockCache;
import org.apache.accumulo.core.spi.scan.ScanDirectives;
import org.apache.accumulo.core.tabletserver.log.LogEntry;
import org.apache.accumulo.core.tabletserver.thrift.TabletStats;
import org.apache.accumulo.core.trace.TraceUtil;
import org.apache.accumulo.core.util.LocalityGroupUtil;
import org.apache.accumulo.core.util.Pair;
import org.apache.accumulo.core.util.ShutdownUtil;
import org.apache.accumulo.core.util.ratelimit.RateLimiter;
import org.apache.accumulo.core.volume.Volume;
import org.apache.accumulo.server.ServerConstants;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.conf.TableConfiguration;
import org.apache.accumulo.server.fs.VolumeChooserEnvironment;
import org.apache.accumulo.server.fs.VolumeChooserEnvironmentImpl;
import org.apache.accumulo.server.fs.VolumeManager;
import org.apache.accumulo.server.fs.VolumeUtil;
import org.apache.accumulo.server.fs.VolumeUtil.TabletFiles;
import org.apache.accumulo.server.master.state.TServerInstance;
import org.apache.accumulo.server.master.tableOps.UserCompactionConfig;
import org.apache.accumulo.server.problems.ProblemReport;
import org.apache.accumulo.server.problems.ProblemReports;
import org.apache.accumulo.server.problems.ProblemType;
import org.apache.accumulo.server.replication.StatusUtil;
import org.apache.accumulo.server.replication.proto.Replication.Status;
import org.apache.accumulo.server.tablets.TabletTime;
import org.apache.accumulo.server.tablets.UniqueNameAllocator;
import org.apache.accumulo.server.util.FileUtil;
import org.apache.accumulo.server.util.MasterMetadataUtil;
import org.apache.accumulo.server.util.MetadataTableUtil;
import org.apache.accumulo.server.util.ReplicationTableUtil;
import org.apache.accumulo.start.classloader.vfs.AccumuloVFSClassLoader;
import org.apache.accumulo.tserver.ConditionCheckerContext.ConditionChecker;
import org.apache.accumulo.tserver.InMemoryMap;
import org.apache.accumulo.tserver.MinorCompactionReason;
import org.apache.accumulo.tserver.TabletServer;
import org.apache.accumulo.tserver.TabletServerResourceManager.TabletResourceManager;
import org.apache.accumulo.tserver.TabletStatsKeeper;
import org.apache.accumulo.tserver.TabletStatsKeeper.Operation;
import org.apache.accumulo.tserver.TooManyFilesException;
import org.apache.accumulo.tserver.TservConstraintEnv;
import org.apache.accumulo.tserver.compaction.CompactionPlan;
import org.apache.accumulo.tserver.compaction.CompactionStrategy;
import org.apache.accumulo.tserver.compaction.DefaultCompactionStrategy;
import org.apache.accumulo.tserver.compaction.MajorCompactionReason;
import org.apache.accumulo.tserver.compaction.MajorCompactionRequest;
import org.apache.accumulo.tserver.compaction.WriteParameters;
import org.apache.accumulo.tserver.constraints.ConstraintChecker;
import org.apache.accumulo.tserver.log.DfsLogger;
import org.apache.accumulo.tserver.mastermessage.TabletStatusMessage;
import org.apache.accumulo.tserver.metrics.TabletServerMinCMetrics;
import org.apache.accumulo.tserver.scan.ScanParameters;
import org.apache.accumulo.tserver.tablet.Compactor.CompactionCanceledException;
import org.apache.accumulo.tserver.tablet.Compactor.CompactionEnv;
import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.htrace.Trace;
import org.apache.htrace.TraceScope;
import org.apache.htrace.impl.ProbabilitySampler;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.cache.Cache;
import com.google.common.collect.ImmutableSet;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * Provide access to a single row range in a living TabletServer.
 */
public class Tablet {
  private static final Logger log = LoggerFactory.getLogger(Tablet.class);

  private static final byte[] EMPTY_BYTES = new byte[0];

  private final TabletServer tabletServer;
  private final ServerContext context;
  private final KeyExtent extent;
  private final TabletResourceManager tabletResources;
  private final DatafileManager datafileManager;
  private final TableConfiguration tableConfiguration;
  private final String dirName;

  private final TabletMemory tabletMemory;

  private final TabletTime tabletTime;
  private final Object timeLock = new Object();
  private long persistedTime;

  private TServerInstance lastLocation = null;
  private volatile Set<Path> checkedTabletDirs = new ConcurrentSkipListSet<>();

  private final AtomicLong dataSourceDeletions = new AtomicLong(0);

  public long getDataSourceDeletions() {
    return dataSourceDeletions.get();
  }

  private final Set<ScanDataSource> activeScans = new HashSet<>();

  private enum CloseState {
    OPEN, CLOSING, CLOSED, COMPLETE
  }

  private volatile CloseState closeState = CloseState.OPEN;

  private boolean updatingFlushID = false;

  private long lastFlushID = -1;
  private long lastCompactID = -1;

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

  private volatile CompactionState minorCompactionState = null;
  private volatile CompactionState majorCompactionState = null;

  private final Set<MajorCompactionReason> majorCompactionQueued =
      Collections.synchronizedSet(EnumSet.noneOf(MajorCompactionReason.class));

  private final Deriver<ConstraintChecker> constraintChecker;

  private int writesInProgress = 0;

  private final TabletStatsKeeper timer = new TabletStatsKeeper();

  private final Rate queryRate = new Rate(0.95);
  private long queryCount = 0;

  private final Rate queryByteRate = new Rate(0.95);
  private long queryBytes = 0;

  private final Rate ingestRate = new Rate(0.95);
  private long ingestCount = 0;

  private final Rate ingestByteRate = new Rate(0.95);
  private long ingestBytes = 0;

  private final Deriver<byte[]> defaultSecurityLabel;

  private long lastMinorCompactionFinishTime = 0;
  private long lastMapFileImportTime = 0;

  private volatile long numEntries = 0;
  private volatile long numEntriesInMemory = 0;

  private final Rate scannedRate = new Rate(0.95);
  private final AtomicLong scannedCount = new AtomicLong(0);

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
        new VolumeChooserEnvironmentImpl(extent.getTableId(), extent.getEndRow(), context);
    String dirUri =
        tabletServer.getFileSystem().choose(chooserEnv, ServerConstants.getBaseUris(context))
            + Constants.HDFS_TABLES_DIR + Path.SEPARATOR + extent.getTableId() + Path.SEPARATOR
            + dirName;
    checkTabletDir(new Path(dirUri));
    return dirUri;
  }

  TabletFile getNextMapFilename(String prefix) throws IOException {
    String extension = FileOperations.getNewFileExtension(tableConfiguration);
    return new TabletFile(new Path(chooseTabletDir() + "/" + prefix
        + context.getUniqueNameAllocator().getNextName() + "." + extension));
  }

  private void checkTabletDir(Path path) throws IOException {
    if (!checkedTabletDirs.contains(path)) {
      FileStatus[] files = null;
      try {
        files = getTabletServer().getFileSystem().listStatus(path);
      } catch (FileNotFoundException ex) {
        // ignored
      }

      if (files == null) {
        log.debug("Tablet {} had no dir, creating {}", extent, path);

        getTabletServer().getFileSystem().mkdirs(path);
      }
      checkedTabletDirs.add(path);
    }
  }

  public Tablet(final TabletServer tabletServer, final KeyExtent extent,
      final TabletResourceManager trm, TabletData data)
      throws IOException, IllegalArgumentException {

    this.tabletServer = tabletServer;
    this.context = tabletServer.getContext();
    this.extent = extent;
    this.tabletResources = trm;
    this.lastLocation = data.getLastLocation();
    this.lastFlushID = data.getFlushID();
    this.lastCompactID = data.getCompactID();
    this.splitCreationTime = data.getSplitTime();
    this.tabletTime = TabletTime.getInstance(data.getTime());
    this.persistedTime = tabletTime.getTime();
    this.logId = tabletServer.createLogId();

    TableConfiguration tblConf = tabletServer.getTableConfiguration(extent);
    if (tblConf == null) {
      Tables.clearCache(tabletServer.getContext());
      tblConf = tabletServer.getTableConfiguration(extent);
      requireNonNull(tblConf, "Could not get table configuration for " + extent.getTableId());
    }

    this.tableConfiguration = tblConf;

    // translate any volume changes
    boolean replicationEnabled =
        ReplicationConfigurationUtil.isEnabled(extent, this.tableConfiguration);
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

    constraintChecker = tableConfiguration.newDeriver(conf -> new ConstraintChecker(conf));

    if (extent.isMeta()) {
      defaultSecurityLabel = () -> EMPTY_BYTES;
    } else {
      defaultSecurityLabel = tableConfiguration.newDeriver(
          conf -> new ColumnVisibility(conf.get(Property.TABLE_DEFAULT_SCANTIME_VISIBILITY))
              .getExpression());
    }

    tabletMemory = new TabletMemory(this);

    if (!logEntries.isEmpty()) {
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

        tabletServer.recover(this.getTabletServer().getFileSystem(), extent, logEntries, absPaths,
            m -> {
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
        } else if (ReplicationConfigurationUtil.isEnabled(extent,
            tabletServer.getTableConfiguration(extent))) {
          // record that logs may have data for this extent
          Status status = StatusUtil.openWithUnknownLength();
          for (LogEntry logEntry : logEntries) {
            log.debug("Writing updated status to metadata table for {} {}", logEntry.filename,
                ProtobufUtil.toString(status));
            ReplicationTableUtil.updateFiles(tabletServer.getContext(), extent, logEntry.filename,
                status);
          }
        }

      } catch (Throwable t) {
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

    String contextName = tableConfiguration.get(Property.TABLE_CLASSPATH);
    if (contextName != null && !contextName.equals("")) {
      // initialize context classloader, instead of possibly waiting for it to initialize for a scan
      // TODO this could hang, causing other tablets to fail to load - ACCUMULO-1292
      AccumuloVFSClassLoader.getContextManager().getClassLoader(contextName);
    }

    // do this last after tablet is completely setup because it
    // could cause major compaction to start
    datafileManager = new DatafileManager(this, datafiles);

    computeNumEntries();

    getDatafileManager().removeFilesAfterScan(data.getScanFiles());

    // look for hints of a failure on the previous tablet server
    if (!logEntries.isEmpty() || needsMajorCompaction(MajorCompactionReason.NORMAL)) {
      // look for any temp files hanging around
      removeOldTemporaryFiles();
    }
  }

  public ServerContext getContext() {
    return context;
  }

  private void removeOldTemporaryFiles() {
    // remove any temporary files created by a previous tablet server
    try {
      Collection<Volume> volumes = getTabletServer().getFileSystem().getVolumes();
      for (Volume volume : volumes) {
        String dirUri = volume.getBasePath() + Constants.HDFS_TABLES_DIR + Path.SEPARATOR
            + extent.getTableId() + Path.SEPARATOR + dirName;

        for (FileStatus tmp : getTabletServer().getFileSystem()
            .globStatus(new Path(dirUri, "*_tmp"))) {
          try {
            log.debug("Removing old temp file {}", tmp.getPath());
            getTabletServer().getFileSystem().delete(tmp.getPath());
          } catch (IOException ex) {
            log.error("Unable to remove old temp file " + tmp.getPath() + ": " + ex);
          }
        }
      }
    } catch (IOException ex) {
      log.error("Error scanning for old temp files", ex);
    }
  }

  private LookupResult lookup(SortedKeyValueIterator<Key,Value> mmfi, List<Range> ranges,
      List<KVEntry> results, ScanParameters scanParams, long maxResultsSize) throws IOException {

    LookupResult lookupResult = new LookupResult();

    boolean exceededMemoryUsage = false;
    boolean tabletClosed = false;

    Set<ByteSequence> cfset = null;
    if (!scanParams.getColumnSet().isEmpty()) {
      cfset = LocalityGroupUtil.families(scanParams.getColumnSet());
    }

    long batchTimeOut = scanParams.getBatchTimeOut();

    long timeToRun = TimeUnit.MILLISECONDS.toNanos(batchTimeOut);
    long startNanos = System.nanoTime();

    if (batchTimeOut <= 0 || batchTimeOut == Long.MAX_VALUE) {
      batchTimeOut = 0;
    }

    // determine if the iterator supported yielding
    YieldCallback<Key> yield = new YieldCallback<>();
    mmfi.enableYielding(yield);
    boolean yielded = false;

    for (Range range : ranges) {

      boolean timesUp = batchTimeOut > 0 && (System.nanoTime() - startNanos) > timeToRun;

      if (exceededMemoryUsage || tabletClosed || timesUp || yielded) {
        lookupResult.unfinishedRanges.add(range);
        continue;
      }

      int entriesAdded = 0;

      try {
        if (cfset != null) {
          mmfi.seek(range, cfset, true);
        } else {
          mmfi.seek(range, LocalityGroupUtil.EMPTY_CF_SET, false);
        }

        while (mmfi.hasTop()) {
          if (yield.hasYielded()) {
            throw new IOException("Coding error: hasTop returned true but has yielded at "
                + yield.getPositionAndReset());
          }
          Key key = mmfi.getTopKey();

          KVEntry kve = new KVEntry(key, mmfi.getTopValue());
          results.add(kve);
          entriesAdded++;
          lookupResult.bytesAdded += kve.estimateMemoryUsed();
          lookupResult.dataSize += kve.numBytes();

          exceededMemoryUsage = lookupResult.bytesAdded > maxResultsSize;

          timesUp = batchTimeOut > 0 && (System.nanoTime() - startNanos) > timeToRun;

          if (exceededMemoryUsage || timesUp) {
            addUnfinishedRange(lookupResult, range, key);
            break;
          }

          mmfi.next();
        }

        if (yield.hasYielded()) {
          yielded = true;
          Key yieldPosition = yield.getPositionAndReset();
          if (!range.contains(yieldPosition)) {
            throw new IOException("Underlying iterator yielded to a position outside of its range: "
                + yieldPosition + " not in " + range);
          }
          if (!results.isEmpty()
              && yieldPosition.compareTo(results.get(results.size() - 1).getKey()) <= 0) {
            throw new IOException("Underlying iterator yielded to a position"
                + " that does not follow the last key returned: " + yieldPosition + " <= "
                + results.get(results.size() - 1).getKey());
          }
          addUnfinishedRange(lookupResult, range, yieldPosition);

          log.debug("Scan yield detected at position " + yieldPosition);
          getTabletServer().getScanMetrics().addYield(1);
        }
      } catch (TooManyFilesException tmfe) {
        // treat this as a closed tablet, and let the client retry
        log.warn("Tablet {} has too many files, batch lookup can not run", getExtent());
        handleTabletClosedDuringScan(results, lookupResult, exceededMemoryUsage, range,
            entriesAdded);
        tabletClosed = true;
      } catch (IOException ioe) {
        if (ShutdownUtil.isShutdownInProgress()) {
          // assume HDFS shutdown hook caused this exception
          log.debug("IOException while shutdown in progress", ioe);
          handleTabletClosedDuringScan(results, lookupResult, exceededMemoryUsage, range,
              entriesAdded);
          tabletClosed = true;
        } else {
          throw ioe;
        }
      } catch (IterationInterruptedException iie) {
        if (isClosed()) {
          handleTabletClosedDuringScan(results, lookupResult, exceededMemoryUsage, range,
              entriesAdded);
          tabletClosed = true;
        } else {
          throw iie;
        }
      } catch (TabletClosedException tce) {
        handleTabletClosedDuringScan(results, lookupResult, exceededMemoryUsage, range,
            entriesAdded);
        tabletClosed = true;
      }

    }

    return lookupResult;
  }

  private void handleTabletClosedDuringScan(List<KVEntry> results, LookupResult lookupResult,
      boolean exceededMemoryUsage, Range range, int entriesAdded) {
    if (exceededMemoryUsage) {
      throw new IllegalStateException(
          "Tablet " + extent + "should not exceed memory usage or close, not both");
    }

    if (entriesAdded > 0) {
      addUnfinishedRange(lookupResult, range, results.get(results.size() - 1).getKey());
    } else {
      lookupResult.unfinishedRanges.add(range);
    }

    lookupResult.closed = true;
  }

  private void addUnfinishedRange(LookupResult lookupResult, Range range, Key key) {
    if (range.getEndKey() == null || key.compareTo(range.getEndKey()) < 0) {
      Range nlur = new Range(new Key(key), false, range.getEndKey(), range.isEndKeyInclusive());
      lookupResult.unfinishedRanges.add(nlur);
    }
  }

  public void checkConditions(ConditionChecker checker, Authorizations authorizations,
      AtomicBoolean iFlag) throws IOException {

    ScanParameters scanParams = new ScanParameters(-1, authorizations, Collections.emptySet(), null,
        null, false, null, -1, null);
    scanParams.setScanDirectives(ScanDirectives.builder().build());

    ScanDataSource dataSource = new ScanDataSource(this, scanParams, false, iFlag);

    try {
      SortedKeyValueIterator<Key,Value> iter = new SourceSwitchingIterator(dataSource);
      checker.check(iter);
    } catch (IOException ioe) {
      dataSource.close(true);
      throw ioe;
    } finally {
      // code in finally block because always want
      // to return mapfiles, even when exception is thrown
      dataSource.close(false);
    }
  }

  public LookupResult lookup(List<Range> ranges, List<KVEntry> results, ScanParameters scanParams,
      long maxResultSize, AtomicBoolean interruptFlag) throws IOException {

    if (ranges.isEmpty()) {
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

    ScanDataSource dataSource = new ScanDataSource(this, scanParams, true, interruptFlag);

    LookupResult result = null;

    try {
      SortedKeyValueIterator<Key,Value> iter = new SourceSwitchingIterator(dataSource);
      result = lookup(iter, ranges, results, scanParams, maxResultSize);
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
        if (result != null) {
          queryBytes += result.dataSize;
        }
      }
    }
  }

  Batch nextBatch(SortedKeyValueIterator<Key,Value> iter, Range range, ScanParameters scanParams)
      throws IOException {

    // log.info("In nextBatch..");

    long batchTimeOut = scanParams.getBatchTimeOut();

    long timeToRun = TimeUnit.MILLISECONDS.toNanos(batchTimeOut);
    long startNanos = System.nanoTime();

    if (batchTimeOut == Long.MAX_VALUE || batchTimeOut <= 0) {
      batchTimeOut = 0;
    }
    List<KVEntry> results = new ArrayList<>();
    Key key = null;

    Value value;
    long resultSize = 0L;
    long resultBytes = 0L;

    long maxResultsSize = tableConfiguration.getAsBytes(Property.TABLE_SCAN_MAXMEM);

    Key continueKey = null;
    boolean skipContinueKey = false;

    YieldCallback<Key> yield = new YieldCallback<>();

    // we cannot yield if we are in isolation mode
    if (!scanParams.isIsolated()) {
      iter.enableYielding(yield);
    }

    if (scanParams.getColumnSet().isEmpty()) {
      iter.seek(range, LocalityGroupUtil.EMPTY_CF_SET, false);
    } else {
      iter.seek(range, LocalityGroupUtil.families(scanParams.getColumnSet()), true);
    }

    while (iter.hasTop()) {
      if (yield.hasYielded()) {
        throw new IOException(
            "Coding error: hasTop returned true but has yielded at " + yield.getPositionAndReset());
      }
      value = iter.getTopValue();
      key = iter.getTopKey();

      KVEntry kvEntry = new KVEntry(key, value); // copies key and value
      results.add(kvEntry);
      resultSize += kvEntry.estimateMemoryUsed();
      resultBytes += kvEntry.numBytes();

      boolean timesUp = batchTimeOut > 0 && (System.nanoTime() - startNanos) >= timeToRun;

      if (resultSize >= maxResultsSize || results.size() >= scanParams.getMaxEntries() || timesUp) {
        continueKey = new Key(key);
        skipContinueKey = true;
        break;
      }

      iter.next();
    }

    if (yield.hasYielded()) {
      continueKey = new Key(yield.getPositionAndReset());
      skipContinueKey = true;
      if (!range.contains(continueKey)) {
        throw new IOException("Underlying iterator yielded to a position outside of its range: "
            + continueKey + " not in " + range);
      }
      if (!results.isEmpty()
          && continueKey.compareTo(results.get(results.size() - 1).getKey()) <= 0) {
        throw new IOException(
            "Underlying iterator yielded to a position that does not follow the last key returned: "
                + continueKey + " <= " + results.get(results.size() - 1).getKey());
      }

      log.debug("Scan yield detected at position " + continueKey);
      getTabletServer().getScanMetrics().addYield(1);
    } else if (!iter.hasTop()) {
      // end of tablet has been reached
      continueKey = null;
      if (results.isEmpty()) {
        results = null;
      }
    }

    return new Batch(skipContinueKey, results, continueKey, resultBytes);
  }

  public Scanner createScanner(Range range, ScanParameters scanParams,
      AtomicBoolean interruptFlag) {
    // do a test to see if this range falls within the tablet, if it does not
    // then clip will throw an exception
    extent.toDataRange().clip(range);

    return new Scanner(this, range, scanParams, interruptFlag);
  }

  DataFileValue minorCompact(InMemoryMap memTable, TabletFile tmpDatafile, TabletFile newDatafile,
      StoredTabletFile mergeFile, long queued, CommitSession commitSession, long flushId,
      MinorCompactionReason mincReason) {
    boolean failed = false;
    long start = System.currentTimeMillis();
    timer.incrementStatusMinor();

    long count = 0;

    String oldName = Thread.currentThread().getName();
    try {
      Thread.currentThread().setName("Minor compacting " + this.extent);
      CompactionStats stats;
      try (TraceScope span = Trace.startSpan("write")) {
        count = memTable.getNumEntries();

        DataFileValue dfv = null;
        if (mergeFile != null) {
          dfv = getDatafileManager().getDatafileSizes().get(mergeFile);
        }

        MinorCompactor compactor = new MinorCompactor(tabletServer, this, memTable, mergeFile, dfv,
            tmpDatafile, mincReason, tableConfiguration);
        stats = compactor.call();
      }

      try (TraceScope span = Trace.startSpan("bringOnline")) {
        getDatafileManager().bringMinorCompactionOnline(tmpDatafile, newDatafile, mergeFile,
            new DataFileValue(stats.getFileSize(), stats.getEntriesWritten()), commitSession,
            flushId);
      }
      return new DataFileValue(stats.getFileSize(), stats.getEntriesWritten());
    } catch (Exception | Error e) {
      failed = true;
      throw new RuntimeException("Exception occurred during minor compaction on " + extent, e);
    } finally {
      Thread.currentThread().setName(oldName);
      try {
        getTabletMemory().finalizeMinC();
      } catch (Throwable t) {
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

    StoredTabletFile mergeFile = null;
    if (mincReason != MinorCompactionReason.RECOVERY) {
      mergeFile = getDatafileManager().reserveMergingMinorCompactionFile();
    }

    double tracePercent =
        tabletServer.getConfiguration().getFraction(Property.TSERV_MINC_TRACE_PERCENT);

    return new MinorCompactionTask(this, mergeFile, oldCommitSession, flushId, mincReason,
        tracePercent);

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

        if (lastFlushID >= tableFlushID) {
          return;
        }

        if (isClosing() || isClosed() || getTabletMemory().memoryReservedForMinC()) {
          return;
        }

        if (getTabletMemory().getMemTable().getNumEntries() == 0) {
          lastFlushID = tableFlushID;
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

        if (isClosing() || isClosed() || majorCompactionState == CompactionState.WAITING_TO_START
            || getTabletMemory().memoryReservedForMinC()
            || getTabletMemory().getMemTable().getNumEntries() == 0 || updatingFlushID) {

          logMessage = new StringBuilder();

          logMessage.append(extent);
          logMessage.append(" closeState " + closeState);
          logMessage.append(" majorCompactionState " + majorCompactionState);
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
          + "/" + extent.getTableId() + Constants.ZTABLE_FLUSH_ID;
      String id = new String(context.getZooReaderWriter().getData(zTablePath, null), UTF_8);
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
        + "/" + extent.getTableId() + Constants.ZTABLE_COMPACT_CANCEL_ID;

    try {
      String id = new String(context.getZooReaderWriter().getData(zTablePath, null), UTF_8);
      return Long.parseLong(id);
    } catch (KeeperException | InterruptedException e) {
      throw new RuntimeException("Exception on " + extent + " getting compact cancel ID", e);
    }
  }

  public Pair<Long,UserCompactionConfig> getCompactionID() throws NoNodeException {
    try {
      String zTablePath = Constants.ZROOT + "/" + tabletServer.getInstanceID() + Constants.ZTABLES
          + "/" + extent.getTableId() + Constants.ZTABLE_COMPACT_ID;

      String[] tokens =
          new String(context.getZooReaderWriter().getData(zTablePath, null), UTF_8).split(",");
      long compactID = Long.parseLong(tokens[0]);

      UserCompactionConfig compactionConfig = new UserCompactionConfig();

      if (tokens.length > 1) {
        Hex hex = new Hex();
        ByteArrayInputStream bais =
            new ByteArrayInputStream(hex.decode(tokens[1].split("=")[1].getBytes(UTF_8)));
        DataInputStream dis = new DataInputStream(bais);

        compactionConfig.readFields(dis);

        KeyExtent ke = new KeyExtent(extent.getTableId(), compactionConfig.getEndRow(),
            compactionConfig.getStartRow());

        if (!ke.overlaps(extent)) {
          // only use iterators if compaction range overlaps
          compactionConfig = new UserCompactionConfig();
        }
      }

      return new Pair<>(compactID, compactionConfig);
    } catch (IOException | InterruptedException | DecoderException | NumberFormatException e) {
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
  public void close(boolean saveState) throws IOException {
    initiateClose(saveState);
    completeClose(saveState, true);
  }

  void initiateClose(boolean saveState) {
    log.trace("initiateClose(saveState={}) {}", saveState, getExtent());

    MinorCompactionTask mct = null;

    synchronized (this) {
      if (isClosed() || isClosing()) {
        String msg = "Tablet " + getExtent() + " already " + closeState;
        throw new IllegalStateException(msg);
      }

      // enter the closing state, no splits, minor, or major compactions can start
      // should cause running major compactions to stop
      closeState = CloseState.CLOSING;
      this.notifyAll();

      // wait for major compactions to finish, setting closing to
      // true should cause any running major compactions to abort
      while (isMajorCompactionRunning()) {
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

      if (!saveState || getTabletMemory().getMemTable().getNumEntries() == 0) {
        return;
      }

      getTabletMemory().waitForMinC();

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

    // wait for reads and writes to complete
    while (writesInProgress > 0 || !activeScans.isEmpty()) {
      try {
        this.wait(50);
      } catch (InterruptedException e) {
        log.error(e.toString());
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
        ProblemReports.getInstance(context).report(new ProblemReport(extent.getTableId(),
            ProblemType.TABLET_LOAD, this.extent.toString(), err));
        log.error("Tablet closed consistency check has failed for {} giving up and closing",
            this.extent);
      }
    }

    try {
      getTabletMemory().getMemTable().delete(0);
    } catch (Throwable t) {
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
      Pair<List<LogEntry>,SortedMap<StoredTabletFile,DataFileValue>> fileLog =
          MetadataTableUtil.getFileAndLogEntries(context, extent);

      if (!fileLog.getFirst().isEmpty()) {
        String msg = "Closed tablet " + extent + " has walog entries in " + MetadataTable.NAME + " "
            + fileLog.getFirst();
        log.error(msg);
        throw new RuntimeException(msg);
      }

      if (!fileLog.getSecond().equals(getDatafileManager().getDatafileSizes())) {
        String msg = "Data files in differ from in memory data " + extent + "  "
            + fileLog.getSecond() + "  " + getDatafileManager().getDatafileSizes();
        log.error(msg);
        throw new RuntimeException(msg);
      }
    } catch (Exception e) {
      String msg = "Failed to do close consistency check for tablet " + extent;
      log.error(msg, e);
      throw new RuntimeException(msg, e);

    }

    if (!otherLogs.isEmpty() || !currentLogs.isEmpty() || !referencedLogs.isEmpty()) {
      String msg = "Closed tablet " + extent + " has walog entries in memory currentLogs = "
          + currentLogs + "  otherLogs = " + otherLogs + " refererncedLogs = " + referencedLogs;
      log.error(msg);
      throw new RuntimeException(msg);
    }

    // TODO check lastFlushID and lostCompactID - ACCUMULO-1290
  }

  public synchronized void initiateMajorCompaction(MajorCompactionReason reason) {

    if (isClosing() || isClosed() || !needsMajorCompaction(reason) || isMajorCompactionRunning()
        || majorCompactionQueued.contains(reason)) {
      return;
    }

    majorCompactionQueued.add(reason);

    try {
      getTabletResources().executeMajorCompaction(getExtent(), new CompactionRunner(this, reason));
    } catch (RuntimeException t) {
      log.debug("removing {} because we encountered an exception enqueing the CompactionRunner",
          reason, t);
      majorCompactionQueued.remove(reason);
      throw t;
    }
  }

  /**
   * Returns true if a major compaction should be performed on the tablet.
   *
   */
  public boolean needsMajorCompaction(MajorCompactionReason reason) {
    if (isMajorCompactionRunning()) {
      return false;
    }
    if (reason == MajorCompactionReason.CHOP || reason == MajorCompactionReason.USER) {
      return true;
    }
    return getTabletResources().needsMajorCompaction(getDatafileManager().getDatafileSizes(),
        reason);
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

  private SplitRowSpec findSplitRow(Collection<TabletFile> files) {

    // never split the root tablet
    // check if we already decided that we can never split
    // check to see if we're big enough to split

    long splitThreshold = tableConfiguration.getAsBytes(Property.TABLE_SPLIT_THRESHOLD);
    long maxEndRow = tableConfiguration.getAsBytes(Property.TABLE_MAX_END_ROW_SIZE);

    if (extent.isRootTablet() || isFindSplitsSuppressed()
        || estimateTabletSize() <= splitThreshold) {
      return null;
    }

    SortedMap<Double,Key> keys = null;

    try {
      // we should make .25 below configurable
      keys = FileUtil.findMidPoint(context, chooseTabletDir(), extent.getPrevEndRow(),
          extent.getEndRow(), files, .25);
    } catch (IOException e) {
      log.error("Failed to find midpoint {}", e.getMessage());
      return null;
    }

    if (keys.isEmpty()) {
      log.info("Cannot split tablet " + extent + ", files contain no data for tablet.");
      suppressFindSplits();
      return null;
    }

    // check to see if one row takes up most of the tablet, in which case we can not split
    try {

      Text lastRow;
      if (extent.getEndRow() == null) {
        Key lastKey = (Key) FileUtil.findLastKey(context, files);
        lastRow = lastKey.getRow();
      } else {
        lastRow = extent.getEndRow();
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
    } catch (IOException e) {
      // don't split now, but check again later
      log.error("Failed to find lastkey {}", e.getMessage());
      return null;
    }

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

  private Map<StoredTabletFile,Pair<Key,Key>>
      getFirstAndLastKeys(SortedMap<StoredTabletFile,DataFileValue> allFiles) throws IOException {
    final Map<StoredTabletFile,Pair<Key,Key>> result = new HashMap<>();
    final FileOperations fileFactory = FileOperations.getInstance();
    final VolumeManager fs = getTabletServer().getFileSystem();
    for (Entry<StoredTabletFile,DataFileValue> entry : allFiles.entrySet()) {
      StoredTabletFile file = entry.getKey();
      FileSystem ns = fs.getFileSystemByPath(file.getPath());
      try (FileSKVIterator openReader = fileFactory.newReaderBuilder()
          .forFile(file.getPathStr(), ns, ns.getConf(), context.getCryptoService())
          .withTableConfiguration(this.getTableConfiguration()).seekToBeginning().build()) {
        Key first = openReader.getFirstKey();
        Key last = openReader.getLastKey();
        result.put(file, new Pair<>(first, last));
      }
    }
    return result;
  }

  List<StoredTabletFile> findChopFiles(KeyExtent extent,
      Map<StoredTabletFile,Pair<Key,Key>> firstAndLastKeys, Collection<StoredTabletFile> allFiles) {
    List<StoredTabletFile> result = new ArrayList<>();
    if (firstAndLastKeys == null) {
      result.addAll(allFiles);
      return result;
    }

    for (StoredTabletFile file : allFiles) {
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
        // If first and last are null, it's an empty file. Add it to the compact set so it goes
        // away.
        if ((first == null && last == null) || (first != null && !extent.contains(first.getRow()))
            || (last != null && !extent.contains(last.getRow()))) {
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
    if (isClosing() || isClosed()) {
      return false;
    }
    return findSplitRow(getDatafileManager().getFiles()) != null;
  }

  // BEGIN PRIVATE METHODS RELATED TO MAJOR COMPACTION
  private CompactionStats _majorCompact(MajorCompactionReason reason)
      throws IOException, CompactionCanceledException {

    long t1, t2, t3;

    Pair<Long,UserCompactionConfig> compactionId = null;
    CompactionStrategy strategy = null;
    Map<StoredTabletFile,Pair<Key,Key>> firstAndLastKeys = null;

    if (reason == MajorCompactionReason.USER) {
      try {
        compactionId = getCompactionID();
        strategy = createCompactionStrategy(compactionId.getSecond().getCompactionStrategy());
      } catch (NoNodeException e) {
        throw new RuntimeException("Exception on " + extent + " during MajC", e);
      }
    } else if (reason == MajorCompactionReason.NORMAL || reason == MajorCompactionReason.IDLE) {
      strategy = Property.createTableInstanceFromPropertyName(tableConfiguration,
          Property.TABLE_COMPACTION_STRATEGY, CompactionStrategy.class,
          new DefaultCompactionStrategy());
      strategy.init(Property.getCompactionStrategyOptions(tableConfiguration));
    } else if (reason == MajorCompactionReason.CHOP) {
      firstAndLastKeys = getFirstAndLastKeys(getDatafileManager().getDatafileSizes());
    } else {
      throw new IllegalArgumentException(
          "Unknown compaction reason " + reason + " during MajC on " + extent);
    }

    if (strategy != null) {
      BlockCache sc = tabletResources.getTabletServerResourceManager().getSummaryCache();
      BlockCache ic = tabletResources.getTabletServerResourceManager().getIndexCache();
      Cache<String,Long> fileLenCache =
          tabletResources.getTabletServerResourceManager().getFileLenCache();
      MajorCompactionRequest request = new MajorCompactionRequest(extent, reason,
          getTabletServer().getFileSystem(), tableConfiguration, sc, ic, fileLenCache, context);
      request.setFiles(getDatafileManager().getDatafileSizes());
      strategy.gatherInformation(request);
    }

    Map<StoredTabletFile,DataFileValue> filesToCompact = null;

    int maxFilesToCompact = tableConfiguration.getCount(Property.TSERV_MAJC_THREAD_MAXOPEN);

    CompactionStats majCStats = new CompactionStats();
    CompactionPlan plan = null;

    boolean propogateDeletes = false;
    boolean updateCompactionID = false;

    synchronized (this) {
      // plan all that work that needs to be done in the sync block... then do the actual work
      // outside the sync block

      t1 = System.currentTimeMillis();

      majorCompactionState = CompactionState.WAITING_TO_START;

      getTabletMemory().waitForMinC();

      t2 = System.currentTimeMillis();

      majorCompactionState = CompactionState.IN_PROGRESS;
      notifyAll();

      SortedMap<StoredTabletFile,DataFileValue> allFiles = getDatafileManager().getDatafileSizes();
      List<StoredTabletFile> inputFiles = new ArrayList<>();
      if (reason == MajorCompactionReason.CHOP) {
        // enforce rules: files with keys outside our range need to be compacted
        inputFiles.addAll(findChopFiles(extent, firstAndLastKeys, allFiles.keySet()));
      } else {
        MajorCompactionRequest request =
            new MajorCompactionRequest(extent, reason, tableConfiguration, context);
        request.setFiles(allFiles);
        plan = strategy.getCompactionPlan(request);
        if (plan != null) {
          plan.validate(allFiles.keySet());
          inputFiles.addAll(plan.inputFiles);
        }
      }

      if (inputFiles.isEmpty()) {
        if (reason == MajorCompactionReason.USER) {
          if (compactionId.getSecond().getIterators().isEmpty()) {
            log.debug(
                "No-op major compaction by USER on 0 input files because no iterators present.");
            lastCompactID = compactionId.getFirst();
            updateCompactionID = true;
          } else {
            log.debug("Major compaction by USER on 0 input files with iterators.");
            filesToCompact = new HashMap<>();
          }
        } else {
          return majCStats;
        }
      } else {
        // If no original files will exist at the end of the compaction, we do not have to propagate
        // deletes
        Set<StoredTabletFile> droppedFiles = new HashSet<>();
        droppedFiles.addAll(inputFiles);
        if (plan != null) {
          droppedFiles.addAll(plan.deleteFiles);
        }
        propogateDeletes = !(droppedFiles.equals(allFiles.keySet()));
        log.debug("Major compaction plan: {} propagate deletes : {}", plan, propogateDeletes);
        filesToCompact = new HashMap<>(allFiles);
        filesToCompact.keySet().retainAll(inputFiles);

        getDatafileManager().reserveMajorCompactingFiles(filesToCompact.keySet());
      }

      t3 = System.currentTimeMillis();
    }

    try {

      log.trace(String.format("MajC initiate lock %.2f secs, wait %.2f secs", (t3 - t2) / 1000.0,
          (t2 - t1) / 1000.0));

      if (updateCompactionID) {
        MetadataTableUtil.updateTabletCompactID(extent, compactionId.getFirst(), context,
            getTabletServer().getLock());
        return majCStats;
      }

      if (!propogateDeletes && compactionId == null) {
        // compacting everything, so update the compaction id in metadata
        try {
          compactionId = getCompactionID();
          if (compactionId.getSecond().getCompactionStrategy() != null) {
            compactionId = null;
            // TODO maybe return unless chop?
          }

        } catch (NoNodeException e) {
          throw new RuntimeException("Exception on " + extent + " during MajC", e);
        }
      }

      List<IteratorSetting> compactionIterators = new ArrayList<>();
      if (compactionId != null) {
        if (reason == MajorCompactionReason.USER) {
          if (getCompactionCancelID() >= compactionId.getFirst()) {
            // compaction was canceled
            return majCStats;
          }
          compactionIterators = compactionId.getSecond().getIterators();

          synchronized (this) {
            if (lastCompactID >= compactionId.getFirst()) {
              // already compacted
              return majCStats;
            }
          }
        }

      }

      // need to handle case where only one file is being major compacted
      // ACCUMULO-3645 run loop at least once, even if filesToCompact.isEmpty()
      do {
        int numToCompact = maxFilesToCompact;

        if (filesToCompact.size() > maxFilesToCompact
            && filesToCompact.size() < 2 * maxFilesToCompact) {
          // on the second to last compaction pass, compact the minimum amount of files possible
          numToCompact = filesToCompact.size() - maxFilesToCompact + 1;
        }

        Set<StoredTabletFile> smallestFiles = removeSmallest(filesToCompact, numToCompact);

        TabletFile newFile =
            getNextMapFilename((filesToCompact.isEmpty() && !propogateDeletes) ? "A" : "C");
        TabletFile compactTmpName = new TabletFile(new Path(newFile.getMetaInsert() + "_tmp"));

        AccumuloConfiguration tableConf = createCompactionConfiguration(tableConfiguration, plan);

        try (TraceScope span = Trace.startSpan("compactFiles")) {
          CompactionEnv cenv = new CompactionEnv() {
            @Override
            public boolean isCompactionEnabled() {
              return !isClosing();
            }

            @Override
            public IteratorScope getIteratorScope() {
              return IteratorScope.majc;
            }

            @Override
            public RateLimiter getReadLimiter() {
              return getTabletServer().getMajorCompactionReadLimiter();
            }

            @Override
            public RateLimiter getWriteLimiter() {
              return getTabletServer().getMajorCompactionWriteLimiter();
            }

          };

          HashMap<StoredTabletFile,DataFileValue> copy =
              new HashMap<>(getDatafileManager().getDatafileSizes());
          if (!copy.keySet().containsAll(smallestFiles)) {
            throw new IllegalStateException("Cannot find data file values for " + smallestFiles
                + " on " + extent + " during MajC");
          }

          copy.keySet().retainAll(smallestFiles);

          log.debug("Starting MajC {} ({}) {} --> {} {}", extent, reason, copy.keySet(),
              compactTmpName, compactionIterators);

          // always propagate deletes, unless last batch
          boolean lastBatch = filesToCompact.isEmpty();
          Compactor compactor = new Compactor(context, this, copy, null, compactTmpName,
              lastBatch ? propogateDeletes : true, cenv, compactionIterators, reason.ordinal(),
              tableConf);

          CompactionStats mcs = compactor.call();

          if (span.getSpan() != null) {
            span.getSpan().addKVAnnotation("files", ("" + smallestFiles.size()));
            span.getSpan().addKVAnnotation("read", ("" + mcs.getEntriesRead()));
            span.getSpan().addKVAnnotation("written", ("" + mcs.getEntriesWritten()));
          }
          majCStats.add(mcs);

          if (lastBatch && plan != null && plan.deleteFiles != null) {
            smallestFiles.addAll(plan.deleteFiles);
          }
          StoredTabletFile newTabletFile = getDatafileManager().bringMajorCompactionOnline(
              smallestFiles, compactTmpName, newFile,
              filesToCompact.isEmpty() && compactionId != null ? compactionId.getFirst() : null,
              new DataFileValue(mcs.getFileSize(), mcs.getEntriesWritten()));

          // when major compaction produces a file w/ zero entries, it will be deleted... do not
          // want to add the deleted file
          if (!filesToCompact.isEmpty() && mcs.getEntriesWritten() > 0) {
            filesToCompact.put(newTabletFile,
                new DataFileValue(mcs.getFileSize(), mcs.getEntriesWritten()));
          }
        }

      } while (!filesToCompact.isEmpty());
      return majCStats;
    } finally {
      synchronized (Tablet.this) {
        getDatafileManager().clearMajorCompactingFile();
      }
    }
  }

  protected static AccumuloConfiguration createCompactionConfiguration(TableConfiguration base,
      CompactionPlan plan) {
    if (plan == null || plan.writeParameters == null) {
      return base;
    }
    WriteParameters p = plan.writeParameters;
    ConfigurationCopy result = new ConfigurationCopy(base);
    if (p.getHdfsBlockSize() > 0) {
      result.set(Property.TABLE_FILE_BLOCK_SIZE, "" + p.getHdfsBlockSize());
    }
    if (p.getBlockSize() > 0) {
      result.set(Property.TABLE_FILE_COMPRESSED_BLOCK_SIZE, "" + p.getBlockSize());
    }
    if (p.getIndexBlockSize() > 0) {
      result.set(Property.TABLE_FILE_COMPRESSED_BLOCK_SIZE_INDEX, "" + p.getIndexBlockSize());
    }
    if (p.getCompressType() != null) {
      result.set(Property.TABLE_FILE_COMPRESSION_TYPE, p.getCompressType());
    }
    if (p.getReplication() != 0) {
      result.set(Property.TABLE_FILE_REPLICATION, "" + p.getReplication());
    }
    return result;
  }

  private Set<StoredTabletFile> removeSmallest(Map<StoredTabletFile,DataFileValue> filesToCompact,
      int maxFilesToCompact) {
    // ensure this method works properly when multiple files have the same size

    // short-circuit; also handles zero files case
    if (filesToCompact.size() <= maxFilesToCompact) {
      Set<StoredTabletFile> smallestFiles = new HashSet<>(filesToCompact.keySet());
      filesToCompact.clear();
      return smallestFiles;
    }

    PriorityQueue<Pair<StoredTabletFile,Long>> fileHeap =
        new PriorityQueue<>(filesToCompact.size(), (o1, o2) -> {
          if (o1.getSecond().equals(o2.getSecond())) {
            return o1.getFirst().compareTo(o2.getFirst());
          }
          if (o1.getSecond() < o2.getSecond()) {
            return -1;
          }
          return 1;
        });

    for (Entry<StoredTabletFile,DataFileValue> entry : filesToCompact.entrySet()) {
      fileHeap.add(new Pair<>(entry.getKey(), entry.getValue().getSize()));
    }

    Set<StoredTabletFile> smallestFiles = new HashSet<>();
    while (smallestFiles.size() < maxFilesToCompact && !fileHeap.isEmpty()) {
      Pair<StoredTabletFile,Long> pair = fileHeap.remove();
      filesToCompact.remove(pair.getFirst());
      smallestFiles.add(pair.getFirst());
    }

    return smallestFiles;
  }

  // END PRIVATE METHODS RELATED TO MAJOR COMPACTION

  /**
   * Performs a major compaction on the tablet. If needsSplit() returns true, the tablet is split
   * and a reference to the new tablet is returned.
   */

  CompactionStats majorCompact(MajorCompactionReason reason, long queued) {
    CompactionStats majCStats = null;
    boolean success = false;
    long start = System.currentTimeMillis();

    timer.incrementStatusMajor();

    synchronized (this) {
      // check that compaction is still needed - defer to splitting
      majorCompactionQueued.remove(reason);

      if (isClosing() || isClosed() || !needsMajorCompaction(reason) || isMajorCompactionRunning()
          || needsSplit()) {
        return null;
      }

      majorCompactionState = CompactionState.WAITING_TO_START;
    }

    double tracePercent =
        tabletServer.getConfiguration().getFraction(Property.TSERV_MAJC_TRACE_PERCENT);
    ProbabilitySampler sampler = TraceUtil.probabilitySampler(tracePercent);
    try (TraceScope span = Trace.startSpan("majorCompaction", sampler)) {

      majCStats = _majorCompact(reason);
      if (reason == MajorCompactionReason.CHOP) {
        MetadataTableUtil.chopped(getTabletServer().getContext(), getExtent(),
            this.getTabletServer().getLock());
        getTabletServer()
            .enqueueMasterMessage(new TabletStatusMessage(TabletLoadState.CHOPPED, extent));
      }
      if (span.getSpan() != null) {
        span.getSpan().addKVAnnotation("extent", ("" + getExtent()));
        if (majCStats != null) {
          span.getSpan().addKVAnnotation("read", ("" + majCStats.getEntriesRead()));
          span.getSpan().addKVAnnotation("written", ("" + majCStats.getEntriesWritten()));
        }
      }
      success = true;
    } catch (CompactionCanceledException cce) {
      log.debug("Major compaction canceled, extent = {}", getExtent());
    } catch (IOException ioe) {
      log.error("MajC Failed, extent = " + getExtent(), ioe);
    } catch (RuntimeException e) {
      log.error("MajC Unexpected exception, extent = " + getExtent(), e);
    } finally {
      // ensure we always reset boolean, even
      // when an exception is thrown
      synchronized (this) {
        majorCompactionState = null;
        this.notifyAll();
      }
    }
    long count = 0;
    if (majCStats != null) {
      count = majCStats.getEntriesRead();
    }
    timer.updateTime(Operation.MAJOR, queued, start, count, !success);

    return majCStats;
  }

  public KeyExtent getExtent() {
    return extent;
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

  public boolean isClosed() {
    // Assign to a local var to avoid race conditions since closeState is volatile and two
    // comparisons are done.
    CloseState localCS = closeState;
    return localCS == CloseState.CLOSED || localCS == CloseState.COMPLETE;
  }

  public boolean isCloseComplete() {
    return closeState == CloseState.COMPLETE;
  }

  public boolean isMajorCompactionRunning() {
    return majorCompactionState != null;
  }

  public boolean isMinorCompactionQueued() {
    return minorCompactionState == CompactionState.WAITING_TO_START;
  }

  public boolean isMinorCompactionRunning() {
    return minorCompactionState == CompactionState.IN_PROGRESS;
  }

  public boolean isMajorCompactionQueued() {
    return !majorCompactionQueued.isEmpty();
  }

  public TreeMap<KeyExtent,TabletData> split(byte[] sp) throws IOException {

    if (sp != null && extent.getEndRow() != null && extent.getEndRow().equals(new Text(sp))) {
      throw new IllegalArgumentException(
          "Attempting to split on EndRow " + extent.getEndRow() + " for " + extent);
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

    try {
      initiateClose(true);
    } catch (IllegalStateException ise) {
      log.debug("File {} not splitting : {}", extent, ise.getMessage());
      return null;
    }

    // obtain this info outside of synch block since it will involve opening
    // the map files... it is ok if the set of map files changes, because
    // this info is used for optimization... it is ok if map files are missing
    // from the set... can still query and insert into the tablet while this
    // map file operation is happening
    Map<TabletFile,FileUtil.FileInfo> firstAndLastRows =
        FileUtil.tryToGetFirstAndLastRows(context, getDatafileManager().getFiles());

    synchronized (this) {
      // java needs tuples ...
      TreeMap<KeyExtent,TabletData> newTablets = new TreeMap<>();

      long t1 = System.currentTimeMillis();
      // choose a split point
      SplitRowSpec splitPoint;
      if (sp == null) {
        splitPoint = findSplitRow(getDatafileManager().getFiles());
      } else {
        Text tsp = new Text(sp);
        splitPoint = new SplitRowSpec(FileUtil.estimatePercentageLTE(context, chooseTabletDir(),
            extent.getPrevEndRow(), extent.getEndRow(), getDatafileManager().getFiles(), tsp), tsp);
      }

      if (splitPoint == null || splitPoint.row == null) {
        log.info("had to abort split because splitRow was null");
        closeState = CloseState.OPEN;
        return null;
      }

      closeState = CloseState.CLOSING;
      completeClose(true, false);

      Text midRow = splitPoint.row;
      double splitRatio = splitPoint.splitRatio;

      KeyExtent low = new KeyExtent(extent.getTableId(), midRow, extent.getPrevEndRow());
      KeyExtent high = new KeyExtent(extent.getTableId(), extent.getEndRow(), midRow);

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

      MetadataTableUtil.splitTablet(high, extent.getPrevEndRow(), splitRatio,
          getTabletServer().getContext(), getTabletServer().getLock());
      MasterMetadataUtil.addNewTablet(getTabletServer().getContext(), low, lowDirectoryName,
          getTabletServer().getTabletSession(), lowDatafileSizes, bulkImported, time, lastFlushID,
          lastCompactID, getTabletServer().getLock());
      MetadataTableUtil.finishSplit(high, highDatafileSizes, highDatafilesToRemove,
          getTabletServer().getContext(), getTabletServer().getLock());

      TabletLogger.split(extent, low, high, getTabletServer().getTabletSession());

      newTablets.put(high, new TabletData(dirName, highDatafileSizes, time, lastFlushID,
          lastCompactID, lastLocation, bulkImported));
      newTablets.put(low, new TabletData(lowDirectoryName, lowDatafileSizes, time, lastFlushID,
          lastCompactID, lastLocation, bulkImported));

      long t2 = System.currentTimeMillis();

      log.debug(String.format("offline split time : %6.2f secs", (t2 - t1) / 1000.0));

      closeState = CloseState.COMPLETE;
      return newTablets;
    }
  }

  public SortedMap<StoredTabletFile,DataFileValue> getDatafiles() {
    return getDatafileManager().getDatafileSizes();
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

  public void importMapFiles(long tid, Map<TabletFile,MapFileInfo> fileMap, boolean setTime)
      throws IOException {
    Map<TabletFile,DataFileValue> entries = new HashMap<>(fileMap.size());
    List<String> files = new ArrayList<>();

    for (Entry<TabletFile,MapFileInfo> entry : fileMap.entrySet()) {
      entries.put(entry.getKey(), new DataFileValue(entry.getValue().estimatedSize, 0L));
      files.add(entry.getKey().getMetaInsert());
    }

    // Clients timeout and will think that this operation failed.
    // Don't do it if we spent too long waiting for the lock
    long now = System.currentTimeMillis();
    synchronized (this) {
      if (isClosed()) {
        throw new IOException("tablet " + extent + " is closed");
      }

      // TODO check seems unneeded now - ACCUMULO-1291
      long lockWait = System.currentTimeMillis() - now;
      if (lockWait
          > getTabletServer().getConfiguration().getTimeInMillis(Property.GENERAL_RPC_TIMEOUT)) {
        throw new IOException(
            "Timeout waiting " + (lockWait / 1000.) + " seconds to get tablet lock for " + extent);
      }

      List<TabletFile> alreadyImported = bulkImported.get(tid);
      if (alreadyImported != null) {
        for (TabletFile entry : alreadyImported) {
          if (fileMap.remove(entry) != null) {
            log.trace("Ignoring import of bulk file already imported: {}", entry);
          }
        }
      }

      fileMap.keySet().removeIf(file -> {
        if (bulkImporting.contains(file)) {
          log.info("Ignoring import of bulk file currently importing: " + file);
          return true;
        }
        return false;
      });

      if (fileMap.isEmpty()) {
        return;
      }

      incrementWritesInProgress();

      // prevent other threads from processing this file while its added to the metadata table.
      bulkImporting.addAll(fileMap.keySet());
    }
    try {
      tabletServer.updateBulkImportState(files, BulkImportState.LOADING);

      getDatafileManager().importMapFiles(tid, entries, setTime);
      lastMapFileImportTime = System.currentTimeMillis();

      if (needsSplit()) {
        getTabletServer().executeSplit(this);
      } else {
        initiateMajorCompaction(MajorCompactionReason.NORMAL);
      }
    } finally {
      synchronized (this) {
        decrementWritesInProgress();

        if (!bulkImporting.removeAll(fileMap.keySet())) {
          throw new AssertionError(
              "Likely bug in code, always expect to remove something.  Please open an Accumulo issue.");
        }

        try {
          bulkImported.computeIfAbsent(tid, k -> new ArrayList<>()).addAll(fileMap.keySet());
        } catch (Exception ex) {
          log.info(ex.toString(), ex);
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

    var builder = ImmutableSet.<DfsLogger>builder();
    builder.addAll(currentLogs);
    builder.addAll(otherLogs);
    referencedLogs = builder.build();

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
    int maxLogs = tableConfiguration.getCount(tableConfiguration
        .resolve(Property.TSERV_WALOG_MAX_REFERENCED, Property.TABLE_MINC_LOGS_MAX));

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
      initiateMinorCompaction(MinorCompactionReason.SYSTEM);
      log.debug("Initiating minor compaction for {} because {}", getExtent(), reason);
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

  public synchronized void chopFiles() {
    initiateMajorCompaction(MajorCompactionReason.CHOP);
  }

  private CompactionStrategy createCompactionStrategy(CompactionStrategyConfig strategyConfig) {
    String context = tableConfiguration.get(Property.TABLE_CLASSPATH);
    String clazzName = strategyConfig.getClassName();
    try {
      Class<? extends CompactionStrategy> clazz;
      if (context != null && !context.equals("")) {
        clazz = AccumuloVFSClassLoader.getContextManager().loadClass(context, clazzName,
            CompactionStrategy.class);
      } else {
        clazz = AccumuloVFSClassLoader.loadClass(clazzName, CompactionStrategy.class);
      }
      CompactionStrategy strategy = clazz.getDeclaredConstructor().newInstance();
      strategy.init(strategyConfig.getOptions());
      return strategy;
    } catch (Exception e) {
      throw new RuntimeException("Error creating compaction strategy on " + extent, e);
    }
  }

  public void compactAll(long compactionId, UserCompactionConfig compactionConfig) {
    boolean updateMetadata = false;

    synchronized (this) {
      if (lastCompactID >= compactionId) {
        return;
      }

      if (isMinorCompactionRunning()) {
        // want to wait for running minc to finish before starting majc, see ACCUMULO-3041
        if (compactionWaitInfo.compactionID == compactionId) {
          if (lastFlushID == compactionWaitInfo.flushID) {
            return;
          }
        } else {
          compactionWaitInfo.compactionID = compactionId;
          compactionWaitInfo.flushID = lastFlushID;
          return;
        }
      }

      if (isClosing() || isClosed() || majorCompactionQueued.contains(MajorCompactionReason.USER)
          || isMajorCompactionRunning()) {
        return;
      }

      CompactionStrategyConfig strategyConfig = compactionConfig.getCompactionStrategy();
      CompactionStrategy strategy = createCompactionStrategy(strategyConfig);

      MajorCompactionRequest request = new MajorCompactionRequest(extent,
          MajorCompactionReason.USER, tableConfiguration, context);
      request.setFiles(getDatafileManager().getDatafileSizes());

      try {
        if (strategy.shouldCompact(request)) {
          initiateMajorCompaction(MajorCompactionReason.USER);
        } else {
          majorCompactionState = CompactionState.IN_PROGRESS;
          updateMetadata = true;
          lastCompactID = compactionId;
        }
      } catch (IOException e) {
        throw new UncheckedIOException("IOException on " + extent + " during compact all", e);
      }
    }

    if (updateMetadata) {
      try {
        // if multiple threads were allowed to update this outside of a sync block, then it would be
        // a race condition
        MetadataTableUtil.updateTabletCompactID(extent, compactionId,
            getTabletServer().getContext(), getTabletServer().getLock());
      } finally {
        synchronized (this) {
          majorCompactionState = null;
          this.notifyAll();
        }
      }
    }
  }

  public TableConfiguration getTableConfiguration() {
    return tableConfiguration;
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

  public synchronized void updateQueryStats(int size, long numBytes) {
    queryCount += size;
    queryBytes += numBytes;
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

  public StoredTabletFile updateTabletDataFile(long maxCommittedTime, TabletFile newDatafile,
      StoredTabletFile absMergeFile, DataFileValue dfv, Set<String> unusedWalLogs,
      Set<StoredTabletFile> filesInUseByScans, long flushId) {
    synchronized (timeLock) {
      if (maxCommittedTime > persistedTime) {
        persistedTime = maxCommittedTime;
      }

      return MasterMetadataUtil.updateTabletDataFile(getTabletServer().getContext(), extent,
          newDatafile, absMergeFile, dfv, tabletTime.getMetadataTime(persistedTime),
          filesInUseByScans, tabletServer.getClientAddressString(), tabletServer.getLock(),
          unusedWalLogs, lastLocation, flushId);
    }

  }

  TabletResourceManager getTabletResources() {
    return tabletResources;
  }

  DatafileManager getDatafileManager() {
    return datafileManager;
  }

  TabletMemory getTabletMemory() {
    return tabletMemory;
  }

  public long getAndUpdateTime() {
    return tabletTime.getAndUpdateTime();
  }

  public byte[] getDefaultSecurityLabels() {
    return defaultSecurityLabel.derive();
  }

  public void flushComplete(long flushId) {
    lastLocation = null;
    dataSourceDeletions.incrementAndGet();
    tabletMemory.finishedMinC();
    lastFlushID = flushId;
    computeNumEntries();
  }

  public TServerInstance resetLastLocation() {
    TServerInstance result = lastLocation;
    lastLocation = null;
    return result;
  }

  public synchronized void addActiveScans(ScanDataSource scanDataSource) {
    activeScans.add(scanDataSource);
  }

  public int removeScan(ScanDataSource scanDataSource) {
    activeScans.remove(scanDataSource);
    return activeScans.size();
  }

  public synchronized void setLastCompactionID(Long compactionId) {
    if (compactionId != null) {
      this.lastCompactID = compactionId;
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

  public AtomicLong getScannedCounter() {
    return scannedCount;
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
}

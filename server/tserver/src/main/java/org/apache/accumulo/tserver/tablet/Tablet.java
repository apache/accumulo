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
package org.apache.accumulo.tserver.tablet;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;
import static org.apache.accumulo.fate.util.UtilWaitThread.sleepUninterruptibly;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
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
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.Durability;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.admin.CompactionStrategyConfig;
import org.apache.accumulo.core.client.impl.DurabilityImpl;
import org.apache.accumulo.core.client.impl.Table;
import org.apache.accumulo.core.client.impl.Tables;
import org.apache.accumulo.core.client.sample.SamplerConfiguration;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.ConfigurationCopy;
import org.apache.accumulo.core.conf.ConfigurationObserver;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.constraints.Violations;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Column;
import org.apache.accumulo.core.data.ColumnUpdate;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.data.impl.KeyExtent;
import org.apache.accumulo.core.data.thrift.IterInfo;
import org.apache.accumulo.core.data.thrift.MapFileInfo;
import org.apache.accumulo.core.file.FileOperations;
import org.apache.accumulo.core.file.FileSKVIterator;
import org.apache.accumulo.core.file.blockfile.cache.BlockCache;
import org.apache.accumulo.core.iterators.IterationInterruptedException;
import org.apache.accumulo.core.iterators.IteratorUtil.IteratorScope;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.YieldCallback;
import org.apache.accumulo.core.iterators.system.SourceSwitchingIterator;
import org.apache.accumulo.core.master.thrift.BulkImportState;
import org.apache.accumulo.core.master.thrift.TabletLoadState;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.metadata.RootTable;
import org.apache.accumulo.core.metadata.schema.DataFileValue;
import org.apache.accumulo.core.protobuf.ProtobufUtil;
import org.apache.accumulo.core.replication.ReplicationConfigurationUtil;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.accumulo.core.tabletserver.log.LogEntry;
import org.apache.accumulo.core.tabletserver.thrift.TabletStats;
import org.apache.accumulo.core.trace.ProbabilitySampler;
import org.apache.accumulo.core.trace.Span;
import org.apache.accumulo.core.trace.Trace;
import org.apache.accumulo.core.util.LocalityGroupUtil;
import org.apache.accumulo.core.util.Pair;
import org.apache.accumulo.core.util.ratelimit.RateLimiter;
import org.apache.accumulo.server.ServerConstants;
import org.apache.accumulo.server.conf.TableConfiguration;
import org.apache.accumulo.server.fs.FileRef;
import org.apache.accumulo.server.fs.VolumeChooserEnvironment;
import org.apache.accumulo.server.fs.VolumeManager;
import org.apache.accumulo.server.fs.VolumeManager.FileType;
import org.apache.accumulo.server.fs.VolumeUtil;
import org.apache.accumulo.server.fs.VolumeUtil.TabletFiles;
import org.apache.accumulo.server.master.state.TServerInstance;
import org.apache.accumulo.server.master.tableOps.UserCompactionConfig;
import org.apache.accumulo.server.metrics.Metrics;
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
import org.apache.accumulo.server.zookeeper.ZooReaderWriter;
import org.apache.accumulo.start.classloader.vfs.AccumuloVFSClassLoader;
import org.apache.accumulo.tserver.ConditionCheckerContext.ConditionChecker;
import org.apache.accumulo.tserver.InMemoryMap;
import org.apache.accumulo.tserver.MinorCompactionReason;
import org.apache.accumulo.tserver.TConstraintViolationException;
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
import org.apache.accumulo.tserver.log.MutationReceiver;
import org.apache.accumulo.tserver.mastermessage.TabletStatusMessage;
import org.apache.accumulo.tserver.metrics.TabletServerMinCMetrics;
import org.apache.accumulo.tserver.metrics.TabletServerScanMetrics;
import org.apache.accumulo.tserver.tablet.Compactor.CompactionCanceledException;
import org.apache.accumulo.tserver.tablet.Compactor.CompactionEnv;
import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

/**
 *
 * Provide access to a single row range in a living TabletServer.
 *
 */
public class Tablet implements TabletCommitter {
  static private final Logger log = LoggerFactory.getLogger(Tablet.class);

  private final TabletServer tabletServer;
  private final KeyExtent extent;
  private final TabletResourceManager tabletResources;
  private final DatafileManager datafileManager;
  private final TableConfiguration tableConfiguration;
  private final String tabletDirectory;
  private final Path location; // absolute path of this tablets dir

  private final TabletMemory tabletMemory;

  private final TabletTime tabletTime;
  private final Object timeLock = new Object();
  private long persistedTime;

  private TServerInstance lastLocation = null;
  private volatile boolean tableDirChecked = false;

  private final AtomicLong dataSourceDeletions = new AtomicLong(0);

  public long getDataSourceDeletions() {
    return dataSourceDeletions.get();
  }

  private final Set<ScanDataSource> activeScans = new HashSet<>();

  private static enum CloseState {
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

  // stores info about user initiated major compaction that is waiting on a minor compaction to finish
  private final CompactionWaitInfo compactionWaitInfo = new CompactionWaitInfo();

  static enum CompactionState {
    WAITING_TO_START, IN_PROGRESS
  }

  private volatile CompactionState minorCompactionState = null;
  private volatile CompactionState majorCompactionState = null;

  private final Set<MajorCompactionReason> majorCompactionQueued = Collections.synchronizedSet(EnumSet.noneOf(MajorCompactionReason.class));

  private final AtomicReference<ConstraintChecker> constraintChecker = new AtomicReference<>();

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

  private byte[] defaultSecurityLabel = new byte[0];

  private long lastMinorCompactionFinishTime = 0;
  private long lastMapFileImportTime = 0;

  private volatile long numEntries = 0;
  private volatile long numEntriesInMemory = 0;

  private final Rate scannedRate = new Rate(0.95);
  private final AtomicLong scannedCount = new AtomicLong(0);

  private final ConfigurationObserver configObserver;

  private final Cache<Long,List<FileRef>> bulkImported = CacheBuilder.newBuilder().build();

  private final int logId;

  @Override
  public int getLogId() {
    return logId;
  }

  public static class LookupResult {
    public List<Range> unfinishedRanges = new ArrayList<>();
    public long bytesAdded = 0;
    public long dataSize = 0;
    public boolean closed = false;
  }

  FileRef getNextMapFilename(String prefix) throws IOException {
    String extension = FileOperations.getNewFileExtension(tableConfiguration);
    checkTabletDir();
    return new FileRef(location.toString() + "/" + prefix + UniqueNameAllocator.getInstance().getNextName() + "." + extension);
  }

  private void checkTabletDir() throws IOException {
    if (!tableDirChecked) {
      FileStatus[] files = null;
      try {
        files = getTabletServer().getFileSystem().listStatus(location);
      } catch (FileNotFoundException ex) {
        // ignored
      }

      if (files == null) {
        if (location.getName().startsWith(Constants.CLONE_PREFIX))
          log.debug("Tablet {} had no dir, creating {}", extent, location); // its a clone dir...
        else
          log.warn("Tablet {} had no dir, creating {}", extent, location);

        getTabletServer().getFileSystem().mkdirs(location);
      }
      tableDirChecked = true;
    }
  }

  /**
   * Only visible for testing
   */
  @VisibleForTesting
  protected Tablet(TabletTime tabletTime, String tabletDirectory, int logId, Path location, DatafileManager datafileManager, TabletServer tabletServer,
      TabletResourceManager tabletResources, TabletMemory tabletMemory, TableConfiguration tableConfiguration, KeyExtent extent,
      ConfigurationObserver configObserver) {
    this.tabletTime = tabletTime;
    this.tabletDirectory = tabletDirectory;
    this.logId = logId;
    this.location = location;
    this.datafileManager = datafileManager;
    this.tabletServer = tabletServer;
    this.tabletResources = tabletResources;
    this.tabletMemory = tabletMemory;
    this.tableConfiguration = tableConfiguration;
    this.extent = extent;
    this.configObserver = configObserver;
    this.splitCreationTime = 0;
  }

  public Tablet(final TabletServer tabletServer, final KeyExtent extent, final TabletResourceManager trm, TabletData data) throws IOException {

    this.tabletServer = tabletServer;
    this.extent = extent;
    this.tabletResources = trm;
    this.lastLocation = data.getLastLocation();
    this.lastFlushID = data.getFlushID();
    this.lastCompactID = data.getCompactID();
    this.splitCreationTime = data.getSplitTime();
    this.tabletTime = TabletTime.getInstance(data.getTime());
    this.persistedTime = tabletTime.getTime();
    this.logId = tabletServer.createLogId(extent);

    TableConfiguration tblConf = tabletServer.getTableConfiguration(extent);
    if (null == tblConf) {
      Tables.clearCache(tabletServer.getInstance());
      tblConf = tabletServer.getTableConfiguration(extent);
      requireNonNull(tblConf, "Could not get table configuration for " + extent.getTableId());
    }

    this.tableConfiguration = tblConf;

    // translate any volume changes
    VolumeManager fs = tabletServer.getFileSystem();
    boolean replicationEnabled = ReplicationConfigurationUtil.isEnabled(extent, this.tableConfiguration);
    TabletFiles tabletPaths = new TabletFiles(data.getDirectory(), data.getLogEntris(), data.getDataFiles());
    tabletPaths = VolumeUtil.updateTabletVolumes(tabletServer, tabletServer.getLock(), fs, extent, tabletPaths, replicationEnabled);

    // deal with relative path for the directory
    Path locationPath;
    if (tabletPaths.dir.contains(":")) {
      locationPath = new Path(tabletPaths.dir);
    } else {
      locationPath = tabletServer.getFileSystem().getFullPath(FileType.TABLE, extent.getTableId() + tabletPaths.dir);
    }
    this.location = locationPath;
    this.tabletDirectory = tabletPaths.dir;
    for (Entry<Long,List<FileRef>> entry : data.getBulkImported().entrySet()) {
      this.bulkImported.put(entry.getKey(), new CopyOnWriteArrayList<>(entry.getValue()));
    }
    setupDefaultSecurityLabels(extent);

    final List<LogEntry> logEntries = tabletPaths.logEntries;
    final SortedMap<FileRef,DataFileValue> datafiles = tabletPaths.datafiles;

    tableConfiguration.addObserver(configObserver = new ConfigurationObserver() {

      private void reloadConstraints() {
        log.debug("Reloading constraints for extent: " + extent);
        constraintChecker.set(new ConstraintChecker(tableConfiguration));
      }

      @Override
      public void propertiesChanged() {
        reloadConstraints();

        try {
          setupDefaultSecurityLabels(extent);
        } catch (Exception e) {
          log.error("Failed to reload default security labels for extent: {}", extent.toString());
        }
      }

      @Override
      public void propertyChanged(String prop) {
        if (prop.startsWith(Property.TABLE_CONSTRAINT_PREFIX.getKey()))
          reloadConstraints();
        else if (prop.equals(Property.TABLE_DEFAULT_SCANTIME_VISIBILITY.getKey())) {
          try {
            log.info("Default security labels changed for extent: {}", extent.toString());
            setupDefaultSecurityLabels(extent);
          } catch (Exception e) {
            log.error("Failed to reload default security labels for extent: {}", extent.toString());
          }
        }

      }

      @Override
      public void sessionExpired() {
        log.debug("Session expired, no longer updating per table props...");
      }

    });

    tableConfiguration.getNamespaceConfiguration().addObserver(configObserver);
    tabletMemory = new TabletMemory(this);

    // Force a load of any per-table properties
    configObserver.propertiesChanged();
    if (!logEntries.isEmpty()) {
      log.info("Starting Write-Ahead Log recovery for {}", this.extent);
      final AtomicLong entriesUsedOnTablet = new AtomicLong(0);
      // track max time from walog entries without timestamps
      final AtomicLong maxTime = new AtomicLong(Long.MIN_VALUE);
      final CommitSession commitSession = getTabletMemory().getCommitSession();
      try {
        Set<String> absPaths = new HashSet<>();
        for (FileRef ref : datafiles.keySet())
          absPaths.add(ref.path().toString());

        tabletServer.recover(this.getTabletServer().getFileSystem(), extent, tableConfiguration, logEntries, absPaths, new MutationReceiver() {
          @Override
          public void receive(Mutation m) {
            // LogReader.printMutation(m);
            Collection<ColumnUpdate> muts = m.getUpdates();
            for (ColumnUpdate columnUpdate : muts) {
              if (!columnUpdate.hasTimestamp()) {
                // if it is not a user set timestamp, it must have been set
                // by the system
                maxTime.set(Math.max(maxTime.get(), columnUpdate.getTimestamp()));
              }
            }
            getTabletMemory().mutate(commitSession, Collections.singletonList(m));
            entriesUsedOnTablet.incrementAndGet();
          }
        });

        if (maxTime.get() != Long.MIN_VALUE) {
          tabletTime.useMaxTimeFromWALog(maxTime.get());
        }
        commitSession.updateMaxCommittedTime(tabletTime.getTime());

        if (entriesUsedOnTablet.get() == 0) {
          log.debug("No replayed mutations applied, removing unused entries for {}", extent);
          MetadataTableUtil.removeUnusedWALEntries(getTabletServer(), extent, logEntries, tabletServer.getLock());

          // No replication update to be made because the fact that this tablet didn't use any mutations
          // from the WAL implies nothing about use of this WAL by other tablets. Do nothing.

          logEntries.clear();
        } else if (ReplicationConfigurationUtil.isEnabled(extent, tabletServer.getTableConfiguration(extent))) {
          // The logs are about to be re-used by this tablet, we need to record that they have data for this extent,
          // but that they may get more data. logEntries is not cleared which will cause the elements
          // in logEntries to be added to the currentLogs for this Tablet below.
          //
          // This update serves the same purpose as an update during a MinC. We know that the WAL was defined
          // (written when the WAL was opened) but this lets us know there are mutations written to this WAL
          // that could potentially be replicated. Because the Tablet is using this WAL, we can be sure that
          // the WAL isn't closed (WRT replication Status) and thus we're safe to update its progress.
          Status status = StatusUtil.openWithUnknownLength();
          for (LogEntry logEntry : logEntries) {
            log.debug("Writing updated status to metadata table for {} {}", logEntry.filename, ProtobufUtil.toString(status));
            ReplicationTableUtil.updateFiles(tabletServer, extent, logEntry.filename, status);
          }
        }

      } catch (Throwable t) {
        if (tableConfiguration.getBoolean(Property.TABLE_FAILURES_IGNORE)) {
          log.warn("Error recovering from log files: ", t);
        } else {
          throw new RuntimeException(t);
        }
      }
      // make some closed references that represent the recovered logs
      currentLogs = new ConcurrentSkipListSet<>();
      for (LogEntry logEntry : logEntries) {
        currentLogs.add(new DfsLogger(tabletServer.getServerConfig(), logEntry.filename, logEntry.getColumnQualifier().toString()));
      }

      log.info("Write-Ahead Log recovery complete for {} ({} mutations applied, {} entries created)", this.extent, entriesUsedOnTablet.get(), getTabletMemory()
          .getNumEntries());
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

    log.debug("TABLET_HIST {} opened", extent);
  }

  private void removeOldTemporaryFiles() {
    // remove any temporary files created by a previous tablet server
    try {
      for (FileStatus tmp : getTabletServer().getFileSystem().globStatus(new Path(location, "*_tmp"))) {
        try {
          log.debug("Removing old temp file {}", tmp.getPath());
          getTabletServer().getFileSystem().delete(tmp.getPath());
        } catch (IOException ex) {
          log.error("Unable to remove old temp file " + tmp.getPath() + ": " + ex);
        }
      }
    } catch (IOException ex) {
      log.error("Error scanning for old temp files in {}", location);
    }
  }

  private void setupDefaultSecurityLabels(KeyExtent extent) {
    if (extent.isMeta()) {
      defaultSecurityLabel = new byte[0];
    } else {
      try {
        ColumnVisibility cv = new ColumnVisibility(tableConfiguration.get(Property.TABLE_DEFAULT_SCANTIME_VISIBILITY));
        this.defaultSecurityLabel = cv.getExpression();
      } catch (Exception e) {
        log.error("{}", e.getMessage(), e);
        this.defaultSecurityLabel = new byte[0];
      }
    }
  }

  private LookupResult lookup(SortedKeyValueIterator<Key,Value> mmfi, List<Range> ranges, HashSet<Column> columnSet, List<KVEntry> results,
      long maxResultsSize, long batchTimeOut) throws IOException {

    LookupResult lookupResult = new LookupResult();

    boolean exceededMemoryUsage = false;
    boolean tabletClosed = false;

    Set<ByteSequence> cfset = null;
    if (columnSet.size() > 0)
      cfset = LocalityGroupUtil.families(columnSet);

    long returnTime = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(batchTimeOut);
    if (batchTimeOut <= 0 || batchTimeOut == Long.MAX_VALUE) {
      batchTimeOut = 0;
    }

    // determine if the iterator supported yielding
    YieldCallback<Key> yield = new YieldCallback<>();
    mmfi.enableYielding(yield);
    boolean yielded = false;

    for (Range range : ranges) {

      boolean timesUp = batchTimeOut > 0 && System.nanoTime() > returnTime;

      if (exceededMemoryUsage || tabletClosed || timesUp || yielded) {
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
          if (yield.hasYielded()) {
            throw new IOException("Coding error: hasTop returned true but has yielded at " + yield.getPositionAndReset());
          }
          Key key = mmfi.getTopKey();

          KVEntry kve = new KVEntry(key, mmfi.getTopValue());
          results.add(kve);
          entriesAdded++;
          lookupResult.bytesAdded += kve.estimateMemoryUsed();
          lookupResult.dataSize += kve.numBytes();

          exceededMemoryUsage = lookupResult.bytesAdded > maxResultsSize;

          timesUp = batchTimeOut > 0 && System.nanoTime() > returnTime;

          if (exceededMemoryUsage || timesUp) {
            addUnfinishedRange(lookupResult, range, key, false);
            break;
          }

          mmfi.next();
        }

        if (yield.hasYielded()) {
          yielded = true;
          Key yieldPosition = yield.getPositionAndReset();
          if (!range.contains(yieldPosition)) {
            throw new IOException("Underlying iterator yielded to a position outside of its range: " + yieldPosition + " not in " + range);
          }
          if (!results.isEmpty() && yieldPosition.compareTo(results.get(results.size() - 1).getKey()) <= 0) {
            throw new IOException("Underlying iterator yielded to a position that does not follow the last key returned: " + yieldPosition + " <= "
                + results.get(results.size() - 1).getKey());
          }
          addUnfinishedRange(lookupResult, range, yieldPosition, false);

          log.debug("Scan yield detected at position " + yieldPosition);
          Metrics scanMetrics = getTabletServer().getScanMetrics();
          if (scanMetrics.isEnabled())
            scanMetrics.add(TabletServerScanMetrics.YIELD, 1);
        }
      } catch (TooManyFilesException tmfe) {
        // treat this as a closed tablet, and let the client retry
        log.warn("Tablet {} has too many files, batch lookup can not run", getExtent());
        handleTabletClosedDuringScan(results, lookupResult, exceededMemoryUsage, range, entriesAdded);
        tabletClosed = true;
      } catch (IOException ioe) {
        if (shutdownInProgress()) {
          // assume HDFS shutdown hook caused this exception
          log.debug("IOException while shutdown in progress", ioe);
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

  private void handleTabletClosedDuringScan(List<KVEntry> results, LookupResult lookupResult, boolean exceededMemoryUsage, Range range, int entriesAdded) {
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

  public void checkConditions(ConditionChecker checker, Authorizations authorizations, AtomicBoolean iFlag) throws IOException {

    ScanDataSource dataSource = new ScanDataSource(this, authorizations, this.defaultSecurityLabel, iFlag);

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

  public LookupResult lookup(List<Range> ranges, HashSet<Column> columns, Authorizations authorizations, List<KVEntry> results, long maxResultSize,
      List<IterInfo> ssiList, Map<String,Map<String,String>> ssio, AtomicBoolean interruptFlag, SamplerConfiguration samplerConfig, long batchTimeOut,
      String classLoaderContext) throws IOException {

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

    ScanDataSource dataSource = new ScanDataSource(this, authorizations, this.defaultSecurityLabel, columns, ssiList, ssio, interruptFlag, samplerConfig,
        batchTimeOut, classLoaderContext);

    LookupResult result = null;

    try {
      SortedKeyValueIterator<Key,Value> iter = new SourceSwitchingIterator(dataSource);
      result = lookup(iter, ranges, columns, results, maxResultSize, batchTimeOut);
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

  Batch nextBatch(SortedKeyValueIterator<Key,Value> iter, Range range, int num, Set<Column> columns, long batchTimeOut, boolean isolated) throws IOException {

    // log.info("In nextBatch..");

    long stopTime = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(batchTimeOut);
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
    if (!isolated) {
      iter.enableYielding(yield);
    }

    if (columns.size() == 0) {
      iter.seek(range, LocalityGroupUtil.EMPTY_CF_SET, false);
    } else {
      iter.seek(range, LocalityGroupUtil.families(columns), true);
    }

    while (iter.hasTop()) {
      if (yield.hasYielded()) {
        throw new IOException("Coding error: hasTop returned true but has yielded at " + yield.getPositionAndReset());
      }
      value = iter.getTopValue();
      key = iter.getTopKey();

      KVEntry kvEntry = new KVEntry(key, value); // copies key and value
      results.add(kvEntry);
      resultSize += kvEntry.estimateMemoryUsed();
      resultBytes += kvEntry.numBytes();

      boolean timesUp = batchTimeOut > 0 && System.nanoTime() >= stopTime;

      if (resultSize >= maxResultsSize || results.size() >= num || timesUp) {
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
        throw new IOException("Underlying iterator yielded to a position outside of its range: " + continueKey + " not in " + range);
      }
      if (!results.isEmpty() && continueKey.compareTo(results.get(results.size() - 1).getKey()) <= 0) {
        throw new IOException("Underlying iterator yielded to a position that does not follow the last key returned: " + continueKey + " <= "
            + results.get(results.size() - 1).getKey());
      }

      log.debug("Scan yield detected at position " + continueKey);
      Metrics scanMetrics = getTabletServer().getScanMetrics();
      if (scanMetrics.isEnabled())
        scanMetrics.add(TabletServerScanMetrics.YIELD, 1);
    } else if (iter.hasTop() == false) {
      // end of tablet has been reached
      continueKey = null;
      if (results.size() == 0)
        results = null;
    }

    return new Batch(skipContinueKey, results, continueKey, resultBytes);
  }

  /**
   * Determine if a JVM shutdown is in progress.
   *
   */
  boolean shutdownInProgress() {
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

  public Scanner createScanner(Range range, int num, Set<Column> columns, Authorizations authorizations, List<IterInfo> ssiList,
      Map<String,Map<String,String>> ssio, boolean isolated, AtomicBoolean interruptFlag, SamplerConfiguration samplerConfig, long batchTimeOut,
      String classLoaderContext) {
    // do a test to see if this range falls within the tablet, if it does not
    // then clip will throw an exception
    extent.toDataRange().clip(range);

    ScanOptions opts = new ScanOptions(num, authorizations, this.defaultSecurityLabel, columns, ssiList, ssio, interruptFlag, isolated, samplerConfig,
        batchTimeOut, classLoaderContext);
    return new Scanner(this, range, opts);
  }

  DataFileValue minorCompact(VolumeManager fs, InMemoryMap memTable, FileRef tmpDatafile, FileRef newDatafile, FileRef mergeFile, boolean hasQueueTime,
      long queued, CommitSession commitSession, long flushId, MinorCompactionReason mincReason) {
    boolean failed = false;
    long start = System.currentTimeMillis();
    timer.incrementStatusMinor();

    long count = 0;

    String oldName = Thread.currentThread().getName();
    try {
      Thread.currentThread().setName("Minor compacting " + this.extent);
      Span span = Trace.start("write");
      CompactionStats stats;
      try {
        count = memTable.getNumEntries();

        DataFileValue dfv = null;
        if (mergeFile != null)
          dfv = getDatafileManager().getDatafileSizes().get(mergeFile);

        MinorCompactor compactor = new MinorCompactor(tabletServer, this, memTable, mergeFile, dfv, tmpDatafile, mincReason, tableConfiguration);
        stats = compactor.call();
      } finally {
        span.stop();
      }
      span = Trace.start("bringOnline");
      try {
        getDatafileManager().bringMinorCompactionOnline(tmpDatafile, newDatafile, mergeFile, new DataFileValue(stats.getFileSize(), stats.getEntriesWritten()),
            commitSession, flushId);
      } finally {
        span.stop();
      }
      return new DataFileValue(stats.getFileSize(), stats.getEntriesWritten());
    } catch (Exception e) {
      failed = true;
      throw new RuntimeException(e);
    } catch (Error e) {
      // Weird errors like "OutOfMemoryError" when trying to create the thread for the compaction
      failed = true;
      throw new RuntimeException(e);
    } finally {
      Thread.currentThread().setName(oldName);
      try {
        getTabletMemory().finalizeMinC();
      } catch (Throwable t) {
        log.error("Failed to free tablet memory", t);
      }

      if (!failed) {
        lastMinorCompactionFinishTime = System.currentTimeMillis();
      }
      Metrics minCMetrics = getTabletServer().getMinCMetrics();
      if (minCMetrics.isEnabled())
        minCMetrics.add(TabletServerMinCMetrics.MINC, (lastMinorCompactionFinishTime - start));
      if (hasQueueTime) {
        timer.updateTime(Operation.MINOR, queued, start, count, failed);
        if (minCMetrics.isEnabled())
          minCMetrics.add(TabletServerMinCMetrics.QUEUE, (start - queued));
      } else
        timer.updateTime(Operation.MINOR, start, count, failed);
    }
  }

  private synchronized MinorCompactionTask prepareForMinC(long flushId, MinorCompactionReason mincReason) {
    CommitSession oldCommitSession = getTabletMemory().prepareForMinC();
    otherLogs = currentLogs;
    currentLogs = new ConcurrentSkipListSet<>();

    FileRef mergeFile = null;
    if (mincReason != MinorCompactionReason.RECOVERY) {
      mergeFile = getDatafileManager().reserveMergingMinorCompactionFile();
    }

    double tracePercent = tabletServer.getConfiguration().getFraction(Property.TSERV_MINC_TRACE_PERCENT);

    return new MinorCompactionTask(this, mergeFile, oldCommitSession, flushId, mincReason, tracePercent);

  }

  public void flush(long tableFlushID) {
    boolean updateMetadata = false;
    boolean initiateMinor = false;

    try {

      synchronized (this) {

        // only want one thing at a time to update flush ID to ensure that metadata table and tablet in memory state are consistent
        if (updatingFlushID)
          return;

        if (lastFlushID >= tableFlushID)
          return;

        if (isClosing() || isClosed() || getTabletMemory().memoryReservedForMinC())
          return;

        if (getTabletMemory().getMemTable().getNumEntries() == 0) {
          lastFlushID = tableFlushID;
          updatingFlushID = true;
          updateMetadata = true;
        } else
          initiateMinor = true;
      }

      if (updateMetadata) {
        // if multiple threads were allowed to update this outside of a sync block, then it would be
        // a race condition
        MetadataTableUtil.updateTabletFlushID(extent, tableFlushID, tabletServer, getTabletServer().getLock());
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

  public boolean initiateMinorCompaction(MinorCompactionReason mincReason) {
    if (isClosed()) {
      // don't bother trying to get flush id if closed... could be closed after this check but that is ok... just trying to cut down on uneeded log messages....
      return false;
    }

    // get the flush id before the new memmap is made available for write
    long flushId;
    try {
      flushId = getFlushID();
    } catch (NoNodeException e) {
      log.info("Asked to initiate MinC when there was no flush id {} {}", getExtent(), e.getMessage());
      return false;
    }
    return initiateMinorCompaction(flushId, mincReason);
  }

  public boolean minorCompactNow(MinorCompactionReason mincReason) {
    long flushId;
    try {
      flushId = getFlushID();
    } catch (NoNodeException e) {
      log.info("Asked to initiate MinC when there was no flush id {} {}", getExtent(), e.getMessage());
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
    getTabletResources().executeMinorCompaction(mct);
    return true;
  }

  private MinorCompactionTask createMinorCompactionTask(long flushId, MinorCompactionReason mincReason) {
    MinorCompactionTask mct;
    long t1, t2;

    StringBuilder logMessage = null;

    try {
      synchronized (this) {
        t1 = System.currentTimeMillis();

        if (isClosing() || isClosed() || majorCompactionState == CompactionState.WAITING_TO_START || getTabletMemory().memoryReservedForMinC()
            || getTabletMemory().getMemTable().getNumEntries() == 0 || updatingFlushID) {

          logMessage = new StringBuilder();

          logMessage.append(extent.toString());
          logMessage.append(" closeState " + closeState);
          logMessage.append(" majorCompactionState " + majorCompactionState);
          if (getTabletMemory() != null)
            logMessage.append(" tabletMemory.memoryReservedForMinC() " + getTabletMemory().memoryReservedForMinC());
          if (getTabletMemory() != null && getTabletMemory().getMemTable() != null)
            logMessage.append(" tabletMemory.getMemTable().getNumEntries() " + getTabletMemory().getMemTable().getNumEntries());
          logMessage.append(" updatingFlushID " + updatingFlushID);

          return null;
        }

        mct = prepareForMinC(flushId, mincReason);
        t2 = System.currentTimeMillis();
      }
    } finally {
      // log outside of sync block
      if (logMessage != null && log.isDebugEnabled())
        log.debug("{}", logMessage);
    }

    log.debug(String.format("MinC initiate lock %.2f secs", (t2 - t1) / 1000.0));
    return mct;
  }

  public long getFlushID() throws NoNodeException {
    try {
      String zTablePath = Constants.ZROOT + "/" + tabletServer.getInstance().getInstanceID() + Constants.ZTABLES + "/" + extent.getTableId()
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
    String zTablePath = Constants.ZROOT + "/" + tabletServer.getInstance().getInstanceID() + Constants.ZTABLES + "/" + extent.getTableId()
        + Constants.ZTABLE_COMPACT_CANCEL_ID;

    try {
      return Long.parseLong(new String(ZooReaderWriter.getInstance().getData(zTablePath, null), UTF_8));
    } catch (KeeperException e) {
      throw new RuntimeException(e);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  public Pair<Long,UserCompactionConfig> getCompactionID() throws NoNodeException {
    try {
      String zTablePath = Constants.ZROOT + "/" + tabletServer.getInstance().getInstanceID() + Constants.ZTABLES + "/" + extent.getTableId()
          + Constants.ZTABLE_COMPACT_ID;

      String[] tokens = new String(ZooReaderWriter.getInstance().getData(zTablePath, null), UTF_8).split(",");
      long compactID = Long.parseLong(tokens[0]);

      UserCompactionConfig compactionConfig = new UserCompactionConfig();

      if (tokens.length > 1) {
        Hex hex = new Hex();
        ByteArrayInputStream bais = new ByteArrayInputStream(hex.decode(tokens[1].split("=")[1].getBytes(UTF_8)));
        DataInputStream dis = new DataInputStream(bais);

        try {
          compactionConfig.readFields(dis);
        } catch (IOException e) {
          throw new RuntimeException(e);
        }

        KeyExtent ke = new KeyExtent(extent.getTableId(), compactionConfig.getEndRow(), compactionConfig.getStartRow());

        if (!ke.overlaps(extent)) {
          // only use iterators if compaction range overlaps
          compactionConfig = new UserCompactionConfig();
        }
      }

      return new Pair<>(compactID, compactionConfig);
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

  private synchronized CommitSession finishPreparingMutations(long time) {
    if (writesInProgress < 0) {
      throw new IllegalStateException("waitingForLogs < 0 " + writesInProgress);
    }

    if (isClosed() || getTabletMemory() == null) {
      return null;
    }

    writesInProgress++;
    CommitSession commitSession = getTabletMemory().getCommitSession();
    commitSession.incrementCommitsInProgress();
    commitSession.updateMaxCommittedTime(time);
    return commitSession;
  }

  public void checkConstraints() {
    ConstraintChecker cc = constraintChecker.get();

    if (cc.classLoaderChanged()) {
      ConstraintChecker ncc = new ConstraintChecker(tableConfiguration);
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
          violators = new ArrayList<>();
        violators.add(mutation);
      }
    }

    long time = tabletTime.setUpdateTimes(mutations);

    if (!violations.isEmpty()) {

      HashSet<Mutation> violatorsSet = new HashSet<>(violators);
      ArrayList<Mutation> nonViolators = new ArrayList<>();

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

  @Override
  public synchronized void abortCommit(CommitSession commitSession, List<Mutation> value) {
    if (writesInProgress <= 0) {
      throw new IllegalStateException("waitingForLogs <= 0 " + writesInProgress);
    }

    if (isCloseComplete() || getTabletMemory() == null) {
      throw new IllegalStateException("aborting commit when tablet is closed");
    }

    commitSession.decrementCommitsInProgress();
    writesInProgress--;
    if (writesInProgress == 0)
      this.notifyAll();
  }

  @Override
  public void commit(CommitSession commitSession, List<Mutation> mutations) {

    int totalCount = 0;
    long totalBytes = 0;

    // write the mutation to the in memory table
    for (Mutation mutation : mutations) {
      totalCount += mutation.size();
      totalBytes += mutation.numBytes();
    }

    getTabletMemory().mutate(commitSession, mutations);

    synchronized (this) {
      if (writesInProgress < 1) {
        throw new IllegalStateException("commiting mutations after logging, but not waiting for any log messages");
      }

      if (isCloseComplete()) {
        throw new IllegalStateException("tablet closed with outstanding messages to the logger");
      }

      getTabletMemory().updateMemoryUsageStats();

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

    log.debug("initiateClose(saveState={} queueMinC={} disableWrites={}) {}", saveState, queueMinC, disableWrites, getExtent());

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

      // determines if inserts and queries can still continue while minor compacting
      if (disableWrites) {
        closeState = CloseState.CLOSED;
      }

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
        throw new RuntimeException(e);
      }

      if (queueMinC) {
        getTabletResources().executeMinorCompaction(mct);
        return;
      }

    }

    // do minor compaction outside of synch block so that tablet can be read and written to while
    // compaction runs
    mct.run();
  }

  private boolean closeCompleting = false;

  synchronized void completeClose(boolean saveState, boolean completeClose) throws IOException {

    if (!isClosing() || isCloseComplete() || closeCompleting) {
      throw new IllegalStateException("closeState = " + closeState);
    }

    log.debug("completeClose(saveState={} completeClose={}) {}", saveState, completeClose, getExtent());

    // ensure this method is only called once, also guards against multiple
    // threads entering the method at the same time
    closeCompleting = true;
    closeState = CloseState.CLOSED;

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

    getTabletMemory().waitForMinC();

    if (saveState && getTabletMemory().getMemTable().getNumEntries() > 0) {
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
          log.error("Consistency check fails, retrying", t);
          sleepUninterruptibly(500, TimeUnit.MILLISECONDS);
        }
      }
      if (err != null) {
        ProblemReports.getInstance(tabletServer).report(new ProblemReport(extent.getTableId(), ProblemType.TABLET_LOAD, this.extent.toString(), err));
        log.error("Tablet closed consistency check has failed for {} giving up and closing", this.extent);
      }
    }

    try {
      getTabletMemory().getMemTable().delete(0);
    } catch (Throwable t) {
      log.error("Failed to delete mem table : " + t.getMessage(), t);
    }

    getTabletMemory().close();

    // close map files
    getTabletResources().close();

    log.debug("TABLET_HIST {} closed", extent);

    tableConfiguration.getNamespaceConfiguration().removeObserver(configObserver);
    tableConfiguration.removeObserver(configObserver);

    if (completeClose)
      closeState = CloseState.COMPLETE;
  }

  private void closeConsistencyCheck() {

    if (getTabletMemory().getMemTable().getNumEntries() != 0) {
      String msg = "Closed tablet " + extent + " has " + getTabletMemory().getMemTable().getNumEntries() + " entries in memory";
      log.error(msg);
      throw new RuntimeException(msg);
    }

    if (getTabletMemory().memoryReservedForMinC()) {
      String msg = "Closed tablet " + extent + " has minor compacting memory";
      log.error(msg);
      throw new RuntimeException(msg);
    }

    try {
      Pair<List<LogEntry>,SortedMap<FileRef,DataFileValue>> fileLog = MetadataTableUtil.getFileAndLogEntries(tabletServer, extent);

      if (fileLog.getFirst().size() != 0) {
        String msg = "Closed tablet " + extent + " has walog entries in " + MetadataTable.NAME + " " + fileLog.getFirst();
        log.error(msg);
        throw new RuntimeException(msg);
      }

      if (extent.isRootTablet()) {
        if (!fileLog.getSecond().keySet().equals(getDatafileManager().getDatafileSizes().keySet())) {
          String msg = "Data file in " + RootTable.NAME + " differ from in memory data " + extent + "  " + fileLog.getSecond().keySet() + "  "
              + getDatafileManager().getDatafileSizes().keySet();
          log.error(msg);
          throw new RuntimeException(msg);
        }
      } else {
        if (!fileLog.getSecond().equals(getDatafileManager().getDatafileSizes())) {
          String msg = "Data file in " + MetadataTable.NAME + " differ from in memory data " + extent + "  " + fileLog.getSecond() + "  "
              + getDatafileManager().getDatafileSizes();
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

  public synchronized boolean initiateMajorCompaction(MajorCompactionReason reason) {

    if (isClosing() || isClosed() || !needsMajorCompaction(reason) || isMajorCompactionRunning() || majorCompactionQueued.contains(reason)) {
      return false;
    }

    majorCompactionQueued.add(reason);

    getTabletResources().executeMajorCompaction(getExtent(), new CompactionRunner(this, reason));

    return false;
  }

  /**
   * Returns true if a major compaction should be performed on the tablet.
   *
   */
  public boolean needsMajorCompaction(MajorCompactionReason reason) {
    if (isMajorCompactionRunning())
      return false;
    if (reason == MajorCompactionReason.CHOP || reason == MajorCompactionReason.USER)
      return true;
    return getTabletResources().needsMajorCompaction(getDatafileManager().getDatafileSizes(), reason);
  }

  /**
   * Returns an int representing the total block size of the files served by this tablet.
   *
   * @return size
   */
  // this is the size of just the files
  public long estimateTabletSize() {
    long size = 0L;

    for (DataFileValue sz : getDatafileManager().getDatafileSizes().values())
      size += sz.getSize();

    return size;
  }

  private boolean sawBigRow = false;
  private long timeOfLastMinCWhenBigFreakinRowWasSeen = 0;
  private long timeOfLastImportWhenBigFreakinRowWasSeen = 0;
  private final long splitCreationTime;

  private SplitRowSpec findSplitRow(Collection<FileRef> files) {

    // never split the root tablet
    // check if we already decided that we can never split
    // check to see if we're big enough to split

    long splitThreshold = tableConfiguration.getAsBytes(Property.TABLE_SPLIT_THRESHOLD);
    long maxEndRow = tableConfiguration.getAsBytes(Property.TABLE_MAX_END_ROW_SIZE);

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
      keys = FileUtil.findMidPoint(getTabletServer().getFileSystem(), tabletDirectory, getTabletServer().getConfiguration(), extent.getPrevEndRow(),
          extent.getEndRow(), FileUtil.toPathStrings(files), .25);
    } catch (IOException e) {
      log.error("Failed to find midpoint {}", e.getMessage());
      return null;
    }

    // check to see if one row takes up most of the tablet, in which case we can not split
    try {

      Text lastRow;
      if (extent.getEndRow() == null) {
        Key lastKey = (Key) FileUtil.findLastKey(getTabletServer().getFileSystem(), getTabletServer().getConfiguration(), files);
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
          if (candidate.getLength() > maxEndRow) {
            log.warn("Cannot split tablet {}, selected split point too long.  Length :  {}", extent, candidate.getLength());

            sawBigRow = true;
            timeOfLastMinCWhenBigFreakinRowWasSeen = lastMinorCompactionFinishTime;
            timeOfLastImportWhenBigFreakinRowWasSeen = lastMapFileImportTime;

            return null;
          }
          if (candidate.compareRow(lastRow) != 0) {
            // we should use this ratio in split size estimations
            if (log.isTraceEnabled())
              log.trace(String.format("Splitting at %6.2f instead of .5, row at .5 is same as end row%n", keys.firstKey()));
            return new SplitRowSpec(keys.firstKey(), candidate.getRow());
          }

        }

        log.warn("Cannot split tablet {} it contains a big row : {}", extent, lastRow);

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

      if (text.getLength() > maxEndRow) {
        log.warn("Cannot split tablet {}, selected split point too long.  Length :  {}", extent, text.getLength());

        sawBigRow = true;
        timeOfLastMinCWhenBigFreakinRowWasSeen = lastMinorCompactionFinishTime;
        timeOfLastImportWhenBigFreakinRowWasSeen = lastMapFileImportTime;

        return null;
      }

      return new SplitRowSpec(.5, text);
    } catch (IOException e) {
      // don't split now, but check again later
      log.error("Failed to find lastkey {}", e.getMessage());
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
    Map<FileRef,Pair<Key,Key>> result = new HashMap<>();
    FileOperations fileFactory = FileOperations.getInstance();
    VolumeManager fs = getTabletServer().getFileSystem();
    for (Entry<FileRef,DataFileValue> entry : allFiles.entrySet()) {
      FileRef file = entry.getKey();
      FileSystem ns = fs.getVolumeByPath(file.path()).getFileSystem();
      FileSKVIterator openReader = fileFactory.newReaderBuilder().forFile(file.path().toString(), ns, ns.getConf())
          .withTableConfiguration(this.getTableConfiguration()).seekToBeginning().build();
      try {
        Key first = openReader.getFirstKey();
        Key last = openReader.getLastKey();
        result.put(file, new Pair<>(first, last));
      } finally {
        openReader.close();
      }
    }
    return result;
  }

  List<FileRef> findChopFiles(KeyExtent extent, Map<FileRef,Pair<Key,Key>> firstAndLastKeys, Collection<FileRef> allFiles) throws IOException {
    List<FileRef> result = new ArrayList<>();
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
        if ((first == null && last == null) || (first != null && !extent.contains(first.getRow())) || (last != null && !extent.contains(last.getRow()))) {
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
    if (isClosing() || isClosed())
      return false;
    return findSplitRow(getDatafileManager().getFiles()) != null;
  }

  // BEGIN PRIVATE METHODS RELATED TO MAJOR COMPACTION

  private boolean isCompactionEnabled() {
    return !isClosing();
  }

  private CompactionStats _majorCompact(MajorCompactionReason reason) throws IOException, CompactionCanceledException {

    long t1, t2, t3;

    Pair<Long,UserCompactionConfig> compactionId = null;
    CompactionStrategy strategy = null;
    Map<FileRef,Pair<Key,Key>> firstAndLastKeys = null;

    if (reason == MajorCompactionReason.USER) {
      try {
        compactionId = getCompactionID();
        strategy = createCompactionStrategy(compactionId.getSecond().getCompactionStrategy());
      } catch (NoNodeException e) {
        throw new RuntimeException(e);
      }
    } else if (reason == MajorCompactionReason.NORMAL || reason == MajorCompactionReason.IDLE) {
      strategy = Property.createTableInstanceFromPropertyName(tableConfiguration, Property.TABLE_COMPACTION_STRATEGY, CompactionStrategy.class,
          new DefaultCompactionStrategy());
      strategy.init(Property.getCompactionStrategyOptions(tableConfiguration));
    } else if (reason == MajorCompactionReason.CHOP) {
      firstAndLastKeys = getFirstAndLastKeys(getDatafileManager().getDatafileSizes());
    } else {
      throw new IllegalArgumentException("Unknown compaction reason " + reason);
    }

    if (strategy != null) {
      BlockCache sc = tabletResources.getTabletServerResourceManager().getSummaryCache();
      BlockCache ic = tabletResources.getTabletServerResourceManager().getIndexCache();
      MajorCompactionRequest request = new MajorCompactionRequest(extent, reason, getTabletServer().getFileSystem(), tableConfiguration, sc, ic);
      request.setFiles(getDatafileManager().getDatafileSizes());
      strategy.gatherInformation(request);
    }

    Map<FileRef,DataFileValue> filesToCompact = null;

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

      VolumeManager fs = getTabletServer().getFileSystem();
      if (extent.isRootTablet()) {
        // very important that we call this before doing major compaction,
        // otherwise deleted compacted files could possible be brought back
        // at some point if the file they were compacted to was legitimately
        // removed by a major compaction
        RootFiles.cleanupReplacement(fs, fs.listStatus(this.location), false);
      }
      SortedMap<FileRef,DataFileValue> allFiles = getDatafileManager().getDatafileSizes();
      List<FileRef> inputFiles = new ArrayList<>();
      if (reason == MajorCompactionReason.CHOP) {
        // enforce rules: files with keys outside our range need to be compacted
        inputFiles.addAll(findChopFiles(extent, firstAndLastKeys, allFiles.keySet()));
      } else {
        MajorCompactionRequest request = new MajorCompactionRequest(extent, reason, tableConfiguration);
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
            log.debug("No-op major compaction by USER on 0 input files because no iterators present.");
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
        // If no original files will exist at the end of the compaction, we do not have to propogate deletes
        Set<FileRef> droppedFiles = new HashSet<>();
        droppedFiles.addAll(inputFiles);
        if (plan != null)
          droppedFiles.addAll(plan.deleteFiles);
        propogateDeletes = !(droppedFiles.equals(allFiles.keySet()));
        log.debug("Major compaction plan: {} propogate deletes : {}", plan, propogateDeletes);
        filesToCompact = new HashMap<>(allFiles);
        filesToCompact.keySet().retainAll(inputFiles);

        getDatafileManager().reserveMajorCompactingFiles(filesToCompact.keySet());
      }

      t3 = System.currentTimeMillis();
    }

    try {

      log.debug(String.format("MajC initiate lock %.2f secs, wait %.2f secs", (t3 - t2) / 1000.0, (t2 - t1) / 1000.0));

      if (updateCompactionID) {
        MetadataTableUtil.updateTabletCompactID(extent, compactionId.getFirst(), tabletServer, getTabletServer().getLock());
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
          throw new RuntimeException(e);
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
            if (lastCompactID >= compactionId.getFirst())
              // already compacted
              return majCStats;
          }
        }

      }

      // need to handle case where only one file is being major compacted
      // ACCUMULO-3645 run loop at least once, even if filesToCompact.isEmpty()
      do {
        int numToCompact = maxFilesToCompact;

        if (filesToCompact.size() > maxFilesToCompact && filesToCompact.size() < 2 * maxFilesToCompact) {
          // on the second to last compaction pass, compact the minimum amount of files possible
          numToCompact = filesToCompact.size() - maxFilesToCompact + 1;
        }

        Set<FileRef> smallestFiles = removeSmallest(filesToCompact, numToCompact);

        FileRef fileName = getNextMapFilename((filesToCompact.size() == 0 && !propogateDeletes) ? "A" : "C");
        FileRef compactTmpName = new FileRef(fileName.path().toString() + "_tmp");

        AccumuloConfiguration tableConf = createTableConfiguration(tableConfiguration, plan);

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

            @Override
            public RateLimiter getReadLimiter() {
              return getTabletServer().getMajorCompactionReadLimiter();
            }

            @Override
            public RateLimiter getWriteLimiter() {
              return getTabletServer().getMajorCompactionWriteLimiter();
            }

          };

          HashMap<FileRef,DataFileValue> copy = new HashMap<>(getDatafileManager().getDatafileSizes());
          if (!copy.keySet().containsAll(smallestFiles))
            throw new IllegalStateException("Cannot find data file values for " + smallestFiles);

          copy.keySet().retainAll(smallestFiles);

          log.debug("Starting MajC {} ({}) {} --> {} {}", extent, reason, copy.keySet(), compactTmpName, compactionIterators);

          // always propagate deletes, unless last batch
          boolean lastBatch = filesToCompact.isEmpty();
          Compactor compactor = new Compactor(tabletServer, this, copy, null, compactTmpName, lastBatch ? propogateDeletes : true, cenv, compactionIterators,
              reason.ordinal(), tableConf);

          CompactionStats mcs = compactor.call();

          span.data("files", "" + smallestFiles.size());
          span.data("read", "" + mcs.getEntriesRead());
          span.data("written", "" + mcs.getEntriesWritten());
          majCStats.add(mcs);

          if (lastBatch && plan != null && plan.deleteFiles != null) {
            smallestFiles.addAll(plan.deleteFiles);
          }
          getDatafileManager().bringMajorCompactionOnline(smallestFiles, compactTmpName, fileName,
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

      } while (filesToCompact.size() > 0);
      return majCStats;
    } finally {
      synchronized (Tablet.this) {
        getDatafileManager().clearMajorCompactingFile();
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

    // short-circuit; also handles zero files case
    if (filesToCompact.size() <= maxFilesToCompact) {
      Set<FileRef> smallestFiles = new HashSet<>(filesToCompact.keySet());
      filesToCompact.clear();
      return smallestFiles;
    }

    PriorityQueue<Pair<FileRef,Long>> fileHeap = new PriorityQueue<>(filesToCompact.size(), new Comparator<Pair<FileRef,Long>>() {
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
      fileHeap.add(new Pair<>(entry.getKey(), entry.getValue().getSize()));
    }

    Set<FileRef> smallestFiles = new HashSet<>();
    while (smallestFiles.size() < maxFilesToCompact && fileHeap.size() > 0) {
      Pair<FileRef,Long> pair = fileHeap.remove();
      filesToCompact.remove(pair.getFirst());
      smallestFiles.add(pair.getFirst());
    }

    return smallestFiles;
  }

  // END PRIVATE METHODS RELATED TO MAJOR COMPACTION

  /**
   * Performs a major compaction on the tablet. If needsSplit() returns true, the tablet is split and a reference to the new tablet is returned.
   */

  CompactionStats majorCompact(MajorCompactionReason reason, long queued) {
    CompactionStats majCStats = null;
    boolean success = false;
    long start = System.currentTimeMillis();

    timer.incrementStatusMajor();

    synchronized (this) {
      // check that compaction is still needed - defer to splitting
      majorCompactionQueued.remove(reason);

      if (isClosing() || isClosed() || !needsMajorCompaction(reason) || isMajorCompactionRunning() || needsSplit()) {
        return null;
      }

      majorCompactionState = CompactionState.WAITING_TO_START;
    }

    Span span = null;

    try {
      double tracePercent = tabletServer.getConfiguration().getFraction(Property.TSERV_MAJC_TRACE_PERCENT);
      ProbabilitySampler sampler = new ProbabilitySampler(tracePercent);
      span = Trace.on("majorCompaction", sampler);

      majCStats = _majorCompact(reason);
      if (reason == MajorCompactionReason.CHOP) {
        MetadataTableUtil.chopped(getTabletServer(), getExtent(), this.getTabletServer().getLock());
        getTabletServer().enqueueMasterMessage(new TabletStatusMessage(TabletLoadState.CHOPPED, extent));
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

      if (span != null) {
        span.data("extent", "" + getExtent());
        if (majCStats != null) {
          span.data("read", "" + majCStats.getEntriesRead());
          span.data("written", "" + majCStats.getEntriesWritten());
        }
        span.stop();
      }
    }
    long count = 0;
    if (majCStats != null)
      count = majCStats.getEntriesRead();
    timer.updateTime(Operation.MAJOR, queued, start, count, !success);

    return majCStats;
  }

  @Override
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

  public synchronized boolean isClosing() {
    return closeState == CloseState.CLOSING;
  }

  public synchronized boolean isClosed() {
    return closeState == CloseState.CLOSED || closeState == CloseState.COMPLETE;
  }

  public synchronized boolean isCloseComplete() {
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
    return majorCompactionQueued.size() > 0;
  }

  public TreeMap<KeyExtent,TabletData> split(byte[] sp) throws IOException {

    if (sp != null && extent.getEndRow() != null && extent.getEndRow().equals(new Text(sp))) {
      throw new IllegalArgumentException();
    }

    if (sp != null && sp.length > tableConfiguration.getAsBytes(Property.TABLE_MAX_END_ROW_SIZE)) {
      String msg = "Cannot split tablet " + extent + ", selected split point too long.  Length :  " + sp.length;
      log.warn(msg);
      throw new IOException(msg);
    }

    if (extent.isRootTablet()) {
      String msg = "Cannot split root tablet";
      log.warn(msg);
      throw new RuntimeException(msg);
    }

    try {
      initiateClose(true, false, false);
    } catch (IllegalStateException ise) {
      log.debug("File {} not splitting : {}", extent, ise.getMessage());
      return null;
    }

    // obtain this info outside of synch block since it will involve opening
    // the map files... it is ok if the set of map files changes, because
    // this info is used for optimization... it is ok if map files are missing
    // from the set... can still query and insert into the tablet while this
    // map file operation is happening
    Map<FileRef,FileUtil.FileInfo> firstAndLastRows = FileUtil.tryToGetFirstAndLastRows(getTabletServer().getFileSystem(),
        getTabletServer().getConfiguration(), getDatafileManager().getFiles());

    synchronized (this) {
      // java needs tuples ...
      TreeMap<KeyExtent,TabletData> newTablets = new TreeMap<>();

      long t1 = System.currentTimeMillis();
      // choose a split point
      SplitRowSpec splitPoint;
      if (sp == null)
        splitPoint = findSplitRow(getDatafileManager().getFiles());
      else {
        Text tsp = new Text(sp);
        splitPoint = new SplitRowSpec(FileUtil.estimatePercentageLTE(getTabletServer().getFileSystem(), tabletDirectory, getTabletServer().getConfiguration(),
            extent.getPrevEndRow(), extent.getEndRow(), FileUtil.toPathStrings(getDatafileManager().getFiles()), tsp), tsp);
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

      String lowDirectory = createTabletDirectory(getTabletServer().getFileSystem(), extent.getTableId(), midRow);

      // write new tablet information to MetadataTable
      SortedMap<FileRef,DataFileValue> lowDatafileSizes = new TreeMap<>();
      SortedMap<FileRef,DataFileValue> highDatafileSizes = new TreeMap<>();
      List<FileRef> highDatafilesToRemove = new ArrayList<>();

      MetadataTableUtil.splitDatafiles(midRow, splitRatio, firstAndLastRows, getDatafileManager().getDatafileSizes(), lowDatafileSizes, highDatafileSizes,
          highDatafilesToRemove);

      log.debug("Files for low split {} {}", low, lowDatafileSizes.keySet());
      log.debug("Files for high split {} {}", high, highDatafileSizes.keySet());

      String time = tabletTime.getMetadataValue();

      MetadataTableUtil.splitTablet(high, extent.getPrevEndRow(), splitRatio, getTabletServer(), getTabletServer().getLock());
      MasterMetadataUtil.addNewTablet(getTabletServer(), low, lowDirectory, getTabletServer().getTabletSession(), lowDatafileSizes, getBulkIngestedFiles(),
          time, lastFlushID, lastCompactID, getTabletServer().getLock());
      MetadataTableUtil.finishSplit(high, highDatafileSizes, highDatafilesToRemove, getTabletServer(), getTabletServer().getLock());

      log.debug("TABLET_HIST {} split {} {}", extent, low, high);

      newTablets.put(high, new TabletData(tabletDirectory, highDatafileSizes, time, lastFlushID, lastCompactID, lastLocation, getBulkIngestedFiles()));
      newTablets.put(low, new TabletData(lowDirectory, lowDatafileSizes, time, lastFlushID, lastCompactID, lastLocation, getBulkIngestedFiles()));

      long t2 = System.currentTimeMillis();

      log.debug(String.format("offline split time : %6.2f secs", (t2 - t1) / 1000.0));

      closeState = CloseState.COMPLETE;
      return newTablets;
    }
  }

  public SortedMap<FileRef,DataFileValue> getDatafiles() {
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
    Map<FileRef,DataFileValue> entries = new HashMap<>(fileMap.size());
    List<String> files = new ArrayList<>();

    for (Entry<FileRef,MapFileInfo> entry : fileMap.entrySet()) {
      entries.put(entry.getKey(), new DataFileValue(entry.getValue().estimatedSize, 0l));
      files.add(entry.getKey().path().toString());
    }

    // Clients timeout and will think that this operation failed.
    // Don't do it if we spent too long waiting for the lock
    long now = System.currentTimeMillis();
    synchronized (this) {
      if (isClosed()) {
        throw new IOException("tablet " + extent + " is closed");
      }

      // TODO check seems uneeded now - ACCUMULO-1291
      long lockWait = System.currentTimeMillis() - now;
      if (lockWait > getTabletServer().getConfiguration().getTimeInMillis(Property.GENERAL_RPC_TIMEOUT)) {
        throw new IOException("Timeout waiting " + (lockWait / 1000.) + " seconds to get tablet lock");
      }

      List<FileRef> alreadyImported = bulkImported.getIfPresent(tid);
      if (alreadyImported != null) {
        for (FileRef entry : alreadyImported) {
          if (fileMap.remove(entry) != null) {
            log.info("Ignoring import of bulk file already imported: " + entry);
          }
        }
      }
      if (fileMap.isEmpty()) {
        return;
      }

      if (writesInProgress < 0) {
        throw new IllegalStateException("writesInProgress < 0 " + writesInProgress);
      }

      writesInProgress++;
    }
    tabletServer.updateBulkImportState(files, BulkImportState.LOADING);
    try {
      getDatafileManager().importMapFiles(tid, entries, setTime);
      lastMapFileImportTime = System.currentTimeMillis();

      if (needsSplit()) {
        getTabletServer().executeSplit(this);
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

        try {
          bulkImported.get(tid, new Callable<List<FileRef>>() {
            @Override
            public List<FileRef> call() throws Exception {
              return new ArrayList<>();
            }
          }).addAll(fileMap.keySet());
        } catch (Exception ex) {
          log.info(ex.toString(), ex);
        }
        tabletServer.removeBulkImportState(files);
      }
    }
  }

  private ConcurrentSkipListSet<DfsLogger> currentLogs = new ConcurrentSkipListSet<>();

  // currentLogs may be updated while a tablet is otherwise locked
  public Set<DfsLogger> getCurrentLogFiles() {
    return new HashSet<>(currentLogs);
  }

  Set<String> beginClearingUnusedLogs() {
    Set<String> doomed = new HashSet<>();

    ArrayList<String> otherLogsCopy = new ArrayList<>();
    ArrayList<String> currentLogsCopy = new ArrayList<>();

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
      log.debug("Logs for memory compacted: {} {}", getExtent(), logger.toString());
    }

    for (String logger : currentLogsCopy) {
      log.debug("Logs for current memory: {} {}", getExtent(), logger);
    }

    for (String logger : doomed) {
      log.debug("Logs to be destroyed: {} {}", getExtent(), logger);
    }

    return doomed;
  }

  synchronized void finishClearingUnusedLogs() {
    removingLogs = false;
    logLock.unlock();
  }

  private Set<DfsLogger> otherLogs = Collections.emptySet();
  private boolean removingLogs = false;

  // this lock is basically used to synchronize writing of log info to metadata
  private final ReentrantLock logLock = new ReentrantLock();

  public int getLogCount() {
    return currentLogs.size();
  }

  // don't release the lock if this method returns true for success; instead, the caller should clean up by calling finishUpdatingLogsUsed()
  @Override
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

        if (memTable == getTabletMemory().getMinCMemTable())
          addToOther = true;
        else if (memTable == getTabletMemory().getMemTable())
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
            return !releaseLock;
        }

        int numAdded = 0;
        int numContained = 0;
        if (addToOther) {
          if (otherLogs.add(more))
            numAdded++;

          if (currentLogs.contains(more))
            numContained++;
        } else {
          if (currentLogs.add(more))
            numAdded++;

          if (otherLogs.contains(more))
            numContained++;
        }

        if (numAdded > 0 && numAdded != 1) {
          // expect to add all or none
          throw new IllegalArgumentException("Added subset of logs " + extent + " " + more + " " + currentLogs);
        }

        if (numContained > 0 && numContained != 1) {
          // expect to contain all or none
          throw new IllegalArgumentException("Other logs contained subset of logs " + extent + " " + more + " " + otherLogs);
        }

        if (numAdded > 0 && numContained == 0) {
          releaseLock = false;
        }

        return !releaseLock;
      }
    } finally {
      if (releaseLock)
        logLock.unlock();
    }
  }

  @Override
  public void finishUpdatingLogsUsed() {
    logLock.unlock();
  }

  synchronized public void chopFiles() {
    initiateMajorCompaction(MajorCompactionReason.CHOP);
  }

  private CompactionStrategy createCompactionStrategy(CompactionStrategyConfig strategyConfig) {
    String context = tableConfiguration.get(Property.TABLE_CLASSPATH);
    String clazzName = strategyConfig.getClassName();
    try {
      Class<? extends CompactionStrategy> clazz;
      if (context != null && !context.equals(""))
        clazz = AccumuloVFSClassLoader.getContextManager().loadClass(context, clazzName, CompactionStrategy.class);
      else
        clazz = AccumuloVFSClassLoader.loadClass(clazzName, CompactionStrategy.class);
      CompactionStrategy strategy = clazz.newInstance();
      strategy.init(strategyConfig.getOptions());
      return strategy;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public void compactAll(long compactionId, UserCompactionConfig compactionConfig) {
    boolean updateMetadata = false;

    synchronized (this) {
      if (lastCompactID >= compactionId)
        return;

      if (isMinorCompactionRunning()) {
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

      if (isClosing() || isClosed() || majorCompactionQueued.contains(MajorCompactionReason.USER) || isMajorCompactionRunning())
        return;

      CompactionStrategyConfig strategyConfig = compactionConfig.getCompactionStrategy();
      CompactionStrategy strategy = createCompactionStrategy(strategyConfig);

      MajorCompactionRequest request = new MajorCompactionRequest(extent, MajorCompactionReason.USER, tableConfiguration);
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
        throw new RuntimeException(e);
      }
    }

    if (updateMetadata) {
      try {
        // if multiple threads were allowed to update this outside of a sync block, then it would be
        // a race condition
        MetadataTableUtil.updateTabletCompactID(extent, compactionId, getTabletServer(), getTabletServer().getLock());
      } finally {
        synchronized (this) {
          majorCompactionState = null;
          this.notifyAll();
        }
      }
    }
  }

  @Override
  public TableConfiguration getTableConfiguration() {
    return tableConfiguration;
  }

  @Override
  public Durability getDurability() {
    return DurabilityImpl.fromString(getTableConfiguration().get(Property.TABLE_DURABILITY));
  }

  @Override
  public void updateMemoryUsageStats(long size, long mincSize) {
    getTabletResources().updateMemoryUsageStats(this, size, mincSize);
  }

  public long incrementDataSourceDeletions() {
    return dataSourceDeletions.incrementAndGet();
  }

  synchronized public void updateQueryStats(int size, long numBytes) {
    queryCount += size;
    queryBytes += numBytes;
  }

  TabletServer getTabletServer() {
    return tabletServer;
  }

  public void updatePersistedTime(long bulkTime, Map<FileRef,DataFileValue> paths, long tid) {
    synchronized (timeLock) {
      if (bulkTime > persistedTime)
        persistedTime = bulkTime;

      MetadataTableUtil.updateTabletDataFile(tid, extent, paths, tabletTime.getMetadataValue(persistedTime), getTabletServer(), getTabletServer().getLock());
    }

  }

  public void updateTabletDataFile(long maxCommittedTime, FileRef newDatafile, FileRef absMergeFile, DataFileValue dfv, Set<String> unusedWalLogs,
      Set<FileRef> filesInUseByScans, long flushId) {
    synchronized (timeLock) {
      if (maxCommittedTime > persistedTime)
        persistedTime = maxCommittedTime;

      String time = tabletTime.getMetadataValue(persistedTime);
      MasterMetadataUtil.updateTabletDataFile(getTabletServer(), extent, newDatafile, absMergeFile, dfv, time, filesInUseByScans,
          tabletServer.getClientAddressString(), tabletServer.getLock(), unusedWalLogs, lastLocation, flushId);
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

  synchronized public void addActiveScans(ScanDataSource scanDataSource) {
    activeScans.add(scanDataSource);
  }

  public int removeScan(ScanDataSource scanDataSource) {
    activeScans.remove(scanDataSource);
    return activeScans.size();
  }

  synchronized public void setLastCompactionID(Long compactionId) {
    if (compactionId != null)
      this.lastCompactID = compactionId;
  }

  public void removeMajorCompactionQueuedReason(MajorCompactionReason reason) {
    majorCompactionQueued.remove(reason);

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

  private static String createTabletDirectory(VolumeManager fs, Table.ID tableId, Text endRow) {
    String lowDirectory;

    UniqueNameAllocator namer = UniqueNameAllocator.getInstance();
    VolumeChooserEnvironment chooserEnv = new VolumeChooserEnvironment(tableId);
    String volume = fs.choose(chooserEnv, ServerConstants.getBaseUris()) + Constants.HDFS_TABLES_DIR + Path.SEPARATOR;

    while (true) {
      try {
        if (endRow == null) {
          lowDirectory = Constants.DEFAULT_TABLET_LOCATION;
          Path lowDirectoryPath = new Path(volume + "/" + tableId + "/" + lowDirectory);
          if (fs.exists(lowDirectoryPath) || fs.mkdirs(lowDirectoryPath)) {
            FileSystem pathFs = fs.getVolumeByPath(lowDirectoryPath).getFileSystem();
            return lowDirectoryPath.makeQualified(pathFs.getUri(), pathFs.getWorkingDirectory()).toString();
          }
          log.warn("Failed to create {} for unknown reason", lowDirectoryPath);
        } else {
          lowDirectory = "/" + Constants.GENERATED_TABLET_DIRECTORY_PREFIX + namer.getNextName();
          Path lowDirectoryPath = new Path(volume + "/" + tableId + "/" + lowDirectory);
          if (fs.exists(lowDirectoryPath))
            throw new IllegalStateException("Dir exist when it should not " + lowDirectoryPath);
          if (fs.mkdirs(lowDirectoryPath)) {
            FileSystem lowDirectoryFs = fs.getVolumeByPath(lowDirectoryPath).getFileSystem();
            return lowDirectoryPath.makeQualified(lowDirectoryFs.getUri(), lowDirectoryFs.getWorkingDirectory()).toString();
          }
        }
      } catch (IOException e) {
        log.warn("{}", e.getMessage(), e);
      }

      log.warn("Failed to create dir for tablet in table {} in volume {} will retry ...", tableId, volume);
      sleepUninterruptibly(3, TimeUnit.SECONDS);

    }
  }

  public Map<Long,List<FileRef>> getBulkIngestedFiles() {
    return new HashMap<>(bulkImported.asMap());
  }

  public void cleanupBulkLoadedFiles(Set<Long> tids) {
    for (Long tid : tids) {
      bulkImported.invalidate(tid);
    }
  }

}

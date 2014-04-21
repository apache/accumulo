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

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.accumulo.core.Constants;
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
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.data.thrift.IterInfo;
import org.apache.accumulo.core.data.thrift.MapFileInfo;
import org.apache.accumulo.core.file.FileOperations;
import org.apache.accumulo.core.file.FileSKVIterator;
import org.apache.accumulo.core.iterators.IterationInterruptedException;
import org.apache.accumulo.core.iterators.IteratorUtil.IteratorScope;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.system.SourceSwitchingIterator;
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
import org.apache.accumulo.core.tabletserver.thrift.TabletStats;
import org.apache.accumulo.core.util.LocalityGroupUtil;
import org.apache.accumulo.core.util.Pair;
import org.apache.accumulo.core.util.UtilWaitThread;
import org.apache.accumulo.server.client.HdfsZooInstance;
import org.apache.accumulo.server.conf.TableConfiguration;
import org.apache.accumulo.server.fs.FileRef;
import org.apache.accumulo.server.fs.VolumeManager;
import org.apache.accumulo.server.fs.VolumeManager.FileType;
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
import org.apache.accumulo.tserver.InMemoryMap;
import org.apache.accumulo.tserver.MinorCompactionReason;
import org.apache.accumulo.tserver.TConstraintViolationException;
import org.apache.accumulo.tserver.TLevel;
import org.apache.accumulo.tserver.TabletServer;
import org.apache.accumulo.tserver.TabletServer.TservConstraintEnv;
import org.apache.accumulo.tserver.TabletServerResourceManager.TabletResourceManager;
import org.apache.accumulo.tserver.TabletStatsKeeper;
import org.apache.accumulo.tserver.TabletStatsKeeper.Operation;
import org.apache.accumulo.tserver.TooManyFilesException;
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
import org.apache.accumulo.tserver.tablet.Compactor.CompactionCanceledException;
import org.apache.accumulo.tserver.tablet.Compactor.CompactionEnv;
import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.NoNodeException;

/**
 * 
 * Provide access to a single row range in a living TabletServer.
 * 
 */
public class Tablet implements TabletCommitter {
  static final Logger log = Logger.getLogger(Tablet.class);
  static private final List<LogEntry> NO_LOG_ENTRIES = Collections.emptyList();

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

  private TServerInstance lastLocation;
  private volatile boolean tableDirChecked = false;

  private final AtomicLong dataSourceDeletions = new AtomicLong(0);
  public long getDataSourceDeletions() { return dataSourceDeletions.get(); }
  private final Set<ScanDataSource> activeScans = new HashSet<ScanDataSource>();

  private volatile boolean closing = false;
  private boolean closed = false;
  private boolean closeComplete = false;

  private boolean updatingFlushID = false;

  private long lastFlushID = -1;
  private long lastCompactID = -1;
  
  private volatile boolean majorCompactionInProgress = false;
  private volatile boolean majorCompactionWaitingToStart = false;
  private Set<MajorCompactionReason> majorCompactionQueued = Collections.synchronizedSet(EnumSet.noneOf(MajorCompactionReason.class));
  
  private volatile boolean minorCompactionInProgress = false;
  private volatile boolean minorCompactionWaitingToStart = false;

  private final AtomicReference<ConstraintChecker> constraintChecker = new AtomicReference<ConstraintChecker>();

  private int writesInProgress = 0;

  private final TabletStatsKeeper timer = new TabletStatsKeeper();

  private final Rate queryRate = new Rate(0.2);
  private long queryCount = 0;

  private final Rate queryByteRate = new Rate(0.2);
  private long queryBytes = 0;

  private final Rate ingestRate = new Rate(0.2);
  private long ingestCount = 0;

  private final Rate ingestByteRate = new Rate(0.2);
  private long ingestBytes = 0;

  private byte[] defaultSecurityLabel = new byte[0];

  private long lastMinorCompactionFinishTime = 0;
  private long lastMapFileImportTime = 0;

  private volatile long numEntries = 0;
  private volatile long numEntriesInMemory = 0;

  private final Rate scannedRate = new Rate(0.2);
  private final AtomicLong scannedCount = new AtomicLong(0);

  private final ConfigurationObserver configObserver;

  private final int logId;
  
  public int getLogId() {
    return logId;
  }
  
  public class LookupResult {
    public List<Range> unfinishedRanges = new ArrayList<Range>();
    public long bytesAdded = 0;
    public long dataSize = 0;
    public boolean closed = false;
  }

  FileRef getNextMapFilename(String prefix) throws IOException {
    String extension = FileOperations.getNewFileExtension(this.tableConfiguration);
    checkTabletDir();
    return new FileRef(location.toString() + "/" + prefix + UniqueNameAllocator.getInstance().getNextName() + "." + extension);
  }

  private void checkTabletDir() throws IOException {
    if (!tableDirChecked) {
      FileStatus[] files = null;
      try {
        files = getTabletServer().getFileSystem().listStatus(this.location);
      } catch (FileNotFoundException ex) {
        // ignored
      }
      
      if (files == null) {
        if (this.location.getName().startsWith("c-"))
          log.debug("Tablet " + extent + " had no dir, creating " + this.location); // its a clone dir...
        else
          log.warn("Tablet " + extent + " had no dir, creating " + this.location);
      
        getTabletServer().getFileSystem().mkdirs(this.location);
      }
      tableDirChecked = true;
    }
  }

  public Tablet(TabletServer tabletServer, KeyExtent extent, TabletResourceManager trm, SplitInfo info) throws IOException {
    this(tabletServer, new Text(info.getDir()), extent, trm, info.getDatafiles(), info.getTime(), info.getInitFlushID(), info.getInitCompactID(), info.getLastLocation());
    splitCreationTime = System.currentTimeMillis();
  }

  private Tablet(TabletServer tabletServer, Text location, KeyExtent extent, TabletResourceManager trm,
      SortedMap<FileRef,DataFileValue> datafiles, String time, long initFlushID, long initCompactID, TServerInstance lastLocation) throws IOException {
    this(tabletServer, extent, location, trm, NO_LOG_ENTRIES, datafiles, time, lastLocation, new HashSet<FileRef>(), initFlushID,
        initCompactID);
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

  private static TServerInstance lookupLastServer(KeyExtent extent, SortedMap<Key,Value> tabletsKeyValues) {
    for (Entry<Key,Value> entry : tabletsKeyValues.entrySet()) {
      if (entry.getKey().getColumnFamily().compareTo(TabletsSection.LastLocationColumnFamily.NAME) == 0) {
        return new TServerInstance(entry.getValue(), entry.getKey().getColumnQualifier());
      }
    }
    return null;
  }
  
  public Tablet(TabletServer tabletServer, KeyExtent extent, Text location, TabletResourceManager trm, SortedMap<Key,Value> tabletsKeyValues) throws IOException {
    this(tabletServer, extent, location, trm, lookupLogEntries(extent, tabletsKeyValues), lookupDatafiles(tabletServer.getSystemConfiguration(), tabletServer.getFileSystem(),
        extent, tabletsKeyValues), lookupTime(tabletServer.getSystemConfiguration(), extent, tabletsKeyValues), lookupLastServer(extent, tabletsKeyValues),
        lookupScanFiles(extent, tabletsKeyValues, tabletServer.getFileSystem()), lookupFlushID(extent, tabletsKeyValues), lookupCompactID(extent, tabletsKeyValues));
  }

  /**
   * yet another constructor - this one allows us to avoid costly lookups into the Metadata table if we already know the files we need - as at split time
   */
  private Tablet(final TabletServer tabletServer, final KeyExtent extent, final Text location, final TabletResourceManager trm, final List<LogEntry> rawLogEntries, final SortedMap<FileRef,DataFileValue> rawDatafiles, String time,
      final TServerInstance lastLocation, Set<FileRef> scanFiles, long initFlushID, long initCompactID) throws IOException {

    TabletFiles tabletPaths = VolumeUtil.updateTabletVolumes(tabletServer.getLock(), tabletServer.getFileSystem(), extent, new TabletFiles(location.toString(), rawLogEntries,
        rawDatafiles));

    Path locationPath;

    if (tabletPaths.dir.contains(":")) {
      locationPath = new Path(tabletPaths.dir.toString());
    } else {
      locationPath = tabletServer.getFileSystem().getFullPath(FileType.TABLE, extent.getTableId().toString() + tabletPaths.dir.toString());
    }

    final List<LogEntry> logEntries = tabletPaths.logEntries;
    final SortedMap<FileRef,DataFileValue> datafiles = tabletPaths.datafiles;

    this.location = locationPath;
    this.lastLocation = lastLocation;
    this.tabletDirectory = tabletPaths.dir;
    this.tableConfiguration = tabletServer.getTableConfiguration(extent);

    this.extent = extent;
    this.tabletResources = trm;

    this.lastFlushID = initFlushID;
    this.lastCompactID = initCompactID;

    if (extent.isRootTablet()) {
      long rtime = Long.MIN_VALUE;
      for (FileRef ref : datafiles.keySet()) {
        Path path = ref.path();
        FileSystem ns = tabletServer.getFileSystem().getVolumeByPath(path).getFileSystem();
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

    setupDefaultSecurityLabels(extent);

    tabletMemory = new TabletMemory(this);
    tabletTime = TabletTime.getInstance(time);
    persistedTime = tabletTime.getTime();

    tableConfiguration.addObserver(configObserver = new ConfigurationObserver() {

      private void reloadConstraints() {
        constraintChecker.set(new ConstraintChecker(tableConfiguration));
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

    tableConfiguration.getNamespaceConfiguration().addObserver(configObserver);

    // Force a load of any per-table properties
    configObserver.propertiesChanged();

    if (!logEntries.isEmpty()) {
      log.info("Starting Write-Ahead Log recovery for " + this.extent);
      final long[] count = new long[2];
      final CommitSession commitSession = getTabletMemory().getCommitSession();
      count[1] = Long.MIN_VALUE;
      try {
        Set<String> absPaths = new HashSet<String>();
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
                count[1] = Math.max(count[1], columnUpdate.getTimestamp());
              }
            }
            getTabletMemory().mutate(commitSession, Collections.singletonList(m));
            count[0]++;
          }
        });

        if (count[1] != Long.MIN_VALUE) {
          tabletTime.useMaxTimeFromWALog(count[1]);
        }
        commitSession.updateMaxCommittedTime(tabletTime.getTime());

        if (count[0] == 0) {
          MetadataTableUtil.removeUnusedWALEntries(extent, logEntries, tabletServer.getLock());
          logEntries.clear();
        }

      } catch (Throwable t) {
        if (tableConfiguration.getBoolean(Property.TABLE_FAILURES_IGNORE)) {
          log.warn("Error recovering from log files: ", t);
        } else {
          throw new RuntimeException(t);
        }
      }
      // make some closed references that represent the recovered logs
      currentLogs = new HashSet<DfsLogger>();
      for (LogEntry logEntry : logEntries) {
        for (String log : logEntry.logSet) {
          currentLogs.add(new DfsLogger(tabletServer.getServerConfig(), log));
        }
      }

      log.info("Write-Ahead Log recovery complete for " + this.extent + " (" + count[0] + " mutations applied, " + getTabletMemory().getNumEntries()
          + " entries created)");
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

    getDatafileManager().removeFilesAfterScan(scanFiles);

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
      for (FileStatus tmp : getTabletServer().getFileSystem().globStatus(new Path(location, "*_tmp"))) {
        try {
          log.debug("Removing old temp file " + tmp.getPath());
          getTabletServer().getFileSystem().delete(tmp.getPath());
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
        ColumnVisibility cv = new ColumnVisibility(tableConfiguration.get(Property.TABLE_DEFAULT_SCANTIME_VISIBILITY));
        this.defaultSecurityLabel = cv.getExpression();
      } catch (Exception e) {
        log.error(e, e);
        this.defaultSecurityLabel = new byte[0];
      }
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

    ScanDataSource dataSource = new ScanDataSource(this, authorizations, this.defaultSecurityLabel, columns, ssiList, ssio, interruptFlag);

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

  Batch nextBatch(SortedKeyValueIterator<Key,Value> iter, Range range, int num, Set<Column> columns) throws IOException {

    // log.info("In nextBatch..");

    List<KVEntry> results = new ArrayList<KVEntry>();
    Key key = null;

    Value value;
    long resultSize = 0L;
    long resultBytes = 0L;

    long maxResultsSize = tableConfiguration.getMemoryInBytes(Property.TABLE_SCAN_MAXMEM);

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

    if (endOfTabletReached) {
      continueKey = null;
    }

    if (endOfTabletReached && results.size() == 0)
      results = null;

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

  public Scanner createScanner(Range range, int num, Set<Column> columns, Authorizations authorizations, List<IterInfo> ssiList, Map<String,Map<String,String>> ssio,
      boolean isolated, AtomicBoolean interruptFlag) {
    // do a test to see if this range falls within the tablet, if it does not
    // then clip will throw an exception
    extent.toDataRange().clip(range);

    ScanOptions opts = new ScanOptions(num, authorizations, this.defaultSecurityLabel, columns, ssiList, ssio, interruptFlag, isolated);
    return new Scanner(this, range, opts);
  }
  DataFileValue minorCompact(VolumeManager fs, InMemoryMap memTable, FileRef tmpDatafile, FileRef newDatafile, FileRef mergeFile,
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
          dfv = getDatafileManager().getDatafileSizes().get(mergeFile);

        MinorCompactor compactor = new MinorCompactor(this, memTable, mergeFile, dfv, tmpDatafile, mincReason, tableConfiguration);
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
      try {
        getTabletMemory().finalizeMinC();
      } catch (Throwable t) {
        log.error("Failed to free tablet memory", t);
      }

      if (!failed) {
        lastMinorCompactionFinishTime = System.currentTimeMillis();
      }
      TabletServerMinCMetrics minCMetrics = getTabletServer().getMinCMetrics();
      if (minCMetrics.isEnabled())
        minCMetrics.add(TabletServerMinCMetrics.minc, (lastMinorCompactionFinishTime - start));
      if (hasQueueTime) {
        timer.updateTime(Operation.MINOR, queued, start, count, failed);
        if (minCMetrics.isEnabled())
          minCMetrics.add(TabletServerMinCMetrics.queue, (start - queued));
      } else
        timer.updateTime(Operation.MINOR, start, count, failed);
    }
  }

  private synchronized MinorCompactionTask prepareForMinC(long flushId, MinorCompactionReason mincReason) {
    CommitSession oldCommitSession = getTabletMemory().prepareForMinC();
    otherLogs = currentLogs;
    currentLogs = new HashSet<DfsLogger>();

    FileRef mergeFile = getDatafileManager().reserveMergingMinorCompactionFile();

    return new MinorCompactionTask(this, mergeFile, oldCommitSession, flushId, mincReason);

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

        if (closing || closed || getTabletMemory().memoryReservedForMinC())
          return;

        if (getTabletMemory().getMemTable().getNumEntries() == 0) {
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
        MetadataTableUtil.updateTabletFlushID(extent, tableFlushID, creds, getTabletServer().getLock());
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
      log.info("Asked to initiate MinC when there was no flush id " + getExtent() + " " + e.getMessage());
      return false;
    }
    return initiateMinorCompaction(flushId, mincReason);
  }

  public boolean minorCompactNow(MinorCompactionReason mincReason) {
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

        if (closing || closed || majorCompactionWaitingToStart || getTabletMemory().memoryReservedForMinC() || getTabletMemory().getMemTable().getNumEntries() == 0
            || updatingFlushID) {

          logMessage = new StringBuilder();

          logMessage.append(extent.toString());
          logMessage.append(" closing " + closing);
          logMessage.append(" closed " + closed);
          logMessage.append(" majorCompactionWaitingToStart " + majorCompactionWaitingToStart);
          if (getTabletMemory() != null)
            logMessage.append(" tabletMemory.memoryReservedForMinC() " + getTabletMemory().memoryReservedForMinC());
          if (getTabletMemory() != null && getTabletMemory().getMemTable() != null)
            logMessage.append(" tabletMemory.getMemTable().getNumEntries() " + getTabletMemory().getMemTable().getNumEntries());
          logMessage.append(" updatingFlushID " + updatingFlushID);

          return null;
        }
        // We're still recovering log entries
        if (getDatafileManager() == null) {
          logMessage = new StringBuilder();
          logMessage.append(extent.toString());
          logMessage.append(" datafileManager " + getDatafileManager());
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

  public long getFlushID() throws NoNodeException {
    try {
      String zTablePath = Constants.ZROOT + "/" + HdfsZooInstance.getInstance().getInstanceID() + Constants.ZTABLES + "/" + extent.getTableId()
          + Constants.ZTABLE_FLUSH_ID;
      return Long.parseLong(new String(ZooReaderWriter.getRetryingInstance().getData(zTablePath, null), StandardCharsets.UTF_8));
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
      return Long.parseLong(new String(ZooReaderWriter.getRetryingInstance().getData(zTablePath, null), StandardCharsets.UTF_8));
    } catch (KeeperException e) {
      throw new RuntimeException(e);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  public Pair<Long,List<IteratorSetting>> getCompactionID() throws NoNodeException {
    try {
      String zTablePath = Constants.ZROOT + "/" + HdfsZooInstance.getInstance().getInstanceID() + Constants.ZTABLES + "/" + extent.getTableId()
          + Constants.ZTABLE_COMPACT_ID;

      String[] tokens = new String(ZooReaderWriter.getRetryingInstance().getData(zTablePath, null), StandardCharsets.UTF_8).split(",");
      long compactID = Long.parseLong(tokens[0]);

      CompactionIterators iters = new CompactionIterators();

      if (tokens.length > 1) {
        Hex hex = new Hex();
        ByteArrayInputStream bais = new ByteArrayInputStream(hex.decode(tokens[1].split("=")[1].getBytes(StandardCharsets.UTF_8)));
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
    getTabletMemory().waitForMinC();
  }

  private synchronized CommitSession finishPreparingMutations(long time) {
    if (writesInProgress < 0) {
      throw new IllegalStateException("waitingForLogs < 0 " + writesInProgress);
    }

    if (closed || getTabletMemory() == null) {
      // log.debug("tablet closed, can't commit");
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

    if (closeComplete || getTabletMemory() == null) {
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

    getTabletMemory().mutate(commitSession, mutations);

    synchronized (this) {
      if (writesInProgress < 1) {
        throw new IllegalStateException("commiting mutations after logging, but not waiting for any log messages");
      }

      if (closed && closeComplete) {
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
      getTabletMemory().getMemTable().delete(0);
    } catch (Throwable t) {
      log.error("Failed to delete mem table : " + t.getMessage(), t);
    }

    getTabletMemory().close();

    // close map files
    getTabletResources().close();

    log.log(TLevel.TABLET_HIST, extent + " closed");

    tableConfiguration.getNamespaceConfiguration().removeObserver(configObserver);
    tableConfiguration.removeObserver(configObserver);

    closeComplete = completeClose;
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
      Pair<List<LogEntry>,SortedMap<FileRef,DataFileValue>> fileLog = MetadataTableUtil.getFileAndLogEntries(SystemCredentials.get(), extent);

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

    if (closing || closed || !needsMajorCompaction(reason) || majorCompactionInProgress || majorCompactionQueued.contains(reason)) {
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
    if (majorCompactionInProgress)
      return false;
    if (reason == MajorCompactionReason.CHOP || reason == MajorCompactionReason.USER)
      return true;
    return getTabletResources().needsMajorCompaction(getDatafileManager().getDatafileSizes(), reason);
  }

  /**
   * Returns an int representing the total block size of the f served by this tablet.
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
  private long splitCreationTime;

  private SplitRowSpec findSplitRow(Collection<FileRef> files) {

    // never split the root tablet
    // check if we already decided that we can never split
    // check to see if we're big enough to split

    long splitThreshold = tableConfiguration.getMemoryInBytes(Property.TABLE_SPLIT_THRESHOLD);
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
      keys = FileUtil.findMidPoint(getTabletServer().getFileSystem(), getTabletServer().getSystemConfiguration(), extent.getPrevEndRow(), extent.getEndRow(), FileUtil.toPathStrings(files), .25);
    } catch (IOException e) {
      log.error("Failed to find midpoint " + e.getMessage());
      return null;
    }

    // check to see if one row takes up most of the tablet, in which case we can not split
    try {

      Text lastRow;
      if (extent.getEndRow() == null) {
        Key lastKey = (Key) FileUtil.findLastKey(getTabletServer().getFileSystem(), getTabletServer().getSystemConfiguration(), files);
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

  private Map<FileRef,Pair<Key,Key>> getFirstAndLastKeys(SortedMap<FileRef,DataFileValue> allFiles) throws IOException {
    Map<FileRef,Pair<Key,Key>> result = new HashMap<FileRef,Pair<Key,Key>>();
    FileOperations fileFactory = FileOperations.getInstance();
    VolumeManager fs = getTabletServer().getFileSystem();
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
      ret = findSplitRow(getDatafileManager().getFiles()) != null;

    return ret;
  }

  // BEGIN PRIVATE METHODS RELATED TO MAJOR COMPACTION

  private boolean isCompactionEnabled() {
    return !closing && !getTabletServer().isMajorCompactionDisabled();
  }

  private CompactionStats _majorCompact(MajorCompactionReason reason) throws IOException, CompactionCanceledException {

    long t1, t2, t3;

    // acquire file info outside of tablet lock
    CompactionStrategy strategy = Property.createInstanceFromPropertyName(tableConfiguration, Property.TABLE_COMPACTION_STRATEGY, CompactionStrategy.class,
        new DefaultCompactionStrategy());
    strategy.init(Property.getCompactionStrategyOptions(tableConfiguration));

    Map<FileRef,Pair<Key,Key>> firstAndLastKeys = null;
    if (reason == MajorCompactionReason.CHOP) {
      firstAndLastKeys = getFirstAndLastKeys(getDatafileManager().getDatafileSizes());
    } else if (reason != MajorCompactionReason.USER) {
      MajorCompactionRequest request = new MajorCompactionRequest(extent, reason, getTabletServer().getFileSystem(), tableConfiguration);
      request.setFiles(getDatafileManager().getDatafileSizes());
      strategy.gatherInformation(request);
    }

    Map<FileRef,DataFileValue> filesToCompact;

    int maxFilesToCompact = tableConfiguration.getCount(Property.TSERV_MAJC_THREAD_MAXOPEN);

    CompactionStats majCStats = new CompactionStats();
    CompactionPlan plan = null;

    boolean propogateDeletes = false;

    synchronized (this) {
      // plan all that work that needs to be done in the sync block... then do the actual work
      // outside the sync block

      t1 = System.currentTimeMillis();

      majorCompactionWaitingToStart = true;

      getTabletMemory().waitForMinC();

      t2 = System.currentTimeMillis();

      majorCompactionWaitingToStart = false;
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
      List<FileRef> inputFiles = new ArrayList<FileRef>();
      if (reason == MajorCompactionReason.CHOP) {
        // enforce rules: files with keys outside our range need to be compacted
        inputFiles.addAll(findChopFiles(extent, firstAndLastKeys, allFiles.keySet()));
      } else if (reason == MajorCompactionReason.USER) {
        inputFiles.addAll(allFiles.keySet());
      } else {
        MajorCompactionRequest request = new MajorCompactionRequest(extent, reason, fs, tableConfiguration);
        request.setFiles(allFiles);
        plan = strategy.getCompactionPlan(request);
        if (plan != null)
          inputFiles.addAll(plan.inputFiles);
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

      getDatafileManager().reserveMajorCompactingFiles(filesToCompact.keySet());
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
          };

          HashMap<FileRef,DataFileValue> copy = new HashMap<FileRef,DataFileValue>(getDatafileManager().getDatafileSizes());
          if (!copy.keySet().containsAll(smallestFiles))
            throw new IllegalStateException("Cannot find data file values for " + smallestFiles);

          copy.keySet().retainAll(smallestFiles);

          log.debug("Starting MajC " + extent + " (" + reason + ") " + copy.keySet() + " --> " + compactTmpName + "  " + compactionIterators);

          // always propagate deletes, unless last batch
          boolean lastBatch = filesToCompact.isEmpty();
          Compactor compactor = new Compactor(this, copy, null, compactTmpName, lastBatch ? propogateDeletes : true, cenv,
              compactionIterators, reason.ordinal(), tableConf);

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

      }
      return majCStats;
    } finally {
      synchronized (Tablet.this) {
        getDatafileManager().clearMajorCompactingFile();
      }
    }
  }

  private AccumuloConfiguration createTableConfiguration(TableConfiguration base, CompactionPlan plan) {
    if (plan == null || plan.writeParameters == null)
      return base;
    WriteParameters p = plan.writeParameters;
    ConfigurationCopy result = new ConfigurationCopy(base);
    if (p.getHdfsBlockSize() > 0)
      result.set(Property.TABLE_FILE_BLOCK_SIZE, "" + p.getHdfsBlockSize());
    if (p.getBlockSize() > 0)
      result.set(Property.TABLE_FILE_COMPRESSED_BLOCK_SIZE, "" + p.getBlockSize());
    if (p.getIndexBlockSize() > 0)
      result.set(Property.TABLE_FILE_COMPRESSED_BLOCK_SIZE_INDEX, "" + p.getBlockSize());
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

  // END PRIVATE METHODS RELATED TO MAJOR COMPACTION

  /**
   * Performs a major compaction on the tablet. If needsSplit() returns true, the tablet is split and a reference to the new tablet is returned.
   */

  CompactionStats majorCompact(MajorCompactionReason reason, long queued) {

    CompactionStats majCStats = null;
    boolean success = false;
    long start = System.currentTimeMillis();

    // Always trace majC
    Span span = Trace.on("majorCompaction");

    try {
      timer.incrementStatusMajor();

      synchronized (this) {
        // check that compaction is still needed - defer to splitting
        majorCompactionQueued.remove(reason);

        if (closing || closed || !needsMajorCompaction(reason) || majorCompactionInProgress || needsSplit()) {
          return null;
        }

        majorCompactionInProgress = true;
      }

      try {
        majCStats = _majorCompact(reason);
        if (reason == MajorCompactionReason.CHOP) {
          MetadataTableUtil.chopped(getExtent(), this.getTabletServer().getLock());
          getTabletServer().enqueueMasterMessage(new TabletStatusMessage(TabletLoadState.CHOPPED, extent));
        }
        success = true;
      } catch (CompactionCanceledException mcce) {
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
        if (majCStats != null) {
          curr.data("read", "" + majCStats.getEntriesRead());
          curr.data("written", "" + majCStats.getEntriesWritten());
        }
      }
    } finally {
      span.stop();
      long count = 0;
      if (majCStats != null)
        count = majCStats.getEntriesRead();
      timer.updateTime(Operation.MAJOR, queued, start, count, !success);
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

  public boolean isMinorCompactionQueued() {
    return minorCompactionWaitingToStart;
  }

  public boolean isMinorCompactionRunning() {
    return minorCompactionInProgress;
  }

  public boolean isMajorCompactionQueued() {
    return majorCompactionQueued.size() > 0;
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
    Map<FileRef,FileUtil.FileInfo> firstAndLastRows = FileUtil.tryToGetFirstAndLastRows(getTabletServer().getFileSystem(), getTabletServer().getSystemConfiguration(), getDatafileManager().getFiles());

    synchronized (this) {
      // java needs tuples ...
      TreeMap<KeyExtent,SplitInfo> newTablets = new TreeMap<KeyExtent,SplitInfo>();

      long t1 = System.currentTimeMillis();

      // choose a split point
      SplitRowSpec splitPoint;
      if (sp == null)
        splitPoint = findSplitRow(getDatafileManager().getFiles());
      else {
        Text tsp = new Text(sp);
        splitPoint = new SplitRowSpec(FileUtil.estimatePercentageLTE(getTabletServer().getFileSystem(), getTabletServer().getSystemConfiguration(), extent.getPrevEndRow(), extent.getEndRow(),
            FileUtil.toPathStrings(getDatafileManager().getFiles()), tsp), tsp);
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

      String lowDirectory = TabletOperations.createTabletDirectory(getTabletServer().getFileSystem(), extent.getTableId().toString(), midRow);

      // write new tablet information to MetadataTable
      SortedMap<FileRef,DataFileValue> lowDatafileSizes = new TreeMap<FileRef,DataFileValue>();
      SortedMap<FileRef,DataFileValue> highDatafileSizes = new TreeMap<FileRef,DataFileValue>();
      List<FileRef> highDatafilesToRemove = new ArrayList<FileRef>();

      MetadataTableUtil.splitDatafiles(extent.getTableId(), midRow, splitRatio, firstAndLastRows, getDatafileManager().getDatafileSizes(), lowDatafileSizes,
          highDatafileSizes, highDatafilesToRemove);

      log.debug("Files for low split " + low + "  " + lowDatafileSizes.keySet());
      log.debug("Files for high split " + high + "  " + highDatafileSizes.keySet());

      String time = tabletTime.getMetadataValue();

      // it is possible that some of the bulk loading flags will be deleted after being read below because the bulk load
      // finishes.... therefore split could propagate load flags for a finished bulk load... there is a special iterator
      // on the metadata table to clean up this type of garbage
      Map<FileRef,Long> bulkLoadedFiles = MetadataTableUtil.getBulkFilesLoaded(SystemCredentials.get(), extent);

      MetadataTableUtil.splitTablet(high, extent.getPrevEndRow(), splitRatio, SystemCredentials.get(), getTabletServer().getLock());
      MasterMetadataUtil.addNewTablet(low, lowDirectory, getTabletServer().getTabletSession(), lowDatafileSizes, bulkLoadedFiles, SystemCredentials.get(), time,
          lastFlushID, lastCompactID, getTabletServer().getLock());
      MetadataTableUtil.finishSplit(high, highDatafileSizes, highDatafilesToRemove, SystemCredentials.get(), getTabletServer().getLock());

      log.log(TLevel.TABLET_HIST, extent + " split " + low + " " + high);

      newTablets.put(high, new SplitInfo(tabletDirectory, highDatafileSizes, time, lastFlushID, lastCompactID, lastLocation));
      newTablets.put(low, new SplitInfo(lowDirectory, lowDatafileSizes, time, lastFlushID, lastCompactID, lastLocation));

      long t2 = System.currentTimeMillis();

      log.debug(String.format("offline split time : %6.2f secs", (t2 - t1) / 1000.0));

      closeComplete = true;

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
      if (lockWait > getTabletServer().getSystemConfiguration().getTimeInMillis(Property.GENERAL_RPC_TIMEOUT)) {
        throw new IOException("Timeout waiting " + (lockWait / 1000.) + " seconds to get tablet lock");
      }

      if (writesInProgress < 0)
        throw new IllegalStateException("writesInProgress < 0 " + writesInProgress);

      writesInProgress++;
    }

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

  Set<String> beginClearingUnusedLogs() {
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
        doomed.add(logger.toString());
      }

      for (DfsLogger logger : currentLogs) {
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

  synchronized void finishClearingUnusedLogs() {
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

  public boolean beginUpdatingLogsUsed(InMemoryMap memTable, Collection<DfsLogger> more, boolean mincFinish) {

    boolean releaseLock = true;

    // do not hold tablet lock while acquiring the log lock
    logLock.lock();

    try {
      synchronized (this) {

        if (closed && closeComplete) {
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

  public void finishUpdatingLogsUsed() {
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

      if (closing || closed || majorCompactionQueued.contains(MajorCompactionReason.USER) || majorCompactionInProgress)
        return;

      if (getDatafileManager().getDatafileSizes().size() == 0) {
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
        MetadataTableUtil.updateTabletCompactID(extent, compactionId, SystemCredentials.get(), getTabletServer().getLock());
      } finally {
        synchronized (this) {
          majorCompactionInProgress = false;
          this.notifyAll();
        }
      }
    }
  }

  public TableConfiguration getTableConfiguration() {
    return tableConfiguration;
  }

  @Override
  public boolean getUseWAL() {
    return getTableConfiguration().getBoolean(Property.TABLE_WALOG_ENABLED);
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

      MetadataTableUtil.updateTabletDataFile(tid, extent, paths, tabletTime.getMetadataValue(persistedTime), SystemCredentials.get(), getTabletServer().getLock());
    }

  }

  public void updateTabletDataFile(long maxCommittedTime, FileRef newDatafile, FileRef absMergeFile, DataFileValue dfv, Set<String> unusedWalLogs, Set<FileRef> filesInUseByScans, long flushId) {
    synchronized (timeLock) {
      if (maxCommittedTime > persistedTime)
        persistedTime = maxCommittedTime;

      String time = tabletTime.getMetadataValue(persistedTime);
      MasterMetadataUtil.updateTabletDataFile(extent, newDatafile, absMergeFile, dfv, time, SystemCredentials.get(), filesInUseByScans,
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

  public void setLastCompactionID(Long compactionId) {
    if (compactionId != null)
      this.lastCompactID = compactionId;
  }

  public void removeMajorCompactionQueuedReason(MajorCompactionReason reason) {
    majorCompactionQueued.remove(reason);
    
  }

  public void minorCompactionWaitingToStart() {
    minorCompactionWaitingToStart = true;
  }

  public void minorCompactionStarted() {
    minorCompactionWaitingToStart = false;
    minorCompactionInProgress = true;
  }

  public void minorCompactionComplete() {
    minorCompactionInProgress = false;
  }

  public boolean isMajorCompactionRunning() {
    return majorCompactionInProgress;
  }

  public TabletStats getTabletStats() {
    return timer.getTabletStats();
  }

  public AtomicLong getScannedCounter() {
    return scannedCount;
  }

  
}

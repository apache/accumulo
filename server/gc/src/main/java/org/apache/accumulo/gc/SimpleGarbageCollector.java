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
package org.apache.accumulo.gc;

import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.DIR;
import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.FILES;
import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.SCANS;
import static org.apache.accumulo.fate.util.UtilWaitThread.sleepUninterruptibly;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.IsolatedScanner;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.clientImpl.Tables;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.gc.thrift.GCMonitorService.Iface;
import org.apache.accumulo.core.gc.thrift.GCMonitorService.Processor;
import org.apache.accumulo.core.gc.thrift.GCStatus;
import org.apache.accumulo.core.gc.thrift.GcCycleStats;
import org.apache.accumulo.core.master.state.tables.TableState;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.metadata.RootTable;
import org.apache.accumulo.core.metadata.TabletFileUtil;
import org.apache.accumulo.core.metadata.schema.Ample;
import org.apache.accumulo.core.metadata.schema.Ample.DataLevel;
import org.apache.accumulo.core.metadata.schema.MetadataSchema;
import org.apache.accumulo.core.metadata.schema.TabletMetadata;
import org.apache.accumulo.core.metadata.schema.TabletsMetadata;
import org.apache.accumulo.core.replication.ReplicationSchema.StatusSection;
import org.apache.accumulo.core.replication.ReplicationTable;
import org.apache.accumulo.core.replication.ReplicationTableOfflineException;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.securityImpl.thrift.TCredentials;
import org.apache.accumulo.core.trace.TraceUtil;
import org.apache.accumulo.core.trace.thrift.TInfo;
import org.apache.accumulo.core.util.HostAndPort;
import org.apache.accumulo.core.util.NamingThreadFactory;
import org.apache.accumulo.core.util.Pair;
import org.apache.accumulo.core.util.ServerServices;
import org.apache.accumulo.core.util.ServerServices.Service;
import org.apache.accumulo.core.volume.Volume;
import org.apache.accumulo.fate.zookeeper.ZooLock;
import org.apache.accumulo.fate.zookeeper.ZooLock.LockLossReason;
import org.apache.accumulo.fate.zookeeper.ZooLock.LockWatcher;
import org.apache.accumulo.gc.metrics.GcCycleMetrics;
import org.apache.accumulo.gc.metrics.GcMetricsFactory;
import org.apache.accumulo.gc.replication.CloseWriteAheadLogReferences;
import org.apache.accumulo.server.AbstractServer;
import org.apache.accumulo.server.ServerConstants;
import org.apache.accumulo.server.ServerOpts;
import org.apache.accumulo.server.fs.VolumeManager;
import org.apache.accumulo.server.fs.VolumeManager.FileType;
import org.apache.accumulo.server.fs.VolumeUtil;
import org.apache.accumulo.server.gc.GcVolumeUtil;
import org.apache.accumulo.server.master.LiveTServerSet;
import org.apache.accumulo.server.replication.proto.Replication.Status;
import org.apache.accumulo.server.rpc.ServerAddress;
import org.apache.accumulo.server.rpc.TCredentialsUpdatingWrapper;
import org.apache.accumulo.server.rpc.TServerUtils;
import org.apache.accumulo.server.rpc.ThriftServerType;
import org.apache.accumulo.server.util.Halt;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.htrace.Trace;
import org.apache.htrace.TraceScope;
import org.apache.htrace.impl.ProbabilitySampler;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Iterators;
import com.google.common.collect.Maps;
import com.google.protobuf.InvalidProtocolBufferException;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

// Could/Should implement HighlyAvaialbleService but the Thrift server is already started before
// the ZK lock is acquired. The server is only for metrics, there are no concerns about clients
// using the service before the lock is acquired.
public class SimpleGarbageCollector extends AbstractServer implements Iface {
  /**
   * A fraction representing how much of the JVM's available memory should be used for gathering
   * candidates.
   */
  static final float CANDIDATE_MEMORY_PERCENTAGE = 0.50f;

  private static final Logger log = LoggerFactory.getLogger(SimpleGarbageCollector.class);

  private ZooLock lock;

  private GCStatus status =
      new GCStatus(new GcCycleStats(), new GcCycleStats(), new GcCycleStats(), new GcCycleStats());

  private GcCycleMetrics gcCycleMetrics = new GcCycleMetrics();

  public static void main(String[] args) throws Exception {
    try (SimpleGarbageCollector gc = new SimpleGarbageCollector(new ServerOpts(), args)) {
      gc.runServer();
    }
  }

  SimpleGarbageCollector(ServerOpts opts, String[] args) {
    super("gc", opts, args);

    final AccumuloConfiguration conf = getConfiguration();

    boolean gcMetricsRegistered = new GcMetricsFactory(conf).register(this);

    if (gcMetricsRegistered) {
      log.info("gc metrics modules registered with metrics system");
    } else {
      log.info("Failed to register gc metrics module");
    }

    final long gcDelay = conf.getTimeInMillis(Property.GC_CYCLE_DELAY);
    final String useFullCompaction = conf.get(Property.GC_USE_FULL_COMPACTION);

    log.info("start delay: {} milliseconds", getStartDelay());
    log.info("time delay: {} milliseconds", gcDelay);
    log.info("safemode: {}", inSafeMode());
    log.info("memory threshold: {} of {} bytes", CANDIDATE_MEMORY_PERCENTAGE,
        Runtime.getRuntime().maxMemory());
    log.info("delete threads: {}", getNumDeleteThreads());
    log.info("gc post metadata action: {}", useFullCompaction);
  }

  /**
   * Gets the delay before the first collection.
   *
   * @return start delay, in milliseconds
   */
  long getStartDelay() {
    return getConfiguration().getTimeInMillis(Property.GC_CYCLE_START);
  }

  /**
   * Checks if the volume manager should move files to the trash rather than delete them.
   *
   * @return true if trash is used
   */
  boolean isUsingTrash() {
    return !getConfiguration().getBoolean(Property.GC_TRASH_IGNORE);
  }

  /**
   * Gets the number of threads used for deleting files.
   *
   * @return number of delete threads
   */
  int getNumDeleteThreads() {
    return getConfiguration().getCount(Property.GC_DELETE_THREADS);
  }

  /**
   * Checks if safemode is set - files will not be deleted.
   *
   * @return number of delete threads
   */
  boolean inSafeMode() {
    return getConfiguration().getBoolean(Property.GC_SAFEMODE);
  }

  private class GCEnv implements GarbageCollectionEnvironment {

    private DataLevel level;

    GCEnv(Ample.DataLevel level) {
      this.level = level;
    }

    @Override
    public boolean getCandidates(String continuePoint, List<String> result)
        throws TableNotFoundException {

      Iterator<String> candidates = getContext().getAmple().getGcCandidates(level, continuePoint);

      result.clear();

      while (candidates.hasNext()) {
        String cand = candidates.next();

        result.add(cand);
        if (almostOutOfMemory(Runtime.getRuntime())) {
          log.info("List of delete candidates has exceeded the memory"
              + " threshold. Attempting to delete what has been gathered so far.");
          return true;
        }
      }

      return false;
    }

    @Override
    public Iterator<String> getBlipIterator() throws TableNotFoundException {

      if (level == DataLevel.ROOT) {
        return Collections.<String>emptySet().iterator();
      }

      @SuppressWarnings("resource")
      IsolatedScanner scanner =
          new IsolatedScanner(getContext().createScanner(level.metaTable(), Authorizations.EMPTY));

      scanner.setRange(MetadataSchema.BlipSection.getRange());

      return Iterators.transform(scanner.iterator(), entry -> entry.getKey().getRow().toString()
          .substring(MetadataSchema.BlipSection.getRowPrefix().length()));
    }

    @Override
    public Stream<Reference> getReferences() {

      Stream<TabletMetadata> tabletStream;

      if (level == DataLevel.ROOT) {
        tabletStream =
            Stream.of(getContext().getAmple().readTablet(RootTable.EXTENT, DIR, FILES, SCANS));
      } else {
        tabletStream = TabletsMetadata.builder().scanTable(level.metaTable()).checkConsistency()
            .fetch(DIR, FILES, SCANS).build(getContext()).stream();
      }

      Stream<Reference> refStream = tabletStream.flatMap(tm -> {
        Stream<Reference> refs = Stream.concat(tm.getFiles().stream(), tm.getScans().stream())
            .map(f -> new Reference(tm.getTableId(), f.getMetaUpdateDelete(), false));
        if (tm.getDirName() != null) {
          refs =
              Stream.concat(refs, Stream.of(new Reference(tm.getTableId(), tm.getDirName(), true)));
        }
        return refs;
      });

      return refStream;
    }

    @Override
    public Set<TableId> getTableIDs() {
      return Tables.getIdToNameMap(getContext()).keySet();
    }

    @Override
    public void delete(SortedMap<String,String> confirmedDeletes) throws TableNotFoundException {
      final VolumeManager fs = getContext().getVolumeManager();
      var metadataLocation = level == DataLevel.ROOT
          ? getContext().getZooKeeperRoot() + " for " + RootTable.NAME : level.metaTable();

      if (inSafeMode()) {
        System.out.println("SAFEMODE: There are " + confirmedDeletes.size()
            + " data file candidates marked for deletion in " + metadataLocation + ".\n"
            + "          Examine the log files to identify them.\n");
        log.info("SAFEMODE: Listing all data file candidates for deletion");
        for (String s : confirmedDeletes.values()) {
          log.info("SAFEMODE: {}", s);
        }
        log.info("SAFEMODE: End candidates for deletion");
        return;
      }

      List<String> processedDeletes = Collections.synchronizedList(new ArrayList<>());

      minimizeDeletes(confirmedDeletes, processedDeletes, fs);

      ExecutorService deleteThreadPool =
          Executors.newFixedThreadPool(getNumDeleteThreads(), new NamingThreadFactory("deleting"));

      final List<Pair<Path,Path>> replacements =
          ServerConstants.getVolumeReplacements(getConfiguration(), getContext().getHadoopConf());

      for (final String delete : confirmedDeletes.values()) {

        Runnable deleteTask = () -> {
          boolean removeFlag = false;

          try {
            Path fullPath;
            Path switchedDelete = VolumeUtil.switchVolume(delete, FileType.TABLE, replacements);
            if (switchedDelete != null) {
              // actually replacing the volumes in the metadata table would be tricky because the
              // entries would be different rows. So it could not be
              // atomically in one mutation and extreme care would need to be taken that delete
              // entry was not lost. Instead of doing that, just deal with
              // volume switching when something needs to be deleted. Since the rest of the code
              // uses suffixes to compare delete entries, there is no danger
              // of deleting something that should not be deleted. Must not change value of delete
              // variable because thats whats stored in metadata table.
              log.debug("Volume replaced {} -> {}", delete, switchedDelete);
              fullPath = TabletFileUtil.validate(switchedDelete);
            } else {
              fullPath = new Path(TabletFileUtil.validate(delete));
            }

            for (Path pathToDel : GcVolumeUtil.expandAllVolumesUri(fs, fullPath)) {
              log.debug("Deleting {}", pathToDel);

              if (moveToTrash(pathToDel) || fs.deleteRecursively(pathToDel)) {
                // delete succeeded, still want to delete
                removeFlag = true;
                synchronized (SimpleGarbageCollector.this) {
                  ++status.current.deleted;
                }
              } else if (fs.exists(pathToDel)) {
                // leave the entry in the metadata; we'll try again later
                removeFlag = false;
                synchronized (SimpleGarbageCollector.this) {
                  ++status.current.errors;
                }
                log.warn("File exists, but was not deleted for an unknown reason: {}", pathToDel);
                break;
              } else {
                // this failure, we still want to remove the metadata entry
                removeFlag = true;
                synchronized (SimpleGarbageCollector.this) {
                  ++status.current.errors;
                }
                String[] parts = pathToDel.toString().split(Constants.ZTABLES)[1].split("/");
                if (parts.length > 2) {
                  TableId tableId = TableId.of(parts[1]);
                  String tabletDir = parts[2];
                  getContext().getTableManager().updateTableStateCache(tableId);
                  TableState tableState = getContext().getTableManager().getTableState(tableId);
                  if (tableState != null && tableState != TableState.DELETING) {
                    // clone directories don't always exist
                    if (!tabletDir.startsWith(Constants.CLONE_PREFIX)) {
                      log.debug("File doesn't exist: {}", pathToDel);
                    }
                  }
                } else {
                  log.warn("Very strange path name: {}", delete);
                }
              }
            }

            // proceed to clearing out the flags for successful deletes and
            // non-existent files
            if (removeFlag) {
              processedDeletes.add(delete);
            }
          } catch (Exception e) {
            log.error("{}", e.getMessage(), e);
          }

        };

        deleteThreadPool.execute(deleteTask);
      }

      deleteThreadPool.shutdown();

      try {
        while (!deleteThreadPool.awaitTermination(1000, TimeUnit.MILLISECONDS)) {}
      } catch (InterruptedException e1) {
        log.error("{}", e1.getMessage(), e1);
      }

      getContext().getAmple().deleteGcCandidates(level, processedDeletes);
    }

    @Override
    public void deleteTableDirIfEmpty(TableId tableID) throws IOException {
      final VolumeManager fs = getContext().getVolumeManager();
      // if dir exist and is empty, then empty list is returned...
      // hadoop 2.0 will throw an exception if the file does not exist
      for (String dir : ServerConstants.getTablesDirs(getContext())) {
        FileStatus[] tabletDirs = null;
        try {
          tabletDirs = fs.listStatus(new Path(dir + "/" + tableID));
        } catch (FileNotFoundException ex) {
          continue;
        }

        if (tabletDirs.length == 0) {
          Path p = new Path(dir + "/" + tableID);
          log.debug("Removing table dir {}", p);
          if (!moveToTrash(p)) {
            fs.delete(p);
          }
        }
      }
    }

    @Override
    public void incrementCandidatesStat(long i) {
      status.current.candidates += i;
    }

    @Override
    public void incrementInUseStat(long i) {
      status.current.inUse += i;
    }

    @Override
    public Iterator<Entry<String,Status>> getReplicationNeededIterator() {
      AccumuloClient client = getContext();
      try {
        Scanner s = ReplicationTable.getScanner(client);
        StatusSection.limit(s);
        return Iterators.transform(s.iterator(), input -> {
          String file = input.getKey().getRow().toString();
          Status stat;
          try {
            stat = Status.parseFrom(input.getValue().get());
          } catch (InvalidProtocolBufferException e) {
            log.warn("Could not deserialize protobuf for: {}", input.getKey());
            stat = null;
          }
          return Maps.immutableEntry(file, stat);
        });
      } catch (ReplicationTableOfflineException e) {
        // No elements that we need to preclude
        return Collections.emptyIterator();
      }
    }
  }

  @Override
  @SuppressFBWarnings(value = "DM_EXIT", justification = "main class can call System.exit")
  public void run() {
    final VolumeManager fs = getContext().getVolumeManager();

    // Sleep for an initial period, giving the master time to start up and
    // old data files to be unused
    log.info("Trying to acquire ZooKeeper lock for garbage collector");

    try {
      getZooLock(startStatsService());
    } catch (Exception ex) {
      log.error("{}", ex.getMessage(), ex);
      System.exit(1);
    }

    try {
      long delay = getStartDelay();
      log.debug("Sleeping for {} milliseconds before beginning garbage collection cycles", delay);
      Thread.sleep(delay);
    } catch (InterruptedException e) {
      log.warn("{}", e.getMessage(), e);
      return;
    }

    ProbabilitySampler sampler =
        TraceUtil.probabilitySampler(getConfiguration().getFraction(Property.GC_TRACE_PERCENT));

    // This is created outside of the run loop and passed to the walogCollector so that
    // only a single timed task is created (internal to LiveTServerSet using SimpleTimer.
    final LiveTServerSet liveTServerSet =
        new LiveTServerSet(getContext(), (current, deleted, added) -> {
          log.debug("Number of current servers {}, tservers added {}, removed {}",
              current == null ? -1 : current.size(), added, deleted);

          if (log.isTraceEnabled()) {
            log.trace("Current servers: {}\nAdded: {}\n Removed: {}", current, added, deleted);
          }
        });

    while (true) {
      try (TraceScope gcOuterSpan = Trace.startSpan("gc", sampler)) {
        try (TraceScope gcSpan = Trace.startSpan("loop")) {
          final long tStart = System.nanoTime();
          try {
            System.gc(); // make room

            status.current.started = System.currentTimeMillis();

            new GarbageCollectionAlgorithm().collect(new GCEnv(DataLevel.ROOT));
            new GarbageCollectionAlgorithm().collect(new GCEnv(DataLevel.METADATA));
            new GarbageCollectionAlgorithm().collect(new GCEnv(DataLevel.USER));

            log.info("Number of data file candidates for deletion: {}", status.current.candidates);
            log.info("Number of data file candidates still in use: {}", status.current.inUse);
            log.info("Number of successfully deleted data files: {}", status.current.deleted);
            log.info("Number of data files delete failures: {}", status.current.errors);

            status.current.finished = System.currentTimeMillis();
            status.last = status.current;
            gcCycleMetrics.setLastCollect(status.current);
            status.current = new GcCycleStats();

          } catch (Exception e) {
            log.error("{}", e.getMessage(), e);
          }

          final long tStop = System.nanoTime();
          log.info(String.format("Collect cycle took %.2f seconds",
              (TimeUnit.NANOSECONDS.toMillis(tStop - tStart) / 1000.0)));

          /*
           * We want to prune references to fully-replicated WALs from the replication table which
           * are no longer referenced in the metadata table before running
           * GarbageCollectWriteAheadLogs to ensure we delete as many files as possible.
           */
          try (TraceScope replSpan = Trace.startSpan("replicationClose")) {
            CloseWriteAheadLogReferences closeWals = new CloseWriteAheadLogReferences(getContext());
            closeWals.run();
          } catch (Exception e) {
            log.error("Error trying to close write-ahead logs for replication table", e);
          }

          // Clean up any unused write-ahead logs
          try (TraceScope waLogs = Trace.startSpan("walogs")) {
            GarbageCollectWriteAheadLogs walogCollector =
                new GarbageCollectWriteAheadLogs(getContext(), fs, liveTServerSet, isUsingTrash());
            log.info("Beginning garbage collection of write-ahead logs");
            walogCollector.collect(status);
            gcCycleMetrics.setLastWalCollect(status.lastLog);
          } catch (Exception e) {
            log.error("{}", e.getMessage(), e);
          }
        }

        // we just made a lot of metadata changes: flush them out
        try {
          AccumuloClient accumuloClient = getContext();

          final long actionStart = System.nanoTime();

          String action = getConfiguration().get(Property.GC_USE_FULL_COMPACTION);
          log.debug("gc post action {} started", action);

          switch (action) {
            case "compact":
              accumuloClient.tableOperations().compact(MetadataTable.NAME, null, null, true, true);
              accumuloClient.tableOperations().compact(RootTable.NAME, null, null, true, true);
              break;
            case "flush":
              accumuloClient.tableOperations().flush(MetadataTable.NAME, null, null, true);
              accumuloClient.tableOperations().flush(RootTable.NAME, null, null, true);
              break;
            default:
              log.trace("\'none - no action\' or invalid value provided: {}", action);
          }

          final long actionComplete = System.nanoTime();

          gcCycleMetrics.setPostOpDurationNanos(actionComplete - actionStart);

          log.info("gc post action {} completed in {} seconds", action, String.format("%.2f",
              (TimeUnit.NANOSECONDS.toMillis(actionComplete - actionStart) / 1000.0)));

        } catch (Exception e) {
          log.warn("{}", e.getMessage(), e);
        }
      }
      try {
        gcCycleMetrics.incrementRunCycleCount();
        long gcDelay = getConfiguration().getTimeInMillis(Property.GC_CYCLE_DELAY);
        log.debug("Sleeping for {} milliseconds", gcDelay);
        Thread.sleep(gcDelay);
      } catch (InterruptedException e) {
        log.warn("{}", e.getMessage(), e);
        return;
      }
    }
  }

  /**
   * Moves a file to trash. If this garbage collector is not using trash, this method returns false
   * and leaves the file alone. If the file is missing, this method returns false as opposed to
   * throwing an exception.
   *
   * @return true if the file was moved to trash
   * @throws IOException
   *           if the volume manager encountered a problem
   */
  boolean moveToTrash(Path path) throws IOException {
    final VolumeManager fs = getContext().getVolumeManager();
    if (!isUsingTrash()) {
      return false;
    }
    try {
      return fs.moveToTrash(path);
    } catch (FileNotFoundException ex) {
      return false;
    }
  }

  private void getZooLock(HostAndPort addr) throws KeeperException, InterruptedException {
    String path = getContext().getZooKeeperRoot() + Constants.ZGC_LOCK;

    LockWatcher lockWatcher = new LockWatcher() {
      @Override
      public void lostLock(LockLossReason reason) {
        Halt.halt("GC lock in zookeeper lost (reason = " + reason + "), exiting!", 1);
      }

      @Override
      public void unableToMonitorLockNode(final Throwable e) {
        // ACCUMULO-3651 Level changed to error and FATAL added to message for slf4j compatibility
        Halt.halt(-1, () -> log.error("FATAL: No longer able to monitor lock node ", e));

      }
    };

    while (true) {
      lock = new ZooLock(getContext().getZooReaderWriter(), path);
      if (lock.tryLock(lockWatcher,
          new ServerServices(addr.toString(), Service.GC_CLIENT).toString().getBytes())) {
        log.debug("Got GC ZooKeeper lock");
        return;
      }
      log.debug("Failed to get GC ZooKeeper lock, will retry");
      sleepUninterruptibly(1, TimeUnit.SECONDS);
    }
  }

  private HostAndPort startStatsService() {
    Iface rpcProxy = TraceUtil.wrapService(this);
    final Processor<Iface> processor;
    if (getContext().getThriftServerType() == ThriftServerType.SASL) {
      Iface tcProxy = TCredentialsUpdatingWrapper.service(rpcProxy, getClass(), getConfiguration());
      processor = new Processor<>(tcProxy);
    } else {
      processor = new Processor<>(rpcProxy);
    }
    int[] port = getConfiguration().getPort(Property.GC_PORT);
    HostAndPort[] addresses = TServerUtils.getHostAndPorts(getHostname(), port);
    long maxMessageSize = getConfiguration().getAsBytes(Property.GENERAL_MAX_MESSAGE_SIZE);
    try {
      ServerAddress server = TServerUtils.startTServer(getMetricsSystem(), getConfiguration(),
          getContext().getThriftServerType(), processor, this.getClass().getSimpleName(),
          "GC Monitor Service", 2,
          getConfiguration().getCount(Property.GENERAL_SIMPLETIMER_THREADPOOL_SIZE), 1000,
          maxMessageSize, getContext().getServerSslParams(), getContext().getSaslParams(), 0,
          addresses);
      log.debug("Starting garbage collector listening on " + server.address);
      return server.address;
    } catch (Exception ex) {
      // ACCUMULO-3651 Level changed to error and FATAL added to message for slf4j compatibility
      log.error("FATAL:", ex);
      throw new RuntimeException(ex);
    }
  }

  /**
   * Checks if the system is almost out of memory.
   *
   * @param runtime
   *          Java runtime
   * @return true if system is almost out of memory
   * @see #CANDIDATE_MEMORY_PERCENTAGE
   */
  static boolean almostOutOfMemory(Runtime runtime) {
    return runtime.totalMemory() - runtime.freeMemory()
        > CANDIDATE_MEMORY_PERCENTAGE * runtime.maxMemory();
  }

  /**
   * Checks if the given string is a directory.
   *
   * @param delete
   *          possible directory
   * @return true if string is a directory
   */
  static boolean isDir(String delete) {
    if (delete == null) {
      return false;
    }

    int slashCount = 0;
    for (int i = 0; i < delete.length(); i++) {
      if (delete.charAt(i) == '/') {
        slashCount++;
      }
    }
    return slashCount == 1;
  }

  @VisibleForTesting
  static void minimizeDeletes(SortedMap<String,String> confirmedDeletes,
      List<String> processedDeletes, VolumeManager fs) {
    Set<Path> seenVolumes = new HashSet<>();
    Collection<Volume> volumes = fs.getVolumes();

    // when deleting a dir and all files in that dir, only need to delete the dir
    // the dir will sort right before the files... so remove the files in this case
    // to minimize namenode ops
    Iterator<Entry<String,String>> cdIter = confirmedDeletes.entrySet().iterator();

    String lastDirRel = null;
    Path lastDirAbs = null;
    while (cdIter.hasNext()) {
      Entry<String,String> entry = cdIter.next();
      String relPath = entry.getKey();
      Path absPath = new Path(entry.getValue());

      if (isDir(relPath)) {
        lastDirRel = relPath;
        lastDirAbs = absPath;
      } else if (lastDirRel != null) {
        if (relPath.startsWith(lastDirRel)) {
          Path vol = FileType.TABLE.getVolume(absPath);

          boolean sameVol = false;

          if (GcVolumeUtil.isAllVolumesUri(lastDirAbs)) {
            if (seenVolumes.contains(vol)) {
              sameVol = true;
            } else {
              for (Volume cvol : volumes) {
                if (cvol.isValidPath(vol)) {
                  seenVolumes.add(vol);
                  sameVol = true;
                }
              }
            }
          } else {
            sameVol = FileType.TABLE.getVolume(lastDirAbs).equals(vol);
          }

          if (sameVol) {
            log.info("Ignoring {} because {} exist", entry.getValue(), lastDirAbs);
            processedDeletes.add(entry.getValue());
            cdIter.remove();
          }
        } else {
          lastDirRel = null;
          lastDirAbs = null;
        }
      }
    }
  }

  @Override
  public GCStatus getStatus(TInfo info, TCredentials credentials) {
    return status;
  }

  public GcCycleMetrics getGcCycleMetrics() {
    return gcCycleMetrics;
  }
}

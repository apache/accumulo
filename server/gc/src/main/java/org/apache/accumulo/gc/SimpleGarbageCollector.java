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
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.SortedMap;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.IsolatedScanner;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.gc.thrift.GCMonitorService.Iface;
import org.apache.accumulo.core.gc.thrift.GCStatus;
import org.apache.accumulo.core.gc.thrift.GcCycleStats;
import org.apache.accumulo.core.manager.state.tables.TableState;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.metadata.RootTable;
import org.apache.accumulo.core.metadata.TabletFileUtil;
import org.apache.accumulo.core.metadata.schema.Ample;
import org.apache.accumulo.core.metadata.schema.Ample.DataLevel;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.BlipSection;
import org.apache.accumulo.core.metadata.schema.TabletMetadata;
import org.apache.accumulo.core.metadata.schema.TabletsMetadata;
import org.apache.accumulo.core.metrics.MetricsUtil;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.securityImpl.thrift.TCredentials;
import org.apache.accumulo.core.trace.TraceUtil;
import org.apache.accumulo.core.trace.thrift.TInfo;
import org.apache.accumulo.core.util.Halt;
import org.apache.accumulo.core.util.HostAndPort;
import org.apache.accumulo.core.util.Pair;
import org.apache.accumulo.core.util.ServerServices;
import org.apache.accumulo.core.util.ServerServices.Service;
import org.apache.accumulo.core.util.threads.ThreadPools;
import org.apache.accumulo.core.volume.Volume;
import org.apache.accumulo.fate.zookeeper.ServiceLock;
import org.apache.accumulo.fate.zookeeper.ServiceLock.LockLossReason;
import org.apache.accumulo.fate.zookeeper.ServiceLock.LockWatcher;
import org.apache.accumulo.gc.metrics.GcCycleMetrics;
import org.apache.accumulo.gc.metrics.GcMetrics;
import org.apache.accumulo.server.AbstractServer;
import org.apache.accumulo.server.ServerOpts;
import org.apache.accumulo.server.fs.VolumeManager;
import org.apache.accumulo.server.fs.VolumeManager.FileType;
import org.apache.accumulo.server.fs.VolumeUtil;
import org.apache.accumulo.server.gc.GcVolumeUtil;
import org.apache.accumulo.server.manager.LiveTServerSet;
import org.apache.accumulo.server.replication.proto.Replication.Status;
import org.apache.accumulo.server.rpc.ServerAddress;
import org.apache.accumulo.server.rpc.TServerUtils;
import org.apache.accumulo.server.rpc.ThriftProcessorTypes;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.thrift.TProcessor;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Iterators;
import com.google.common.collect.Maps;
import com.google.protobuf.InvalidProtocolBufferException;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.context.Scope;

// Could/Should implement HighlyAvailableService but the Thrift server is already started before
// the ZK lock is acquired. The server is only for metrics, there are no concerns about clients
// using the service before the lock is acquired.
public class SimpleGarbageCollector extends AbstractServer implements Iface {

  private static final Logger log = LoggerFactory.getLogger(SimpleGarbageCollector.class);

  private final GCStatus status =
      new GCStatus(new GcCycleStats(), new GcCycleStats(), new GcCycleStats(), new GcCycleStats());

  private final GcCycleMetrics gcCycleMetrics = new GcCycleMetrics();

  public SimpleGarbageCollector(ServerOpts opts, String[] args) {
    super("gc", opts, args);

    final AccumuloConfiguration conf = getConfiguration();

    final long gcDelay = conf.getTimeInMillis(Property.GC_CYCLE_DELAY);
    final String useFullCompaction = conf.get(Property.GC_USE_FULL_COMPACTION);

    log.info("start delay: {} milliseconds", getStartDelay());
    log.info("time delay: {} milliseconds", gcDelay);
    log.info("safemode: {}", inSafeMode());
    log.info("candidate batch size: {} bytes", getCandidateBatchSize());
    log.info("delete threads: {}", getNumDeleteThreads());
    log.info("gc post metadata action: {}", useFullCompaction);
  }

  public static void main(String[] args) throws Exception {
    try (SimpleGarbageCollector gc = new SimpleGarbageCollector(new ServerOpts(), args)) {
      gc.runServer();
    }
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
   * Gets the batch size for garbage collecting.
   *
   * @return candidate batch size.
   */
  long getCandidateBatchSize() {
    return getConfiguration().getAsBytes(Property.GC_CANDIDATE_BATCH_SIZE);
  }

  /**
   * Checks if safemode is set - files will not be deleted.
   *
   * @return number of delete threads
   */
  boolean inSafeMode() {
    return getConfiguration().getBoolean(Property.GC_SAFEMODE);
  }

  public class GCEnv implements GarbageCollectionEnvironment {

    private final DataLevel level;

    public GCEnv(Ample.DataLevel level) {
      this.level = level;
    }

    @Override
    public Iterator<String> getCandidates() {
      return getContext().getAmple().getGcCandidates(level);
    }

    @Override
    public List<String> readCandidatesThatFitInMemory(Iterator<String> candidates) {
      long candidateLength = 0;
      // Converting the bytes to approximate number of characters for batch size.
      long candidateBatchSize = getCandidateBatchSize() / 2;

      List<String> candidatesBatch = new ArrayList<>();

      while (candidates.hasNext()) {
        String candidate = candidates.next();
        candidateLength += candidate.length();
        candidatesBatch.add(candidate);
        if (candidateLength > candidateBatchSize) {
          log.info("Candidate batch of size {} has exceeded the threshold. Attempting to delete "
              + "what has been gathered so far.", candidateLength);
          return candidatesBatch;
        }
      }
      return candidatesBatch;
    }

    @Override
    public Stream<String> getBlipPaths() throws TableNotFoundException {

      if (level == DataLevel.ROOT) {
        return Stream.empty();
      }

      int blipPrefixLen = BlipSection.getRowPrefix().length();
      var scanner =
          new IsolatedScanner(getContext().createScanner(level.metaTable(), Authorizations.EMPTY));
      scanner.setRange(BlipSection.getRange());
      return scanner.stream()
          .map(entry -> entry.getKey().getRow().toString().substring(blipPrefixLen))
          .onClose(scanner::close);
    }

    @Override
    public Stream<Reference> getReferences() {

      Stream<TabletMetadata> tabletStream;

      if (level == DataLevel.ROOT) {
        tabletStream =
            Stream.of(getContext().getAmple().readTablet(RootTable.EXTENT, DIR, FILES, SCANS));
      } else {
        tabletStream = TabletsMetadata.builder(getContext()).scanTable(level.metaTable())
            .checkConsistency().fetch(DIR, FILES, SCANS).build().stream();
      }

      var tabletReferences = tabletStream.flatMap(tm -> {
        Stream<Reference> refs = Stream.concat(tm.getFiles().stream(), tm.getScans().stream())
            .map(f -> new Reference(tm.getTableId(), f.getMetaUpdateDelete(), false));
        if (tm.getDirName() != null) {
          refs =
              Stream.concat(refs, Stream.of(new Reference(tm.getTableId(), tm.getDirName(), true)));
        }
        return refs;
      });

      var scanServerRefs = getContext().getAmple().getScanServerFileReferences()
          .map(sfr -> new Reference(sfr.getTableId(), sfr.getPathStr(), false));

      return Stream.concat(tabletReferences, scanServerRefs);
    }

    @Override
    public Set<TableId> getTableIDs() {
      return getContext().getTableIdToNameMap().keySet();
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

      ExecutorService deleteThreadPool = ThreadPools.getServerThreadPools()
          .createExecutorService(getConfiguration(), Property.GC_DELETE_THREADS, false);

      final List<Pair<Path,Path>> replacements = getContext().getVolumeReplacements();

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
              // variable because that's what's stored in metadata table.
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
        while (!deleteThreadPool.awaitTermination(1000, TimeUnit.MILLISECONDS)) { // empty
        }
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
      for (String dir : getContext().getTablesDirs()) {
        FileStatus[] tabletDirs;
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
    @Deprecated
    public Iterator<Entry<String,Status>> getReplicationNeededIterator() {
      AccumuloClient client = getContext();
      try {
        Scanner s = org.apache.accumulo.core.replication.ReplicationTable.getScanner(client);
        org.apache.accumulo.core.replication.ReplicationSchema.StatusSection.limit(s);
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
      } catch (org.apache.accumulo.core.replication.ReplicationTableOfflineException e) {
        // No elements that we need to preclude
        return Collections.emptyIterator();
      }
    }
  }

  @Override
  @SuppressFBWarnings(value = "DM_EXIT", justification = "main class can call System.exit")
  public void run() {
    final VolumeManager fs = getContext().getVolumeManager();

    // Sleep for an initial period, giving the manager time to start up and
    // old data files to be unused
    log.info("Trying to acquire ZooKeeper lock for garbage collector");

    HostAndPort address = startStatsService();

    try {
      getZooLock(address);
    } catch (Exception ex) {
      log.error("{}", ex.getMessage(), ex);
      System.exit(1);
    }

    try {
      MetricsUtil.initializeMetrics(getContext().getConfiguration(), this.applicationName, address);
      MetricsUtil.initializeProducers(new GcMetrics(this));
    } catch (Exception e1) {
      log.error("Error initializing metrics, metrics will not be emitted.", e1);
    }

    try {
      long delay = getStartDelay();
      log.debug("Sleeping for {} milliseconds before beginning garbage collection cycles", delay);
      Thread.sleep(delay);
    } catch (InterruptedException e) {
      log.warn("{}", e.getMessage(), e);
      return;
    }

    // This is created outside of the run loop and passed to the walogCollector so that
    // only a single timed task is created (internal to LiveTServerSet) using SimpleTimer.
    final LiveTServerSet liveTServerSet =
        new LiveTServerSet(getContext(), (current, deleted, added) -> {
          log.debug("Number of current servers {}, tservers added {}, removed {}",
              current == null ? -1 : current.size(), added, deleted);

          if (log.isTraceEnabled()) {
            log.trace("Current servers: {}\nAdded: {}\n Removed: {}", current, added, deleted);
          }
        });

    while (true) {
      Span outerSpan = TraceUtil.startSpan(this.getClass(), "gc");
      try (Scope outerScope = outerSpan.makeCurrent()) {
        Span innerSpan = TraceUtil.startSpan(this.getClass(), "loop");
        try (Scope innerScope = innerSpan.makeCurrent()) {
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
            TraceUtil.setException(innerSpan, e, false);
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
          Span replSpan = TraceUtil.startSpan(this.getClass(), "replicationClose");
          try (Scope replScope = replSpan.makeCurrent()) {
            @SuppressWarnings("deprecation")
            Runnable closeWals =
                new org.apache.accumulo.gc.replication.CloseWriteAheadLogReferences(getContext());
            closeWals.run();
          } catch (Exception e) {
            TraceUtil.setException(replSpan, e, false);
            log.error("Error trying to close write-ahead logs for replication table", e);
          } finally {
            replSpan.end();
          }

          // Clean up any unused write-ahead logs
          Span walSpan = TraceUtil.startSpan(this.getClass(), "walogs");
          try (Scope walScope = walSpan.makeCurrent()) {
            GarbageCollectWriteAheadLogs walogCollector =
                new GarbageCollectWriteAheadLogs(getContext(), fs, liveTServerSet, isUsingTrash());
            log.info("Beginning garbage collection of write-ahead logs");
            walogCollector.collect(status);
            gcCycleMetrics.setLastWalCollect(status.lastLog);
          } catch (Exception e) {
            TraceUtil.setException(walSpan, e, false);
            log.error("{}", e.getMessage(), e);
          } finally {
            walSpan.end();
          }
        } catch (Exception e) {
          TraceUtil.setException(innerSpan, e, true);
          throw e;
        } finally {
          innerSpan.end();
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
              log.trace("'none - no action' or invalid value provided: {}", action);
          }

          final long actionComplete = System.nanoTime();

          gcCycleMetrics.setPostOpDurationNanos(actionComplete - actionStart);

          log.info("gc post action {} completed in {} seconds", action, String.format("%.2f",
              (TimeUnit.NANOSECONDS.toMillis(actionComplete - actionStart) / 1000.0)));

        } catch (Exception e) {
          TraceUtil.setException(outerSpan, e, false);
          log.warn("{}", e.getMessage(), e);
        }
      } catch (Exception e) {
        TraceUtil.setException(outerSpan, e, true);
        throw e;
      } finally {
        outerSpan.end();
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
    var path = ServiceLock.path(getContext().getZooKeeperRoot() + Constants.ZGC_LOCK);

    LockWatcher lockWatcher = new LockWatcher() {
      @Override
      public void lostLock(LockLossReason reason) {
        Halt.halt("GC lock in zookeeper lost (reason = " + reason + "), exiting!", 1);
      }

      @Override
      public void unableToMonitorLockNode(final Exception e) {
        // ACCUMULO-3651 Level changed to error and FATAL added to message for slf4j compatibility
        Halt.halt(-1, () -> log.error("FATAL: No longer able to monitor lock node ", e));

      }
    };

    UUID zooLockUUID = UUID.randomUUID();
    while (true) {
      ServiceLock lock =
          new ServiceLock(getContext().getZooReaderWriter().getZooKeeper(), path, zooLockUUID);
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

    try {
      TProcessor processor =
          ThriftProcessorTypes.getGcTProcessor(this, getContext(), getConfiguration());
      IntStream port = getConfiguration().getPortStream(Property.GC_PORT);
      HostAndPort[] addresses = TServerUtils.getHostAndPorts(getHostname(), port);
      long maxMessageSize = getConfiguration().getAsBytes(Property.GENERAL_MAX_MESSAGE_SIZE);
      ServerAddress server = TServerUtils.startTServer(getConfiguration(),
          getContext().getThriftServerType(), processor, this.getClass().getSimpleName(),
          "GC Monitor Service", 2, ThreadPools.DEFAULT_TIMEOUT_MILLISECS, 1000, maxMessageSize,
          getContext().getServerSslParams(), getContext().getSaslParams(), 0, addresses);
      log.debug("Starting garbage collector listening on " + server.address);
      return server.address;
    } catch (Exception ex) {
      // ACCUMULO-3651 Level changed to error and FATAL added to message for slf4j compatibility
      log.error("FATAL:", ex);
      throw new RuntimeException(ex);
    }
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

    // when deleting a dir and all files in that dir, only need to delete the dir.
    // The dir will sort right before the files... so remove the files in this case
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
              for (Volume cvol : fs.getVolumes()) {
                if (cvol.containsPath(vol)) {
                  seenVolumes.add(vol);
                  sameVol = true;
                }
              }
            }
          } else {
            sameVol = Objects.equals(FileType.TABLE.getVolume(lastDirAbs), vol);
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

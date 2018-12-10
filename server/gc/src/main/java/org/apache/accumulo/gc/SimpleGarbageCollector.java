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
package org.apache.accumulo.gc;

import static org.apache.accumulo.fate.util.UtilWaitThread.sleepUninterruptibly;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.UnknownHostException;
import java.util.Collections;
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
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.IsolatedScanner;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.clientImpl.Table;
import org.apache.accumulo.core.clientImpl.Tables;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.gc.thrift.GCMonitorService.Iface;
import org.apache.accumulo.core.gc.thrift.GCMonitorService.Processor;
import org.apache.accumulo.core.gc.thrift.GCStatus;
import org.apache.accumulo.core.gc.thrift.GcCycleStats;
import org.apache.accumulo.core.master.state.tables.TableState;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.metadata.RootTable;
import org.apache.accumulo.core.metadata.schema.MetadataSchema;
import org.apache.accumulo.core.metadata.schema.TabletMetadata;
import org.apache.accumulo.core.metadata.schema.TabletsMetadata;
import org.apache.accumulo.core.replication.ReplicationSchema.StatusSection;
import org.apache.accumulo.core.replication.ReplicationTable;
import org.apache.accumulo.core.replication.ReplicationTableOfflineException;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.securityImpl.thrift.TCredentials;
import org.apache.accumulo.core.trace.Span;
import org.apache.accumulo.core.trace.Trace;
import org.apache.accumulo.core.trace.TraceSamplers;
import org.apache.accumulo.core.trace.thrift.TInfo;
import org.apache.accumulo.core.trace.wrappers.TraceWrap;
import org.apache.accumulo.core.util.HostAndPort;
import org.apache.accumulo.core.util.NamingThreadFactory;
import org.apache.accumulo.core.util.Pair;
import org.apache.accumulo.core.util.ServerServices;
import org.apache.accumulo.core.util.ServerServices.Service;
import org.apache.accumulo.fate.zookeeper.ZooLock;
import org.apache.accumulo.fate.zookeeper.ZooLock.LockLossReason;
import org.apache.accumulo.fate.zookeeper.ZooLock.LockWatcher;
import org.apache.accumulo.gc.replication.CloseWriteAheadLogReferences;
import org.apache.accumulo.server.ServerConstants;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.ServerOpts;
import org.apache.accumulo.server.fs.VolumeManager;
import org.apache.accumulo.server.fs.VolumeManager.FileType;
import org.apache.accumulo.server.fs.VolumeUtil;
import org.apache.accumulo.server.replication.proto.Replication.Status;
import org.apache.accumulo.server.rpc.ServerAddress;
import org.apache.accumulo.server.rpc.TCredentialsUpdatingWrapper;
import org.apache.accumulo.server.rpc.TServerUtils;
import org.apache.accumulo.server.rpc.ThriftServerType;
import org.apache.accumulo.server.util.Halt;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.htrace.impl.ProbabilitySampler;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.beust.jcommander.Parameter;
import com.google.common.collect.Iterators;
import com.google.common.collect.Maps;
import com.google.protobuf.InvalidProtocolBufferException;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

// Could/Should implement HighlyAvaialbleService but the Thrift server is already started before
// the ZK lock is acquired. The server is only for metrics, there are no concerns about clients
// using the service before the lock is acquired.
public class SimpleGarbageCollector implements Iface {
  private static final Text EMPTY_TEXT = new Text();

  /**
   * Options for the garbage collector.
   */
  static class Opts extends ServerOpts {
    @Parameter(names = {"-v", "--verbose"},
        description = "extra information will get printed to stdout also")
    boolean verbose = false;
    @Parameter(names = {"-s", "--safemode"}, description = "safe mode will not delete files")
    boolean safeMode = false;
  }

  /**
   * A fraction representing how much of the JVM's available memory should be used for gathering
   * candidates.
   */
  static final float CANDIDATE_MEMORY_PERCENTAGE = 0.50f;

  private static final Logger log = LoggerFactory.getLogger(SimpleGarbageCollector.class);

  private ServerContext context;
  private VolumeManager fs;
  private Opts opts;
  private ZooLock lock;

  private GCStatus status = new GCStatus(new GcCycleStats(), new GcCycleStats(), new GcCycleStats(),
      new GcCycleStats());

  public static void main(String[] args) {
    final String app = "gc";
    Opts opts = new Opts();
    opts.parseArgs(app, args);
    ServerContext context = new ServerContext(opts.getSiteConfiguration());
    context.setupServer(app, SimpleGarbageCollector.class.getName(), opts.getAddress());
    try {
      SimpleGarbageCollector gc = new SimpleGarbageCollector(opts, context);
      gc.run();
    } finally {
      context.teardownServer();
    }
  }

  public SimpleGarbageCollector(Opts opts, ServerContext context) {
    this.context = context;
    this.opts = opts;
    this.fs = context.getVolumeManager();

    long gcDelay = getConfiguration().getTimeInMillis(Property.GC_CYCLE_DELAY);
    log.info("start delay: {} milliseconds", getStartDelay());
    log.info("time delay: {} milliseconds", gcDelay);
    log.info("safemode: {}", opts.safeMode);
    log.info("verbose: {}", opts.verbose);
    log.info("memory threshold: {} of bytes", CANDIDATE_MEMORY_PERCENTAGE,
        Runtime.getRuntime().maxMemory());
    log.info("delete threads: {}", getNumDeleteThreads());
  }

  ServerContext getContext() {
    return context;
  }

  AccumuloConfiguration getConfiguration() {
    return context.getConfiguration();
  }

  AccumuloClient getClient() throws AccumuloSecurityException, AccumuloException {
    return context.getClient();
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
   * Gets the volume manager used by this GC.
   *
   * @return volume manager
   */
  VolumeManager getVolumeManager() {
    return fs;
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
   * Gets the options for this garbage collector.
   */
  Opts getOpts() {
    return opts;
  }

  /**
   * Gets the number of threads used for deleting files.
   *
   * @return number of delete threads
   */
  int getNumDeleteThreads() {
    return getConfiguration().getCount(Property.GC_DELETE_THREADS);
  }

  private class GCEnv implements GarbageCollectionEnvironment {

    private String tableName;

    GCEnv(String tableName) {
      this.tableName = tableName;
    }

    @Override
    public boolean getCandidates(String continuePoint, List<String> result)
        throws TableNotFoundException, AccumuloException, AccumuloSecurityException {
      // want to ensure GC makes progress... if the 1st N deletes are stable and we keep processing
      // them,
      // then will never inspect deletes after N
      Range range = MetadataSchema.DeletesSection.getRange();
      if (continuePoint != null && !continuePoint.isEmpty()) {
        String continueRow = MetadataSchema.DeletesSection.getRowPrefix() + continuePoint;
        range = new Range(new Key(continueRow).followingKey(PartialKey.ROW), true,
            range.getEndKey(), range.isEndKeyInclusive());
      }

      Scanner scanner = getClient().createScanner(tableName, Authorizations.EMPTY);
      scanner.setRange(range);
      result.clear();
      // find candidates for deletion; chop off the prefix
      for (Entry<Key,Value> entry : scanner) {
        String cand = entry.getKey().getRow().toString()
            .substring(MetadataSchema.DeletesSection.getRowPrefix().length());
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
    public Iterator<String> getBlipIterator()
        throws TableNotFoundException, AccumuloException, AccumuloSecurityException {
      @SuppressWarnings("resource")
      IsolatedScanner scanner = new IsolatedScanner(
          getClient().createScanner(tableName, Authorizations.EMPTY));

      scanner.setRange(MetadataSchema.BlipSection.getRange());

      return Iterators.transform(scanner.iterator(), entry -> entry.getKey().getRow().toString()
          .substring(MetadataSchema.BlipSection.getRowPrefix().length()));
    }

    @Override
    public Stream<Reference> getReferences()
        throws TableNotFoundException, AccumuloException, AccumuloSecurityException {

      Stream<TabletMetadata> tabletStream = TabletsMetadata.builder().scanTable(tableName)
          .checkConsistency().fetchDir().fetchFiles().fetchScans().build(getClient()).stream();

      Stream<Reference> refStream = tabletStream.flatMap(tm -> {
        Stream<Reference> refs = Stream.concat(tm.getFiles().stream(), tm.getScans().stream())
            .map(f -> new Reference(tm.getTableId(), f, false));
        if (tm.getDir() != null) {
          refs = Stream.concat(refs, Stream.of(new Reference(tm.getTableId(), tm.getDir(), true)));
        }
        return refs;
      });

      return refStream;
    }

    @Override
    public Set<Table.ID> getTableIDs() {
      return Tables.getIdToNameMap(context).keySet();
    }

    @Override
    public void delete(SortedMap<String,String> confirmedDeletes)
        throws IOException, AccumuloException, AccumuloSecurityException, TableNotFoundException {

      if (opts.safeMode) {
        if (opts.verbose)
          System.out.println("SAFEMODE: There are " + confirmedDeletes.size()
              + " data file candidates marked for deletion.%n"
              + "          Examine the log files to identify them.%n");
        log.info("SAFEMODE: Listing all data file candidates for deletion");
        for (String s : confirmedDeletes.values())
          log.info("SAFEMODE: {}", s);
        log.info("SAFEMODE: End candidates for deletion");
        return;
      }

      AccumuloClient c = getClient();
      BatchWriter writer = c.createBatchWriter(tableName, new BatchWriterConfig());

      // when deleting a dir and all files in that dir, only need to delete the dir
      // the dir will sort right before the files... so remove the files in this case
      // to minimize namenode ops
      Iterator<Entry<String,String>> cdIter = confirmedDeletes.entrySet().iterator();

      String lastDir = null;
      while (cdIter.hasNext()) {
        Entry<String,String> entry = cdIter.next();
        String relPath = entry.getKey();
        String absPath = fs.getFullPath(FileType.TABLE, entry.getValue()).toString();

        if (isDir(relPath)) {
          lastDir = absPath;
        } else if (lastDir != null) {
          if (absPath.startsWith(lastDir)) {
            log.debug("Ignoring {} because {} exist", entry.getValue(), lastDir);
            try {
              putMarkerDeleteMutation(entry.getValue(), writer);
            } catch (MutationsRejectedException e) {
              throw new RuntimeException(e);
            }
            cdIter.remove();
          } else {
            lastDir = null;
          }
        }
      }

      final BatchWriter finalWriter = writer;

      ExecutorService deleteThreadPool = Executors.newFixedThreadPool(getNumDeleteThreads(),
          new NamingThreadFactory("deleting"));

      final List<Pair<Path,Path>> replacements = ServerConstants
          .getVolumeReplacements(getConfiguration());

      for (final String delete : confirmedDeletes.values()) {

        Runnable deleteTask = new Runnable() {
          @Override
          public void run() {
            boolean removeFlag;

            try {
              Path fullPath;
              String switchedDelete = VolumeUtil.switchVolume(delete, FileType.TABLE, replacements);
              if (switchedDelete != null) {
                // actually replacing the volumes in the metadata table would be tricky because the
                // entries would be different rows. So it could not be
                // atomically in one mutation and extreme care would need to be taken that delete
                // entry was not lost. Instead of doing that, just deal with
                // volume switching when something needs to be deleted. Since the rest of the code
                // uses suffixes to compare delete entries, there is no danger
                // of deleting something that should not be deleted. Must not change value of delete
                // variable because thats whats stored in metadata table.
                log.debug("Volume replaced {} -> ", delete, switchedDelete);
                fullPath = fs.getFullPath(FileType.TABLE, switchedDelete);
              } else {
                fullPath = fs.getFullPath(FileType.TABLE, delete);
              }

              log.debug("Deleting {}", fullPath);

              if (moveToTrash(fullPath) || fs.deleteRecursively(fullPath)) {
                // delete succeeded, still want to delete
                removeFlag = true;
                synchronized (SimpleGarbageCollector.this) {
                  ++status.current.deleted;
                }
              } else if (fs.exists(fullPath)) {
                // leave the entry in the metadata; we'll try again later
                removeFlag = false;
                synchronized (SimpleGarbageCollector.this) {
                  ++status.current.errors;
                }
                log.warn("File exists, but was not deleted for an unknown reason: {}", fullPath);
              } else {
                // this failure, we still want to remove the metadata entry
                removeFlag = true;
                synchronized (SimpleGarbageCollector.this) {
                  ++status.current.errors;
                }
                String parts[] = fullPath.toString().split(Constants.ZTABLES)[1].split("/");
                if (parts.length > 2) {
                  Table.ID tableId = Table.ID.of(parts[1]);
                  String tabletDir = parts[2];
                  context.getTableManager().updateTableStateCache(tableId);
                  TableState tableState = context.getTableManager().getTableState(tableId);
                  if (tableState != null && tableState != TableState.DELETING) {
                    // clone directories don't always exist
                    if (!tabletDir.startsWith(Constants.CLONE_PREFIX))
                      log.debug("File doesn't exist: {}", fullPath);
                  }
                } else {
                  log.warn("Very strange path name: {}", delete);
                }
              }

              // proceed to clearing out the flags for successful deletes and
              // non-existent files
              if (removeFlag && finalWriter != null) {
                putMarkerDeleteMutation(delete, finalWriter);
              }
            } catch (Exception e) {
              log.error("{}", e.getMessage(), e);
            }

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

      if (writer != null) {
        try {
          writer.close();
        } catch (MutationsRejectedException e) {
          log.error("Problem removing entries from the metadata table: ", e);
        }
      }
    }

    @Override
    public void deleteTableDirIfEmpty(Table.ID tableID) throws IOException {
      // if dir exist and is empty, then empty list is returned...
      // hadoop 2.0 will throw an exception if the file does not exist
      for (String dir : ServerConstants.getTablesDirs(context.getConfiguration())) {
        FileStatus[] tabletDirs = null;
        try {
          tabletDirs = fs.listStatus(new Path(dir + "/" + tableID));
        } catch (FileNotFoundException ex) {
          continue;
        }

        if (tabletDirs.length == 0) {
          Path p = new Path(dir + "/" + tableID);
          log.debug("Removing table dir {}", p);
          if (!moveToTrash(p))
            fs.delete(p);
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
    public Iterator<Entry<String,Status>> getReplicationNeededIterator()
        throws AccumuloException, AccumuloSecurityException {
      AccumuloClient client = getClient();
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

  @SuppressFBWarnings(value = "DM_EXIT", justification = "main class can call System.exit")
  private void run() {
    long tStart, tStop;

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

    ProbabilitySampler sampler = TraceSamplers
        .probabilitySampler(getConfiguration().getFraction(Property.GC_TRACE_PERCENT));

    while (true) {
      Trace.on("gc", sampler);

      Span gcSpan = Trace.start("loop");
      tStart = System.currentTimeMillis();
      try {
        System.gc(); // make room

        status.current.started = System.currentTimeMillis();

        new GarbageCollectionAlgorithm().collect(new GCEnv(RootTable.NAME));
        new GarbageCollectionAlgorithm().collect(new GCEnv(MetadataTable.NAME));

        log.info("Number of data file candidates for deletion: {}", status.current.candidates);
        log.info("Number of data file candidates still in use: {}", status.current.inUse);
        log.info("Number of successfully deleted data files: {}", status.current.deleted);
        log.info("Number of data files delete failures: {}", status.current.errors);

        status.current.finished = System.currentTimeMillis();
        status.last = status.current;
        status.current = new GcCycleStats();

      } catch (Exception e) {
        log.error("{}", e.getMessage(), e);
      }

      tStop = System.currentTimeMillis();
      log.info(String.format("Collect cycle took %.2f seconds", ((tStop - tStart) / 1000.0)));

      // We want to prune references to fully-replicated WALs from the replication table which are
      // no longer referenced in the metadata table
      // before running GarbageCollectWriteAheadLogs to ensure we delete as many files as possible.
      Span replSpan = Trace.start("replicationClose");
      try {
        CloseWriteAheadLogReferences closeWals = new CloseWriteAheadLogReferences(context);
        closeWals.run();
      } catch (Exception e) {
        log.error("Error trying to close write-ahead logs for replication table", e);
      } finally {
        replSpan.stop();
      }

      // Clean up any unused write-ahead logs
      Span waLogs = Trace.start("walogs");
      try {
        GarbageCollectWriteAheadLogs walogCollector = new GarbageCollectWriteAheadLogs(context, fs,
            isUsingTrash());
        log.info("Beginning garbage collection of write-ahead logs");
        walogCollector.collect(status);
      } catch (Exception e) {
        log.error("{}", e.getMessage(), e);
      } finally {
        waLogs.stop();
      }
      gcSpan.stop();

      // we just made a lot of metadata changes: flush them out
      try {
        AccumuloClient accumuloClient = getClient();
        accumuloClient.tableOperations().compact(MetadataTable.NAME, null, null, true, true);
        accumuloClient.tableOperations().compact(RootTable.NAME, null, null, true, true);
      } catch (Exception e) {
        log.warn("{}", e.getMessage(), e);
      }

      Trace.off();
      try {
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
    if (!isUsingTrash())
      return false;
    try {
      return fs.moveToTrash(path);
    } catch (FileNotFoundException ex) {
      return false;
    }
  }

  private void getZooLock(HostAndPort addr) throws KeeperException, InterruptedException {
    String path = context.getZooKeeperRoot() + Constants.ZGC_LOCK;

    LockWatcher lockWatcher = new LockWatcher() {
      @Override
      public void lostLock(LockLossReason reason) {
        Halt.halt("GC lock in zookeeper lost (reason = " + reason + "), exiting!", 1);
      }

      @Override
      public void unableToMonitorLockNode(final Throwable e) {
        // ACCUMULO-3651 Level changed to error and FATAL added to message for slf4j compatibility
        Halt.halt(-1, new Runnable() {

          @Override
          public void run() {
            log.error("FATAL: No longer able to monitor lock node ", e);
          }
        });

      }
    };

    while (true) {
      lock = new ZooLock(context.getZooReaderWriter(), path);
      if (lock.tryLock(lockWatcher,
          new ServerServices(addr.toString(), Service.GC_CLIENT).toString().getBytes())) {
        log.debug("Got GC ZooKeeper lock");
        return;
      }
      log.debug("Failed to get GC ZooKeeper lock, will retry");
      sleepUninterruptibly(1, TimeUnit.SECONDS);
    }
  }

  private HostAndPort startStatsService() throws UnknownHostException {
    Iface rpcProxy = TraceWrap.service(this);
    final Processor<Iface> processor;
    if (ThriftServerType.SASL == context.getThriftServerType()) {
      Iface tcProxy = TCredentialsUpdatingWrapper.service(rpcProxy, getClass(), getConfiguration());
      processor = new Processor<>(tcProxy);
    } else {
      processor = new Processor<>(rpcProxy);
    }
    int port[] = getConfiguration().getPort(Property.GC_PORT);
    HostAndPort[] addresses = TServerUtils.getHostAndPorts(this.opts.getAddress(), port);
    long maxMessageSize = getConfiguration().getAsBytes(Property.GENERAL_MAX_MESSAGE_SIZE);
    try {
      ServerAddress server = TServerUtils.startTServer(getConfiguration(),
          context.getThriftServerType(), processor, this.getClass().getSimpleName(),
          "GC Monitor Service", 2,
          getConfiguration().getCount(Property.GENERAL_SIMPLETIMER_THREADPOOL_SIZE), 1000,
          maxMessageSize, context.getServerSslParams(), context.getSaslParams(), 0, addresses);
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
    return runtime.totalMemory() - runtime.freeMemory() > CANDIDATE_MEMORY_PERCENTAGE
        * runtime.maxMemory();
  }

  private static void putMarkerDeleteMutation(final String delete, final BatchWriter writer)
      throws MutationsRejectedException {
    Mutation m = new Mutation(MetadataSchema.DeletesSection.getRowPrefix() + delete);
    m.putDelete(EMPTY_TEXT, EMPTY_TEXT);
    writer.addMutation(m);
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
    for (int i = 0; i < delete.length(); i++)
      if (delete.charAt(i) == '/')
        slashCount++;
    return slashCount == 1;
  }

  @Override
  public GCStatus getStatus(TInfo info, TCredentials credentials) {
    return status;
  }
}

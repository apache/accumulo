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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.gc.thrift.GCStatus;
import org.apache.accumulo.core.gc.thrift.GcCycleStats;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.metadata.RootTable;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.CurrentLogsSection;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.ReplicationSection;
import org.apache.accumulo.core.protobuf.ProtobufUtil;
import org.apache.accumulo.core.replication.ReplicationSchema.StatusSection;
import org.apache.accumulo.core.replication.ReplicationTable;
import org.apache.accumulo.core.replication.ReplicationTableOfflineException;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.tabletserver.log.LogEntry;
import org.apache.accumulo.core.trace.Trace;
import org.apache.accumulo.core.volume.Volume;
import org.apache.accumulo.core.zookeeper.ZooUtil;
import org.apache.accumulo.server.AccumuloServerContext;
import org.apache.accumulo.server.ServerConstants;
import org.apache.accumulo.server.fs.VolumeManager;
import org.apache.accumulo.server.fs.VolumeManagerImpl;
import org.apache.accumulo.server.master.LiveTServerSet;
import org.apache.accumulo.server.master.LiveTServerSet.Listener;
import org.apache.accumulo.server.master.state.MetaDataStateStore;
import org.apache.accumulo.server.master.state.RootTabletStateStore;
import org.apache.accumulo.server.master.state.TServerInstance;
import org.apache.accumulo.server.master.state.TabletLocationState;
import org.apache.accumulo.server.master.state.TabletState;
import org.apache.accumulo.server.replication.StatusUtil;
import org.apache.accumulo.server.replication.proto.Replication.Status;
import org.apache.accumulo.server.zookeeper.ZooReaderWriter;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.io.Text;
import org.apache.htrace.Span;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.net.HostAndPort;
import com.google.protobuf.InvalidProtocolBufferException;

public class GarbageCollectWriteAheadLogs {
  private static final Logger log = LoggerFactory.getLogger(GarbageCollectWriteAheadLogs.class);

  private final AccumuloServerContext context;
  private final VolumeManager fs;
  private final boolean useTrash;
  private final LiveTServerSet liveServers;

  /**
   * Creates a new GC WAL object.
   *
   * @param context
   *          the collection server's context
   * @param fs
   *          volume manager to use
   * @param useTrash
   *          true to move files to trash rather than delete them
   */
  GarbageCollectWriteAheadLogs(AccumuloServerContext context, VolumeManager fs, boolean useTrash) throws IOException {
    this.context = context;
    this.fs = fs;
    this.useTrash = useTrash;
    this.liveServers = new LiveTServerSet(context, new Listener() {
      @Override
      public void update(LiveTServerSet current, Set<TServerInstance> deleted, Set<TServerInstance> added) {
        log.debug("New tablet servers noticed: " + added);
        log.debug("Tablet servers removed: " + deleted);
      }
    });
    liveServers.startListeningForTabletServerChanges();
  }

  public void collect(GCStatus status) {

    Span span = Trace.start("getCandidates");
    try {
      Set<TServerInstance> currentServers = liveServers.getCurrentServers();


      status.currentLog.started = System.currentTimeMillis();

      Map<TServerInstance, Set<Path> > candidates = new HashMap<>();
      long count = getCurrent(candidates, currentServers);
      long fileScanStop = System.currentTimeMillis();

      log.info(String.format("Fetched %d files for %d servers in %.2f seconds", count, candidates.size(),
          (fileScanStop - status.currentLog.started) / 1000.));
      status.currentLog.candidates = count;
      span.stop();

      span = Trace.start("removeMetadataEntries");
      try {
        count = removeMetadataEntries(candidates, status, currentServers);
      } catch (Exception ex) {
        log.error("Unable to scan metadata table", ex);
        return;
      } finally {
        span.stop();
      }

      long logEntryScanStop = System.currentTimeMillis();
      log.info(String.format("%d log entries scanned in %.2f seconds", count, (logEntryScanStop - fileScanStop) / 1000.));

      span = Trace.start("removeReplicationEntries");
      try {
        count = removeReplicationEntries(candidates, status);
      } catch (Exception ex) {
        log.error("Unable to scan replication table", ex);
        return;
      } finally {
        span.stop();
      }

      long replicationEntryScanStop = System.currentTimeMillis();
      log.info(String.format("%d replication entries scanned in %.2f seconds", count, (replicationEntryScanStop - logEntryScanStop) / 1000.));

      span = Trace.start("removeFiles");

      count = removeFiles(candidates, status);

      long removeStop = System.currentTimeMillis();
      log.info(String.format("%d total logs removed from %d servers in %.2f seconds", count, candidates.size(), (removeStop - logEntryScanStop) / 1000.));
      span.stop();

      span = Trace.start("removeMarkers");
      count = removeMarkers(candidates);
      long removeMarkersStop = System.currentTimeMillis();
      log.info(String.format("%d markers removed in %.2f seconds", count, (removeMarkersStop - removeStop) / 1000.));
      span.stop();


      status.currentLog.finished = removeStop;
      status.lastLog = status.currentLog;
      status.currentLog = new GcCycleStats();

    } catch (Exception e) {
      log.error("exception occured while garbage collecting write ahead logs", e);
    } finally {
      span.stop();
    }
  }

  private long removeMarkers(Map<TServerInstance,Set<Path>> candidates) {
    long result = 0;
    try {
      BatchWriter root = null;
      BatchWriter meta = null;
      try {
        root = context.getConnector().createBatchWriter(RootTable.NAME, null);
        meta = context.getConnector().createBatchWriter(MetadataTable.NAME, null);
        for (Entry<TServerInstance,Set<Path>> entry : candidates.entrySet()) {
          Mutation m = new Mutation(CurrentLogsSection.getRowPrefix() + entry.getKey().toString());
          for (Path path : entry.getValue()) {
            m.putDelete(CurrentLogsSection.COLF, new Text(path.toString()));
            result++;
          }
          root.addMutation(m);
          meta.addMutation(m);
        }
      } finally  {
        if (meta != null) {
          meta.close();
        }
        if (root != null) {
          root.close();
        }
      }
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
    return result;
  }

  private long removeFiles(Map<TServerInstance, Set<Path> > candidates, final GCStatus status) {
    for (Entry<TServerInstance,Set<Path>> entry : candidates.entrySet()) {
      for (Path path : entry.getValue()) {
        log.debug("Removing unused WAL for server " + entry.getKey() + " log " + path);
        try {
          if (!useTrash || !fs.moveToTrash(path))
            fs.deleteRecursively(path);
          status.currentLog.deleted++;
        } catch (FileNotFoundException ex) {
          // ignored
        } catch (IOException ex) {
          log.error("Unable to delete wal " + path + ": " + ex);
        }
      }
    }
    return status.currentLog.deleted;
  }

  private long removeMetadataEntries(Map<TServerInstance, Set<Path> > candidates, GCStatus status, Set<TServerInstance> liveServers) throws IOException, KeeperException,
      InterruptedException {

    // remove any entries if there's a log reference, or a tablet is still assigned to the dead server

    Map<Path, TServerInstance> walToDeadServer = new HashMap<>();
    for (Entry<TServerInstance,Set<Path>> entry : candidates.entrySet()) {
      for (Path file : entry.getValue()) {
        walToDeadServer.put(file, entry.getKey());
      }
    }
    long count = 0;
    RootTabletStateStore root = new RootTabletStateStore(context);
    MetaDataStateStore meta = new MetaDataStateStore(context);
    Iterator<TabletLocationState> states = Iterators.concat(root.iterator(), meta.iterator());
    while (states.hasNext()) {
      count++;
      TabletLocationState state = states.next();
      if (state.getState(liveServers) == TabletState.ASSIGNED_TO_DEAD_SERVER) {
        candidates.remove(state.current);
      }
      for (Collection<String> wals : state.walogs) {
        for (String wal : wals) {
          TServerInstance dead = walToDeadServer.get(new Path(wal));
          if (dead != null) {
            candidates.get(dead).remove(wal);
          }
        }
      }
    }
    return count;
  }

  protected int removeReplicationEntries(Map<TServerInstance, Set<Path> > candidates, GCStatus status) throws IOException, KeeperException,
  InterruptedException {
    Connector conn;
    try {
      conn = context.getConnector();
    } catch (AccumuloException | AccumuloSecurityException e) {
      log.error("Failed to get connector", e);
      throw new IllegalArgumentException(e);
    }

    int count = 0;

    Iterator<Entry<TServerInstance,Set<Path>>> walIter = candidates.entrySet().iterator();

    while (walIter.hasNext()) {
      Entry<TServerInstance,Set<Path>> wal = walIter.next();
      Iterator<Path> paths = wal.getValue().iterator();
      while (paths.hasNext()) {
        Path fullPath = paths.next();
        if (neededByReplication(conn, fullPath)) {
          log.debug("Removing WAL from candidate deletion as it is still needed for replication: {}", fullPath);
          // If we haven't already removed it, check to see if this WAL is
          // "in use" by replication (needed for replication purposes)
          status.currentLog.inUse++;
          paths.remove();
        } else {
          log.debug("WAL not needed for replication {}", fullPath);
        }
      }
      if (wal.getValue().isEmpty()) {
        walIter.remove();
      }
      count++;
    }

    return count;
  }


  /**
   * Determine if the given WAL is needed for replication
   *
   * @param wal
   *          The full path (URI)
   * @return True if the WAL is still needed by replication (not a candidate for deletion)
   */
  protected boolean neededByReplication(Connector conn, Path wal) {
    log.info("Checking replication table for " + wal);

    Iterable<Entry<Key,Value>> iter = getReplicationStatusForFile(conn, wal);

    // TODO Push down this filter to the tserver to only return records
    // that are not completely replicated and convert this loop into a
    // `return s.iterator.hasNext()` statement
    for (Entry<Key,Value> entry : iter) {
      try {
        Status status = Status.parseFrom(entry.getValue().get());
        log.info("Checking if {} is safe for removal with {}", wal, ProtobufUtil.toString(status));
        if (!StatusUtil.isSafeForRemoval(status)) {
          return true;
        }
      } catch (InvalidProtocolBufferException e) {
        log.error("Could not deserialize Status protobuf for " + entry.getKey(), e);
      }
    }

    return false;
  }

  protected Iterable<Entry<Key,Value>> getReplicationStatusForFile(Connector conn, Path wal) {
    Scanner metaScanner;
    try {
      metaScanner = conn.createScanner(MetadataTable.NAME, Authorizations.EMPTY);
    } catch (TableNotFoundException e) {
      throw new RuntimeException(e);
    }

    // Need to add in the replication section prefix
    metaScanner.setRange(Range.exact(ReplicationSection.getRowPrefix() + wal));
    // Limit the column family to be sure
    metaScanner.fetchColumnFamily(ReplicationSection.COLF);

    try {
      Scanner replScanner = ReplicationTable.getScanner(conn);

      // Scan only the Status records
      StatusSection.limit(replScanner);

      // Only look for this specific WAL
      replScanner.setRange(Range.exact(wal.toString()));

      return Iterables.concat(metaScanner, replScanner);
    } catch (ReplicationTableOfflineException e) {
      // do nothing
    }

    return metaScanner;
  }




  /**
   * Scans log markers. The map passed in is populated with the logs for dead servers.
   *
   * @param unusedLogs
   *          map of dead server to log file entries
   * @return total number of log files
   */
  private long getCurrent(Map<TServerInstance, Set<Path> > unusedLogs, Set<TServerInstance> currentServers) throws Exception {
    Set<Path> rootWALs = new HashSet<>();
    // Get entries in zookeeper:
    String zpath = ZooUtil.getRoot(context.getInstance()) + RootTable.ZROOT_TABLET_WALOGS;
    ZooReaderWriter zoo = ZooReaderWriter.getInstance();
    List<String> children = zoo.getChildren(zpath);
    for (String child : children) {
      LogEntry entry = LogEntry.fromBytes(zoo.getData(zpath + "/" + child, null));
      rootWALs.add(new Path(entry.filename));
    }
    long count = 0;

    // get all the WAL markers that are not in zookeeper for dead servers
    Scanner rootScanner = context.getConnector().createScanner(RootTable.NAME, Authorizations.EMPTY);
    rootScanner.setRange(CurrentLogsSection.getRange());
    Scanner metaScanner = context.getConnector().createScanner(MetadataTable.NAME, Authorizations.EMPTY);
    metaScanner.setRange(CurrentLogsSection.getRange());
    Iterator<Entry<Key,Value>> entries = Iterators.concat(rootScanner.iterator(), metaScanner.iterator());
    Text hostAndPort = new Text();
    Text sessionId = new Text();
    Text filename = new Text();
    while (entries.hasNext()) {
      Entry<Key,Value> entry = entries.next();
      CurrentLogsSection.getTabletServer(entry.getKey(), hostAndPort, sessionId);
      CurrentLogsSection.getPath(entry.getKey(), filename);
      TServerInstance tsi = new TServerInstance(HostAndPort.fromString(hostAndPort.toString()), sessionId.toString());
      Path path = new Path(filename.toString());
      if (!currentServers.contains(tsi) || entry.getValue().equals(CurrentLogsSection.UNUSED) && !rootWALs.contains(path)) {
        Set<Path> logs = unusedLogs.get(tsi);
        if (logs == null) {
          unusedLogs.put(tsi, logs = new HashSet<Path>());
        }
        if (logs.add(path)) {
          count++;
        }
      }
    }

    // scan HDFS for logs for dead servers
    for (Volume volume : VolumeManagerImpl.get().getVolumes()) {
      RemoteIterator<LocatedFileStatus> iter =  volume.getFileSystem().listFiles(volume.prefixChild(ServerConstants.WAL_DIR), true);
      while (iter.hasNext()) {
        LocatedFileStatus next = iter.next();
        // recursive listing returns directories, too
        if (next.isDirectory()) {
          continue;
        }
        // make sure we've waited long enough for zookeeper propagation
        if (System.currentTimeMillis() - next.getModificationTime() < context.getConnector().getInstance().getZooKeepersSessionTimeOut()) {
          continue;
        }
        Path path = next.getPath();
        String hostPlusPort = path.getParent().getName();
        // server is still alive, or has a replacement
        TServerInstance instance = liveServers.find(hostPlusPort);
        if (instance != null) {
          continue;
        }
        TServerInstance fake = new TServerInstance(hostPlusPort, 0L);
        Set<Path> paths = unusedLogs.get(fake);
        if (paths == null) {
          paths = new HashSet<>();
        }
        paths.add(path);
        unusedLogs.put(fake, paths);
      }
    }
    return count;
  }

}

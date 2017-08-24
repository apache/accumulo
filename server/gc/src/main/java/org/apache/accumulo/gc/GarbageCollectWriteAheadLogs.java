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
import java.util.UUID;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.gc.thrift.GCStatus;
import org.apache.accumulo.core.gc.thrift.GcCycleStats;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.metadata.schema.MetadataSchema;
import org.apache.accumulo.core.replication.ReplicationSchema.StatusSection;
import org.apache.accumulo.core.replication.ReplicationTable;
import org.apache.accumulo.core.replication.ReplicationTableOfflineException;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.trace.Span;
import org.apache.accumulo.core.trace.Trace;
import org.apache.accumulo.core.util.Pair;
import org.apache.accumulo.server.AccumuloServerContext;
import org.apache.accumulo.server.fs.VolumeManager;
import org.apache.accumulo.server.log.WalStateManager;
import org.apache.accumulo.server.log.WalStateManager.WalMarkerException;
import org.apache.accumulo.server.log.WalStateManager.WalState;
import org.apache.accumulo.server.master.LiveTServerSet;
import org.apache.accumulo.server.master.LiveTServerSet.Listener;
import org.apache.accumulo.server.master.state.MetaDataStateStore;
import org.apache.accumulo.server.master.state.RootTabletStateStore;
import org.apache.accumulo.server.master.state.TServerInstance;
import org.apache.accumulo.server.master.state.TabletLocationState;
import org.apache.accumulo.server.master.state.TabletState;
import org.apache.accumulo.server.zookeeper.ZooReaderWriter;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Iterators;

public class GarbageCollectWriteAheadLogs {
  private static final Logger log = LoggerFactory.getLogger(GarbageCollectWriteAheadLogs.class);

  private final AccumuloServerContext context;
  private final VolumeManager fs;
  private final boolean useTrash;
  private final LiveTServerSet liveServers;
  private final WalStateManager walMarker;
  private final Iterable<TabletLocationState> store;

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
  GarbageCollectWriteAheadLogs(final AccumuloServerContext context, VolumeManager fs, boolean useTrash) throws IOException {
    this.context = context;
    this.fs = fs;
    this.useTrash = useTrash;
    this.liveServers = new LiveTServerSet(context, new Listener() {
      @Override
      public void update(LiveTServerSet current, Set<TServerInstance> deleted, Set<TServerInstance> added) {
        log.debug("New tablet servers noticed: {}", added);
        log.debug("Tablet servers removed: {}", deleted);
      }
    });
    liveServers.startListeningForTabletServerChanges();
    this.walMarker = new WalStateManager(context.getInstance(), ZooReaderWriter.getInstance());
    this.store = new Iterable<TabletLocationState>() {
      @Override
      public Iterator<TabletLocationState> iterator() {
        return Iterators.concat(new RootTabletStateStore(context).iterator(), new MetaDataStateStore(context).iterator());
      }
    };
  }

  /**
   * Creates a new GC WAL object. Meant for testing -- allows mocked objects.
   *
   * @param context
   *          the collection server's context
   * @param fs
   *          volume manager to use
   * @param useTrash
   *          true to move files to trash rather than delete them
   * @param liveTServerSet
   *          a started LiveTServerSet instance
   */
  @VisibleForTesting
  GarbageCollectWriteAheadLogs(AccumuloServerContext context, VolumeManager fs, boolean useTrash, LiveTServerSet liveTServerSet, WalStateManager walMarker,
      Iterable<TabletLocationState> store) throws IOException {
    this.context = context;
    this.fs = fs;
    this.useTrash = useTrash;
    this.liveServers = liveTServerSet;
    this.walMarker = walMarker;
    this.store = store;
  }

  public void collect(GCStatus status) {

    Span span = Trace.start("getCandidates");
    try {
      status.currentLog.started = System.currentTimeMillis();

      Map<TServerInstance,Set<UUID>> logsByServer = new HashMap<>();
      Map<UUID,Pair<WalState,Path>> logsState = new HashMap<>();
      // Scan for log file info first: the order is important
      // Consider:
      // * get live servers
      // * new server gets a lock, creates a log
      // * get logs
      // * the log appears to belong to a dead server
      long count = getCurrent(logsByServer, logsState);
      long fileScanStop = System.currentTimeMillis();

      log.info(String.format("Fetched %d files for %d servers in %.2f seconds", count, logsByServer.size(), (fileScanStop - status.currentLog.started) / 1000.));
      status.currentLog.candidates = count;
      span.stop();

      // now it's safe to get the liveServers
      Set<TServerInstance> currentServers = liveServers.getCurrentServers();

      Map<UUID,TServerInstance> uuidToTServer;
      span = Trace.start("removeEntriesInUse");
      try {
        uuidToTServer = removeEntriesInUse(logsByServer, currentServers, logsState);
        count = uuidToTServer.size();
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
        count = removeReplicationEntries(uuidToTServer);
      } catch (Exception ex) {
        log.error("Unable to scan replication table", ex);
        return;
      } finally {
        span.stop();
      }

      long replicationEntryScanStop = System.currentTimeMillis();
      log.info(String.format("%d replication entries scanned in %.2f seconds", count, (replicationEntryScanStop - logEntryScanStop) / 1000.));

      span = Trace.start("removeFiles");

      logsState.keySet().retainAll(uuidToTServer.keySet());
      count = removeFiles(logsState.values(), status);

      long removeStop = System.currentTimeMillis();
      log.info(String.format("%d total logs removed from %d servers in %.2f seconds", count, logsByServer.size(), (removeStop - logEntryScanStop) / 1000.));
      span.stop();

      span = Trace.start("removeMarkers");
      count = removeTabletServerMarkers(uuidToTServer, logsByServer, currentServers);
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

  private long removeTabletServerMarkers(Map<UUID,TServerInstance> uidMap, Map<TServerInstance,Set<UUID>> candidates, Set<TServerInstance> liveServers) {
    long result = 0;
    // remove markers for files removed
    try {
      for (Entry<UUID,TServerInstance> entry : uidMap.entrySet()) {
        walMarker.removeWalMarker(entry.getValue(), entry.getKey());
      }
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
    // remove parent znode for dead tablet servers
    for (Entry<TServerInstance,Set<UUID>> entry : candidates.entrySet()) {
      if (!liveServers.contains(entry.getKey())) {
        log.info("Removing znode for " + entry.getKey());
        try {
          walMarker.forget(entry.getKey());
        } catch (WalMarkerException ex) {
          log.info("Error removing znode for " + entry.getKey() + " " + ex.toString());
        }
      }
    }
    return result;
  }

  private long removeFiles(Collection<Pair<WalState,Path>> collection, final GCStatus status) {
    for (Pair<WalState,Path> stateFile : collection) {
      Path path = stateFile.getSecond();
      log.debug("Removing {} WAL {}", stateFile.getFirst(), path);
      try {
        if (!useTrash || !fs.moveToTrash(path)) {
          fs.deleteRecursively(path);
        }
        status.currentLog.deleted++;
      } catch (FileNotFoundException ex) {
        // ignored
      } catch (IOException ex) {
        log.error("Unable to delete wal {}", path, ex);
      }
    }
    return status.currentLog.deleted;
  }

  private UUID path2uuid(Path path) {
    return UUID.fromString(path.getName());
  }

  private Map<UUID,TServerInstance> removeEntriesInUse(Map<TServerInstance,Set<UUID>> candidates, Set<TServerInstance> liveServers,
      Map<UUID,Pair<WalState,Path>> logsState) throws IOException, KeeperException, InterruptedException {

    Map<UUID,TServerInstance> result = new HashMap<>();
    for (Entry<TServerInstance,Set<UUID>> entry : candidates.entrySet()) {
      for (UUID id : entry.getValue()) {
        result.put(id, entry.getKey());
      }
    }

    // remove any entries if there's a log reference (recovery hasn't finished)
    Iterator<TabletLocationState> states = store.iterator();
    while (states.hasNext()) {
      TabletLocationState state = states.next();

      // Tablet is still assigned to a dead server. Master has moved markers and reassigned it
      // Easiest to just ignore all the WALs for the dead server.
      if (state.getState(liveServers) == TabletState.ASSIGNED_TO_DEAD_SERVER) {
        Set<UUID> idsToIgnore = candidates.remove(state.current);
        if (idsToIgnore != null) {
          for (UUID id : idsToIgnore) {
            result.remove(id);
          }
        }
      }
      // Tablet is being recovered and has WAL references, remove all the WALs for the dead server
      // that made the WALs.
      for (Collection<String> wals : state.walogs) {
        for (String wal : wals) {
          UUID walUUID = path2uuid(new Path(wal));
          TServerInstance dead = result.get(walUUID);
          // There's a reference to a log file, so skip that server's logs
          Set<UUID> idsToIgnore = candidates.remove(dead);
          if (idsToIgnore != null) {
            for (UUID id : idsToIgnore) {
              result.remove(id);
            }
          }
        }
      }
    }

    // Remove OPEN and CLOSED logs for live servers: they are still in use
    for (TServerInstance liveServer : liveServers) {
      Set<UUID> idsForServer = candidates.get(liveServer);
      // Server may not have any logs yet
      if (idsForServer != null) {
        for (UUID id : idsForServer) {
          Pair<WalState,Path> stateFile = logsState.get(id);
          if (stateFile.getFirst() != WalState.UNREFERENCED) {
            result.remove(id);
          }
        }
      }
    }
    return result;
  }

  protected int removeReplicationEntries(Map<UUID,TServerInstance> candidates) throws IOException, KeeperException, InterruptedException {
    Connector conn;
    try {
      conn = context.getConnector();
      try {
        final Scanner s = ReplicationTable.getScanner(conn);
        StatusSection.limit(s);
        for (Entry<Key,Value> entry : s) {
          UUID id = path2uuid(new Path(entry.getKey().getRow().toString()));
          candidates.remove(id);
          log.info("Ignore closed log " + id + " because it is being replicated");
        }
      } catch (ReplicationTableOfflineException ex) {
        return candidates.size();
      }

      final Scanner scanner = conn.createScanner(MetadataTable.NAME, Authorizations.EMPTY);
      scanner.fetchColumnFamily(MetadataSchema.ReplicationSection.COLF);
      scanner.setRange(MetadataSchema.ReplicationSection.getRange());
      for (Entry<Key,Value> entry : scanner) {
        Text file = new Text();
        MetadataSchema.ReplicationSection.getFile(entry.getKey(), file);
        UUID id = path2uuid(new Path(file.toString()));
        candidates.remove(id);
        log.info("Ignore closed log " + id + " because it is being replicated");
      }

      return candidates.size();
    } catch (AccumuloException | AccumuloSecurityException | TableNotFoundException e) {
      log.error("Failed to scan metadata table", e);
      throw new IllegalArgumentException(e);
    }
  }

  /**
   * Scans log markers. The map passed in is populated with the log ids.
   *
   * @param logsByServer
   *          map of dead server to log file entries
   * @return total number of log files
   */
  private long getCurrent(Map<TServerInstance,Set<UUID>> logsByServer, Map<UUID,Pair<WalState,Path>> logState) throws Exception {

    // get all the unused WALs in zookeeper
    long result = 0;
    Map<TServerInstance,List<UUID>> markers = walMarker.getAllMarkers();
    for (Entry<TServerInstance,List<UUID>> entry : markers.entrySet()) {
      HashSet<UUID> ids = new HashSet<>(entry.getValue().size());
      for (UUID id : entry.getValue()) {
        ids.add(id);
        logState.put(id, walMarker.state(entry.getKey(), id));
        result++;
      }
      logsByServer.put(entry.getKey(), ids);
    }
    return result;
  }
}

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
package org.apache.accumulo.gc;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;

import org.apache.accumulo.core.gc.thrift.GCStatus;
import org.apache.accumulo.core.gc.thrift.GcCycleStats;
import org.apache.accumulo.core.metadata.TServerInstance;
import org.apache.accumulo.core.metadata.TabletLocationState;
import org.apache.accumulo.core.metadata.TabletState;
import org.apache.accumulo.core.metadata.schema.Ample.DataLevel;
import org.apache.accumulo.core.trace.TraceUtil;
import org.apache.accumulo.core.util.Pair;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.fs.VolumeManager;
import org.apache.accumulo.server.log.WalStateManager;
import org.apache.accumulo.server.log.WalStateManager.WalMarkerException;
import org.apache.accumulo.server.log.WalStateManager.WalState;
import org.apache.accumulo.server.manager.LiveTServerSet;
import org.apache.accumulo.server.manager.state.TabletStateStore;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Iterators;

import io.opentelemetry.api.trace.Span;
import io.opentelemetry.context.Scope;

public class GarbageCollectWriteAheadLogs {
  private static final Logger log = LoggerFactory.getLogger(GarbageCollectWriteAheadLogs.class);

  private final ServerContext context;
  private final VolumeManager fs;
  private final boolean useTrash;
  private final LiveTServerSet liveServers;
  private final WalStateManager walMarker;
  private final Iterable<TabletLocationState> store;

  /**
   * Creates a new GC WAL object.
   *
   * @param context the collection server's context
   * @param fs volume manager to use
   * @param useTrash true to move files to trash rather than delete them
   */
  GarbageCollectWriteAheadLogs(final ServerContext context, final VolumeManager fs,
      final LiveTServerSet liveServers, boolean useTrash) {
    this.context = context;
    this.fs = fs;
    this.useTrash = useTrash;
    this.liveServers = liveServers;
    this.walMarker = new WalStateManager(context);
    this.store = () -> Iterators.concat(
        TabletStateStore.getStoreForLevel(DataLevel.ROOT, context).iterator(),
        TabletStateStore.getStoreForLevel(DataLevel.METADATA, context).iterator(),
        TabletStateStore.getStoreForLevel(DataLevel.USER, context).iterator());
  }

  /**
   * Creates a new GC WAL object. Meant for testing -- allows mocked objects.
   *
   * @param context the collection server's context
   * @param fs volume manager to use
   * @param useTrash true to move files to trash rather than delete them
   * @param liveTServerSet a started LiveTServerSet instance
   */
  @VisibleForTesting
  GarbageCollectWriteAheadLogs(ServerContext context, VolumeManager fs, boolean useTrash,
      LiveTServerSet liveTServerSet, WalStateManager walMarker,
      Iterable<TabletLocationState> store) {
    this.context = context;
    this.fs = fs;
    this.useTrash = useTrash;
    this.liveServers = liveTServerSet;
    this.walMarker = walMarker;
    this.store = store;
  }

  public void collect(GCStatus status) {
    try {
      long count;
      long fileScanStop;
      Map<TServerInstance,Set<UUID>> logsByServer;
      Map<UUID,Pair<WalState,Path>> logsState;
      Map<UUID,Path> recoveryLogs;

      Span span = TraceUtil.startSpan(this.getClass(), "getCandidates");
      try (Scope scope = span.makeCurrent()) {
        status.currentLog.started = System.currentTimeMillis();

        recoveryLogs = getSortedWALogs();

        logsByServer = new HashMap<>();
        logsState = new HashMap<>();
        // Scan for log file info first: the order is important
        // Consider:
        // * get live servers
        // * new server gets a lock, creates a log
        // * get logs
        // * the log appears to belong to a dead server
        count = getCurrent(logsByServer, logsState);
        fileScanStop = System.currentTimeMillis();

        log.info(String.format("Fetched %d files for %d servers in %.2f seconds", count,
            logsByServer.size(), (fileScanStop - status.currentLog.started) / 1000.));
        status.currentLog.candidates = count;
      } catch (Exception e) {
        TraceUtil.setException(span, e, true);
        throw e;
      } finally {
        span.end();
      }

      // now it's safe to get the liveServers
      liveServers.scanServers();
      Set<TServerInstance> currentServers = liveServers.getCurrentServers();

      Map<UUID,TServerInstance> uuidToTServer;
      Span span2 = TraceUtil.startSpan(this.getClass(), "removeEntriesInUse");
      try (Scope scope = span2.makeCurrent()) {
        uuidToTServer = removeEntriesInUse(logsByServer, currentServers, logsState, recoveryLogs);
        count = uuidToTServer.size();
      } catch (Exception ex) {
        log.error("Unable to scan metadata table", ex);
        TraceUtil.setException(span2, ex, false);
        return;
      } finally {
        span2.end();
      }

      long logEntryScanStop = System.currentTimeMillis();
      log.info(String.format("%d log entries scanned in %.2f seconds", count,
          (logEntryScanStop - fileScanStop) / 1000.));

      long removeStop;
      Span span4 = TraceUtil.startSpan(this.getClass(), "removeFiles");
      try (Scope scope = span4.makeCurrent()) {

        logsState.keySet().retainAll(uuidToTServer.keySet());
        count = removeFiles(logsState.values(), status);

        removeStop = System.currentTimeMillis();
        log.info(String.format("%d total logs removed from %d servers in %.2f seconds", count,
            logsByServer.size(), (removeStop - logEntryScanStop) / 1000.));

        count = removeFiles(recoveryLogs.values());
        log.info("{} recovery logs removed", count);
      } catch (Exception e) {
        TraceUtil.setException(span4, e, true);
        throw e;
      } finally {
        span4.end();
      }

      Span span5 = TraceUtil.startSpan(this.getClass(), "removeMarkers");
      try (Scope scope = span5.makeCurrent()) {
        count = removeTabletServerMarkers(uuidToTServer, logsByServer, currentServers);
        long removeMarkersStop = System.currentTimeMillis();
        log.info(String.format("%d markers removed in %.2f seconds", count,
            (removeMarkersStop - removeStop) / 1000.));
      } catch (Exception e) {
        TraceUtil.setException(span5, e, true);
        throw e;
      } finally {
        span5.end();
      }

      status.currentLog.finished = removeStop;
      status.lastLog = status.currentLog;
      status.currentLog = new GcCycleStats();

    } catch (Exception e) {
      log.error("exception occurred while garbage collecting write ahead logs", e);
    }
  }

  private long removeTabletServerMarkers(Map<UUID,TServerInstance> uidMap,
      Map<TServerInstance,Set<UUID>> candidates, Set<TServerInstance> liveServers) {
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
          log.info("Error removing znode for " + entry.getKey() + " " + ex);
        }
      }
    }
    return result;
  }

  private long removeFile(Path path) {
    try {
      if (!useTrash || !fs.moveToTrash(path)) {
        fs.deleteRecursively(path);
      }
      return 1;
    } catch (FileNotFoundException ex) {
      // ignored
    } catch (IOException ex) {
      log.error("Unable to delete wal {}", path, ex);
    }

    return 0;
  }

  private long removeFiles(Collection<Pair<WalState,Path>> collection, final GCStatus status) {
    for (Pair<WalState,Path> stateFile : collection) {
      Path path = stateFile.getSecond();
      log.debug("Removing {} WAL {}", stateFile.getFirst(), path);
      status.currentLog.deleted += removeFile(path);
    }
    return status.currentLog.deleted;
  }

  private long removeFiles(Collection<Path> values) {
    long count = 0;
    for (Path path : values) {
      log.debug("Removing recovery log {}", path);
      count += removeFile(path);
    }
    return count;
  }

  private UUID path2uuid(Path path) {
    return UUID.fromString(path.getName());
  }

  private Map<UUID,TServerInstance> removeEntriesInUse(Map<TServerInstance,Set<UUID>> candidates,
      Set<TServerInstance> liveServers, Map<UUID,Pair<WalState,Path>> logsState,
      Map<UUID,Path> recoveryLogs) {

    Map<UUID,TServerInstance> result = new HashMap<>();
    for (Entry<TServerInstance,Set<UUID>> entry : candidates.entrySet()) {
      for (UUID id : entry.getValue()) {
        if (result.put(id, entry.getKey()) != null) {
          throw new IllegalArgumentException("WAL " + id + " owned by multiple tservers");
        }
      }
    }

    // remove any entries if there's a log reference (recovery hasn't finished)
    for (TabletLocationState state : store) {
      // Tablet is still assigned to a dead server. Manager has moved markers and reassigned it
      // Easiest to just ignore all the WALs for the dead server.
      if (state.getState(liveServers) == TabletState.ASSIGNED_TO_DEAD_SERVER) {
        Set<UUID> idsToIgnore = candidates.remove(state.current);
        if (idsToIgnore != null) {
          result.keySet().removeAll(idsToIgnore);
          recoveryLogs.keySet().removeAll(idsToIgnore);
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
            result.keySet().removeAll(idsToIgnore);
            recoveryLogs.keySet().removeAll(idsToIgnore);
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

        recoveryLogs.keySet().removeAll(idsForServer);
      }
    }
    return result;
  }

  /**
   * Scans log markers. The map passed in is populated with the log ids.
   *
   * @param logsByServer map of dead server to log file entries
   * @return total number of log files
   */
  private long getCurrent(Map<TServerInstance,Set<UUID>> logsByServer,
      Map<UUID,Pair<WalState,Path>> logState) throws Exception {

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

  /**
   * Looks for write-ahead logs in recovery directories.
   *
   * @return map of log uuids to paths
   */
  protected Map<UUID,Path> getSortedWALogs() throws IOException {
    Map<UUID,Path> result = new HashMap<>();

    for (String dir : context.getRecoveryDirs()) {
      Path recoveryDir = new Path(dir);
      if (fs.exists(recoveryDir)) {
        for (FileStatus status : fs.listStatus(recoveryDir)) {
          try {
            UUID logId = path2uuid(status.getPath());
            result.put(logId, status.getPath());
          } catch (IllegalArgumentException iae) {
            log.debug("Ignoring file " + status.getPath() + " because it doesn't look like a uuid");
          }

        }
      }
    }
    return result;
  }
}

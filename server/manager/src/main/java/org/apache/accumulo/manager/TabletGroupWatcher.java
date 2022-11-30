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
package org.apache.accumulo.manager;

import static java.lang.Math.min;
import static org.apache.accumulo.core.util.UtilWaitThread.sleepUninterruptibly;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.RowIterator;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.gc.ReferenceFile;
import org.apache.accumulo.core.logging.TabletLogger;
import org.apache.accumulo.core.manager.state.tables.TableState;
import org.apache.accumulo.core.manager.thrift.ManagerState;
import org.apache.accumulo.core.master.thrift.TabletServerStatus;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.metadata.RootTable;
import org.apache.accumulo.core.metadata.StoredTabletFile;
import org.apache.accumulo.core.metadata.TServerInstance;
import org.apache.accumulo.core.metadata.TabletLocationState;
import org.apache.accumulo.core.metadata.TabletLocationState.BadLocationStateException;
import org.apache.accumulo.core.metadata.TabletState;
import org.apache.accumulo.core.metadata.schema.Ample;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.ChoppedColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.CurrentLocationColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.DataFileColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.ExternalCompactionColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.FutureLocationColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.ServerColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.TabletColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataTime;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.tabletserver.thrift.NotServingTabletException;
import org.apache.accumulo.core.util.threads.Threads.AccumuloDaemonThread;
import org.apache.accumulo.manager.Manager.TabletGoalState;
import org.apache.accumulo.manager.state.MergeStats;
import org.apache.accumulo.manager.state.TableCounts;
import org.apache.accumulo.manager.state.TableStats;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.conf.TableConfiguration;
import org.apache.accumulo.server.gc.AllVolumesDirectory;
import org.apache.accumulo.server.log.WalStateManager;
import org.apache.accumulo.server.log.WalStateManager.WalMarkerException;
import org.apache.accumulo.server.manager.LiveTServerSet.TServerConnection;
import org.apache.accumulo.server.manager.state.Assignment;
import org.apache.accumulo.server.manager.state.ClosableIterator;
import org.apache.accumulo.server.manager.state.DistributedStoreException;
import org.apache.accumulo.server.manager.state.MergeInfo;
import org.apache.accumulo.server.manager.state.MergeState;
import org.apache.accumulo.server.manager.state.TabletStateStore;
import org.apache.accumulo.server.tablets.TabletTime;
import org.apache.accumulo.server.util.MetadataTableUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.thrift.TException;

import com.google.common.collect.ImmutableSortedSet;
import com.google.common.collect.Iterators;

abstract class TabletGroupWatcher extends AccumuloDaemonThread {
  // Constants used to make sure assignment logging isn't excessive in quantity or size

  private final Manager manager;
  private final TabletStateStore store;
  private final TabletGroupWatcher dependentWatcher;
  final TableStats stats = new TableStats();
  private SortedSet<TServerInstance> lastScanServers = Collections.emptySortedSet();

  TabletGroupWatcher(Manager manager, TabletStateStore store, TabletGroupWatcher dependentWatcher) {
    super("Watching " + store.name());
    this.manager = manager;
    this.store = store;
    this.dependentWatcher = dependentWatcher;
  }

  /** Should this {@code TabletGroupWatcher} suspend tablets? */
  abstract boolean canSuspendTablets();

  Map<TableId,TableCounts> getStats() {
    return stats.getLast();
  }

  TableCounts getStats(TableId tableId) {
    return stats.getLast(tableId);
  }

  /**
   * True if the collection of live tservers specified in 'candidates' hasn't changed since the last
   * time an assignment scan was started.
   */
  synchronized boolean isSameTserversAsLastScan(Set<TServerInstance> candidates) {
    return candidates.equals(lastScanServers);
  }

  /**
   * Collection of data structures used to track Tablet assignments
   */
  private static class TabletLists {
    private final List<Assignment> assignments = new ArrayList<>();
    private final List<Assignment> assigned = new ArrayList<>();
    private final List<TabletLocationState> assignedToDeadServers = new ArrayList<>();
    private final List<TabletLocationState> suspendedToGoneServers = new ArrayList<>();
    private final Map<KeyExtent,TServerInstance> unassigned = new HashMap<>();
    private final Map<TServerInstance,List<Path>> logsForDeadServers = new TreeMap<>();
    // read only lists of tablet servers
    private final SortedMap<TServerInstance,TabletServerStatus> currentTServers;
    private final SortedMap<TServerInstance,TabletServerStatus> destinations;

    public TabletLists(Manager m, SortedMap<TServerInstance,TabletServerStatus> curTServers) {
      var destinationsMod = new TreeMap<>(curTServers);
      // Don't move tablets to servers that are shutting down
      destinationsMod.keySet().removeAll(m.serversToShutdown);
      this.destinations = Collections.unmodifiableSortedMap(destinationsMod);
      this.currentTServers = Collections.unmodifiableSortedMap(curTServers);
    }

    public void reset() {
      assignments.clear();
      assigned.clear();
      assignedToDeadServers.clear();
      suspendedToGoneServers.clear();
      unassigned.clear();
    }
  }

  @Override
  public void run() {
    int[] oldCounts = new int[TabletState.values().length];
    EventCoordinator.Listener eventListener = this.manager.nextEvent.getListener();

    WalStateManager wals = new WalStateManager(manager.getContext());

    while (manager.stillManager()) {
      // slow things down a little, otherwise we spam the logs when there are many wake-up events
      sleepUninterruptibly(100, TimeUnit.MILLISECONDS);

      int totalUnloaded = 0;
      int unloaded = 0;
      ClosableIterator<TabletLocationState> iter = null;
      try {
        Map<TableId,MergeStats> mergeStatsCache = new HashMap<>();
        Map<TableId,MergeStats> currentMerges = new HashMap<>();
        for (MergeInfo merge : manager.merges()) {
          if (merge.getExtent() != null) {
            currentMerges.put(merge.getExtent().tableId(), new MergeStats(merge));
          }
        }

        // Get the current status for the current list of tservers
        SortedMap<TServerInstance,TabletServerStatus> currentTServers = new TreeMap<>();
        for (TServerInstance entry : manager.tserverSet.getCurrentServers()) {
          currentTServers.put(entry, manager.tserverStatus.get(entry));
        }

        if (currentTServers.isEmpty()) {
          eventListener.waitForEvents(Manager.TIME_TO_WAIT_BETWEEN_SCANS);
          synchronized (this) {
            lastScanServers = Collections.emptySortedSet();
          }
          continue;
        }

        TabletLists tLists = new TabletLists(manager, currentTServers);

        ManagerState managerState = manager.getManagerState();
        int[] counts = new int[TabletState.values().length];
        stats.begin();
        // Walk through the tablets in our store, and work tablets
        // towards their goal
        iter = store.iterator();
        while (iter.hasNext()) {
          TabletLocationState tls = iter.next();
          if (tls == null) {
            continue;
          }

          // ignore entries for tables that do not exist in zookeeper
          if (manager.getTableManager().getTableState(tls.extent.tableId()) == null) {
            continue;
          }

          // Don't overwhelm the tablet servers with work
          if (tLists.unassigned.size() + unloaded
              > Manager.MAX_TSERVER_WORK_CHUNK * currentTServers.size()) {
            flushChanges(tLists, wals);
            tLists.reset();
            unloaded = 0;
            eventListener.waitForEvents(Manager.TIME_TO_WAIT_BETWEEN_SCANS);
          }
          TableId tableId = tls.extent.tableId();
          TableConfiguration tableConf = manager.getContext().getTableConfiguration(tableId);

          MergeStats mergeStats = mergeStatsCache.computeIfAbsent(tableId, k -> {
            var mStats = currentMerges.get(k);
            return mStats != null ? mStats : new MergeStats(new MergeInfo());
          });
          TabletGoalState goal = manager.getGoalState(tls, mergeStats.getMergeInfo());
          TServerInstance location = tls.getLocation();
          TabletState state = tls.getState(currentTServers.keySet());

          TabletLogger.missassigned(tls.extent, goal.toString(), state.toString(), tls.future,
              tls.current, tls.walogs.size());

          stats.update(tableId, state);
          mergeStats.update(tls.extent, state, tls.chopped, !tls.walogs.isEmpty());
          sendChopRequest(mergeStats.getMergeInfo(), state, tls);
          sendSplitRequest(mergeStats.getMergeInfo(), state, tls);

          // Always follow through with assignments
          if (state == TabletState.ASSIGNED) {
            goal = TabletGoalState.HOSTED;
          }

          // if we are shutting down all the tabletservers, we have to do it in order
          if ((goal == TabletGoalState.SUSPENDED && state == TabletState.HOSTED)
              && manager.serversToShutdown.equals(currentTServers.keySet())) {
            if (dependentWatcher != null && dependentWatcher.assignedOrHosted() > 0) {
              goal = TabletGoalState.HOSTED;
            }
          }

          if (goal == TabletGoalState.HOSTED) {
            if ((state != TabletState.HOSTED && !tls.walogs.isEmpty())
                && manager.recoveryManager.recoverLogs(tls.extent, tls.walogs)) {
              continue;
            }
            switch (state) {
              case HOSTED:
                if (location.equals(manager.migrations.get(tls.extent))) {
                  manager.migrations.remove(tls.extent);
                }
                break;
              case ASSIGNED_TO_DEAD_SERVER:
                hostDeadTablet(tLists, tls, location, wals);
                break;
              case SUSPENDED:
                hostSuspendedTablet(tLists, tls, location, tableConf);
                break;
              case UNASSIGNED:
                hostUnassignedTablet(tLists, tls.extent, location);
                break;
              case ASSIGNED:
                // Send another reminder
                tLists.assigned.add(new Assignment(tls.extent, tls.future));
                break;
            }
          } else {
            switch (state) {
              case SUSPENDED:
                // Request a move to UNASSIGNED, so as to allow balancing to continue.
                tLists.suspendedToGoneServers.add(tls);
                cancelOfflineTableMigrations(tls.extent);
                break;
              case UNASSIGNED:
                cancelOfflineTableMigrations(tls.extent);
                break;
              case ASSIGNED_TO_DEAD_SERVER:
                unassignDeadTablet(tLists, tls, wals);
                break;
              case HOSTED:
                TServerConnection client = manager.tserverSet.getConnection(location);
                if (client != null) {
                  client.unloadTablet(manager.managerLock, tls.extent, goal.howUnload(),
                      manager.getSteadyTime());
                  unloaded++;
                  totalUnloaded++;
                } else {
                  Manager.log.warn("Could not connect to server {}", location);
                }
                break;
              case ASSIGNED:
                break;
            }
          }
          counts[state.ordinal()]++;
        }

        flushChanges(tLists, wals);

        // provide stats after flushing changes to avoid race conditions w/ delete table
        stats.end(managerState);

        // Report changes
        for (TabletState state : TabletState.values()) {
          int i = state.ordinal();
          if (counts[i] > 0 && counts[i] != oldCounts[i]) {
            manager.nextEvent.event("[%s]: %d tablets are %s", store.name(), counts[i],
                state.name());
          }
        }
        Manager.log.debug(String.format("[%s]: scan time %.2f seconds", store.name(),
            stats.getScanTime() / 1000.));
        oldCounts = counts;
        if (totalUnloaded > 0) {
          manager.nextEvent.event("[%s]: %d tablets unloaded", store.name(), totalUnloaded);
        }

        updateMergeState(mergeStatsCache);

        synchronized (this) {
          lastScanServers = ImmutableSortedSet.copyOf(currentTServers.keySet());
        }
        if (manager.tserverSet.getCurrentServers().equals(currentTServers.keySet())) {
          Manager.log.debug(String.format("[%s] sleeping for %.2f seconds", store.name(),
              Manager.TIME_TO_WAIT_BETWEEN_SCANS / 1000.));
          eventListener.waitForEvents(Manager.TIME_TO_WAIT_BETWEEN_SCANS);
        } else {
          Manager.log.info("Detected change in current tserver set, re-running state machine.");
        }
      } catch (Exception ex) {
        Manager.log.error("Error processing table state for store " + store.name(), ex);
        if (ex.getCause() != null && ex.getCause() instanceof BadLocationStateException) {
          repairMetadata(((BadLocationStateException) ex.getCause()).getEncodedEndRow());
        } else {
          sleepUninterruptibly(Manager.WAIT_BETWEEN_ERRORS, TimeUnit.MILLISECONDS);
        }
      } finally {
        if (iter != null) {
          try {
            iter.close();
          } catch (IOException ex) {
            Manager.log.warn("Error closing TabletLocationState iterator: " + ex, ex);
          }
        }
      }
    }
  }

  private void unassignDeadTablet(TabletLists tLists, TabletLocationState tls, WalStateManager wals)
      throws WalMarkerException {
    tLists.assignedToDeadServers.add(tls);
    if (!tLists.logsForDeadServers.containsKey(tls.futureOrCurrent())) {
      tLists.logsForDeadServers.put(tls.futureOrCurrent(),
          wals.getWalsInUse(tls.futureOrCurrent()));
    }
  }

  private void hostUnassignedTablet(TabletLists tLists, KeyExtent tablet,
      TServerInstance location) {
    // maybe it's a finishing migration
    TServerInstance dest = manager.migrations.get(tablet);
    if (dest != null) {
      // if destination is still good, assign it
      if (tLists.destinations.containsKey(dest)) {
        tLists.assignments.add(new Assignment(tablet, dest));
      } else {
        // get rid of this migration
        manager.migrations.remove(tablet);
        tLists.unassigned.put(tablet, location);
      }
    } else {
      tLists.unassigned.put(tablet, location);
    }
  }

  private void hostSuspendedTablet(TabletLists tLists, TabletLocationState tls,
      TServerInstance location, TableConfiguration tableConf) {
    if (manager.getSteadyTime() - tls.suspend.suspensionTime
        < tableConf.getTimeInMillis(Property.TABLE_SUSPEND_DURATION)) {
      // Tablet is suspended. See if its tablet server is back.
      TServerInstance returnInstance = null;
      Iterator<TServerInstance> find = tLists.destinations
          .tailMap(new TServerInstance(tls.suspend.server, " ")).keySet().iterator();
      if (find.hasNext()) {
        TServerInstance found = find.next();
        if (found.getHostAndPort().equals(tls.suspend.server)) {
          returnInstance = found;
        }
      }

      // Old tablet server is back. Return this tablet to its previous owner.
      if (returnInstance != null) {
        tLists.assignments.add(new Assignment(tls.extent, returnInstance));
      }
      // else - tablet server not back. Don't ask for a new assignment right now.

    } else {
      // Treat as unassigned, ask for a new assignment.
      tLists.unassigned.put(tls.extent, location);
    }
  }

  private void hostDeadTablet(TabletLists tLists, TabletLocationState tls, TServerInstance location,
      WalStateManager wals) throws WalMarkerException {
    tLists.assignedToDeadServers.add(tls);
    if (location.equals(manager.migrations.get(tls.extent))) {
      manager.migrations.remove(tls.extent);
    }
    TServerInstance tserver = tls.futureOrCurrent();
    if (!tLists.logsForDeadServers.containsKey(tserver)) {
      tLists.logsForDeadServers.put(tserver, wals.getWalsInUse(tserver));
    }
  }

  private void cancelOfflineTableMigrations(KeyExtent extent) {
    TServerInstance dest = manager.migrations.get(extent);
    TableState tableState = manager.getTableManager().getTableState(extent.tableId());
    if (dest != null && tableState == TableState.OFFLINE) {
      manager.migrations.remove(extent);
    }
  }

  private void repairMetadata(Text row) {
    Manager.log.debug("Attempting repair on {}", row);
    // ACCUMULO-2261 if a dying tserver writes a location before its lock information propagates, it
    // may cause duplicate assignment.
    // Attempt to find the dead server entry and remove it.
    try {
      Map<Key,Value> future = new HashMap<>();
      Map<Key,Value> assigned = new HashMap<>();
      KeyExtent extent = KeyExtent.fromMetaRow(row);
      String table = MetadataTable.NAME;
      if (extent.isMeta()) {
        table = RootTable.NAME;
      }
      Scanner scanner = manager.getContext().createScanner(table, Authorizations.EMPTY);
      scanner.fetchColumnFamily(CurrentLocationColumnFamily.NAME);
      scanner.fetchColumnFamily(FutureLocationColumnFamily.NAME);
      scanner.setRange(new Range(row));
      for (Entry<Key,Value> entry : scanner) {
        if (entry.getKey().getColumnFamily().equals(CurrentLocationColumnFamily.NAME)) {
          assigned.put(entry.getKey(), entry.getValue());
        } else if (entry.getKey().getColumnFamily().equals(FutureLocationColumnFamily.NAME)) {
          future.put(entry.getKey(), entry.getValue());
        }
      }
      if (!future.isEmpty() && !assigned.isEmpty()) {
        Manager.log.warn("Found a tablet assigned and hosted, attempting to repair");
      } else if (future.size() > 1 && assigned.isEmpty()) {
        Manager.log.warn("Found a tablet assigned to multiple servers, attempting to repair");
      } else if (future.isEmpty() && assigned.size() > 1) {
        Manager.log.warn("Found a tablet hosted on multiple servers, attempting to repair");
      } else {
        Manager.log.info("Attempted a repair, but nothing seems to be obviously wrong. {} {}",
            assigned, future);
        return;
      }
      Iterator<Entry<Key,Value>> iter =
          Iterators.concat(future.entrySet().iterator(), assigned.entrySet().iterator());
      while (iter.hasNext()) {
        Entry<Key,Value> entry = iter.next();
        TServerInstance alive = manager.tserverSet.find(entry.getValue().toString());
        if (alive == null) {
          Manager.log.info("Removing entry  {}", entry);
          BatchWriter bw = manager.getContext().createBatchWriter(table);
          Mutation m = new Mutation(entry.getKey().getRow());
          m.putDelete(entry.getKey().getColumnFamily(), entry.getKey().getColumnQualifier());
          bw.addMutation(m);
          bw.close();
          return;
        }
      }
      Manager.log.error(
          "Metadata table is inconsistent at {} and all assigned/future tservers are still online.",
          row);
    } catch (Exception e) {
      Manager.log.error("Error attempting repair of metadata " + row + ": " + e, e);
    }
  }

  private int assignedOrHosted() {
    int result = 0;
    for (TableCounts counts : stats.getLast().values()) {
      result += counts.assigned() + counts.hosted();
    }
    return result;
  }

  private void sendSplitRequest(MergeInfo info, TabletState state, TabletLocationState tls) {
    // Already split?
    if (!info.getState().equals(MergeState.SPLITTING)) {
      return;
    }
    // Merges don't split
    if (!info.isDelete()) {
      return;
    }
    // Online and ready to split?
    if (!state.equals(TabletState.HOSTED)) {
      return;
    }
    // Does this extent cover the end points of the delete?
    KeyExtent range = info.getExtent();
    if (tls.extent.overlaps(range)) {
      for (Text splitPoint : new Text[] {range.prevEndRow(), range.endRow()}) {
        if (splitPoint == null) {
          continue;
        }
        if (!tls.extent.contains(splitPoint)) {
          continue;
        }
        if (splitPoint.equals(tls.extent.endRow())) {
          continue;
        }
        if (splitPoint.equals(tls.extent.prevEndRow())) {
          continue;
        }
        try {
          TServerConnection conn;
          conn = manager.tserverSet.getConnection(tls.current);
          if (conn != null) {
            Manager.log.info("Asking {} to split {} at {}", tls.current, tls.extent, splitPoint);
            conn.splitTablet(tls.extent, splitPoint);
          } else {
            Manager.log.warn("Not connected to server {}", tls.current);
          }
        } catch (NotServingTabletException e) {
          Manager.log.debug("Error asking tablet server to split a tablet: ", e);
        } catch (Exception e) {
          Manager.log.warn("Error asking tablet server to split a tablet: ", e);
        }
      }
    }
  }

  private void sendChopRequest(MergeInfo info, TabletState state, TabletLocationState tls) {
    // Don't bother if we're in the wrong state
    if (!info.getState().equals(MergeState.WAITING_FOR_CHOPPED)) {
      return;
    }
    // Tablet must be online
    if (!state.equals(TabletState.HOSTED)) {
      return;
    }
    // Tablet isn't already chopped
    if (tls.chopped) {
      return;
    }
    // Tablet ranges intersect
    if (info.needsToBeChopped(tls.extent)) {
      TServerConnection conn;
      try {
        conn = manager.tserverSet.getConnection(tls.current);
        if (conn != null) {
          Manager.log.info("Asking {} to chop {}", tls.current, tls.extent);
          conn.chop(manager.managerLock, tls.extent);
        } else {
          Manager.log.warn("Could not connect to server {}", tls.current);
        }
      } catch (TException e) {
        Manager.log.warn("Communications error asking tablet server to chop a tablet");
      }
    }
  }

  private void updateMergeState(Map<TableId,MergeStats> mergeStatsCache) {
    for (MergeStats stats : mergeStatsCache.values()) {
      try {
        MergeState update = stats.nextMergeState(manager.getContext(), manager);
        // when next state is MERGING, its important to persist this before
        // starting the merge... the verification check that is done before
        // moving into the merging state could fail if merge starts but does
        // not finish
        if (update == MergeState.COMPLETE) {
          update = MergeState.NONE;
        }
        if (update != stats.getMergeInfo().getState()) {
          manager.setMergeState(stats.getMergeInfo(), update);
        }

        if (update == MergeState.MERGING) {
          try {
            if (stats.getMergeInfo().isDelete()) {
              deleteTablets(stats.getMergeInfo());
            } else {
              mergeMetadataRecords(stats.getMergeInfo());
            }
            update = MergeState.COMPLETE;
            manager.setMergeState(stats.getMergeInfo(), update);
          } catch (Exception ex) {
            Manager.log.error("Unable merge metadata table records", ex);
          }
        }
      } catch (Exception ex) {
        Manager.log.error(
            "Unable to update merge state for merge " + stats.getMergeInfo().getExtent(), ex);
      }
    }
  }

  private void deleteTablets(MergeInfo info) throws AccumuloException {
    KeyExtent extent = info.getExtent();
    String targetSystemTable = extent.isMeta() ? RootTable.NAME : MetadataTable.NAME;
    Manager.log.debug("Deleting tablets for {}", extent);
    MetadataTime metadataTime = null;
    KeyExtent followingTablet = null;
    if (extent.endRow() != null) {
      Key nextExtent = new Key(extent.endRow()).followingKey(PartialKey.ROW);
      followingTablet =
          getHighTablet(new KeyExtent(extent.tableId(), nextExtent.getRow(), extent.endRow()));
      Manager.log.debug("Found following tablet {}", followingTablet);
    }
    try {
      AccumuloClient client = manager.getContext();
      ServerContext context = manager.getContext();
      Ample ample = context.getAmple();
      Text start = extent.prevEndRow();
      if (start == null) {
        start = new Text();
      }
      Manager.log.debug("Making file deletion entries for {}", extent);
      Range deleteRange = new Range(TabletsSection.encodeRow(extent.tableId(), start), false,
          TabletsSection.encodeRow(extent.tableId(), extent.endRow()), true);
      Scanner scanner = client.createScanner(targetSystemTable, Authorizations.EMPTY);
      scanner.setRange(deleteRange);
      ServerColumnFamily.DIRECTORY_COLUMN.fetch(scanner);
      ServerColumnFamily.TIME_COLUMN.fetch(scanner);
      scanner.fetchColumnFamily(DataFileColumnFamily.NAME);
      scanner.fetchColumnFamily(CurrentLocationColumnFamily.NAME);
      Set<ReferenceFile> datafilesAndDirs = new TreeSet<>();
      for (Entry<Key,Value> entry : scanner) {
        Key key = entry.getKey();
        if (key.compareColumnFamily(DataFileColumnFamily.NAME) == 0) {
          var stf = new StoredTabletFile(key.getColumnQualifierData().toString());
          datafilesAndDirs.add(new ReferenceFile(stf.getTableId(), stf.getMetaUpdateDelete()));
          if (datafilesAndDirs.size() > 1000) {
            ample.putGcFileAndDirCandidates(extent.tableId(), datafilesAndDirs);
            datafilesAndDirs.clear();
          }
        } else if (ServerColumnFamily.TIME_COLUMN.hasColumns(key)) {
          metadataTime = MetadataTime.parse(entry.getValue().toString());
        } else if (key.compareColumnFamily(CurrentLocationColumnFamily.NAME) == 0) {
          throw new IllegalStateException(
              "Tablet " + key.getRow() + " is assigned during a merge!");
        } else if (ServerColumnFamily.DIRECTORY_COLUMN.hasColumns(key)) {
          var allVolumesDirectory =
              new AllVolumesDirectory(extent.tableId(), entry.getValue().toString());
          datafilesAndDirs.add(allVolumesDirectory);
          if (datafilesAndDirs.size() > 1000) {
            ample.putGcFileAndDirCandidates(extent.tableId(), datafilesAndDirs);
            datafilesAndDirs.clear();
          }
        }
      }
      ample.putGcFileAndDirCandidates(extent.tableId(), datafilesAndDirs);
      BatchWriter bw = client.createBatchWriter(targetSystemTable);
      try {
        deleteTablets(info, deleteRange, bw, client);
      } finally {
        bw.close();
      }

      if (followingTablet != null) {
        Manager.log.debug("Updating prevRow of {} to {}", followingTablet, extent.prevEndRow());
        bw = client.createBatchWriter(targetSystemTable);
        try {
          Mutation m = new Mutation(followingTablet.toMetaRow());
          TabletColumnFamily.PREV_ROW_COLUMN.put(m,
              TabletColumnFamily.encodePrevEndRow(extent.prevEndRow()));
          ChoppedColumnFamily.CHOPPED_COLUMN.putDelete(m);
          bw.addMutation(m);
          bw.flush();
        } finally {
          bw.close();
        }
      } else {
        // Recreate the default tablet to hold the end of the table
        MetadataTableUtil.addTablet(new KeyExtent(extent.tableId(), null, extent.prevEndRow()),
            ServerColumnFamily.DEFAULT_TABLET_DIR_NAME, manager.getContext(),
            metadataTime.getType(), manager.managerLock);
      }
    } catch (RuntimeException | TableNotFoundException ex) {
      throw new AccumuloException(ex);
    }
  }

  private void mergeMetadataRecords(MergeInfo info) throws AccumuloException {
    KeyExtent range = info.getExtent();
    Manager.log.debug("Merging metadata for {}", range);
    KeyExtent stop = getHighTablet(range);
    Manager.log.debug("Highest tablet is {}", stop);
    Value firstPrevRowValue = null;
    Text stopRow = stop.toMetaRow();
    Text start = range.prevEndRow();
    if (start == null) {
      start = new Text();
    }
    Range scanRange =
        new Range(TabletsSection.encodeRow(range.tableId(), start), false, stopRow, false);
    String targetSystemTable = MetadataTable.NAME;
    if (range.isMeta()) {
      targetSystemTable = RootTable.NAME;
    }

    AccumuloClient client = manager.getContext();

    try (BatchWriter bw = client.createBatchWriter(targetSystemTable)) {
      long fileCount = 0;
      // Make file entries in highest tablet
      Scanner scanner = client.createScanner(targetSystemTable, Authorizations.EMPTY);
      scanner.setRange(scanRange);
      TabletColumnFamily.PREV_ROW_COLUMN.fetch(scanner);
      ServerColumnFamily.TIME_COLUMN.fetch(scanner);
      ServerColumnFamily.DIRECTORY_COLUMN.fetch(scanner);
      scanner.fetchColumnFamily(DataFileColumnFamily.NAME);
      Mutation m = new Mutation(stopRow);
      MetadataTime maxLogicalTime = null;
      for (Entry<Key,Value> entry : scanner) {
        Key key = entry.getKey();
        Value value = entry.getValue();
        if (key.getColumnFamily().equals(DataFileColumnFamily.NAME)) {
          m.put(key.getColumnFamily(), key.getColumnQualifier(), value);
          fileCount++;
        } else if (TabletColumnFamily.PREV_ROW_COLUMN.hasColumns(key)
            && firstPrevRowValue == null) {
          Manager.log.debug("prevRow entry for lowest tablet is {}", value);
          firstPrevRowValue = new Value(value);
        } else if (ServerColumnFamily.TIME_COLUMN.hasColumns(key)) {
          maxLogicalTime =
              TabletTime.maxMetadataTime(maxLogicalTime, MetadataTime.parse(value.toString()));
        } else if (ServerColumnFamily.DIRECTORY_COLUMN.hasColumns(key)) {
          var allVolumesDir = new AllVolumesDirectory(range.tableId(), value.toString());
          bw.addMutation(manager.getContext().getAmple().createDeleteMutation(allVolumesDir));
        }
      }

      // read the logical time from the last tablet in the merge range, it is not included in
      // the loop above
      scanner = client.createScanner(targetSystemTable, Authorizations.EMPTY);
      scanner.setRange(new Range(stopRow));
      ServerColumnFamily.TIME_COLUMN.fetch(scanner);
      scanner.fetchColumnFamily(ExternalCompactionColumnFamily.NAME);
      Set<String> extCompIds = new HashSet<>();
      for (Entry<Key,Value> entry : scanner) {
        if (ServerColumnFamily.TIME_COLUMN.hasColumns(entry.getKey())) {
          maxLogicalTime = TabletTime.maxMetadataTime(maxLogicalTime,
              MetadataTime.parse(entry.getValue().toString()));
        } else if (ExternalCompactionColumnFamily.NAME.equals(entry.getKey().getColumnFamily())) {
          extCompIds.add(entry.getKey().getColumnQualifierData().toString());
        }
      }

      if (maxLogicalTime != null) {
        ServerColumnFamily.TIME_COLUMN.put(m, new Value(maxLogicalTime.encode()));
      }

      // delete any entries for external compactions
      extCompIds.forEach(ecid -> m.putDelete(ExternalCompactionColumnFamily.STR_NAME, ecid));

      if (!m.getUpdates().isEmpty()) {
        bw.addMutation(m);
      }

      bw.flush();

      Manager.log.debug("Moved {} files to {}", fileCount, stop);

      if (firstPrevRowValue == null) {
        Manager.log.debug("tablet already merged");
        return;
      }

      stop = new KeyExtent(stop.tableId(), stop.endRow(),
          TabletColumnFamily.decodePrevEndRow(firstPrevRowValue));
      Mutation updatePrevRow = TabletColumnFamily.createPrevRowMutation(stop);
      Manager.log.debug("Setting the prevRow for last tablet: {}", stop);
      bw.addMutation(updatePrevRow);
      bw.flush();

      deleteTablets(info, scanRange, bw, client);

      // Clean-up the last chopped marker
      var m2 = new Mutation(stopRow);
      ChoppedColumnFamily.CHOPPED_COLUMN.putDelete(m2);
      bw.addMutation(m2);
      bw.flush();

    } catch (Exception ex) {
      throw new AccumuloException(ex);
    }
  }

  private void deleteTablets(MergeInfo info, Range scanRange, BatchWriter bw, AccumuloClient client)
      throws TableNotFoundException, MutationsRejectedException {
    Scanner scanner;
    Mutation m;
    // Delete everything in the other tablets
    // group all deletes into tablet into one mutation, this makes tablets
    // either disappear entirely or not all.. this is important for the case
    // where the process terminates in the loop below...
    scanner = client.createScanner(info.getExtent().isMeta() ? RootTable.NAME : MetadataTable.NAME,
        Authorizations.EMPTY);
    Manager.log.debug("Deleting range {}", scanRange);
    scanner.setRange(scanRange);
    RowIterator rowIter = new RowIterator(scanner);
    while (rowIter.hasNext()) {
      Iterator<Entry<Key,Value>> row = rowIter.next();
      m = null;
      while (row.hasNext()) {
        Entry<Key,Value> entry = row.next();
        Key key = entry.getKey();

        if (m == null) {
          m = new Mutation(key.getRow());
        }

        m.putDelete(key.getColumnFamily(), key.getColumnQualifier());
        Manager.log.debug("deleting entry {}", key);
      }
      bw.addMutation(m);
    }

    bw.flush();
  }

  private KeyExtent getHighTablet(KeyExtent range) throws AccumuloException {
    try {
      AccumuloClient client = manager.getContext();
      Scanner scanner = client.createScanner(range.isMeta() ? RootTable.NAME : MetadataTable.NAME,
          Authorizations.EMPTY);
      TabletColumnFamily.PREV_ROW_COLUMN.fetch(scanner);
      KeyExtent start = new KeyExtent(range.tableId(), range.endRow(), null);
      scanner.setRange(new Range(start.toMetaRow(), null));
      Iterator<Entry<Key,Value>> iterator = scanner.iterator();
      if (!iterator.hasNext()) {
        throw new AccumuloException("No last tablet for a merge " + range);
      }
      Entry<Key,Value> entry = iterator.next();
      KeyExtent highTablet = KeyExtent.fromMetaPrevRow(entry);
      if (!highTablet.tableId().equals(range.tableId())) {
        throw new AccumuloException("No last tablet for merge " + range + " " + highTablet);
      }
      return highTablet;
    } catch (Exception ex) {
      throw new AccumuloException("Unexpected failure finding the last tablet for a merge " + range,
          ex);
    }
  }

  private void handleDeadTablets(TabletLists tLists, WalStateManager wals)
      throws WalMarkerException, DistributedStoreException {
    var deadTablets = tLists.assignedToDeadServers;
    var deadLogs = tLists.logsForDeadServers;

    if (!deadTablets.isEmpty()) {
      int maxServersToShow = min(deadTablets.size(), 100);
      Manager.log.debug("{} assigned to dead servers: {}...", deadTablets.size(),
          deadTablets.subList(0, maxServersToShow));
      Manager.log.debug("logs for dead servers: {}", deadLogs);
      if (canSuspendTablets()) {
        store.suspend(deadTablets, deadLogs, manager.getSteadyTime());
      } else {
        store.unassign(deadTablets, deadLogs);
      }
      markDeadServerLogsAsClosed(wals, deadLogs);
      manager.nextEvent.event(
          "Marked %d tablets as suspended because they don't have current servers",
          deadTablets.size());
    }
    if (!tLists.suspendedToGoneServers.isEmpty()) {
      int maxServersToShow = min(deadTablets.size(), 100);
      Manager.log.debug(deadTablets.size() + " suspended to gone servers: "
          + deadTablets.subList(0, maxServersToShow) + "...");
      store.unsuspend(tLists.suspendedToGoneServers);
    }
  }

  private void getAssignmentsFromBalancer(TabletLists tLists,
      Map<KeyExtent,TServerInstance> unassigned) {
    if (!tLists.currentTServers.isEmpty()) {
      Map<KeyExtent,TServerInstance> assignedOut = new HashMap<>();
      manager.getAssignments(tLists.currentTServers, unassigned, assignedOut);
      for (Entry<KeyExtent,TServerInstance> assignment : assignedOut.entrySet()) {
        if (unassigned.containsKey(assignment.getKey())) {
          if (assignment.getValue() != null) {
            if (!tLists.currentTServers.containsKey(assignment.getValue())) {
              Manager.log.warn(
                  "balancer assigned {} to a tablet server that is not current {} ignoring",
                  assignment.getKey(), assignment.getValue());
              continue;
            }

            tLists.assignments.add(new Assignment(assignment.getKey(), assignment.getValue()));
          }
        } else {
          Manager.log.warn(
              "{} load balancer assigning tablet that was not nominated for assignment {}",
              store.name(), assignment.getKey());
        }
      }

      if (!unassigned.isEmpty() && assignedOut.isEmpty()) {
        Manager.log.warn("Load balancer failed to assign any tablets");
      }
    }
  }

  private void flushChanges(TabletLists tLists, WalStateManager wals)
      throws DistributedStoreException, TException, WalMarkerException {
    var unassigned = Collections.unmodifiableMap(tLists.unassigned);

    handleDeadTablets(tLists, wals);

    getAssignmentsFromBalancer(tLists, unassigned);

    if (!tLists.assignments.isEmpty()) {
      Manager.log.info(String.format("Assigning %d tablets", tLists.assignments.size()));
      store.setFutureLocations(tLists.assignments);
    }
    tLists.assignments.addAll(tLists.assigned);
    for (Assignment a : tLists.assignments) {
      TServerConnection client = manager.tserverSet.getConnection(a.server);
      if (client != null) {
        client.assignTablet(manager.managerLock, a.tablet);
      } else {
        Manager.log.warn("Could not connect to server {}", a.server);
      }
      manager.assignedTablet(a.tablet);
    }
  }

  private static void markDeadServerLogsAsClosed(WalStateManager mgr,
      Map<TServerInstance,List<Path>> logsForDeadServers) throws WalMarkerException {
    for (Entry<TServerInstance,List<Path>> server : logsForDeadServers.entrySet()) {
      for (Path path : server.getValue()) {
        mgr.closeWal(server.getKey(), path);
      }
    }
  }
}

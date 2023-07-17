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

import static com.google.common.util.concurrent.Uninterruptibles.sleepUninterruptibly;
import static java.lang.Math.min;
import static java.util.Objects.requireNonNull;

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
import java.util.stream.Collectors;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.RowIterator;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.TabletHostingGoal;
import org.apache.accumulo.core.clientImpl.TabletHostingGoalUtil;
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
import org.apache.accumulo.core.manager.balancer.TabletServerIdImpl;
import org.apache.accumulo.core.manager.state.TabletManagement;
import org.apache.accumulo.core.manager.state.TabletManagement.ManagementAction;
import org.apache.accumulo.core.manager.state.tables.TableState;
import org.apache.accumulo.core.manager.thrift.ManagerState;
import org.apache.accumulo.core.manager.thrift.TabletServerStatus;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.metadata.RootTable;
import org.apache.accumulo.core.metadata.StoredTabletFile;
import org.apache.accumulo.core.metadata.TServerInstance;
import org.apache.accumulo.core.metadata.TabletState;
import org.apache.accumulo.core.metadata.schema.Ample;
import org.apache.accumulo.core.metadata.schema.Ample.DataLevel;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.ChoppedColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.CurrentLocationColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.DataFileColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.ExternalCompactionColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.FutureLocationColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.HostingColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.ServerColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.TabletColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataTime;
import org.apache.accumulo.core.metadata.schema.TabletMetadata;
import org.apache.accumulo.core.metadata.schema.TabletMetadata.Location;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.spi.balancer.data.TabletServerId;
import org.apache.accumulo.core.util.TextUtil;
import org.apache.accumulo.core.util.threads.Threads.AccumuloDaemonThread;
import org.apache.accumulo.manager.Manager.TabletGoalState;
import org.apache.accumulo.manager.split.SplitTask;
import org.apache.accumulo.manager.state.MergeStats;
import org.apache.accumulo.manager.state.TableCounts;
import org.apache.accumulo.manager.state.TableStats;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.ServiceEnvironmentImpl;
import org.apache.accumulo.server.compaction.CompactionJobGenerator;
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
import org.apache.accumulo.server.manager.state.TabletManagementIterator;
import org.apache.accumulo.server.manager.state.TabletStateStore;
import org.apache.accumulo.server.manager.state.UnassignedTablet;
import org.apache.accumulo.server.tablets.TabletTime;
import org.apache.accumulo.server.util.MetadataTableUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableSortedSet;
import com.google.common.collect.Iterators;

abstract class TabletGroupWatcher extends AccumuloDaemonThread {

  public static class BadLocationStateException extends Exception {
    private static final long serialVersionUID = 2L;

    // store as byte array because Text isn't Serializable
    private final byte[] metadataTableEntry;

    public BadLocationStateException(String msg, Text row) {
      super(msg);
      this.metadataTableEntry = TextUtil.getBytes(requireNonNull(row));
    }

    public Text getEncodedEndRow() {
      return new Text(metadataTableEntry);
    }
  }

  private static final Logger LOG = LoggerFactory.getLogger(TabletGroupWatcher.class);
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
    private final List<TabletMetadata> assignedToDeadServers = new ArrayList<>();
    private final List<TabletMetadata> suspendedToGoneServers = new ArrayList<>();
    private final Map<KeyExtent,UnassignedTablet> unassigned = new HashMap<>();
    private final Map<TServerInstance,List<Path>> logsForDeadServers = new TreeMap<>();
    // read only list of tablet servers that are not shutting down
    private final SortedMap<TServerInstance,TabletServerStatus> destinations;
    private final Map<String,Set<TServerInstance>> currentTServerGrouping;

    public TabletLists(Manager m, SortedMap<TServerInstance,TabletServerStatus> curTServers,
        Map<String,Set<TServerInstance>> grouping) {
      var destinationsMod = new TreeMap<>(curTServers);
      destinationsMod.keySet().removeAll(m.serversToShutdown);
      this.destinations = Collections.unmodifiableSortedMap(destinationsMod);
      this.currentTServerGrouping = grouping;
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

      final long waitTimeBetweenScans = manager.getConfiguration()
          .getTimeInMillis(Property.MANAGER_TABLET_GROUP_WATCHER_INTERVAL);

      int totalUnloaded = 0;
      int unloaded = 0;
      ClosableIterator<TabletManagement> iter = null;
      try {
        Map<TableId,MergeStats> mergeStatsCache = new HashMap<>();
        Map<TableId,MergeStats> currentMerges = new HashMap<>();
        for (MergeInfo merge : manager.merges()) {
          if (merge.getExtent() != null) {
            currentMerges.put(merge.getExtent().tableId(), new MergeStats(merge));
          }
        }

        // Get the current status for the current list of tservers
        final SortedMap<TServerInstance,TabletServerStatus> currentTServers = new TreeMap<>();
        for (TServerInstance entry : manager.tserverSet.getCurrentServers()) {
          currentTServers.put(entry, manager.tserverStatus.get(entry));
        }

        if (currentTServers.isEmpty()) {
          eventListener.waitForEvents(waitTimeBetweenScans);
          synchronized (this) {
            lastScanServers = Collections.emptySortedSet();
          }
          continue;
        }

        final Map<String,Set<TServerInstance>> currentTServerGrouping =
            manager.tserverSet.getCurrentServersGroups();

        TabletLists tLists = new TabletLists(manager, currentTServers, currentTServerGrouping);

        ManagerState managerState = manager.getManagerState();
        int[] counts = new int[TabletState.values().length];
        stats.begin();

        CompactionJobGenerator compactionGenerator = new CompactionJobGenerator(
            new ServiceEnvironmentImpl(manager.getContext()), manager.getCompactionHints());

        final Map<String,Set<TabletServerId>> resourceGroups = new HashMap<>();
        manager.tServerResourceGroups().forEach((k, v) -> {
          resourceGroups.put(k,
              v.stream().map(s -> new TabletServerIdImpl(s)).collect(Collectors.toSet()));
        });

        // Walk through the tablets in our store, and work tablets
        // towards their goal
        iter = store.iterator();
        while (iter.hasNext()) {
          final TabletManagement mti = iter.next();
          if (mti == null) {
            throw new IllegalStateException("State store returned a null ManagerTabletInfo object");
          }

          final Set<ManagementAction> actions = mti.getActions();
          final TabletMetadata tm = mti.getTabletMetadata();

          if (tm.isFutureAndCurrentLocationSet()) {
            throw new BadLocationStateException(
                tm.getExtent() + " is both assigned and hosted, which should never happen: " + this,
                tm.getExtent().toMetaRow());
          }
          if (tm.isOperationIdAndCurrentLocationSet()) {
            throw new BadLocationStateException(tm.getExtent()
                + " has both operation id and current location, which should never happen: " + this,
                tm.getExtent().toMetaRow());
          }

          final TableId tableId = tm.getTableId();
          // ignore entries for tables that do not exist in zookeeper
          if (manager.getTableManager().getTableState(tableId) == null) {
            continue;
          }

          // Don't overwhelm the tablet servers with work
          if (tLists.unassigned.size() + unloaded
              > Manager.MAX_TSERVER_WORK_CHUNK * currentTServers.size()) {
            flushChanges(tLists, wals);
            tLists.reset();
            unloaded = 0;
            eventListener.waitForEvents(waitTimeBetweenScans);
          }
          final TableConfiguration tableConf = manager.getContext().getTableConfiguration(tableId);

          final MergeStats mergeStats = mergeStatsCache.computeIfAbsent(tableId, k -> {
            var mStats = currentMerges.get(k);
            return mStats != null ? mStats : new MergeStats(new MergeInfo());
          });
          TabletGoalState goal = manager.getGoalState(tm, mergeStats.getMergeInfo());
          TabletState state =
              tm.getTabletState(currentTServers.keySet(), manager.tabletBalancer, resourceGroups);

          final Location location = tm.getLocation();
          Location current = null;
          Location future = null;
          if (tm.hasCurrent()) {
            current = tm.getLocation();
          } else {
            future = tm.getLocation();
          }
          TabletLogger.missassigned(tm.getExtent(), goal.toString(), state.toString(),
              future != null ? future.getServerInstance() : null,
              current != null ? current.getServerInstance() : null, tm.getLogs().size());

          stats.update(tableId, state);
          mergeStats.update(tm.getExtent(), state, tm.hasChopped(), !tm.getLogs().isEmpty());
          sendChopRequest(mergeStats.getMergeInfo(), state, tm);

          // Always follow through with assignments
          if (state == TabletState.ASSIGNED) {
            goal = TabletGoalState.HOSTED;
          } else if (state == TabletState.ASSIGNED_TO_WRONG_GROUP) {
            goal = TabletGoalState.UNASSIGNED;
          }
          if (Manager.log.isTraceEnabled()) {
            Manager.log.trace(
                "[{}] Shutting down all Tservers: {}, dependentCount: {} Extent: {}, state: {}, goal: {}",
                store.name(), manager.serversToShutdown.equals(currentTServers.keySet()),
                dependentWatcher == null ? "null" : dependentWatcher.assignedOrHosted(),
                tm.getExtent(), state, goal);
          }

          // if we are shutting down all the tabletservers, we have to do it in order
          if ((goal == TabletGoalState.SUSPENDED && state == TabletState.HOSTED)
              && manager.serversToShutdown.equals(currentTServers.keySet())) {
            if (dependentWatcher != null) {
              // If the dependentWatcher is for the user tables, check to see
              // that user tables exist.
              DataLevel dependentLevel = dependentWatcher.store.getLevel();
              boolean userTablesExist = true;
              switch (dependentLevel) {
                case USER:
                  Set<TableId> onlineTables = manager.onlineTables();
                  onlineTables.remove(RootTable.ID);
                  onlineTables.remove(MetadataTable.ID);
                  userTablesExist = !onlineTables.isEmpty();
                  break;
                case METADATA:
                case ROOT:
                default:
                  break;
              }
              // If the stats object in the dependentWatcher is empty, then it
              // currently does not have data about what is hosted or not. In
              // that case host these tablets until the dependent watcher can
              // gather some data.
              final Map<TableId,TableCounts> stats = dependentWatcher.getStats();
              if (dependentLevel == DataLevel.USER) {
                if (userTablesExist
                    && (stats == null || stats.isEmpty() || assignedOrHosted(stats) > 0)) {
                  goal = TabletGoalState.HOSTED;
                }
              } else if (stats == null || stats.isEmpty() || assignedOrHosted(stats) > 0) {
                goal = TabletGoalState.HOSTED;
              }
            }
          }

          if (actions.contains(ManagementAction.NEEDS_SPLITTING)) {
            LOG.debug("{} may need splitting.", tm.getExtent());
            if (manager.getSplitter().isSplittable(tm)) {
              if (manager.getSplitter().addSplitStarting(tm.getExtent())) {
                LOG.debug("submitting tablet {} for split", tm.getExtent());
                manager.getSplitter()
                    .executeSplit(new SplitTask(manager.getContext(), tm, manager));
              }
            } else {
              LOG.debug("{} is not splittable.", tm.getExtent());
            }
            // ELASITICITY_TODO: See #3605. Merge is non-functional. Left this commented out code to
            // show where merge used to make a call to split a tablet.
            // sendSplitRequest(mergeStats.getMergeInfo(), state, tm);
          }

          if (actions.contains(ManagementAction.NEEDS_COMPACTING)) {
            var jobs = compactionGenerator.generateJobs(tm,
                TabletManagementIterator.determineCompactionKinds(actions));
            LOG.debug("{} may need compacting.", tm.getExtent());
            manager.getCompactionQueues().add(tm, jobs);
          }

          // ELASITICITY_TODO the case where a planner generates compactions at time T1 for tablet
          // and later at time T2 generates nothing for the same tablet is not being handled. At
          // time T1 something could have been queued. However at time T2 we will not clear those
          // entries from the queue because we see nothing here for that case. After a full
          // metadata scan could remove any tablets that were not updated during the scan.

          if (actions.contains(ManagementAction.NEEDS_LOCATION_UPDATE)) {
            if (goal == TabletGoalState.HOSTED) {
              if ((state != TabletState.HOSTED && !tm.getLogs().isEmpty())
                  && manager.recoveryManager.recoverLogs(tm.getExtent(), tm.getLogs())) {
                continue;
              }
              switch (state) {
                case HOSTED:
                  if (location.getServerInstance().equals(manager.migrations.get(tm.getExtent()))) {
                    manager.migrations.remove(tm.getExtent());
                  }
                  break;
                case ASSIGNED_TO_DEAD_SERVER:
                  hostDeadTablet(tLists, tm, location, wals);
                  break;
                case SUSPENDED:
                  hostSuspendedTablet(tLists, tm, location, tableConf);
                  break;
                case UNASSIGNED:
                  hostUnassignedTablet(tLists, tm.getExtent(),
                      new UnassignedTablet(location, tm.getLast()));
                  break;
                case ASSIGNED:
                  // Send another reminder
                  tLists.assigned.add(new Assignment(tm.getExtent(),
                      future != null ? future.getServerInstance() : null, tm.getLast()));
                  break;
                default:
                  break;
              }
            } else {
              switch (state) {
                case SUSPENDED:
                  // Request a move to UNASSIGNED, so as to allow balancing to continue.
                  tLists.suspendedToGoneServers.add(tm);
                  cancelOfflineTableMigrations(tm.getExtent());
                  break;
                case UNASSIGNED:
                  cancelOfflineTableMigrations(tm.getExtent());
                  break;
                case ASSIGNED_TO_DEAD_SERVER:
                  unassignDeadTablet(tLists, tm, wals);
                  break;
                case ASSIGNED_TO_WRONG_GROUP:
                case HOSTED:
                  TServerConnection client =
                      manager.tserverSet.getConnection(location.getServerInstance());
                  if (client != null) {
                    client.unloadTablet(manager.managerLock, tm.getExtent(), goal.howUnload(),
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
        }

        // ELASTICITY_TODO: Add handling for other actions

        flushChanges(tLists, wals);

        // provide stats after flushing changes to avoid race conditions w/ delete table
        stats.end(managerState);
        Manager.log.trace("[{}] End stats collection: {}", store.name(), stats);

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
              waitTimeBetweenScans / 1000.));
          eventListener.waitForEvents(waitTimeBetweenScans);
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

  private void unassignDeadTablet(TabletLists tLists, TabletMetadata tm, WalStateManager wals)
      throws WalMarkerException {
    tLists.assignedToDeadServers.add(tm);
    if (!tLists.logsForDeadServers.containsKey(tm.getLocation().getServerInstance())) {
      tLists.logsForDeadServers.put(tm.getLocation().getServerInstance(),
          wals.getWalsInUse(tm.getLocation().getServerInstance()));
    }
  }

  private void hostUnassignedTablet(TabletLists tLists, KeyExtent tablet,
      UnassignedTablet unassignedTablet) {
    // maybe it's a finishing migration
    TServerInstance dest = manager.migrations.get(tablet);
    if (dest != null) {
      // if destination is still good, assign it
      if (tLists.destinations.containsKey(dest)) {
        tLists.assignments.add(new Assignment(tablet, dest, unassignedTablet.getLastLocation()));
      } else {
        // get rid of this migration
        manager.migrations.remove(tablet);
        tLists.unassigned.put(tablet, unassignedTablet);
      }
    } else {
      tLists.unassigned.put(tablet, unassignedTablet);
    }
  }

  private void hostSuspendedTablet(TabletLists tLists, TabletMetadata tm, Location location,
      TableConfiguration tableConf) {
    if (manager.getSteadyTime() - tm.getSuspend().suspensionTime
        < tableConf.getTimeInMillis(Property.TABLE_SUSPEND_DURATION)) {
      // Tablet is suspended. See if its tablet server is back.
      TServerInstance returnInstance = null;
      Iterator<TServerInstance> find = tLists.destinations
          .tailMap(new TServerInstance(tm.getSuspend().server, " ")).keySet().iterator();
      if (find.hasNext()) {
        TServerInstance found = find.next();
        if (found.getHostAndPort().equals(tm.getSuspend().server)) {
          returnInstance = found;
        }
      }

      // Old tablet server is back. Return this tablet to its previous owner.
      if (returnInstance != null) {
        tLists.assignments.add(new Assignment(tm.getExtent(), returnInstance, tm.getLast()));
      }
      // else - tablet server not back. Don't ask for a new assignment right now.

    } else {
      // Treat as unassigned, ask for a new assignment.
      tLists.unassigned.put(tm.getExtent(), new UnassignedTablet(location, tm.getLast()));
    }
  }

  private void hostDeadTablet(TabletLists tLists, TabletMetadata tm, Location location,
      WalStateManager wals) throws WalMarkerException {
    tLists.assignedToDeadServers.add(tm);
    if (location.getServerInstance().equals(manager.migrations.get(tm.getExtent()))) {
      manager.migrations.remove(tm.getExtent());
    }
    TServerInstance tserver = tm.getLocation().getServerInstance();
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
    return assignedOrHosted(stats.getLast());
  }

  private int assignedOrHosted(Map<TableId,TableCounts> last) {
    int result = 0;
    for (TableCounts counts : last.values()) {
      result += counts.assigned() + counts.hosted();
    }
    return result;
  }

  private void sendSplitRequest(MergeInfo info, TabletState state, TabletMetadata tm) {
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
    // Tablet is not hosted
    if (!tm.hasCurrent()) {
      return;
    }
    // Does this extent cover the end points of the delete?
    KeyExtent range = info.getExtent();
    if (tm.getExtent().overlaps(range)) {
      for (Text splitPoint : new Text[] {range.prevEndRow(), range.endRow()}) {
        if (splitPoint == null) {
          continue;
        }
        if (!tm.getExtent().contains(splitPoint)) {
          continue;
        }
        if (splitPoint.equals(tm.getExtent().endRow())) {
          continue;
        }
        if (splitPoint.equals(tm.getExtent().prevEndRow())) {
          continue;
        }
        try {
          // ELASTICITY_TODO this used to send a split req to tserver, what should it do now? Issues
          // #3605 was opened about this.
          throw new UnsupportedOperationException();
        } catch (Exception e) {
          Manager.log.warn("Error asking tablet server to split a tablet: ", e);
        }
      }
    }
  }

  private void sendChopRequest(MergeInfo info, TabletState state, TabletMetadata tm) {
    // Don't bother if we're in the wrong state
    if (!info.getState().equals(MergeState.WAITING_FOR_CHOPPED)) {
      return;
    }
    // Tablet must be online
    if (!state.equals(TabletState.HOSTED)) {
      return;
    }
    // Tablet isn't already chopped
    if (tm.hasChopped()) {
      return;
    }
    // Tablet is not hosted
    if (!tm.hasCurrent()) {
      return;
    }
    // Tablet ranges intersect
    if (info.needsToBeChopped(tm.getExtent())) {
      throw new UnsupportedOperationException("The tablet server can no longer chop tablets");
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
    Set<TabletHostingGoal> goals = new HashSet<>();

    AccumuloClient client = manager.getContext();

    try (BatchWriter bw = client.createBatchWriter(targetSystemTable)) {
      long fileCount = 0;
      // Make file entries in highest tablet
      Scanner scanner = client.createScanner(targetSystemTable, Authorizations.EMPTY);
      scanner.setRange(scanRange);
      TabletColumnFamily.PREV_ROW_COLUMN.fetch(scanner);
      ServerColumnFamily.TIME_COLUMN.fetch(scanner);
      ServerColumnFamily.DIRECTORY_COLUMN.fetch(scanner);
      HostingColumnFamily.GOAL_COLUMN.fetch(scanner);
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
        } else if (HostingColumnFamily.GOAL_COLUMN.hasColumns(key)) {
          TabletHostingGoal thisGoal = TabletHostingGoalUtil.fromValue(value);
          goals.add(thisGoal);
        }
      }

      // read the logical time from the last tablet in the merge range, it is not included in
      // the loop above
      scanner = client.createScanner(targetSystemTable, Authorizations.EMPTY);
      scanner.setRange(new Range(stopRow));
      ServerColumnFamily.TIME_COLUMN.fetch(scanner);
      HostingColumnFamily.GOAL_COLUMN.fetch(scanner);
      scanner.fetchColumnFamily(ExternalCompactionColumnFamily.NAME);
      Set<String> extCompIds = new HashSet<>();
      for (Entry<Key,Value> entry : scanner) {
        if (ServerColumnFamily.TIME_COLUMN.hasColumns(entry.getKey())) {
          maxLogicalTime = TabletTime.maxMetadataTime(maxLogicalTime,
              MetadataTime.parse(entry.getValue().toString()));
        } else if (ExternalCompactionColumnFamily.NAME.equals(entry.getKey().getColumnFamily())) {
          extCompIds.add(entry.getKey().getColumnQualifierData().toString());
        } else if (HostingColumnFamily.GOAL_COLUMN.hasColumns(entry.getKey())) {
          TabletHostingGoal thisGoal = TabletHostingGoalUtil.fromValue(entry.getValue());
          goals.add(thisGoal);
        }
      }

      if (maxLogicalTime != null) {
        ServerColumnFamily.TIME_COLUMN.put(m, new Value(maxLogicalTime.encode()));
      }

      // delete any entries for external compactions
      extCompIds.forEach(ecid -> m.putDelete(ExternalCompactionColumnFamily.STR_NAME, ecid));

      // Set the TabletHostingGoal for this tablet based on the goals of the other tablets in
      // the merge range. Always takes priority over never.
      TabletHostingGoal mergeHostingGoal = TabletHostingGoal.ONDEMAND;
      if (range.isMeta() || goals.contains(TabletHostingGoal.ALWAYS)) {
        mergeHostingGoal = TabletHostingGoal.ALWAYS;
      } else if (goals.contains(TabletHostingGoal.NEVER)) {
        mergeHostingGoal = TabletHostingGoal.NEVER;
      }
      HostingColumnFamily.GOAL_COLUMN.put(m, TabletHostingGoalUtil.toValue(mergeHostingGoal));

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
      Map<KeyExtent,UnassignedTablet> unassigned) {
    if (!tLists.destinations.isEmpty()) {
      Map<KeyExtent,TServerInstance> assignedOut = new HashMap<>();
      manager.getAssignments(tLists.destinations, tLists.currentTServerGrouping, unassigned,
          assignedOut);
      for (Entry<KeyExtent,TServerInstance> assignment : assignedOut.entrySet()) {
        if (unassigned.containsKey(assignment.getKey())) {
          if (assignment.getValue() != null) {
            if (!tLists.destinations.containsKey(assignment.getValue())) {
              Manager.log.warn(
                  "balancer assigned {} to a tablet server that is not current {} ignoring",
                  assignment.getKey(), assignment.getValue());
              continue;
            }

            final UnassignedTablet unassignedTablet = unassigned.get(assignment.getKey());
            tLists.assignments.add(new Assignment(assignment.getKey(), assignment.getValue(),
                unassignedTablet != null ? unassignedTablet.getLastLocation() : null));
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

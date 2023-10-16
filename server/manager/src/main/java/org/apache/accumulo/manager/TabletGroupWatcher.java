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
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Predicate;
import java.util.stream.Stream;

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
import org.apache.accumulo.core.manager.thrift.ManagerGoalState;
import org.apache.accumulo.core.manager.thrift.ManagerState;
import org.apache.accumulo.core.manager.thrift.TabletServerStatus;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.metadata.RootTable;
import org.apache.accumulo.core.metadata.StoredTabletFile;
import org.apache.accumulo.core.metadata.TServerInstance;
import org.apache.accumulo.core.metadata.TabletState;
import org.apache.accumulo.core.metadata.schema.Ample;
import org.apache.accumulo.core.metadata.schema.Ample.DataLevel;
import org.apache.accumulo.core.metadata.schema.DataFileValue;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.CurrentLocationColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.DataFileColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.ExternalCompactionColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.FutureLocationColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.HostingColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.ServerColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.TabletColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataTime;
import org.apache.accumulo.core.metadata.schema.TabletMetadata;
import org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType;
import org.apache.accumulo.core.metadata.schema.TabletMetadata.Location;
import org.apache.accumulo.core.metadata.schema.TabletsMetadata;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.spi.balancer.data.TabletServerId;
import org.apache.accumulo.core.util.Pair;
import org.apache.accumulo.core.util.TextUtil;
import org.apache.accumulo.core.util.threads.Threads;
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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSortedSet;
import com.google.common.collect.Iterators;
import com.google.common.collect.Sets;

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
  private final EventHandler eventHandler;

  private WalStateManager walStateManager;

  TabletGroupWatcher(Manager manager, TabletStateStore store, TabletGroupWatcher dependentWatcher) {
    super("Watching " + store.name());
    this.manager = manager;
    this.store = store;
    this.dependentWatcher = dependentWatcher;
    this.walStateManager = new WalStateManager(manager.getContext());
    this.eventHandler = new EventHandler();
    manager.getEventCoordinator().addListener(store.getLevel(), eventHandler);
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

  class EventHandler implements EventCoordinator.Listener {

    // Setting this to true to start with because its not know what happended before this object was
    // created, so just start off with full scan.
    private boolean needsFullScan = true;

    private final BlockingQueue<Range> rangesToProcess;

    class RangeProccessor implements Runnable {
      @Override
      public void run() {
        try {
          while (manager.stillManager()) {
            var range = rangesToProcess.poll(100, TimeUnit.MILLISECONDS);
            if (range == null) {
              // check to see if still the manager
              continue;
            }

            ArrayList<Range> ranges = new ArrayList<>();
            ranges.add(range);

            rangesToProcess.drainTo(ranges);

            if (manager.getManagerGoalState() == ManagerGoalState.CLEAN_STOP) {
              // only do full scans when trying to shutdown
              setNeedsFullScan();
              continue;
            }

            var currentTservers = getCurrentTservers();
            if (currentTservers.isEmpty()) {
              setNeedsFullScan();
              continue;
            }

            try (var iter = store.iterator(ranges)) {
              long t1 = System.currentTimeMillis();
              manageTablets(iter, currentTservers, false);
              long t2 = System.currentTimeMillis();
              Manager.log.debug(String.format("[%s]: partial scan time %.2f seconds for %,d ranges",
                  store.name(), (t2 - t1) / 1000., ranges.size()));
            } catch (Exception e) {
              Manager.log.error("Error processing {} ranges for store {} ", ranges.size(),
                  store.name(), e);
            }
          }
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        }
      }
    }

    EventHandler() {
      rangesToProcess = new ArrayBlockingQueue<>(3000);

      Threads
          .createThread("TGW [" + store.name() + "] event range processor", new RangeProccessor())
          .start();
    }

    private synchronized void setNeedsFullScan() {
      needsFullScan = true;
      notifyAll();
    }

    public synchronized void clearNeedsFullScan() {
      needsFullScan = false;
    }

    @Override
    public void process(EventCoordinator.Event event) {

      switch (event.getScope()) {
        case ALL:
        case DATA_LEVEL:
          setNeedsFullScan();
          break;
        case TABLE:
        case TABLE_RANGE:
          if (!rangesToProcess.offer(event.getExtent().toMetaRange())) {
            Manager.log.debug("[{}] unable to process event range {} because queue is full",
                store.name(), event.getExtent());
            setNeedsFullScan();
          }
          break;
        default:
          throw new IllegalArgumentException("Unhandled scope " + event.getScope());
      }
    }

    synchronized void waitForFullScan(long millis) {
      if (!needsFullScan) {
        try {
          wait(millis);
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        }
      }
    }
  }

  private static class TableMgmtStats {
    int[] counts = new int[TabletState.values().length];
    private int totalUnloaded;

    Map<TableId,MergeStats> mergeStatsCache = new HashMap<>();
  }

  private TableMgmtStats manageTablets(Iterator<TabletManagement> iter,
      SortedMap<TServerInstance,TabletServerStatus> currentTServers, boolean isFullScan)
      throws BadLocationStateException, TException, DistributedStoreException, WalMarkerException,
      IOException {

    TableMgmtStats tableMgmtStats = new TableMgmtStats();
    int unloaded = 0;

    Map<TableId,MergeStats> currentMerges = new HashMap<>();
    for (MergeInfo merge : manager.merges()) {
      if (merge.getExtent() != null) {
        currentMerges.put(merge.getExtent().tableId(), new MergeStats(merge));
      }
    }

    final Map<String,Set<TServerInstance>> currentTServerGrouping =
        manager.tserverSet.getCurrentServersGroups();

    TabletLists tLists = new TabletLists(manager, currentTServers, currentTServerGrouping);

    CompactionJobGenerator compactionGenerator = new CompactionJobGenerator(
        new ServiceEnvironmentImpl(manager.getContext()), manager.getCompactionHints());

    final Map<TabletServerId,String> resourceGroups = new HashMap<>();
    manager.tServerResourceGroups().forEach((group, tservers) -> {
      tservers.stream().map(TabletServerIdImpl::new)
          .forEach(tabletServerId -> resourceGroups.put(tabletServerId, group));
    });

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

      final TableId tableId = tm.getTableId();
      // ignore entries for tables that do not exist in zookeeper
      if (manager.getTableManager().getTableState(tableId) == null) {
        continue;
      }

      // Don't overwhelm the tablet servers with work
      if (tLists.unassigned.size() + unloaded
          > Manager.MAX_TSERVER_WORK_CHUNK * currentTServers.size()) {
        flushChanges(tLists);
        tLists.reset();
        unloaded = 0;
      }

      final TableConfiguration tableConf = manager.getContext().getTableConfiguration(tableId);

      final MergeStats mergeStats = tableMgmtStats.mergeStatsCache.computeIfAbsent(tableId, k -> {
        var mStats = currentMerges.get(k);
        return mStats != null ? mStats : new MergeStats(new MergeInfo());
      });
      TabletGoalState goal = manager.getGoalState(tm, mergeStats.getMergeInfo());
      TabletState state =
          TabletState.compute(tm, currentTServers.keySet(), manager.tabletBalancer, resourceGroups);

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

      if (isFullScan) {
        stats.update(tableId, state);
      }
      mergeStats.update(tm.getExtent(), state);

      // Always follow through with assignments
      if (state == TabletState.ASSIGNED) {
        goal = TabletGoalState.HOSTED;
      } else if (state == TabletState.NEEDS_REASSIGNMENT) {
        goal = TabletGoalState.UNASSIGNED;
      }

      if (tm.getOperationId() != null) {
        goal = TabletGoalState.UNASSIGNED;
      }

      if (Manager.log.isTraceEnabled()) {
        Manager.log.trace(
            "[{}] Shutting down all Tservers: {}, dependentCount: {} Extent: {}, state: {}, goal: {} actions:{}",
            store.name(), manager.serversToShutdown.equals(currentTServers.keySet()),
            dependentWatcher == null ? "null" : dependentWatcher.assignedOrHosted(), tm.getExtent(),
            state, goal, actions);
      }

      // if we are shutting down all the tabletservers, we have to do it in order
      if (isFullScan && (goal == TabletGoalState.SUSPENDED && state == TabletState.HOSTED)
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
            manager.getSplitter().executeSplit(new SplitTask(manager.getContext(), tm, manager));
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
        LOG.debug("{} may need compacting adding {} jobs", tm.getExtent(), jobs.size());
        manager.getCompactionQueues().add(tm, jobs);
      }

      // ELASITICITY_TODO the case where a planner generates compactions at time T1 for tablet
      // and later at time T2 generates nothing for the same tablet is not being handled. At
      // time T1 something could have been queued. However at time T2 we will not clear those
      // entries from the queue because we see nothing here for that case. After a full
      // metadata scan could remove any tablets that were not updated during the scan.

      if (actions.contains(ManagementAction.NEEDS_LOCATION_UPDATE)
          || actions.contains(ManagementAction.IS_MERGING)) {
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
              hostDeadTablet(tLists, tm, location);
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
              unassignDeadTablet(tLists, tm);
              break;
            case NEEDS_REASSIGNMENT:
            case HOSTED:
              TServerConnection client =
                  manager.tserverSet.getConnection(location.getServerInstance());
              if (client != null) {
                client.unloadTablet(manager.managerLock, tm.getExtent(), goal.howUnload(),
                    manager.getSteadyTime());
                tableMgmtStats.totalUnloaded++;
                unloaded++;
              } else {
                Manager.log.warn("Could not connect to server {}", location);
              }
              break;
            case ASSIGNED:
              break;
          }
        }
        tableMgmtStats.counts[state.ordinal()]++;
      }
    }

    flushChanges(tLists);
    return tableMgmtStats;
  }

  private SortedMap<TServerInstance,TabletServerStatus> getCurrentTservers() {
    // Get the current status for the current list of tservers
    final SortedMap<TServerInstance,TabletServerStatus> currentTServers = new TreeMap<>();
    for (TServerInstance entry : manager.tserverSet.getCurrentServers()) {
      currentTServers.put(entry, manager.tserverStatus.get(entry));
    }
    return currentTServers;
  }

  @Override
  public void run() {
    int[] oldCounts = new int[TabletState.values().length];

    while (manager.stillManager()) {
      // slow things down a little, otherwise we spam the logs when there are many wake-up events
      sleepUninterruptibly(100, TimeUnit.MILLISECONDS);
      // ELASTICITY_TODO above sleep in the case when not doing a full scan to make manager more
      // responsive

      final long waitTimeBetweenScans = manager.getConfiguration()
          .getTimeInMillis(Property.MANAGER_TABLET_GROUP_WATCHER_INTERVAL);

      var currentTServers = getCurrentTservers();

      ClosableIterator<TabletManagement> iter = null;
      try {
        if (currentTServers.isEmpty()) {
          eventHandler.waitForFullScan(waitTimeBetweenScans);
          synchronized (this) {
            lastScanServers = Collections.emptySortedSet();
          }
          continue;
        }

        stats.begin();

        ManagerState managerState = manager.getManagerState();

        // Clear the need for a full scan before starting a full scan inorder to detect events that
        // happen during the full scan.
        eventHandler.clearNeedsFullScan();

        iter = store.iterator();
        var tabletMgmtStats = manageTablets(iter, currentTServers, true);

        // provide stats after flushing changes to avoid race conditions w/ delete table
        stats.end(managerState);
        Manager.log.trace("[{}] End stats collection: {}", store.name(), stats);

        // Report changes
        for (TabletState state : TabletState.values()) {
          int i = state.ordinal();
          if (tabletMgmtStats.counts[i] > 0 && tabletMgmtStats.counts[i] != oldCounts[i]) {
            manager.nextEvent.event(store.getLevel(), "[%s]: %d tablets are %s", store.name(),
                tabletMgmtStats.counts[i], state.name());
          }
        }
        Manager.log.debug(String.format("[%s]: full scan time %.2f seconds", store.name(),
            stats.getScanTime() / 1000.));
        oldCounts = tabletMgmtStats.counts;
        if (tabletMgmtStats.totalUnloaded > 0) {
          manager.nextEvent.event(store.getLevel(), "[%s]: %d tablets unloaded", store.name(),
              tabletMgmtStats.totalUnloaded);
        }

        updateMergeState(tabletMgmtStats.mergeStatsCache);

        synchronized (this) {
          lastScanServers = ImmutableSortedSet.copyOf(currentTServers.keySet());
        }
        if (manager.tserverSet.getCurrentServers().equals(currentTServers.keySet())) {
          Manager.log.debug(String.format("[%s] sleeping for %.2f seconds", store.name(),
              waitTimeBetweenScans / 1000.));
          eventHandler.waitForFullScan(waitTimeBetweenScans);
        } else {
          // Create an event at the store level, this will force the next scan to be a full scan
          manager.nextEvent.event(store.getLevel(), "Set of tablet servers changed");
        }
      } catch (Exception ex) {
        Manager.log.error("Error processing table state for store " + store.name(), ex);
        if (ex.getCause() != null && ex.getCause() instanceof BadLocationStateException) {
          // ELASTICITY_TODO review this function
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

  private void unassignDeadTablet(TabletLists tLists, TabletMetadata tm) throws WalMarkerException {
    tLists.assignedToDeadServers.add(tm);
    if (!tLists.logsForDeadServers.containsKey(tm.getLocation().getServerInstance())) {
      tLists.logsForDeadServers.put(tm.getLocation().getServerInstance(),
          walStateManager.getWalsInUse(tm.getLocation().getServerInstance()));
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

  private void hostDeadTablet(TabletLists tLists, TabletMetadata tm, Location location)
      throws WalMarkerException {
    tLists.assignedToDeadServers.add(tm);
    if (location.getServerInstance().equals(manager.migrations.get(tm.getExtent()))) {
      manager.migrations.remove(tm.getExtent());
    }
    TServerInstance tserver = tm.getLocation().getServerInstance();
    if (!tLists.logsForDeadServers.containsKey(tserver)) {
      tLists.logsForDeadServers.put(tserver, walStateManager.getWalsInUse(tserver));
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

  // This method finds returns the deletion starting row (exclusive) for tablets that
  // need to be actually deleted. If the startTablet is null then
  // the deletion start row will just be null as all tablets are being deleted
  // up to the end. Otherwise, this returns the endRow of the first tablet
  // as the first tablet should be kept and will have been previously
  // fenced if necessary
  private Text getDeletionStartRow(final KeyExtent startTablet) {
    if (startTablet == null) {
      Manager.log.debug("First tablet for delete range is null");
      return null;
    }

    final Text deletionStartRow = startTablet.endRow();
    Manager.log.debug("Start row is {} for deletion", deletionStartRow);

    return deletionStartRow;
  }

  // This method finds returns the deletion ending row (inclusive) for tablets that
  // need to be actually deleted. If the endTablet is null then
  // the deletion end row will just be null as all tablets are being deleted
  // after the start row. Otherwise, this returns the prevEndRow of the last tablet
  // as the last tablet should be kept and will have been previously
  // fenced if necessary
  private Text getDeletionEndRow(final KeyExtent endTablet) {
    if (endTablet == null) {
      Manager.log.debug("Last tablet for delete range is null");
      return null;
    }

    Text deletionEndRow = endTablet.prevEndRow();
    Manager.log.debug("Deletion end row is {}", deletionEndRow);

    return deletionEndRow;
  }

  private static boolean isFirstTabletInTable(KeyExtent tablet) {
    return tablet != null && tablet.prevEndRow() == null;
  }

  private static boolean isLastTabletInTable(KeyExtent tablet) {
    return tablet != null && tablet.endRow() == null;
  }

  private static boolean areContiguousTablets(KeyExtent firstTablet, KeyExtent lastTablet) {
    return firstTablet != null && lastTablet != null
        && Objects.equals(firstTablet.endRow(), lastTablet.prevEndRow());
  }

  private boolean hasTabletsToDelete(final KeyExtent firstTabletInRange,
      final KeyExtent lastTableInRange) {
    // If the tablets are equal (and not null) then the deletion range is just part of 1 tablet
    // which will be fenced so there are no tablets to delete. The null check is because if both
    // are null then we are just deleting everything, so we do have tablets to delete
    if (Objects.equals(firstTabletInRange, lastTableInRange) && firstTabletInRange != null) {
      Manager.log.trace(
          "No tablets to delete, firstTablet {} equals lastTablet {} in deletion range and was fenced.",
          firstTabletInRange, lastTableInRange);
      return false;
      // If the lastTablet of the deletion range is the first tablet of the table it has been fenced
      // already so nothing to actually delete before it
    } else if (isFirstTabletInTable(lastTableInRange)) {
      Manager.log.trace(
          "No tablets to delete, lastTablet {} in deletion range is the first tablet of the table and was fenced.",
          lastTableInRange);
      return false;
      // If the firstTablet of the deletion range is the last tablet of the table it has been fenced
      // already so nothing to actually delete after it
    } else if (isLastTabletInTable(firstTabletInRange)) {
      Manager.log.trace(
          "No tablets to delete, firstTablet {} in deletion range is the last tablet of the table and was fenced.",
          firstTabletInRange);
      return false;
      // If the firstTablet and lastTablet are contiguous tablets then there is nothing to delete as
      // each will be fenced and nothing between
    } else if (areContiguousTablets(firstTabletInRange, lastTableInRange)) {
      Manager.log.trace(
          "No tablets to delete, firstTablet {} and lastTablet {} in deletion range are contiguous and were fenced.",
          firstTabletInRange, lastTableInRange);
      return false;
    }

    return true;
  }

  private void deleteTablets(MergeInfo info) throws AccumuloException {
    // Before updated metadata and get the first and last tablets which
    // are fenced if necessary
    final Pair<KeyExtent,KeyExtent> firstAndLastTablets = updateMetadataRecordsForDelete(info);

    // Find the deletion start row (exclusive) for tablets that need to be actually deleted
    // This will be null if deleting everything up until the end row or it will be
    // the endRow of the first tablet as the first tablet should be kept and will have
    // already been fenced if necessary
    final Text deletionStartRow = getDeletionStartRow(firstAndLastTablets.getFirst());

    // Find the deletion end row (inclusive) for tablets that need to be actually deleted
    // This will be null if deleting everything after the starting row or it will be
    // the prevEndRow of the last tablet as the last tablet should be kept and will have
    // already been fenced if necessary
    Text deletionEndRow = getDeletionEndRow(firstAndLastTablets.getSecond());

    // check if there are any tablets to delete and if not return
    if (!hasTabletsToDelete(firstAndLastTablets.getFirst(), firstAndLastTablets.getSecond())) {
      Manager.log.trace("No tablets to delete for range {}, returning", info.getExtent());
      return;
    }

    // Build an extent for the actual deletion range
    final KeyExtent extent =
        new KeyExtent(info.getExtent().tableId(), deletionEndRow, deletionStartRow);
    Manager.log.debug("Tablet deletion range is {}", extent);
    String targetSystemTable = extent.isMeta() ? RootTable.NAME : MetadataTable.NAME;
    Manager.log.debug("Deleting tablets for {}", extent);
    MetadataTime metadataTime = null;
    KeyExtent followingTablet = null;
    Set<TabletHostingGoal> goals = new HashSet<>();
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
      HostingColumnFamily.GOAL_COLUMN.fetch(scanner);
      scanner.fetchColumnFamily(DataFileColumnFamily.NAME);
      scanner.fetchColumnFamily(CurrentLocationColumnFamily.NAME);
      Set<ReferenceFile> datafilesAndDirs = new TreeSet<>();
      for (Entry<Key,Value> entry : scanner) {
        Key key = entry.getKey();
        if (key.compareColumnFamily(DataFileColumnFamily.NAME) == 0) {
          var stf = new StoredTabletFile(key.getColumnQualifierData().toString());
          datafilesAndDirs.add(new ReferenceFile(stf.getTableId(), stf));
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
        } else if (HostingColumnFamily.GOAL_COLUMN.hasColumns(key)) {
          TabletHostingGoal thisGoal = TabletHostingGoalUtil.fromValue(entry.getValue());
          goals.add(thisGoal);
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
          bw.addMutation(m);
          bw.flush();
        } finally {
          bw.close();
        }
      } else {
        // Recreate the default tablet to hold the end of the table
        MetadataTableUtil.addTablet(new KeyExtent(extent.tableId(), null, extent.prevEndRow()),
            ServerColumnFamily.DEFAULT_TABLET_DIR_NAME, manager.getContext(),
            metadataTime.getType(), manager.managerLock, getMergeHostingGoal(extent, goals));
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

    KeyExtent stopExtent = KeyExtent.fromMetaRow(stop.toMetaRow());
    KeyExtent previousKeyExtent = null;
    KeyExtent lastExtent = null;

    try (BatchWriter bw = client.createBatchWriter(targetSystemTable)) {
      long fileCount = 0;
      // Make file entries in highest tablet
      Scanner scanner = client.createScanner(targetSystemTable, Authorizations.EMPTY);
      // Update to set the range to include the highest tablet
      scanner.setRange(
          new Range(TabletsSection.encodeRow(range.tableId(), start), false, stopRow, true));
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

        final KeyExtent keyExtent = KeyExtent.fromMetaRow(key.getRow());

        // Keep track of the last Key Extent seen so we can use it to fence
        // of RFiles when merging the metadata
        if (lastExtent != null && !keyExtent.equals(lastExtent)) {
          previousKeyExtent = lastExtent;
        }

        // Special case to handle the highest/stop tablet, which is where files are
        // merged to. The existing merge code won't delete files from this tablet
        // so we need to handle the deletes in this tablet when fencing files.
        // We may be able to make this simpler in the future.
        if (keyExtent.equals(stopExtent)) {
          if (previousKeyExtent != null
              && key.getColumnFamily().equals(DataFileColumnFamily.NAME)) {

            // Fence off existing files by the end row of the previous tablet and current tablet
            final StoredTabletFile existing = StoredTabletFile.of(key.getColumnQualifier());
            // The end row should be inclusive for the current tablet and the previous end row
            // should be exclusive for the start row
            Range fenced = new Range(previousKeyExtent.endRow(), false, keyExtent.endRow(), true);

            // Clip range if exists
            fenced = existing.hasRange() ? existing.getRange().clip(fenced) : fenced;

            final StoredTabletFile newFile = StoredTabletFile.of(existing.getPath(), fenced);
            // If the existing metadata does not match then we need to delete the old
            // and replace with a new range
            if (!existing.equals(newFile)) {
              m.putDelete(DataFileColumnFamily.NAME, existing.getMetadataText());
              m.put(key.getColumnFamily(), newFile.getMetadataText(), value);
            }

            fileCount++;
          }
          // For the highest tablet we only care about the DataFileColumnFamily
          continue;
        }

        // Handle metadata updates for all other tablets except the highest tablet
        // Ranges are created for the files and then added to the highest tablet in
        // the merge range. Deletes are handled later for the old files when the tablets
        // are removed.
        if (key.getColumnFamily().equals(DataFileColumnFamily.NAME)) {
          final StoredTabletFile existing = StoredTabletFile.of(key.getColumnQualifier());

          // Fence off files by the previous tablet and current tablet that is being merged
          // The end row should be inclusive for the current tablet and the previous end row should
          // be exclusive for the start row.
          Range fenced = new Range(previousKeyExtent != null ? previousKeyExtent.endRow() : null,
              false, keyExtent.endRow(), true);

          // Clip range with the tablet range if the range already exists
          fenced = existing.hasRange() ? existing.getRange().clip(fenced) : fenced;

          // Move the file and range to the last tablet
          StoredTabletFile newFile = StoredTabletFile.of(existing.getPath(), fenced);
          m.put(key.getColumnFamily(), newFile.getMetadataText(), value);

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

        lastExtent = keyExtent;
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
      TabletHostingGoal mergeHostingGoal = getMergeHostingGoal(range, goals);
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

    } catch (Exception ex) {
      throw new AccumuloException(ex);
    }
  }

  private static TabletHostingGoal getMergeHostingGoal(KeyExtent range,
      Set<TabletHostingGoal> goals) {
    TabletHostingGoal mergeHostingGoal = TabletHostingGoal.ONDEMAND;
    if (range.isMeta() || goals.contains(TabletHostingGoal.ALWAYS)) {
      mergeHostingGoal = TabletHostingGoal.ALWAYS;
    } else if (goals.contains(TabletHostingGoal.NEVER)) {
      mergeHostingGoal = TabletHostingGoal.NEVER;
    }
    return mergeHostingGoal;
  }

  // This method is used to detect if a tablet needs to be split/chopped for a delete
  // Instead of performing a split or chop compaction, the tablet will have its files fenced.
  private boolean needsFencingForDeletion(MergeInfo info, KeyExtent keyExtent) {
    // Does this extent cover the end points of the delete?
    final Predicate<Text> isWithin = r -> r != null && keyExtent.contains(r);
    final Predicate<Text> isNotBoundary =
        r -> !r.equals(keyExtent.endRow()) && !r.equals(keyExtent.prevEndRow());
    final KeyExtent deleteRange = info.getExtent();

    return (keyExtent.overlaps(deleteRange) && Stream
        .of(deleteRange.prevEndRow(), deleteRange.endRow()).anyMatch(isWithin.and(isNotBoundary)))
        || info.needsToBeChopped(keyExtent);
  }

  // Instead of splitting or chopping tablets for a delete we instead create ranges
  // to exclude the portion of the tablet that should be deleted
  private Text followingRow(Text row) {
    if (row == null) {
      return null;
    }
    return new Key(row).followingKey(PartialKey.ROW).getRow();
  }

  // Instead of splitting or chopping tablets for a delete we instead create ranges
  // to exclude the portion of the tablet that should be deleted
  private List<Range> createRangesForDeletion(TabletMetadata tabletMetadata,
      final KeyExtent deleteRange) {
    final KeyExtent tabletExtent = tabletMetadata.getExtent();

    // If the delete range wholly contains the tablet being deleted then there is no range to clip
    // files to because the files should be completely dropped.
    Preconditions.checkArgument(!deleteRange.contains(tabletExtent), "delete range:%s tablet:%s",
        deleteRange, tabletExtent);

    final List<Range> ranges = new ArrayList<>();

    if (deleteRange.overlaps(tabletExtent)) {
      if (deleteRange.prevEndRow() != null
          && tabletExtent.contains(followingRow(deleteRange.prevEndRow()))) {
        Manager.log.trace("Fencing tablet {} files to ({},{}]", tabletExtent,
            tabletExtent.prevEndRow(), deleteRange.prevEndRow());
        ranges.add(new Range(tabletExtent.prevEndRow(), false, deleteRange.prevEndRow(), true));
      }

      // This covers the case of when a deletion range overlaps the last tablet. We need to create a
      // range that excludes the deletion.
      if (deleteRange.endRow() != null
          && tabletMetadata.getExtent().contains(deleteRange.endRow())) {
        Manager.log.trace("Fencing tablet {} files to ({},{}]", tabletExtent, deleteRange.endRow(),
            tabletExtent.endRow());
        ranges.add(new Range(deleteRange.endRow(), false, tabletExtent.endRow(), true));
      }
    } else {
      Manager.log.trace(
          "Fencing tablet {} files to itself because it does not overlap delete range",
          tabletExtent);
      ranges.add(tabletExtent.toDataRange());
    }

    return ranges;
  }

  private Pair<KeyExtent,KeyExtent> updateMetadataRecordsForDelete(MergeInfo info)
      throws AccumuloException {
    final KeyExtent range = info.getExtent();

    String targetSystemTable = MetadataTable.NAME;
    if (range.isMeta()) {
      targetSystemTable = RootTable.NAME;
    }
    final Pair<KeyExtent,KeyExtent> startAndEndTablets;

    final AccumuloClient client = manager.getContext();

    try (BatchWriter bw = client.createBatchWriter(targetSystemTable)) {
      final Text startRow = range.prevEndRow();
      final Text endRow = range.endRow() != null
          ? new Key(range.endRow()).followingKey(PartialKey.ROW).getRow() : null;

      // Find the tablets that overlap the start and end row of the deletion range
      // If the startRow is null then there will be an empty startTablet we don't need
      // to fence a starting tablet as we are deleting everything up to the end tablet
      // Likewise, if the endRow is null there will be an empty endTablet as we are deleting
      // all tablets after the starting tablet
      final Optional<TabletMetadata> startTablet = Optional.ofNullable(startRow).flatMap(
          row -> loadTabletMetadata(range.tableId(), row, ColumnType.PREV_ROW, ColumnType.FILES));
      final Optional<TabletMetadata> endTablet = Optional.ofNullable(endRow).flatMap(
          row -> loadTabletMetadata(range.tableId(), row, ColumnType.PREV_ROW, ColumnType.FILES));

      // Store the tablets in a Map if present so that if we have the same Tablet we
      // only need to process the same tablet once when fencing
      final SortedMap<KeyExtent,TabletMetadata> tabletMetadatas = new TreeMap<>();
      startTablet.ifPresent(ft -> tabletMetadatas.put(ft.getExtent(), ft));
      endTablet.ifPresent(lt -> tabletMetadatas.putIfAbsent(lt.getExtent(), lt));

      // Capture the tablets to return them or null if not loaded
      startAndEndTablets = new Pair<>(startTablet.map(TabletMetadata::getExtent).orElse(null),
          endTablet.map(TabletMetadata::getExtent).orElse(null));

      for (TabletMetadata tabletMetadata : tabletMetadatas.values()) {
        final KeyExtent keyExtent = tabletMetadata.getExtent();

        // Check if this tablet needs to have its files fenced for the deletion
        if (needsFencingForDeletion(info, keyExtent)) {
          Manager.log.debug("Found overlapping keyExtent {} for delete, fencing files.", keyExtent);

          // Create the ranges for fencing the files, this takes the place of
          // chop compactions and splits
          final List<Range> ranges = createRangesForDeletion(tabletMetadata, range);
          Preconditions.checkState(!ranges.isEmpty(),
              "No ranges found that overlap deletion range.");

          // Go through and fence each of the files that are part of the tablet
          for (Entry<StoredTabletFile,DataFileValue> entry : tabletMetadata.getFilesMap()
              .entrySet()) {
            final StoredTabletFile existing = entry.getKey();
            final DataFileValue value = entry.getValue();

            final Mutation m = new Mutation(keyExtent.toMetaRow());

            // Go through each range that was created and modify the metadata for the file
            // The end row should be inclusive for the current tablet and the previous end row
            // should be exclusive for the start row.
            final Set<StoredTabletFile> newFiles = new HashSet<>();
            final Set<StoredTabletFile> existingFile = Set.of(existing);

            for (Range fenced : ranges) {
              // Clip range with the tablet range if the range already exists
              fenced = existing.hasRange() ? existing.getRange().clip(fenced, true) : fenced;

              // If null the range is disjoint which can happen if there are existing fenced files
              // If the existing file is disjoint then later we will delete if the file is not part
              // of the newFiles set which means it is disjoint with all ranges
              if (fenced != null) {
                final StoredTabletFile newFile = StoredTabletFile.of(existing.getPath(), fenced);
                Manager.log.trace("Adding new file {} with range {}", newFile.getMetadataPath(),
                    newFile.getRange());

                // Add the new file to the newFiles set, it will be added later if it doesn't match
                // the existing file already. We still need to add to the set to be checked later
                // even if it matches the existing file as later the deletion logic will check to
                // see if the existing file is part of this set before deleting. This is done to
                // make sure the existing file isn't deleted unless it is not needed/disjoint
                // with all ranges.
                newFiles.add(newFile);
              } else {
                Manager.log.trace("Found a disjoint file {} with  range {} on delete",
                    existing.getMetadataPath(), existing.getRange());
              }
            }

            // If the existingFile is not contained in the newFiles set then we can delete it
            Sets.difference(existingFile, newFiles).forEach(
                delete -> m.putDelete(DataFileColumnFamily.NAME, existing.getMetadataText()));

            // Add any new files that don't match the existingFile
            // As of now we will only have at most 2 files as up to 2 ranges are created
            final List<StoredTabletFile> filesToAdd =
                new ArrayList<>(Sets.difference(newFiles, existingFile));
            Preconditions.checkArgument(filesToAdd.size() <= 2,
                "There should only be at most 2 StoredTabletFiles after computing new ranges.");

            // If more than 1 new file then re-calculate the num entries and size
            if (filesToAdd.size() == 2) {
              // This splits up the values in half and makes sure they total the original
              // values
              final Pair<DataFileValue,DataFileValue> newDfvs = computeNewDfv(value);
              m.put(DataFileColumnFamily.NAME, filesToAdd.get(0).getMetadataText(),
                  newDfvs.getFirst().encodeAsValue());
              m.put(DataFileColumnFamily.NAME, filesToAdd.get(1).getMetadataText(),
                  newDfvs.getSecond().encodeAsValue());
            } else {
              // Will be 0 or 1 files
              filesToAdd.forEach(newFile -> m.put(DataFileColumnFamily.NAME,
                  newFile.getMetadataText(), value.encodeAsValue()));
            }

            if (!m.getUpdates().isEmpty()) {
              bw.addMutation(m);
            }
          }
        } else {
          Manager.log.debug(
              "Skipping metadata update on file for keyExtent {} for delete as not overlapping on rows.",
              keyExtent);
        }
      }

      bw.flush();

      return startAndEndTablets;
    } catch (Exception ex) {
      throw new AccumuloException(ex);
    }
  }

  // Divide each new DFV in half and make sure the sum equals the original
  @VisibleForTesting
  protected static Pair<DataFileValue,DataFileValue> computeNewDfv(DataFileValue value) {
    final DataFileValue file1Value = new DataFileValue(Math.max(1, value.getSize() / 2),
        Math.max(1, value.getNumEntries() / 2), value.getTime());

    final DataFileValue file2Value =
        new DataFileValue(Math.max(1, value.getSize() - file1Value.getSize()),
            Math.max(1, value.getNumEntries() - file1Value.getNumEntries()), value.getTime());

    return new Pair<>(file1Value, file2Value);
  }

  private Optional<TabletMetadata> loadTabletMetadata(TableId tabletId, final Text row,
      ColumnType... columns) {
    try (TabletsMetadata tabletsMetadata = manager.getContext().getAmple().readTablets()
        .forTable(tabletId).overlapping(row, true, row).fetch(columns).build()) {
      return tabletsMetadata.stream().findFirst();
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

  private void handleDeadTablets(TabletLists tLists)
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
      markDeadServerLogsAsClosed(walStateManager, deadLogs);
      manager.nextEvent.event(store.getLevel(),
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

  private final Lock flushLock = new ReentrantLock();

  private void flushChanges(TabletLists tLists)
      throws DistributedStoreException, TException, WalMarkerException {
    var unassigned = Collections.unmodifiableMap(tLists.unassigned);

    flushLock.lock();
    try {
      // This method was originally only ever called by one thread. The code was modified so that
      // two threads could possibly call this flush method concurrently. It is not clear the
      // following methods are thread safe so a lock is acquired out of caution. Balancer plugins
      // may not expect multiple threads to call them concurrently, Accumulo has not done this in
      // the past. The log recovery code needs to be evaluated for thread safety.
      handleDeadTablets(tLists);

      getAssignmentsFromBalancer(tLists, unassigned);
    } finally {
      flushLock.unlock();
    }

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

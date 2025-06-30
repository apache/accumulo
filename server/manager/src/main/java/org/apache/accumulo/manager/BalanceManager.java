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
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.apache.accumulo.manager.Manager.ONE_SECOND;
import static org.apache.accumulo.manager.Manager.WAIT_BETWEEN_ERRORS;

import java.util.Collections;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.manager.balancer.AssignmentParamsImpl;
import org.apache.accumulo.core.manager.balancer.BalanceParamsImpl;
import org.apache.accumulo.core.manager.balancer.TServerStatusImpl;
import org.apache.accumulo.core.manager.balancer.TabletServerIdImpl;
import org.apache.accumulo.core.manager.state.tables.TableState;
import org.apache.accumulo.core.manager.thrift.TableInfo;
import org.apache.accumulo.core.manager.thrift.TabletServerStatus;
import org.apache.accumulo.core.metadata.SystemTables;
import org.apache.accumulo.core.metadata.TServerInstance;
import org.apache.accumulo.core.metadata.schema.Ample;
import org.apache.accumulo.core.metadata.schema.TabletMetadata;
import org.apache.accumulo.core.metadata.schema.filters.HasMigrationFilter;
import org.apache.accumulo.core.metrics.MetricsProducer;
import org.apache.accumulo.core.spi.balancer.BalancerEnvironment;
import org.apache.accumulo.core.spi.balancer.DoNothingBalancer;
import org.apache.accumulo.core.spi.balancer.TabletBalancer;
import org.apache.accumulo.core.spi.balancer.data.TServerStatus;
import org.apache.accumulo.core.spi.balancer.data.TabletMigration;
import org.apache.accumulo.core.spi.balancer.data.TabletServerId;
import org.apache.accumulo.core.trace.TraceUtil;
import org.apache.accumulo.core.util.Timer;
import org.apache.accumulo.core.util.UtilWaitThread;
import org.apache.accumulo.core.util.threads.Threads;
import org.apache.accumulo.manager.metrics.BalancerMetrics;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.manager.balancer.BalancerEnvironmentImpl;
import org.apache.accumulo.server.manager.state.UnassignedTablet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

import io.opentelemetry.api.trace.Span;
import io.opentelemetry.context.Scope;

class BalanceManager {

  private static final Logger log = LoggerFactory.getLogger(BalanceManager.class);

  private final AtomicReference<Manager> manager;
  // all access to this should be through getBalancer()
  private TabletBalancer tabletBalancer;
  private volatile BalancerEnvironment balancerEnvironment;
  private final BalancerMetrics balancerMetrics = new BalancerMetrics();
  private final Object balancedNotifier = new Object();
  private static final long CLEANUP_INTERVAL_MINUTES = Manager.CLEANUP_INTERVAL_MINUTES;

  private AtomicLong runCounter = new AtomicLong(0);
  private Map<Ample.DataLevel,LastRunInfo> lastRunInfo = new EnumMap<>(Ample.DataLevel.class);

  BalanceManager() {
    this.manager = new AtomicReference<>(null);
    for (var dl : Ample.DataLevel.values()) {
      lastRunInfo.put(dl, new LastRunInfo(0, 0));
    }
  }

  void setManager(Manager manager) {
    Objects.requireNonNull(manager);
    if (this.manager.compareAndSet(null, manager)) {
      this.balancerEnvironment = new BalancerEnvironmentImpl(manager.getContext());
    } else if (this.manager.get() != manager) {
      throw new IllegalStateException("Attempted to set different manager object");
    }
  }

  private Manager getManager() {
    // fail fast if not yet set
    return Objects.requireNonNull(manager.get(), "Manager has not been set.");
  }

  synchronized TabletBalancer getBalancer() {
    String configuredBalancerClass =
        getManager().getConfiguration().get(Property.MANAGER_TABLET_BALANCER);
    try {
      if (tabletBalancer == null
          || !tabletBalancer.getClass().getName().equals(configuredBalancerClass)) {
        log.debug("Attempting to initialize balancer using class {}, was {}",
            configuredBalancerClass,
            tabletBalancer == null ? "null" : tabletBalancer.getClass().getName());
        var localTabletBalancer =
            Property.createInstanceFromPropertyName(getManager().getConfiguration(),
                Property.MANAGER_TABLET_BALANCER, TabletBalancer.class, new DoNothingBalancer());
        localTabletBalancer.init(balancerEnvironment);
        tabletBalancer = localTabletBalancer;
        log.info("tablet balancer changed to {}", localTabletBalancer.getClass().getName());
      }
    } catch (Exception e) {
      log.warn("Failed to create balancer {} using {} instead", configuredBalancerClass,
          DoNothingBalancer.class, e);
      var localTabletBalancer = new DoNothingBalancer();
      localTabletBalancer.init(balancerEnvironment);
      tabletBalancer = localTabletBalancer;
    }

    return tabletBalancer;
  }

  private ServerContext getContext() {
    return getManager().getContext();
  }

  MetricsProducer getMetrics() {
    return balancerMetrics;
  }

  /**
   * balanceTablets() balances tables by DataLevel. Return the current set of migrations partitioned
   * by DataLevel
   */
  private Set<KeyExtent> getMigrations(Ample.DataLevel dl) {
    Set<KeyExtent> extents = new HashSet<>();
    // prev row needed for the extent
    try (var tabletsMetadata = getContext()
        .getAmple().readTablets().forLevel(dl).fetch(TabletMetadata.ColumnType.PREV_ROW,
            TabletMetadata.ColumnType.MIGRATION, TabletMetadata.ColumnType.LOCATION)
        .filter(new HasMigrationFilter()).build()) {
      // filter out migrations that are awaiting cleanup
      tabletsMetadata.stream().filter(tm -> !shouldCleanupMigration(tm))
          .forEach(tm -> extents.add(tm.getExtent()));
    }

    return extents;
  }

  /**
   * Given the current tserverStatus map and a DataLevel, return a view of the tserverStatus map
   * that only contains entries for tables in the DataLevel
   */
  private SortedMap<TServerInstance,TabletServerStatus> createTServerStatusView(
      final Ample.DataLevel dl, final SortedMap<TServerInstance,TabletServerStatus> status) {
    final SortedMap<TServerInstance,TabletServerStatus> tserverStatusForLevel = new TreeMap<>();
    final String METADATA_TABLE_ID = SystemTables.METADATA.tableId().canonical();
    final String ROOT_TABLE_ID = SystemTables.ROOT.tableId().canonical();
    status.forEach((tsi, tss) -> {
      final TabletServerStatus copy = tss.deepCopy();
      final Map<String,TableInfo> oldTableMap = copy.getTableMap();
      final Map<String,TableInfo> newTableMap =
          new HashMap<>(dl == Ample.DataLevel.USER ? oldTableMap.size() : 1);
      switch (dl) {
        case ROOT: {
          var tableInfo = oldTableMap.get(ROOT_TABLE_ID);
          if (tableInfo != null) {
            newTableMap.put(ROOT_TABLE_ID, tableInfo);
          }
          break;
        }
        case METADATA: {
          var tableInfo = oldTableMap.get(METADATA_TABLE_ID);
          if (tableInfo != null) {
            newTableMap.put(METADATA_TABLE_ID, tableInfo);
          }
          break;
        }
        case USER:
          if (!oldTableMap.containsKey(METADATA_TABLE_ID)
              && !oldTableMap.containsKey(ROOT_TABLE_ID)) {
            newTableMap.putAll(oldTableMap);
          } else {
            oldTableMap.forEach((table, info) -> {
              if (!table.equals(ROOT_TABLE_ID) && !table.equals(METADATA_TABLE_ID)) {
                newTableMap.put(table, info);
              }
            });
          }
          break;

        default:
          throw new IllegalArgumentException("Unhandled DataLevel value: " + dl);
      }
      copy.setTableMap(newTableMap);
      tserverStatusForLevel.put(tsi, copy);
    });
    return tserverStatusForLevel;
  }

  private Map<String,TableId> getTablesForLevel(Ample.DataLevel dataLevel) {
    switch (dataLevel) {
      case ROOT:
        return Map.of(SystemTables.ROOT.tableName(), SystemTables.ROOT.tableId());
      case METADATA:
        return Map.of(SystemTables.METADATA.tableName(), SystemTables.METADATA.tableId());
      case USER: {
        Map<String,TableId> userTables = getContext().createQualifiedTableNameToIdMap();
        for (var accumuloTable : SystemTables.values()) {
          if (Ample.DataLevel.of(accumuloTable.tableId()) != Ample.DataLevel.USER) {
            userTables.remove(accumuloTable.tableName());
          }
        }
        return Collections.unmodifiableMap(userTables);
      }
      default:
        throw new IllegalArgumentException("Unknown data level " + dataLevel);
    }
  }

  private List<TabletMigration> checkMigrationSanity(Set<TabletServerId> current,
      List<TabletMigration> migrations, Ample.DataLevel level) {
    return migrations.stream().filter(m -> {
      boolean includeMigration = false;
      if (m.getTablet() == null) {
        log.error("Balancer gave back a null tablet {}", m);
      } else if (Ample.DataLevel.of(m.getTablet().getTable()) != level) {
        log.warn(
            "Balancer wants to move a tablet ({}) outside of the current processing level ({}), "
                + "ignoring and should be processed at the correct level ({})",
            m.getTablet(), level, Ample.DataLevel.of(m.getTablet().getTable()));
      } else if (m.getNewTabletServer() == null) {
        log.error("Balancer did not set the destination {}", m);
      } else if (m.getOldTabletServer() == null) {
        log.error("Balancer did not set the source {}", m);
      } else if (!current.contains(m.getOldTabletServer())) {
        log.warn("Balancer wants to move a tablet from a server that is not current: {}", m);
      } else if (!current.contains(m.getNewTabletServer())) {
        log.warn("Balancer wants to move a tablet to a server that is not current: {}", m);
      } else {
        includeMigration = true;
      }
      return includeMigration;
    }).collect(Collectors.toList());
  }

  private boolean canBalance(Ample.DataLevel dl, Manager.TServerStatus tservers) {
    if (dl == Ample.DataLevel.USER) {

      if (!canAssignAndBalance()) {
        log.debug("Not balancing user tablets because not enough tablet servers");
        return false;
      }

      final int tabletsNotHosted = getManager().notHosted();
      if (tabletsNotHosted > 0) {
        log.debug("not balancing user tablets because there are {} unhosted tablets",
            tabletsNotHosted);
        return false;
      }
    }

    return getManager().canBalance(dl, tservers);
  }

  private static class LastRunInfo {
    private final long runCount;
    private final long migrations;

    LastRunInfo(long runCount, long migrations) {
      this.runCount = runCount;
      this.migrations = migrations;
    }

    @Override
    public String toString() {
      return "runCount:" + runCount + " migrations:" + migrations;
    }
  }

  private void balanceCompleted(Ample.DataLevel level, long migrations) {
    log.trace("Balance completed {} migrations {}", level, migrations);

    synchronized (balancedNotifier) {
      lastRunInfo.put(level, new LastRunInfo(runCounter.getAndIncrement(), migrations));
      balancedNotifier.notifyAll();
    }
  }

  /**
   * Waits for the given data levels to complete a run of balancing with zero migrations.
   */
  private void waitForBalance(Set<Ample.DataLevel> levels) {
    synchronized (balancedNotifier) {
      var snapshot = new EnumMap<Ample.DataLevel,Long>(Ample.DataLevel.class);
      for (var dl : levels) {
        snapshot.put(dl, lastRunInfo.get(dl).runCount);
      }

      log.trace("waitForBalance levels:{} snapshot:{}", levels, snapshot);

      while (!snapshot.isEmpty()) {
        try {
          balancedNotifier.wait();
        } catch (InterruptedException e) {
          log.debug(e.toString(), e);
        }

        log.trace("waitForBalance levels:{} snapshot:{}  lastRunInfo:{}", levels, snapshot,
            lastRunInfo);

        snapshot.entrySet().removeIf(entry -> {
          var dataLevel = entry.getKey();
          var snapRunCount = entry.getValue();
          var lastRunInfo = this.lastRunInfo.get(dataLevel);
          // check if balancing has run for this level since entering this method and if it had zero
          // migrations
          return snapRunCount < lastRunInfo.runCount && lastRunInfo.migrations == 0;
        });
      }
    }
  }

  void waitForBalance() {
    waitForBalance(EnumSet.allOf(Ample.DataLevel.class));
    int unassigned = getManager().displayUnassigned();
    while (unassigned > 0) {
      log.debug("displayUnassigned():{}", unassigned);
      UtilWaitThread.sleep(50);
      unassigned = getManager().displayUnassigned();
    }
  }

  void getAssignments(SortedMap<TServerInstance,TabletServerStatus> currentStatus,
      Map<String,Set<TServerInstance>> currentTServerGroups,
      Map<KeyExtent,UnassignedTablet> unassigned, Map<KeyExtent,TServerInstance> assignedOut) {
    AssignmentParamsImpl params =
        AssignmentParamsImpl.fromThrift(currentStatus, currentTServerGroups,
            unassigned.entrySet().stream().collect(HashMap::new,
                (m, e) -> m.put(e.getKey(),
                    e.getValue().getLastLocation() == null ? null
                        : e.getValue().getLastLocation().getServerInstance()),
                Map::putAll),
            assignedOut);
    getBalancer().getAssignments(params);
    if (!canAssignAndBalance()) {
      // remove assignment for user tables
      Iterator<KeyExtent> iter = assignedOut.keySet().iterator();
      while (iter.hasNext()) {
        KeyExtent ke = iter.next();
        if (!ke.isMeta()) {
          iter.remove();
          log.trace("Removed assignment for {} as assignments for user tables is disabled.", ke);
        }
      }
    }
  }

  private boolean canAssignAndBalance() {
    final int threshold = getManager().getConfiguration()
        .getCount(Property.MANAGER_TABLET_BALANCER_TSERVER_THRESHOLD);
    if (threshold == 0) {
      return true;
    }
    final int numTServers = getManager().tserverSet.size();
    final boolean result = numTServers >= threshold;
    if (!result) {
      log.warn("Not assigning or balancing as number of tservers ({}) is below threshold ({})",
          numTServers, threshold);
    }
    return result;
  }

  private boolean shouldCleanupMigration(TabletMetadata tabletMetadata) {
    var tableState = getContext().getTableManager().getTableState(tabletMetadata.getTableId());
    var migration = tabletMetadata.getMigration();
    Preconditions.checkState(migration != null,
        "This method should only be called if there is a migration");
    return tableState == TableState.OFFLINE
        || !getManager().onlineTabletServers().contains(migration)
        || (tabletMetadata.getLocation() != null
            && tabletMetadata.getLocation().getServerInstance().equals(migration));
  }

  void startBackGroundTask() {
    Threads.createCriticalThread("Migration Cleanup Thread", new MigrationCleanupThread()).start();
    for (var dataLevel : Ample.DataLevel.values()) {
      Threads.createCriticalThread(dataLevel + " balancer", new BalancerThread(dataLevel)).start();
    }
  }

  private class MigrationCleanupThread implements Runnable {

    @Override
    public void run() {
      while (getManager().stillManager()) {
        try {
          // - Remove any migrations for tablets of offline tables, as the migration can never
          // succeed because no tablet server will load the tablet
          // - Remove any migrations to tablet servers that are not live
          // - Remove any migrations where the tablets current location equals the migration
          // (the migration has completed)
          var ample = getContext().getAmple();
          for (Ample.DataLevel dl : Ample.DataLevel.values()) {
            // prev row needed for the extent
            try (var tabletsMetadata = ample.readTablets().forLevel(dl)
                .fetch(TabletMetadata.ColumnType.PREV_ROW, TabletMetadata.ColumnType.MIGRATION,
                    TabletMetadata.ColumnType.LOCATION)
                .filter(new HasMigrationFilter()).build();
                var tabletsMutator = ample.conditionallyMutateTablets(result -> {})) {
              for (var tabletMetadata : tabletsMetadata) {
                var migration = tabletMetadata.getMigration();
                if (shouldCleanupMigration(tabletMetadata)) {
                  tabletsMutator.mutateTablet(tabletMetadata.getExtent()).requireAbsentOperation()
                      .requireMigration(migration).deleteMigration().submit(tm -> false);
                }
              }
            }
          }
        } catch (Exception ex) {
          log.error("Error cleaning up migrations", ex);
        }
        sleepUninterruptibly(CLEANUP_INTERVAL_MINUTES, MINUTES);
      }
    }
  }

  private class BalancerThread implements Runnable {
    private final Ample.DataLevel dataLevel;
    private Manager.TServerStatus lastStatus = null;

    public BalancerThread(Ample.DataLevel dataLevel) {
      this.dataLevel = dataLevel;
    }

    @Override
    public void run() {
      while (getManager().stillManager()) {
        Span span = TraceUtil.startSpan(this.getClass(), "run::balanceTablets");
        try (Scope scope = span.makeCurrent()) {
          Timer timer = Timer.startNew();
          if (dataLevel == Ample.DataLevel.USER) {
            waitForBalance(EnumSet.of(Ample.DataLevel.METADATA, Ample.DataLevel.ROOT));
            log.trace("{} waiting for balance of {} and {} took {}ms", dataLevel,
                Ample.DataLevel.METADATA, Ample.DataLevel.ROOT, timer.elapsed(MILLISECONDS));
          } else if (dataLevel == Ample.DataLevel.METADATA) {
            waitForBalance(EnumSet.of(Ample.DataLevel.ROOT));
            log.trace("{} waiting for balance of {} took {}ms", dataLevel, Ample.DataLevel.ROOT,
                timer.elapsed(MILLISECONDS));
          }

          timer.restart();
          var tservers = getManager().getTserverStatus(lastStatus);
          log.trace("{} getting tserver status took {}ms and returned {} {}", dataLevel,
              timer.elapsed(MILLISECONDS), tservers.snapshot.getTservers().size(),
              tservers.status.size());
          lastStatus = tservers;

          if (!canBalance(dataLevel, tservers)) {
            continue;
          }

          var wait = balance(tservers);
          UtilWaitThread.sleep(wait);
        } catch (Exception e) {
          TraceUtil.setException(span, e, false);
          log.error("Error balancing tablets for {} will wait for {} (seconds) and then retry ",
              dataLevel, WAIT_BETWEEN_ERRORS / ONE_SECOND, e);
          sleepUninterruptibly(WAIT_BETWEEN_ERRORS, MILLISECONDS);
        } finally {
          span.end();
        }
      }
    }

    private long balance(Manager.TServerStatus tservers) {

      var existingMigrations = getMigrations(dataLevel);

      // Create a view of the tserver status such that it only contains the tables
      // for this level in the tableMap.
      SortedMap<TServerInstance,TabletServerStatus> tserverStatusForLevel =
          createTServerStatusView(dataLevel, tservers.status);
      // Construct the Thrift variant of the map above for the BalancerParams
      final SortedMap<TabletServerId,TServerStatus> tserverStatusForBalancerLevel = new TreeMap<>();
      tserverStatusForLevel.forEach((tsi, status) -> tserverStatusForBalancerLevel
          .put(new TabletServerIdImpl(tsi), TServerStatusImpl.fromThrift(status)));

      log.debug("Balancing for tables at level {}", dataLevel);

      var params = BalanceParamsImpl.fromThrift(tserverStatusForBalancerLevel,
          tservers.snapshot.getTserverGroups(), tserverStatusForLevel, existingMigrations,
          dataLevel, getTablesForLevel(dataLevel));
      var wait = getBalancer().balance(params);
      long migrationsOutForLevel = 0;
      try (var tabletsMutator = getContext().getAmple().conditionallyMutateTablets(result -> {})) {
        for (TabletMigration m : checkMigrationSanity(tserverStatusForBalancerLevel.keySet(),
            params.migrationsOut(), dataLevel)) {
          final KeyExtent ke = KeyExtent.fromTabletId(m.getTablet());
          if (existingMigrations.contains(ke)) {
            log.warn("balancer requested migration more than once, skipping {}", m);
            continue;
          }
          migrationsOutForLevel++;
          var migration = TabletServerIdImpl.toThrift(m.getNewTabletServer());
          tabletsMutator.mutateTablet(ke).requireAbsentOperation()
              .requireCurrentLocationNotEqualTo(migration).putMigration(migration)
              .submit(tm -> false);
          log.debug("migration {}", m);
        }

        balanceCompleted(dataLevel, migrationsOutForLevel + existingMigrations.size());
        if (migrationsOutForLevel > 0) {
          // signal the tablet group watcher for this data level that it needs to start working on migrations
          getManager().nextEvent.event(dataLevel, "%s migrating %d more tablets, %d total",
              dataLevel, migrationsOutForLevel, migrationsOutForLevel + existingMigrations.size());
        }
      }
      return wait;
    }
  }
}

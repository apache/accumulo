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

import java.util.Collections;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.stream.Collectors;

import org.apache.accumulo.core.conf.DeprecatedPropertyUtil;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.manager.balancer.AssignmentParamsImpl;
import org.apache.accumulo.core.manager.balancer.BalanceParamsImpl;
import org.apache.accumulo.core.manager.balancer.TServerStatusImpl;
import org.apache.accumulo.core.manager.balancer.TabletServerIdImpl;
import org.apache.accumulo.core.manager.thrift.TableInfo;
import org.apache.accumulo.core.manager.thrift.TabletServerStatus;
import org.apache.accumulo.core.metadata.SystemTables;
import org.apache.accumulo.core.metadata.TServerInstance;
import org.apache.accumulo.core.metadata.schema.Ample;
import org.apache.accumulo.core.metadata.schema.TabletMetadata;
import org.apache.accumulo.core.metrics.MetricsProducer;
import org.apache.accumulo.core.spi.balancer.BalancerEnvironment;
import org.apache.accumulo.core.spi.balancer.TableLoadBalancer;
import org.apache.accumulo.core.spi.balancer.TabletBalancer;
import org.apache.accumulo.core.spi.balancer.data.TServerStatus;
import org.apache.accumulo.core.spi.balancer.data.TabletMigration;
import org.apache.accumulo.core.spi.balancer.data.TabletServerId;
import org.apache.accumulo.manager.metrics.BalancerMetrics;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.manager.balancer.BalancerEnvironmentImpl;
import org.apache.accumulo.server.manager.state.UnassignedTablet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

public class BalanceManager {

  private static final Logger log = LoggerFactory.getLogger(BalanceManager.class);

  private final Manager manager;
  protected volatile TabletBalancer tabletBalancer;
  private final BalancerEnvironment balancerEnvironment;
  private final BalancerMetrics balancerMetrics = new BalancerMetrics();
  private final Object balancedNotifier = new Object();

  BalanceManager(Manager manager) {
    this.manager = manager;
    this.balancerEnvironment = new BalancerEnvironmentImpl(manager.getContext());
    initializeBalancer();
  }

  private void initializeBalancer() {
    var localTabletBalancer =
        Property.createInstanceFromPropertyName(getContext().getConfiguration(),
            Property.MANAGER_TABLET_BALANCER, TabletBalancer.class, new TableLoadBalancer());
    localTabletBalancer.init(balancerEnvironment);
    tabletBalancer = localTabletBalancer;
  }

  void propertyChanged(String property) {
    String resolved = DeprecatedPropertyUtil.getReplacementName(property, (log, replacement) -> {});
    if (resolved.equals(Property.MANAGER_TABLET_BALANCER.getKey())) {
      initializeBalancer();
      log.info("tablet balancer changed to {}", tabletBalancer.getClass().getName());
    }
  }

  private ServerContext getContext() {
    return manager.getContext();
  }

  MetricsProducer getMetrics() {
    return balancerMetrics;
  }

  /**
   * balanceTablets() balances tables by DataLevel. Return the current set of migrations partitioned
   * by DataLevel
   */
  private Map<Ample.DataLevel,Set<KeyExtent>> partitionMigrations() {
    final Map<Ample.DataLevel,Set<KeyExtent>> partitionedMigrations =
        new EnumMap<>(Ample.DataLevel.class);
    for (Ample.DataLevel dl : Ample.DataLevel.values()) {
      Set<KeyExtent> extents = new HashSet<>();
      // prev row needed for the extent
      try (
          var tabletsMetadata = getContext()
              .getAmple().readTablets().forLevel(dl).fetch(TabletMetadata.ColumnType.PREV_ROW,
                  TabletMetadata.ColumnType.LOCATION, TabletMetadata.ColumnType.MIGRATION)
              .build()) {
        // filter out migrations that are awaiting cleanup
        tabletsMetadata.stream()
            .filter(tm -> tm.getMigration() != null && !manager.shouldCleanupMigration(tm))
            .forEach(tm -> extents.add(tm.getExtent()));
      }
      partitionedMigrations.put(dl, extents);
    }
    return partitionedMigrations;
  }

  /**
   * Given the current tserverStatus map and a DataLevel, return a view of the tserverStatus map
   * that only contains entries for tables in the DataLevel
   */
  private SortedMap<TServerInstance,TabletServerStatus> createTServerStatusView(
      final Ample.DataLevel dl, final SortedMap<TServerInstance,TabletServerStatus> status) {
    final SortedMap<TServerInstance,TabletServerStatus> tserverStatusForLevel = new TreeMap<>();
    status.forEach((tsi, tss) -> {
      final TabletServerStatus copy = tss.deepCopy();
      final Map<String,TableInfo> oldTableMap = copy.getTableMap();
      final Map<String,TableInfo> newTableMap =
          new HashMap<>(dl == Ample.DataLevel.USER ? oldTableMap.size() : 1);
      if (dl == Ample.DataLevel.ROOT) {
        if (oldTableMap.containsKey(SystemTables.ROOT.tableId().canonical())) {
          newTableMap.put(SystemTables.ROOT.tableId().canonical(),
              oldTableMap.get(SystemTables.ROOT.tableId().canonical()));
        }
      } else if (dl == Ample.DataLevel.METADATA) {
        if (oldTableMap.containsKey(SystemTables.METADATA.tableId().canonical())) {
          newTableMap.put(SystemTables.METADATA.tableId().canonical(),
              oldTableMap.get(SystemTables.METADATA.tableId().canonical()));
        }
      } else if (dl == Ample.DataLevel.USER) {
        if (!oldTableMap.containsKey(SystemTables.METADATA.tableId().canonical())
            && !oldTableMap.containsKey(SystemTables.ROOT.tableId().canonical())) {
          newTableMap.putAll(oldTableMap);
        } else {
          oldTableMap.forEach((table, info) -> {
            if (!table.equals(SystemTables.ROOT.tableId().canonical())
                && !table.equals(SystemTables.METADATA.tableId().canonical())) {
              newTableMap.put(table, info);
            }
          });
        }
      } else {
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

  long balanceTablets() {

    final int tabletsNotHosted = manager.notHosted();
    BalanceParamsImpl params = null;
    long wait = 0;
    long totalMigrationsOut = 0;
    final Map<Ample.DataLevel,Set<KeyExtent>> partitionedMigrations = partitionMigrations();
    int levelsCompleted = 0;

    for (Ample.DataLevel dl : Ample.DataLevel.values()) {
      if (dl == Ample.DataLevel.USER && tabletsNotHosted > 0) {
        log.debug("not balancing user tablets because there are {} unhosted tablets",
            tabletsNotHosted);
        continue;
      }

      if ((dl == Ample.DataLevel.METADATA || dl == Ample.DataLevel.USER)
          && !partitionedMigrations.get(Ample.DataLevel.ROOT).isEmpty()) {
        log.debug("Not balancing {} because {} has migrations", dl, Ample.DataLevel.ROOT);
        continue;
      }

      if (dl == Ample.DataLevel.USER
          && !partitionedMigrations.get(Ample.DataLevel.METADATA).isEmpty()) {
        log.debug("Not balancing {} because {} has migrations", dl, Ample.DataLevel.METADATA);
        continue;
      }

      // Create a view of the tserver status such that it only contains the tables
      // for this level in the tableMap.
      SortedMap<TServerInstance,TabletServerStatus> tserverStatusForLevel =
          createTServerStatusView(dl, manager.tserverStatus);
      // Construct the Thrift variant of the map above for the BalancerParams
      final SortedMap<TabletServerId,TServerStatus> tserverStatusForBalancerLevel = new TreeMap<>();
      tserverStatusForLevel.forEach((tsi, status) -> tserverStatusForBalancerLevel
          .put(new TabletServerIdImpl(tsi), TServerStatusImpl.fromThrift(status)));

      log.debug("Balancing for tables at level {}", dl);

      SortedMap<TabletServerId,TServerStatus> statusForBalancerLevel =
          tserverStatusForBalancerLevel;
      params =
          BalanceParamsImpl.fromThrift(statusForBalancerLevel, manager.tServerGroupingForBalancer,
              tserverStatusForLevel, partitionedMigrations.get(dl), dl, getTablesForLevel(dl));
      wait = Math.max(tabletBalancer.balance(params), wait);
      long migrationsOutForLevel = 0;
      try (var tabletsMutator = getContext().getAmple().conditionallyMutateTablets(result -> {})) {
        for (TabletMigration m : checkMigrationSanity(statusForBalancerLevel.keySet(),
            params.migrationsOut(), dl)) {
          final KeyExtent ke = KeyExtent.fromTabletId(m.getTablet());
          if (partitionedMigrations.get(dl).contains(ke)) {
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
      }
      totalMigrationsOut += migrationsOutForLevel;

      // increment this at end of loop to signal complete run w/o any continue
      levelsCompleted++;
    }
    final long totalMigrations =
        totalMigrationsOut + partitionedMigrations.values().stream().mapToLong(Set::size).sum();
    balancerMetrics.assignMigratingCount(() -> totalMigrations);

    if (totalMigrationsOut == 0 && levelsCompleted == Ample.DataLevel.values().length) {
      synchronized (balancedNotifier) {
        balancedNotifier.notifyAll();
      }
    } else if (totalMigrationsOut > 0) {
      manager.nextEvent.event("Migrating %d more tablets, %d total", totalMigrationsOut,
          totalMigrations);
    }
    return wait;
  }

  @SuppressFBWarnings(value = "UW_UNCOND_WAIT", justification = "TODO needs triage")
  public void waitForBalance() {
    synchronized (balancedNotifier) {
      long eventCounter;
      do {
        eventCounter = manager.nextEvent.waitForEvents(0, 0);
        try {
          balancedNotifier.wait();
        } catch (InterruptedException e) {
          log.debug(e.toString(), e);
        }
      } while (manager.displayUnassigned() > 0 || manager.numMigrations() > 0
          || eventCounter != manager.nextEvent.waitForEvents(0, 0));
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
    tabletBalancer.getAssignments(params);
  }

  public TabletBalancer getBalancer() {
    return tabletBalancer;
  }
}

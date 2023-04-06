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
package org.apache.accumulo.core.spi.balancer;

import static java.util.concurrent.TimeUnit.SECONDS;

import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.SortedMap;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.data.TabletId;
import org.apache.accumulo.core.manager.balancer.TabletServerIdImpl;
import org.apache.accumulo.core.spi.balancer.data.TServerStatus;
import org.apache.accumulo.core.spi.balancer.data.TableStatistics;
import org.apache.accumulo.core.spi.balancer.data.TabletMigration;
import org.apache.accumulo.core.spi.balancer.data.TabletServerId;
import org.apache.accumulo.core.spi.balancer.data.TabletStatistics;
import org.apache.accumulo.core.spi.balancer.util.ThrottledBalancerProblemReporter;
import org.apache.accumulo.core.spi.balancer.util.ThrottledBalancerProblemReporter.OutstandingMigrationsProblem;
import org.apache.accumulo.core.spi.balancer.util.ThrottledBalancerProblemReporter.Problem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A simple tablet balancer that attempts to spread tablets out evenly across all available tablet
 * servers. The goal is to achieve the same number of tablets on each tablet server.
 *
 * <p>
 * Initial assignments attempt to maintain locality by assigning tablets to their previous location
 * if possible, but otherwise assignments are made in a random fashion across all available tablet
 * servers.
 *
 * <p>
 * This balancer replaces the deprecated
 * org.apache.accumulo.server.master.balancer.DefaultLoadBalancer which will be removed in a future
 * release. This balancer has the same functionality but uses the stable SPI which does not expose
 * internal types on public methods.
 *
 * @since 2.1.0
 */
public class SimpleLoadBalancer implements TabletBalancer {

  private static final Logger log = LoggerFactory.getLogger(SimpleLoadBalancer.class);

  protected BalancerEnvironment environment;

  Iterator<TabletServerId> assignments;
  // if tableToBalance is set, then only balance the given table
  TableId tableToBalance = null;

  public SimpleLoadBalancer() {}

  public SimpleLoadBalancer(TableId table) {
    tableToBalance = table;
  }

  @Override
  public void init(BalancerEnvironment balancerEnvironment) {
    this.environment = balancerEnvironment;
  }

  List<TabletServerId> randomize(Set<TabletServerId> locations) {
    List<TabletServerId> result = new ArrayList<>(locations);
    Collections.shuffle(result);
    return result;
  }

  public TabletServerId getAssignment(SortedMap<TabletServerId,TServerStatus> locations,
      TabletServerId last) {
    if (locations.isEmpty()) {
      return null;
    }

    if (last != null) {
      // Maintain locality
      String fakeSessionID = " ";
      TabletServerId simple = new TabletServerIdImpl(last.getHost(), last.getPort(), fakeSessionID);
      Iterator<TabletServerId> find = locations.tailMap(simple).keySet().iterator();
      if (find.hasNext()) {
        TabletServerId current = find.next();
        if (current.getHost().equals(last.getHost())) {
          return current;
        }
      }
    }

    // The strategy here is to walk through the locations and hand them back, one at a time
    // Grab an iterator off of the set of options; use a new iterator if it hands back something not
    // in the current list.
    if (assignments == null || !assignments.hasNext()) {
      assignments = randomize(locations.keySet()).iterator();
    }
    TabletServerId result = assignments.next();
    if (!locations.containsKey(result)) {
      assignments = null;
      return randomize(locations.keySet()).iterator().next();
    }
    return result;
  }

  static class ServerCounts implements Comparable<ServerCounts> {
    public final TabletServerId server;
    public int count;
    public final TServerStatus status;

    ServerCounts(int count, TabletServerId server, TServerStatus status) {
      this.count = count;
      this.server = server;
      this.status = status;
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(server) + count;
    }

    @Override
    public boolean equals(Object obj) {
      return obj == this || (obj instanceof ServerCounts && compareTo((ServerCounts) obj) == 0);
    }

    @Override
    public int compareTo(ServerCounts obj) {
      int result = count - obj.count;
      if (result == 0) {
        return server.compareTo(obj.server);
      }
      return result;
    }
  }

  public boolean getMigrations(Map<TabletServerId,TServerStatus> current,
      List<TabletMigration> result) {
    boolean moreBalancingNeeded = false;
    try {
      // no moves possible
      if (current.size() < 2) {
        return false;
      }
      final Map<TableId,Map<TabletId,TabletStatistics>> donerTabletStats = new HashMap<>();

      // Sort by total number of online tablets, per server
      int total = 0;
      ArrayList<ServerCounts> totals = new ArrayList<>();
      for (Entry<TabletServerId,TServerStatus> entry : current.entrySet()) {
        int serverTotal = 0;
        if (entry.getValue() != null && entry.getValue().getTableMap() != null) {
          for (Entry<String,TableStatistics> e : entry.getValue().getTableMap().entrySet()) {
            /*
             * The check below was on entry.getKey(), but that resolves to a tabletserver not a
             * tablename. Believe it should be e.getKey() which is a tablename
             */
            if (tableToBalance == null || tableToBalance.canonical().equals(e.getKey())) {
              serverTotal += e.getValue().getOnlineTabletCount();
            }
          }
        }
        totals.add(new ServerCounts(serverTotal, entry.getKey(), entry.getValue()));
        total += serverTotal;
      }

      // order from low to high
      totals.sort(Collections.reverseOrder());
      int even = total / totals.size();
      int numServersOverEven = total % totals.size();

      // Move tablets from the servers with too many to the servers with
      // the fewest but only nominate tablets to move once. This allows us
      // to fill new servers with tablets from a mostly balanced server
      // very quickly. However, it may take several balancing passes to move
      // tablets from one hugely overloaded server to many slightly
      // under-loaded servers.
      int end = totals.size() - 1;
      int movedAlready = 0;
      int tooManyIndex = 0;
      while (tooManyIndex < end) {
        ServerCounts tooMany = totals.get(tooManyIndex);
        int goal = even;
        if (tooManyIndex < numServersOverEven) {
          goal++;
        }
        int needToUnload = tooMany.count - goal;
        ServerCounts tooLittle = totals.get(end);
        int needToLoad = goal - tooLittle.count - movedAlready;
        if (needToUnload < 1 && needToLoad < 1) {
          break;
        }
        if (needToUnload >= needToLoad) {
          result.addAll(move(tooMany, tooLittle, needToLoad, donerTabletStats));
          end--;
          movedAlready = 0;
        } else {
          result.addAll(move(tooMany, tooLittle, needToUnload, donerTabletStats));
          movedAlready += needToUnload;
        }
        if (needToUnload > needToLoad) {
          moreBalancingNeeded = true;
        } else {
          tooManyIndex++;
          donerTabletStats.clear();
        }
      }

    } finally {
      log.trace("balance ended with {} migrations", result.size());
    }
    return moreBalancingNeeded;
  }

  /**
   * Select a tablet based on differences between table loads; if the loads are even, use the
   * busiest table
   */
  List<TabletMigration> move(ServerCounts tooMuch, ServerCounts tooLittle, int count,
      Map<TableId,Map<TabletId,TabletStatistics>> donerTabletStats) {

    if (count == 0) {
      return Collections.emptyList();
    }

    List<TabletMigration> result = new ArrayList<>();
    // Copy counts so we can update them as we propose migrations
    Map<TableId,Integer> tooMuchMap = tabletCountsPerTable(tooMuch.status);
    Map<TableId,Integer> tooLittleMap = tabletCountsPerTable(tooLittle.status);

    for (int i = 0; i < count; i++) {
      TableId table = getTableToMigrate(tooMuch, tooMuchMap, tooLittleMap);

      Map<TabletId,TabletStatistics> onlineTabletsForTable = donerTabletStats.get(table);
      try {
        if (onlineTabletsForTable == null) {
          onlineTabletsForTable = new HashMap<>();
          List<TabletStatistics> stats = getOnlineTabletsForTable(tooMuch.server, table);
          if (stats == null) {
            log.warn("Unable to find tablets to move");
            return result;
          }
          for (TabletStatistics stat : stats) {
            onlineTabletsForTable.put(stat.getTabletId(), stat);
          }
          donerTabletStats.put(table, onlineTabletsForTable);
        }
      } catch (Exception ex) {
        log.error("Unable to select a tablet to move", ex);
        return result;
      }
      TabletId tabletId = selectTablet(onlineTabletsForTable);
      onlineTabletsForTable.remove(tabletId);
      if (tabletId == null) {
        return result;
      }
      tooMuchMap.put(table, tooMuchMap.get(table) - 1);
      /*
       * If a table grows from 1 tablet then tooLittleMap.get(table) can return a null, since there
       * is only one tabletserver that holds all of the tablets. Here we check to see if in fact
       * that is the case and if so set the value to 0.
       */
      Integer tooLittleCount = tooLittleMap.getOrDefault(table, 0);
      tooLittleMap.put(table, tooLittleCount + 1);
      tooMuch.count--;
      tooLittle.count++;
      result.add(new TabletMigration(tabletId, tooMuch.server, tooLittle.server));
    }
    return result;
  }

  private TableId getTableToMigrate(ServerCounts tooMuch, Map<TableId,Integer> tooMuchMap,
      Map<TableId,Integer> tooLittleMap) {

    if (tableToBalance != null) {
      return tableToBalance;
    }

    // find a table to migrate
    // look for an uneven table count
    Entry<TableId,Integer> biggestEntry = tooMuchMap.entrySet().stream().map(entry -> {
      TableId tableID = entry.getKey();
      int diff = entry.getValue() - tooLittleMap.getOrDefault(tableID, 0);
      return new SimpleEntry<>(tableID, diff); // map the table count to the difference
    }).max(Entry.comparingByValue()) // get the largest difference
        .orElseGet(() -> new SimpleEntry<>(null, 0));

    if (biggestEntry.getValue() < 2) {
      return busiest(tooMuch.status.getTableMap());
    } else {
      return biggestEntry.getKey();
    }
  }

  protected List<TabletStatistics> getOnlineTabletsForTable(TabletServerId tabletServerId,
      TableId tableId) throws AccumuloSecurityException, AccumuloException {
    return environment.listOnlineTabletsForTable(tabletServerId, tableId);
  }

  static Map<TableId,Integer> tabletCountsPerTable(TServerStatus status) {
    Map<TableId,Integer> result = new HashMap<>();
    if (status != null && status.getTableMap() != null) {
      Map<String,TableStatistics> tableMap = status.getTableMap();
      for (Entry<String,TableStatistics> entry : tableMap.entrySet()) {
        result.put(TableId.of(entry.getKey()), entry.getValue().getOnlineTabletCount());
      }
    }
    return result;
  }

  static TabletId selectTablet(Map<TabletId,TabletStatistics> extents) {
    if (extents.isEmpty()) {
      return null;
    }
    TabletId mostRecentlySplit = null;
    long splitTime = 0;
    for (Entry<TabletId,TabletStatistics> entry : extents.entrySet()) {
      if (entry.getValue().getSplitCreationTime() >= splitTime) {
        splitTime = entry.getValue().getSplitCreationTime();
        mostRecentlySplit = entry.getKey();
      }
    }
    return mostRecentlySplit;
  }

  // define what it means for a tablet to be busy
  private static TableId busiest(Map<String,TableStatistics> tables) {
    TableId result = null;
    double busiest = Double.NEGATIVE_INFINITY;
    for (Entry<String,TableStatistics> entry : tables.entrySet()) {
      TableStatistics info = entry.getValue();
      double busy = info.getIngestRate() + info.getQueryRate();
      if (busy > busiest) {
        busiest = busy;
        result = TableId.of(entry.getKey());
      }
    }
    return result;
  }

  @Override
  public void getAssignments(AssignmentParameters params) {
    params.unassignedTablets().forEach((tabletId, tserverId) -> params.addAssignment(tabletId,
        getAssignment(params.currentStatus(), tserverId)));
  }

  private final ThrottledBalancerProblemReporter problemReporter =
      new ThrottledBalancerProblemReporter(getClass());
  private final Problem noTserversProblem = problemReporter.createNoTabletServersProblem();
  private final OutstandingMigrationsProblem outstandingMigrationsProblem =
      problemReporter.createOutstandingMigrationsProblem();

  @Override
  public long balance(BalanceParameters params) {
    // do we have any servers?
    if (params.currentStatus().isEmpty()) {
      problemReporter.reportProblem(noTserversProblem);
    } else {
      // Don't migrate if we have migrations in progress
      if (params.currentMigrations().isEmpty()) {
        problemReporter.clearProblemReportTimes();
        if (getMigrations(params.currentStatus(), params.migrationsOut())) {
          return SECONDS.toMillis(1);
        }
      } else {
        outstandingMigrationsProblem.setMigrations(params.currentMigrations());
        problemReporter.reportProblem(outstandingMigrationsProblem);
      }
    }
    return 5_000;
  }

}

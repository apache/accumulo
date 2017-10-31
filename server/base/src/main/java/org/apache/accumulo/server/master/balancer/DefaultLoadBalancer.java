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
package org.apache.accumulo.server.master.balancer;

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

import org.apache.accumulo.core.client.impl.Table;
import org.apache.accumulo.core.data.impl.KeyExtent;
import org.apache.accumulo.core.master.thrift.TableInfo;
import org.apache.accumulo.core.master.thrift.TabletServerStatus;
import org.apache.accumulo.core.tabletserver.thrift.TabletStats;
import org.apache.accumulo.server.master.state.TServerInstance;
import org.apache.accumulo.server.master.state.TabletMigration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DefaultLoadBalancer extends TabletBalancer {

  private static final Logger log = LoggerFactory.getLogger(DefaultLoadBalancer.class);

  Iterator<TServerInstance> assignments;
  // if tableToBalance is set, then only balance the given table
  Table.ID tableToBalance = null;

  public DefaultLoadBalancer() {

  }

  public DefaultLoadBalancer(Table.ID table) {
    tableToBalance = table;
  }

  List<TServerInstance> randomize(Set<TServerInstance> locations) {
    List<TServerInstance> result = new ArrayList<>(locations);
    Collections.shuffle(result);
    return result;
  }

  public TServerInstance getAssignment(SortedMap<TServerInstance,TabletServerStatus> locations, KeyExtent extent, TServerInstance last) {
    if (locations.size() == 0)
      return null;

    if (last != null) {
      // Maintain locality
      String fakeSessionID = " ";
      TServerInstance simple = new TServerInstance(last.getLocation(), fakeSessionID);
      Iterator<TServerInstance> find = locations.tailMap(simple).keySet().iterator();
      if (find.hasNext()) {
        TServerInstance current = find.next();
        if (current.host().equals(last.host()))
          return current;
      }
    }

    // The strategy here is to walk through the locations and hand them back, one at a time
    // Grab an iterator off of the set of options; use a new iterator if it hands back something not in the current list.
    if (assignments == null || !assignments.hasNext())
      assignments = randomize(locations.keySet()).iterator();
    TServerInstance result = assignments.next();
    if (!locations.containsKey(result)) {
      assignments = null;
      return randomize(locations.keySet()).iterator().next();
    }
    return result;
  }

  static class ServerCounts implements Comparable<ServerCounts> {
    public final TServerInstance server;
    public int count;
    public final TabletServerStatus status;

    ServerCounts(int count, TServerInstance server, TabletServerStatus status) {
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
      return obj == this || (obj != null && obj instanceof ServerCounts && 0 == compareTo((ServerCounts) obj));
    }

    @Override
    public int compareTo(ServerCounts obj) {
      int result = count - obj.count;
      if (result == 0)
        return server.compareTo(obj.server);
      return result;
    }
  }

  public boolean getMigrations(Map<TServerInstance,TabletServerStatus> current, List<TabletMigration> result) {
    boolean moreBalancingNeeded = false;
    try {
      // no moves possible
      if (current.size() < 2) {
        return false;
      }
      final Map<Table.ID,Map<KeyExtent,TabletStats>> donerTabletStats = new HashMap<>();

      // Sort by total number of online tablets, per server
      int total = 0;
      ArrayList<ServerCounts> totals = new ArrayList<>();
      for (Entry<TServerInstance,TabletServerStatus> entry : current.entrySet()) {
        int serverTotal = 0;
        if (entry.getValue() != null && entry.getValue().tableMap != null) {
          for (Entry<String,TableInfo> e : entry.getValue().tableMap.entrySet()) {
            /**
             * The check below was on entry.getKey(), but that resolves to a tabletserver not a tablename. Believe it should be e.getKey() which is a tablename
             */
            if (tableToBalance == null || tableToBalance.canonicalID().equals(e.getKey()))
              serverTotal += e.getValue().onlineTablets;
          }
        }
        totals.add(new ServerCounts(serverTotal, entry.getKey(), entry.getValue()));
        total += serverTotal;
      }

      // order from low to high
      Collections.sort(totals);
      Collections.reverse(totals);
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
   * Select a tablet based on differences between table loads; if the loads are even, use the busiest table
   */
  List<TabletMigration> move(ServerCounts tooMuch, ServerCounts tooLittle, int count, Map<Table.ID,Map<KeyExtent,TabletStats>> donerTabletStats) {

    List<TabletMigration> result = new ArrayList<>();
    if (count == 0)
      return result;

    // Copy counts so we can update them as we propose migrations
    Map<Table.ID,Integer> tooMuchMap = tabletCountsPerTable(tooMuch.status);
    Map<Table.ID,Integer> tooLittleMap = tabletCountsPerTable(tooLittle.status);

    for (int i = 0; i < count; i++) {
      Table.ID table;
      Integer tooLittleCount;
      if (tableToBalance == null) {
        // find a table to migrate
        // look for an uneven table count
        int biggestDifference = 0;
        Table.ID biggestDifferenceTable = null;
        for (Entry<Table.ID,Integer> tableEntry : tooMuchMap.entrySet()) {
          Table.ID tableID = tableEntry.getKey();
          if (tooLittleMap.get(tableID) == null)
            tooLittleMap.put(tableID, 0);
          int diff = tableEntry.getValue() - tooLittleMap.get(tableID);
          if (diff > biggestDifference) {
            biggestDifference = diff;
            biggestDifferenceTable = tableID;
          }
        }
        if (biggestDifference < 2) {
          table = busiest(tooMuch.status.tableMap);
        } else {
          table = biggestDifferenceTable;
        }
      } else {
        // just balance the given table
        table = tableToBalance;
      }
      Map<KeyExtent,TabletStats> onlineTabletsForTable = donerTabletStats.get(table);
      try {
        if (onlineTabletsForTable == null) {
          onlineTabletsForTable = new HashMap<>();
          List<TabletStats> stats = getOnlineTabletsForTable(tooMuch.server, table);
          if (null == stats) {
            log.warn("Unable to find tablets to move");
            return result;
          }
          for (TabletStats stat : stats)
            onlineTabletsForTable.put(new KeyExtent(stat.extent), stat);
          donerTabletStats.put(table, onlineTabletsForTable);
        }
      } catch (Exception ex) {
        log.error("Unable to select a tablet to move", ex);
        return result;
      }
      KeyExtent extent = selectTablet(tooMuch.server, onlineTabletsForTable);
      onlineTabletsForTable.remove(extent);
      if (extent == null)
        return result;
      tooMuchMap.put(table, tooMuchMap.get(table) - 1);
      /**
       * If a table grows from 1 tablet then tooLittleMap.get(table) can return a null, since there is only one tabletserver that holds all of the tablets. Here
       * we check to see if in fact that is the case and if so set the value to 0.
       */
      tooLittleCount = tooLittleMap.get(table);
      if (tooLittleCount == null) {
        tooLittleCount = 0;
      }
      tooLittleMap.put(table, tooLittleCount + 1);
      tooMuch.count--;
      tooLittle.count++;
      result.add(new TabletMigration(extent, tooMuch.server, tooLittle.server));
    }
    return result;
  }

  static Map<Table.ID,Integer> tabletCountsPerTable(TabletServerStatus status) {
    Map<Table.ID,Integer> result = new HashMap<>();
    if (status != null && status.tableMap != null) {
      Map<String,TableInfo> tableMap = status.tableMap;
      for (Entry<String,TableInfo> entry : tableMap.entrySet()) {
        result.put(Table.ID.of(entry.getKey()), entry.getValue().onlineTablets);
      }
    }
    return result;
  }

  static KeyExtent selectTablet(TServerInstance tserver, Map<KeyExtent,TabletStats> extents) {
    if (extents.size() == 0)
      return null;
    KeyExtent mostRecentlySplit = null;
    long splitTime = 0;
    for (Entry<KeyExtent,TabletStats> entry : extents.entrySet())
      if (entry.getValue().splitCreationTime >= splitTime) {
        splitTime = entry.getValue().splitCreationTime;
        mostRecentlySplit = entry.getKey();
      }
    return mostRecentlySplit;
  }

  // define what it means for a tablet to be busy
  private static Table.ID busiest(Map<String,TableInfo> tables) {
    Table.ID result = null;
    double busiest = Double.NEGATIVE_INFINITY;
    for (Entry<String,TableInfo> entry : tables.entrySet()) {
      TableInfo info = entry.getValue();
      double busy = info.ingestRate + info.queryRate;
      if (busy > busiest) {
        busiest = busy;
        result = Table.ID.of(entry.getKey());
      }
    }
    return result;
  }

  @Override
  public void getAssignments(SortedMap<TServerInstance,TabletServerStatus> current, Map<KeyExtent,TServerInstance> unassigned,
      Map<KeyExtent,TServerInstance> assignments) {
    for (Entry<KeyExtent,TServerInstance> entry : unassigned.entrySet()) {
      assignments.put(entry.getKey(), getAssignment(current, entry.getKey(), entry.getValue()));
    }
  }

  private static final NoTservers NO_SERVERS = new NoTservers(log);

  protected final OutstandingMigrations outstandingMigrations = new OutstandingMigrations(log);

  @Override
  public long balance(SortedMap<TServerInstance,TabletServerStatus> current, Set<KeyExtent> migrations, List<TabletMigration> migrationsOut) {
    // do we have any servers?
    if (current.size() > 0) {
      // Don't migrate if we have migrations in progress
      if (migrations.size() == 0) {
        resetBalancerErrors();
        if (getMigrations(current, migrationsOut))
          return 1 * 1000;
      } else {
        outstandingMigrations.migrations = migrations;
        constraintNotMet(outstandingMigrations);
      }
    } else {
      constraintNotMet(NO_SERVERS);
    }
    return 5 * 1000;
  }

}

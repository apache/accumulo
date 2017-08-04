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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.SortedMap;
import java.util.function.Function;

import org.apache.accumulo.core.client.IsolatedScanner;
import org.apache.accumulo.core.client.RowIterator;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.data.impl.KeyExtent;
import org.apache.accumulo.core.master.thrift.TabletServerStatus;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.metadata.schema.MetadataSchema;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.util.ComparablePair;
import org.apache.accumulo.core.util.MapCounter;
import org.apache.accumulo.core.util.Pair;
import org.apache.accumulo.server.master.state.TServerInstance;
import org.apache.accumulo.server.master.state.TabletMigration;
import org.apache.commons.lang.mutable.MutableInt;

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Iterators;
import com.google.common.collect.Multimap;
import com.google.common.collect.Table;

/**
 * A balancer that evenly spreads groups of tablets across all tablet server. This balancer accomplishes the following two goals :
 *
 * <ul>
 * <li>Evenly spreads each group across all tservers.
 * <li>Minimizes the total number of groups on each tserver.
 * </ul>
 *
 * <p>
 * To use this balancer you must extend it and implement {@link #getPartitioner()}. See {@link RegexGroupBalancer} as an example.
 */

public abstract class GroupBalancer extends TabletBalancer {

  private final String tableId;
  private long lastRun = 0;

  /**
   * @return A function that groups tablets into named groups.
   */
  protected abstract Function<KeyExtent,String> getPartitioner();

  public GroupBalancer(String tableId) {
    this.tableId = tableId;
  }

  protected Iterable<Pair<KeyExtent,Location>> getLocationProvider() {
    return new MetadataLocationProvider();
  }

  /**
   * The amount of time to wait between balancing.
   */
  protected long getWaitTime() {
    return 60000;
  }

  /**
   * The maximum number of migrations to perform in a single pass.
   */
  protected int getMaxMigrations() {
    return 1000;
  }

  /**
   * @return Examine current tserver and migrations and return true if balancing should occur.
   */
  protected boolean shouldBalance(SortedMap<TServerInstance,TabletServerStatus> current, Set<KeyExtent> migrations) {

    if (current.size() < 2) {
      return false;
    }

    for (KeyExtent keyExtent : migrations) {
      if (keyExtent.getTableId().canonicalID().equals(tableId)) {
        return false;
      }
    }

    return true;
  }

  @Override
  public void getAssignments(SortedMap<TServerInstance,TabletServerStatus> current, Map<KeyExtent,TServerInstance> unassigned,
      Map<KeyExtent,TServerInstance> assignments) {

    if (current.size() == 0) {
      return;
    }

    Function<KeyExtent,String> partitioner = getPartitioner();

    List<ComparablePair<String,KeyExtent>> tabletsByGroup = new ArrayList<>();
    for (Entry<KeyExtent,TServerInstance> entry : unassigned.entrySet()) {
      TServerInstance last = entry.getValue();
      if (last != null) {
        // Maintain locality
        String fakeSessionID = " ";
        TServerInstance simple = new TServerInstance(last.getLocation(), fakeSessionID);
        Iterator<TServerInstance> find = current.tailMap(simple).keySet().iterator();
        if (find.hasNext()) {
          TServerInstance tserver = find.next();
          if (tserver.host().equals(last.host())) {
            assignments.put(entry.getKey(), tserver);
            continue;
          }
        }
      }

      tabletsByGroup.add(new ComparablePair<>(partitioner.apply(entry.getKey()), entry.getKey()));
    }

    Collections.sort(tabletsByGroup);

    Iterator<TServerInstance> tserverIter = Iterators.cycle(current.keySet());
    for (ComparablePair<String,KeyExtent> pair : tabletsByGroup) {
      KeyExtent ke = pair.getSecond();
      assignments.put(ke, tserverIter.next());
    }

  }

  @Override
  public long balance(SortedMap<TServerInstance,TabletServerStatus> current, Set<KeyExtent> migrations, List<TabletMigration> migrationsOut) {

    // The terminology extra and expected are used in this code. Expected tablets is the number of tablets a tserver must have for a given group and is
    // numInGroup/numTservers. Extra tablets are any tablets more than the number expected for a given group. If numInGroup % numTservers > 0, then a tserver
    // may have one extra tablet for a group.
    //
    // Assume we have 4 tservers and group A has 11 tablets.
    // * expected tablets : group A is expected to have 2 tablets on each tservers
    // * extra tablets : group A may have an additional tablet on each tserver. Group A has a total of 3 extra tablets.
    //
    // This balancer also evens out the extra tablets across all groups. The terminology extraExpected and extraExtra is used to describe these tablets.
    // ExtraExpected is totalExtra/numTservers. ExtraExtra is totalExtra%numTservers. Each tserver should have at least expectedExtra extra tablets and at most
    // one extraExtra tablets. All extra tablets on a tserver must be from different groups.
    //
    // Assume we have 6 tservers and three groups (G1, G2, G3) with 9 tablets each. Each tserver is expected to have one tablet from each group and could
    // possibly have 2 tablets from a group. Below is an illustration of an ideal balancing of extra tablets. To understand the illustration, the first column
    // shows tserver T1 with 2 tablets from G1, 1 tablet from G2, and two tablets from G3. EE means empty, put it there so eclipse formating would not mess up
    // table.
    //
    // T1 | T2 | T3 | T4 | T5 | T6
    // ---+----+----+----+----+-----
    // G3 | G2 | G3 | EE | EE | EE <-- extra extra tablets
    // G1 | G1 | G1 | G2 | G3 | G2 <-- extra expected tablets.
    // G1 | G1 | G1 | G1 | G1 | G1 <-- expected tablets for group 1
    // G2 | G2 | G2 | G2 | G2 | G2 <-- expected tablets for group 2
    // G3 | G3 | G3 | G3 | G3 | G3 <-- expected tablets for group 3
    //
    // Do not want to balance the extra tablets like the following. There are two problem with this. First extra tablets are not evenly spread. Since there are
    // a total of 9 extra tablets, every tserver is expected to have at least one extra tablet. Second tserver T1 has two extra tablet for group G1. This
    // violates the principal that a tserver can only have one extra tablet for a given group.
    //
    // T1 | T2 | T3 | T4 | T5 | T6
    // ---+----+----+----+----+-----
    // G1 | EE | EE | EE | EE | EE <--- one extra tablets from group 1
    // G3 | G3 | G3 | EE | EE | EE <--- three extra tablets from group 3
    // G2 | G2 | G2 | EE | EE | EE <--- three extra tablets from group 2
    // G1 | G1 | EE | EE | EE | EE <--- two extra tablets from group 1
    // G1 | G1 | G1 | G1 | G1 | G1 <-- expected tablets for group 1
    // G2 | G2 | G2 | G2 | G2 | G2 <-- expected tablets for group 2
    // G3 | G3 | G3 | G3 | G3 | G3 <-- expected tablets for group 3

    if (!shouldBalance(current, migrations)) {
      return 5000;
    }

    if (System.currentTimeMillis() - lastRun < getWaitTime()) {
      return 5000;
    }

    MapCounter<String> groupCounts = new MapCounter<>();
    Map<TServerInstance,TserverGroupInfo> tservers = new HashMap<>();

    for (TServerInstance tsi : current.keySet()) {
      tservers.put(tsi, new TserverGroupInfo(tsi));
    }

    Function<KeyExtent,String> partitioner = getPartitioner();

    // collect stats about current state
    for (Pair<KeyExtent,Location> entry : getLocationProvider()) {
      String group = partitioner.apply(entry.getFirst());
      Location loc = entry.getSecond();

      if (loc.equals(Location.NONE) || !tservers.containsKey(loc.getTserverInstance())) {
        return 5000;
      }

      groupCounts.increment(group, 1);
      TserverGroupInfo tgi = tservers.get(loc.getTserverInstance());
      tgi.addGroup(group);
    }

    Map<String,Integer> expectedCounts = new HashMap<>();

    int totalExtra = 0;
    for (String group : groupCounts.keySet()) {
      long groupCount = groupCounts.get(group);
      totalExtra += groupCount % current.size();
      expectedCounts.put(group, (int) (groupCount / current.size()));
    }

    // The number of extra tablets from all groups that each tserver must have.
    int expectedExtra = totalExtra / current.size();
    int maxExtraGroups = expectedExtra + 1;

    expectedCounts = Collections.unmodifiableMap(expectedCounts);
    tservers = Collections.unmodifiableMap(tservers);

    for (TserverGroupInfo tgi : tservers.values()) {
      tgi.finishedAdding(expectedCounts);
    }

    Moves moves = new Moves();

    // The order of the following steps is important, because as ordered each step should not move any tablets moved by a previous step.
    balanceExpected(tservers, moves);
    if (moves.size() < getMaxMigrations()) {
      balanceExtraExpected(tservers, expectedExtra, moves);
      if (moves.size() < getMaxMigrations()) {
        boolean cont = balanceExtraMultiple(tservers, maxExtraGroups, moves);
        if (cont && moves.size() < getMaxMigrations()) {
          balanceExtraExtra(tservers, maxExtraGroups, moves);
        }
      }
    }

    populateMigrations(tservers.keySet(), migrationsOut, moves);

    lastRun = System.currentTimeMillis();

    return 5000;
  }

  public static class Location {
    public static final Location NONE = new Location();
    private final TServerInstance tserverInstance;

    public Location() {
      this(null);
    }

    public Location(TServerInstance tsi) {
      tserverInstance = tsi;
    }

    public TServerInstance getTserverInstance() {
      return tserverInstance;
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(tserverInstance);
    }

    @Override
    public boolean equals(Object o) {
      if (o instanceof Location) {
        Location ol = ((Location) o);
        if (tserverInstance == ol.tserverInstance) {
          return true;
        }
        return tserverInstance.equals(ol.tserverInstance);
      }
      return false;
    }
  }

  static class TserverGroupInfo {

    private Map<String,Integer> expectedCounts;
    private final Map<String,MutableInt> initialCounts = new HashMap<>();
    private final Map<String,Integer> extraCounts = new HashMap<>();
    private final Map<String,Integer> expectedDeficits = new HashMap<>();

    private final TServerInstance tsi;
    private boolean finishedAdding = false;

    TserverGroupInfo(TServerInstance tsi) {
      this.tsi = tsi;
    }

    public void addGroup(String group) {
      checkState(!finishedAdding);

      MutableInt mi = initialCounts.get(group);
      if (mi == null) {
        mi = new MutableInt();
        initialCounts.put(group, mi);
      }

      mi.increment();
    }

    public void finishedAdding(Map<String,Integer> expectedCounts) {
      checkState(!finishedAdding);
      finishedAdding = true;
      this.expectedCounts = expectedCounts;

      for (Entry<String,Integer> entry : expectedCounts.entrySet()) {
        String group = entry.getKey();
        int expected = entry.getValue();

        MutableInt count = initialCounts.get(group);
        int num = count == null ? 0 : count.intValue();

        if (num < expected) {
          expectedDeficits.put(group, expected - num);
        } else if (num > expected) {
          extraCounts.put(group, num - expected);
        }
      }

    }

    public void moveOff(String group, int num) {
      checkArgument(num > 0);
      checkState(finishedAdding);

      Integer extraCount = extraCounts.get(group);

      checkArgument(extraCount != null && extraCount >= num, "group=%s num=%s extraCount=%s", group, num, extraCount);

      MutableInt initialCount = initialCounts.get(group);

      checkArgument(initialCount.intValue() >= num);

      initialCount.subtract(num);

      if (extraCount - num == 0) {
        extraCounts.remove(group);
      } else {
        extraCounts.put(group, extraCount - num);
      }
    }

    public void moveTo(String group, int num) {
      checkArgument(num > 0);
      checkArgument(expectedCounts.containsKey(group));
      checkState(finishedAdding);

      Integer deficit = expectedDeficits.get(group);
      if (deficit != null) {
        if (num >= deficit) {
          expectedDeficits.remove(group);
          num -= deficit;
        } else {
          expectedDeficits.put(group, deficit - num);
          num = 0;
        }
      }

      if (num > 0) {
        Integer extra = extraCounts.get(group);
        if (extra == null) {
          extra = 0;
        }

        extraCounts.put(group, extra + num);
      }

      // TODO could check extra constraints
    }

    public Map<String,Integer> getExpectedDeficits() {
      checkState(finishedAdding);
      return Collections.unmodifiableMap(expectedDeficits);
    }

    public Map<String,Integer> getExtras() {
      checkState(finishedAdding);
      return Collections.unmodifiableMap(extraCounts);
    }

    public TServerInstance getTserverInstance() {
      return tsi;
    }

    @Override
    public int hashCode() {
      return tsi.hashCode();
    }

    @Override
    public boolean equals(Object o) {
      if (o instanceof TserverGroupInfo) {
        TserverGroupInfo otgi = (TserverGroupInfo) o;
        return tsi.equals(otgi.tsi);
      }

      return false;
    }

    @Override
    public String toString() {
      return tsi.toString();
    }

  }

  private static class Move {
    TserverGroupInfo dest;
    int count;

    public Move(TserverGroupInfo dest, int num) {
      this.dest = dest;
      this.count = num;
    }
  }

  private static class Moves {

    private final Table<TServerInstance,String,List<Move>> moves = HashBasedTable.create();
    private int totalMoves = 0;

    public void move(String group, int num, TserverGroupInfo src, TserverGroupInfo dest) {
      checkArgument(num > 0);
      checkArgument(!src.equals(dest));

      src.moveOff(group, num);
      dest.moveTo(group, num);

      List<Move> srcMoves = moves.get(src.getTserverInstance(), group);
      if (srcMoves == null) {
        srcMoves = new ArrayList<>();
        moves.put(src.getTserverInstance(), group, srcMoves);
      }

      srcMoves.add(new Move(dest, num));
      totalMoves += num;
    }

    public TServerInstance removeMove(TServerInstance src, String group) {
      List<Move> srcMoves = moves.get(src, group);
      if (srcMoves == null) {
        return null;
      }

      Move move = srcMoves.get(srcMoves.size() - 1);
      TServerInstance ret = move.dest.getTserverInstance();
      totalMoves--;

      move.count--;
      if (move.count == 0) {
        srcMoves.remove(srcMoves.size() - 1);
        if (srcMoves.size() == 0) {
          moves.remove(src, group);
        }
      }

      return ret;
    }

    public int size() {
      return totalMoves;
    }
  }

  private void balanceExtraExtra(Map<TServerInstance,TserverGroupInfo> tservers, int maxExtraGroups, Moves moves) {
    Table<String,TServerInstance,TserverGroupInfo> surplusExtra = HashBasedTable.create();
    for (TserverGroupInfo tgi : tservers.values()) {
      Map<String,Integer> extras = tgi.getExtras();
      if (extras.size() > maxExtraGroups) {
        for (String group : extras.keySet()) {
          surplusExtra.put(group, tgi.getTserverInstance(), tgi);
        }
      }
    }

    ArrayList<Pair<String,TServerInstance>> serversGroupsToRemove = new ArrayList<>();
    ArrayList<TServerInstance> serversToRemove = new ArrayList<>();

    for (TserverGroupInfo destTgi : tservers.values()) {
      if (surplusExtra.size() == 0) {
        break;
      }

      Map<String,Integer> extras = destTgi.getExtras();
      if (extras.size() < maxExtraGroups) {
        serversToRemove.clear();
        serversGroupsToRemove.clear();
        for (String group : surplusExtra.rowKeySet()) {
          if (!extras.containsKey(group)) {
            TserverGroupInfo srcTgi = surplusExtra.row(group).values().iterator().next();

            moves.move(group, 1, srcTgi, destTgi);

            if (srcTgi.getExtras().size() <= maxExtraGroups) {
              serversToRemove.add(srcTgi.getTserverInstance());
            } else {
              serversGroupsToRemove.add(new Pair<>(group, srcTgi.getTserverInstance()));
            }

            if (destTgi.getExtras().size() >= maxExtraGroups || moves.size() >= getMaxMigrations()) {
              break;
            }
          }
        }

        if (serversToRemove.size() > 0) {
          surplusExtra.columnKeySet().removeAll(serversToRemove);
        }

        for (Pair<String,TServerInstance> pair : serversGroupsToRemove) {
          surplusExtra.remove(pair.getFirst(), pair.getSecond());
        }

        if (moves.size() >= getMaxMigrations()) {
          break;
        }
      }
    }
  }

  private boolean balanceExtraMultiple(Map<TServerInstance,TserverGroupInfo> tservers, int maxExtraGroups, Moves moves) {
    Multimap<String,TserverGroupInfo> extraMultiple = HashMultimap.create();

    for (TserverGroupInfo tgi : tservers.values()) {
      Map<String,Integer> extras = tgi.getExtras();
      for (Entry<String,Integer> entry : extras.entrySet()) {
        if (entry.getValue() > 1) {
          extraMultiple.put(entry.getKey(), tgi);
        }
      }
    }

    balanceExtraMultiple(tservers, maxExtraGroups, moves, extraMultiple, false);
    if (moves.size() < getMaxMigrations() && extraMultiple.size() > 0) {
      // no place to move so must exceed maxExtra temporarily... subsequent balancer calls will smooth things out
      balanceExtraMultiple(tservers, maxExtraGroups, moves, extraMultiple, true);
      return false;
    } else {
      return true;
    }
  }

  private void balanceExtraMultiple(Map<TServerInstance,TserverGroupInfo> tservers, int maxExtraGroups, Moves moves,
      Multimap<String,TserverGroupInfo> extraMultiple, boolean alwaysAdd) {

    ArrayList<Pair<String,TserverGroupInfo>> serversToRemove = new ArrayList<>();
    for (TserverGroupInfo destTgi : tservers.values()) {
      Map<String,Integer> extras = destTgi.getExtras();
      if (alwaysAdd || extras.size() < maxExtraGroups) {
        serversToRemove.clear();
        for (String group : extraMultiple.keySet()) {
          if (!extras.containsKey(group)) {
            Collection<TserverGroupInfo> sources = extraMultiple.get(group);
            Iterator<TserverGroupInfo> iter = sources.iterator();
            TserverGroupInfo srcTgi = iter.next();

            int num = srcTgi.getExtras().get(group);

            moves.move(group, 1, srcTgi, destTgi);

            if (num == 2) {
              serversToRemove.add(new Pair<>(group, srcTgi));
            }

            if (destTgi.getExtras().size() >= maxExtraGroups || moves.size() >= getMaxMigrations()) {
              break;
            }
          }
        }

        for (Pair<String,TserverGroupInfo> pair : serversToRemove) {
          extraMultiple.remove(pair.getFirst(), pair.getSecond());
        }

        if (extraMultiple.size() == 0 || moves.size() >= getMaxMigrations()) {
          break;
        }
      }
    }
  }

  private void balanceExtraExpected(Map<TServerInstance,TserverGroupInfo> tservers, int expectedExtra, Moves moves) {

    Table<String,TServerInstance,TserverGroupInfo> extraSurplus = HashBasedTable.create();

    for (TserverGroupInfo tgi : tservers.values()) {
      Map<String,Integer> extras = tgi.getExtras();
      if (extras.size() > expectedExtra) {
        for (String group : extras.keySet()) {
          extraSurplus.put(group, tgi.getTserverInstance(), tgi);
        }
      }
    }

    ArrayList<TServerInstance> emptyServers = new ArrayList<>();
    ArrayList<Pair<String,TServerInstance>> emptyServerGroups = new ArrayList<>();
    for (TserverGroupInfo destTgi : tservers.values()) {
      if (extraSurplus.size() == 0) {
        break;
      }

      Map<String,Integer> extras = destTgi.getExtras();
      if (extras.size() < expectedExtra) {
        emptyServers.clear();
        emptyServerGroups.clear();
        nextGroup: for (String group : extraSurplus.rowKeySet()) {
          if (!extras.containsKey(group)) {
            Iterator<TserverGroupInfo> iter = extraSurplus.row(group).values().iterator();
            TserverGroupInfo srcTgi = iter.next();

            while (srcTgi.getExtras().size() <= expectedExtra) {
              if (iter.hasNext()) {
                srcTgi = iter.next();
              } else {
                continue nextGroup;
              }
            }

            moves.move(group, 1, srcTgi, destTgi);

            if (srcTgi.getExtras().size() <= expectedExtra) {
              emptyServers.add(srcTgi.getTserverInstance());
            } else if (srcTgi.getExtras().get(group) == null) {
              emptyServerGroups.add(new Pair<>(group, srcTgi.getTserverInstance()));
            }

            if (destTgi.getExtras().size() >= expectedExtra || moves.size() >= getMaxMigrations()) {
              break;
            }
          }
        }

        if (emptyServers.size() > 0) {
          extraSurplus.columnKeySet().removeAll(emptyServers);
        }

        for (Pair<String,TServerInstance> pair : emptyServerGroups) {
          extraSurplus.remove(pair.getFirst(), pair.getSecond());
        }

        if (moves.size() >= getMaxMigrations()) {
          break;
        }
      }
    }
  }

  private void balanceExpected(Map<TServerInstance,TserverGroupInfo> tservers, Moves moves) {
    Multimap<String,TserverGroupInfo> groupDefecits = HashMultimap.create();
    Multimap<String,TserverGroupInfo> groupSurplus = HashMultimap.create();

    for (TserverGroupInfo tgi : tservers.values()) {
      for (String group : tgi.getExpectedDeficits().keySet()) {
        groupDefecits.put(group, tgi);
      }

      for (String group : tgi.getExtras().keySet()) {
        groupSurplus.put(group, tgi);
      }
    }

    for (String group : groupDefecits.keySet()) {
      Collection<TserverGroupInfo> defecitServers = groupDefecits.get(group);
      for (TserverGroupInfo defecitTsi : defecitServers) {
        int numToMove = defecitTsi.getExpectedDeficits().get(group);

        Iterator<TserverGroupInfo> surplusIter = groupSurplus.get(group).iterator();
        while (numToMove > 0) {
          TserverGroupInfo surplusTsi = surplusIter.next();

          int available = surplusTsi.getExtras().get(group);

          if (numToMove >= available) {
            surplusIter.remove();
          }

          int transfer = Math.min(numToMove, available);

          numToMove -= transfer;

          moves.move(group, transfer, surplusTsi, defecitTsi);
          if (moves.size() >= getMaxMigrations()) {
            return;
          }
        }
      }
    }
  }

  private void populateMigrations(Set<TServerInstance> current, List<TabletMigration> migrationsOut, Moves moves) {
    if (moves.size() == 0) {
      return;
    }

    Function<KeyExtent,String> partitioner = getPartitioner();

    for (Pair<KeyExtent,Location> entry : getLocationProvider()) {
      String group = partitioner.apply(entry.getFirst());
      Location loc = entry.getSecond();

      if (loc.equals(Location.NONE) || !current.contains(loc.getTserverInstance())) {
        migrationsOut.clear();
        return;
      }

      TServerInstance dest = moves.removeMove(loc.getTserverInstance(), group);
      if (dest != null) {
        migrationsOut.add(new TabletMigration(entry.getFirst(), loc.getTserverInstance(), dest));
        if (moves.size() == 0) {
          break;
        }
      }
    }
  }

  static class LocationFunction implements Function<Iterator<Entry<Key,Value>>,Pair<KeyExtent,Location>> {
    @Override
    public Pair<KeyExtent,Location> apply(Iterator<Entry<Key,Value>> input) {
      Location loc = Location.NONE;
      KeyExtent extent = null;
      while (input.hasNext()) {
        Entry<Key,Value> entry = input.next();
        if (entry.getKey().getColumnFamily().equals(MetadataSchema.TabletsSection.CurrentLocationColumnFamily.NAME)) {
          loc = new Location(new TServerInstance(entry.getValue(), entry.getKey().getColumnQualifier()));
        } else if (MetadataSchema.TabletsSection.TabletColumnFamily.PREV_ROW_COLUMN.hasColumns(entry.getKey())) {
          extent = new KeyExtent(entry.getKey().getRow(), entry.getValue());
        }
      }

      return new Pair<>(extent, loc);
    }

  }

  class MetadataLocationProvider implements Iterable<Pair<KeyExtent,Location>> {

    @Override
    public Iterator<Pair<KeyExtent,Location>> iterator() {
      try {
        Scanner scanner = new IsolatedScanner(context.getConnector().createScanner(MetadataTable.NAME, Authorizations.EMPTY));
        scanner.fetchColumnFamily(MetadataSchema.TabletsSection.CurrentLocationColumnFamily.NAME);
        MetadataSchema.TabletsSection.TabletColumnFamily.PREV_ROW_COLUMN.fetch(scanner);
        scanner.setRange(MetadataSchema.TabletsSection.getRange(org.apache.accumulo.core.client.impl.Table.ID.of(tableId)));

        RowIterator rowIter = new RowIterator(scanner);

        Function<Iterator<Entry<Key,Value>>,Pair<KeyExtent,Location>> f = new LocationFunction();
        return Iterators.transform(rowIter, x -> f.apply(x));
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }
}

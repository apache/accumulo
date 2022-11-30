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
package org.apache.accumulo.core.clientImpl;

import static org.easymock.EasyMock.replay;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.accumulo.core.clientImpl.TabletLocator.TabletLocation;
import org.apache.accumulo.core.clientImpl.TabletLocator.TabletLocations;
import org.apache.accumulo.core.clientImpl.TabletLocator.TabletServerMutations;
import org.apache.accumulo.core.clientImpl.TabletLocatorImpl.TabletLocationObtainer;
import org.apache.accumulo.core.clientImpl.TabletLocatorImpl.TabletServerLockChecker;
import org.apache.accumulo.core.data.InstanceId;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.metadata.MetadataLocationObtainer;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.metadata.RootTable;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.CurrentLocationColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.TabletColumnFamily;
import org.apache.hadoop.io.Text;
import org.easymock.EasyMock;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TabletLocatorImplTest {

  private static final KeyExtent ROOT_TABLE_EXTENT = RootTable.EXTENT;
  private static final KeyExtent METADATA_TABLE_EXTENT =
      new KeyExtent(MetadataTable.ID, null, ROOT_TABLE_EXTENT.endRow());

  static KeyExtent createNewKeyExtent(String table, String endRow, String prevEndRow) {
    return new KeyExtent(TableId.of(table), endRow == null ? null : new Text(endRow),
        prevEndRow == null ? null : new Text(prevEndRow));
  }

  static Range createNewRange(String key1, boolean startInclusive, String key2,
      boolean endInclusive) {
    return new Range(key1 == null ? null : new Text(key1), startInclusive,
        key2 == null ? null : new Text(key2), endInclusive);
  }

  static Range createNewRange(String key1, String key2) {
    return new Range(key1 == null ? null : new Text(key1), key2 == null ? null : new Text(key2));
  }

  static List<Range> createNewRangeList(Range... ranges) {
    return Arrays.asList(ranges);
  }

  static class RangeLocation {
    String location;
    Map<KeyExtent,List<Range>> extents = new HashMap<>();

    public RangeLocation(String location, KeyExtent extent1, List<Range> range1) {
      this.location = location;
      this.extents.put(extent1, range1);
    }

    public RangeLocation(String location, KeyExtent extent1, List<Range> range1, KeyExtent extent2,
        List<Range> range2) {
      this.location = location;
      this.extents.put(extent1, range1);
      this.extents.put(extent2, range2);
    }
  }

  static RangeLocation createRangeLocation(String location, KeyExtent extent, List<Range> ranges) {
    return new RangeLocation(location, extent, ranges);
  }

  static RangeLocation createRangeLocation(String location, KeyExtent extent1, List<Range> range1,
      KeyExtent extent2, List<Range> range2) {
    return new RangeLocation(location, extent1, range1, extent2, range2);
  }

  static Map<String,Map<KeyExtent,List<Range>>>
      createExpectedBinnings(RangeLocation... rangeLocations) {

    Map<String,Map<KeyExtent,List<Range>>> expBinnedRanges = new HashMap<>();

    for (RangeLocation rl : rangeLocations) {
      HashMap<KeyExtent,List<Range>> binnedKE = new HashMap<>();
      expBinnedRanges.put(rl.location, binnedKE);
      binnedKE.putAll(rl.extents);
    }
    return expBinnedRanges;
  }

  static TreeMap<KeyExtent,TabletLocation> createMetaCacheKE(Object... data) {
    TreeMap<KeyExtent,TabletLocation> mcke = new TreeMap<>();

    for (int i = 0; i < data.length; i += 2) {
      KeyExtent ke = (KeyExtent) data[i];
      String loc = (String) data[i + 1];
      mcke.put(ke, new TabletLocation(ke, loc, "1"));
    }

    return mcke;
  }

  static TreeMap<Text,TabletLocation> createMetaCache(Object... data) {
    TreeMap<KeyExtent,TabletLocation> mcke = createMetaCacheKE(data);

    TreeMap<Text,TabletLocation> mc = new TreeMap<>(TabletLocatorImpl.END_ROW_COMPARATOR);

    for (Entry<KeyExtent,TabletLocation> entry : mcke.entrySet()) {
      if (entry.getKey().endRow() == null) {
        mc.put(TabletLocatorImpl.MAX_TEXT, entry.getValue());
      } else {
        mc.put(entry.getKey().endRow(), entry.getValue());
      }
    }

    return mc;
  }

  static TabletLocatorImpl createLocators(TServers tservers, String rootTabLoc, String metaTabLoc,
      String table, TabletServerLockChecker tslc, Object... data) {

    TreeMap<KeyExtent,TabletLocation> mcke = createMetaCacheKE(data);

    TestTabletLocationObtainer ttlo = new TestTabletLocationObtainer(tservers);

    RootTabletLocator rtl = new TestRootTabletLocator();
    TabletLocatorImpl rootTabletCache =
        new TabletLocatorImpl(MetadataTable.ID, rtl, ttlo, new YesLockChecker());
    TabletLocatorImpl tab1TabletCache =
        new TabletLocatorImpl(TableId.of(table), rootTabletCache, ttlo, tslc);

    setLocation(tservers, rootTabLoc, ROOT_TABLE_EXTENT, METADATA_TABLE_EXTENT, metaTabLoc);

    for (Entry<KeyExtent,TabletLocation> entry : mcke.entrySet()) {
      setLocation(tservers, metaTabLoc, METADATA_TABLE_EXTENT, entry.getKey(),
          entry.getValue().tablet_location);
    }

    return tab1TabletCache;

  }

  static TabletLocatorImpl createLocators(TServers tservers, String rootTabLoc, String metaTabLoc,
      String table, Object... data) {
    return createLocators(tservers, rootTabLoc, metaTabLoc, table, new YesLockChecker(), data);
  }

  static TabletLocatorImpl createLocators(String table, Object... data) {
    TServers tservers = new TServers();
    return createLocators(tservers, "tserver1", "tserver2", table, data);
  }

  private ClientContext context;
  private InstanceId iid;

  @BeforeEach
  public void setUp() {
    context = EasyMock.createMock(ClientContext.class);
    iid = InstanceId.of("instance1");
    EasyMock.expect(context.getRootTabletLocation()).andReturn("tserver1").anyTimes();
    EasyMock.expect(context.getInstanceID()).andReturn(iid).anyTimes();
    replay(context);
  }

  private void runTest(List<Range> ranges, TabletLocatorImpl tab1TabletCache,
      Map<String,Map<KeyExtent,List<Range>>> expected) throws Exception {
    List<Range> failures = Collections.emptyList();
    runTest(ranges, tab1TabletCache, expected, failures);
  }

  private void runTest(List<Range> ranges, TabletLocatorImpl tab1TabletCache,
      Map<String,Map<KeyExtent,List<Range>>> expected, List<Range> efailures) throws Exception {

    Map<String,Map<KeyExtent,List<Range>>> binnedRanges = new HashMap<>();
    List<Range> f = tab1TabletCache.binRanges(context, ranges, binnedRanges);
    assertEquals(expected, binnedRanges);

    HashSet<Range> f1 = new HashSet<>(f);
    HashSet<Range> f2 = new HashSet<>(efailures);

    assertEquals(f2, f1);
  }

  static Set<KeyExtent> createNewKeyExtentSet(KeyExtent... extents) {
    HashSet<KeyExtent> keyExtentSet = new HashSet<>();

    Collections.addAll(keyExtentSet, extents);

    return keyExtentSet;
  }

  static void runTest(TreeMap<Text,TabletLocation> metaCache, KeyExtent remove,
      Set<KeyExtent> expected) {
    // copy so same metaCache can be used for multiple test

    metaCache = new TreeMap<>(metaCache);

    TabletLocatorImpl.removeOverlapping(metaCache, remove);

    HashSet<KeyExtent> eic = new HashSet<>();
    for (TabletLocation tl : metaCache.values()) {
      eic.add(tl.tablet_extent);
    }

    assertEquals(expected, eic);
  }

  static Mutation createNewMutation(String row, String... data) {
    Mutation mut = new Mutation(new Text(row));

    for (String element : data) {
      String[] cvp = element.split("=");
      String[] cols = cvp[0].split(":");

      mut.put(cols[0], cols[1], cvp[1]);
    }

    return mut;
  }

  static List<Mutation> createNewMutationList(Mutation... ma) {
    return Arrays.asList(ma);
  }

  private void runTest(TabletLocatorImpl metaCache, List<Mutation> ml,
      Map<String,Map<KeyExtent,List<String>>> emb, String... efailures) throws Exception {
    Map<String,TabletServerMutations<Mutation>> binnedMutations = new HashMap<>();
    List<Mutation> afailures = new ArrayList<>();
    metaCache.binMutations(context, ml, binnedMutations, afailures);

    verify(emb, binnedMutations);

    ArrayList<String> afs = new ArrayList<>();
    ArrayList<String> efs = new ArrayList<>(Arrays.asList(efailures));

    for (Mutation mutation : afailures) {
      afs.add(new String(mutation.getRow()));
    }

    Collections.sort(afs);
    Collections.sort(efs);

    assertEquals(efs, afs);

  }

  private void verify(Map<String,Map<KeyExtent,List<String>>> expected,
      Map<String,TabletServerMutations<Mutation>> actual) {
    assertEquals(expected.keySet(), actual.keySet());

    for (String server : actual.keySet()) {
      TabletServerMutations<Mutation> atb = actual.get(server);
      Map<KeyExtent,List<String>> etb = expected.get(server);

      assertEquals(etb.keySet(), atb.getMutations().keySet());

      for (KeyExtent ke : etb.keySet()) {
        ArrayList<String> eRows = new ArrayList<>(etb.get(ke));
        ArrayList<String> aRows = new ArrayList<>();

        for (Mutation m : atb.getMutations().get(ke)) {
          aRows.add(new String(m.getRow()));
        }

        Collections.sort(eRows);
        Collections.sort(aRows);

        assertEquals(eRows, aRows);
      }
    }

  }

  static class ServerExtent {
    public String location;
    public String row;
    public KeyExtent extent;

    public ServerExtent(String location, String row, KeyExtent extent) {
      this.location = location;
      this.row = row;
      this.extent = extent;
    }
  }

  static ServerExtent createServerExtent(String row, String location, KeyExtent extent) {
    return new ServerExtent(location, row, extent);
  }

  static Map<String,Map<KeyExtent,List<String>>> createServerExtentMap(ServerExtent... locations) {

    Map<String,Map<KeyExtent,List<String>>> serverExtents = new HashMap<>();

    for (ServerExtent se : locations) {
      serverExtents.computeIfAbsent(se.location, k -> new HashMap<>())
          .computeIfAbsent(se.extent, k -> new ArrayList<>()).add(se.row);
    }

    return serverExtents;
  }

  @Test
  public void testRemoveOverlapping1() {
    TreeMap<Text,TabletLocation> mc = createMetaCache(createNewKeyExtent("0", null, null), "l1");

    runTest(mc, createNewKeyExtent("0", "a", null), createNewKeyExtentSet());
    runTest(mc, createNewKeyExtent("0", null, null), createNewKeyExtentSet());
    runTest(mc, createNewKeyExtent("0", null, "a"), createNewKeyExtentSet());

    mc = createMetaCache(createNewKeyExtent("0", "g", null), "l1",
        createNewKeyExtent("0", "r", "g"), "l1", createNewKeyExtent("0", null, "r"), "l1");
    runTest(mc, createNewKeyExtent("0", null, null), createNewKeyExtentSet());

    runTest(mc, createNewKeyExtent("0", "a", null), createNewKeyExtentSet(
        createNewKeyExtent("0", "r", "g"), createNewKeyExtent("0", null, "r")));
    runTest(mc, createNewKeyExtent("0", "g", null), createNewKeyExtentSet(
        createNewKeyExtent("0", "r", "g"), createNewKeyExtent("0", null, "r")));
    runTest(mc, createNewKeyExtent("0", "h", null),
        createNewKeyExtentSet(createNewKeyExtent("0", null, "r")));
    runTest(mc, createNewKeyExtent("0", "r", null),
        createNewKeyExtentSet(createNewKeyExtent("0", null, "r")));
    runTest(mc, createNewKeyExtent("0", "s", null), createNewKeyExtentSet());

    runTest(mc, createNewKeyExtent("0", "b", "a"), createNewKeyExtentSet(
        createNewKeyExtent("0", "r", "g"), createNewKeyExtent("0", null, "r")));
    runTest(mc, createNewKeyExtent("0", "g", "a"), createNewKeyExtentSet(
        createNewKeyExtent("0", "r", "g"), createNewKeyExtent("0", null, "r")));
    runTest(mc, createNewKeyExtent("0", "h", "a"),
        createNewKeyExtentSet(createNewKeyExtent("0", null, "r")));
    runTest(mc, createNewKeyExtent("0", "r", "a"),
        createNewKeyExtentSet(createNewKeyExtent("0", null, "r")));
    runTest(mc, createNewKeyExtent("0", "s", "a"), createNewKeyExtentSet());

    runTest(mc, createNewKeyExtent("0", "h", "g"), createNewKeyExtentSet(
        createNewKeyExtent("0", "g", null), createNewKeyExtent("0", null, "r")));
    runTest(mc, createNewKeyExtent("0", "r", "g"), createNewKeyExtentSet(
        createNewKeyExtent("0", "g", null), createNewKeyExtent("0", null, "r")));
    runTest(mc, createNewKeyExtent("0", "s", "g"),
        createNewKeyExtentSet(createNewKeyExtent("0", "g", null)));

    runTest(mc, createNewKeyExtent("0", "i", "h"), createNewKeyExtentSet(
        createNewKeyExtent("0", "g", null), createNewKeyExtent("0", null, "r")));
    runTest(mc, createNewKeyExtent("0", "r", "h"), createNewKeyExtentSet(
        createNewKeyExtent("0", "g", null), createNewKeyExtent("0", null, "r")));
    runTest(mc, createNewKeyExtent("0", "s", "h"),
        createNewKeyExtentSet(createNewKeyExtent("0", "g", null)));

    runTest(mc, createNewKeyExtent("0", "z", "f"), createNewKeyExtentSet());
    runTest(mc, createNewKeyExtent("0", "z", "g"),
        createNewKeyExtentSet(createNewKeyExtent("0", "g", null)));
    runTest(mc, createNewKeyExtent("0", "z", "q"),
        createNewKeyExtentSet(createNewKeyExtent("0", "g", null)));
    runTest(mc, createNewKeyExtent("0", "z", "r"), createNewKeyExtentSet(
        createNewKeyExtent("0", "g", null), createNewKeyExtent("0", "r", "g")));
    runTest(mc, createNewKeyExtent("0", "z", "s"), createNewKeyExtentSet(
        createNewKeyExtent("0", "g", null), createNewKeyExtent("0", "r", "g")));

    runTest(mc, createNewKeyExtent("0", null, "f"), createNewKeyExtentSet());
    runTest(mc, createNewKeyExtent("0", null, "g"),
        createNewKeyExtentSet(createNewKeyExtent("0", "g", null)));
    runTest(mc, createNewKeyExtent("0", null, "q"),
        createNewKeyExtentSet(createNewKeyExtent("0", "g", null)));
    runTest(mc, createNewKeyExtent("0", null, "r"), createNewKeyExtentSet(
        createNewKeyExtent("0", "g", null), createNewKeyExtent("0", "r", "g")));
    runTest(mc, createNewKeyExtent("0", null, "s"), createNewKeyExtentSet(
        createNewKeyExtent("0", "g", null), createNewKeyExtent("0", "r", "g")));

  }

  @Test
  public void testRemoveOverlapping2() {

    // test removes when cache does not contain all tablets in a table
    TreeMap<Text,TabletLocation> mc = createMetaCache(createNewKeyExtent("0", "r", "g"), "l1",
        createNewKeyExtent("0", null, "r"), "l1");

    runTest(mc, createNewKeyExtent("0", "a", null), createNewKeyExtentSet(
        createNewKeyExtent("0", "r", "g"), createNewKeyExtent("0", null, "r")));
    runTest(mc, createNewKeyExtent("0", "g", null), createNewKeyExtentSet(
        createNewKeyExtent("0", "r", "g"), createNewKeyExtent("0", null, "r")));
    runTest(mc, createNewKeyExtent("0", "h", null),
        createNewKeyExtentSet(createNewKeyExtent("0", null, "r")));
    runTest(mc, createNewKeyExtent("0", "r", null),
        createNewKeyExtentSet(createNewKeyExtent("0", null, "r")));
    runTest(mc, createNewKeyExtent("0", "s", null), createNewKeyExtentSet());

    runTest(mc, createNewKeyExtent("0", "b", "a"), createNewKeyExtentSet(
        createNewKeyExtent("0", "r", "g"), createNewKeyExtent("0", null, "r")));
    runTest(mc, createNewKeyExtent("0", "g", "a"), createNewKeyExtentSet(
        createNewKeyExtent("0", "r", "g"), createNewKeyExtent("0", null, "r")));
    runTest(mc, createNewKeyExtent("0", "h", "a"),
        createNewKeyExtentSet(createNewKeyExtent("0", null, "r")));
    runTest(mc, createNewKeyExtent("0", "r", "a"),
        createNewKeyExtentSet(createNewKeyExtent("0", null, "r")));
    runTest(mc, createNewKeyExtent("0", "s", "a"), createNewKeyExtentSet());

    runTest(mc, createNewKeyExtent("0", "h", "g"),
        createNewKeyExtentSet(createNewKeyExtent("0", null, "r")));

    mc = createMetaCache(createNewKeyExtent("0", "g", null), "l1",
        createNewKeyExtent("0", null, "r"), "l1");

    runTest(mc, createNewKeyExtent("0", "h", "g"), createNewKeyExtentSet(
        createNewKeyExtent("0", "g", null), createNewKeyExtent("0", null, "r")));
    runTest(mc, createNewKeyExtent("0", "h", "a"),
        createNewKeyExtentSet(createNewKeyExtent("0", null, "r")));
    runTest(mc, createNewKeyExtent("0", "s", "g"),
        createNewKeyExtentSet(createNewKeyExtent("0", "g", null)));
    runTest(mc, createNewKeyExtent("0", "s", "a"), createNewKeyExtentSet());

    mc = createMetaCache(createNewKeyExtent("0", "g", null), "l1",
        createNewKeyExtent("0", "r", "g"), "l1");

    runTest(mc, createNewKeyExtent("0", "z", "f"), createNewKeyExtentSet());
    runTest(mc, createNewKeyExtent("0", "z", "g"),
        createNewKeyExtentSet(createNewKeyExtent("0", "g", null)));
    runTest(mc, createNewKeyExtent("0", "z", "q"),
        createNewKeyExtentSet(createNewKeyExtent("0", "g", null)));
    runTest(mc, createNewKeyExtent("0", "z", "r"), createNewKeyExtentSet(
        createNewKeyExtent("0", "g", null), createNewKeyExtent("0", "r", "g")));
    runTest(mc, createNewKeyExtent("0", "z", "s"), createNewKeyExtentSet(
        createNewKeyExtent("0", "g", null), createNewKeyExtent("0", "r", "g")));

    runTest(mc, createNewKeyExtent("0", null, "f"), createNewKeyExtentSet());
    runTest(mc, createNewKeyExtent("0", null, "g"),
        createNewKeyExtentSet(createNewKeyExtent("0", "g", null)));
    runTest(mc, createNewKeyExtent("0", null, "q"),
        createNewKeyExtentSet(createNewKeyExtent("0", "g", null)));
    runTest(mc, createNewKeyExtent("0", null, "r"), createNewKeyExtentSet(
        createNewKeyExtent("0", "g", null), createNewKeyExtent("0", "r", "g")));
    runTest(mc, createNewKeyExtent("0", null, "s"), createNewKeyExtentSet(
        createNewKeyExtent("0", "g", null), createNewKeyExtent("0", "r", "g")));
  }

  static class TServers {
    private final Map<String,Map<KeyExtent,SortedMap<Key,Value>>> tservers = new HashMap<>();
  }

  static class TestTabletLocationObtainer implements TabletLocationObtainer {

    private final Map<String,Map<KeyExtent,SortedMap<Key,Value>>> tservers;

    TestTabletLocationObtainer(TServers tservers) {
      this.tservers = tservers.tservers;
    }

    @Override
    public TabletLocations lookupTablet(ClientContext context, TabletLocation src, Text row,
        Text stopRow, TabletLocator parent) {

      Map<KeyExtent,SortedMap<Key,Value>> tablets = tservers.get(src.tablet_location);

      if (tablets == null) {
        parent.invalidateCache(context, src.tablet_location);
        return null;
      }

      SortedMap<Key,Value> tabletData = tablets.get(src.tablet_extent);

      if (tabletData == null) {
        parent.invalidateCache(src.tablet_extent);
        return null;
      }

      // the following clip is done on a tablet, do it here to see if it throws exceptions
      src.tablet_extent.toDataRange().clip(new Range(row, true, stopRow, true));

      Key startKey = new Key(row);
      Key stopKey = new Key(stopRow).followingKey(PartialKey.ROW);

      SortedMap<Key,Value> results = tabletData.tailMap(startKey).headMap(stopKey);

      return MetadataLocationObtainer.getMetadataLocationEntries(results);
    }

    @Override
    public List<TabletLocation> lookupTablets(ClientContext context, String tserver,
        Map<KeyExtent,List<Range>> map, TabletLocator parent) {

      ArrayList<TabletLocation> list = new ArrayList<>();

      Map<KeyExtent,SortedMap<Key,Value>> tablets = tservers.get(tserver);

      if (tablets == null) {
        parent.invalidateCache(context, tserver);
        return list;
      }

      TreeMap<Key,Value> results = new TreeMap<>();

      Set<Entry<KeyExtent,List<Range>>> es = map.entrySet();
      List<KeyExtent> failures = new ArrayList<>();
      for (Entry<KeyExtent,List<Range>> entry : es) {
        SortedMap<Key,Value> tabletData = tablets.get(entry.getKey());

        if (tabletData == null) {
          failures.add(entry.getKey());
          continue;
        }
        List<Range> ranges = entry.getValue();
        for (Range range : ranges) {
          SortedMap<Key,Value> tm;
          if (range.getStartKey() == null) {
            tm = tabletData;
          } else {
            tm = tabletData.tailMap(range.getStartKey());
          }

          for (Entry<Key,Value> de : tm.entrySet()) {
            if (range.afterEndKey(de.getKey())) {
              break;
            }

            if (range.contains(de.getKey())) {
              results.put(de.getKey(), de.getValue());
            }
          }
        }
      }

      if (!failures.isEmpty()) {
        parent.invalidateCache(failures);
      }

      return MetadataLocationObtainer.getMetadataLocationEntries(results).getLocations();

    }

  }

  static class YesLockChecker implements TabletServerLockChecker {
    @Override
    public boolean isLockHeld(String tserver, String session) {
      return true;
    }

    @Override
    public void invalidateCache(String server) {}
  }

  static class TestRootTabletLocator extends RootTabletLocator {

    TestRootTabletLocator() {
      super(new YesLockChecker());
    }

    @Override
    protected TabletLocation getRootTabletLocation(ClientContext context) {
      return new TabletLocation(RootTable.EXTENT, context.getRootTabletLocation(), "1");
    }

    @Override
    public void invalidateCache(ClientContext context, String server) {}

  }

  static void createEmptyTablet(TServers tservers, String server, KeyExtent tablet) {
    Map<KeyExtent,SortedMap<Key,Value>> tablets =
        tservers.tservers.computeIfAbsent(server, k -> new HashMap<>());
    SortedMap<Key,Value> tabletData = tablets.computeIfAbsent(tablet, k -> new TreeMap<>());
    if (!tabletData.isEmpty()) {
      throw new RuntimeException("Asked for empty tablet, but non empty tablet exists");
    }
  }

  static void clearLocation(TServers tservers, String server, KeyExtent tablet, KeyExtent ke,
      String instance) {
    Map<KeyExtent,SortedMap<Key,Value>> tablets = tservers.tservers.get(server);
    if (tablets == null) {
      return;
    }

    SortedMap<Key,Value> tabletData = tablets.get(tablet);
    if (tabletData == null) {
      return;
    }

    Text mr = ke.toMetaRow();
    Key lk = new Key(mr, CurrentLocationColumnFamily.NAME, new Text(instance));
    tabletData.remove(lk);

  }

  static void setLocation(TServers tservers, String server, KeyExtent tablet, KeyExtent ke,
      String location, String instance) {
    Map<KeyExtent,SortedMap<Key,Value>> tablets =
        tservers.tservers.computeIfAbsent(server, k -> new HashMap<>());
    SortedMap<Key,Value> tabletData = tablets.computeIfAbsent(tablet, k -> new TreeMap<>());

    Text mr = ke.toMetaRow();
    Value per = TabletColumnFamily.encodePrevEndRow(ke.prevEndRow());

    if (location != null) {
      if (instance == null) {
        instance = "";
      }
      Key lk = new Key(mr, CurrentLocationColumnFamily.NAME, new Text(instance));
      tabletData.put(lk, new Value(location));
    }

    Key pk = new Key(mr, TabletColumnFamily.PREV_ROW_COLUMN.getColumnFamily(),
        TabletColumnFamily.PREV_ROW_COLUMN.getColumnQualifier());
    tabletData.put(pk, per);
  }

  static void setLocation(TServers tservers, String server, KeyExtent tablet, KeyExtent ke,
      String location) {
    setLocation(tservers, server, tablet, ke, location, "");
  }

  static void deleteServer(TServers tservers, String server) {
    tservers.tservers.remove(server);

  }

  private void locateTabletTest(TabletLocatorImpl cache, String row, boolean skipRow,
      KeyExtent expected, String server) throws Exception {
    TabletLocation tl = cache.locateTablet(context, new Text(row), skipRow, false);

    if (expected == null) {
      if (tl != null) {
        System.out.println("tl = " + tl);
      }
      assertNull(tl);
    } else {
      assertNotNull(tl);
      assertEquals(server, tl.tablet_location);
      assertEquals(expected, tl.tablet_extent);
    }
  }

  private void locateTabletTest(TabletLocatorImpl cache, String row, KeyExtent expected,
      String server) throws Exception {
    locateTabletTest(cache, row, false, expected, server);
  }

  @Test
  public void test1() throws Exception {
    TServers tservers = new TServers();
    TestTabletLocationObtainer ttlo = new TestTabletLocationObtainer(tservers);

    RootTabletLocator rtl = new TestRootTabletLocator();
    TabletLocatorImpl rootTabletCache =
        new TabletLocatorImpl(MetadataTable.ID, rtl, ttlo, new YesLockChecker());
    TabletLocatorImpl tab1TabletCache =
        new TabletLocatorImpl(TableId.of("tab1"), rootTabletCache, ttlo, new YesLockChecker());

    locateTabletTest(tab1TabletCache, "r1", null, null);

    KeyExtent tab1e = createNewKeyExtent("tab1", null, null);

    setLocation(tservers, "tserver1", ROOT_TABLE_EXTENT, METADATA_TABLE_EXTENT, "tserver2");
    setLocation(tservers, "tserver2", METADATA_TABLE_EXTENT, tab1e, "tserver3");

    locateTabletTest(tab1TabletCache, "r1", tab1e, "tserver3");
    locateTabletTest(tab1TabletCache, "r2", tab1e, "tserver3");

    // simulate a split
    KeyExtent tab1e1 = createNewKeyExtent("tab1", "g", null);
    KeyExtent tab1e2 = createNewKeyExtent("tab1", null, "g");

    setLocation(tservers, "tserver2", METADATA_TABLE_EXTENT, tab1e1, "tserver4");
    setLocation(tservers, "tserver2", METADATA_TABLE_EXTENT, tab1e2, "tserver5");

    locateTabletTest(tab1TabletCache, "r1", tab1e, "tserver3");
    tab1TabletCache.invalidateCache(tab1e);
    locateTabletTest(tab1TabletCache, "r1", tab1e2, "tserver5");
    locateTabletTest(tab1TabletCache, "a", tab1e1, "tserver4");
    locateTabletTest(tab1TabletCache, "a", true, tab1e1, "tserver4");
    locateTabletTest(tab1TabletCache, "g", tab1e1, "tserver4");
    locateTabletTest(tab1TabletCache, "g", true, tab1e2, "tserver5");

    // simulate a partial split
    KeyExtent tab1e22 = createNewKeyExtent("tab1", null, "m");
    setLocation(tservers, "tserver2", METADATA_TABLE_EXTENT, tab1e22, "tserver6");
    locateTabletTest(tab1TabletCache, "r1", tab1e2, "tserver5");
    tab1TabletCache.invalidateCache(tab1e2);
    locateTabletTest(tab1TabletCache, "r1", tab1e22, "tserver6");
    locateTabletTest(tab1TabletCache, "h", null, null);
    locateTabletTest(tab1TabletCache, "a", tab1e1, "tserver4");
    KeyExtent tab1e21 = createNewKeyExtent("tab1", "m", "g");
    setLocation(tservers, "tserver2", METADATA_TABLE_EXTENT, tab1e21, "tserver7");
    locateTabletTest(tab1TabletCache, "r1", tab1e22, "tserver6");
    locateTabletTest(tab1TabletCache, "h", tab1e21, "tserver7");
    locateTabletTest(tab1TabletCache, "a", tab1e1, "tserver4");

    // simulate a migration
    setLocation(tservers, "tserver2", METADATA_TABLE_EXTENT, tab1e21, "tserver8");
    tab1TabletCache.invalidateCache(tab1e21);
    locateTabletTest(tab1TabletCache, "r1", tab1e22, "tserver6");
    locateTabletTest(tab1TabletCache, "h", tab1e21, "tserver8");
    locateTabletTest(tab1TabletCache, "a", tab1e1, "tserver4");

    // simulate a server failure
    setLocation(tservers, "tserver2", METADATA_TABLE_EXTENT, tab1e21, "tserver9");
    tab1TabletCache.invalidateCache(context, "tserver8");
    locateTabletTest(tab1TabletCache, "r1", tab1e22, "tserver6");
    locateTabletTest(tab1TabletCache, "h", tab1e21, "tserver9");
    locateTabletTest(tab1TabletCache, "a", tab1e1, "tserver4");

    // simulate all servers failing
    deleteServer(tservers, "tserver1");
    deleteServer(tservers, "tserver2");
    tab1TabletCache.invalidateCache(context, "tserver4");
    tab1TabletCache.invalidateCache(context, "tserver6");
    tab1TabletCache.invalidateCache(context, "tserver9");

    locateTabletTest(tab1TabletCache, "r1", null, null);
    locateTabletTest(tab1TabletCache, "h", null, null);
    locateTabletTest(tab1TabletCache, "a", null, null);

    EasyMock.verify(context);

    context = EasyMock.createMock(ClientContext.class);
    EasyMock.expect(context.getInstanceID()).andReturn(iid).anyTimes();
    EasyMock.expect(context.getRootTabletLocation()).andReturn("tserver4").anyTimes();
    replay(context);

    setLocation(tservers, "tserver4", ROOT_TABLE_EXTENT, METADATA_TABLE_EXTENT, "tserver5");
    setLocation(tservers, "tserver5", METADATA_TABLE_EXTENT, tab1e1, "tserver1");
    setLocation(tservers, "tserver5", METADATA_TABLE_EXTENT, tab1e21, "tserver2");
    setLocation(tservers, "tserver5", METADATA_TABLE_EXTENT, tab1e22, "tserver3");

    locateTabletTest(tab1TabletCache, "a", tab1e1, "tserver1");
    locateTabletTest(tab1TabletCache, "h", tab1e21, "tserver2");
    locateTabletTest(tab1TabletCache, "r", tab1e22, "tserver3");

    // simulate the metadata table splitting
    KeyExtent mte1 =
        new KeyExtent(MetadataTable.ID, tab1e21.toMetaRow(), ROOT_TABLE_EXTENT.endRow());
    KeyExtent mte2 = new KeyExtent(MetadataTable.ID, null, tab1e21.toMetaRow());

    setLocation(tservers, "tserver4", ROOT_TABLE_EXTENT, mte1, "tserver5");
    setLocation(tservers, "tserver4", ROOT_TABLE_EXTENT, mte2, "tserver6");
    deleteServer(tservers, "tserver5");
    setLocation(tservers, "tserver5", mte1, tab1e1, "tserver7");
    setLocation(tservers, "tserver5", mte1, tab1e21, "tserver8");
    setLocation(tservers, "tserver6", mte2, tab1e22, "tserver9");

    tab1TabletCache.invalidateCache(tab1e1);
    tab1TabletCache.invalidateCache(tab1e21);
    tab1TabletCache.invalidateCache(tab1e22);

    locateTabletTest(tab1TabletCache, "a", tab1e1, "tserver7");
    locateTabletTest(tab1TabletCache, "h", tab1e21, "tserver8");
    locateTabletTest(tab1TabletCache, "r", tab1e22, "tserver9");

    // simulate metadata and regular server down and the reassigned
    deleteServer(tservers, "tserver5");
    tab1TabletCache.invalidateCache(context, "tserver7");
    locateTabletTest(tab1TabletCache, "a", null, null);
    locateTabletTest(tab1TabletCache, "h", tab1e21, "tserver8");
    locateTabletTest(tab1TabletCache, "r", tab1e22, "tserver9");

    setLocation(tservers, "tserver4", ROOT_TABLE_EXTENT, mte1, "tserver10");
    setLocation(tservers, "tserver10", mte1, tab1e1, "tserver7");
    setLocation(tservers, "tserver10", mte1, tab1e21, "tserver8");

    locateTabletTest(tab1TabletCache, "a", tab1e1, "tserver7");
    locateTabletTest(tab1TabletCache, "h", tab1e21, "tserver8");
    locateTabletTest(tab1TabletCache, "r", tab1e22, "tserver9");
    tab1TabletCache.invalidateCache(context, "tserver7");
    setLocation(tservers, "tserver10", mte1, tab1e1, "tserver2");
    locateTabletTest(tab1TabletCache, "a", tab1e1, "tserver2");
    locateTabletTest(tab1TabletCache, "h", tab1e21, "tserver8");
    locateTabletTest(tab1TabletCache, "r", tab1e22, "tserver9");

    // simulate a hole in the metadata, caused by a partial split
    KeyExtent mte11 =
        new KeyExtent(MetadataTable.ID, tab1e1.toMetaRow(), ROOT_TABLE_EXTENT.endRow());
    KeyExtent mte12 = new KeyExtent(MetadataTable.ID, tab1e21.toMetaRow(), tab1e1.toMetaRow());
    deleteServer(tservers, "tserver10");
    setLocation(tservers, "tserver4", ROOT_TABLE_EXTENT, mte12, "tserver10");
    setLocation(tservers, "tserver10", mte12, tab1e21, "tserver12");

    // at this point should be no table1 metadata
    tab1TabletCache.invalidateCache(tab1e1);
    tab1TabletCache.invalidateCache(tab1e21);
    locateTabletTest(tab1TabletCache, "a", null, null);
    locateTabletTest(tab1TabletCache, "h", tab1e21, "tserver12");
    locateTabletTest(tab1TabletCache, "r", tab1e22, "tserver9");

    setLocation(tservers, "tserver4", ROOT_TABLE_EXTENT, mte11, "tserver5");
    setLocation(tservers, "tserver5", mte11, tab1e1, "tserver13");

    locateTabletTest(tab1TabletCache, "a", tab1e1, "tserver13");
    locateTabletTest(tab1TabletCache, "h", tab1e21, "tserver12");
    locateTabletTest(tab1TabletCache, "r", tab1e22, "tserver9");
  }

  @Test
  public void test2() throws Exception {
    TServers tservers = new TServers();
    TabletLocatorImpl metaCache = createLocators(tservers, "tserver1", "tserver2", "foo");

    KeyExtent ke1 = createNewKeyExtent("foo", "m", null);
    KeyExtent ke2 = createNewKeyExtent("foo", null, "m");

    setLocation(tservers, "tserver2", METADATA_TABLE_EXTENT, ke1, null);
    setLocation(tservers, "tserver2", METADATA_TABLE_EXTENT, ke2, "L1");

    locateTabletTest(metaCache, "a", null, null);
    locateTabletTest(metaCache, "r", ke2, "L1");

    setLocation(tservers, "tserver2", METADATA_TABLE_EXTENT, ke1, "L2");

    locateTabletTest(metaCache, "a", ke1, "L2");
    locateTabletTest(metaCache, "r", ke2, "L1");
  }

  @Test
  public void testBinRanges1() throws Exception {

    TabletLocatorImpl metaCache =
        createLocators("foo", createNewKeyExtent("foo", null, null), "l1");

    List<Range> ranges = createNewRangeList(createNewRange(null, null));
    Map<String,Map<KeyExtent,List<Range>>> expected =
        createExpectedBinnings(createRangeLocation("l1", createNewKeyExtent("foo", null, null),
            createNewRangeList(createNewRange(null, null))));

    runTest(ranges, metaCache, expected);

    ranges = createNewRangeList(createNewRange("a", null));
    expected = createExpectedBinnings(createRangeLocation("l1",
        createNewKeyExtent("foo", null, null), createNewRangeList(createNewRange("a", null)))

    );

    runTest(ranges, metaCache, expected);

    ranges = createNewRangeList(createNewRange(null, "b"));
    expected = createExpectedBinnings(createRangeLocation("l1",
        createNewKeyExtent("foo", null, null), createNewRangeList(createNewRange(null, "b")))

    );

    runTest(ranges, metaCache, expected);
  }

  @Test
  public void testBinRanges2() throws Exception {

    List<Range> ranges = createNewRangeList(createNewRange(null, null));
    TabletLocatorImpl metaCache = createLocators("foo", createNewKeyExtent("foo", "g", null), "l1",
        createNewKeyExtent("foo", null, "g"), "l2");

    Map<String,
        Map<KeyExtent,List<Range>>> expected = createExpectedBinnings(
            createRangeLocation("l1", createNewKeyExtent("foo", "g", null),
                createNewRangeList(createNewRange(null, null))),
            createRangeLocation("l2", createNewKeyExtent("foo", null, "g"),
                createNewRangeList(createNewRange(null, null))));

    runTest(ranges, metaCache, expected);
  }

  @Test
  public void testBinRanges3() throws Exception {

    // test with three tablets and a range that covers the whole table
    List<Range> ranges = createNewRangeList(createNewRange(null, null));
    TabletLocatorImpl metaCache = createLocators("foo", createNewKeyExtent("foo", "g", null), "l1",
        createNewKeyExtent("foo", "m", "g"), "l2", createNewKeyExtent("foo", null, "m"), "l2");

    Map<String,Map<KeyExtent,List<Range>>> expected = createExpectedBinnings(
        createRangeLocation("l1", createNewKeyExtent("foo", "g", null),
            createNewRangeList(createNewRange(null, null))),
        createRangeLocation("l2", createNewKeyExtent("foo", "m", "g"),
            createNewRangeList(createNewRange(null, null)), createNewKeyExtent("foo", null, "m"),
            createNewRangeList(createNewRange(null, null))));

    runTest(ranges, metaCache, expected);

    // test with three tablets where one range falls within the first tablet and last two ranges
    // fall within the last tablet
    ranges = createNewRangeList(createNewRange(null, "c"), createNewRange("s", "y"),
        createNewRange("z", null));
    expected = createExpectedBinnings(
        createRangeLocation("l1", createNewKeyExtent("foo", "g", null),
            createNewRangeList(createNewRange(null, "c"))),
        createRangeLocation("l2", createNewKeyExtent("foo", null, "m"),
            createNewRangeList(createNewRange("s", "y"), createNewRange("z", null))));

    runTest(ranges, metaCache, expected);

    // test is same as above, but has an additional range that spans the first two tablets
    ranges = createNewRangeList(createNewRange(null, "c"), createNewRange("f", "i"),
        createNewRange("s", "y"), createNewRange("z", null));
    expected = createExpectedBinnings(
        createRangeLocation("l1", createNewKeyExtent("foo", "g", null),
            createNewRangeList(createNewRange(null, "c"), createNewRange("f", "i"))),
        createRangeLocation("l2", createNewKeyExtent("foo", "m", "g"),
            createNewRangeList(createNewRange("f", "i")), createNewKeyExtent("foo", null, "m"),
            createNewRangeList(createNewRange("s", "y"), createNewRange("z", null))));

    runTest(ranges, metaCache, expected);

    // test where start of range is not inclusive and same as tablet endRow
    ranges = createNewRangeList(createNewRange("g", false, "m", true));
    expected = createExpectedBinnings(createRangeLocation("l2", createNewKeyExtent("foo", "m", "g"),
        createNewRangeList(createNewRange("g", false, "m", true))));

    runTest(ranges, metaCache, expected);

    // test where start of range is inclusive and same as tablet endRow
    ranges = createNewRangeList(createNewRange("g", true, "m", true));
    expected = createExpectedBinnings(
        createRangeLocation("l1", createNewKeyExtent("foo", "g", null),
            createNewRangeList(createNewRange("g", true, "m", true))),
        createRangeLocation("l2", createNewKeyExtent("foo", "m", "g"),
            createNewRangeList(createNewRange("g", true, "m", true))));

    runTest(ranges, metaCache, expected);

    ranges = createNewRangeList(createNewRange("g", true, "m", false));
    expected = createExpectedBinnings(
        createRangeLocation("l1", createNewKeyExtent("foo", "g", null),
            createNewRangeList(createNewRange("g", true, "m", false))),
        createRangeLocation("l2", createNewKeyExtent("foo", "m", "g"),
            createNewRangeList(createNewRange("g", true, "m", false))));

    runTest(ranges, metaCache, expected);

    ranges = createNewRangeList(createNewRange("g", false, "m", false));
    expected = createExpectedBinnings(createRangeLocation("l2", createNewKeyExtent("foo", "m", "g"),
        createNewRangeList(createNewRange("g", false, "m", false))));

    runTest(ranges, metaCache, expected);
  }

  @Test
  public void testBinRanges4() throws Exception {

    List<Range> ranges = createNewRangeList(new Range(new Text("1")));
    TabletLocatorImpl metaCache = createLocators("foo", createNewKeyExtent("foo", "0", null), "l1",
        createNewKeyExtent("foo", "1", "0"), "l2", createNewKeyExtent("foo", "2", "1"), "l3",
        createNewKeyExtent("foo", "3", "2"), "l4", createNewKeyExtent("foo", null, "3"), "l5");

    Map<String,Map<KeyExtent,List<Range>>> expected =
        createExpectedBinnings(createRangeLocation("l2", createNewKeyExtent("foo", "1", "0"),
            createNewRangeList(new Range(new Text("1")))));

    runTest(ranges, metaCache, expected);

    Key rowColKey = new Key(new Text("3"), new Text("cf1"), new Text("cq1"));
    Range range =
        new Range(rowColKey, true, new Key(new Text("3")).followingKey(PartialKey.ROW), false);

    ranges = createNewRangeList(range);
    Map<String,Map<KeyExtent,List<Range>>> expected4 = createExpectedBinnings(
        createRangeLocation("l4", createNewKeyExtent("foo", "3", "2"), createNewRangeList(range)));

    runTest(ranges, metaCache, expected4, createNewRangeList());

    range = new Range(rowColKey, true, new Key(new Text("3")).followingKey(PartialKey.ROW), true);

    ranges = createNewRangeList(range);
    Map<String,Map<KeyExtent,List<Range>>> expected5 = createExpectedBinnings(
        createRangeLocation("l4", createNewKeyExtent("foo", "3", "2"), createNewRangeList(range)),
        createRangeLocation("l5", createNewKeyExtent("foo", null, "3"), createNewRangeList(range)));

    runTest(ranges, metaCache, expected5, createNewRangeList());

    range = new Range(new Text("2"), false, new Text("3"), false);
    ranges = createNewRangeList(range);
    Map<String,Map<KeyExtent,List<Range>>> expected6 = createExpectedBinnings(
        createRangeLocation("l4", createNewKeyExtent("foo", "3", "2"), createNewRangeList(range)));
    runTest(ranges, metaCache, expected6, createNewRangeList());

    range = new Range(new Text("2"), true, new Text("3"), false);
    ranges = createNewRangeList(range);
    Map<String,Map<KeyExtent,List<Range>>> expected7 = createExpectedBinnings(
        createRangeLocation("l3", createNewKeyExtent("foo", "2", "1"), createNewRangeList(range)),
        createRangeLocation("l4", createNewKeyExtent("foo", "3", "2"), createNewRangeList(range)));
    runTest(ranges, metaCache, expected7, createNewRangeList());

    range = new Range(new Text("2"), false, new Text("3"), true);
    ranges = createNewRangeList(range);
    Map<String,Map<KeyExtent,List<Range>>> expected8 = createExpectedBinnings(
        createRangeLocation("l4", createNewKeyExtent("foo", "3", "2"), createNewRangeList(range)));
    runTest(ranges, metaCache, expected8, createNewRangeList());

    range = new Range(new Text("2"), true, new Text("3"), true);
    ranges = createNewRangeList(range);
    Map<String,Map<KeyExtent,List<Range>>> expected9 = createExpectedBinnings(
        createRangeLocation("l3", createNewKeyExtent("foo", "2", "1"), createNewRangeList(range)),
        createRangeLocation("l4", createNewKeyExtent("foo", "3", "2"), createNewRangeList(range)));
    runTest(ranges, metaCache, expected9, createNewRangeList());

  }

  @Test
  public void testBinRanges5() throws Exception {
    // Test binning when there is a hole in the metadata

    List<Range> ranges = createNewRangeList(new Range(new Text("1")));
    TabletLocatorImpl metaCache = createLocators("foo", createNewKeyExtent("foo", "0", null), "l1",
        createNewKeyExtent("foo", "1", "0"), "l2", createNewKeyExtent("foo", "3", "2"), "l4",
        createNewKeyExtent("foo", null, "3"), "l5");

    Map<String,Map<KeyExtent,List<Range>>> expected1 =
        createExpectedBinnings(createRangeLocation("l2", createNewKeyExtent("foo", "1", "0"),
            createNewRangeList(new Range(new Text("1")))));

    runTest(ranges, metaCache, expected1);

    ranges = createNewRangeList(new Range(new Text("2")), new Range(new Text("11")));
    Map<String,Map<KeyExtent,List<Range>>> expected2 = createExpectedBinnings();

    runTest(ranges, metaCache, expected2, ranges);

    ranges = createNewRangeList(new Range(new Text("1")), new Range(new Text("2")));

    runTest(ranges, metaCache, expected1, createNewRangeList(new Range(new Text("2"))));

    ranges = createNewRangeList(createNewRange("0", "2"), createNewRange("3", "4"));
    Map<String,
        Map<KeyExtent,List<Range>>> expected3 = createExpectedBinnings(
            createRangeLocation("l4", createNewKeyExtent("foo", "3", "2"),
                createNewRangeList(createNewRange("3", "4"))),
            createRangeLocation("l5", createNewKeyExtent("foo", null, "3"),
                createNewRangeList(createNewRange("3", "4"))));

    runTest(ranges, metaCache, expected3, createNewRangeList(createNewRange("0", "2")));

    ranges = createNewRangeList(createNewRange("0", "1"), createNewRange("0", "11"),
        createNewRange("1", "2"), createNewRange("0", "4"), createNewRange("2", "4"),
        createNewRange("21", "4"));
    Map<String,
        Map<KeyExtent,List<Range>>> expected4 = createExpectedBinnings(
            createRangeLocation("l1", createNewKeyExtent("foo", "0", null),
                createNewRangeList(createNewRange("0", "1"))),
            createRangeLocation("l2", createNewKeyExtent("foo", "1", "0"),
                createNewRangeList(createNewRange("0", "1"))),
            createRangeLocation("l4", createNewKeyExtent("foo", "3", "2"),
                createNewRangeList(createNewRange("21", "4"))),
            createRangeLocation("l5", createNewKeyExtent("foo", null, "3"),
                createNewRangeList(createNewRange("21", "4"))));

    runTest(ranges, metaCache, expected4, createNewRangeList(createNewRange("0", "11"),
        createNewRange("1", "2"), createNewRange("0", "4"), createNewRange("2", "4")));
  }

  @Test
  public void testBinMutations1() throws Exception {
    // one tablet table
    KeyExtent ke1 = createNewKeyExtent("foo", null, null);
    TabletLocatorImpl metaCache = createLocators("foo", ke1, "l1");

    List<Mutation> ml = createNewMutationList(createNewMutation("a", "cf1:cq1=v1", "cf1:cq2=v2"),
        createNewMutation("c", "cf1:cq1=v3", "cf1:cq2=v4"));
    Map<String,Map<KeyExtent,List<String>>> emb = createServerExtentMap(
        createServerExtent("a", "l1", ke1), createServerExtent("c", "l1", ke1));
    runTest(metaCache, ml, emb);

    ml = createNewMutationList(createNewMutation("a", "cf1:cq1=v1", "cf1:cq2=v2"));
    emb = createServerExtentMap(createServerExtent("a", "l1", ke1));
    runTest(metaCache, ml, emb);

    ml = createNewMutationList(createNewMutation("a", "cf1:cq1=v1", "cf1:cq2=v2"),
        createNewMutation("a", "cf1:cq3=v3"));
    emb = createServerExtentMap(createServerExtent("a", "l1", ke1),
        createServerExtent("a", "l1", ke1));
    runTest(metaCache, ml, emb);

  }

  @Test
  public void testBinMutations2() throws Exception {
    // no tablets for table
    TabletLocatorImpl metaCache = createLocators("foo");

    List<Mutation> ml = createNewMutationList(createNewMutation("a", "cf1:cq1=v1", "cf1:cq2=v2"),
        createNewMutation("c", "cf1:cq1=v3", "cf1:cq2=v4"));
    Map<String,Map<KeyExtent,List<String>>> emb = createServerExtentMap();
    runTest(metaCache, ml, emb, "a", "c");
  }

  @Test
  public void testBinMutations3() throws Exception {
    // three tablet table
    KeyExtent ke1 = createNewKeyExtent("foo", "h", null);
    KeyExtent ke2 = createNewKeyExtent("foo", "t", "h");
    KeyExtent ke3 = createNewKeyExtent("foo", null, "t");

    TabletLocatorImpl metaCache = createLocators("foo", ke1, "l1", ke2, "l2", ke3, "l3");

    List<Mutation> ml = createNewMutationList(createNewMutation("a", "cf1:cq1=v1", "cf1:cq2=v2"),
        createNewMutation("i", "cf1:cq1=v3", "cf1:cq2=v4"));
    Map<String,Map<KeyExtent,List<String>>> emb = createServerExtentMap(
        createServerExtent("a", "l1", ke1), createServerExtent("i", "l2", ke2));
    runTest(metaCache, ml, emb);

    ml = createNewMutationList(createNewMutation("a", "cf1:cq1=v1", "cf1:cq2=v2"));
    emb = createServerExtentMap(createServerExtent("a", "l1", ke1));
    runTest(metaCache, ml, emb);

    ml = createNewMutationList(createNewMutation("a", "cf1:cq1=v1", "cf1:cq2=v2"),
        createNewMutation("a", "cf1:cq3=v3"));
    emb = createServerExtentMap(createServerExtent("a", "l1", ke1),
        createServerExtent("a", "l1", ke1));
    runTest(metaCache, ml, emb);

    ml = createNewMutationList(createNewMutation("a", "cf1:cq1=v1", "cf1:cq2=v2"),
        createNewMutation("w", "cf1:cq3=v3"));
    emb = createServerExtentMap(createServerExtent("a", "l1", ke1),
        createServerExtent("w", "l3", ke3));
    runTest(metaCache, ml, emb);

    ml = createNewMutationList(createNewMutation("a", "cf1:cq1=v1", "cf1:cq2=v2"),
        createNewMutation("w", "cf1:cq3=v3"), createNewMutation("z", "cf1:cq4=v4"));
    emb = createServerExtentMap(createServerExtent("a", "l1", ke1),
        createServerExtent("w", "l3", ke3), createServerExtent("z", "l3", ke3));
    runTest(metaCache, ml, emb);

    ml = createNewMutationList(createNewMutation("h", "cf1:cq1=v1", "cf1:cq2=v2"),
        createNewMutation("t", "cf1:cq1=v1", "cf1:cq2=v2"));
    emb = createServerExtentMap(createServerExtent("h", "l1", ke1),
        createServerExtent("t", "l2", ke2));
    runTest(metaCache, ml, emb);
  }

  @Test
  public void testBinMutations4() throws Exception {
    // three table with hole
    KeyExtent ke1 = createNewKeyExtent("foo", "h", null);

    KeyExtent ke3 = createNewKeyExtent("foo", null, "t");

    TabletLocatorImpl metaCache = createLocators("foo", ke1, "l1", ke3, "l3");

    List<Mutation> ml = createNewMutationList(createNewMutation("a", "cf1:cq1=v1", "cf1:cq2=v2"),
        createNewMutation("i", "cf1:cq1=v3", "cf1:cq2=v4"));
    Map<String,Map<KeyExtent,List<String>>> emb =
        createServerExtentMap(createServerExtent("a", "l1", ke1));
    runTest(metaCache, ml, emb, "i");

    ml = createNewMutationList(createNewMutation("a", "cf1:cq1=v1", "cf1:cq2=v2"));
    emb = createServerExtentMap(createServerExtent("a", "l1", ke1));
    runTest(metaCache, ml, emb);

    ml = createNewMutationList(createNewMutation("a", "cf1:cq1=v1", "cf1:cq2=v2"),
        createNewMutation("a", "cf1:cq3=v3"));
    emb = createServerExtentMap(createServerExtent("a", "l1", ke1),
        createServerExtent("a", "l1", ke1));
    runTest(metaCache, ml, emb);

    ml = createNewMutationList(createNewMutation("a", "cf1:cq1=v1", "cf1:cq2=v2"),
        createNewMutation("w", "cf1:cq3=v3"));
    emb = createServerExtentMap(createServerExtent("a", "l1", ke1),
        createServerExtent("w", "l3", ke3));
    runTest(metaCache, ml, emb);

    ml = createNewMutationList(createNewMutation("a", "cf1:cq1=v1", "cf1:cq2=v2"),
        createNewMutation("w", "cf1:cq3=v3"), createNewMutation("z", "cf1:cq4=v4"));
    emb = createServerExtentMap(createServerExtent("a", "l1", ke1),
        createServerExtent("w", "l3", ke3), createServerExtent("z", "l3", ke3));
    runTest(metaCache, ml, emb);

    ml = createNewMutationList(createNewMutation("a", "cf1:cq1=v1", "cf1:cq2=v2"),
        createNewMutation("w", "cf1:cq3=v3"), createNewMutation("z", "cf1:cq4=v4"),
        createNewMutation("t", "cf1:cq5=v5"));
    emb = createServerExtentMap(createServerExtent("a", "l1", ke1),
        createServerExtent("w", "l3", ke3), createServerExtent("z", "l3", ke3));
    runTest(metaCache, ml, emb, "t");
  }

  @Test
  public void testBinSplit() throws Exception {
    // try binning mutations and ranges when a tablet splits

    for (int i = 0; i < 3; i++) {
      // when i == 0 only test binning mutations
      // when i == 1 only test binning ranges
      // when i == 2 test both

      KeyExtent ke1 = createNewKeyExtent("foo", null, null);
      TServers tservers = new TServers();
      TabletLocatorImpl metaCache =
          createLocators(tservers, "tserver1", "tserver2", "foo", ke1, "l1");

      List<Mutation> ml = createNewMutationList(createNewMutation("a", "cf1:cq1=v1", "cf1:cq2=v2"),
          createNewMutation("m", "cf1:cq1=v3", "cf1:cq2=v4"), createNewMutation("z", "cf1:cq1=v5"));
      Map<String,Map<KeyExtent,List<String>>> emb =
          createServerExtentMap(createServerExtent("a", "l1", ke1),
              createServerExtent("m", "l1", ke1), createServerExtent("z", "l1", ke1));
      if (i == 0 || i == 2) {
        runTest(metaCache, ml, emb);
      }

      List<Range> ranges = createNewRangeList(new Range(new Text("a")), new Range(new Text("m")),
          new Range(new Text("z")));

      Map<String,Map<KeyExtent,List<Range>>> expected1 = createExpectedBinnings(
          createRangeLocation("l1", createNewKeyExtent("foo", null, null), ranges));

      if (i == 1 || i == 2) {
        runTest(ranges, metaCache, expected1);
      }

      KeyExtent ke11 = createNewKeyExtent("foo", "n", null);
      KeyExtent ke12 = createNewKeyExtent("foo", null, "n");

      setLocation(tservers, "tserver2", METADATA_TABLE_EXTENT, ke12, "l2");

      metaCache.invalidateCache(ke1);

      emb = createServerExtentMap(createServerExtent("z", "l2", ke12));
      if (i == 0 || i == 2) {
        runTest(metaCache, ml, emb, "a", "m");
      }

      Map<String,Map<KeyExtent,List<Range>>> expected2 =
          createExpectedBinnings(createRangeLocation("l2", createNewKeyExtent("foo", null, "n"),
              createNewRangeList(new Range(new Text("z")))));

      if (i == 1 || i == 2) {
        runTest(ranges, metaCache, expected2,
            createNewRangeList(new Range(new Text("a")), new Range(new Text("m"))));
      }

      setLocation(tservers, "tserver2", METADATA_TABLE_EXTENT, ke11, "l3");
      emb = createServerExtentMap(createServerExtent("a", "l3", ke11),
          createServerExtent("m", "l3", ke11), createServerExtent("z", "l2", ke12));
      if (i == 0 || i == 2) {
        runTest(metaCache, ml, emb);
      }

      Map<String,
          Map<KeyExtent,List<Range>>> expected3 = createExpectedBinnings(
              createRangeLocation("l2", createNewKeyExtent("foo", null, "n"),
                  createNewRangeList(new Range(new Text("z")))),
              createRangeLocation("l3", createNewKeyExtent("foo", "n", null),
                  createNewRangeList(new Range(new Text("a")), new Range(new Text("m"))))

      );

      if (i == 1 || i == 2) {
        runTest(ranges, metaCache, expected3);
      }
    }
  }

  @Test
  public void testBug1() throws Exception {
    // a bug that occurred while running continuous ingest
    KeyExtent mte1 = new KeyExtent(MetadataTable.ID, new Text("0;0bc"), ROOT_TABLE_EXTENT.endRow());
    KeyExtent mte2 = new KeyExtent(MetadataTable.ID, null, new Text("0;0bc"));

    TServers tservers = new TServers();
    TestTabletLocationObtainer ttlo = new TestTabletLocationObtainer(tservers);

    RootTabletLocator rtl = new TestRootTabletLocator();
    TabletLocatorImpl rootTabletCache =
        new TabletLocatorImpl(MetadataTable.ID, rtl, ttlo, new YesLockChecker());
    TabletLocatorImpl tab0TabletCache =
        new TabletLocatorImpl(TableId.of("0"), rootTabletCache, ttlo, new YesLockChecker());

    setLocation(tservers, "tserver1", ROOT_TABLE_EXTENT, mte1, "tserver2");
    setLocation(tservers, "tserver1", ROOT_TABLE_EXTENT, mte2, "tserver3");

    // create two tablets that straddle a metadata split point
    KeyExtent ke1 = new KeyExtent(TableId.of("0"), new Text("0bbf20e"), null);
    KeyExtent ke2 = new KeyExtent(TableId.of("0"), new Text("0bc0756"), new Text("0bbf20e"));

    setLocation(tservers, "tserver2", mte1, ke1, "tserver4");
    setLocation(tservers, "tserver3", mte2, ke2, "tserver5");

    // look up something that comes after the last entry in mte1
    locateTabletTest(tab0TabletCache, "0bbff", ke2, "tserver5");
  }

  @Test
  public void testBug2() throws Exception {
    // a bug that occurred while running a functional test
    KeyExtent mte1 = new KeyExtent(MetadataTable.ID, new Text("~"), ROOT_TABLE_EXTENT.endRow());
    KeyExtent mte2 = new KeyExtent(MetadataTable.ID, null, new Text("~"));

    TServers tservers = new TServers();
    TestTabletLocationObtainer ttlo = new TestTabletLocationObtainer(tservers);

    RootTabletLocator rtl = new TestRootTabletLocator();
    TabletLocatorImpl rootTabletCache =
        new TabletLocatorImpl(MetadataTable.ID, rtl, ttlo, new YesLockChecker());
    TabletLocatorImpl tab0TabletCache =
        new TabletLocatorImpl(TableId.of("0"), rootTabletCache, ttlo, new YesLockChecker());

    setLocation(tservers, "tserver1", ROOT_TABLE_EXTENT, mte1, "tserver2");
    setLocation(tservers, "tserver1", ROOT_TABLE_EXTENT, mte2, "tserver3");

    // create the ~ tablet so it exists
    Map<KeyExtent,SortedMap<Key,Value>> ts3 = new HashMap<>();
    ts3.put(mte2, new TreeMap<>());
    tservers.tservers.put("tserver3", ts3);

    assertNull(tab0TabletCache.locateTablet(context, new Text("row_0000000000"), false, false));

  }

  // this test reproduces a problem where empty metadata tablets, that were created by user tablets
  // being merged away, caused locating tablets to fail
  @Test
  public void testBug3() throws Exception {
    KeyExtent mte1 = new KeyExtent(MetadataTable.ID, new Text("1;c"), ROOT_TABLE_EXTENT.endRow());
    KeyExtent mte2 = new KeyExtent(MetadataTable.ID, new Text("1;f"), new Text("1;c"));
    KeyExtent mte3 = new KeyExtent(MetadataTable.ID, new Text("1;j"), new Text("1;f"));
    KeyExtent mte4 = new KeyExtent(MetadataTable.ID, new Text("1;r"), new Text("1;j"));
    KeyExtent mte5 = new KeyExtent(MetadataTable.ID, null, new Text("1;r"));

    KeyExtent ke1 = new KeyExtent(TableId.of("1"), null, null);

    TServers tservers = new TServers();
    TestTabletLocationObtainer ttlo = new TestTabletLocationObtainer(tservers);

    RootTabletLocator rtl = new TestRootTabletLocator();

    TabletLocatorImpl rootTabletCache =
        new TabletLocatorImpl(MetadataTable.ID, rtl, ttlo, new YesLockChecker());
    TabletLocatorImpl tab0TabletCache =
        new TabletLocatorImpl(TableId.of("1"), rootTabletCache, ttlo, new YesLockChecker());

    setLocation(tservers, "tserver1", ROOT_TABLE_EXTENT, mte1, "tserver2");
    setLocation(tservers, "tserver1", ROOT_TABLE_EXTENT, mte2, "tserver3");
    setLocation(tservers, "tserver1", ROOT_TABLE_EXTENT, mte3, "tserver4");
    setLocation(tservers, "tserver1", ROOT_TABLE_EXTENT, mte4, "tserver5");
    setLocation(tservers, "tserver1", ROOT_TABLE_EXTENT, mte5, "tserver6");

    createEmptyTablet(tservers, "tserver2", mte1);
    createEmptyTablet(tservers, "tserver3", mte2);
    createEmptyTablet(tservers, "tserver4", mte3);
    createEmptyTablet(tservers, "tserver5", mte4);
    setLocation(tservers, "tserver6", mte5, ke1, "tserver7");

    locateTabletTest(tab0TabletCache, "a", ke1, "tserver7");

  }

  @Test
  public void testAccumulo1248() {
    TServers tservers = new TServers();
    TabletLocatorImpl metaCache = createLocators(tservers, "tserver1", "tserver2", "foo");

    KeyExtent ke1 = createNewKeyExtent("foo", null, null);

    // set two locations for a tablet, this is not supposed to happen. The metadata cache should
    // throw an exception if it sees this rather than caching one of
    // the locations.
    setLocation(tservers, "tserver2", METADATA_TABLE_EXTENT, ke1, "L1", "I1");
    setLocation(tservers, "tserver2", METADATA_TABLE_EXTENT, ke1, "L2", "I2");

    var e = assertThrows(IllegalStateException.class,
        () -> metaCache.locateTablet(context, new Text("a"), false, false));
    assertTrue(e.getMessage().startsWith("Tablet has multiple locations : "));

  }

  @Test
  public void testLostLock() throws Exception {

    final HashSet<String> activeLocks = new HashSet<>();

    TServers tservers = new TServers();
    TabletLocatorImpl metaCache =
        createLocators(tservers, "tserver1", "tserver2", "foo", new TabletServerLockChecker() {
          @Override
          public boolean isLockHeld(String tserver, String session) {
            return activeLocks.contains(tserver + ":" + session);
          }

          @Override
          public void invalidateCache(String server) {}
        });

    KeyExtent ke1 = createNewKeyExtent("foo", null, null);
    setLocation(tservers, "tserver2", METADATA_TABLE_EXTENT, ke1, "L1", "5");

    activeLocks.add("L1:5");

    locateTabletTest(metaCache, "a", ke1, "L1");
    locateTabletTest(metaCache, "a", ke1, "L1");

    activeLocks.clear();

    locateTabletTest(metaCache, "a", null, null);
    locateTabletTest(metaCache, "a", null, null);
    locateTabletTest(metaCache, "a", null, null);

    clearLocation(tservers, "tserver2", METADATA_TABLE_EXTENT, ke1, "5");
    setLocation(tservers, "tserver2", METADATA_TABLE_EXTENT, ke1, "L2", "6");

    activeLocks.add("L2:6");

    locateTabletTest(metaCache, "a", ke1, "L2");
    locateTabletTest(metaCache, "a", ke1, "L2");

    clearLocation(tservers, "tserver2", METADATA_TABLE_EXTENT, ke1, "6");

    locateTabletTest(metaCache, "a", ke1, "L2");

    setLocation(tservers, "tserver2", METADATA_TABLE_EXTENT, ke1, "L3", "7");

    locateTabletTest(metaCache, "a", ke1, "L2");

    activeLocks.clear();

    locateTabletTest(metaCache, "a", null, null);
    locateTabletTest(metaCache, "a", null, null);

    activeLocks.add("L3:7");

    locateTabletTest(metaCache, "a", ke1, "L3");
    locateTabletTest(metaCache, "a", ke1, "L3");

    List<Mutation> ml = createNewMutationList(createNewMutation("a", "cf1:cq1=v1", "cf1:cq2=v2"),
        createNewMutation("w", "cf1:cq3=v3"));
    Map<String,Map<KeyExtent,List<String>>> emb = createServerExtentMap(
        createServerExtent("a", "L3", ke1), createServerExtent("w", "L3", ke1));
    runTest(metaCache, ml, emb);

    clearLocation(tservers, "tserver2", METADATA_TABLE_EXTENT, ke1, "7");

    runTest(metaCache, ml, emb);

    activeLocks.clear();

    emb.clear();

    runTest(metaCache, ml, emb, "a", "w");
    runTest(metaCache, ml, emb, "a", "w");

    KeyExtent ke11 = createNewKeyExtent("foo", "m", null);
    KeyExtent ke12 = createNewKeyExtent("foo", null, "m");

    setLocation(tservers, "tserver2", METADATA_TABLE_EXTENT, ke11, "L1", "8");
    setLocation(tservers, "tserver2", METADATA_TABLE_EXTENT, ke12, "L2", "9");

    runTest(metaCache, ml, emb, "a", "w");

    activeLocks.add("L1:8");

    emb = createServerExtentMap(createServerExtent("a", "L1", ke11));
    runTest(metaCache, ml, emb, "w");

    activeLocks.add("L2:9");

    emb = createServerExtentMap(createServerExtent("a", "L1", ke11),
        createServerExtent("w", "L2", ke12));
    runTest(metaCache, ml, emb);

    List<Range> ranges =
        createNewRangeList(new Range("a"), createNewRange("b", "o"), createNewRange("r", "z"));
    Map<String,
        Map<KeyExtent,List<Range>>> expected = createExpectedBinnings(
            createRangeLocation("L1", ke11,
                createNewRangeList(new Range("a"), createNewRange("b", "o"))),
            createRangeLocation("L2", ke12,
                createNewRangeList(createNewRange("b", "o"), createNewRange("r", "z"))));

    runTest(ranges, metaCache, expected);

    activeLocks.remove("L2:9");

    expected =
        createExpectedBinnings(createRangeLocation("L1", ke11, createNewRangeList(new Range("a"))));
    runTest(ranges, metaCache, expected,
        createNewRangeList(createNewRange("b", "o"), createNewRange("r", "z")));

    activeLocks.clear();

    expected = createExpectedBinnings();
    runTest(ranges, metaCache, expected,
        createNewRangeList(new Range("a"), createNewRange("b", "o"), createNewRange("r", "z")));

    clearLocation(tservers, "tserver2", METADATA_TABLE_EXTENT, ke11, "8");
    clearLocation(tservers, "tserver2", METADATA_TABLE_EXTENT, ke12, "9");
    setLocation(tservers, "tserver2", METADATA_TABLE_EXTENT, ke11, "L3", "10");
    setLocation(tservers, "tserver2", METADATA_TABLE_EXTENT, ke12, "L4", "11");

    runTest(ranges, metaCache, expected,
        createNewRangeList(new Range("a"), createNewRange("b", "o"), createNewRange("r", "z")));

    activeLocks.add("L3:10");

    expected =
        createExpectedBinnings(createRangeLocation("L3", ke11, createNewRangeList(new Range("a"))));
    runTest(ranges, metaCache, expected,
        createNewRangeList(createNewRange("b", "o"), createNewRange("r", "z")));

    activeLocks.add("L4:11");

    expected = createExpectedBinnings(
        createRangeLocation("L3", ke11,
            createNewRangeList(new Range("a"), createNewRange("b", "o"))),
        createRangeLocation("L4", ke12,
            createNewRangeList(createNewRange("b", "o"), createNewRange("r", "z"))));
    runTest(ranges, metaCache, expected);
  }

}

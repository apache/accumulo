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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.client.admin.TabletAvailability;
import org.apache.accumulo.core.clientImpl.ClientTabletCache.CachedTablet;
import org.apache.accumulo.core.clientImpl.ClientTabletCache.CachedTablets;
import org.apache.accumulo.core.clientImpl.ClientTabletCache.LocationNeed;
import org.apache.accumulo.core.clientImpl.ClientTabletCache.TabletServerMutations;
import org.apache.accumulo.core.clientImpl.ClientTabletCacheImpl.CachedTabletObtainer;
import org.apache.accumulo.core.clientImpl.ClientTabletCacheImpl.TabletServerLockChecker;
import org.apache.accumulo.core.data.InstanceId;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.metadata.AccumuloTable;
import org.apache.accumulo.core.metadata.MetadataCachedTabletObtainer;
import org.apache.accumulo.core.metadata.RootTable;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.CurrentLocationColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.TabletColumnFamily;
import org.apache.accumulo.core.util.Pair;
import org.apache.hadoop.io.Text;
import org.easymock.EasyMock;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class ClientTabletCacheImplTest {

  private static final KeyExtent ROOT_TABLE_EXTENT = RootTable.EXTENT;
  private static final KeyExtent METADATA_TABLE_EXTENT =
      new KeyExtent(AccumuloTable.METADATA.tableId(), null, ROOT_TABLE_EXTENT.endRow());

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
    return List.of(ranges);
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
      Map<KeyExtent,List<Range>> binnedKE =
          expBinnedRanges.computeIfAbsent(rl.location, k -> new HashMap<>());
      expBinnedRanges.put(rl.location, binnedKE);
      binnedKE.putAll(rl.extents);
    }
    return expBinnedRanges;
  }

  static TreeMap<KeyExtent,CachedTablet> createMetaCacheKE(Object... data) {
    TreeMap<KeyExtent,CachedTablet> mcke = new TreeMap<>();

    for (int i = 0; i < data.length; i += 2) {
      KeyExtent ke = (KeyExtent) data[i];
      String loc = (String) data[i + 1];
      mcke.put(ke, new CachedTablet(ke, loc, "1", TabletAvailability.ONDEMAND, false));
    }

    return mcke;
  }

  static TreeMap<Text,CachedTablet> createMetaCache(Object... data) {
    TreeMap<KeyExtent,CachedTablet> mcke = createMetaCacheKE(data);

    TreeMap<Text,CachedTablet> mc = new TreeMap<>(ClientTabletCacheImpl.END_ROW_COMPARATOR);

    for (Entry<KeyExtent,CachedTablet> entry : mcke.entrySet()) {
      if (entry.getKey().endRow() == null) {
        mc.put(ClientTabletCacheImpl.MAX_TEXT, entry.getValue());
      } else {
        mc.put(entry.getKey().endRow(), entry.getValue());
      }
    }

    return mc;
  }

  static ClientTabletCacheImpl createLocators(TServers tservers, String rootTabLoc,
      String metaTabLoc, String table, TabletServerLockChecker tslc, Object... data) {

    TreeMap<KeyExtent,CachedTablet> mcke = createMetaCacheKE(data);

    TestCachedTabletObtainer ttlo = new TestCachedTabletObtainer(tservers);

    RootClientTabletCache rtl = new TestRootClientTabletCache();
    ClientTabletCacheImpl rootTabletCache = new ClientTabletCacheImpl(
        AccumuloTable.METADATA.tableId(), rtl, ttlo, new YesLockChecker());
    ClientTabletCacheImpl tab1TabletCache =
        new ClientTabletCacheImpl(TableId.of(table), rootTabletCache, ttlo, tslc);
    // disable hosting requests for these tests
    tab1TabletCache.enableTabletHostingRequests(false);

    setLocation(tservers, rootTabLoc, ROOT_TABLE_EXTENT, METADATA_TABLE_EXTENT, metaTabLoc);

    for (Entry<KeyExtent,CachedTablet> entry : mcke.entrySet()) {
      setLocation(tservers, metaTabLoc, METADATA_TABLE_EXTENT, entry.getKey(),
          entry.getValue().getTserverLocation().orElseThrow());
    }

    return tab1TabletCache;

  }

  static ClientTabletCacheImpl createLocators(TServers tservers, String rootTabLoc,
      String metaTabLoc, String table, Object... data) {
    return createLocators(tservers, rootTabLoc, metaTabLoc, table, new YesLockChecker(), data);
  }

  static ClientTabletCacheImpl createLocators(String table, Object... data) {
    TServers tservers = new TServers();
    return createLocators(tservers, "tserver1", "tserver2", table, data);
  }

  private ClientContext context;
  private InstanceId iid;

  @BeforeEach
  public void setUp() throws AccumuloException, TableNotFoundException {
    context = EasyMock.createMock(ClientContext.class);
    TableOperations tops = EasyMock.createMock(TableOperations.class);
    EasyMock.expect(context.tableOperations()).andReturn(tops).anyTimes();
    EasyMock.expect(context.getTableName(AccumuloTable.ROOT.tableId()))
        .andReturn(AccumuloTable.ROOT.tableName()).anyTimes();
    EasyMock.expect(context.getTableName(AccumuloTable.METADATA.tableId()))
        .andReturn(AccumuloTable.METADATA.tableName()).anyTimes();
    EasyMock.expect(context.getTableName(TableId.of("foo"))).andReturn("foo").anyTimes();
    EasyMock.expect(context.getTableName(TableId.of("0"))).andReturn("0").anyTimes();
    EasyMock.expect(context.getTableName(TableId.of("1"))).andReturn("1").anyTimes();
    EasyMock.expect(context.getTableName(TableId.of("tab1"))).andReturn("tab1").anyTimes();
    EasyMock.expect(tops.isOnline("foo")).andReturn(true).anyTimes();
    EasyMock.expect(tops.isOnline("0")).andReturn(true).anyTimes();
    EasyMock.expect(tops.isOnline("1")).andReturn(true).anyTimes();
    EasyMock.expect(tops.isOnline("tab1")).andReturn(true).anyTimes();
    iid = InstanceId.of("instance1");
    EasyMock.expect(context.getRootTabletLocation()).andReturn("tserver1").anyTimes();
    EasyMock.expect(context.getInstanceID()).andReturn(iid).anyTimes();
    replay(context, tops);
  }

  private void runTest(List<Range> ranges, ClientTabletCacheImpl tab1TabletCache,
      Map<String,Map<KeyExtent,List<Range>>> expected) throws Exception {
    List<Range> failures = Collections.emptyList();
    runTest(ranges, tab1TabletCache, expected, failures);
  }

  private void runTest(List<Range> ranges, ClientTabletCacheImpl tab1TabletCache,
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

  static void runTest(TreeMap<Text,CachedTablet> metaCache, KeyExtent remove,
      Set<KeyExtent> expected) {
    // copy so same metaCache can be used for multiple test

    metaCache = new TreeMap<>(metaCache);

    ClientTabletCacheImpl.removeOverlapping(metaCache, remove);

    HashSet<KeyExtent> eic = new HashSet<>();
    for (CachedTablet tl : metaCache.values()) {
      eic.add(tl.getExtent());
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
    return List.of(ma);
  }

  private void runTest(ClientTabletCacheImpl metaCache, List<Mutation> ml,
      Map<String,Map<KeyExtent,List<String>>> emb, String... efailures) throws Exception {
    Map<String,TabletServerMutations<Mutation>> binnedMutations = new HashMap<>();
    List<Mutation> afailures = new ArrayList<>();
    metaCache.binMutations(context, ml, binnedMutations, afailures);

    verify(emb, binnedMutations);

    ArrayList<String> afs = new ArrayList<>();
    ArrayList<String> efs = new ArrayList<>(List.of(efailures));

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
    TreeMap<Text,CachedTablet> mc = createMetaCache(createNewKeyExtent("0", null, null), "l1");

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
    TreeMap<Text,CachedTablet> mc = createMetaCache(createNewKeyExtent("0", "r", "g"), "l1",
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

  static class TestCachedTabletObtainer implements CachedTabletObtainer {

    private final Map<String,Map<KeyExtent,SortedMap<Key,Value>>> tservers;

    TestCachedTabletObtainer(TServers tservers) {
      this.tservers = tservers.tservers;
    }

    @Override
    public CachedTablets lookupTablet(ClientContext context, CachedTablet src, Text row,
        Text stopRow, ClientTabletCache parent) {

      Map<KeyExtent,SortedMap<Key,Value>> tablets =
          tservers.get(src.getTserverLocation().orElseThrow());

      if (tablets == null) {
        parent.invalidateCache(context, src.getTserverLocation().orElseThrow());
        return null;
      }

      SortedMap<Key,Value> tabletData = tablets.get(src.getExtent());

      if (tabletData == null) {
        parent.invalidateCache(src.getExtent());
        return null;
      }

      // the following clip is done on a tablet, do it here to see if it throws exceptions
      src.getExtent().toDataRange().clip(new Range(row, true, stopRow, true));

      Key startKey = new Key(row);
      Key stopKey = new Key(stopRow).followingKey(PartialKey.ROW);

      SortedMap<Key,Value> results = tabletData.tailMap(startKey).headMap(stopKey);

      return MetadataCachedTabletObtainer.getMetadataLocationEntries(results);
    }

    @Override
    public List<CachedTablet> lookupTablets(ClientContext context, String tserver,
        Map<KeyExtent,List<Range>> map, ClientTabletCache parent) {

      ArrayList<CachedTablet> list = new ArrayList<>();

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

      return MetadataCachedTabletObtainer.getMetadataLocationEntries(results).getCachedTablets();

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

  static class TestRootClientTabletCache extends RootClientTabletCache {

    TestRootClientTabletCache() {
      super(new YesLockChecker());
    }

    @Override
    protected CachedTablet getRootTabletLocation(ClientContext context) {
      return new CachedTablet(RootTable.EXTENT, context.getRootTabletLocation(), "1",
          TabletAvailability.HOSTED, false);
    }

    @Override
    public void invalidateCache(ClientContext context, String server) {}

  }

  static void createEmptyTablet(TServers tservers, String server, KeyExtent tablet) {
    Map<KeyExtent,SortedMap<Key,Value>> tablets =
        tservers.tservers.computeIfAbsent(server, k -> new HashMap<>());
    SortedMap<Key,Value> tabletData = tablets.computeIfAbsent(tablet, k -> new TreeMap<>());
    if (!tabletData.isEmpty()) {
      throw new IllegalStateException("Asked for empty tablet, but non empty tablet exists");
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

    Key hk = new Key(mr, TabletColumnFamily.AVAILABILITY_COLUMN.getColumnFamily(),
        TabletColumnFamily.AVAILABILITY_COLUMN.getColumnQualifier());
    tabletData.put(hk, TabletAvailabilityUtil.toValue(TabletAvailability.ONDEMAND));

    Key pk = new Key(mr, TabletColumnFamily.PREV_ROW_COLUMN.getColumnFamily(),
        TabletColumnFamily.PREV_ROW_COLUMN.getColumnQualifier());
    tabletData.put(pk, per);
  }

  static void deleteLocation(TServers tservers, String server, KeyExtent tablet, KeyExtent ke,
      String instance) {
    Map<KeyExtent,SortedMap<Key,Value>> tablets =
        tservers.tservers.computeIfAbsent(server, k -> new HashMap<>());
    SortedMap<Key,Value> tabletData = tablets.computeIfAbsent(tablet, k -> new TreeMap<>());

    Text mr = ke.toMetaRow();
    Key lk = new Key(mr, CurrentLocationColumnFamily.NAME, new Text(instance));
    tabletData.remove(lk);
  }

  static void setLocation(TServers tservers, String server, KeyExtent tablet, KeyExtent ke,
      String location) {
    setLocation(tservers, server, tablet, ke, location, "");
  }

  static void deleteServer(TServers tservers, String server) {
    tservers.tservers.remove(server);

  }

  private void locateTabletTest(ClientTabletCacheImpl cache, String row, boolean skipRow,
      KeyExtent expected, String server, LocationNeed locationNeeded) throws Exception {
    CachedTablet tl = cache.findTablet(context, new Text(row), skipRow, locationNeeded);

    if (expected == null) {
      if (tl != null) {
        System.out.println("tl = " + tl);
      }
      assertNull(tl);
    } else {
      assertNotNull(tl);
      if (server == null) {
        assertTrue(tl.getTserverLocation().isEmpty());
        assertTrue(tl.getTserverSession().isEmpty());
      } else {
        assertEquals(server, tl.getTserverLocation().orElseThrow());
      }
      assertEquals(expected, tl.getExtent());
    }
  }

  private void locateTabletTest(ClientTabletCacheImpl cache, String row, KeyExtent expected,
      String server) throws Exception {
    locateTabletTest(cache, row, false, expected, server, LocationNeed.REQUIRED);
  }

  @Test
  public void test1() throws Exception {
    TServers tservers = new TServers();
    TestCachedTabletObtainer ttlo = new TestCachedTabletObtainer(tservers);

    RootClientTabletCache rtl = new TestRootClientTabletCache();
    ClientTabletCacheImpl rootTabletCache = new ClientTabletCacheImpl(
        AccumuloTable.METADATA.tableId(), rtl, ttlo, new YesLockChecker());
    ClientTabletCacheImpl tab1TabletCache =
        new ClientTabletCacheImpl(TableId.of("tab1"), rootTabletCache, ttlo, new YesLockChecker());

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
    locateTabletTest(tab1TabletCache, "a", true, tab1e1, "tserver4", LocationNeed.REQUIRED);
    locateTabletTest(tab1TabletCache, "g", tab1e1, "tserver4");
    locateTabletTest(tab1TabletCache, "g", true, tab1e2, "tserver5", LocationNeed.REQUIRED);

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
    TableOperations tops = EasyMock.createMock(TableOperations.class);
    EasyMock.expect(context.tableOperations()).andReturn(tops).anyTimes();
    EasyMock.expect(context.getTableName(AccumuloTable.ROOT.tableId()))
        .andReturn(AccumuloTable.ROOT.tableName()).anyTimes();
    EasyMock.expect(context.getTableName(AccumuloTable.METADATA.tableId()))
        .andReturn(AccumuloTable.METADATA.tableName()).anyTimes();
    EasyMock.expect(context.getTableName(TableId.of("foo"))).andReturn("foo").anyTimes();
    EasyMock.expect(context.getTableName(TableId.of("0"))).andReturn("0").anyTimes();
    EasyMock.expect(context.getTableName(TableId.of("1"))).andReturn("1").anyTimes();
    EasyMock.expect(context.getTableName(TableId.of("tab1"))).andReturn("tab1").anyTimes();
    iid = InstanceId.of("instance1");
    EasyMock.expect(context.getRootTabletLocation()).andReturn("tserver4").anyTimes();
    EasyMock.expect(context.getInstanceID()).andReturn(iid).anyTimes();
    replay(context, tops);

    setLocation(tservers, "tserver4", ROOT_TABLE_EXTENT, METADATA_TABLE_EXTENT, "tserver5");
    setLocation(tservers, "tserver5", METADATA_TABLE_EXTENT, tab1e1, "tserver1");
    setLocation(tservers, "tserver5", METADATA_TABLE_EXTENT, tab1e21, "tserver2");
    setLocation(tservers, "tserver5", METADATA_TABLE_EXTENT, tab1e22, "tserver3");

    locateTabletTest(tab1TabletCache, "a", tab1e1, "tserver1");
    locateTabletTest(tab1TabletCache, "h", tab1e21, "tserver2");
    locateTabletTest(tab1TabletCache, "r", tab1e22, "tserver3");

    // simulate the metadata table splitting
    KeyExtent mte1 = new KeyExtent(AccumuloTable.METADATA.tableId(), tab1e21.toMetaRow(),
        ROOT_TABLE_EXTENT.endRow());
    KeyExtent mte2 = new KeyExtent(AccumuloTable.METADATA.tableId(), null, tab1e21.toMetaRow());

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
    KeyExtent mte11 = new KeyExtent(AccumuloTable.METADATA.tableId(), tab1e1.toMetaRow(),
        ROOT_TABLE_EXTENT.endRow());
    KeyExtent mte12 =
        new KeyExtent(AccumuloTable.METADATA.tableId(), tab1e21.toMetaRow(), tab1e1.toMetaRow());
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
    ClientTabletCacheImpl metaCache = createLocators(tservers, "tserver1", "tserver2", "foo");

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

    ClientTabletCacheImpl metaCache =
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
    ClientTabletCacheImpl metaCache = createLocators("foo", createNewKeyExtent("foo", "g", null),
        "l1", createNewKeyExtent("foo", null, "g"), "l2");

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
    ClientTabletCacheImpl metaCache =
        createLocators("foo", createNewKeyExtent("foo", "g", null), "l1",
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
    ClientTabletCacheImpl metaCache = createLocators("foo", createNewKeyExtent("foo", "0", null),
        "l1", createNewKeyExtent("foo", "1", "0"), "l2", createNewKeyExtent("foo", "2", "1"), "l3",
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
    ClientTabletCacheImpl metaCache = createLocators("foo", createNewKeyExtent("foo", "0", null),
        "l1", createNewKeyExtent("foo", "1", "0"), "l2", createNewKeyExtent("foo", "3", "2"), "l4",
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
  public void testBinRangesNonContiguousExtents() throws Exception {

    // This test exercises a bug that was seen in the tablet locator code.

    KeyExtent e1 = createNewKeyExtent("foo", "05", null);
    KeyExtent e2 = createNewKeyExtent("foo", "1", "05");
    KeyExtent e3 = createNewKeyExtent("foo", "2", "05");

    TServers tservers = new TServers();
    ClientTabletCacheImpl metaCache =
        createLocators(tservers, "tserver1", "tserver2", "foo", e1, "l1", e2, "l1");

    List<Range> ranges = createNewRangeList(createNewRange("01", "07"));
    Map<String,
        Map<KeyExtent,List<Range>>> expected = createExpectedBinnings(
            createRangeLocation("l1", e1, createNewRangeList(createNewRange("01", "07"))),
            createRangeLocation("l1", e2, createNewRangeList(createNewRange("01", "07"))));

    // The following will result in extents e1 and e2 being placed in the cache.
    runTest(ranges, metaCache, expected, createNewRangeList());

    // Add e3 to the metadata table. Extent e3 could not be added earlier in the test because it
    // overlaps e2. If e2 and e3 are seen in the same metadata read then one will be removed from
    // the cache because the cache can never contain overlapping extents.
    setLocation(tservers, "tserver2", METADATA_TABLE_EXTENT, e3, "l1");

    // The following test reproduces a bug. Extents e1 and e2 are in the cache. Extent e3 overlaps
    // e2 but is not in the cache. The range used by the test overlaps e1,e2,and e3. The bug was
    // that for this situation the binRanges code in tablet locator used to return e1,e2,and e3. The
    // desired behavior is that the range fails for this situation. This tablet locator bug caused
    // the batch scanner to return duplicate data.
    ranges = createNewRangeList(createNewRange("01", "17"));
    runTest(ranges, metaCache, new HashMap<>(), createNewRangeList(createNewRange("01", "17")));

    // After the above test fails it should cause e3 to be added to the cache. Because e3 overlaps
    // e2, when e3 is added then e2 is removed. Therefore, the following binRanges call should
    // succeed and find the range overlaps e1 and e3.
    expected = createExpectedBinnings(
        createRangeLocation("l1", e1, createNewRangeList(createNewRange("01", "17"))),
        createRangeLocation("l1", e3, createNewRangeList(createNewRange("01", "17"))));
    runTest(ranges, metaCache, expected, createNewRangeList());
  }

  @Test
  public void testBinRangesNonContiguousExtentsAndMultipleRanges() throws Exception {
    KeyExtent e1 = createNewKeyExtent("foo", "c", null);
    KeyExtent e2 = createNewKeyExtent("foo", "g", "c");
    KeyExtent e3 = createNewKeyExtent("foo", "k", "c");
    KeyExtent e4 = createNewKeyExtent("foo", "n", "k");
    KeyExtent e5 = createNewKeyExtent("foo", "q", "n");
    KeyExtent e6 = createNewKeyExtent("foo", "s", "n");
    KeyExtent e7 = createNewKeyExtent("foo", null, "s");

    TServers tservers = new TServers();
    ClientTabletCacheImpl metaCache = createLocators(tservers, "tserver1", "tserver2", "foo", e1,
        "l1", e2, "l1", e4, "l1", e5, "l1", e7, "l1");

    Range r1 = createNewRange("art", "cooking"); // overlaps e1 e2
    Range r2 = createNewRange("loop", "nope"); // overlaps e4 e5
    Range r3 = createNewRange("silly", "sunny"); // overlaps e7

    Map<String,Map<KeyExtent,List<Range>>> expected = createExpectedBinnings(

        createRangeLocation("l1", e1, createNewRangeList(r1)),
        createRangeLocation("l1", e2, createNewRangeList(r1)),
        createRangeLocation("l1", e4, createNewRangeList(r2)),
        createRangeLocation("l1", e5, createNewRangeList(r2)),
        createRangeLocation("l1", e7, createNewRangeList(r3)));
    runTest(createNewRangeList(r1, r2, r3), metaCache, expected, createNewRangeList());

    setLocation(tservers, "tserver2", METADATA_TABLE_EXTENT, e3, "l1");

    Range r4 = createNewRange("art", "good"); // overlaps e1 e3
    Range r5 = createNewRange("gum", "run"); // overlaps e3 e4 e6

    expected = createExpectedBinnings(createRangeLocation("l1", e7, createNewRangeList(r3)));
    runTest(createNewRangeList(r4, r5, r3), metaCache, expected, createNewRangeList(r4, r5));

    setLocation(tservers, "tserver2", METADATA_TABLE_EXTENT, e6, "l1");

    expected = createExpectedBinnings(createRangeLocation("l1", e1, createNewRangeList(r4)),
        createRangeLocation("l1", e3, createNewRangeList(r4)),
        createRangeLocation("l1", e7, createNewRangeList(r3)));
    runTest(createNewRangeList(r4, r5, r3), metaCache, expected, createNewRangeList(r5));

    expected = createExpectedBinnings(createRangeLocation("l1", e1, createNewRangeList(r4)),
        createRangeLocation("l1", e3, createNewRangeList(r4, r5)),
        createRangeLocation("l1", e4, createNewRangeList(r5)),
        createRangeLocation("l1", e6, createNewRangeList(r5)),
        createRangeLocation("l1", e7, createNewRangeList(r3)));
    runTest(createNewRangeList(r4, r5, r3), metaCache, expected, createNewRangeList());
  }

  @Test
  public void testIsContiguous() {
    CachedTablet e1 = new CachedTablet(createNewKeyExtent("foo", "1", null), "l1", "1",
        TabletAvailability.ONDEMAND, false);
    CachedTablet e2 = new CachedTablet(createNewKeyExtent("foo", "2", "1"), "l1", "1",
        TabletAvailability.ONDEMAND, false);
    CachedTablet e3 = new CachedTablet(createNewKeyExtent("foo", "3", "2"), "l1", "1",
        TabletAvailability.ONDEMAND, false);
    CachedTablet e4 = new CachedTablet(createNewKeyExtent("foo", null, "3"), "l1", "1",
        TabletAvailability.ONDEMAND, false);

    assertTrue(ClientTabletCacheImpl.isContiguous(List.of(e1, e2, e3, e4)));
    assertTrue(ClientTabletCacheImpl.isContiguous(List.of(e1, e2, e3)));
    assertTrue(ClientTabletCacheImpl.isContiguous(List.of(e2, e3, e4)));
    assertTrue(ClientTabletCacheImpl.isContiguous(List.of(e2, e3)));
    assertTrue(ClientTabletCacheImpl.isContiguous(List.of(e1)));
    assertTrue(ClientTabletCacheImpl.isContiguous(List.of(e2)));
    assertTrue(ClientTabletCacheImpl.isContiguous(List.of(e4)));

    assertFalse(ClientTabletCacheImpl.isContiguous(List.of(e1, e2, e4)));
    assertFalse(ClientTabletCacheImpl.isContiguous(List.of(e1, e3, e4)));

    CachedTablet e5 = new CachedTablet(createNewKeyExtent("foo", null, null), "l1", "1",
        TabletAvailability.ONDEMAND, false);
    assertFalse(ClientTabletCacheImpl.isContiguous(List.of(e1, e2, e3, e4, e5)));
    assertFalse(ClientTabletCacheImpl.isContiguous(List.of(e5, e1, e2, e3, e4)));
    assertFalse(ClientTabletCacheImpl.isContiguous(List.of(e1, e2, e3, e5)));
    assertFalse(ClientTabletCacheImpl.isContiguous(List.of(e5, e2, e3, e4)));
    assertTrue(ClientTabletCacheImpl.isContiguous(List.of(e5)));

    CachedTablet e6 = new CachedTablet(createNewKeyExtent("foo", null, "1"), "l1", "1",
        TabletAvailability.ONDEMAND, false);

    assertFalse(ClientTabletCacheImpl.isContiguous(List.of(e1, e2, e3, e6)));

    CachedTablet e7 = new CachedTablet(createNewKeyExtent("foo", "33", "11"), "l1", "1",
        TabletAvailability.ONDEMAND, false);

    assertFalse(ClientTabletCacheImpl.isContiguous(List.of(e1, e2, e7, e4)));
  }

  @Test
  public void testBinMutations1() throws Exception {
    // one tablet table
    KeyExtent ke1 = createNewKeyExtent("foo", null, null);
    ClientTabletCacheImpl metaCache = createLocators("foo", ke1, "l1");

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
    ClientTabletCacheImpl metaCache = createLocators("foo");

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

    ClientTabletCacheImpl metaCache = createLocators("foo", ke1, "l1", ke2, "l2", ke3, "l3");

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

    ClientTabletCacheImpl metaCache = createLocators("foo", ke1, "l1", ke3, "l3");

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
      ClientTabletCacheImpl metaCache =
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
    KeyExtent mte1 = new KeyExtent(AccumuloTable.METADATA.tableId(), new Text("0;0bc"),
        ROOT_TABLE_EXTENT.endRow());
    KeyExtent mte2 = new KeyExtent(AccumuloTable.METADATA.tableId(), null, new Text("0;0bc"));

    TServers tservers = new TServers();
    TestCachedTabletObtainer ttlo = new TestCachedTabletObtainer(tservers);

    RootClientTabletCache rtl = new TestRootClientTabletCache();
    ClientTabletCacheImpl rootTabletCache = new ClientTabletCacheImpl(
        AccumuloTable.METADATA.tableId(), rtl, ttlo, new YesLockChecker());
    ClientTabletCacheImpl tab0TabletCache =
        new ClientTabletCacheImpl(TableId.of("0"), rootTabletCache, ttlo, new YesLockChecker());

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
    KeyExtent mte1 =
        new KeyExtent(AccumuloTable.METADATA.tableId(), new Text("~"), ROOT_TABLE_EXTENT.endRow());
    KeyExtent mte2 = new KeyExtent(AccumuloTable.METADATA.tableId(), null, new Text("~"));

    TServers tservers = new TServers();
    TestCachedTabletObtainer ttlo = new TestCachedTabletObtainer(tservers);

    RootClientTabletCache rtl = new TestRootClientTabletCache();
    ClientTabletCacheImpl rootTabletCache = new ClientTabletCacheImpl(
        AccumuloTable.METADATA.tableId(), rtl, ttlo, new YesLockChecker());
    ClientTabletCacheImpl tab0TabletCache =
        new ClientTabletCacheImpl(TableId.of("0"), rootTabletCache, ttlo, new YesLockChecker());

    setLocation(tservers, "tserver1", ROOT_TABLE_EXTENT, mte1, "tserver2");
    setLocation(tservers, "tserver1", ROOT_TABLE_EXTENT, mte2, "tserver3");

    // create the ~ tablet so it exists
    Map<KeyExtent,SortedMap<Key,Value>> ts3 = new HashMap<>();
    ts3.put(mte2, new TreeMap<>());
    tservers.tservers.put("tserver3", ts3);

    assertNull(tab0TabletCache.findTablet(context, new Text("row_0000000000"), false,
        LocationNeed.REQUIRED));

  }

  // this test reproduces a problem where empty metadata tablets, that were created by user tablets
  // being merged away, caused locating tablets to fail
  @Test
  public void testBug3() throws Exception {
    KeyExtent mte1 = new KeyExtent(AccumuloTable.METADATA.tableId(), new Text("1;c"),
        ROOT_TABLE_EXTENT.endRow());
    KeyExtent mte2 =
        new KeyExtent(AccumuloTable.METADATA.tableId(), new Text("1;f"), new Text("1;c"));
    KeyExtent mte3 =
        new KeyExtent(AccumuloTable.METADATA.tableId(), new Text("1;j"), new Text("1;f"));
    KeyExtent mte4 =
        new KeyExtent(AccumuloTable.METADATA.tableId(), new Text("1;r"), new Text("1;j"));
    KeyExtent mte5 = new KeyExtent(AccumuloTable.METADATA.tableId(), null, new Text("1;r"));

    KeyExtent ke1 = new KeyExtent(TableId.of("1"), null, null);

    TServers tservers = new TServers();
    TestCachedTabletObtainer ttlo = new TestCachedTabletObtainer(tservers);

    RootClientTabletCache rtl = new TestRootClientTabletCache();

    ClientTabletCacheImpl rootTabletCache = new ClientTabletCacheImpl(
        AccumuloTable.METADATA.tableId(), rtl, ttlo, new YesLockChecker());
    ClientTabletCacheImpl tab0TabletCache =
        new ClientTabletCacheImpl(TableId.of("1"), rootTabletCache, ttlo, new YesLockChecker());

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
    ClientTabletCacheImpl metaCache = createLocators(tservers, "tserver1", "tserver2", "foo");

    KeyExtent ke1 = createNewKeyExtent("foo", null, null);

    // set two locations for a tablet, this is not supposed to happen. The metadata cache should
    // throw an exception if it sees this rather than caching one of
    // the locations.
    setLocation(tservers, "tserver2", METADATA_TABLE_EXTENT, ke1, "L1", "I1");
    setLocation(tservers, "tserver2", METADATA_TABLE_EXTENT, ke1, "L2", "I2");

    var e = assertThrows(IllegalStateException.class,
        () -> metaCache.findTablet(context, new Text("a"), false, LocationNeed.REQUIRED));
    assertTrue(e.getMessage().startsWith("Tablet has multiple locations : "));

  }

  @Test
  public void testLostLock() throws Exception {

    final HashSet<String> activeLocks = new HashSet<>();

    TServers tservers = new TServers();
    ClientTabletCacheImpl metaCache =
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

  @Test
  public void testCachingUnhosted() throws Exception {

    // this test caching tablets without a location

    TServers tservers = new TServers();
    ClientTabletCacheImpl metaCache = createLocators(tservers, "tserver1", "tserver2", "foo");

    var ke1 = createNewKeyExtent("foo", "g", null);
    var ke2 = createNewKeyExtent("foo", "m", "g");
    var ke3 = createNewKeyExtent("foo", "r", "m");
    var ke4 = createNewKeyExtent("foo", null, "r");

    setLocation(tservers, "tserver2", METADATA_TABLE_EXTENT, ke1, null, null);
    setLocation(tservers, "tserver2", METADATA_TABLE_EXTENT, ke2, null, null);
    setLocation(tservers, "tserver2", METADATA_TABLE_EXTENT, ke3, "L2", "I2");
    setLocation(tservers, "tserver2", METADATA_TABLE_EXTENT, ke4, "L2", "I2");

    locateTabletTest(metaCache, "a", false, ke1, null, LocationNeed.NOT_REQUIRED);
    locateTabletTest(metaCache, "a", false, null, null, LocationNeed.REQUIRED);

    locateTabletTest(metaCache, "n", false, ke3, "L2", LocationNeed.NOT_REQUIRED);
    locateTabletTest(metaCache, "n", false, ke3, "L2", LocationNeed.REQUIRED);

    var r1 = new Range(null, "a");
    var r2 = new Range("d", "o");

    List<Range> ranges = List.of(r1, r2);
    Set<Pair<CachedTablet,Range>> actual = new HashSet<>();
    var failures = metaCache.findTablets(context, ranges, (tl, r) -> actual.add(new Pair<>(tl, r)),
        LocationNeed.NOT_REQUIRED);
    assertEquals(List.of(), failures);
    var tl1 = new CachedTablet(ke1, TabletAvailability.ONDEMAND, false);
    var tl2 = new CachedTablet(ke2, TabletAvailability.ONDEMAND, false);
    var tl3 = new CachedTablet(ke3, "L2", "I2", TabletAvailability.ONDEMAND, false);
    var expected =
        Set.of(new Pair<>(tl1, r1), new Pair<>(tl1, r2), new Pair<>(tl2, r2), new Pair<>(tl3, r2));
    assertEquals(expected, actual);

    actual.clear();
    failures = metaCache.findTablets(context, ranges, (tl, r) -> actual.add(new Pair<>(tl, r)),
        LocationNeed.REQUIRED);
    assertEquals(new HashSet<>(ranges), new HashSet<>(failures));
    assertEquals(Set.of(), actual);

    setLocation(tservers, "tserver2", METADATA_TABLE_EXTENT, ke1, "L3", "I3");
    // the cache contains ke1 w/o a location, even though the location is now set we should get the
    // cached version w/o the location
    locateTabletTest(metaCache, "a", false, ke1, null, LocationNeed.NOT_REQUIRED);
    // the cache contains an extent w/o a location this should force it to clear
    locateTabletTest(metaCache, "a", false, ke1, "L3", LocationNeed.REQUIRED);
    // now that the location is cached, should see it
    locateTabletTest(metaCache, "a", false, ke1, "L3", LocationNeed.NOT_REQUIRED);

    setLocation(tservers, "tserver2", METADATA_TABLE_EXTENT, ke2, "L4", "I4");
    // even though the location is set for ke2 the cache should have ke2 w/o a location and that
    // should be seeen
    actual.clear();
    failures = metaCache.findTablets(context, ranges, (tl, r) -> actual.add(new Pair<>(tl, r)),
        LocationNeed.NOT_REQUIRED);
    assertEquals(List.of(), failures);
    tl1 = new CachedTablet(ke1, "L3", "I3", TabletAvailability.ONDEMAND, false);
    expected =
        Set.of(new Pair<>(tl1, r1), new Pair<>(tl1, r2), new Pair<>(tl2, r2), new Pair<>(tl3, r2));
    assertEquals(expected, actual);
    // this should cause the location for ke2 to be pulled into the cache
    actual.clear();
    failures = metaCache.findTablets(context, ranges, (tl, r) -> actual.add(new Pair<>(tl, r)),
        LocationNeed.REQUIRED);
    assertEquals(List.of(), failures);
    tl2 = new CachedTablet(ke2, "L4", "I4", TabletAvailability.ONDEMAND, false);
    expected =
        Set.of(new Pair<>(tl1, r1), new Pair<>(tl1, r2), new Pair<>(tl2, r2), new Pair<>(tl3, r2));
    assertEquals(expected, actual);
    // should still see locations in cache
    actual.clear();
    failures = metaCache.findTablets(context, ranges, (tl, r) -> actual.add(new Pair<>(tl, r)),
        LocationNeed.NOT_REQUIRED);
    assertEquals(List.of(), failures);
    assertEquals(expected, actual);

    // ensure bin mutations works when the cache contains an entry w/o a location
    deleteLocation(tservers, "tserver2", METADATA_TABLE_EXTENT, ke2, "I4");
    metaCache.invalidateCache(ke2);
    locateTabletTest(metaCache, "i", false, ke2, null, LocationNeed.NOT_REQUIRED);

    // one mutation should fail because there is no location for ke2
    List<Mutation> ml = createNewMutationList(createNewMutation("a", "cf1:cq1=v1", "cf1:cq2=v2"),
        createNewMutation("i", "cf1:cq1=v3", "cf1:cq2=v4"));
    Map<String,Map<KeyExtent,List<String>>> emb =
        createServerExtentMap(createServerExtent("a", "L3", ke1));
    runTest(metaCache, ml, emb, "i");

    // set location for ke2 and both mutations should bin.. the cached entry for ke2 w/o a location
    // should be jettisoned
    setLocation(tservers, "tserver2", METADATA_TABLE_EXTENT, ke2, "L4", "I4");
    emb = createServerExtentMap(createServerExtent("a", "L3", ke1),
        createServerExtent("i", "L4", ke2));
    runTest(metaCache, ml, emb);

  }

}

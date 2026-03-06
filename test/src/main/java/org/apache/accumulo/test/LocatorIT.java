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
package org.apache.accumulo.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeSet;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.TableOfflineException;
import org.apache.accumulo.core.client.admin.Locations;
import org.apache.accumulo.core.client.admin.NewTableConfiguration;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.clientImpl.ClientContext;
import org.apache.accumulo.core.clientImpl.TabletLocator;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.data.TabletId;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.dataImpl.TabletIdImpl;
import org.apache.accumulo.core.manager.state.tables.TableState;
import org.apache.accumulo.harness.AccumuloClusterHarness;
import org.apache.accumulo.test.util.Wait;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.Test;

public class LocatorIT extends AccumuloClusterHarness {

  @Override
  protected Duration defaultTimeout() {
    return Duration.ofMinutes(2);
  }

  private void assertContains(Locations locations, HashSet<String> tservers,
      Map<Range,Set<TabletId>> expected1, Map<TabletId,Set<Range>> expected2) {

    Map<Range,Set<TabletId>> gbr = new HashMap<>();
    for (Entry<Range,List<TabletId>> entry : locations.groupByRange().entrySet()) {
      gbr.put(entry.getKey(), new HashSet<>(entry.getValue()));
    }

    assertEquals(expected1, gbr);

    Map<TabletId,Set<Range>> gbt = new HashMap<>();
    for (Entry<TabletId,List<Range>> entry : locations.groupByTablet().entrySet()) {
      gbt.put(entry.getKey(), new HashSet<>(entry.getValue()));

      TabletId tid = entry.getKey();
      String location = locations.getTabletLocation(tid);
      assertNotNull(location, "Location for " + tid + " was null");
      assertTrue(tservers.contains(location), "Unknown location " + location);
      assertEquals(2, location.split(":").length, "Expected <host>:<port> " + location);

    }

    assertEquals(expected2, gbt);
  }

  private static TabletId newTabletId(String tableId, String endRow, String prevRow) {
    return new TabletIdImpl(new KeyExtent(TableId.of(tableId),
        endRow == null ? null : new Text(endRow), prevRow == null ? null : new Text(prevRow)));
  }

  @Test
  public void testBasic() throws Exception {
    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
      String tableName = getUniqueNames(1)[0];

      TableOperations tableOps = client.tableOperations();
      tableOps.create(tableName);

      Range r1 = new Range("m");
      Range r2 = new Range("o", "x");

      String tableId = tableOps.tableIdMap().get(tableName);

      TabletId t1 = newTabletId(tableId, null, null);
      TabletId t2 = newTabletId(tableId, "r", null);
      TabletId t3 = newTabletId(tableId, null, "r");

      ArrayList<Range> ranges = new ArrayList<>();

      HashSet<String> tservers = new HashSet<>(client.instanceOperations().getTabletServers());

      ranges.add(r1);
      Locations ret = tableOps.locate(tableName, ranges);
      assertContains(ret, tservers, Map.of(r1, Set.of(t1)), Map.of(t1, Set.of(r1)));

      ranges.add(r2);
      ret = tableOps.locate(tableName, ranges);
      assertContains(ret, tservers, Map.of(r1, Set.of(t1), r2, Set.of(t1)),
          Map.of(t1, Set.of(r1, r2)));

      TreeSet<Text> splits = new TreeSet<>();
      splits.add(new Text("r"));
      tableOps.addSplits(tableName, splits);

      ret = tableOps.locate(tableName, ranges);
      assertContains(ret, tservers, Map.of(r1, Set.of(t2), r2, Set.of(t2, t3)),
          Map.of(t2, Set.of(r1, r2), t3, Set.of(r2)));

      tableOps.offline(tableName, true);

      assertThrows(TableOfflineException.class, () -> tableOps.locate(tableName, ranges));

      tableOps.delete(tableName);

      assertThrows(TableNotFoundException.class, () -> tableOps.locate(tableName, ranges));
    }
  }

  @Test
  public void testClearingUnused() throws Exception {
    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
      String[] tables = getUniqueNames(4);
      String table1 = tables[0];
      String table2 = tables[1];
      String table3 = tables[2];
      String table4 = tables[3];

      TableOperations tableOps = client.tableOperations();
      tableOps.create(table1);
      tableOps.create(table2);
      tableOps.create(table3, new NewTableConfiguration().createOffline());
      tableOps.create(table4, new NewTableConfiguration().createOffline());

      TabletLocator.setClearFrequency(Duration.ofMillis(100));

      ClientContext ctx = (ClientContext) client;
      TableId tableId1 = ctx.getTableId(table1);
      TableId tableId2 = ctx.getTableId(table2);
      TableId tableId3 = ctx.getTableId(table3);
      TableId tableId4 = ctx.getTableId(table4);

      for (var tableId : List.of(tableId1, tableId2, tableId3, tableId4)) {
        assertFalse(TabletLocator.isPresent(ctx, tableId));
        assertNotNull(TabletLocator.getLocator(ctx, tableId));
        assertTrue(TabletLocator.isPresent(ctx, tableId));
      }

      // Put table2 and table3 into a different state than what is in the cache
      assertEquals(TableState.ONLINE, ctx.getTableState(tableId2));
      assertEquals(TableState.OFFLINE, ctx.getTableState(tableId3));
      tableOps.offline(table2, true);
      tableOps.online(table3, true);
      assertEquals(TableState.OFFLINE, ctx.getTableState(tableId2));
      assertEquals(TableState.ONLINE, ctx.getTableState(tableId3));

      Wait.waitFor(() -> {
        // Accessing table1 in the cache should cause table2 and table3 to eventually be cleared
        // because their table state does not match what was cached
        assertNotNull(TabletLocator.getLocator(ctx, tableId1));
        return !TabletLocator.isPresent(ctx, tableId2) && !TabletLocator.isPresent(ctx, tableId3);
      });

      assertTrue(TabletLocator.isPresent(ctx, tableId1));
      assertTrue(TabletLocator.isPresent(ctx, tableId4));

      // bring table2 and table3 back into the cache
      for (var tableId : List.of(tableId2, tableId3)) {
        assertFalse(TabletLocator.isPresent(ctx, tableId));
        assertNotNull(TabletLocator.getLocator(ctx, tableId));
        assertTrue(TabletLocator.isPresent(ctx, tableId));
      }

      tableOps.delete(table2);
      tableOps.delete(table3);

      Wait.waitFor(() -> {
        // Accessing table4 in the cache should cause table2 and table3 to eventually be cleared
        // because they no longer exist. This also test that online and offline tables a properly
        // cleared from the cache.
        assertNotNull(TabletLocator.getLocator(ctx, tableId4));
        return !TabletLocator.isPresent(ctx, tableId2) && !TabletLocator.isPresent(ctx, tableId3);
      });

      // table1 and table4 should be left in the cache, check that online or offline tables are not
      // removed unnecessarily.
      assertTrue(TabletLocator.isPresent(ctx, tableId1));
      assertTrue(TabletLocator.isPresent(ctx, tableId4));
      assertEquals(TableState.ONLINE, ctx.getTableState(tableId1));
      assertEquals(TableState.OFFLINE, ctx.getTableState(tableId4));
    }
  }
}

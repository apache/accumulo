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

package org.apache.accumulo.test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

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
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.data.TabletId;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.dataImpl.TabletIdImpl;
import org.apache.accumulo.harness.AccumuloClusterHarness;
import org.apache.hadoop.io.Text;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

public class LocatorIT extends AccumuloClusterHarness {

  @Override
  protected int defaultTimeoutSeconds() {
    return 60;
  }

  private void assertContains(Locations locations, HashSet<String> tservers,
      Map<Range,ImmutableSet<TabletId>> expected1, Map<TabletId,ImmutableSet<Range>> expected2) {

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
      assertNotNull("Location for " + tid + " was null", location);
      assertTrue("Unknown location " + location, tservers.contains(location));
      assertEquals("Expected <host>:<port> " + location, 2, location.split(":").length);

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

      client.tableOperations().create(tableName);

      Range r1 = new Range("m");
      Range r2 = new Range("o", "x");

      String tableId = client.tableOperations().tableIdMap().get(tableName);

      TabletId t1 = newTabletId(tableId, null, null);
      TabletId t2 = newTabletId(tableId, "r", null);
      TabletId t3 = newTabletId(tableId, null, "r");

      ArrayList<Range> ranges = new ArrayList<>();

      HashSet<String> tservers = new HashSet<>(client.instanceOperations().getTabletServers());

      ranges.add(r1);
      Locations ret = client.tableOperations().locate(tableName, ranges);
      assertContains(ret, tservers, ImmutableMap.of(r1, ImmutableSet.of(t1)),
          ImmutableMap.of(t1, ImmutableSet.of(r1)));

      ranges.add(r2);
      ret = client.tableOperations().locate(tableName, ranges);
      assertContains(ret, tservers,
          ImmutableMap.of(r1, ImmutableSet.of(t1), r2, ImmutableSet.of(t1)),
          ImmutableMap.of(t1, ImmutableSet.of(r1, r2)));

      TreeSet<Text> splits = new TreeSet<>();
      splits.add(new Text("r"));
      client.tableOperations().addSplits(tableName, splits);

      ret = client.tableOperations().locate(tableName, ranges);
      assertContains(ret, tservers,
          ImmutableMap.of(r1, ImmutableSet.of(t2), r2, ImmutableSet.of(t2, t3)),
          ImmutableMap.of(t2, ImmutableSet.of(r1, r2), t3, ImmutableSet.of(r2)));

      client.tableOperations().offline(tableName, true);

      try {
        client.tableOperations().locate(tableName, ranges);
        fail();
      } catch (TableOfflineException e) {
        // expected
      }

      client.tableOperations().delete(tableName);

      try {
        client.tableOperations().locate(tableName, ranges);
        fail();
      } catch (TableNotFoundException e) {
        // expected
      }
    }
  }
}

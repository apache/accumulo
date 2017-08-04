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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeSet;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.TableOfflineException;
import org.apache.accumulo.core.client.admin.Locations;
import org.apache.accumulo.core.client.impl.Table;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.TabletId;
import org.apache.accumulo.core.data.impl.KeyExtent;
import org.apache.accumulo.core.data.impl.TabletIdImpl;
import org.apache.accumulo.harness.AccumuloClusterHarness;
import org.apache.hadoop.io.Text;
import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

public class LocatorIT extends AccumuloClusterHarness {

  private void assertContains(Locations locations, HashSet<String> tservers, Map<Range,ImmutableSet<TabletId>> expected1,
      Map<TabletId,ImmutableSet<Range>> expected2) {

    Map<Range,Set<TabletId>> gbr = new HashMap<>();
    for (Entry<Range,List<TabletId>> entry : locations.groupByRange().entrySet()) {
      gbr.put(entry.getKey(), new HashSet<>(entry.getValue()));
    }

    Assert.assertEquals(expected1, gbr);

    Map<TabletId,Set<Range>> gbt = new HashMap<>();
    for (Entry<TabletId,List<Range>> entry : locations.groupByTablet().entrySet()) {
      gbt.put(entry.getKey(), new HashSet<>(entry.getValue()));

      TabletId tid = entry.getKey();
      String location = locations.getTabletLocation(tid);
      Assert.assertNotNull("Location for " + tid + " was null", location);
      Assert.assertTrue("Unknown location " + location, tservers.contains(location));
      Assert.assertTrue("Expected <host>:<port> " + location, location.split(":").length == 2);

    }

    Assert.assertEquals(expected2, gbt);
  }

  private static TabletId newTabletId(String tableId, String endRow, String prevRow) {
    return new TabletIdImpl(new KeyExtent(Table.ID.of(tableId), endRow == null ? null : new Text(endRow), prevRow == null ? null : new Text(prevRow)));
  }

  @Test
  public void testBasic() throws Exception {
    Connector conn = getConnector();
    String tableName = getUniqueNames(1)[0];

    conn.tableOperations().create(tableName);

    Range r1 = new Range("m");
    Range r2 = new Range("o", "x");

    String tableId = conn.tableOperations().tableIdMap().get(tableName);

    TabletId t1 = newTabletId(tableId, null, null);
    TabletId t2 = newTabletId(tableId, "r", null);
    TabletId t3 = newTabletId(tableId, null, "r");

    ArrayList<Range> ranges = new ArrayList<>();

    HashSet<String> tservers = new HashSet<>(conn.instanceOperations().getTabletServers());

    ranges.add(r1);
    Locations ret = conn.tableOperations().locate(tableName, ranges);
    assertContains(ret, tservers, ImmutableMap.of(r1, ImmutableSet.of(t1)), ImmutableMap.of(t1, ImmutableSet.of(r1)));

    ranges.add(r2);
    ret = conn.tableOperations().locate(tableName, ranges);
    assertContains(ret, tservers, ImmutableMap.of(r1, ImmutableSet.of(t1), r2, ImmutableSet.of(t1)), ImmutableMap.of(t1, ImmutableSet.of(r1, r2)));

    TreeSet<Text> splits = new TreeSet<>();
    splits.add(new Text("r"));
    conn.tableOperations().addSplits(tableName, splits);

    ret = conn.tableOperations().locate(tableName, ranges);
    assertContains(ret, tservers, ImmutableMap.of(r1, ImmutableSet.of(t2), r2, ImmutableSet.of(t2, t3)),
        ImmutableMap.of(t2, ImmutableSet.of(r1, r2), t3, ImmutableSet.of(r2)));

    conn.tableOperations().offline(tableName, true);

    try {
      conn.tableOperations().locate(tableName, ranges);
      Assert.fail();
    } catch (TableOfflineException e) {
      // expected
    }

    conn.tableOperations().delete(tableName);

    try {
      conn.tableOperations().locate(tableName, ranges);
      Assert.fail();
    } catch (TableNotFoundException e) {
      // expected
    }
  }
}

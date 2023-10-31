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
import java.util.function.Predicate;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.TableOfflineException;
import org.apache.accumulo.core.client.admin.Locations;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.client.admin.TabletHostingGoal;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.data.TabletId;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.dataImpl.TabletIdImpl;
import org.apache.accumulo.core.metadata.schema.TabletMetadata;
import org.apache.accumulo.harness.AccumuloClusterHarness;
import org.apache.accumulo.test.functional.ManagerAssignmentIT;
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

    final Predicate<TabletMetadata> alwaysHostedAndCurrentNotNull =
        t -> t.getHostingGoal() == TabletHostingGoal.ALWAYS && t.hasCurrent()
            && t.getLocation().getHostAndPort() != null;

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

      // locate won't find any locations, tablets are not hosted
      ranges.add(r1);
      assertThrows(AccumuloException.class, () -> tableOps.locate(tableName, ranges));

      ranges.add(r2);
      assertThrows(AccumuloException.class, () -> tableOps.locate(tableName, ranges));

      ranges.clear();

      tableOps.setTabletHostingGoal(tableName, new Range(), TabletHostingGoal.ALWAYS);
      Wait.waitFor(() -> alwaysHostedAndCurrentNotNull
          .test(ManagerAssignmentIT.getTabletMetadata(client, tableId, null)), 60000, 250);

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

      Wait.waitFor(() -> alwaysHostedAndCurrentNotNull
          .test(ManagerAssignmentIT.getTabletMetadata(client, tableId, null)), 60000, 250);

      ret = tableOps.locate(tableName, ranges);
      assertContains(ret, tservers, Map.of(r1, Set.of(t2), r2, Set.of(t2, t3)),
          Map.of(t2, Set.of(r1, r2), t3, Set.of(r2)));

      tableOps.offline(tableName, true);

      assertThrows(TableOfflineException.class, () -> tableOps.locate(tableName, ranges));

      tableOps.online(tableName, true);

      Wait.waitFor(
          () -> alwaysHostedAndCurrentNotNull
              .test(ManagerAssignmentIT.getTabletMetadata(client, tableId, new Text("r"))),
          60000, 250);

      ArrayList<Range> ranges2 = new ArrayList<>();
      ranges2.add(r1);
      ret = tableOps.locate(tableName, ranges2);
      assertContains(ret, tservers, Map.of(r1, Set.of(t2)), Map.of(t2, Set.of(r1)));

      tableOps.delete(tableName);

      assertThrows(TableNotFoundException.class, () -> tableOps.locate(tableName, ranges));
    }
  }
}

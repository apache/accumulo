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
package org.apache.accumulo.test.functional;

import static org.apache.accumulo.core.client.admin.TabletAvailability.HOSTED;
import static org.apache.accumulo.core.client.admin.TabletAvailability.UNHOSTED;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.InvalidTabletHostingRequestException;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.ScannerBase;
import org.apache.accumulo.core.client.admin.TabletAvailability;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.harness.AccumuloClusterHarness;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.Test;

public class TabletAvailabilityIT extends AccumuloClusterHarness {

  @Test
  public void testBoundries() throws Exception {
    // Tests operating on tablets with different tablet availabilites that are next to each other.

    String table = getUniqueNames(1)[0];
    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {

      client.tableOperations().create(table);

      SortedSet<Text> splits = new TreeSet<>();
      for (int i = 1; i <= 18; i++) {
        splits.add(rowt(i));
      }
      client.tableOperations().addSplits(table, splits);

      int r = 1;

      SortedMap<String,TabletAvailability> availabilites = new TreeMap<>();
      // create tablets that cover all possible pairs of TabletAvailability
      for (var a1 : TabletAvailability.values()) {
        for (var a2 : TabletAvailability.values()) {
          availabilites.put(row(r), a1);
          client.tableOperations().setTabletAvailability(table, new Range(row(r++)), a1);
          availabilites.put(row(r), a2);
          client.tableOperations().setTabletAvailability(table, new Range(row(r++)), a2);
        }
      }

      assertEquals(19, r);

      // ensure tablet availability is as expected
      SortedMap<String,TabletAvailability> availabilitesSeen = new TreeMap<>();
      client.tableOperations().getTabletInformation(table, new Range()).forEach(ti -> {
        if (ti.getTabletId().getEndRow() != null) {
          availabilitesSeen.put(ti.getTabletId().getEndRow().toString(),
              ti.getTabletAvailability());
        }
      });
      assertEquals(availabilites, availabilitesSeen);

      int v = 0;
      // try read/write to all the pairs of tablets
      for (int i = 1; i <= 18; i += 2) {
        var expectFail =
            availabilites.get(row(i)) == UNHOSTED || availabilites.get(row(i + 1)) == UNHOSTED;

        try (var writer = client.createBatchWriter(table)) {
          writer.addMutation(newMutation(i, v++));
          writer.addMutation(newMutation(i + 1, v++));
          writer.flush();
          assertFalse(expectFail);
        } catch (MutationsRejectedException e) {
          assertTrue(expectFail);
          assertEquals(InvalidTabletHostingRequestException.class, e.getCause().getClass());
        }

        try (var bscanner = client.createBatchScanner(table)) {
          bscanner.setRanges(List.of(new Range(row(i)), new Range(row(i + 1))));
          assertEquals(Map.of(i, v - 2, i + 1, v - 1), scan(bscanner));
          assertFalse(expectFail);
        } catch (IllegalStateException e) {
          assertTrue(expectFail);
          assertEquals(InvalidTabletHostingRequestException.class, e.getCause().getClass());
        }

        try (var scanner = client.createScanner(table)) {
          scanner.setRange(new Range(row(i), row(i + 1)));
          assertEquals(Map.of(i, v - 2, i + 1, v - 1), scan(scanner));
          assertFalse(expectFail);
        } catch (IllegalStateException e) {
          assertTrue(expectFail);
          assertEquals(InvalidTabletHostingRequestException.class,
              e.getCause().getCause().getClass());
        }
      }

      // verify nothing was actually written to the unhosted tablets
      client.tableOperations().setTabletAvailability(table, new Range(), HOSTED);
      for (var entry : availabilites.entrySet()) {
        if (entry.getValue() == UNHOSTED) {
          var row = entry.getKey();
          try (var scanner = client.createScanner(table)) {
            scanner.setRange(new Range(row));
            assertEquals(0, scanner.stream().count());
          }
        }
      }
    }
  }

  private static Map<Integer,Integer> scan(ScannerBase scanner) {
    Map<Integer,Integer> saw = new HashMap<>();
    for (var entry : scanner) {
      saw.put(Integer.parseInt(entry.getKey().getRow().toString()),
          Integer.parseInt(entry.getValue().toString()));
    }
    return saw;
  }

  private static String row(int i) {
    return String.format("%010d", i);
  }

  private static Text rowt(int i) {
    return new Text(row(i));
  }

  private static Mutation newMutation(int row, int val) {
    var m = new Mutation(row(row));
    m.put("f", "q", String.format("%010d", val));
    return m;
  }

}

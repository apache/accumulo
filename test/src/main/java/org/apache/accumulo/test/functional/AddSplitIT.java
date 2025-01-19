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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.TabletMergeability;
import org.apache.accumulo.core.clientImpl.TabletMergeabilityUtil;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.metadata.schema.TabletMergeabilityMetadata;
import org.apache.accumulo.core.metadata.schema.TabletsMetadata;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.harness.AccumuloClusterHarness;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.Test;

public class AddSplitIT extends AccumuloClusterHarness {

  @Override
  protected Duration defaultTimeout() {
    return Duration.ofMinutes(1);
  }

  @Test
  public void addSplitTest() throws Exception {

    String tableName = getUniqueNames(1)[0];
    try (AccumuloClient c = Accumulo.newClient().from(getClientProps()).build()) {
      c.tableOperations().create(tableName);

      insertData(c, tableName, 1L);

      TreeSet<Text> splits = new TreeSet<>();
      splits.add(new Text(String.format("%09d", 333)));
      splits.add(new Text(String.format("%09d", 666)));

      c.tableOperations().addSplits(tableName, splits);

      Thread.sleep(100);

      Collection<Text> actualSplits = c.tableOperations().listSplits(tableName);

      if (!splits.equals(new TreeSet<>(actualSplits))) {
        throw new Exception(splits + " != " + actualSplits);
      }

      verifyData(c, tableName, 1L);
      insertData(c, tableName, 2L);

      // did not clear splits on purpose, it should ignore existing split points
      // and still create the three additional split points

      splits.add(new Text(String.format("%09d", 200)));
      splits.add(new Text(String.format("%09d", 500)));
      splits.add(new Text(String.format("%09d", 800)));

      c.tableOperations().addSplits(tableName, splits);

      Thread.sleep(100);

      actualSplits = c.tableOperations().listSplits(tableName);

      if (!splits.equals(new TreeSet<>(actualSplits))) {
        throw new Exception(splits + " != " + actualSplits);
      }

      verifyData(c, tableName, 2L);

      TableId id = TableId.of(c.tableOperations().tableIdMap().get(tableName));
      try (TabletsMetadata tm = getServerContext().getAmple().readTablets().forTable(id).build()) {
        // Default for user created tablets should be mergeability set to NEVER
        tm.stream().forEach(tablet -> assertEquals(TabletMergeabilityMetadata.never(),
            tablet.getTabletMergeability()));
      }
    }
  }

  @Test
  public void addSplitWithMergeabilityTest() throws Exception {
    String tableName = getUniqueNames(1)[0];
    try (AccumuloClient c = Accumulo.newClient().from(getClientProps()).build()) {
      c.tableOperations().create(tableName);

      SortedMap<Text,TabletMergeability> splits = new TreeMap<>();
      splits.put(new Text(String.format("%09d", 333)), TabletMergeability.always());
      splits.put(new Text(String.format("%09d", 666)), TabletMergeability.never());
      splits.put(new Text(String.format("%09d", 888)),
          TabletMergeability.after(Duration.ofSeconds(100)));
      splits.put(new Text(String.format("%09d", 999)),
          TabletMergeability.after(Duration.ofDays(1)));

      c.tableOperations().putSplits(tableName, splits);
      Thread.sleep(100);
      assertEquals(splits.keySet(), new TreeSet<>(c.tableOperations().listSplits(tableName)));

      TableId id = TableId.of(c.tableOperations().tableIdMap().get(tableName));
      verifySplits(id, splits);
      verifySplitsWithApi(c, tableName, splits);

    }
  }

  @Test
  public void updateSplitWithMergeabilityTest() throws Exception {
    String tableName = getUniqueNames(1)[0];
    try (AccumuloClient c = Accumulo.newClient().from(getClientProps()).build()) {
      c.tableOperations().create(tableName);

      NavigableMap<Text,TabletMergeability> splits = new TreeMap<>();
      splits.put(new Text(String.format("%09d", 333)), TabletMergeability.never());
      splits.put(new Text(String.format("%09d", 666)), TabletMergeability.never());
      splits.put(new Text(String.format("%09d", 999)),
          TabletMergeability.after(Duration.ofDays(10)));

      c.tableOperations().putSplits(tableName, splits);
      Thread.sleep(100);
      assertEquals(splits.keySet(), new TreeSet<>(c.tableOperations().listSplits(tableName)));

      TableId id = TableId.of(c.tableOperations().tableIdMap().get(tableName));
      verifySplits(id, splits);

      // Set all splits to system default (always) and update
      var splitUpdates = TabletMergeabilityUtil.systemDefaultSplits(splits.navigableKeySet());
      c.tableOperations().putSplits(tableName, splitUpdates);
      verifySplits(id, splitUpdates);
      verifySplitsWithApi(c, tableName, splitUpdates);

      // Update two existing and add two new splits
      splits.put(new Text(String.format("%09d", 666)),
          TabletMergeability.after(Duration.ofHours(10)));
      splits.put(new Text(String.format("%09d", 999)),
          TabletMergeability.after(Duration.ofMinutes(7)));
      splits.put(new Text(String.format("%09d", 444)), TabletMergeability.always());
      splits.put(new Text(String.format("%09d", 777)),
          TabletMergeability.after(Duration.ofMinutes(5)));

      c.tableOperations().putSplits(tableName, splits);
      verifySplits(id, splits);
      verifySplitsWithApi(c, tableName, splits);

      // Update with same and make sure insertion time updated
      splits.clear();
      var split = new Text(String.format("%09d", 777));
      splits.put(split, TabletMergeability.after(Duration.ofMinutes(5)));

      var originalTmi = c.tableOperations().getTabletInformation(tableName, new Range(split))
          .findFirst().orElseThrow().getTabletMergeabilityInfo();
      // getCurrentTime() uses a supplier and delays until invoking so trigger here
      assertTrue(originalTmi.getCurrentTime().getNano() > 0);

      // Update existing with same delay of 5 minutes
      c.tableOperations().putSplits(tableName, splits);
      var updatedTmi = c.tableOperations().getTabletInformation(tableName, new Range(split))
          .findFirst().orElseThrow().getTabletMergeabilityInfo();

      // TabletMergeability setting should be the same but the insertion and current time
      // should be different
      assertEquals(originalTmi.getTabletMergeability(), updatedTmi.getTabletMergeability());
      assertTrue(updatedTmi.getInsertionTime().orElseThrow()
          .compareTo(originalTmi.getInsertionTime().orElseThrow()) > 0);
      // we previously called getCurrentTime() on the originalTmi object so the updated one
      // should be newer
      assertTrue(updatedTmi.getCurrentTime().compareTo(originalTmi.getCurrentTime()) > 0);
    }
  }

  @Test
  public void addSplitIsMergeableTest() throws Exception {
    String tableName = getUniqueNames(1)[0];
    try (AccumuloClient c = Accumulo.newClient().from(getClientProps()).build()) {
      c.tableOperations().create(tableName);

      var split1 = new Text(String.format("%09d", 333));
      var split2 = new Text(String.format("%09d", 666));
      var split3 = new Text(String.format("%09d", 888));
      var split4 = new Text(String.format("%09d", 999));

      SortedMap<Text,TabletMergeability> splits = new TreeMap<>();
      splits.put(split1, TabletMergeability.always());
      splits.put(split2, TabletMergeability.never());
      splits.put(split3, TabletMergeability.after(Duration.ofMillis(1)));
      splits.put(split4, TabletMergeability.after(Duration.ofDays(1)));

      c.tableOperations().putSplits(tableName, splits);
      Thread.sleep(100);
      assertEquals(splits.keySet(), new TreeSet<>(c.tableOperations().listSplits(tableName)));

      var tableInfo = c.tableOperations().getTabletInformation(tableName, new Range())
          .collect(Collectors.toMap(ti -> ti.getTabletId().getEndRow(), Function.identity()));

      // Set to always
      var split1Tmi = tableInfo.get(split1).getTabletMergeabilityInfo();
      assertTrue(split1Tmi.isMergeable());
      assertTrue(split1Tmi.getInsertionTime().isPresent());
      assertTrue(
          split1Tmi.getCurrentTime().compareTo(split1Tmi.getInsertionTime().orElseThrow()) > 0);

      // Set to never
      var split2Tmi = tableInfo.get(split2).getTabletMergeabilityInfo();
      assertFalse(split2Tmi.isMergeable());
      assertFalse(split2Tmi.getInsertionTime().isPresent());

      // Set to a delay of 1 ms and current time should have elapsed long enough
      var split3Tmi = tableInfo.get(split3).getTabletMergeabilityInfo();
      assertTrue(split3Tmi.isMergeable());
      assertTrue(split3Tmi.getInsertionTime().isPresent());
      assertTrue(
          split3Tmi.getCurrentTime().compareTo(split3Tmi.getInsertionTime().orElseThrow()) > 0);

      // Set to a delay of 1 day and current time has NOT elapsed long enough
      var split4Tmi = tableInfo.get(split4).getTabletMergeabilityInfo();
      assertFalse(split4Tmi.isMergeable());
      assertTrue(split4Tmi.getInsertionTime().isPresent());
      assertTrue(
          split4Tmi.getCurrentTime().compareTo(split4Tmi.getInsertionTime().orElseThrow()) > 0);

    }
  }

  // Checks that TabletMergeability in metadata matches split settings in the map
  private void verifySplits(TableId id, SortedMap<Text,TabletMergeability> splits) {
    try (TabletsMetadata tm = getServerContext().getAmple().readTablets().forTable(id).build()) {
      tm.stream().forEach(t -> {
        // default tablet should be set to never
        if (t.getEndRow() == null) {
          assertEquals(TabletMergeability.never(),
              t.getTabletMergeability().getTabletMergeability());
        } else {
          // New splits should match the original setting in the map
          assertEquals(splits.get(t.getEndRow()),
              t.getTabletMergeability().getTabletMergeability());
        }
      });
    }
  }

  private void verifySplitsWithApi(AccumuloClient c, String tableName,
      SortedMap<Text,TabletMergeability> splits) throws TableNotFoundException {
    c.tableOperations().getTabletInformation(tableName, new Range()).forEach(ti -> {
      var tmInfo = ti.getTabletMergeabilityInfo();
      // default tablet should always be set to never
      if (ti.getTabletId().getEndRow() == null) {
        assertEquals(TabletMergeability.never(),
            ti.getTabletMergeabilityInfo().getTabletMergeability());
      } else {
        assertEquals(splits.get(ti.getTabletId().getEndRow()), tmInfo.getTabletMergeability());
      }
      assertTrue(tmInfo.getCurrentTime().toNanos() > 0);
      // isMergeable() should only be true for the always case because
      // there has not been a long enough delay after insertion
      // for the other tablets to be mergeable
      assertEquals(tmInfo.getTabletMergeability().isAlways(), tmInfo.isMergeable());
    });
  }

  private void verifyData(AccumuloClient client, String tableName, long ts) throws Exception {
    try (Scanner scanner = client.createScanner(tableName, Authorizations.EMPTY)) {

      Iterator<Entry<Key,Value>> iter = scanner.iterator();

      for (int i = 0; i < 10000; i++) {
        if (!iter.hasNext()) {
          throw new Exception("row " + i + " not found");
        }

        Entry<Key,Value> entry = iter.next();

        String row = String.format("%09d", i);

        if (!entry.getKey().getRow().equals(new Text(row))) {
          throw new Exception("unexpected row " + entry.getKey() + " " + i);
        }

        if (entry.getKey().getTimestamp() != ts) {
          throw new Exception("unexpected ts " + entry.getKey() + " " + ts);
        }

        if (Integer.parseInt(entry.getValue().toString()) != i) {
          throw new Exception("unexpected value " + entry + " " + i);
        }
      }

      if (iter.hasNext()) {
        throw new Exception("found more than expected " + iter.next());
      }
    }
  }

  private void insertData(AccumuloClient client, String tableName, long ts) throws Exception {
    try (BatchWriter bw = client.createBatchWriter(tableName)) {
      for (int i = 0; i < 10000; i++) {
        String row = String.format("%09d", i);
        Mutation m = new Mutation(new Text(row));
        m.put(new Text("cf1"), new Text("cq1"), ts, new Value(Integer.toString(i)));
        bw.addMutation(m);
      }
    }
  }
}

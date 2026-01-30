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
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.TabletMergeability;
import org.apache.accumulo.core.client.admin.TabletMergeabilityInfo;
import org.apache.accumulo.core.clientImpl.TabletMergeabilityUtil;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.RowRange;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.metadata.schema.TabletsMetadata;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.harness.SharedMiniClusterBase;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.google.common.collect.ImmutableMap;

public class AddSplitIT_SimpleSuite extends SharedMiniClusterBase {

  @BeforeAll
  public static void setup() throws Exception {
    SharedMiniClusterBase.startMiniCluster();
  }

  @AfterAll
  public static void teardown() {
    SharedMiniClusterBase.stopMiniCluster();
  }

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
      verifySplits(id, TabletMergeabilityUtil.userDefaultSplits(splits));
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
      splits.put(new Text(String.format("%09d", 111)), TabletMergeability.always());
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

      // Update existing and add two new splits
      // remove split 111 from map so we don't update it
      splits.remove(new Text(String.format("%09d", 111)));
      splits.put(new Text(String.format("%09d", 333)),
          TabletMergeability.after(Duration.ofHours(11)));
      splits.put(new Text(String.format("%09d", 666)),
          TabletMergeability.after(Duration.ofHours(10)));
      splits.put(new Text(String.format("%09d", 444)), TabletMergeability.always());
      splits.put(new Text(String.format("%09d", 777)),
          TabletMergeability.after(Duration.ofMinutes(5)));

      c.tableOperations().putSplits(tableName, splits);

      // re-add split 111 and verify it has not changed
      splits.put(new Text(String.format("%09d", 111)), TabletMergeability.always());

      verifySplits(id, splits);
      verifySplitsWithApi(c, tableName, splits);

      // Update with same and make sure insertion time updated
      splits.clear();
      var split = new Text(String.format("%09d", 777));
      splits.put(split, TabletMergeability.after(Duration.ofMinutes(5)));

      var originalTmi =
          c.tableOperations().getTabletInformation(tableName, List.of(RowRange.closed(split)))
              .findFirst().orElseThrow().getTabletMergeabilityInfo();
      assertTrue(originalTmi.getElapsed().orElseThrow().toNanos() > 0);

      // Update existing with same delay of 5 minutes
      c.tableOperations().putSplits(tableName, splits);
      var updatedTmi =
          c.tableOperations().getTabletInformation(tableName, List.of(RowRange.closed(split)))
              .findFirst().orElseThrow().getTabletMergeabilityInfo();

      // TabletMergeability setting should be the same but the new elapsed time should
      // be different (less time has passed as it was updated with a new insertion time)
      assertEquals(originalTmi.getTabletMergeability(), updatedTmi.getTabletMergeability());
      assertTrue(
          originalTmi.getElapsed().orElseThrow().compareTo(updatedTmi.getElapsed().orElseThrow())
              > 0);
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

      var tableInfo = c.tableOperations().getTabletInformation(tableName, List.of(RowRange.all()))
          .collect(Collectors.toMap(ti -> ti.getTabletId().getEndRow(), Function.identity()));

      // Set to always
      var split1Tmi = tableInfo.get(split1).getTabletMergeabilityInfo();
      assertTrue(split1Tmi.isMergeable());
      assertTrue(split1Tmi.getTabletMergeability().isAlways());
      assertTrue(split1Tmi.getElapsed().orElseThrow().toNanos() > 0);
      assertEquals(Duration.ZERO, split1Tmi.getRemaining().orElseThrow());
      assertEquals(computeRemaining(split1Tmi), split1Tmi.getRemaining());

      // Set to never
      var split2Tmi = tableInfo.get(split2).getTabletMergeabilityInfo();
      assertFalse(split2Tmi.isMergeable());
      assertTrue(split2Tmi.getTabletMergeability().isNever());
      assertTrue(split2Tmi.getElapsed().isEmpty());
      assertTrue(split2Tmi.getDelay().isEmpty());
      assertTrue(split2Tmi.getRemaining().isEmpty());

      // Set to a delay of 1 ms and current time should have elapsed long enough
      var split3Tmi = tableInfo.get(split3).getTabletMergeabilityInfo();
      assertTrue(split3Tmi.isMergeable());
      assertEquals(Duration.ofMillis(1), split3Tmi.getDelay().orElseThrow());
      assertTrue(split3Tmi.getElapsed().orElseThrow().toNanos() > 0);
      assertEquals(computeRemaining(split3Tmi), split3Tmi.getRemaining());

      // Set to a delay of 1 day and current time has NOT elapsed long enough
      var split4Tmi = tableInfo.get(split4).getTabletMergeabilityInfo();
      assertFalse(split4Tmi.isMergeable());
      assertEquals(Duration.ofDays(1), split4Tmi.getDelay().orElseThrow());
      assertTrue(split4Tmi.getElapsed().orElseThrow().toNanos() > 0);
      assertEquals(computeRemaining(split4Tmi), split4Tmi.getRemaining());
    }
  }

  // test remaining matches logic in the TabletMergeabilityInfo class
  private static Optional<Duration> computeRemaining(TabletMergeabilityInfo tmi) {
    return tmi.getDelay().map(delay -> delay.minus(tmi.getElapsed().orElseThrow()))
        .map(remaining -> remaining.isNegative() ? Duration.ZERO : remaining);
  }

  @Test
  public void concurrentAddSplitTest() throws Exception {
    var threads = 10;
    var service = Executors.newFixedThreadPool(threads);
    var latch = new CountDownLatch(threads);

    String tableName = getUniqueNames(1)[0];
    try (AccumuloClient c = Accumulo.newClient().from(getClientProps()).build()) {
      c.tableOperations().create(tableName);

      // Create a map to hold all splits to verify later
      SortedMap<Text,TabletMergeability> allSplits =
          Collections.synchronizedNavigableMap(new TreeMap<>());
      var commonBuilder = ImmutableMap.<Text,TabletMergeability>builder();
      for (int i = 0; i < 50; i++) {
        commonBuilder.put(new Text(String.format("%09d", i)),
            TabletMergeability.after(Duration.ofHours(1 + i)));
      }

      // create 50 splits that will be added to all threads
      var commonSplits = commonBuilder.build();
      allSplits.putAll(commonSplits);

      // Spin up 10 threads and concurrently submit all 50 existing splits
      // as well as 50 unique splits, this should create a collision with fate
      // and cause retries
      for (int i = 1; i <= threads; i++) {
        var start = i * 100;
        service.execute(() -> {
          // add the 50 common splits
          SortedMap<Text,TabletMergeability> splits = new TreeMap<>(commonSplits);
          // create 50 unique splits
          for (int j = start; j < start + 50; j++) {
            splits.put(new Text(String.format("%09d", j)),
                TabletMergeability.after(Duration.ofHours(1 + j)));
          }
          // make sure all splits are captured
          allSplits.putAll(splits);
          // Wait for all 10 threads to be ready before calling putSplits()
          // to increase the chance of collisions
          latch.countDown();
          try {
            latch.await();
            c.tableOperations().putSplits(tableName, splits);
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        });
      }

      // Wait for all 10 threads to finish
      service.shutdown();
      assertTrue(service.awaitTermination(2, TimeUnit.MINUTES));

      // Verify we have 550 splits and then all splits are correctly set
      assertEquals(50 + (threads * 50), allSplits.size());
      TableId id = TableId.of(c.tableOperations().tableIdMap().get(tableName));
      verifySplits(id, allSplits);
      verifySplitsWithApi(c, tableName, allSplits);
    }
  }

  // Checks that TabletMergeability in metadata matches split settings in the map
  private void verifySplits(TableId id, SortedMap<Text,TabletMergeability> splits) {
    final Set<Text> addedSplits = new HashSet<>(splits.keySet());
    try (TabletsMetadata tm =
        getCluster().getServerContext().getAmple().readTablets().forTable(id).build()) {
      tm.stream().forEach(t -> {
        var split = t.getEndRow();
        // default tablet should be set to always
        if (split == null) {
          assertEquals(TabletMergeability.always(),
              t.getTabletMergeability().getTabletMergeability());
        } else {
          assertTrue(addedSplits.remove(split));
          // New splits should match the original setting in the map
          assertEquals(splits.get(split), t.getTabletMergeability().getTabletMergeability());
        }
      });
    }
    // All splits should be seen
    assertTrue(addedSplits.isEmpty());
  }

  private void verifySplitsWithApi(AccumuloClient c, String tableName,
      SortedMap<Text,TabletMergeability> splits) throws TableNotFoundException {
    final Set<Text> addedSplits = new HashSet<>(splits.keySet());
    c.tableOperations().getTabletInformation(tableName, List.of(RowRange.all())).forEach(ti -> {
      var tmInfo = ti.getTabletMergeabilityInfo();
      var split = ti.getTabletId().getEndRow();
      // default tablet should always be set to always
      if (split == null) {
        assertEquals(TabletMergeability.always(),
            ti.getTabletMergeabilityInfo().getTabletMergeability());
      } else {
        assertTrue(addedSplits.remove(split));
        assertEquals(splits.get(split), tmInfo.getTabletMergeability());
      }
      if (tmInfo.getTabletMergeability().isNever()) {
        assertTrue(tmInfo.getElapsed().isEmpty());
      } else {
        assertTrue(tmInfo.getElapsed().orElseThrow().toNanos() > 0);
      }
      // isMergeable() should only be true for the always case because
      // there has not been a long enough delay after insertion
      // for the other tablets to be mergeable
      assertEquals(tmInfo.getTabletMergeability().isAlways(), tmInfo.isMergeable());
    });
    // All splits should be seen
    assertTrue(addedSplits.isEmpty());
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

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
package org.apache.accumulo.test.fate;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.admin.CloneConfiguration;
import org.apache.accumulo.core.client.admin.CompactionConfig;
import org.apache.accumulo.core.client.admin.NewTableConfiguration;
import org.apache.accumulo.core.client.admin.TabletAvailability;
import org.apache.accumulo.core.client.admin.TabletMergeability;
import org.apache.accumulo.core.client.rfile.RFile;
import org.apache.accumulo.core.clientImpl.AcceptableThriftTableOperationException;
import org.apache.accumulo.core.clientImpl.ClientContext;
import org.apache.accumulo.core.clientImpl.thrift.TableOperation;
import org.apache.accumulo.core.clientImpl.thrift.TableOperationExceptionType;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.fate.AdminUtil;
import org.apache.accumulo.core.fate.FateId;
import org.apache.accumulo.core.fate.zookeeper.LockRange;
import org.apache.accumulo.core.metadata.schema.Ample;
import org.apache.accumulo.harness.AccumuloClusterHarness;
import org.apache.accumulo.manager.tableOps.Utils;
import org.apache.accumulo.minicluster.ServerType;
import org.apache.accumulo.test.util.Wait;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.Test;

/**
 * Tests ranged table locks used by fate operations.
 */
public class RangedTableLocksIT extends AccumuloClusterHarness {

  @Test
  public void testCompactBulkMergeClone() throws Exception {
    getCluster().getClusterControl().stopAllServers(ServerType.COMPACTOR);

    ExecutorService executor = Executors.newCachedThreadPool();

    var names = getUniqueNames(2);
    String table = names[0];
    String clone = names[1];

    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
      SortedMap<Text,TabletMergeability> splits = new TreeMap<>();
      for (char c = 'b'; c < 'y'; c++) {
        splits.put(new Text(c + ""), TabletMergeability.never());
      }
      client.tableOperations().create(table, new NewTableConfiguration().withSplits(splits));

      SortedSet<String> expectedRows = new TreeSet<>();
      try (var writer = client.createBatchWriter(table)) {
        for (char c = 'b'; c < 'y'; c++) {
          Mutation m = new Mutation(c + "");
          m.at().family("test").qualifier("1").put("3");
          writer.addMutation(m);
          expectedRows.add(c + "");
        }
      }

      // start a compaction that will get stuck because no compactors are running
      var compactionFuture = executor.submit(() -> {
        client.tableOperations().compact(table,
            new CompactionConfig().setEndRow(new Text("b5")).setWait(true));
        return null;
      });

      Wait.waitFor(() -> !getLocksInfo().heldLocks.isEmpty());
      var lockInfo = getLocksInfo();
      assertEquals(lockInfo.heldLocks.keySet(), lockInfo.findId(LockRange.of(null, "b5")));

      // this bulk will overlap the compaction range, but should not get stuck because both are read
      // locks
      bulkImport(client, table, List.of("aa"));
      expectedRows.add("aa");
      assertEquals(expectedRows, getTableRows(client, table));

      // start a merge operation that should get stuck because it overlaps the tablet that
      // compaction has locked for read
      var mergeFuture = executor.submit(() -> {
        client.tableOperations().merge(table, new Text("b6"), new Text("c9"));
        return null;
      });

      Wait.waitFor(() -> getLocksInfo().waitingLocks.size() == 1);
      // the merge range should widen to table split points
      lockInfo = getLocksInfo();
      assertEquals(lockInfo.waitingLocks.keySet(), lockInfo.findId(LockRange.of("b", "d")));

      // start a bulk operation that should get stuck waiting on the merge operation
      var bulkFuture = executor.submit(() -> {
        bulkImport(client, table, List.of("ab", "cc"));
        return null;
      });

      Wait.waitFor(() -> getLocksInfo().waitingLocks.size() == 2);

      var splitsCopy = new TreeSet<>(splits.keySet());
      // splits should still be the same as when the table was created
      assertEquals(splitsCopy, new TreeSet<>(client.tableOperations().listSplits(table)));

      // merge should not block because its range does not overlap w/ the other blocked operations
      client.tableOperations().merge(table, new Text("h1"), new Text("k9"));

      assertTrue(splitsCopy.remove(new Text("i")));
      assertTrue(splitsCopy.remove(new Text("j")));
      assertTrue(splitsCopy.remove(new Text("k")));
      // verify the split removed the expected ranges
      assertEquals(splitsCopy, new TreeSet<>(client.tableOperations().listSplits(table)));

      // the clone operation should get stuck trying to get a read lock on the entire source table
      // range
      var cloneFuture = executor.submit(() -> {
        client.tableOperations().clone(table, clone, CloneConfiguration.empty());
        return null;
      });

      Wait.waitFor(() -> getLocksInfo().waitingLocks.size() == 3);

      assertEquals(expectedRows, getTableRows(client, table));

      // bulk import should not block because it does not overlap any write locks. It does overlap
      // the clone operation which has a read lock.
      bulkImport(client, table, List.of("qq", "xx"));
      expectedRows.add("qq");
      expectedRows.add("xx");
      assertEquals(expectedRows, getTableRows(client, table));

      // nothing should have changed w/ the locks as a result of the merge running, also any locks
      // from the merge should be gone
      lockInfo = getLocksInfo();

      // remove the fate op that only has a lock on the namespace
      lockInfo.heldLocks.values().removeIf(idList -> idList.equals(List.of("R:+default")));
      assertEquals(lockInfo.heldLocks.keySet(), lockInfo.findId(LockRange.of(null, "b5")));
      assertEquals(lockInfo.waitingLocks.keySet(),
          lockInfo.findId(LockRange.of("b", "d"), LockRange.of(null, "d"), LockRange.infinite()));

      // should not be able to complete until compactors are started
      assertFalse(compactionFuture.isDone());
      assertFalse(mergeFuture.isDone());
      assertFalse(bulkFuture.isDone());
      assertFalse(cloneFuture.isDone());

      // starting the compactors should allow the compaction to finish and then the merge to finish
      getCluster().getClusterControl().startAllServers(ServerType.COMPACTOR);

      // should complete w/o errors
      compactionFuture.get();
      mergeFuture.get();
      bulkFuture.get();
      cloneFuture.get();

      // verify the merge removed the split
      assertTrue(splitsCopy.remove(new Text("c")));
      assertEquals(splitsCopy, new TreeSet<>(client.tableOperations().listSplits(table)));

      var prevExpectedRows = new TreeSet<>(expectedRows);

      // verify data from the bulk import that was blocked and then ran
      expectedRows.add("ab");
      expectedRows.add("cc");
      assertEquals(expectedRows, getTableRows(client, table));

      var cloneRows = getTableRows(client, clone);
      // The clone operation and the bulk operation were both waiting on the merge operation. Once
      // the merge completes, both clone and bulk import can start running concurrently. So the
      // clone may or may not pick up the imported data.
      assertTrue(cloneRows.equals(expectedRows) || cloneRows.equals(prevExpectedRows),
          cloneRows::toString);

    } finally {
      executor.shutdownNow();
    }

  }

  @Test
  public void testWiden() throws Exception {
    var names = getUniqueNames(2);
    var table1 = names[0];
    var table2 = names[1];
    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
      SortedMap<Text,TabletMergeability> splits = new TreeMap<>();
      for (char c = 'b'; c < 'y'; c++) {
        splits.put(new Text(c + ""), TabletMergeability.never());
      }
      client.tableOperations().create(table1, new NewTableConfiguration().withSplits(splits));
      client.tableOperations().create(table2);

      ClientContext ctx = ((ClientContext) client);
      Ample ample = ctx.getAmple();

      TableId tableId1 = ctx.getTableId(table1);
      TableId tableId2 = ctx.getTableId(table2);

      TableOperation op = TableOperation.MERGE;

      assertEquals(LockRange.infinite(),
          Utils.widen(ample, tableId1, LockRange.infinite(), op, true));
      assertEquals(LockRange.infinite(),
          Utils.widen(ample, tableId2, LockRange.infinite(), op, true));
      assertEquals(LockRange.infinite(),
          Utils.widen(ample, tableId2, LockRange.of(null, "m"), op, true));
      assertEquals(LockRange.infinite(),
          Utils.widen(ample, tableId2, LockRange.of("m", "q"), op, true));
      assertEquals(LockRange.infinite(),
          Utils.widen(ample, tableId2, LockRange.of("m", null), op, true));

      assertEquals(LockRange.of(null, "c"),
          Utils.widen(ample, tableId1, LockRange.of(null, "bb"), op, true));
      assertEquals(LockRange.of(null, "c"),
          Utils.widen(ample, tableId1, LockRange.of(null, "c"), op, true));
      assertEquals(LockRange.of(null, "c"),
          Utils.widen(ample, tableId1, LockRange.of("aa", "bb"), op, true));
      assertEquals(LockRange.of(null, "c"),
          Utils.widen(ample, tableId1, LockRange.of("aa", "c"), op, true));

      assertEquals(LockRange.of("e", "j"),
          Utils.widen(ample, tableId1, LockRange.of("ee", "ii"), op, true));
      assertEquals(LockRange.of("e", "j"),
          Utils.widen(ample, tableId1, LockRange.of("e", "ii"), op, true));
      assertEquals(LockRange.of("e", "j"),
          Utils.widen(ample, tableId1, LockRange.of("ee", "j"), op, true));
      assertEquals(LockRange.of("e", "j"),
          Utils.widen(ample, tableId1, LockRange.of("e", "j"), op, true));

      assertEquals(LockRange.of("q", null),
          Utils.widen(ample, tableId1, LockRange.of("qq", null), op, true));
      assertEquals(LockRange.of("q", null),
          Utils.widen(ample, tableId1, LockRange.of("q", null), op, true));
      assertEquals(LockRange.of("q", null),
          Utils.widen(ample, tableId1, LockRange.of("qq", "zz"), op, true));
      assertEquals(LockRange.of("q", null),
          Utils.widen(ample, tableId1, LockRange.of("q", "zz"), op, true));

      TableId nonExistentTableId = TableId.of("abcdefg");

      for (var lockRange : List.of(LockRange.of("c", "f"), LockRange.of(null, "f"),
          LockRange.of("c", null))) {
        var expception = assertThrows(AcceptableThriftTableOperationException.class,
            () -> Utils.widen(ample, nonExistentTableId, lockRange, op, true));
        assertEquals(nonExistentTableId.canonical(), expception.getTableId());
        assertEquals(TableOperationExceptionType.NOTFOUND, expception.getType());

        assertEquals(LockRange.infinite(),
            Utils.widen(ample, nonExistentTableId, lockRange, op, false));
      }

      // when the lock range is infinite no metadata lookup is done
      assertEquals(LockRange.infinite(),
          Utils.widen(ample, nonExistentTableId, LockRange.infinite(), op, true));
    }
  }

  /**
   * Ranged table locks have mechanisms for handling race conditions during widening of ranges.
   * Concurrent merge operations may exercise this code.
   */
  @Test
  public void testConcurrentMerge() throws Exception {
    String table = getUniqueNames(1)[0];

    ExecutorService executor = Executors.newCachedThreadPool();

    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
      SortedMap<Text,TabletMergeability> splits = new TreeMap<>();
      for (int r = 0; r < 1_000; r += 10) {
        splits.put(new Text(String.format("%06d", r)), TabletMergeability.never());
      }
      client.tableOperations().create(table, new NewTableConfiguration().withSplits(splits)
          .withInitialTabletAvailability(TabletAvailability.HOSTED));

      Set<String> expectedRows = new TreeSet<>();
      try (var writer = client.createBatchWriter(table)) {
        for (int r = 0; r < 1_000; r++) {
          var row = String.format("%06d", r);
          expectedRows.add(row);
          Mutation m = new Mutation(row);
          m.put("f", "q", "v");
          writer.addMutation(m);
        }
      }

      List<Future<?>> futures = new ArrayList<>();
      // create 9 sets of 3 concurrent merge operations where each set of 3 merge operations overlap
      // w/ each other
      for (int r = 100; r < 1_000; r += 100) {
        int row = r;
        futures.add(executor.submit(() -> {
          client.tableOperations().merge(table, new Text(String.format("%06d", row - 15)),
              new Text(String.format("%06d", row + 5)));
          return null;
        }));
        futures.add(executor.submit(() -> {
          client.tableOperations().merge(table, new Text(String.format("%06d", row - 5)),
              new Text(String.format("%06d", row + 15)));
          return null;
        }));
        futures.add(executor.submit(() -> {
          client.tableOperations().merge(table, new Text(String.format("%06d", row)),
              new Text(String.format("%06d", row + 20)));
          return null;
        }));

        // remove splits that should be removed by these merge operations.
        splits.subMap(new Text(String.format("%06d", row - 15)),
            new Text(String.format("%06d", row + 20))).clear();
      }

      // wait for all merge operations to complete and ensure none failed
      for (var future : futures) {
        future.get();
      }

      // ensure the table data and splits are correct
      assertEquals(expectedRows, getTableRows(client, table));
      assertEquals(splits.keySet(), new TreeSet<>(client.tableOperations().listSplits(table)));
    } finally {
      executor.shutdownNow();
    }
  }

  private SortedSet<String> getTableRows(AccumuloClient client, String table) throws Exception {
    try (var scanner = client.createScanner(table)) {
      return scanner.stream().map(Map.Entry::getKey).map(k -> k.getRowData().toString())
          .collect(Collectors.toCollection(TreeSet::new));
    }
  }

  private void bulkImport(AccumuloClient client, String table, List<String> rows) throws Exception {
    var bulkDir = new Path(getCluster().getTemporaryPath(), UUID.randomUUID().toString());
    try (var writer = RFile.newWriter().to(new Path(bulkDir, "f1.rf").toString())
        .withFileSystem(getFileSystem()).build()) {
      writer.startDefaultLocalityGroup();
      for (var row : rows) {
        writer.append(Key.builder().row(row).family("").qualifier("").build(), "");
      }
    }

    client.tableOperations().importDirectory(bulkDir.toString()).to(table).load();
  }

  static class LocksInfo {
    Map<FateId,List<String>> heldLocks = new HashMap<>();
    Map<FateId,List<String>> waitingLocks = new HashMap<>();
    Map<FateId,LockRange> lockRanges = new HashMap<>();

    Set<FateId> findId(LockRange... ranges) {
      var lrs = Set.of(ranges);
      return lockRanges.entrySet().stream().filter(e -> lrs.contains(e.getValue()))
          .map(Map.Entry::getKey).collect(Collectors.toSet());
    }
  }

  static LocksInfo getLocksInfo() throws Exception {
    var zTableLocksPath = getServerContext().getServerPaths().createTableLocksPath();
    LocksInfo locksInfo = new LocksInfo();
    AdminUtil.findLocks(getServerContext().getZooSession(), zTableLocksPath, locksInfo.heldLocks,
        locksInfo.waitingLocks, locksInfo.lockRanges);
    return locksInfo;
  }

}

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

import static org.apache.accumulo.harness.AccumuloITBase.SUNNY_DAY;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Random;
import java.util.TreeSet;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.IsolatedScanner;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.admin.CompactionConfig;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.harness.AccumuloClusterHarness;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * This test verifies that scans will always see data written before the scan started even when
 * there are concurrent scans, writes, and table operations running.
 */
@Tag(SUNNY_DAY)
public class ScanConsistencyIT extends AccumuloClusterHarness {

  private static final Logger log = LoggerFactory.getLogger(ScanConsistencyIT.class);

  @SuppressFBWarnings(value = {"PREDICTABLE_RANDOM", "DMI_RANDOM_USED_ONLY_ONCE"},
      justification = "predictable random is ok for testing")
  @Test
  public void testConcurrentScanConsistency() throws Exception {
    final String table = this.getUniqueNames(1)[0];

    var executor = Executors.newCachedThreadPool();
    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
      client.tableOperations().create(table);

      TestContext testContext = new TestContext(client, table);

      List<Future<WriteStats>> writeTasks = new ArrayList<>();
      List<Future<ScanStats>> scanTasks = new ArrayList<>();

      Random random = new Random();

      int numWriteTask = random.nextInt(10) + 1;
      int numsScanTask = random.nextInt(10) + 1;

      for (int i = 0; i < numWriteTask; i++) {
        writeTasks.add(executor.submit(new WriteTask(testContext)));
      }

      for (int i = 0; i < numsScanTask; i++) {
        scanTasks.add(executor.submit(new ScanTask(testContext)));
      }

      var tableOpsTask = executor.submit(new TableOpsTask(testContext));

      // let the concurrent mayhem run for a bit
      Thread.sleep(60000);

      // let the threads know to exit
      testContext.keepRunning.set(false);

      for (Future<WriteStats> writeTask : writeTasks) {
        var stats = writeTask.get();
        log.debug(String.format("Wrote:%,d Deleted:%,d", stats.written, stats.deleted));
        assertTrue(stats.written > 0);
        assertTrue(stats.deleted > 0);
      }

      for (Future<ScanStats> scanTask : scanTasks) {
        var stats = scanTask.get();
        log.debug(String.format("Scanned:%,d verified:%,d", stats.scanned, stats.verified));
        assertTrue(stats.verified > 0);
        // These scans were running concurrently with writes, so a scan will see more data than what
        // was written before the scan started.
        assertTrue(stats.scanned > stats.verified);
      }

      log.debug(tableOpsTask.get());

      var stats1 = scanData(testContext, new Range(), false);
      var stats2 = scanData(testContext, new Range(), true);
      log.debug(
          String.format("Final scan, scanned:%,d verified:%,d", stats1.scanned, stats1.verified));
      assertTrue(stats1.verified > 0);
      // Should see all expected data now that there are no concurrent writes happening
      assertEquals(stats1.scanned, stats1.verified);
      assertEquals(stats2.scanned, stats1.scanned);
      assertEquals(stats2.verified, stats1.verified);
    } finally {
      executor.shutdownNow();
    }

  }

  /**
   * Tracks what data has been written and deleted in an Accumulo table.
   */
  private static class DataTracker {
    // ConcurrentLinkedQueue was chosen because its safe to iterate over concurrently w/o locking
    private final Queue<DataSet> dataSets = new ConcurrentLinkedQueue<>();

    /**
     * Reserves data for scan so that it will not be deleted and returns the data that is expected
     * to exist in the table at this point.
     */
    public ExpectedScanData beginScan() {
      List<DataSet> reservedData = new ArrayList<>();

      for (var dataSet : dataSets) {
        if (dataSet.reserveForScan()) {
          reservedData.add(dataSet);
        }
      }

      return new ExpectedScanData(reservedData);
    }

    /**
     * add new data that scans should expect to see
     */
    public void addExpectedData(List<Mutation> data) {
      dataSets.add(new DataSet(data));
    }

    /**
     * @return data to delete from the table that is not reserved for scans
     */
    public Iterable<Mutation> getDeletes() {
      DataSet dataSet = dataSets.poll();

      if (dataSet == null) {
        return List.of();
      }

      dataSet.reserveForDelete();

      return () -> dataSet.data.stream().map(m -> {
        Mutation delMutation = new Mutation(m.getRow());
        m.getUpdates()
            .forEach(cu -> delMutation.putDelete(cu.getColumnFamily(), cu.getColumnQualifier()));
        return delMutation;
      }).iterator();
    }

    public long estimatedRows() {
      return dataSets.stream().mapToLong(ds -> ds.data.size()).sum();
    }
  }

  private static class DataSet {
    private final List<Mutation> data;

    private int activeScans = 0;

    private boolean deleting = false;

    public DataSet(List<Mutation> data) {
      this.data = data;
    }

    synchronized boolean reserveForScan() {
      if (deleting) {
        return false;
      }

      activeScans++;

      return true;
    }

    synchronized void unreserveForScan() {
      activeScans--;
      Preconditions.checkState(activeScans >= 0);
      if (activeScans == 0) {
        notify();
      }
    }

    synchronized void reserveForDelete() {
      Preconditions.checkState(!deleting);
      deleting = true;
      while (activeScans > 0) {
        try {
          wait(50);
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        }
      }
    }

    Stream<Key> getExpectedData(Range range) {
      return data
          .stream().flatMap(m -> m.getUpdates().stream().map(cu -> new Key(m.getRow(),
              cu.getColumnFamily(), cu.getColumnQualifier(), cu.getColumnVisibility(), 0L)))
          .filter(range::contains);
    }

  }

  private static class ExpectedScanData implements AutoCloseable {

    private final List<DataSet> reservedData;

    public ExpectedScanData(List<DataSet> reservedData) {
      this.reservedData = reservedData;
    }

    /**
     * @return keys that are expected to exist in the accumulo table
     */
    Stream<Key> getExpectedData(Range range) {
      return reservedData.stream().flatMap(ds -> ds.getExpectedData(range));
    }

    @Override
    public void close() {
      reservedData.forEach(DataSet::unreserveForScan);
    }
  }

  private static class TestContext {
    final DataTracker dataTracker = new DataTracker();
    final AccumuloClient client;
    final String table;
    final AtomicBoolean keepRunning = new AtomicBoolean(true);
    final AtomicLong generationCounter = new AtomicLong(0);

    private TestContext(AccumuloClient client, String table) {
      this.client = client;
      this.table = table;
    }
  }

  private static class ScanStats {
    long scanned;
    long verified;

    public void add(ScanStats stats) {
      scanned += stats.scanned;
      verified += stats.verified;
    }
  }

  private static ScanStats scanData(TestContext tctx, Range range, boolean scanIsolated)
      throws Exception {
    ScanStats stats = new ScanStats();
    try (ExpectedScanData expectedScanData = tctx.dataTracker.beginScan();
        Scanner scanner = tctx.client.createScanner(tctx.table)) {
      HashSet<Key> expected = new HashSet<>();
      expectedScanData.getExpectedData(range).forEach(expected::add);
      scanner.setRange(range);

      Scanner s = scanner;
      if (scanIsolated) {
        s = new IsolatedScanner(scanner);
      }

      for (Map.Entry<Key,Value> entry : s) {
        stats.scanned++;
        Key key = entry.getKey();
        key.setTimestamp(0);
        if (expected.remove(key)) {
          stats.verified++;
        }
      }

      assertTrue(expected.isEmpty());
    }
    return stats;
  }

  private static class ScanTask implements Callable<ScanStats> {

    private final TestContext tctx;

    private ScanTask(TestContext testContext) {
      this.tctx = testContext;
    }

    @SuppressFBWarnings(value = {"PREDICTABLE_RANDOM", "DMI_RANDOM_USED_ONLY_ONCE"},
        justification = "predictable random is ok for testing")
    @Override
    public ScanStats call() throws Exception {
      ScanStats allStats = new ScanStats();

      Random random = new Random();

      while (tctx.keepRunning.get()) {

        Range range;
        if (random.nextInt(10) == 0) {
          // 1 in 10 chance of doing a full table scan
          range = new Range();
        } else {
          long start = nextLongAbs(random);
          long end = nextLongAbs(random);

          while (end <= start) {
            end = nextLongAbs(random);
          }

          range = new Range(String.format("%016x", start), String.format("%016x", end));
        }

        var stats = scanData(tctx, range, random.nextBoolean());
        allStats.add(stats);
      }

      return allStats;
    }
  }

  private static class WriteStats {
    long written;
    long deleted;
  }

  private static class WriteTask implements Callable<WriteStats> {

    private final TestContext tctx;

    private WriteTask(TestContext testContext) {
      this.tctx = testContext;
    }

    @SuppressFBWarnings(value = {"PREDICTABLE_RANDOM", "DMI_RANDOM_USED_ONLY_ONCE"},
        justification = "predictable random is ok for testing")
    @Override
    public WriteStats call() throws Exception {
      WriteStats stats = new WriteStats();

      Random random = new Random();

      while (tctx.keepRunning.get()) {

        // Each row has on average of 50 cols, so 20K rows would be around 100K entries.
        if (tctx.dataTracker.estimatedRows() > 100_000 / 50) {
          // There is a good bit of data, so delete some.
          try (var writer = tctx.client.createBatchWriter(tctx.table)) {
            for (Mutation m : tctx.dataTracker.getDeletes()) {
              writer.addMutation(m);
              stats.deleted += m.getUpdates().size();
            }
          }

        } else {

          List<Mutation> dataAdded = new ArrayList<>();

          int rowsToGenerate = random.nextInt(1000);

          try (var writer = tctx.client.createBatchWriter(tctx.table)) {
            for (int i = 0; i < rowsToGenerate; i++) {
              Mutation m = generateMutation(random);
              writer.addMutation(m);
              stats.written += m.getUpdates().size();
              dataAdded.add(m);
            }
          }

          // Make the data just written visible to scans. Now that the writer is closed scans should
          // be able to see the data in the table.
          tctx.dataTracker.addExpectedData(dataAdded);
        }
      }

      return stats;
    }

    private Mutation generateMutation(Random random) {

      int cols = random.nextInt(100) + 1;

      // the generation counter ensures the row is unique
      String row = String.format("%016x:%016x", nextLongAbs(random),
          tctx.generationCounter.getAndIncrement());

      Mutation m = new Mutation(row);
      for (int i = 0; i < cols; i++) {
        m.put(String.valueOf(random.nextInt(10)), String.format("%04x", random.nextInt(256 * 256)),
            "");
      }

      return m;
    }
  }

  private static class TableOpsTask implements Callable<String> {
    private final TestContext tctx;

    private TableOpsTask(TestContext testContext) {
      this.tctx = testContext;
    }

    @SuppressFBWarnings(value = {"PREDICTABLE_RANDOM", "DMI_RANDOM_USED_ONLY_ONCE"},
        justification = "predictable random is ok for testing")
    @Override
    public String call() throws Exception {
      int numFlushes = 0;
      int numCompactions = 0;
      int numSplits = 0;

      Random random = new Random();

      while (tctx.keepRunning.get()) {
        Thread.sleep(1000);

        int pick = random.nextInt(100);

        if (pick < 10) {
          // 1 in 10 chance of flushing
          tctx.client.tableOperations().flush(tctx.table, null, null, random.nextBoolean());
          numFlushes++;
        } else if (pick < 15) {
          // 1 in 20 chance of compacting
          tctx.client.tableOperations().compact(tctx.table,
              new CompactionConfig().setFlush(random.nextBoolean()).setWait(random.nextBoolean()));
          numCompactions++;
        } else if (pick < 20) {
          // 1 in 20 chance of splitting
          int splitsToAdd = random.nextInt(10);
          TreeSet<Text> splits = new TreeSet<>();

          for (int i = 0; i < splitsToAdd; i++) {
            splits.add(new Text(String.format("%016x", nextLongAbs(random))));
          }

          tctx.client.tableOperations().addSplits(tctx.table, splits);
          numSplits += splitsToAdd;
        }

        if (random.nextInt(10) == 0) {
          tctx.client.tableOperations().flush(tctx.table, null, null, true);
          numFlushes++;
        }
      }

      return String.format("Flushes:%,d Compactions:%,d Splits added:%,d", numFlushes,
          numCompactions, numSplits);
    }
  }

  /**
   * @return absolute value of a random long
   */
  private static long nextLongAbs(Random r) {
    return random.nextLong() & 0x7fffffffffffffffL;
  }
}

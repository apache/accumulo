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

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.IsolatedScanner;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.admin.CompactionConfig;
import org.apache.accumulo.core.client.rfile.RFile;
import org.apache.accumulo.core.client.rfile.RFileWriter;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.Filter;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.harness.AccumuloClusterHarness;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.Collections2;
import com.google.common.collect.MoreCollectors;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * This test verifies that scans will always see data written before the scan started even when
 * there are concurrent scans, writes, and table operations running.
 */
public class ScanConsistencyIT extends AccumuloClusterHarness {

  private static final Logger log = LoggerFactory.getLogger(ScanConsistencyIT.class);

  @SuppressFBWarnings(value = {"PREDICTABLE_RANDOM", "DMI_RANDOM_USED_ONLY_ONCE"},
      justification = "predictable random is ok for testing")
  @Test
  public void testConcurrentScanConsistency() throws Exception {
    final String table = this.getUniqueNames(1)[0];

    /**
     * Tips for debugging this test when it sees a row that should not exist or does not see a row
     * that should exist.
     *
     * 1. Disable the GC from running for the test.
     *
     * 2. Modify the test code to print some of the offending rows, just need a few to start
     * investigating.
     *
     * 3. After the test fails, somehow run the static function findRow() passing it the Accumulo
     * table directory that the failed test used and one of the problem rows.
     *
     * 4. Once the files containing the row is found, analyze what happened with those files in the
     * servers logs.
     */
    // getClusterControl().stopAllServers(ServerType.GARBAGE_COLLECTOR);

    var executor = Executors.newCachedThreadPool();
    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
      client.tableOperations().create(table);

      TestContext testContext = new TestContext(client, table, getCluster().getFileSystem(),
          getCluster().getTemporaryPath().toString());

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
        log.debug(String.format("Wrote:%,d Bulk imported:%,d Deleted:%,d Bulk deleted:%,d",
            stats.written, stats.bulkImported, stats.deleted, stats.bulkDeleted));
        assertTrue(stats.written + stats.bulkImported > 0);
        assertTrue(stats.deleted + stats.bulkDeleted > 0);
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

      var stats1 = scanData(testContext, random, new Range(), false);
      var stats2 = scanData(testContext, random, new Range(), true);
      var stats3 = batchScanData(testContext, new Range());
      log.debug(
          String.format("Final scan, scanned:%,d verified:%,d", stats1.scanned, stats1.verified));
      assertTrue(stats1.verified > 0);
      // Should see all expected data now that there are no concurrent writes happening
      assertEquals(stats1.scanned, stats1.verified);
      assertEquals(stats2.scanned, stats1.scanned);
      assertEquals(stats2.verified, stats1.verified);
      assertEquals(stats3.scanned, stats1.scanned);
      assertEquals(stats3.verified, stats1.verified);
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
    public Collection<Mutation> getDeletes() {
      DataSet dataSet = dataSets.poll();

      if (dataSet == null) {
        return List.of();
      }

      dataSet.reserveForDelete();

      return Collections2.transform(dataSet.data, m -> {
        Mutation delMutation = new Mutation(m.getRow());
        m.getUpdates()
            .forEach(cu -> delMutation.putDelete(cu.getColumnFamily(), cu.getColumnQualifier()));
        return delMutation;
      });
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
      return data.stream().flatMap(ScanConsistencyIT::toKeys).filter(range::contains);
    }
  }

  private static Stream<Key> toKeys(Mutation m) {
    return m.getUpdates().stream().map(cu -> new Key(m.getRow(), cu.getColumnFamily(),
        cu.getColumnQualifier(), cu.getColumnVisibility(), 0L, cu.isDeleted(), false));
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
    final FileSystem fileSystem;
    private final String tmpDir;

    private TestContext(AccumuloClient client, String table, FileSystem fs, String tmpDir) {
      this.client = client;
      this.table = table;
      this.fileSystem = fs;
      this.tmpDir = tmpDir;
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

  private static ScanStats scan(Stream<Map.Entry<Key,Value>> scanner, Set<Key> expected) {
    ScanStats stats = new ScanStats();

    scanner.forEach(entry -> {
      stats.scanned++;
      Key key = entry.getKey();
      key.setTimestamp(0);
      if (expected.remove(key)) {
        stats.verified++;
      }
    });

    assertTrue(expected.isEmpty());
    return stats;
  }

  // TODO create multiple ranges for batch scanner
  private static ScanStats batchScanData(TestContext tctx, Range range) throws Exception {
    try (ExpectedScanData expectedScanData = tctx.dataTracker.beginScan();
        BatchScanner scanner = tctx.client.createBatchScanner(tctx.table)) {
      Set<Key> expected = expectedScanData.getExpectedData(range).collect(Collectors.toSet());
      scanner.setRanges(List.of(range));
      return scan(scanner.stream(), expected);
    }
  }

  private static ScanStats scanData(TestContext tctx, Random random, Range range,
      boolean scanIsolated) throws Exception {
    try (ExpectedScanData expectedScanData = tctx.dataTracker.beginScan();
        Scanner scanner = scanIsolated ? new IsolatedScanner(tctx.client.createScanner(tctx.table))
            : tctx.client.createScanner(tctx.table)) {
      Set<Key> expected = expectedScanData.getExpectedData(range).collect(Collectors.toSet());

      Stream<Map.Entry<Key,Value>> scanStream;

      if (!range.isInfiniteStopKey() && random.nextBoolean()) {
        // Simulate the case where not all data in the range is read.
        var openEndedRange =
            new Range(range.getStartKey(), range.isStartKeyInclusive(), null, true);
        scanner.setRange(openEndedRange);
        // Create a stream that only reads the data in the original range, possibly leaving data
        // unread on the scanner.
        scanStream = scanner.stream().takeWhile(entry -> range.contains(entry.getKey()));
      } else {
        scanner.setRange(range);
        scanStream = scanner.stream();
      }

      return scan(scanStream, expected);
    }
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

        int scanChance = random.nextInt(3);
        if (scanChance == 0) {
          allStats.add(scanData(tctx, random, range, false));
        } else if (scanChance == 1) {
          allStats.add(scanData(tctx, random, range, true));
        } else {
          allStats.add(batchScanData(tctx, range));
        }
      }

      return allStats;
    }
  }

  private static class WriteStats {
    long written;
    long deleted;
    long bulkImported;
    long bulkDeleted;
  }

  private static class WriteTask implements Callable<WriteStats> {

    private final TestContext tctx;

    private WriteTask(TestContext testContext) {
      this.tctx = testContext;
    }

    @SuppressWarnings("deprecation")
    private long bulkImport(Random random, Collection<Mutation> mutations) throws Exception {

      if (mutations.isEmpty()) {
        return 0;
      }

      String name = "/bulkimport_" + nextLongAbs(random);
      Path bulkDir = new Path(tctx.tmpDir + name);
      Path failDir = new Path(tctx.tmpDir + name + "_failures");

      List<Key> keys = mutations.stream().flatMap(ScanConsistencyIT::toKeys).sorted()
          .collect(Collectors.toList());

      Value val = new Value();
      try {
        tctx.fileSystem.mkdirs(bulkDir);
        try (RFileWriter writer =
            RFile.newWriter().to(bulkDir + "/f1.rf").withFileSystem(tctx.fileSystem).build()) {
          writer.startDefaultLocalityGroup();
          for (Key key : keys) {
            writer.append(key, val);
          }
        }

        if (random.nextBoolean()) {
          // use bulk import v1
          tctx.fileSystem.mkdirs(failDir);
          tctx.client.tableOperations().importDirectory(tctx.table, bulkDir.toString(),
              failDir.toString(), true);
          assertEquals(0, tctx.fileSystem.listStatus(failDir).length,
              "Failure dir was not empty " + failDir);
        } else {
          // use bulk import v2
          tctx.client.tableOperations().importDirectory(bulkDir.toString()).to(tctx.table)
              .tableTime(true).load();
        }

      } finally {
        tctx.fileSystem.delete(bulkDir, true);
        tctx.fileSystem.delete(failDir, true);
      }

      return keys.size();
    }

    private long write(Iterable<Mutation> mutations) throws Exception {
      long written = 0;
      try (var writer = tctx.client.createBatchWriter(tctx.table)) {
        for (Mutation m : mutations) {
          writer.addMutation(m);
          written += m.size();
        }
      }

      return written;
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
          if (random.nextInt(5) == 0) {
            // Get the data to delete from the data tracker before flushing, that way its known to
            // be completely written and in memory or bulk imported.
            var deletes = tctx.dataTracker.getDeletes();
            // When bulk importing deletes must flush before the bulk import for the case where the
            // data being deleted is in memory. The bulk import may cause a full system compaction
            // which will drop delete keys that suppress the in memory data. Once the deletes are
            // dropped, the in memory data is no longer deleted.
            tctx.client.tableOperations().flush(tctx.table, null, null, true);
            stats.bulkDeleted += bulkImport(random, deletes);
          } else {
            stats.deleted += write(tctx.dataTracker.getDeletes());
          }
        } else {

          List<Mutation> dataAdded = new ArrayList<>();

          long generation = tctx.generationCounter.getAndIncrement();

          int rowsToGenerate = random.nextInt(1000);

          Set<Long> seen = new HashSet<>();

          for (int i = 0; i < rowsToGenerate; i++) {
            Mutation m = generateMutation(random, generation, seen);
            dataAdded.add(m);
          }

          if (random.nextInt(5) == 0) {
            stats.bulkImported += bulkImport(random, dataAdded);
          } else {
            stats.written += write(dataAdded);
          }

          // Make the data just written visible to scans. Now that the writer is closed scans should
          // be able to see the data in the table.
          tctx.dataTracker.addExpectedData(dataAdded);
        }
      }

      return stats;
    }

    private Mutation generateMutation(Random random, long generation, Set<Long> seen) {

      int cols = random.nextInt(100) + 1;

      // Ensuring every key in a generation is unique ensures there are no duplicate keys in the
      // test. The generation is unique for each data set and if the data inside a generation is
      // unique it guarantees each row is globally unique across dataset generated for the test. The
      // seen set ensures uniqueness inside the generation/dataset. The way the test works,
      // duplicates could cause false positives. Even though duplicates are highly unlikely, this
      // avoid a potential bug in the test itself that could be hard to track down if it actually
      // happened.
      var nextRow = nextLongAbs(random);
      while (!seen.add(nextRow)) {
        nextRow = nextLongAbs(random);
      }

      String row = String.format("%016x:%016x", nextRow, generation);

      Mutation m = new Mutation(row);
      for (int i = 0; i < cols; i++) {
        // The qualifiers are all unique in the row. This together with rows being globally unique
        // in the test ensures each key is globally unique in the test.
        m.put(String.valueOf(random.nextInt(10)), String.format("%04x", i), "");
      }

      return m;
    }
  }

  public static class GenerationFilter extends Filter {

    private String generation;

    @Override
    public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options,
        IteratorEnvironment env) throws IOException {
      super.init(source, options, env);
      this.generation = options.get("generation");
    }

    @Override
    public boolean accept(Key k, Value v) {
      String kgen = k.getRowData().toString().split(":")[1];
      return !generation.equals(kgen);
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
      int numMerges = 0;
      int numFilters = 0;

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
        } else if (pick < 25) {
          // 1 in 20 chance of merging
          long start = nextLongAbs(random);
          long end = nextLongAbs(random);

          while (end <= start) {
            end = nextLongAbs(random);
          }

          tctx.client.tableOperations().merge(tctx.table, new Text(String.format("%016x", start)),
              new Text(String.format("%016x", end)));
          numMerges++;
        } else if (pick < 30) {
          // 1 in 20 chance of doing a filter compaction. This compaction will delete a data set.
          var deletes = tctx.dataTracker.getDeletes();

          if (!deletes.isEmpty()) {
            // The row has the format <random long>:<generation>, the following gets the generations
            // from the rows. Expect the generation to be the same for a set of data to delete.
            String gen = deletes.stream().map(m -> new String(m.getRow(), UTF_8))
                .map(row -> row.split(":")[1]).distinct().collect(MoreCollectors.onlyElement());

            IteratorSetting iterSetting =
                new IteratorSetting(100, "genfilter", GenerationFilter.class);
            iterSetting.addOptions(Map.of("generation", gen));

            // run a compaction that deletes every key with the specified generation. Must wait on
            // the
            // compaction because at the end of the test it will try to verify deleted data is not
            // present. Must flush the table in case data to delete is still in memory.
            tctx.client.tableOperations().compact(tctx.table, new CompactionConfig().setFlush(true)
                .setWait(true).setIterators(List.of(iterSetting)));
            numFilters++;
          }
        }
      }

      return String.format(
          "Flushes:%,d Compactions:%,d Splits added:%,d Merges:%,d Filter compactions:%,d",
          numFlushes, numCompactions, numSplits, numMerges, numFilters);
    }
  }

  /**
   * @return absolute value of a random long
   */
  private static long nextLongAbs(Random random) {
    return random.nextLong() & 0x7fffffffffffffffL;
  }

  /**
   * This function was created to help debug issues with this test, leaving in case its useful in
   * the future. It finds all rfiles in a directory that contain a given row.
   */
  public static void findRow(String row, String tableDir) throws Exception {
    FileSystem fs = FileSystem.getLocal(new Configuration());

    var iter = fs.listFiles(new Path(tableDir), true);

    while (iter.hasNext()) {
      var f = iter.next();
      if (f.isFile() && f.getPath().getName().endsWith(".rf")) {
        // Calling withoutSystemIterators() disables filtering of delete keys allowing files that
        // only contain deletes for the row to be found.
        try (var scanner = RFile.newScanner().from(f.getPath().toString()).withFileSystem(fs)
            .withoutSystemIterators().build()) {
          scanner.setRange(new Range(new Text(row)));

          var siter = scanner.iterator();

          if (siter.hasNext()) {
            System.out.println("File " + f.getPath().getName());
            var e = siter.next();
            System.out.println("  " + e.getKey() + " " + e.getValue());
          }
        }
      }
    }
  }
}

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

import static org.apache.accumulo.core.util.UtilWaitThread.sleepUninterruptibly;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.time.Duration;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.ActiveScan;
import org.apache.accumulo.core.clientImpl.Namespace;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorUtil;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.accumulo.harness.AccumuloClusterHarness;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ACCUMULO-2641 Integration test. ACCUMULO-2641 Adds scan id to thrift protocol so that
 * {@code org.apache.accumulo.core.client.admin.ActiveScan.getScanid()} returns a unique scan id.
 *
 * <p>
 * The test uses the Minicluster and the {@code org.apache.accumulo.test.functional.SlowIterator} to
 * create multiple scan sessions. The test exercises multiple tablet servers with splits and
 * multiple ranges to force the scans to occur across multiple tablet servers for completeness.
 *
 * <p>
 * This patch modified thrift, the TraceRepoDeserializationTest test seems to fail unless the
 * following be added:
 *
 * <p>
 * private static final long serialVersionUID = -4659975753252858243l;
 *
 * <p>
 * back into org.apache.accumulo.trace.thrift.TInfo until that test signature is regenerated.
 */
public class ScanIdIT extends AccumuloClusterHarness {

  private static final Logger log = LoggerFactory.getLogger(ScanIdIT.class);

  private static final int NUM_SCANNERS = 8;

  private static final int NUM_DATA_ROWS = 100;

  private static final ExecutorService pool = Executors.newFixedThreadPool(NUM_SCANNERS);

  private static final AtomicBoolean testInProgress = new AtomicBoolean(true);

  private static final Map<Integer,Value> resultsByWorker = new ConcurrentHashMap<>();

  @Override
  protected Duration defaultTimeout() {
    return Duration.ofMinutes(1);
  }

  /**
   * @throws Exception any exception is a test failure.
   */
  @Test
  public void testScanId() throws Exception {

    final String tableName = getUniqueNames(1)[0];
    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
      client.tableOperations().create(tableName);

      addSplits(client, tableName);

      log.info("Splits added");

      generateSampleData(client, tableName);

      log.info("Generated data for {}", tableName);

      attachSlowIterator(client, tableName);

      CountDownLatch latch = new CountDownLatch(NUM_SCANNERS);

      List<ScannerThread> scanThreadsToClose = new ArrayList<>(NUM_SCANNERS);
      for (int scannerIndex = 0; scannerIndex < NUM_SCANNERS; scannerIndex++) {
        ScannerThread st = new ScannerThread(client, scannerIndex, tableName, latch);
        scanThreadsToClose.add(st);
        pool.execute(st);
      }

      // wait for scanners to report a result.
      while (testInProgress.get()) {

        if (resultsByWorker.size() < NUM_SCANNERS) {
          log.trace("Results reported {}", resultsByWorker.size());
          sleepUninterruptibly(750, TimeUnit.MILLISECONDS);
        } else {
          // each worker has reported at least one result.
          testInProgress.set(false);

          log.debug("Final result count {}", resultsByWorker.size());

          // delay to allow scanners to react to end of test and cleanly close.
          sleepUninterruptibly(1, TimeUnit.SECONDS);
        }

      }

      Set<Long> scanIds = getScanIds(client);
      assertTrue(scanIds.size() >= NUM_SCANNERS,
          "Expected at least " + NUM_SCANNERS + " scanIds, but saw " + scanIds.size());

      scanThreadsToClose.forEach(st -> {
        if (st.scanner != null) {
          st.scanner.close();
        }
      });

      while (!(scanIds = getScanIds(client)).isEmpty()) {
        log.debug("Waiting for active scans to stop...");
        Thread.sleep(200);
      }
      assertEquals(0, scanIds.size(), "Expected no scanIds after closing scanners");

    }
  }

  private Set<Long> getScanIds(AccumuloClient client)
      throws AccumuloSecurityException, InterruptedException, AccumuloException {
    // all scanner have reported at least 1 result, so check for unique scan ids.
    Set<Long> scanIds = new HashSet<>();

    List<String> tservers = client.instanceOperations().getTabletServers();

    log.debug("tablet servers {}", tservers);

    for (String tserver : tservers) {

      List<ActiveScan> activeScans = null;
      for (int i = 0; i < 10; i++) {
        try {
          activeScans = client.instanceOperations().getActiveScans(tserver);
          break;
        } catch (AccumuloException e) {
          if (e.getCause() instanceof TableNotFoundException) {
            log.debug("Got TableNotFoundException, will retry");
            Thread.sleep(200);
            continue;
          }
          throw e;
        }
      }

      assertNotNull(activeScans, "Repeatedly got exception trying to active scans");

      activeScans.removeIf(
          scan -> scan.getTable().startsWith(Namespace.ACCUMULO.name() + Namespace.SEPARATOR));
      log.debug("TServer {} has {} active non-metadata scans", tserver, activeScans.size());

      for (ActiveScan scan : activeScans) {
        log.debug("Tserver {} scan id {} ({})", tserver, scan.getScanid(), scan.getTable());
        scanIds.add(scan.getScanid());
      }
    }

    return scanIds;
  }

  /**
   * Runs scanner in separate thread to allow multiple scanners to execute in parallel.
   * <p>
   * The thread run method is terminated when the testInProgress flag is set to false.
   */
  private static class ScannerThread implements Runnable {

    private final AccumuloClient accumuloClient;
    private Scanner scanner = null;
    private final int workerIndex;
    private final String tablename;
    private final CountDownLatch latch;

    public ScannerThread(final AccumuloClient accumuloClient, final int workerIndex,
        final String tablename, final CountDownLatch latch) {
      this.accumuloClient = accumuloClient;
      this.workerIndex = workerIndex;
      this.tablename = tablename;
      this.latch = latch;
    }

    /**
     * execute the scan across the sample data and put scan result into result map until
     * testInProgress flag is set to false.
     */
    @Override
    public void run() {

      latch.countDown();
      try {
        latch.await();
      } catch (InterruptedException e) {
        log.error("Thread interrupted with id {}", workerIndex);
        Thread.currentThread().interrupt();
        return;
      }

      log.debug("Creating scanner in worker thread {}", workerIndex);

      try {

        scanner = accumuloClient.createScanner(tablename, new Authorizations());

        // Never start readahead
        scanner.setReadaheadThreshold(Long.MAX_VALUE);
        scanner.setBatchSize(1);

        // create different ranges to try to hit more than one tablet.
        scanner.setRange(new Range(new Text(Integer.toString(workerIndex)), new Text("9")));

        scanner.fetchColumnFamily(new Text("fam1"));

        for (Map.Entry<Key,Value> entry : scanner) {

          // exit when success condition is met.
          if (!testInProgress.get()) {
            scanner.clearScanIterators();
            return;
          }

          Text row = entry.getKey().getRow();

          log.debug("worker {}, row {}", workerIndex, row);

          if (entry.getValue() != null) {

            Value prevValue = resultsByWorker.put(workerIndex, entry.getValue());

            // value should always being increasing
            if (prevValue != null) {

              log.trace("worker {} values {}", workerIndex,
                  String.format("%1$s < %2$s", prevValue, entry.getValue()));

              assertTrue(prevValue.compareTo(entry.getValue()) > 0);
            }
          } else {
            log.info("Scanner returned null");
            fail("Scanner returned unexpected null value");
          }

        }
        log.debug("Scanner ran out of data. (info only, not an error) ");
      } catch (TableNotFoundException e) {
        throw new IllegalStateException("Initialization failure. Could not create scanner", e);
      } finally {
        // don't close scanner here, because it will clean up the scan ids we're checking for
      }
    }
  }

  /**
   * Create splits on table and force migration by taking table offline and then bring back online
   * for test.
   *
   * @param client Accumulo client to test cluster or MAC instance.
   */
  private void addSplits(final AccumuloClient client, final String tableName) {

    SortedSet<Text> splits = createSplits();

    try {

      client.tableOperations().addSplits(tableName, splits);

      client.tableOperations().offline(tableName, true);

      sleepUninterruptibly(2, TimeUnit.SECONDS);
      client.tableOperations().online(tableName, true);

      for (Text split : client.tableOperations().listSplits(tableName)) {
        log.trace("Split {}", split);
      }

    } catch (AccumuloSecurityException | AccumuloException | TableNotFoundException e) {
      throw new IllegalStateException("Initialization failed. Could not add splits to " + tableName,
          e);
    }

  }

  /**
   * Create splits to distribute data across multiple tservers.
   *
   * @return splits in sorted set for addSplits.
   */
  private SortedSet<Text> createSplits() {

    SortedSet<Text> splits = new TreeSet<>();

    for (int split = 0; split < 10; split++) {
      splits.add(new Text(Integer.toString(split)));
    }

    return splits;
  }

  /**
   * Generate some sample data using random row id to distribute across splits.
   * <p>
   * The primary goal is to determine that each scanner is assigned a unique scan id. This test does
   * check that the count value for fam1 increases if a scanner reads multiple value, but this is
   * secondary consideration for this test, that is included for completeness.
   *
   * @param accumuloClient Accumulo client to test cluster or MAC instance.
   */
  private void generateSampleData(AccumuloClient accumuloClient, final String tablename) {

    try (BatchWriter bw = accumuloClient.createBatchWriter(tablename)) {

      ColumnVisibility vis = new ColumnVisibility("public");

      for (int i = 0; i < NUM_DATA_ROWS; i++) {

        Text rowId = new Text(String.format("%d", ((random.nextInt(10) * 100) + i)));

        Mutation m = new Mutation(rowId);
        m.put("fam1", "count", Integer.toString(i));
        m.put(new Text("fam1"), new Text("positive"), vis,
            new Value(Integer.toString(NUM_DATA_ROWS - i)));
        m.put(new Text("fam1"), new Text("negative"), vis,
            new Value(Integer.toString(i - NUM_DATA_ROWS)));

        log.trace("Added row {}", rowId);

        bw.addMutation(m);
      }
    } catch (TableNotFoundException | MutationsRejectedException ex) {
      throw new IllegalStateException("Initialization failed. Could not create test data", ex);
    }
  }

  /**
   * Attach the test slow iterator so that we have time to read the scan id without creating a large
   * dataset. Uses a fairly large sleep and delay times because we are not concerned with how much
   * data is read and we do not read all of the data - the test stops once each scanner reports a
   * scan id.
   *
   * @param accumuloClient Accumulo client to test cluster or MAC instance.
   */
  private void attachSlowIterator(AccumuloClient accumuloClient, final String tablename) {
    try {

      IteratorSetting slowIter =
          new IteratorSetting(50, "slowIter", "org.apache.accumulo.test.functional.SlowIterator");
      slowIter.addOption("sleepTime", "200");
      slowIter.addOption("seekSleepTime", "200");

      accumuloClient.tableOperations().attachIterator(tablename, slowIter,
          EnumSet.of(IteratorUtil.IteratorScope.scan));

    } catch (AccumuloException | AccumuloSecurityException | TableNotFoundException ex) {
      throw new IllegalStateException("Initialization failed. Could not attach slow iterator", ex);
    }
  }

}

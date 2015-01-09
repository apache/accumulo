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
package org.apache.accumulo.test.functional;

import static com.google.common.base.Charsets.UTF_8;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.ActiveScan;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorUtil;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.accumulo.core.util.UtilWaitThread;
import org.apache.accumulo.harness.AccumuloClusterIT;
import org.apache.hadoop.io.Text;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ACCUMULO-2641 Integration test. ACCUMULO-2641 Adds scan id to thrift protocol so that {@code org.apache.accumulo.core.client.admin.ActiveScan.getScanid()}
 * returns a unique scan id.
 * <p>
 * <p/>
 * The test uses the Minicluster and the {@code org.apache.accumulo.test.functional.SlowIterator} to create multiple scan sessions. The test exercises multiple
 * tablet servers with splits and multiple ranges to force the scans to occur across multiple tablet servers for completeness.
 * <p/>
 * This patch modified thrift, the TraceRepoDeserializationTest test seems to fail unless the following be added:
 * <p/>
 * private static final long serialVersionUID = -4659975753252858243l;
 * <p/>
 * back into org.apache.accumulo.trace.thrift.TInfo until that test signature is regenerated.
 */
public class ScanIdIT extends AccumuloClusterIT {

  private static final Logger log = LoggerFactory.getLogger(ScanIdIT.class);

  private static final int NUM_SCANNERS = 8;

  private static final int NUM_DATA_ROWS = 100;

  private static final Random random = new Random();

  private static final ExecutorService pool = Executors.newFixedThreadPool(NUM_SCANNERS);

  private static volatile boolean testInProgress = true;

  private static final Map<Integer,Value> resultsByWorker = new ConcurrentHashMap<Integer,Value>();

  @Override
  protected int defaultTimeoutSeconds() {
    return 60;
  }

  /**
   * @throws Exception
   *           any exception is a test failure.
   */
  @Test
  public void testScanId() throws Exception {

    final String tableName = getUniqueNames(1)[0];
    Connector conn = getConnector();
    conn.tableOperations().create(tableName);

    addSplits(conn, tableName);

    generateSampleData(conn, tableName);

    attachSlowIterator(conn, tableName);

    for (int scannerIndex = 0; scannerIndex < NUM_SCANNERS; scannerIndex++) {
      ScannerThread st = new ScannerThread(conn, scannerIndex, tableName);
      pool.submit(st);
    }

    // wait for scanners to report a result.
    while (testInProgress) {

      if (resultsByWorker.size() < NUM_SCANNERS) {
        log.trace("Results reported {}", resultsByWorker.size());
        UtilWaitThread.sleep(750);
      } else {
        // each worker has reported at least one result.
        testInProgress = false;

        log.debug("Final result count {}", resultsByWorker.size());

        // delay to allow scanners to react to end of test and cleanly close.
        UtilWaitThread.sleep(1000);
      }

    }

    // all scanner have reported at least 1 result, so check for unique scan ids.
    Set<Long> scanIds = new HashSet<Long>();

    List<String> tservers = conn.instanceOperations().getTabletServers();

    log.debug("tablet servers {}", tservers.toString());

    for (String tserver : tservers) {

      List<ActiveScan> activeScans = conn.instanceOperations().getActiveScans(tserver);

      log.debug("TServer {} has {} active scans", tserver, activeScans.size());

      for (ActiveScan scan : activeScans) {
        log.debug("Tserver {} scan id {}", tserver, scan.getScanid());
        scanIds.add(scan.getScanid());
      }
    }

    assertTrue(NUM_SCANNERS <= scanIds.size());

  }

  /**
   * Runs scanner in separate thread to allow multiple scanners to execute in parallel.
   * <p/>
   * The thread run method is terminated when the testInProgress flag is set to false.
   */
  private static class ScannerThread implements Runnable {

    private final Connector connector;
    private Scanner scanner = null;
    private final int workerIndex;
    private final String tablename;

    public ScannerThread(final Connector connector, final int workerIndex, final String tablename) {

      this.connector = connector;
      this.workerIndex = workerIndex;
      this.tablename = tablename;

    }

    /**
     * execute the scan across the sample data and put scan result into result map until testInProgress flag is set to false.
     */
    @Override
    public void run() {

      /*
       * set random initial delay of up to to allow scanners to proceed to different points.
       */

      long delay = random.nextInt(5000);

      log.trace("Start delay for worker thread {} is {}", workerIndex, delay);

      UtilWaitThread.sleep(delay);

      try {

        scanner = connector.createScanner(tablename, new Authorizations());

        // Never start readahead
        scanner.setReadaheadThreshold(Long.MAX_VALUE);
        scanner.setBatchSize(1);

        // create different ranges to try to hit more than one tablet.
        scanner.setRange(new Range(new Text(Integer.toString(workerIndex)), new Text("9")));

      } catch (TableNotFoundException e) {
        throw new IllegalStateException("Initialization failure. Could not create scanner", e);
      }

      scanner.fetchColumnFamily(new Text("fam1"));

      for (Map.Entry<Key,Value> entry : scanner) {

        // exit when success condition is met.
        if (!testInProgress) {
          scanner.clearScanIterators();
          scanner.close();

          return;
        }

        Text row = entry.getKey().getRow();

        log.trace("worker {}, row {}", workerIndex, row.toString());

        if (entry.getValue() != null) {

          Value prevValue = resultsByWorker.put(workerIndex, entry.getValue());

          // value should always being increasing
          if (prevValue != null) {

            log.trace("worker {} values {}", workerIndex, String.format("%1$s < %2$s", prevValue, entry.getValue()));

            assertTrue(prevValue.compareTo(entry.getValue()) > 0);
          }
        } else {
          log.info("Scanner returned null");
          fail("Scanner returned unexpected null value");
        }

      }

      log.debug("Scanner ran out of data. (info only, not an error) ");

    }
  }

  /**
   * Create splits on table and force migration by taking table offline and then bring back online for test.
   *
   * @param conn
   *          Accumulo connector Accumulo connector to test cluster or MAC instance.
   */
  private void addSplits(final Connector conn, final String tableName) {

    SortedSet<Text> splits = createSplits();

    try {

      conn.tableOperations().addSplits(tableName, splits);

      conn.tableOperations().offline(tableName, true);

      UtilWaitThread.sleep(2000);
      conn.tableOperations().online(tableName, true);

      for (Text split : conn.tableOperations().listSplits(tableName)) {
        log.trace("Split {}", split);
      }

    } catch (AccumuloSecurityException e) {
      throw new IllegalStateException("Initialization failed. Could not add splits to " + tableName, e);
    } catch (TableNotFoundException e) {
      throw new IllegalStateException("Initialization failed. Could not add splits to " + tableName, e);
    } catch (AccumuloException e) {
      throw new IllegalStateException("Initialization failed. Could not add splits to " + tableName, e);
    }

  }

  /**
   * Create splits to distribute data across multiple tservers.
   *
   * @return splits in sorted set for addSplits.
   */
  private SortedSet<Text> createSplits() {

    SortedSet<Text> splits = new TreeSet<Text>();

    for (int split = 0; split < 10; split++) {
      splits.add(new Text(Integer.toString(split)));
    }

    return splits;
  }

  /**
   * Generate some sample data using random row id to distribute across splits.
   * <p/>
   * The primary goal is to determine that each scanner is assigned a unique scan id. This test does check that the count value for fam1 increases if a scanner
   * reads multiple value, but this is secondary consideration for this test, that is included for completeness.
   *
   * @param connector
   *          Accumulo connector Accumulo connector to test cluster or MAC instance.
   */
  private void generateSampleData(Connector connector, final String tablename) {

    try {

      BatchWriter bw = connector.createBatchWriter(tablename, new BatchWriterConfig());

      ColumnVisibility vis = new ColumnVisibility("public");

      for (int i = 0; i < NUM_DATA_ROWS; i++) {

        Text rowId = new Text(String.format("%d", ((random.nextInt(10) * 100) + i)));

        Mutation m = new Mutation(rowId);
        m.put(new Text("fam1"), new Text("count"), new Value(Integer.toString(i).getBytes(UTF_8)));
        m.put(new Text("fam1"), new Text("positive"), vis, new Value(Integer.toString(NUM_DATA_ROWS - i).getBytes(UTF_8)));
        m.put(new Text("fam1"), new Text("negative"), vis, new Value(Integer.toString(i - NUM_DATA_ROWS).getBytes(UTF_8)));

        log.trace("Added row {}", rowId);

        bw.addMutation(m);
      }

      bw.close();
    } catch (TableNotFoundException ex) {
      throw new IllegalStateException("Initialization failed. Could not create test data", ex);
    } catch (MutationsRejectedException ex) {
      throw new IllegalStateException("Initialization failed. Could not create test data", ex);
    }
  }

  /**
   * Attach the test slow iterator so that we have time to read the scan id without creating a large dataset. Uses a fairly large sleep and delay times because
   * we are not concerned with how much data is read and we do not read all of the data - the test stops once each scanner reports a scan id.
   *
   * @param connector
   *          Accumulo connector Accumulo connector to test cluster or MAC instance.
   */
  private void attachSlowIterator(Connector connector, final String tablename) {
    try {

      IteratorSetting slowIter = new IteratorSetting(50, "slowIter", "org.apache.accumulo.test.functional.SlowIterator");
      slowIter.addOption("sleepTime", "200");
      slowIter.addOption("seekSleepTime", "200");

      connector.tableOperations().attachIterator(tablename, slowIter, EnumSet.of(IteratorUtil.IteratorScope.scan));

    } catch (AccumuloException ex) {
      throw new IllegalStateException("Initialization failed. Could not attach slow iterator", ex);
    } catch (TableNotFoundException ex) {
      throw new IllegalStateException("Initialization failed. Could not attach slow iterator", ex);
    } catch (AccumuloSecurityException ex) {
      throw new IllegalStateException("Initialization failed. Could not attach slow iterator", ex);
    }
  }

}

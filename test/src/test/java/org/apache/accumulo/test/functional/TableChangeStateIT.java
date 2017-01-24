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

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.impl.Tables;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.master.state.tables.TableState;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.zookeeper.ZooUtil;
import org.apache.accumulo.fate.zookeeper.ZooCache;
import org.apache.accumulo.fate.zookeeper.ZooReader;
import org.apache.accumulo.harness.AccumuloClusterIT;
import org.apache.hadoop.io.Text;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * ACCUMULO-4574. Test to verify that changing table state to online / offline {@link org.apache.accumulo.core.client.admin.TableOperations#online(String)} when
 * the table is already in that state returns without blocking.
 */
public class TableChangeStateIT extends AccumuloClusterIT {

  private static final Logger log = LoggerFactory.getLogger(TableChangeStateIT.class);

  private static final int NUM_ROWS = 1000;
  private static final long SLOW_SCAN_SLEEP_MS = 100L;

  @Override
  protected int defaultTimeoutSeconds() {
    return 4 * 60;
  }

  /**
   * Validate that {@code TableOperations} online operation does not block when table is already online and fate transaction lock is held by other operations.
   * The test creates, populates a table and then runs a compaction with a slow iterator so that operation takes long enough to simulate the condition. After
   * the online operation while compaction is running completes, the test is complete and the compaction is canceled so that other tests can run.
   *
   * @throws Exception
   *           any exception is a test failure.
   */
  @Test
  public void changeTableStateTest() throws Exception {

    ExecutorService pool = Executors.newCachedThreadPool();

    Connector connector = getConnector();
    String tableName = getUniqueNames(1)[0];

    createData(tableName);

    assertEquals("verify table online after created", TableState.ONLINE, getTableState(connector, tableName));

    OnLineCallable onlineOp = new OnLineCallable(tableName);

    Future<OnlineOpTiming> task = pool.submit(onlineOp);

    OnlineOpTiming timing1 = task.get();

    log.trace("Online 1 in {} ms", TimeUnit.MILLISECONDS.convert(timing1.runningTime(), TimeUnit.NANOSECONDS));

    assertEquals("verify table is still online", TableState.ONLINE, getTableState(connector, tableName));

    // verify that offline then online functions as expected.

    connector.tableOperations().offline(tableName, true);
    assertEquals("verify table is offline", TableState.OFFLINE, getTableState(connector, tableName));

    onlineOp = new OnLineCallable(tableName);

    task = pool.submit(onlineOp);

    OnlineOpTiming timing2 = task.get();

    log.trace("Online 2 in {} ms", TimeUnit.MILLISECONDS.convert(timing2.runningTime(), TimeUnit.NANOSECONDS));

    assertEquals("verify table is back online", TableState.ONLINE, getTableState(connector, tableName));

    // launch a full table compaction with the slow iterator to ensure table lock is acquired and held by the compaction

    Future compactTask = pool.submit(new SlowCompactionRunner(tableName));
    assertTrue("verify that compaction running and fate transaction exists", blockUntilCompactionRunning());

    // try to set online while fate transaction is in progress - before ACCUMULO-4574 this would block

    onlineOp = new OnLineCallable(tableName);

    task = pool.submit(onlineOp);

    OnlineOpTiming timing3 = task.get();

    assertTrue("online should take less time than expected compaction time",
        timing3.runningTime() < TimeUnit.NANOSECONDS.convert(NUM_ROWS * SLOW_SCAN_SLEEP_MS, TimeUnit.MILLISECONDS));

    assertEquals("verify table is still online", TableState.ONLINE, getTableState(connector, tableName));

    assertTrue("verify compaction still running and fate transaction still exists", blockUntilCompactionRunning());

    // test complete, cancel compaction and move on.
    connector.tableOperations().cancelCompaction(tableName);

    log.debug("Success: Timing results for online commands.");
    log.debug("Time for unblocked online {} ms", TimeUnit.MILLISECONDS.convert(timing1.runningTime(), TimeUnit.NANOSECONDS));
    log.debug("Time for online when offline {} ms", TimeUnit.MILLISECONDS.convert(timing2.runningTime(), TimeUnit.NANOSECONDS));
    log.debug("Time for blocked online {} ms", TimeUnit.MILLISECONDS.convert(timing3.runningTime(), TimeUnit.NANOSECONDS));

    // block if compaction still running
    compactTask.get();

  }

  /**
   * Blocks current thread until compaction is running.
   *
   * @return true if compaction and associate fate found.
   */
  private boolean blockUntilCompactionRunning() {

    try {

      Connector connector = getConnector();

      int runningCompactions = 0;

      List<String> tservers = connector.instanceOperations().getTabletServers();

      /*
       * wait for compaction to start - The compaction will acquire a fate transaction lock that used to block a subsequent online command while the fate
       * transaction lock was held.
       */
      while (runningCompactions == 0) {

        try {

          for (String tserver : tservers) {
            runningCompactions += connector.instanceOperations().getActiveCompactions(tserver).size();
            log.trace("tserver {}, running compactions {}", tservers, runningCompactions);
          }

        } catch (AccumuloSecurityException | AccumuloException ex) {
          throw new IllegalStateException("failed to get active compactions, test fails.", ex);
        }

        try {
          Thread.sleep(250);
        } catch (InterruptedException ex) {
          // reassert interrupt
          Thread.currentThread().interrupt();
        }
      }

      // Validate that there is a compaction fate transaction - otherwise test is invalid.
      return findFate();

    } catch (AccumuloSecurityException | AccumuloException ex) {
      throw new IllegalStateException("test failed waiting for compaction to start", ex);
    }
  }

  /**
   * Checks fates in zookeeper looking for transaction associated with a compaction as a double check that the test will be valid because the running compaction
   * does have a fate transaction lock.
   *
   * @return true if corresponding fate transaction found, false otherwise
   * @throws AccumuloException
   *           from {@link ConfigurableMacIT#getConnector}
   * @throws AccumuloSecurityException
   *           from {@link ConfigurableMacIT#getConnector}
   */
  private boolean findFate() throws AccumuloException, AccumuloSecurityException {

    Connector connector = getConnector();

    String zPath = ZooUtil.getRoot(connector.getInstance()) + Constants.ZFATE;
    ZooReader zooReader = new ZooReader(connector.getInstance().getZooKeepers(), connector.getInstance().getZooKeepersSessionTimeOut());
    ZooCache zooCache = new ZooCache(zooReader, null);

    List<String> lockedIds = zooCache.getChildren(zPath);

    for (String fateId : lockedIds) {

      List<String> lockNodes = zooCache.getChildren(zPath + "/" + fateId);
      lockNodes = new ArrayList<>(lockNodes);
      Collections.sort(lockNodes);

      for (String node : lockNodes) {

        byte[] data = zooCache.get(zPath + "/" + fateId + "/" + node);
        String lda[] = new String(data, UTF_8).split(":");

        for (String fateString : lda) {

          log.trace("Lock: {}:{}: {}", fateId, node, lda);

          if (node.contains("prop_debug") && fateString.contains("CompactRange")) {
            // found fate associated with table compaction.
            log.trace("FOUND - Lock: {}:{}: {}", fateId, node, lda);
            return Boolean.TRUE;
          }
        }
      }
    }

    // did not find appropriate fate transaction for compaction.
    return Boolean.FALSE;
  }

  /**
   * Returns the current table state (ONLINE, OFFLINE,...) of named table.
   *
   * @param connector
   *          connector to Accumulo instance
   * @param tableName
   *          the table name
   * @return the current table state
   * @throws TableNotFoundException
   *           if table does not exist
   */
  private TableState getTableState(Connector connector, String tableName) throws TableNotFoundException {

    String tableId = Tables.getTableId(connector.getInstance(), tableName);

    TableState tstate = Tables.getTableState(connector.getInstance(), tableId);

    log.trace("tableName: '{}': tableId {}, current state: {}", tableName, tableId, tstate);

    return tstate;
  }

  /**
   * Create the provided table and populate with some data using a batch writer. The table is scanned to ensure it was populated as expected.
   *
   * @param tableName
   *          the name of the table
   */
  private void createData(final String tableName) {

    try {

      Connector connector = getConnector();

      // create table.
      connector.tableOperations().create(tableName);
      BatchWriter bw = connector.createBatchWriter(tableName, new BatchWriterConfig());

      // populate
      for (int i = 0; i < NUM_ROWS; i++) {
        Mutation m = new Mutation(new Text(String.format("%05d", i)));
        m.put(new Text("col" + Integer.toString((i % 3) + 1)), new Text("qual"), new Value("junk".getBytes(UTF_8)));
        bw.addMutation(m);
      }
      bw.close();

      long startTimestamp = System.nanoTime();

      Scanner scanner = connector.createScanner(tableName, Authorizations.EMPTY);
      int count = 0;
      for (Map.Entry<Key,Value> elt : scanner) {
        String expected = String.format("%05d", count);
        assert (elt.getKey().getRow().toString().equals(expected));
        count++;
      }

      log.trace("Scan time for {} rows {} ms", NUM_ROWS, TimeUnit.MILLISECONDS.convert((System.nanoTime() - startTimestamp), TimeUnit.NANOSECONDS));

      scanner.close();

      if (count != NUM_ROWS) {
        throw new IllegalStateException(String.format("Number of rows %1$d does not match expected %2$d", count, NUM_ROWS));
      }
    } catch (AccumuloException | AccumuloSecurityException | TableNotFoundException | TableExistsException ex) {
      throw new IllegalStateException("Create data failed with exception", ex);
    }
  }

  /**
   * Provides timing information for oline operation.
   */
  private static class OnlineOpTiming {

    private long started = 0L;
    private long completed = 0L;

    OnlineOpTiming() {
      started = System.nanoTime();
    }

    /**
     * stop timing and set completion flag.
     */
    void setComplete() {
      completed = System.nanoTime();
    }

    /**
     * @return running time in nanoseconds.
     */
    long runningTime() {
      return completed - started;
    }
  }

  /**
   * Run online operation in a separate thread and gather timing information.
   */
  private class OnLineCallable implements Callable<OnlineOpTiming> {

    final String tableName;

    /**
     * Create an instance of this class to set the provided table online.
     *
     * @param tableName
     *          The table name that will be set online.
     */
    OnLineCallable(final String tableName) {
      this.tableName = tableName;
    }

    @Override
    public OnlineOpTiming call() throws Exception {

      OnlineOpTiming status = new OnlineOpTiming();

      log.trace("Setting {} online", tableName);

      getConnector().tableOperations().online(tableName, true);

      // stop timing
      status.setComplete();

      log.trace("Online completed in {} ms", TimeUnit.MILLISECONDS.convert(status.runningTime(), TimeUnit.NANOSECONDS));

      return status;
    }
  }

  /**
   * Instance to create / run a compaction using a slow iterator.
   */
  private class SlowCompactionRunner implements Runnable {

    private final String tableName;

    /**
     * Create an instance of this class.
     *
     * @param tableName
     *          the name of the table that will be compacted with the slow iterator.
     */
    SlowCompactionRunner(final String tableName) {
      this.tableName = tableName;
    }

    @Override
    public void run() {

      long startTimestamp = System.nanoTime();

      IteratorSetting slow = new IteratorSetting(30, "slow", SlowIterator.class);
      SlowIterator.setSleepTime(slow, SLOW_SCAN_SLEEP_MS);

      List<IteratorSetting> compactIterators = new ArrayList<>();
      compactIterators.add(slow);

      log.trace("Slow iterator {}", slow.toString());

      try {

        Connector connector = getConnector();

        log.trace("Start compaction");

        connector.tableOperations().compact(tableName, new Text("0"), new Text("z"), compactIterators, true, true);

        log.trace("Compaction wait is complete");

        log.trace("Slow compaction of {} rows took {} ms", NUM_ROWS, TimeUnit.MILLISECONDS.convert((System.nanoTime() - startTimestamp), TimeUnit.NANOSECONDS));

        // validate that number of rows matches expected.

        startTimestamp = System.nanoTime();

        // validate expected data created and exists in table.

        Scanner scanner = connector.createScanner(tableName, Authorizations.EMPTY);

        int count = 0;
        for (Map.Entry<Key,Value> elt : scanner) {
          String expected = String.format("%05d", count);
          assert (elt.getKey().getRow().toString().equals(expected));
          count++;
        }

        log.trace("After compaction, scan time for {} rows {} ms", NUM_ROWS,
            TimeUnit.MILLISECONDS.convert((System.nanoTime() - startTimestamp), TimeUnit.NANOSECONDS));

        if (count != NUM_ROWS) {
          throw new IllegalStateException(String.format("After compaction, number of rows %1$d does not match expected %2$d", count, NUM_ROWS));
        }

      } catch (TableNotFoundException ex) {
        throw new IllegalStateException("test failed, table " + tableName + " does not exist", ex);
      } catch (AccumuloSecurityException ex) {
        throw new IllegalStateException("test failed, could not add iterator due to security exception", ex);
      } catch (AccumuloException ex) {
        // test cancels compaction on complete, so ignore it as an exception.
        if (!ex.getMessage().contains("Compaction canceled")) {
          throw new IllegalStateException("test failed with an Accumulo exception", ex);
        }
      }
    }
  }
}

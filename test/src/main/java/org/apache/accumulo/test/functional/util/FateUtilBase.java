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
package org.apache.accumulo.test.functional.util;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.impl.Tables;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.master.state.tables.TableState;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.zookeeper.ZooUtil;
import org.apache.accumulo.fate.AdminUtil;
import org.apache.accumulo.fate.ZooStore;
import org.apache.accumulo.fate.zookeeper.IZooReaderWriter;
import org.apache.accumulo.harness.AccumuloClusterHarness;
import org.apache.accumulo.server.zookeeper.ZooReaderWriterFactory;
import org.apache.accumulo.test.functional.SlowIterator;
import org.apache.hadoop.io.Text;
import org.apache.zookeeper.KeeperException;
import org.junit.AfterClass;
import org.junit.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Base utility to provide common functions to run "slow" FATE transactions that is common to
 * multiple ITs.
 */
public class FateUtilBase extends AccumuloClusterHarness {

  private static final Logger log = LoggerFactory.getLogger(FateUtilBase.class);

  private static final int NUM_ROWS = 1000;
  private static final long SLOW_SCAN_SLEEP_MS = 250L;

  protected Connector connector;

  private static final ExecutorService pool = Executors.newCachedThreadPool();

  protected String tableName;

  protected String secret;

  @Before
  public void setup() {

    connector = getConnector();

    tableName = getUniqueNames(1)[0];

    secret = cluster.getSiteConfiguration().get(Property.INSTANCE_SECRET);

    createData(tableName);
  }

  @AfterClass
  public static void cleanup() {
    pool.shutdownNow();
  }

  @Override
  protected int defaultTimeoutSeconds() {
    return 4 * 60;
  }

  /**
   * Create and run a slow running compaction task. The method will block until the compaction has
   * been started.
   *
   * @return a reference to the running compaction task.
   */
  protected Future<?> startCompactTask() {
    Future<?> compactTask = pool.submit(new SlowCompactionRunner(tableName));
    assertTrue("verify that compaction running and fate transaction exists",
        blockUntilCompactionRunning(tableName));
    return compactTask;
  }

  /**
   * Blocks current thread until compaction is running.
   *
   * @return true if compaction and associate fate found.
   */
  protected boolean blockUntilCompactionRunning(final String tableName) {

    int runningCompactions = 0;

    List<String> tservers = connector.instanceOperations().getTabletServers();

    /*
     * wait for compaction to start - The compaction will acquire a fate transaction lock that used
     * to block a subsequent online command while the fate transaction lock was held.
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
    return findFate(tableName);
  }

  /**
   * Checks fates in zookeeper looking for transaction associated with a compaction as a double
   * check that the test will be valid because the running compaction does have a fate transaction
   * lock.
   *
   * @return true if corresponding fate transaction found, false otherwise
   */
  protected boolean findFate(final String tableName) {

    Instance instance = connector.getInstance();
    AdminUtil<String> admin = new AdminUtil<>(false);

    try {

      String tableId = Tables.getTableId(instance, tableName);

      log.trace("tid: {}", tableId);

      IZooReaderWriter zk = new ZooReaderWriterFactory().getZooReaderWriter(
          instance.getZooKeepers(), instance.getZooKeepersSessionTimeOut(), secret);
      ZooStore<String> zs = new ZooStore<>(ZooUtil.getRoot(instance) + Constants.ZFATE, zk);
      AdminUtil.FateStatus fateStatus = admin.getStatus(zs, zk,
          ZooUtil.getRoot(instance) + Constants.ZTABLE_LOCKS + "/" + tableId, null, null);

      log.trace("current fates: {}", fateStatus.getTransactions().size());

      for (AdminUtil.TransactionStatus tx : fateStatus.getTransactions()) {

        log.trace("Fate id: {}, status: {}", tx.getTxid(), tx.getStatus());

        if (tx.getTop().contains("CompactionDriver") && tx.getDebug().contains("CompactRange")) {
          return true;
        }
      }

    } catch (KeeperException | TableNotFoundException | InterruptedException ex) {
      throw new IllegalStateException(ex);
    }

    // did not find appropriate fate transaction for compaction.
    return Boolean.FALSE;
  }

  /**
   * Returns the current table state (ONLINE, OFFLINE,...) of named table.
   *
   * @param tableName
   *          the table name
   * @return the current table state
   * @throws TableNotFoundException
   *           if table does not exist
   */
  protected TableState getTableState(String tableName) throws TableNotFoundException {

    String tableId = Tables.getTableId(connector.getInstance(), tableName);

    TableState tstate = Tables.getTableState(connector.getInstance(), tableId);

    log.trace("tableName: '{}': tableId {}, current state: {}", tableName, tableId, tstate);

    return tstate;
  }

  /**
   * Create the provided table and populate with some data using a batch writer. The table is
   * scanned to ensure it was populated as expected.
   *
   * @param tableName
   *          the name of the table
   */
  private void createData(final String tableName) {

    try {

      // create table.
      connector.tableOperations().create(tableName);
      BatchWriter bw = connector.createBatchWriter(tableName, new BatchWriterConfig());

      // populate
      for (int i = 0; i < NUM_ROWS; i++) {
        Mutation m = new Mutation(new Text(String.format("%05d", i)));
        m.put(new Text("col" + ((i % 3) + 1)), new Text("qual"), new Value("junk".getBytes(UTF_8)));
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

      log.trace("Scan time for {} rows {} ms", NUM_ROWS, TimeUnit.MILLISECONDS
          .convert((System.nanoTime() - startTimestamp), TimeUnit.NANOSECONDS));

      scanner.close();

      if (count != NUM_ROWS) {
        throw new IllegalStateException(
            String.format("Number of rows %1$d does not match expected %2$d", count, NUM_ROWS));
      }
    } catch (AccumuloException | AccumuloSecurityException | TableNotFoundException
        | TableExistsException ex) {
      throw new IllegalStateException("Create data failed with exception", ex);
    }
  }

  /**
   * Provides timing information for online operation.
   */
  private static class OnlineOpTiming {

    private final long started;
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

      connector.tableOperations().online(tableName, true);
      // stop timing
      status.setComplete();

      log.trace("Online completed in {} ms",
          TimeUnit.MILLISECONDS.convert(status.runningTime(), TimeUnit.NANOSECONDS));

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

        log.trace("Start compaction");

        connector.tableOperations().compact(tableName, new Text("0"), new Text("z"),
            compactIterators, true, true);

        log.trace("Compaction wait is complete");

        log.trace("Slow compaction of {} rows took {} ms", NUM_ROWS, TimeUnit.MILLISECONDS
            .convert((System.nanoTime() - startTimestamp), TimeUnit.NANOSECONDS));

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

        log.trace("After compaction, scan time for {} rows {} ms", NUM_ROWS, TimeUnit.MILLISECONDS
            .convert((System.nanoTime() - startTimestamp), TimeUnit.NANOSECONDS));

        if (count != NUM_ROWS) {
          throw new IllegalStateException(
              String.format("After compaction, number of rows %1$d does not match expected %2$d",
                  count, NUM_ROWS));
        }

      } catch (TableNotFoundException ex) {
        throw new IllegalStateException("test failed, table " + tableName + " does not exist", ex);
      } catch (AccumuloSecurityException ex) {
        throw new IllegalStateException(
            "test failed, could not add iterator due to security exception", ex);
      } catch (AccumuloException ex) {
        // test cancels compaction on complete, so ignore it as an exception.
        if (!ex.getMessage().contains("Compaction canceled")) {
          throw new IllegalStateException("test failed with an Accumulo exception", ex);
        }
      }
    }
  }
}

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
package org.apache.accumulo.test.util;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.ActiveCompaction;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.test.functional.SlowIterator;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Common methods for performing operations that are deliberately take some period of time so that
 * tests can interact while the operations are in progress.
 */
public class SlowOps {

  private static final Logger log = LoggerFactory.getLogger(SlowOps.class);

  private static final String TSERVER_COMPACTION_MAJOR_CONCURRENT_MAX =
      "tserver.compaction.major.concurrent.max";

  private static final long SLOW_SCAN_SLEEP_MS = 250L;
  private static final int NUM_DATA_ROWS = 1000;

  private final Connector connector;
  private final String tableName;
  private final long maxWait;

  // private final int numRows = DEFAULT_NUM_DATA_ROWS;

  private static final ExecutorService pool = Executors.newCachedThreadPool();

  private Future<?> compactTask = null;

  private SlowOps(final Connector connector, final String tableName, final long maxWait) {

    this.connector = connector;
    this.tableName = tableName;
    this.maxWait = maxWait;

    createData();
  }

  public SlowOps(final Connector connector, final String tableName, final long maxWait,
      final int numParallelExpected) {

    this(connector, tableName, maxWait);

    setExpectedCompactions(numParallelExpected);

  }

  public void setExpectedCompactions(final int numParallelExpected) {

    final int target = numParallelExpected + 1;

    Map<String,String> sysConfig;

    try {

      sysConfig = connector.instanceOperations().getSystemConfiguration();

      int current = Integer.parseInt(sysConfig.get("tserver.compaction.major.concurrent.max"));

      if (current < target) {
        connector.instanceOperations().setProperty(TSERVER_COMPACTION_MAJOR_CONCURRENT_MAX,
            Integer.toString(target));

        sysConfig = connector.instanceOperations().getSystemConfiguration();

      }

      Integer.parseInt(sysConfig.get(TSERVER_COMPACTION_MAJOR_CONCURRENT_MAX));

    } catch (AccumuloException | AccumuloSecurityException | NumberFormatException ex) {
      throw new IllegalStateException("Could not set parallel compaction limit to " + target, ex);
    }
  }

  public String getTableName() {
    return tableName;
  }

  private void createData() {

    try {

      // create table.
      connector.tableOperations().create(tableName);

      log.info("Created table id: {}, name \'{}\'",
          connector.tableOperations().tableIdMap().get(tableName), tableName);

      try (BatchWriter bw = connector.createBatchWriter(tableName, new BatchWriterConfig())) {
        // populate
        for (int i = 0; i < NUM_DATA_ROWS; i++) {
          Mutation m = new Mutation(new Text(String.format("%05d", i)));
          m.put(new Text("col" + ((i % 3) + 1)), new Text("qual"),
              new Value("junk".getBytes(UTF_8)));
          bw.addMutation(m);
        }
      }

      verifyRows();

    } catch (AccumuloException | AccumuloSecurityException | TableNotFoundException
        | TableExistsException ex) {
      throw new IllegalStateException("Create data failed with exception", ex);
    }
  }

  private void verifyRows() {

    long startTimestamp = System.nanoTime();

    int count = scanCount();

    log.trace("Scan time for {} rows {} ms", NUM_DATA_ROWS,
        TimeUnit.MILLISECONDS.convert((System.nanoTime() - startTimestamp), TimeUnit.NANOSECONDS));

    if (count != NUM_DATA_ROWS) {
      throw new IllegalStateException(
          String.format("Number of rows %1$d does not match expected %2$d", count, NUM_DATA_ROWS));
    }
  }

  private int scanCount() {
    try (Scanner scanner = connector.createScanner(tableName, Authorizations.EMPTY)) {

      int count = 0;

      for (Map.Entry<Key,Value> elt : scanner) {
        String expected = String.format("%05d", count);
        assert (elt.getKey().getRow().toString().equals(expected));
        count++;
      }
      return count;
    } catch (TableNotFoundException ex) {
      log.debug("cannot verify row count, table \'{}\' does not exist", tableName);
      throw new IllegalStateException(ex);
    }
  }

  /**
   * Create and run a slow running compaction task. The method will block until the compaction has
   * been started. The compaction should be cancelled using Accumulo tableOps, and then the caller
   * can use blockWhileCompactionRunning() on the instance of this class.
   */
  public void startCompactTask() {

    compactTask = pool.submit(new SlowCompactionRunner());

    if (!blockUntilCompactionRunning()) {
      throw new IllegalStateException("Compaction could not be started for " + tableName);
    }
  }

  /**
   * Instance to create / run a compaction using a slow iterator.
   */
  private class SlowCompactionRunner implements Runnable {

    SlowCompactionRunner() {}

    @Override
    public void run() {

      long startTimestamp = System.nanoTime();

      IteratorSetting slow = new IteratorSetting(30, "slow", SlowIterator.class);
      SlowIterator.setSleepTime(slow, SLOW_SCAN_SLEEP_MS);

      List<IteratorSetting> compactIterators = new ArrayList<>();
      compactIterators.add(slow);

      log.trace("Starting slow operation using iterator: {}", slow);

      int retry = 0;
      boolean completed = false;

      while (!completed && retry++ < 5) {

        try {
          log.info("Starting compaction.  Attempt {}", retry);
          connector.tableOperations().compact(tableName, null, null, compactIterators, true, true);
          completed = true;
        } catch (Throwable ex) {
          // test cancels compaction on complete, so ignore it as an exception.
          if (ex.getMessage().contains("Compaction canceled")) {
            return;
          }
          log.info("Exception thrown while waiting for compaction - will retry", ex);
          try {
            Thread.sleep(10_000 * retry);
          } catch (InterruptedException iex) {
            Thread.currentThread().interrupt();
            return;
          }
        }
      }
      log.debug("Compaction wait is complete");

      log.trace("Slow compaction of {} rows took {} ms", NUM_DATA_ROWS, TimeUnit.MILLISECONDS
          .convert((System.nanoTime() - startTimestamp), TimeUnit.NANOSECONDS));

      // validate that number of rows matches expected.

      startTimestamp = System.nanoTime();

      // validate expected data created and exists in table.

      int count = scanCount();

      log.trace("After compaction, scan time for {} rows {} ms", NUM_DATA_ROWS,
          TimeUnit.MILLISECONDS.convert((System.nanoTime() - startTimestamp),
              TimeUnit.NANOSECONDS));

      if (count != NUM_DATA_ROWS) {
        throw new IllegalStateException(
            String.format("After compaction, number of rows %1$d does not match expected %2$d",
                count, NUM_DATA_ROWS));
      }
    }
  }

  /**
   * Blocks current thread until compaction is running.
   *
   * @return true if compaction and associate fate found.
   */
  private boolean blockUntilCompactionRunning() {

    long startWait = System.currentTimeMillis();

    List<String> tservers = connector.instanceOperations().getTabletServers();

    /*
     * wait for compaction to start on table - The compaction will acquire a fate transaction lock
     * that used to block a subsequent online command while the fate transaction lock was held.
     */
    while (System.currentTimeMillis() < (startWait + maxWait)) {

      try {

        List<ActiveCompaction> activeCompactions = new ArrayList<>();

        for (String tserver : tservers) {
          List<ActiveCompaction> ac = connector.instanceOperations().getActiveCompactions(tserver);
          activeCompactions.addAll(ac);
          // runningCompactions += ac.size();
          log.trace("tserver {}, running compactions {}", tservers, ac.size());
        }

        if (!activeCompactions.isEmpty()) {
          try {
            for (ActiveCompaction compaction : activeCompactions) {
              log.debug("Compaction running for {}", compaction.getTable());
              if (compaction.getTable().compareTo(tableName) == 0) {
                return true;
              }
            }
          } catch (TableNotFoundException ex) {
            log.trace("Compaction found for unknown table {}", activeCompactions);
          }
        }
      } catch (AccumuloSecurityException | AccumuloException ex) {
        throw new IllegalStateException("failed to get active compactions, test fails.", ex);
      }

      try {
        Thread.sleep(3_000);
      } catch (InterruptedException ex) {
        // reassert interrupt
        Thread.currentThread().interrupt();
      }
    }

    log.debug("Could not find compaction for {} after {} seconds", tableName,
        TimeUnit.MILLISECONDS.toSeconds(maxWait));

    return false;

  }

  /**
   * Will block as long as the underlying compaction task is running. This method is intended to be
   * used when the the compaction is cancelled via table operation cancel method - when the cancel
   * command completed, the running task will terminate and then this method will return.
   *
   * @return true if the task returned.
   */
  public boolean blockWhileCompactionRunning() {

    try {
      if (compactTask == null) {
        throw new IllegalStateException(
            "Compaction task has not been started - call startCompactionTask() before blocking");
      }
      compactTask.get();
      return true;
    } catch (InterruptedException ex) {
      Thread.currentThread().interrupt();
      return false;
    } catch (ExecutionException ex) {
      return false;
    }
  }

}

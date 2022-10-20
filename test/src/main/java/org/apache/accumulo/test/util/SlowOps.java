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
package org.apache.accumulo.test.util;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.ActiveCompaction;
import org.apache.accumulo.core.clientImpl.TableOperationsImpl;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.util.UtilWaitThread;
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

  private static final long SLOW_SCAN_SLEEP_MS = 250L;
  private static final int NUM_DATA_ROWS = 1000;

  private final AccumuloClient client;
  private final String tableName;
  private final long maxWaitMillis;

  // private final int numRows = DEFAULT_NUM_DATA_ROWS;

  private static final ExecutorService pool = Executors.newCachedThreadPool();

  private Future<?> compactTask = null;

  public SlowOps(final AccumuloClient client, final String tableName, final long maxWaitMillis) {
    this.client = client;
    this.tableName = tableName;
    this.maxWaitMillis = maxWaitMillis;
    createData();
  }

  public static void setExpectedCompactions(AccumuloClient client, final int numParallelExpected) {
    final int target = numParallelExpected + 1;
    try {
      client.instanceOperations().setProperty(
          Property.TSERV_COMPACTION_SERVICE_DEFAULT_EXECUTORS.getKey(),
          "[{'name':'any','numThreads':" + target + "}]".replaceAll("'", "\""));
      UtilWaitThread.sleep(3_000); // give it time to propagate
    } catch (AccumuloException | AccumuloSecurityException | NumberFormatException ex) {
      throw new IllegalStateException("Could not set parallel compaction limit to " + target, ex);
    }
  }

  public String getTableName() {
    return tableName;
  }

  private void createData() {
    try {
      client.tableOperations().create(tableName);
      log.info("Created table id: {}, name \'{}\'",
          client.tableOperations().tableIdMap().get(tableName), tableName);
      try (BatchWriter bw = client.createBatchWriter(tableName)) {
        // populate
        for (int i = 0; i < NUM_DATA_ROWS; i++) {
          Mutation m = new Mutation(new Text(String.format("%05d", i)));
          m.put("col" + ((i % 3) + 1), "qual", "junk");
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
        TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startTimestamp));
    if (count != NUM_DATA_ROWS) {
      throw new IllegalStateException(
          String.format("Number of rows %1$d does not match expected %2$d", count, NUM_DATA_ROWS));
    }
  }

  private int scanCount() {
    try (Scanner scanner = client.createScanner(tableName, Authorizations.EMPTY)) {
      int count = 0;
      for (Map.Entry<Key,Value> elt : scanner) {
        String expected = String.format("%05d", count);
        assert elt.getKey().getRow().toString().equals(expected);
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

    private SlowCompactionRunner() {}

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
          client.tableOperations().compact(tableName, null, null, compactIterators, true, true);
          completed = true;
        } catch (Throwable ex) {
          // test cancels compaction on complete, so ignore it as an exception.
          if (ex.getMessage().contains(TableOperationsImpl.COMPACTION_CANCELED_MSG)) {
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

      log.trace("Slow compaction of {} rows took {} ms", NUM_DATA_ROWS,
          TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startTimestamp));

      // validate that number of rows matches expected.

      startTimestamp = System.nanoTime();

      // validate expected data created and exists in table.

      int count = scanCount();

      log.trace("After compaction, scan time for {} rows {} ms", NUM_DATA_ROWS,
          TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startTimestamp));

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
    long startWaitNanos = System.nanoTime();
    long maxWaitNanos = TimeUnit.MILLISECONDS.toNanos(maxWaitMillis);

    /*
     * wait for compaction to start on table - The compaction will acquire a fate transaction lock
     * that used to block a subsequent online command while the fate transaction lock was held.
     */
    do {
      List<String> tservers = client.instanceOperations().getTabletServers();
      boolean tableFound = tservers.stream().flatMap(tserver -> {
        // get active compactions from each server
        try {
          List<ActiveCompaction> ac = client.instanceOperations().getActiveCompactions(tserver);
          log.trace("tserver {}, running compactions {}", tserver, ac.size());
          return ac.stream();
        } catch (AccumuloException | AccumuloSecurityException e) {
          throw new IllegalStateException("failed to get active compactions, test fails.", e);
        }
      }).map(activeCompaction -> {
        // emit table being compacted
        try {
          String compactionTable = activeCompaction.getTable();
          log.debug("Compaction running for {}", compactionTable);
          return compactionTable;
        } catch (TableNotFoundException ex) {
          log.trace("Compaction found for unknown table {}", activeCompaction);
          return null;
        }
      }).anyMatch(tableName::equals);

      if (tableFound) {
        return true;
      }

      UtilWaitThread.sleepUninterruptibly(3, TimeUnit.SECONDS);
    } while ((System.nanoTime() - startWaitNanos) < maxWaitNanos);

    log.debug("Could not find compaction for {} after {} seconds", tableName,
        TimeUnit.MILLISECONDS.toSeconds(maxWaitMillis));
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

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

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.clientImpl.ClientContext;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.InstanceId;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.fate.AdminUtil;
import org.apache.accumulo.core.fate.ZooStore;
import org.apache.accumulo.core.fate.zookeeper.ServiceLock;
import org.apache.accumulo.core.fate.zookeeper.ZooReaderWriter;
import org.apache.accumulo.core.fate.zookeeper.ZooUtil;
import org.apache.accumulo.core.manager.state.tables.TableState;
import org.apache.accumulo.harness.AccumuloClusterHarness;
import org.apache.accumulo.test.util.SlowOps;
import org.apache.zookeeper.KeeperException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * IT Tests that create / run a "slow" FATE transaction so that other operations can be checked.
 * Included tests for:
 * <ul>
 * <li>ACCUMULO-4574. Test to verify that changing table state to online / offline
 * {@link org.apache.accumulo.core.client.admin.TableOperations#online(String)} when the table is
 * already in that state returns without blocking.</li>
 * <li>AdminUtil refactor to provide methods that provide FATE status, one with table lock info
 * (original) and additional method without.</li>
 * </ul>
 */
public class FateConcurrencyIT extends AccumuloClusterHarness {

  private static final Logger log = LoggerFactory.getLogger(FateConcurrencyIT.class);

  private static final int NUM_ROWS = 1000;
  private static final long SLOW_SCAN_SLEEP_MS = 250L;

  private AccumuloClient client;
  private ClientContext context;

  private static final ExecutorService pool = Executors.newCachedThreadPool();

  private String secret;

  private long maxWaitMillis;

  private SlowOps slowOps;

  @Override
  protected Duration defaultTimeout() {
    return Duration.ofMinutes(4);
  }

  @BeforeEach
  public void setup() {
    client = Accumulo.newClient().from(getClientProps()).build();
    context = (ClientContext) client;
    secret = cluster.getSiteConfiguration().get(Property.INSTANCE_SECRET);
    maxWaitMillis = Math.max(MINUTES.toMillis(1), defaultTimeout().toMillis() / 2);
  }

  @AfterEach
  public void closeClient() {
    client.close();
  }

  @AfterAll
  public static void cleanup() {
    pool.shutdownNow();
  }

  /**
   * Validate that {@code TableOperations} online operation does not block when table is already
   * online and fate transaction lock is held by other operations. The test creates, populates a
   * table and then runs a compaction with a slow iterator so that operation takes long enough to
   * simulate the condition. After the online operation while compaction is running completes, the
   * test is complete and the compaction is canceled so that other tests can run.
   *
   * @throws Exception any exception is a test failure.
   */
  @Test
  public void changeTableStateTest() throws Exception {
    String tableName = getUniqueNames(1)[0];
    SlowOps.setExpectedCompactions(client, 1);
    slowOps = new SlowOps(client, tableName, maxWaitMillis);

    assertEquals(TableState.ONLINE, getTableState(tableName), "verify table online after created");

    OnLineCallable onlineOp = new OnLineCallable(tableName);

    Future<OnlineOpTiming> task = pool.submit(onlineOp);

    OnlineOpTiming timing1 = task.get();

    log.trace("Online 1 in {} ms", NANOSECONDS.toMillis(timing1.runningTime()));

    assertEquals(TableState.ONLINE, getTableState(tableName), "verify table is still online");

    // verify that offline then online functions as expected.

    client.tableOperations().offline(tableName, true);
    assertEquals(TableState.OFFLINE, getTableState(tableName), "verify table is offline");

    onlineOp = new OnLineCallable(tableName);

    task = pool.submit(onlineOp);

    OnlineOpTiming timing2 = task.get();

    log.trace("Online 2 in {} ms", NANOSECONDS.toMillis(timing2.runningTime()));

    assertEquals(TableState.ONLINE, getTableState(tableName), "verify table is back online");

    // launch a full table compaction with the slow iterator to ensure table lock is acquired and
    // held by the compaction
    slowOps.startCompactTask();

    // try to set online while fate transaction is in progress - before ACCUMULO-4574 this would
    // block

    onlineOp = new OnLineCallable(tableName);

    task = pool.submit(onlineOp);

    OnlineOpTiming timing3 = task.get();

    assertTrue(timing3.runningTime() < MILLISECONDS.toNanos(NUM_ROWS * SLOW_SCAN_SLEEP_MS),
        "online should take less time than expected compaction time");

    assertEquals(TableState.ONLINE, getTableState(tableName), "verify table is still online");

    assertTrue(findFate(tableName), "Find FATE operation for table");

    // test complete, cancel compaction and move on.
    client.tableOperations().cancelCompaction(tableName);

    log.debug("Success: Timing results for online commands.");
    log.debug("Time for unblocked online {} ms", NANOSECONDS.toMillis(timing1.runningTime()));
    log.debug("Time for online when offline {} ms", NANOSECONDS.toMillis(timing2.runningTime()));
    log.debug("Time for blocked online {} ms", NANOSECONDS.toMillis(timing3.runningTime()));

    // block if compaction still running
    slowOps.blockWhileCompactionRunning();

  }

  private boolean findFate(String aTableName) {
    log.debug("Look for fate {}", aTableName);
    for (int retry = 0; retry < 5; retry++) {
      try {
        boolean found = lookupFateInZookeeper(aTableName);
        log.trace("Try {}: Fate in zk for table {} : {}", retry, aTableName, found);
        if (found) {
          log.debug("Found fate {}", aTableName);
          return true;
        } else {
          Thread.sleep(150);
        }
      } catch (InterruptedException ex) {
        Thread.currentThread().interrupt();
        return false;
      } catch (Exception ex) {
        log.debug("Find fate failed for table name {} with exception, will retry", aTableName, ex);
      }
    }
    return false;
  }

  /**
   * Validate the AdminUtil.getStatus works correctly after refactor and validate that
   * getTransactionStatus can be called without lock map(s). The test starts a long running fate
   * transaction (slow compaction) and the calls AdminUtil functions to get the FATE.
   */
  @Test
  public void getFateStatus() {
    SlowOps.setExpectedCompactions(client, 1);
    String tableName = getUniqueNames(1)[0];
    slowOps = new SlowOps(client, tableName, maxWaitMillis);

    TableId tableId;

    try {

      assertEquals(TableState.ONLINE, getTableState(tableName),
          "verify table online after created");

      tableId = context.getTableId(tableName);

      log.trace("tid: {}", tableId);

    } catch (TableNotFoundException ex) {
      throw new IllegalStateException(
          String.format("Table %s does not exist, failing test", tableName));
    }

    slowOps.startCompactTask();

    AdminUtil.FateStatus withLocks = null;
    List<AdminUtil.TransactionStatus> noLocks = null;

    int maxRetries = 3;

    AdminUtil<String> admin = new AdminUtil<>(false);

    while (maxRetries > 0) {

      try {

        InstanceId instanceId = context.getInstanceID();
        ZooReaderWriter zk = context.getZooReader().asWriter(secret);
        ZooStore<String> zs = new ZooStore<>(ZooUtil.getRoot(instanceId) + Constants.ZFATE, zk);
        var lockPath =
            ServiceLock.path(ZooUtil.getRoot(instanceId) + Constants.ZTABLE_LOCKS + "/" + tableId);

        withLocks = admin.getStatus(zs, zk, lockPath, null, null);

        // call method that does not use locks.
        noLocks = admin.getTransactionStatus(zs, null, null);

        // no zk exception, no need to retry
        break;

      } catch (InterruptedException ex) {
        Thread.currentThread().interrupt();
        fail("Interrupt received - test failed");
        return;
      } catch (KeeperException ex) {
        maxRetries--;
        try {
          Thread.sleep(1000);
        } catch (InterruptedException intr_ex) {
          Thread.currentThread().interrupt();
          return;
        }
      }
    }

    assertNotNull(withLocks);
    assertNotNull(noLocks);

    // fast check - count number of transactions
    assertEquals(withLocks.getTransactions().size(), noLocks.size());

    int matchCount = 0;

    for (AdminUtil.TransactionStatus tx : withLocks.getTransactions()) {

      if (isCompaction(tx)) {

        log.trace("Fate id: {}, status: {}", tx.getTxid(), tx.getStatus());

        for (AdminUtil.TransactionStatus tx2 : noLocks) {
          if (tx2.getTxid().equals(tx.getTxid())) {
            matchCount++;
          }
        }
      }
    }

    assertTrue(matchCount > 0, "Number of fates matches should be > 0");

    try {

      // test complete, cancel compaction and move on.
      client.tableOperations().cancelCompaction(tableName);

      // block if compaction still running
      boolean cancelled = slowOps.blockWhileCompactionRunning();
      log.debug("Cancel completed successfully: {}", cancelled);

    } catch (TableNotFoundException | AccumuloSecurityException | AccumuloException ex) {
      log.debug("Could not cancel compaction due to exception", ex);
    }
  }

  /**
   * Checks fates in zookeeper looking for transaction associated with a compaction as a double
   * check that the test will be valid because the running compaction does have a fate transaction
   * lock.
   * <p>
   * This method throws can throw either IllegalStateException (failed) or a Zookeeper exception.
   * Throwing the Zookeeper exception allows for retries if desired to handle transient zookeeper
   * issues.
   *
   * @param tableName a table name
   * @return true if corresponding fate transaction found, false otherwise
   * @throws KeeperException if a zookeeper error occurred - allows for retries.
   */
  private boolean lookupFateInZookeeper(final String tableName) throws KeeperException {

    AdminUtil<String> admin = new AdminUtil<>(false);

    try {

      TableId tableId = context.getTableId(tableName);

      log.trace("tid: {}", tableId);

      InstanceId instanceId = context.getInstanceID();
      ZooReaderWriter zk = context.getZooReader().asWriter(secret);
      ZooStore<String> zs = new ZooStore<>(ZooUtil.getRoot(instanceId) + Constants.ZFATE, zk);
      var lockPath =
          ServiceLock.path(ZooUtil.getRoot(instanceId) + Constants.ZTABLE_LOCKS + "/" + tableId);
      AdminUtil.FateStatus fateStatus = admin.getStatus(zs, zk, lockPath, null, null);

      log.trace("current fates: {}", fateStatus.getTransactions().size());

      for (AdminUtil.TransactionStatus tx : fateStatus.getTransactions()) {

        if (isCompaction(tx)) {
          return true;
        }
      }

    } catch (TableNotFoundException | InterruptedException ex) {
      throw new IllegalStateException(ex);
    }

    // did not find appropriate fate transaction for compaction.
    return Boolean.FALSE;
  }

  /**
   * Test that the transaction top contains "CompactionDriver" and the debug message contains
   * "CompactRange"
   *
   * @param tx transaction status
   * @return true if tx top and debug have compaction messages.
   */
  private boolean isCompaction(AdminUtil.TransactionStatus tx) {

    if (tx == null) {
      log.trace("Fate tx is null");
      return false;
    }

    log.trace("Fate id: {}, status: {}", tx.getTxid(), tx.getStatus());

    String top = tx.getTop();
    String txName = tx.getTxName();

    return top != null && txName != null && top.contains("CompactionDriver")
        && tx.getTxName().equals("TABLE_COMPACT");
  }

  /**
   * Returns the current table state (ONLINE, OFFLINE,...) of named table.
   *
   * @param tableName the table name
   * @return the current table state
   * @throws TableNotFoundException if table does not exist
   */
  private TableState getTableState(String tableName) throws TableNotFoundException {

    TableId tableId = context.getTableId(tableName);

    TableState tstate = context.getTableState(tableId);

    log.trace("tableName: '{}': tableId {}, current state: {}", tableName, tableId, tstate);

    return tstate;
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
     * @param tableName The table name that will be set online.
     */
    OnLineCallable(final String tableName) {
      this.tableName = tableName;
    }

    @Override
    public OnlineOpTiming call() throws Exception {

      OnlineOpTiming status = new OnlineOpTiming();

      log.trace("Setting {} online", tableName);

      client.tableOperations().online(tableName, true);
      // stop timing
      status.setComplete();

      log.trace("Online completed in {} ms", NANOSECONDS.toMillis(status.runningTime()));

      return status;
    }
  }

  /**
   * Concurrency testing - ensure that tests are valid if multiple compactions are running. for
   * development testing - force transient condition that was failing this test so that we know if
   * multiple compactions are running, they are properly handled by the test code and the tests are
   * valid.
   */
  @Test
  public void multipleCompactions() {

    int tableCount = 4;
    SlowOps.setExpectedCompactions(client, tableCount);

    List<SlowOps> tables = Arrays.stream(getUniqueNames(tableCount))
        .map(tableName -> new SlowOps(client, tableName, maxWaitMillis))
        .collect(Collectors.toList());
    tables.forEach(SlowOps::startCompactTask);

    assertEquals(tableCount,
        tables.stream().map(SlowOps::getTableName).filter(this::findFate).count());

    tables.forEach(t -> {
      try {
        client.tableOperations().cancelCompaction(t.getTableName());
      } catch (AccumuloSecurityException | TableNotFoundException | AccumuloException ex) {
        log.debug("Exception throw during multiple table test clean-up", ex);
      }
      // block if compaction still running
      boolean cancelled = t.blockWhileCompactionRunning();
      if (!cancelled) {
        log.info("Failed to cancel compaction during multiple compaction test clean-up for {}",
            t.getTableName());
      }
    });

  }
}

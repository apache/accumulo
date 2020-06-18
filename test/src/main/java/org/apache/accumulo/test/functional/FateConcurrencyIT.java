/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.accumulo.test.functional;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.clientImpl.ClientContext;
import org.apache.accumulo.core.clientImpl.Tables;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.master.state.tables.TableState;
import org.apache.accumulo.fate.AdminUtil;
import org.apache.accumulo.fate.ZooStore;
import org.apache.accumulo.fate.zookeeper.ZooReaderWriter;
import org.apache.accumulo.fate.zookeeper.ZooUtil;
import org.apache.accumulo.harness.AccumuloClusterHarness;
import org.apache.accumulo.test.util.SlowOps;
import org.apache.zookeeper.KeeperException;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;
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

  private String tableName;

  private String secret;

  private long maxWait;

  private SlowOps slowOps;

  @Before
  public void setup() {

    client = Accumulo.newClient().from(getClientProps()).build();
    context = (ClientContext) client;

    tableName = getUniqueNames(1)[0];

    secret = cluster.getSiteConfiguration().get(Property.INSTANCE_SECRET);

    maxWait = defaultTimeoutSeconds() <= 0 ? 60_000 : ((defaultTimeoutSeconds() * 1000) / 2);

    slowOps = new SlowOps(client, tableName, maxWait, 1);
  }

  @After
  public void closeClient() {
    client.close();
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
   * Validate that {@code TableOperations} online operation does not block when table is already
   * online and fate transaction lock is held by other operations. The test creates, populates a
   * table and then runs a compaction with a slow iterator so that operation takes long enough to
   * simulate the condition. After the online operation while compaction is running completes, the
   * test is complete and the compaction is canceled so that other tests can run.
   *
   * @throws Exception
   *           any exception is a test failure.
   */
  @Test
  public void changeTableStateTest() throws Exception {

    assertEquals("verify table online after created", TableState.ONLINE, getTableState(tableName));

    OnLineCallable onlineOp = new OnLineCallable(tableName);

    Future<OnlineOpTiming> task = pool.submit(onlineOp);

    OnlineOpTiming timing1 = task.get();

    log.trace("Online 1 in {} ms",
        TimeUnit.MILLISECONDS.convert(timing1.runningTime(), TimeUnit.NANOSECONDS));

    assertEquals("verify table is still online", TableState.ONLINE, getTableState(tableName));

    // verify that offline then online functions as expected.

    client.tableOperations().offline(tableName, true);
    assertEquals("verify table is offline", TableState.OFFLINE, getTableState(tableName));

    onlineOp = new OnLineCallable(tableName);

    task = pool.submit(onlineOp);

    OnlineOpTiming timing2 = task.get();

    log.trace("Online 2 in {} ms",
        TimeUnit.MILLISECONDS.convert(timing2.runningTime(), TimeUnit.NANOSECONDS));

    assertEquals("verify table is back online", TableState.ONLINE, getTableState(tableName));

    // launch a full table compaction with the slow iterator to ensure table lock is acquired and
    // held by the compaction
    slowOps.startCompactTask();

    // try to set online while fate transaction is in progress - before ACCUMULO-4574 this would
    // block

    onlineOp = new OnLineCallable(tableName);

    task = pool.submit(onlineOp);

    OnlineOpTiming timing3 = task.get();

    assertTrue("online should take less time than expected compaction time", timing3.runningTime()
        < TimeUnit.NANOSECONDS.convert(NUM_ROWS * SLOW_SCAN_SLEEP_MS, TimeUnit.MILLISECONDS));

    assertEquals("verify table is still online", TableState.ONLINE, getTableState(tableName));

    assertTrue("Find FATE operation for table", findFate(tableName));

    // test complete, cancel compaction and move on.
    client.tableOperations().cancelCompaction(tableName);

    log.debug("Success: Timing results for online commands.");
    log.debug("Time for unblocked online {} ms",
        TimeUnit.MILLISECONDS.convert(timing1.runningTime(), TimeUnit.NANOSECONDS));
    log.debug("Time for online when offline {} ms",
        TimeUnit.MILLISECONDS.convert(timing2.runningTime(), TimeUnit.NANOSECONDS));
    log.debug("Time for blocked online {} ms",
        TimeUnit.MILLISECONDS.convert(timing3.runningTime(), TimeUnit.NANOSECONDS));

    // block if compaction still running
    slowOps.blockWhileCompactionRunning();

  }

  private boolean findFate(String aTableName) {

    for (int retry = 0; retry < 5; retry++) {

      try {
        boolean found = lookupFateInZookeeper(aTableName);
        log.trace("Try {}: Fate in zk for table {} : {}", retry, aTableName, found);
        if (found) {
          log.trace("found for {}", aTableName);
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
   * Validate the the AdminUtil.getStatus works correctly after refactor and validate that
   * getTransactionStatus can be called without lock map(s). The test starts a long running fate
   * transaction (slow compaction) and the calls AdminUtil functions to get the FATE.
   */
  @Test
  public void getFateStatus() {

    TableId tableId;

    try {

      assertEquals("verify table online after created", TableState.ONLINE,
          getTableState(tableName));

      tableId = Tables.getTableId(context, tableName);

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

        String instanceId = context.getInstanceID();
        ZooReaderWriter zk = new ZooReaderWriter(context.getZooKeepers(),
            context.getZooKeepersSessionTimeOut(), secret);
        ZooStore<String> zs = new ZooStore<>(ZooUtil.getRoot(instanceId) + Constants.ZFATE, zk);

        withLocks = admin.getStatus(zs, zk,
            ZooUtil.getRoot(instanceId) + Constants.ZTABLE_LOCKS + "/" + tableId, null, null);

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

    assertTrue("Number of fates matches should be > 0", matchCount > 0);

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
   * @param tableName
   *          a table name
   * @return true if corresponding fate transaction found, false otherwise
   * @throws KeeperException
   *           if a zookeeper error occurred - allows for retries.
   */
  private boolean lookupFateInZookeeper(final String tableName) throws KeeperException {

    AdminUtil<String> admin = new AdminUtil<>(false);

    try {

      TableId tableId = Tables.getTableId(context, tableName);

      log.trace("tid: {}", tableId);

      String instanceId = context.getInstanceID();
      ZooReaderWriter zk = new ZooReaderWriter(context.getZooKeepers(),
          context.getZooKeepersSessionTimeOut(), secret);
      ZooStore<String> zs = new ZooStore<>(ZooUtil.getRoot(instanceId) + Constants.ZFATE, zk);
      AdminUtil.FateStatus fateStatus = admin.getStatus(zs, zk,
          ZooUtil.getRoot(instanceId) + Constants.ZTABLE_LOCKS + "/" + tableId, null, null);

      log.trace("current fates: {}", fateStatus.getTransactions().size());

      for (AdminUtil.TransactionStatus tx : fateStatus.getTransactions()) {

        if (isCompaction(tx))
          return true;
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
   * @param tx
   *          transaction status
   * @return true if tx top and debug have compaction messages.
   */
  private boolean isCompaction(AdminUtil.TransactionStatus tx) {

    if (tx == null) {
      log.trace("Fate tx is null");
      return false;
    }

    log.trace("Fate id: {}, status: {}", tx.getTxid(), tx.getStatus());

    String top = tx.getTop();
    String debug = tx.getDebug();

    return top != null && debug != null && top.contains("CompactionDriver")
        && tx.getDebug().contains("CompactRange");

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
  private TableState getTableState(String tableName) throws TableNotFoundException {

    TableId tableId = Tables.getTableId(context, tableName);

    TableState tstate = Tables.getTableState(context, tableId);

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

      client.tableOperations().online(tableName, true);
      // stop timing
      status.setComplete();

      log.trace("Online completed in {} ms",
          TimeUnit.MILLISECONDS.convert(status.runningTime(), TimeUnit.NANOSECONDS));

      return status;
    }
  }

  /**
   * Concurrency testing - ensure that tests are valid id multiple compactions are running. for
   * development testing - force transient condition that was failing this test so that we know if
   * multiple compactions are running, they are properly handled by the test code and the tests are
   * valid.
   */
  @Test
  public void multipleCompactions() {

    int tableCount = 4;

    List<SlowOps> tables = new ArrayList<>();

    for (int i = 0; i < tableCount; i++) {
      String uniqueName = getUniqueNames(1)[0] + "_" + i;
      SlowOps gen = new SlowOps(client, uniqueName, maxWait, tableCount);
      tables.add(gen);
      gen.startCompactTask();
    }

    int foundCount = 0;

    for (SlowOps t : tables) {
      log.debug("Look for fate {}", t.getTableName());
      if (findFate(t.getTableName())) {
        log.debug("Found fate {}", t.getTableName());
        foundCount++;
      }
    }

    assertEquals(tableCount, foundCount);

    for (SlowOps t : tables) {
      try {
        client.tableOperations().cancelCompaction(t.getTableName());
        // block if compaction still running
        boolean cancelled = t.blockWhileCompactionRunning();
        if (!cancelled) {
          log.info("Failed to cancel compaction during multiple compaction test clean-up for {}",
              t.getTableName());
        }
      } catch (AccumuloSecurityException | TableNotFoundException | AccumuloException ex) {
        log.debug("Exception throw during multiple table test clean-up", ex);
      }
    }
  }
}

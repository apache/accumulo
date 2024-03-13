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
package org.apache.accumulo.test.fate;

import static org.apache.accumulo.core.util.compaction.ExternalCompactionUtil.getCompactorAddrs;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.time.Duration;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.admin.NewTableConfiguration;
import org.apache.accumulo.core.conf.ConfigurationCopy;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.fate.Fate;
import org.apache.accumulo.core.fate.FateId;
import org.apache.accumulo.core.fate.FateInstanceType;
import org.apache.accumulo.core.fate.FateStore;
import org.apache.accumulo.core.fate.ReadOnlyFateStore;
import org.apache.accumulo.core.fate.Repo;
import org.apache.accumulo.core.iterators.IteratorUtil;
import org.apache.accumulo.manager.Manager;
import org.apache.accumulo.manager.tableOps.ManagerRepo;
import org.apache.accumulo.minicluster.ServerType;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloClusterImpl.ProcessInfo;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloConfigImpl;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.util.Admin;
import org.apache.accumulo.server.util.fateCommand.FateSummaryReport;
import org.apache.accumulo.server.util.fateCommand.FateTxnDetails;
import org.apache.accumulo.test.fate.accumulo.AccumuloFateIT;
import org.apache.accumulo.test.functional.ConfigurableMacBase;
import org.apache.accumulo.test.functional.ReadWriteIT;
import org.apache.accumulo.test.functional.SlowIterator;
import org.apache.accumulo.test.util.Wait;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.checkerframework.checker.units.qual.C;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class FateOpsCommandsIT extends ConfigurableMacBase
    implements FateTestRunner<FateTestRunner.TestEnv> {
  private static final Logger LOG = LoggerFactory.getLogger(FateOpsCommandsIT.class);
  private static CountDownLatch callStarted;
  private static CountDownLatch finishCall;

  public static class TestOp implements Repo<TestEnv> {
    private static final long serialVersionUID = 1L;
    private final String name;

    public TestOp(String name) {
      this.name = name;
    }
    @Override
    public long isReady(FateId fateId, TestEnv environment) throws Exception {
      return 0;
    }

    @Override
    public String getName() {
      return name;
    }

    @Override
    public void undo(FateId fateId, TestEnv environment) throws Exception {

    }

    @Override
    public Repo<TestEnv> call(FateId fateId, TestEnv environment) throws Exception {
      LOG.info("Entered call {}", fateId);
      // Signal that the Repo has entered call()
      callStarted.countDown();
      // Wait for signal to leave call()
      finishCall.await();
      LOG.info("Leaving call {}", fateId);
      return null;
    }

    @Override
    public String getReturn() {
      return "none";
    }
  }

  @Override
  protected Duration defaultTimeout() {
    return Duration.ofMinutes(3);
  }

  @Override
  public void configure(MiniAccumuloConfigImpl cfg, Configuration hadoopCoreSite) {
    // Used for tests that shutdown the manager so the sleep time after shutdown isn't too long
    cfg.setProperty(Property.INSTANCE_ZK_TIMEOUT.getKey(), "10s");
  }

  @Test
  public void testFateSummaryCommand() throws Exception {
    executeTest(this::testFateSummaryCommand);
  }

  protected void testFateSummaryCommand(FateStore<TestEnv> store, ServerContext sctx)
      throws Exception {
    // Configure Fate and latches
    Fate<TestEnv> fate = initializeFate(store);
    callStarted = new CountDownLatch(2);
    finishCall = new CountDownLatch(1);
    // Occasionally, the summary/print cmds will see a COMMIT_COMPACTION transaction which was
    // initiated on starting the manager, causing the test to fail. Stopping the compactor fixes
    // this issue.
    getCluster().getClusterControl().stopAllServers(ServerType.COMPACTOR);
    Wait.waitFor(() -> getCompactorAddrs(getCluster().getServerContext()).isEmpty(), 60_000);

    // validate blank report, no transactions have started
    ProcessInfo p = getCluster().exec(Admin.class, "fate", "--summary", "-j");
    assertEquals(0, p.getProcess().waitFor());
    String result = p.readStdOut();
    result = result.substring(result.indexOf("{"), result.lastIndexOf("}") + 1);
    FateSummaryReport report = FateSummaryReport.fromJson(result);
    assertNotNull(report);
    assertNotEquals(0, report.getReportTime());
    assertTrue(report.getStatusCounts().isEmpty());
    assertTrue(report.getStepCounts().isEmpty());
    assertTrue(report.getCmdCounts().isEmpty());
    assertTrue(report.getStatusFilterNames().isEmpty());
    assertTrue(report.getInstanceTypesFilterNames().isEmpty());
    assertTrue(report.getFateIdFilter().isEmpty());
    assertEquals(0, report.getFateDetails().size());

    // create Fate transactions, seed them with dummy work, and wait for them to enter call()
    FateId fateId1 = fate.startTransaction();
    FateId fateId2 = fate.startTransaction();
    fate.seedTransaction("tx1", fateId1, new TestOp("tx1_name"), false, "tx1_message");
    fate.seedTransaction("tx2", fateId2, new TestOp("tx2_name"), false, "tx2_message");
    callStarted.await();
    List<String> fateIdsStarted = List.of(fateId1.canonical(), fateId2.canonical());

    // validate no filters
    p = getCluster().exec(Admin.class, "fate", "--summary", "-j");
    assertEquals(0, p.getProcess().waitFor());
    result = p.readStdOut();
    result = result.substring(result.indexOf("{"), result.lastIndexOf("}") + 1);
    report = FateSummaryReport.fromJson(result);
    assertNotNull(report);
    assertNotEquals(0, report.getReportTime());
    assertFalse(report.getStatusCounts().isEmpty());
    assertFalse(report.getStepCounts().isEmpty());
    assertFalse(report.getCmdCounts().isEmpty());
    assertTrue(report.getStatusFilterNames().isEmpty());
    assertTrue(report.getInstanceTypesFilterNames().isEmpty());
    assertTrue(report.getFateIdFilter().isEmpty());
    assertEquals(2, report.getFateDetails().size());
    validateFateDetails(report.getFateDetails(), fateIdsStarted);

    // validate filtering by both transactions
    p = getCluster().exec(Admin.class, "fate", fateId1.canonical(), fateId2.canonical(),
        "--summary", "-j");
    assertEquals(0, p.getProcess().waitFor());
    result = p.readStdOut();
    result = result.substring(result.indexOf("{"), result.lastIndexOf("}") + 1);
    report = FateSummaryReport.fromJson(result);
    assertNotNull(report);
    assertNotEquals(0, report.getReportTime());
    assertFalse(report.getStatusCounts().isEmpty());
    assertFalse(report.getStepCounts().isEmpty());
    assertFalse(report.getCmdCounts().isEmpty());
    assertTrue(report.getStatusFilterNames().isEmpty());
    assertTrue(report.getInstanceTypesFilterNames().isEmpty());
    assertEquals(2, report.getFateIdFilter().size());
    assertTrue(report.getFateIdFilter().containsAll(fateIdsStarted));
    assertEquals(2, report.getFateDetails().size());
    validateFateDetails(report.getFateDetails(), fateIdsStarted);

    // validate filtering by just one transaction
    p = getCluster().exec(Admin.class, "fate", fateId1.canonical(), "--summary", "-j");
    assertEquals(0, p.getProcess().waitFor());
    result = p.readStdOut();
    result = result.substring(result.indexOf("{"), result.lastIndexOf("}") + 1);
    report = FateSummaryReport.fromJson(result);
    assertNotNull(report);
    assertNotEquals(0, report.getReportTime());
    assertFalse(report.getStatusCounts().isEmpty());
    assertFalse(report.getStepCounts().isEmpty());
    assertFalse(report.getCmdCounts().isEmpty());
    assertTrue(report.getStatusFilterNames().isEmpty());
    assertTrue(report.getInstanceTypesFilterNames().isEmpty());
    assertEquals(1, report.getFateIdFilter().size());
    assertTrue(report.getFateIdFilter().contains(fateId1.canonical()));
    assertEquals(1, report.getFateDetails().size());
    validateFateDetails(report.getFateDetails(), fateIdsStarted);

    // validate filtering by IN_PROGRESS transactions, should be 2
    // TODO KEVIN RATHBUN


    // validate filtering by FAILED and FAILED_IN_PROGRESS transactions, should be none
    p = getCluster().exec(Admin.class, "fate", "--summary", "-j", "-s", "FAILED", "-s", "FAILED_IN_PROGRESS");
    assertEquals(0, p.getProcess().waitFor());
    result = p.readStdOut();
    result = result.substring(result.indexOf("{"), result.lastIndexOf("}") + 1);
    report = FateSummaryReport.fromJson(result);
    assertNotNull(report);
    assertNotEquals(0, report.getReportTime());
    assertFalse(report.getStatusCounts().isEmpty());
    assertFalse(report.getStepCounts().isEmpty());
    assertFalse(report.getCmdCounts().isEmpty());
    assertEquals(Set.of("FAILED", "FAILED_IN_PROGRESS"), report.getStatusFilterNames());
    assertTrue(report.getInstanceTypesFilterNames().isEmpty());
    assertTrue(report.getFateIdFilter().isEmpty());
    assertEquals(0, report.getFateDetails().size());

    // validate FateInstanceType filter by only including transactions with META filter
    p = getCluster().exec(Admin.class, "fate", "--summary", "-j", "-t", "META");
    assertEquals(0, p.getProcess().waitFor());
    result = p.readStdOut();
    result = result.substring(result.indexOf("{"), result.lastIndexOf("}") + 1);
    report = FateSummaryReport.fromJson(result);
    assertNotNull(report);
    assertNotEquals(0, report.getReportTime());
    assertFalse(report.getStatusCounts().isEmpty());
    assertFalse(report.getStepCounts().isEmpty());
    assertFalse(report.getCmdCounts().isEmpty());
    assertTrue(report.getStatusFilterNames().isEmpty());
    assertEquals(Set.of("META"), report.getInstanceTypesFilterNames());
    assertTrue(report.getFateIdFilter().isEmpty());
    if (store.type() == FateInstanceType.META) {
      assertEquals(2, report.getFateDetails().size());
      validateFateDetails(report.getFateDetails(), fateIdsStarted);
    } else { // USER
      assertEquals(0, report.getFateDetails().size());
    }

    // validate FateInstanceType filter by only including transactions with USER filter
    p = getCluster().exec(Admin.class, "fate", "--summary", "-j", "-t", "USER");
    assertEquals(0, p.getProcess().waitFor());
    result = p.readStdOut();
    result = result.substring(result.indexOf("{"), result.lastIndexOf("}") + 1);
    report = FateSummaryReport.fromJson(result);
    assertNotNull(report);
    assertNotEquals(0, report.getReportTime());
    assertFalse(report.getStatusCounts().isEmpty());
    assertFalse(report.getStepCounts().isEmpty());
    assertFalse(report.getCmdCounts().isEmpty());
    assertTrue(report.getStatusFilterNames().isEmpty());
    assertEquals(Set.of("USER"), report.getInstanceTypesFilterNames());
    assertTrue(report.getFateIdFilter().isEmpty());
    if (store.type() == FateInstanceType.META) {
      assertEquals(0, report.getFateDetails().size());
    } else { // USER
      assertEquals(2, report.getFateDetails().size());
      validateFateDetails(report.getFateDetails(), fateIdsStarted);
    }

    // Signal to exit call() for both the transactions and shutdown fate
    finishCall.countDown();
    fate.shutdown(10, TimeUnit.MINUTES);
  }

  @Test
  public void testFateSummaryCommandPlainText() throws Exception {
    executeTest(this::testFateSummaryCommandPlainText);
  }

  protected void testFateSummaryCommandPlainText(FateStore<TestEnv> store, ServerContext sctx)
      throws Exception {
    // Configure Fate
    Fate<TestEnv> fate = initializeFate(store);
    // Occasionally, the summary/print cmds will see a COMMIT_COMPACTION transaction which was
    // initiated on starting the manager, causing the test to fail. Stopping the compactor fixes
    // this issue.
    getCluster().getClusterControl().stopAllServers(ServerType.COMPACTOR);
    Wait.waitFor(() -> getCompactorAddrs(getCluster().getServerContext()).isEmpty(), 60_000);

    // Start some transactions
    FateId fateId1 = fate.startTransaction();
    FateId fateId2 = fate.startTransaction();

    ProcessInfo p = getCluster().exec(Admin.class, "fate", fateId1.canonical(), fateId2.canonical(),
        "--summary", "-s", "NEW", "-t", store.type().name());
    assertEquals(0, p.getProcess().waitFor());
    String result = p.readStdOut();
    assertTrue(result.contains("Status Filters: [NEW]"));
    assertTrue(result
        .contains("Fate ID Filters: [" + fateId1.canonical() + ", " + fateId2.canonical() + "]")
        || result.contains(
            "Fate ID Filters: [" + fateId2.canonical() + ", " + fateId1.canonical() + "]"));
    assertTrue(result.contains("Instance Types Filters: [" + store.type().name() + "]"));
  }

  @Test
  public void testFatePrintCommand() throws Exception {
    executeTest(this::testFatePrintCommand);
  }

  protected void testFatePrintCommand(FateStore<TestEnv> store, ServerContext sctx)
      throws Exception {
    // Configure Fate and latches
    Fate<TestEnv> fate = initializeFate(store);
    callStarted = new CountDownLatch(2);
    finishCall = new CountDownLatch(1);
    // Occasionally, the summary/print cmds will see a COMMIT_COMPACTION transaction which was
    // initiated on starting the manager, causing the test to fail. Stopping the compactor fixes
    // this issue.
    getCluster().getClusterControl().stopAllServers(ServerType.COMPACTOR);
    Wait.waitFor(() -> getCompactorAddrs(getCluster().getServerContext()).isEmpty(), 60_000);

    // validate no transactions
    ProcessInfo p = getCluster().exec(Admin.class, "fate", "--print");
    assertEquals(0, p.getProcess().waitFor());
    String result = p.readStdOut();
    assertTrue(result.contains("0 transactions"));

    // create Fate transactions, seed them with dummy work, and wait for them to enter call()
    FateId fateId1 = fate.startTransaction();
    FateId fateId2 = fate.startTransaction();
    fate.seedTransaction("tx1", fateId1, new TestOp("tx1_name"), false, "tx1_message");
    fate.seedTransaction("tx2", fateId2, new TestOp("tx2_name"), false, "tx2_message");
    callStarted.await();

    // Get all transactions. Should be 2 FateIds with a NEW status
    p = getCluster().exec(Admin.class, "fate", "--print");
    assertEquals(0, p.getProcess().waitFor());
    result = p.readStdOut();
    Map<String,String> fateIdsFromResult = getFateIdsFromPrint(result);
    assertEquals(Map.of(fateId1.canonical(), "NEW", fateId2.canonical(), "NEW"), fateIdsFromResult);

    /*
     * Test filtering by States
     */

    // Filter by NEW state
    p = getCluster().exec(Admin.class, "fate", "--print", "-s", "NEW");
    assertEquals(0, p.getProcess().waitFor());
    result = p.readStdOut();
    fateIdsFromResult = getFateIdsFromPrint(result);
    assertEquals(Map.of(fateId1.canonical(), "NEW", fateId2.canonical(), "NEW"), fateIdsFromResult);

    // Filter by FAILED state
    p = getCluster().exec(Admin.class, "fate", "--print", "-s", "FAILED");
    assertEquals(0, p.getProcess().waitFor());
    result = p.readStdOut();
    fateIdsFromResult = getFateIdsFromPrint(result);
    assertTrue(fateIdsFromResult.isEmpty());

    /*
     * Test filtering by FateIds
     */

    // Filter by one FateId
    p = getCluster().exec(Admin.class, "fate", fateId1.canonical(), "--print");
    assertEquals(0, p.getProcess().waitFor());
    result = p.readStdOut();
    fateIdsFromResult = getFateIdsFromPrint(result);
    assertEquals(Map.of(fateId1.canonical(), "NEW"), fateIdsFromResult);

    // Filter by both FateIds
    p = getCluster().exec(Admin.class, "fate", fateId1.canonical(), fateId2.canonical(), "--print");
    assertEquals(0, p.getProcess().waitFor());
    result = p.readStdOut();
    fateIdsFromResult = getFateIdsFromPrint(result);
    assertEquals(Map.of(fateId1.canonical(), "NEW", fateId2.canonical(), "NEW"), fateIdsFromResult);

    /*
     * Test filtering by FateInstanceType
     */

    // Test filter by USER FateInstanceType
    p = getCluster().exec(Admin.class, "fate", "--print", "-t", "USER");
    assertEquals(0, p.getProcess().waitFor());
    result = p.readStdOut();
    fateIdsFromResult = getFateIdsFromPrint(result);
    if (store.type() == FateInstanceType.META) {
      assertTrue(fateIdsFromResult.isEmpty());
    } else { // USER
      assertEquals(Map.of(fateId1.canonical(), "NEW", fateId2.canonical(), "NEW"),
          fateIdsFromResult);
    }

    // Test filter by META FateInstanceType
    p = getCluster().exec(Admin.class, "fate", "--print", "-t", "META");
    assertEquals(0, p.getProcess().waitFor());
    result = p.readStdOut();
    fateIdsFromResult = getFateIdsFromPrint(result);
    if (store.type() == FateInstanceType.META) {
      assertEquals(Map.of(fateId1.canonical(), "NEW", fateId2.canonical(), "NEW"),
          fateIdsFromResult);
    } else { // USER
      assertTrue(fateIdsFromResult.isEmpty());
    }

    fate.shutdown(10, TimeUnit.MINUTES);
  }

  @Test
  public void testFateCancelCommand() throws Exception {
    executeTest(this::testFateCancelCommand);
  }

  protected void testFateCancelCommand(FateStore<TestEnv> store, ServerContext sctx)
      throws Exception {
    // Configure Fate
    Fate<TestEnv> fate = initializeFate(store);
    // Occasionally, the summary/print cmds will see a COMMIT_COMPACTION transaction which was
    // initiated on starting the manager, causing the test to fail. Stopping the compactor fixes
    // this issue.
    getCluster().getClusterControl().stopAllServers(ServerType.COMPACTOR);
    Wait.waitFor(() -> getCompactorAddrs(getCluster().getServerContext()).isEmpty(), 60_000);

    // Start some transactions
    FateId fateId1 = fate.startTransaction();
    FateId fateId2 = fate.startTransaction();

    // Check that summary output lists both the transactions with a NEW status
    Map<String,String> fateIdsFromSummary = getFateIdsFromSummary();
    assertEquals(Map.of(fateId1.canonical(), "NEW", fateId2.canonical(), "NEW"),
        fateIdsFromSummary);

    // Cancel the first transaction and ensure that it was cancelled
    ProcessInfo p = getCluster().exec(Admin.class, "fate", fateId1.canonical(), "--cancel");
    assertEquals(0, p.getProcess().waitFor());
    String result = p.readStdOut();

    assertTrue(result
        .contains("transaction " + fateId1.canonical() + " was cancelled or already completed"));
    fateIdsFromSummary = getFateIdsFromSummary();
    assertEquals(Map.of(fateId1.canonical(), "FAILED", fateId2.canonical(), "NEW"),
        fateIdsFromSummary);

    fate.shutdown(10, TimeUnit.MINUTES);
  }

  @Test
  public void testFateFailCommand() throws Exception {
    executeTest(this::testFateFailCommand);
  }

  protected void testFateFailCommand(FateStore<TestEnv> store, ServerContext sctx)
      throws Exception {
    // Configure Fate
    Fate<TestEnv> fate = initializeFate(store);
    // Occasionally, the summary/print cmds will see a COMMIT_COMPACTION transaction which was
    // initiated on starting the manager, causing the test to fail. Stopping the compactor fixes
    // this issue.
    getCluster().getClusterControl().stopAllServers(ServerType.COMPACTOR);
    Wait.waitFor(() -> getCompactorAddrs(getCluster().getServerContext()).isEmpty(), 60_000);

    // Start some transactions
    FateId fateId1 = fate.startTransaction();
    FateId fateId2 = fate.startTransaction();

    // Check that summary output lists both the transactions with a NEW status
    Map<String,String> fateIdsFromSummary = getFateIdsFromSummary();
    assertEquals(Map.of(fateId1.canonical(), "NEW", fateId2.canonical(), "NEW"),
        fateIdsFromSummary);

    // Attempt to --fail the transaction. Should not work as the Manager is still up
    ProcessInfo p = getCluster().exec(Admin.class, "fate", fateId1.canonical(), "--fail");
    assertEquals(1, p.getProcess().waitFor());
    fateIdsFromSummary = getFateIdsFromSummary();
    assertEquals(Map.of(fateId1.canonical(), "NEW", fateId2.canonical(), "NEW"),
        fateIdsFromSummary);

    // Stop MANAGER so --fail can be called
    getCluster().getClusterControl().stopAllServers(ServerType.MANAGER);
    Thread.sleep(20_000);

    // Fail the first transaction and ensure that it was failed
    p = getCluster().exec(Admin.class, "fate", fateId1.canonical(), "--fail");
    assertEquals(0, p.getProcess().waitFor());
    String result = p.readStdOut();

    assertTrue(result.contains("Failing transaction: " + fateId1));
    fateIdsFromSummary = getFateIdsFromSummary();
    assertTrue(fateIdsFromSummary
        .equals(Map.of(fateId1.canonical(), "FAILED_IN_PROGRESS", fateId2.canonical(), "NEW"))
        || fateIdsFromSummary
            .equals(Map.of(fateId1.canonical(), "FAILED", fateId2.canonical(), "NEW")));

    fate.shutdown(10, TimeUnit.MINUTES);
  }

  @Test
  public void testFateDeleteCommand() throws Exception {
    executeTest(this::testFateDeleteCommand);
  }

  protected void testFateDeleteCommand(FateStore<TestEnv> store, ServerContext sctx)
      throws Exception {
    // Configure Fate
    Fate<TestEnv> fate = initializeFate(store);
    // Occasionally, the summary/print cmds will see a COMMIT_COMPACTION transaction which was
    // initiated on starting the manager, causing the test to fail. Stopping the compactor fixes
    // this issue.
    getCluster().getClusterControl().stopAllServers(ServerType.COMPACTOR);
    Wait.waitFor(() -> getCompactorAddrs(getCluster().getServerContext()).isEmpty(), 60_000);

    // Start some transactions
    FateId fateId1 = fate.startTransaction();
    FateId fateId2 = fate.startTransaction();

    // Check that summary output lists both the transactions with a NEW status
    Map<String,String> fateIdsFromSummary = getFateIdsFromSummary();
    assertEquals(Map.of(fateId1.canonical(), "NEW", fateId2.canonical(), "NEW"),
        fateIdsFromSummary);

    // Attempt to --delete the transaction. Should not work as the Manager is still up
    ProcessInfo p = getCluster().exec(Admin.class, "fate", fateId1.canonical(), "--delete");
    assertEquals(1, p.getProcess().waitFor());
    fateIdsFromSummary = getFateIdsFromSummary();
    assertEquals(Map.of(fateId1.canonical(), "NEW", fateId2.canonical(), "NEW"),
        fateIdsFromSummary);

    // Stop MANAGER so --delete can be called
    getCluster().getClusterControl().stopAllServers(ServerType.MANAGER);
    Thread.sleep(20_000);

    // Delete the first transaction and ensure that it was deleted
    p = getCluster().exec(Admin.class, "fate", fateId1.canonical(), "--delete");
    assertEquals(0, p.getProcess().waitFor());
    String result = p.readStdOut();

    assertTrue(result.contains("Deleting transaction: " + fateId1));
    fateIdsFromSummary = getFateIdsFromSummary();
    assertEquals(Map.of(fateId2.canonical(), "NEW"), fateIdsFromSummary);

    fate.shutdown(10, TimeUnit.MINUTES);
  }

  /**
   *
   * @param printResult the output of the --print fate command
   * @return a map of each of the FateIds to their status using the output of --print
   */
  private Map<String,String> getFateIdsFromPrint(String printResult) {
    Map<String,String> fateIdToStatus = new HashMap<>();
    String lastFateIdSeen = null;
    String[] words = printResult.split(" ");
    for (String word : words) {
      if (FateId.isFateId(word)) {
        if (!fateIdToStatus.containsKey(word)) {
          lastFateIdSeen = word;
        } else {
          log.debug(
              "--print listed the same transaction more than once. This should not occur, failing");
          fail();
        }
      } else if (wordIsTStatus(word)) {
        fateIdToStatus.put(lastFateIdSeen, word);
      }
    }
    return fateIdToStatus;
  }

  /**
   *
   * @return a map of each of the FateIds to their status using the --summary command
   */
  private Map<String,String> getFateIdsFromSummary() throws Exception {
    ProcessInfo p = getCluster().exec(Admin.class, "fate", "--summary", "-j");
    assertEquals(0, p.getProcess().waitFor());
    String result = p.readStdOut();
    result = result.substring(result.indexOf("{"), result.lastIndexOf("}") + 1);
    FateSummaryReport report = FateSummaryReport.fromJson(result);
    assertNotNull(report);
    Map<String,String> fateIdToStatus = new HashMap<>();
    report.getFateDetails().forEach((d) -> {
      fateIdToStatus.put(d.getFateId(), d.getStatus());
    });
    return fateIdToStatus;
  }

  private void validateFateDetails(Set<FateTxnDetails> details, List<String> fateIdsStarted) {
    for (FateTxnDetails d : details) {
      assertTrue(fateIdsStarted.contains(d.getFateId()));
      assertEquals("IN_PROGRESS", d.getStatus());
      assertNotEquals("?", d.getStep());
      assertTrue(d.getTxName().equals("tx1") || d.getTxName().equals("tx2"));
      assertNotEquals(0, d.getRunning());
      assertEquals("[]", d.getLocksHeld().toString());
      assertEquals("[]", d.getLocksWaiting().toString());
    }
  }

  private Fate<TestEnv> initializeFate(FateStore<TestEnv> store) {
    ConfigurationCopy config = new ConfigurationCopy();
    config.set(Property.GENERAL_THREADPOOL_SIZE, "2");
    config.set(Property.MANAGER_FATE_THREADPOOL_SIZE, "2");
    return new Fate<>(new TestEnv(), store, Object::toString, config);
  }

  private boolean wordIsTStatus(String word) {
    try {
      ReadOnlyFateStore.TStatus.valueOf(word);
    } catch (IllegalArgumentException e) {
      return false;
    }
    return true;
  }
}

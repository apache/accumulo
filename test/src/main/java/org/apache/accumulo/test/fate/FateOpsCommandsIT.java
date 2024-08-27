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
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.lang.reflect.Method;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.admin.NewTableConfiguration;
import org.apache.accumulo.core.clientImpl.ClientContext;
import org.apache.accumulo.core.conf.ConfigurationCopy;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.fate.AbstractFateStore;
import org.apache.accumulo.core.fate.AdminUtil;
import org.apache.accumulo.core.fate.Fate;
import org.apache.accumulo.core.fate.FateId;
import org.apache.accumulo.core.fate.FateInstanceType;
import org.apache.accumulo.core.fate.FateStore;
import org.apache.accumulo.core.fate.MetaFateStore;
import org.apache.accumulo.core.fate.ReadOnlyFateStore;
import org.apache.accumulo.core.fate.user.UserFateStore;
import org.apache.accumulo.core.fate.zookeeper.ZooReaderWriter;
import org.apache.accumulo.core.iterators.IteratorUtil;
import org.apache.accumulo.minicluster.ServerType;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloClusterImpl.ProcessInfo;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloConfigImpl;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.util.Admin;
import org.apache.accumulo.server.util.fateCommand.FateSummaryReport;
import org.apache.accumulo.server.util.fateCommand.FateTxnDetails;
import org.apache.accumulo.test.functional.ConfigurableMacBase;
import org.apache.accumulo.test.functional.ReadWriteIT;
import org.apache.accumulo.test.functional.SlowIterator;
import org.apache.accumulo.test.util.Wait;
import org.apache.hadoop.conf.Configuration;
import org.easymock.EasyMock;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public abstract class FateOpsCommandsIT extends ConfigurableMacBase
    implements FateTestRunner<FateTestRunner.TestEnv> {

  @Override
  protected Duration defaultTimeout() {
    return Duration.ofMinutes(3);
  }

  @Override
  public void configure(MiniAccumuloConfigImpl cfg, Configuration hadoopCoreSite) {
    // Used for tests that shutdown the manager so the sleep time after shutdown isn't too long
    cfg.setProperty(Property.INSTANCE_ZK_TIMEOUT.getKey(), "10s");
  }

  @BeforeEach
  public void shutdownCompactor() throws Exception {
    // Occasionally, the summary/print cmds will see a COMMIT_COMPACTION transaction which was
    // initiated on starting the manager, causing the test to fail. Stopping the compactor fixes
    // this issue.
    getCluster().getClusterControl().stopAllServers(ServerType.COMPACTOR);
    Wait.waitFor(() -> getCompactorAddrs(getCluster().getServerContext()).isEmpty(), 60_000);
  }

  @Test
  public void testFateSummaryCommand() throws Exception {
    executeTest(this::testFateSummaryCommand);
  }

  protected void testFateSummaryCommand(FateStore<TestEnv> store, ServerContext sctx)
      throws Exception {
    // Configure Fate
    Fate<TestEnv> fate = initializeFate(store);

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
    validateFateDetails(report.getFateDetails(), 0, null);

    // create Fate transactions
    FateId fateId1 = fate.startTransaction();
    FateId fateId2 = fate.startTransaction();
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
    validateFateDetails(report.getFateDetails(), 2, fateIdsStarted);

    /*
     * Test filtering by FateIds
     */

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
    validateFateDetails(report.getFateDetails(), 2, fateIdsStarted);

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
    validateFateDetails(report.getFateDetails(), 1, fateIdsStarted);

    // validate filtering by non-existent transaction
    FateId fakeFateId = FateId.from(store.type(), UUID.randomUUID());
    p = getCluster().exec(Admin.class, "fate", fakeFateId.canonical(), "--summary", "-j");
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
    assertTrue(report.getFateIdFilter().contains(fakeFateId.canonical()));
    validateFateDetails(report.getFateDetails(), 0, fateIdsStarted);

    /*
     * Test filtering by States
     */

    // validate status filter by including only FAILED transactions, should be none
    p = getCluster().exec(Admin.class, "fate", "--summary", "-j", "-s", "FAILED");
    assertEquals(0, p.getProcess().waitFor());
    result = p.readStdOut();
    result = result.substring(result.indexOf("{"), result.lastIndexOf("}") + 1);
    report = FateSummaryReport.fromJson(result);
    assertNotNull(report);
    assertNotEquals(0, report.getReportTime());
    assertFalse(report.getStatusCounts().isEmpty());
    assertFalse(report.getStepCounts().isEmpty());
    assertFalse(report.getCmdCounts().isEmpty());
    assertEquals(Set.of("FAILED"), report.getStatusFilterNames());
    assertTrue(report.getInstanceTypesFilterNames().isEmpty());
    assertTrue(report.getFateIdFilter().isEmpty());
    validateFateDetails(report.getFateDetails(), 0, fateIdsStarted);

    // validate status filter by including only NEW transactions, should be 2
    p = getCluster().exec(Admin.class, "fate", "--summary", "-j", "-s", "NEW");
    assertEquals(0, p.getProcess().waitFor());
    result = p.readStdOut();
    result = result.substring(result.indexOf("{"), result.lastIndexOf("}") + 1);
    report = FateSummaryReport.fromJson(result);
    assertNotNull(report);
    assertNotEquals(0, report.getReportTime());
    assertFalse(report.getStatusCounts().isEmpty());
    assertFalse(report.getStepCounts().isEmpty());
    assertFalse(report.getCmdCounts().isEmpty());
    assertEquals(Set.of("NEW"), report.getStatusFilterNames());
    assertTrue(report.getInstanceTypesFilterNames().isEmpty());
    assertTrue(report.getFateIdFilter().isEmpty());
    validateFateDetails(report.getFateDetails(), 2, fateIdsStarted);

    /*
     * Test filtering by FateInstanceType
     */

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
      validateFateDetails(report.getFateDetails(), 2, fateIdsStarted);
    } else { // USER
      validateFateDetails(report.getFateDetails(), 0, fateIdsStarted);
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
      validateFateDetails(report.getFateDetails(), 0, fateIdsStarted);
    } else { // USER
      validateFateDetails(report.getFateDetails(), 2, fateIdsStarted);
    }

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

    fate.shutdown(10, TimeUnit.MINUTES);
  }

  @Test
  public void testFatePrintCommand() throws Exception {
    executeTest(this::testFatePrintCommand);
  }

  protected void testFatePrintCommand(FateStore<TestEnv> store, ServerContext sctx)
      throws Exception {
    // Configure Fate
    Fate<TestEnv> fate = initializeFate(store);

    // validate no transactions
    ProcessInfo p = getCluster().exec(Admin.class, "fate", "--print");
    assertEquals(0, p.getProcess().waitFor());
    String result = p.readStdOut();
    assertTrue(result.contains(" 0 transactions"));

    // create Fate transactions
    FateId fateId1 = fate.startTransaction();
    FateId fateId2 = fate.startTransaction();

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

    // Filter by non-existent FateId
    FateId fakeFateId = FateId.from(store.type(), UUID.randomUUID());
    p = getCluster().exec(Admin.class, "fate", fakeFateId.canonical(), "--print");
    assertEquals(0, p.getProcess().waitFor());
    result = p.readStdOut();
    fateIdsFromResult = getFateIdsFromPrint(result);
    assertEquals(0, fateIdsFromResult.size());

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
  public void testTransactionNameAndStep() throws Exception {
    executeTest(this::testTransactionNameAndStep);
  }

  protected void testTransactionNameAndStep(FateStore<TestEnv> store, ServerContext sctx)
      throws Exception {
    // Since the other tests just use NEW transactions for simplicity, there are some fields of the
    // summary and print outputs which are null and not tested for (transaction name and transaction
    // step). This test uses seeded/in progress transactions to test that the summary and print
    // commands properly output these fields.
    try (AccumuloClient client = Accumulo.newClient().from(getClientProperties()).build()) {
      final String table = getUniqueNames(1)[0];

      IteratorSetting is = new IteratorSetting(1, SlowIterator.class);
      is.addOption("sleepTime", "10000");

      NewTableConfiguration cfg = new NewTableConfiguration();
      cfg.attachIterator(is, EnumSet.of(IteratorUtil.IteratorScope.majc));
      client.tableOperations().create(table, cfg);

      ReadWriteIT.ingest(client, 10, 10, 10, 0, table);
      client.tableOperations().flush(table, null, null, true);

      // create 2 Fate transactions
      client.tableOperations().compact(table, null, null, false, false);
      client.tableOperations().compact(table, null, null, false, false);
      List<String> fateIdsStarted = new ArrayList<>();

      ProcessInfo p = getCluster().exec(Admin.class, "fate", "--summary", "-j");
      assertEquals(0, p.getProcess().waitFor());

      String result = p.readStdOut();
      result = result.substring(result.indexOf("{"), result.lastIndexOf("}") + 1);
      FateSummaryReport report = FateSummaryReport.fromJson(result);

      // Validate transaction name and transaction step from summary command

      for (FateTxnDetails d : report.getFateDetails()) {
        assertEquals("TABLE_COMPACT", d.getTxName());
        assertEquals("CompactionDriver", d.getStep());
        fateIdsStarted.add(d.getFateId());
      }
      assertEquals(2, fateIdsStarted.size());

      p = getCluster().exec(Admin.class, "fate", "--print");
      assertEquals(0, p.getProcess().waitFor());
      result = p.readStdOut();

      // Validate transaction name and transaction step from print command

      String[] lines = result.split("\n");
      // Filter out the result to just include the info about the transactions
      List<String> transactionInfo = Arrays.stream(lines)
          .filter(
              line -> line.contains(fateIdsStarted.get(0)) || line.contains(fateIdsStarted.get(1)))
          .collect(Collectors.toList());
      assertEquals(2, transactionInfo.size());
      for (String info : transactionInfo) {
        assertTrue(info.contains("TABLE_COMPACT"));
        assertTrue(info.contains("op: CompactionDriver"));
      }

      client.tableOperations().delete(table);
    }
  }

  @Test
  public void testFateCancelCommand() throws Exception {
    executeTest(this::testFateCancelCommand);
  }

  protected void testFateCancelCommand(FateStore<TestEnv> store, ServerContext sctx)
      throws Exception {
    // Configure Fate
    Fate<TestEnv> fate = initializeFate(store);

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

  @Test
  public void testFatePrintAndSummaryCommandsWithInProgressTxns() throws Exception {
    executeTest(this::testFatePrintAndSummaryCommandsWithInProgressTxns);
  }

  protected void testFatePrintAndSummaryCommandsWithInProgressTxns(FateStore<TestEnv> store,
      ServerContext sctx) throws Exception {
    // This test was written for an issue with the 'admin fate --print' and 'admin fate --summary'
    // commands where transactions could complete mid-print causing the command to fail. These
    // commands first get a list of the transactions and then probe for info on the transactions.
    // If a transaction completed between getting the list and probing for info on that
    // transaction, the command would fail. This test ensures that this problem has been fixed
    // (if the tx no longer exists, it should just be ignored so the print/summary can complete).
    FateStore<TestEnv> mockedStore;

    // This error was occurring in AdminUtil.getTransactionStatus(), so we will test this method.
    if (store.type().equals(FateInstanceType.USER)) {
      Method listMethod = UserFateStore.class.getMethod("list");
      mockedStore =
          EasyMock.createMockBuilder(UserFateStore.class).withConstructor(ClientContext.class)
              .withArgs(sctx).addMockedMethod(listMethod).createMock();
    } else {
      Method listMethod = MetaFateStore.class.getMethod("list");
      mockedStore = EasyMock.createMockBuilder(MetaFateStore.class)
          .withConstructor(String.class, ZooReaderWriter.class)
          .withArgs(sctx.getZooKeeperRoot() + Constants.ZFATE, sctx.getZooReaderWriter())
          .addMockedMethod(listMethod).createMock();
    }

    // 3 FateIds, two that exist and one that does not. We are simulating that a transaction that
    // doesn't exist is accessed in getTransactionStatus() and ensuring that this doesn't cause
    // the method to fail or have any unexpected behavior.
    FateId tx1 = store.create();
    FateId tx2 = FateId.from(store.type(), UUID.randomUUID());
    FateId tx3 = store.create();

    List<ReadOnlyFateStore.FateIdStatus> fateIdStatusList =
        List.of(createFateIdStatus(tx1), createFateIdStatus(tx2), createFateIdStatus(tx3));
    expect(mockedStore.list()).andReturn(fateIdStatusList.stream()).once();

    replay(mockedStore);

    AdminUtil.FateStatus status = null;
    try {
      status = AdminUtil.getTransactionStatus(Map.of(store.type(), mockedStore), null, null, null,
          new HashMap<>(), new HashMap<>());
    } catch (Exception e) {
      fail("An unexpected error occurred in getTransactionStatus():\n" + e);
    }

    verify(mockedStore);

    assertNotNull(status);
    // All three should be returned
    assertEquals(3, status.getTransactions().size());
    assertEquals(status.getTransactions().stream().map(AdminUtil.TransactionStatus::getFateId)
        .collect(Collectors.toList()), List.of(tx1, tx2, tx3));
    // The two real FateIds should have NEW status and the fake one should be UNKNOWN
    assertEquals(
        status.getTransactions().stream().map(AdminUtil.TransactionStatus::getStatus)
            .collect(Collectors.toList()),
        List.of(ReadOnlyFateStore.TStatus.NEW, ReadOnlyFateStore.TStatus.UNKNOWN,
            ReadOnlyFateStore.TStatus.NEW));
    // None of them should have a name since none of them were seeded with work
    assertEquals(status.getTransactions().stream().map(AdminUtil.TransactionStatus::getTxName)
        .collect(Collectors.toList()), Arrays.asList(null, null, null));
    // None of them should have a Repo since none of them were seeded with work
    assertEquals(status.getTransactions().stream().map(AdminUtil.TransactionStatus::getTop)
        .collect(Collectors.toList()), Arrays.asList(null, null, null));
    // The FateId that doesn't exist should have a creation time of 0, the others should not
    List<Long> timeCreated = status.getTransactions().stream()
        .map(AdminUtil.TransactionStatus::getTimeCreated).collect(Collectors.toList());
    assertNotEquals(timeCreated.get(0), 0);
    assertEquals(timeCreated.get(1), 0);
    assertNotEquals(timeCreated.get(2), 0);
    // All should have the store.type() type
    assertEquals(status.getTransactions().stream().map(AdminUtil.TransactionStatus::getInstanceType)
        .collect(Collectors.toList()), List.of(store.type(), store.type(), store.type()));
  }

  private ReadOnlyFateStore.FateIdStatus createFateIdStatus(FateId fateId) {
    return new AbstractFateStore.FateIdStatusBase(fateId) {
      @Override
      public ReadOnlyFateStore.TStatus getStatus() {
        return null;
      }
    };
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

  /**
   * Validates the fate details of NEW transactions
   *
   * @param details the fate details from the {@link FateSummaryReport}
   * @param expDetailsSize the expected size of details
   * @param fateIdsStarted the list of fate ids that have been started
   */
  private void validateFateDetails(Set<FateTxnDetails> details, int expDetailsSize,
      List<String> fateIdsStarted) {
    assertEquals(expDetailsSize, details.size());
    for (FateTxnDetails d : details) {
      assertTrue(fateIdsStarted.contains(d.getFateId()));
      assertEquals("NEW", d.getStatus());
      assertEquals("?", d.getStep());
      assertEquals("?", d.getTxName());
      assertNotEquals(0, d.getRunning());
      assertEquals("[]", d.getLocksHeld().toString());
      assertEquals("[]", d.getLocksWaiting().toString());
    }
  }

  private Fate<TestEnv> initializeFate(FateStore<TestEnv> store) {
    ConfigurationCopy config = new ConfigurationCopy();
    config.set(Property.GENERAL_THREADPOOL_SIZE, "2");
    config.set(Property.MANAGER_FATE_THREADPOOL_SIZE, "1");
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

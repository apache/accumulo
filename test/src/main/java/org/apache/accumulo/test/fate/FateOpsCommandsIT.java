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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.conf.ConfigurationCopy;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.fate.Fate;
import org.apache.accumulo.core.fate.FateId;
import org.apache.accumulo.core.fate.FateInstanceType;
import org.apache.accumulo.core.fate.FateStore;
import org.apache.accumulo.core.fate.ReadOnlyFateStore;
import org.apache.accumulo.minicluster.ServerType;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloClusterImpl.ProcessInfo;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloConfigImpl;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.util.Admin;
import org.apache.accumulo.server.util.fateCommand.FateSummaryReport;
import org.apache.accumulo.test.functional.ConfigurableMacBase;
import org.apache.hadoop.conf.Configuration;
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

  @Test
  public void testFateSummaryCommand() throws Exception {
    executeTest(this::testFateSummaryCommand);
  }

  protected void testFateSummaryCommand(FateStore<TestEnv> store, ServerContext sctx)
      throws Exception {
    // Configure Fate
    Fate<TestEnv> fate = initializeFate(store);
    // Occasionally, the summary/print cmds will see a COMMIT_COMPACTION transaction which was
    // initiated on starting the manager, causing the test to fail. Stopping the compactor fixes
    // this issue.
    getCluster().getClusterControl().stopAllServers(ServerType.COMPACTOR);
    Thread.sleep(20_000);

    // validate blank report, no transactions have started
    ProcessInfo p = getCluster().exec(Admin.class, "fate", "--summary", "-j", "-s", "NEW", "-s",
        "IN_PROGRESS", "-s", "FAILED");
    assertEquals(0, p.getProcess().waitFor());
    String result = p.readStdOut();
    result = result.substring(result.indexOf("{"), result.lastIndexOf("}") + 1);
    FateSummaryReport report = FateSummaryReport.fromJson(result);
    assertNotNull(report);
    assertNotEquals(0, report.getReportTime());
    assertTrue(report.getStatusCounts().isEmpty());
    assertTrue(report.getStepCounts().isEmpty());
    assertTrue(report.getCmdCounts().isEmpty());
    assertEquals(Set.of("FAILED", "IN_PROGRESS", "NEW"), report.getStatusFilterNames());
    assertTrue(report.getInstanceTypesFilterNames().isEmpty());
    assertTrue(report.getFateIdFilter().isEmpty());
    assertEquals(0, report.getFateDetails().size());

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
    assertEquals(2, report.getFateDetails().size());
    ArrayList<String> fateIdsFromResult1 = new ArrayList<>();
    report.getFateDetails().forEach((d) -> {
      fateIdsFromResult1.add(d.getFateId());
    });
    assertTrue(fateIdsFromResult1.containsAll(fateIdsStarted));

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
    ArrayList<String> fateIdsFromResult2 = new ArrayList<>();
    report.getFateDetails().forEach((d) -> {
      fateIdsFromResult2.add(d.getFateId());
    });
    assertTrue(fateIdsFromResult2.containsAll(fateIdsStarted));

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
    ArrayList<String> fateIdsFromResult3 = new ArrayList<>();
    report.getFateDetails().forEach((d) -> {
      fateIdsFromResult3.add(d.getFateId());
    });
    assertTrue(fateIdsFromResult3.contains(fateId1.canonical()));
    assertFalse(fateIdsFromResult3.contains(fateId2.canonical()));

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
      ArrayList<String> fateIdsFromResult4 = new ArrayList<>();
      report.getFateDetails().forEach((d) -> {
        fateIdsFromResult4.add(d.getFateId());
      });
      assertTrue(fateIdsFromResult4.containsAll(fateIdsStarted));
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
      ArrayList<String> fateIdsFromResult4 = new ArrayList<>();
      report.getFateDetails().forEach((d) -> {
        fateIdsFromResult4.add(d.getFateId());
      });
      assertTrue(fateIdsFromResult4.containsAll(fateIdsStarted));
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
    // Occasionally, the summary/print cmds will see a COMMIT_COMPACTION transaction which was
    // initiated on starting the manager, causing the test to fail. Stopping the compactor fixes
    // this issue.
    getCluster().getClusterControl().stopAllServers(ServerType.COMPACTOR);
    Thread.sleep(20_000);

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
    // Configure Fate
    Fate<TestEnv> fate = initializeFate(store);
    // Occasionally, the summary/print cmds will see a COMMIT_COMPACTION transaction which was
    // initiated on starting the manager, causing the test to fail. Stopping the compactor fixes
    // this issue.
    getCluster().getClusterControl().stopAllServers(ServerType.COMPACTOR);
    Thread.sleep(20_000);

    // validate no transactions
    ProcessInfo p = getCluster().exec(Admin.class, "fate", "--print");
    assertEquals(0, p.getProcess().waitFor());
    String result = p.readStdOut();
    assertTrue(result.contains("0 transactions"));

    // create Fate transactions
    FateId fateId1 = fate.startTransaction();
    FateId fateId2 = fate.startTransaction();
    List<String> fateIdsStarted = List.of(fateId1.canonical(), fateId2.canonical());

    // Get all transactions. Should be 2 FateIds with a NEW status
    p = getCluster().exec(Admin.class, "fate", "--print");
    assertEquals(0, p.getProcess().waitFor());
    result = p.readStdOut();
    Map<String,String> fateIdsFromResult = getFateIdsFromPrint(result);
    assertEquals(2, fateIdsFromResult.size());
    assertTrue(fateIdsFromResult.keySet().containsAll(fateIdsStarted));
    assertEquals("NEW", fateIdsFromResult.get(fateId1.canonical()));
    assertEquals("NEW", fateIdsFromResult.get(fateId2.canonical()));

    /*
     * Test filtering by States
     */

    // Filter by NEW state
    p = getCluster().exec(Admin.class, "fate", "--print", "-s", "NEW");
    assertEquals(0, p.getProcess().waitFor());
    result = p.readStdOut();
    fateIdsFromResult = getFateIdsFromPrint(result);
    assertEquals(2, fateIdsFromResult.size());
    assertTrue(fateIdsFromResult.keySet().containsAll(fateIdsStarted));

    // Filter by FAILED state
    p = getCluster().exec(Admin.class, "fate", "--print", "-s", "FAILED");
    assertEquals(0, p.getProcess().waitFor());
    result = p.readStdOut();
    fateIdsFromResult = getFateIdsFromPrint(result);
    assertEquals(0, fateIdsFromResult.size());

    /*
     * Test filtering by FateIds
     */

    // Filter by one FateId
    p = getCluster().exec(Admin.class, "fate", fateId1.canonical(), "--print");
    assertEquals(0, p.getProcess().waitFor());
    result = p.readStdOut();
    fateIdsFromResult = getFateIdsFromPrint(result);
    assertEquals(1, fateIdsFromResult.size());
    assertTrue(fateIdsFromResult.containsKey(fateId1.canonical()));

    // Filter by both FateIds
    p = getCluster().exec(Admin.class, "fate", fateId1.canonical(), fateId2.canonical(), "--print");
    assertEquals(0, p.getProcess().waitFor());
    result = p.readStdOut();
    fateIdsFromResult = getFateIdsFromPrint(result);
    assertEquals(2, fateIdsFromResult.size());
    assertTrue(fateIdsFromResult.keySet().containsAll(fateIdsStarted));

    /*
     * Test filtering by FateInstanceType
     */

    // Test filter by USER FateInstanceType
    p = getCluster().exec(Admin.class, "fate", "--print", "-t", "USER");
    assertEquals(0, p.getProcess().waitFor());
    result = p.readStdOut();
    fateIdsFromResult = getFateIdsFromPrint(result);
    if (store.type() == FateInstanceType.META) {
      assertEquals(0, fateIdsFromResult.size());
    } else { // USER
      assertEquals(2, fateIdsFromResult.size());
      assertTrue(fateIdsFromResult.keySet().containsAll(fateIdsStarted));
    }

    // Test filter by META FateInstanceType
    p = getCluster().exec(Admin.class, "fate", "--print", "-t", "META");
    assertEquals(0, p.getProcess().waitFor());
    result = p.readStdOut();
    fateIdsFromResult = getFateIdsFromPrint(result);
    if (store.type() == FateInstanceType.META) {
      assertEquals(2, fateIdsFromResult.size());
      assertTrue(fateIdsFromResult.keySet().containsAll(fateIdsStarted));
    } else { // USER
      assertEquals(0, fateIdsFromResult.size());
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
    Thread.sleep(20_000);

    // Start some transactions
    FateId fateId1 = fate.startTransaction();
    FateId fateId2 = fate.startTransaction();
    List<String> fateIdsStarted = List.of(fateId1.canonical(), fateId2.canonical());

    // Check that summary output lists both the transactions with a NEW status
    Map<String,String> fateIdsFromSummary = getFateIdsFromSummary();
    assertEquals(2, fateIdsFromSummary.size());
    assertTrue(fateIdsFromSummary.keySet().containsAll(fateIdsStarted));
    assertEquals("NEW", fateIdsFromSummary.get(fateId1.canonical()));
    assertEquals("NEW", fateIdsFromSummary.get(fateId2.canonical()));

    // Cancel the first transaction and ensure that it was cancelled
    ProcessInfo p = getCluster().exec(Admin.class, "fate", fateId1.canonical(), "--cancel");
    assertEquals(0, p.getProcess().waitFor());
    String result = p.readStdOut();

    assertTrue(result
        .contains("transaction " + fateId1.canonical() + " was cancelled or already completed"));
    fateIdsFromSummary = getFateIdsFromSummary();
    assertEquals(2, fateIdsFromSummary.size());
    assertTrue(fateIdsFromSummary.keySet().containsAll(fateIdsStarted));
    assertEquals("FAILED", fateIdsFromSummary.get(fateId1.canonical()));
    assertEquals("NEW", fateIdsFromSummary.get(fateId2.canonical()));

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
    Thread.sleep(20_000);

    // Start some transactions
    FateId fateId1 = fate.startTransaction();
    FateId fateId2 = fate.startTransaction();
    List<String> fateIdsStarted = List.of(fateId1.canonical(), fateId2.canonical());

    // Check that summary output lists both the transactions with a NEW status
    Map<String,String> fateIdsFromSummary = getFateIdsFromSummary();
    assertEquals(2, fateIdsFromSummary.size());
    assertTrue(fateIdsFromSummary.keySet().containsAll(fateIdsStarted));
    assertEquals("NEW", fateIdsFromSummary.get(fateId1.canonical()));
    assertEquals("NEW", fateIdsFromSummary.get(fateId2.canonical()));

    // Stop MANAGER so --fail can be called
    getCluster().getClusterControl().stopAllServers(ServerType.MANAGER);
    Thread.sleep(20_000);

    // Fail the first transaction and ensure that it was failed
    ProcessInfo p = getCluster().exec(Admin.class, "fate", fateId1.canonical(), "--fail");
    assertEquals(0, p.getProcess().waitFor());
    String result = p.readStdOut();

    assertTrue(result.contains("Failing transaction: " + fateId1));
    fateIdsFromSummary = getFateIdsFromSummary();
    assertEquals(2, fateIdsFromSummary.size());
    assertTrue(fateIdsFromSummary.keySet().containsAll(fateIdsStarted));
    assertTrue(fateIdsFromSummary.get(fateId1.canonical()).equals("FAILED_IN_PROGRESS")
        || fateIdsFromSummary.get(fateId1.canonical()).equals("FAILED"));
    assertEquals("NEW", fateIdsFromSummary.get(fateId2.canonical()));

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
    Thread.sleep(20_000);

    // Start some transactions
    FateId fateId1 = fate.startTransaction();
    FateId fateId2 = fate.startTransaction();
    List<String> fateIdsStarted = List.of(fateId1.canonical(), fateId2.canonical());

    // Check that summary output lists both the transactions with a NEW status
    Map<String,String> fateIdsFromSummary = getFateIdsFromSummary();
    assertEquals(2, fateIdsFromSummary.size());
    assertTrue(fateIdsFromSummary.keySet().containsAll(fateIdsStarted));
    assertEquals("NEW", fateIdsFromSummary.get(fateId1.canonical()));
    assertEquals("NEW", fateIdsFromSummary.get(fateId2.canonical()));

    // Stop MANAGER so --delete can be called
    getCluster().getClusterControl().stopAllServers(ServerType.MANAGER);
    Thread.sleep(20_000);

    // Delete the first transaction and ensure that it was deleted
    ProcessInfo p = getCluster().exec(Admin.class, "fate", fateId1.canonical(), "--delete");
    assertEquals(0, p.getProcess().waitFor());
    String result = p.readStdOut();

    assertTrue(result.contains("Deleting transaction: " + fateId1));
    fateIdsFromSummary = getFateIdsFromSummary();
    assertEquals(1, fateIdsFromSummary.size());
    assertFalse(fateIdsFromSummary.containsKey(fateId1.canonical()));
    assertTrue(fateIdsFromSummary.containsKey(fateId2.canonical()));
    assertEquals("NEW", fateIdsFromSummary.get(fateId2.canonical()));

    fate.shutdown(10, TimeUnit.MINUTES);
  }

  /**
   *
   * @param printResult the output of the --print fate command
   * @return a map of each of the FateIds to their status using the output of --print
   */
  private Map<String,String> getFateIdsFromPrint(String printResult) throws Exception {
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

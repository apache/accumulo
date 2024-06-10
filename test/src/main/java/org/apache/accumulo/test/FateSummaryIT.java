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
package org.apache.accumulo.test;

import static org.apache.accumulo.core.util.compaction.ExternalCompactionUtil.getCompactorAddrs;
import static org.easymock.EasyMock.anyLong;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.stream.Collectors;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.admin.NewTableConfiguration;
import org.apache.accumulo.core.fate.AdminUtil;
import org.apache.accumulo.core.fate.Repo;
import org.apache.accumulo.core.fate.ZooStore;
import org.apache.accumulo.core.fate.zookeeper.ZooReaderWriter;
import org.apache.accumulo.core.iterators.IteratorUtil.IteratorScope;
import org.apache.accumulo.core.util.FastFormat;
import org.apache.accumulo.minicluster.ServerType;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloClusterImpl.ProcessInfo;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloConfigImpl;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.util.Admin;
import org.apache.accumulo.server.util.fateCommand.FateSummaryReport;
import org.apache.accumulo.test.functional.ConfigurableMacBase;
import org.apache.accumulo.test.functional.ReadWriteIT;
import org.apache.accumulo.test.functional.SlowIterator;
import org.apache.accumulo.test.util.Wait;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.zookeeper.KeeperException;
import org.easymock.EasyMock;
import org.junit.jupiter.api.Test;

public class FateSummaryIT extends ConfigurableMacBase {

  @Override
  protected Duration defaultTimeout() {
    return Duration.ofMinutes(2);
  }

  @Override
  public void configure(MiniAccumuloConfigImpl cfg, Configuration hadoopCoreSite) {}

  @Test
  public void testFateSummaryCommandWithSlowCompaction() throws Exception {
    // Occasionally, the summary/print cmds will see a COMMIT_COMPACTION transaction which was
    // initiated on starting the manager, causing the test to fail. Stopping the compactor fixes
    // this issue.
    getCluster().getClusterControl().stopAllServers(ServerType.COMPACTOR);
    Wait.waitFor(() -> getCompactorAddrs(getCluster().getServerContext()).isEmpty(), 60_000);

    try (AccumuloClient client = Accumulo.newClient().from(getClientProperties()).build()) {
      String namespace = "ns1";
      final String table = namespace + "." + getUniqueNames(1)[0];
      client.namespaceOperations().create(namespace);

      SortedSet<Text> splits = new TreeSet<Text>();
      splits.add(new Text("h"));
      splits.add(new Text("m"));
      splits.add(new Text("r"));
      splits.add(new Text("w"));
      IteratorSetting is = new IteratorSetting(1, SlowIterator.class);
      is.addOption("sleepTime", "10000");

      NewTableConfiguration cfg = new NewTableConfiguration();
      cfg.withSplits(splits);
      cfg.attachIterator(is, EnumSet.of(IteratorScope.majc));
      client.tableOperations().create(table, cfg);

      ReadWriteIT.ingest(client, 10, 10, 10, 0, table);
      client.tableOperations().flush(table, null, null, true);

      // validate blank report, compactions have not started yet
      ProcessInfo p = getCluster().exec(Admin.class, "fate", "--summary", "-j", "-s", "NEW", "-s",
          "IN_PROGRESS", "-s", "FAILED");
      assertEquals(0, p.getProcess().waitFor());
      String result = p.readStdOut();
      result = result.substring(result.indexOf("{"), result.lastIndexOf("}") + 1);
      FateSummaryReport report = FateSummaryReport.fromJson(result);
      assertNotNull(report);
      assertNotEquals(0, report.getReportTime());
      Set<String> expected = new HashSet<>();
      expected.add("FAILED");
      expected.add("IN_PROGRESS");
      expected.add("NEW");
      assertEquals(expected, report.getStatusFilterNames());
      assertEquals(Map.of(), report.getStatusCounts());
      assertEquals(Map.of(), report.getStepCounts());
      assertEquals(Map.of(), report.getCmdCounts());

      // create Fate transactions
      client.tableOperations().compact(table, null, null, false, false);
      client.tableOperations().compact(table, null, null, false, false);

      // validate no filters
      p = getCluster().exec(Admin.class, "fate", "--summary", "-j");
      assertEquals(0, p.getProcess().waitFor());
      result = p.readStdOut();
      result = result.substring(result.indexOf("{"), result.lastIndexOf("}") + 1);
      report = FateSummaryReport.fromJson(result);
      assertNotNull(report);
      assertNotEquals(0, report.getReportTime());
      assertEquals(Set.of(), report.getStatusFilterNames());
      assertFalse(report.getStatusCounts().isEmpty());
      assertFalse(report.getStepCounts().isEmpty());
      assertFalse(report.getCmdCounts().isEmpty());
      assertEquals(2, report.getFateDetails().size());
      ArrayList<String> txns = new ArrayList<>();
      report.getFateDetails().forEach((d) -> {
        txns.add(d.getTxnId());
      });
      assertEquals(2, txns.size());

      // validate tx ids
      p = getCluster().exec(Admin.class, "fate", txns.get(0), txns.get(1), "--summary", "-j");
      assertEquals(0, p.getProcess().waitFor());
      result = p.readStdOut();
      result = result.substring(result.indexOf("{"), result.lastIndexOf("}") + 1);
      report = FateSummaryReport.fromJson(result);
      assertNotNull(report);
      assertNotEquals(0, report.getReportTime());
      assertEquals(Set.of(), report.getStatusFilterNames());
      assertFalse(report.getStatusCounts().isEmpty());
      assertFalse(report.getStepCounts().isEmpty());
      assertFalse(report.getCmdCounts().isEmpty());
      assertEquals(2, report.getFateDetails().size());

      // validate filter by including only FAILED transactions, should be none
      p = getCluster().exec(Admin.class, "fate", "--summary", "-j", "-s", "FAILED");
      assertEquals(0, p.getProcess().waitFor());
      result = p.readStdOut();
      result = result.substring(result.indexOf("{"), result.lastIndexOf("}") + 1);
      report = FateSummaryReport.fromJson(result);
      assertNotNull(report);
      assertNotEquals(0, report.getReportTime());
      assertEquals(Set.of("FAILED"), report.getStatusFilterNames());
      assertFalse(report.getStatusCounts().isEmpty());
      assertFalse(report.getStepCounts().isEmpty());
      assertFalse(report.getCmdCounts().isEmpty());
      assertEquals(0, report.getFateDetails().size());

      client.tableOperations().delete(table);
    }
  }

  @Test
  public void testFatePrintAndSummaryCommandsWithInProgressTxns() throws Exception {
    // This test was written for an issue with the 'admin fate --print' and 'admin fate --summary'
    // commands where ZK NoNodeExceptions could occur. These commands first get a list of the
    // transactions and then probe for info on these transactions. If a transaction completes
    // between getting the list and probing for info on that transaction, a NoNodeException would
    // occur causing the cmd to fail. This test ensures that this problem has been fixed (if the
    // tx no longer exists, it should just be ignored so the print/summary can complete).
    ServerContext sctx = getCluster().getServerContext();

    // This error was occurring in AdminUtil.getTransactionStatus(). One of the methods that is
    // called which may throw the NNE is top(), so we will mock this method to sometimes throw a
    // NNE and ensure it is handled/ignored within getTransactionStatus()
    ZooStore<String> zs = EasyMock.createMockBuilder(ZooStore.class)
        .withConstructor(String.class, ZooReaderWriter.class)
        .withArgs(sctx.getZooKeeperRoot() + Constants.ZFATE, sctx.getZooReaderWriter())
        .addMockedMethod("top").addMockedMethod("list").createMock();
    // Create 3 transactions, when iterating through the list of transactions in
    // getTransactionStatus(), the 2nd transaction should cause a NNE which should be
    // handled/ignored in getTransactionStatus(). The other two transactions should still
    // be returned.
    long tx1 = zs.create();
    long tx2 = zs.create();
    long tx3 = zs.create();
    // Mock list() to ensure same order every run
    expect(zs.list()).andReturn(List.of(tx1, tx2, tx3)).once();

    expect(zs.top(anyLong())).andReturn(new TestRepo()).once();
    expect(zs.top(anyLong())).andThrow(new RuntimeException(new KeeperException.NoNodeException()))
        .once();
    expect(zs.top(anyLong())).andReturn(new TestRepo()).once();
    replay(zs);

    AdminUtil.FateStatus status = null;
    try {
      status = AdminUtil.getTransactionStatus(zs, null, null, new HashMap<>(), new HashMap<>());
    } catch (Exception e) {
      fail(
          "Either an unexpected error occurred in getTransactionStatus() or the NoNodeException which"
              + " is expected to be handled in getTransactionStatus() was not handled. Error:\n"
              + e);
    }
    verify(zs);
    assertNotNull(status);
    assertEquals(2, status.getTransactions().size());
    assertTrue(status.getTransactions().stream().map(AdminUtil.TransactionStatus::getTxid)
        .collect(Collectors.toList())
        .containsAll(List.of(FastFormat.toHexString(tx1), FastFormat.toHexString(tx3))));
  }

  private static class TestRepo implements Repo<String> {

    private static final long serialVersionUID = 1L;

    @Override
    public long isReady(long tid, String environment) throws Exception {
      return 0;
    }

    @Override
    public String getName() {
      return null;
    }

    @Override
    public Repo<String> call(long tid, String environment) throws Exception {
      return null;
    }

    @Override
    public void undo(long tid, String environment) throws Exception {

    }

    @Override
    public String getReturn() {
      return null;
    }
  }
}

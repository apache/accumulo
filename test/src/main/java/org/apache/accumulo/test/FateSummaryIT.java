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
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.time.Duration;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.admin.NewTableConfiguration;
import org.apache.accumulo.core.iterators.IteratorUtil;
import org.apache.accumulo.core.iterators.IteratorUtil.IteratorScope;
import org.apache.accumulo.minicluster.ServerType;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloClusterImpl.ProcessInfo;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloConfigImpl;
import org.apache.accumulo.server.util.Admin;
import org.apache.accumulo.server.util.fateCommand.FateSummaryReport;
import org.apache.accumulo.test.functional.ConfigurableMacBase;
import org.apache.accumulo.test.functional.ReadWriteIT;
import org.apache.accumulo.test.functional.SlowIterator;
import org.apache.accumulo.test.util.Wait;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
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

    String[] commandsToTest = {"--print", "--summary"};
    // We want to have enough transactions to give enough opportunity for a transaction to
    // complete mid-print
    final int numTxns = 250;
    final String table = getUniqueNames(1)[0];

    // Occasionally, the summary/print cmds will see a COMMIT_COMPACTION transaction which was
    // initiated on starting the manager, causing the test to fail. Stopping the compactor fixes
    // this issue.
    getCluster().getClusterControl().stopAllServers(ServerType.COMPACTOR);
    Wait.waitFor(() -> getCompactorAddrs(getCluster().getServerContext()).isEmpty(), 60_000);

    try (AccumuloClient client = Accumulo.newClient().from(getClientProperties()).build()) {
      for (String command : commandsToTest) {
        IteratorSetting is = new IteratorSetting(1, SlowIterator.class);
        is.addOption("sleepTime", "2");

        NewTableConfiguration cfg = new NewTableConfiguration();
        cfg.attachIterator(is, EnumSet.of(IteratorUtil.IteratorScope.majc));
        client.tableOperations().create(table, cfg);

        ReadWriteIT.ingest(client, 10, 1, 10, 0, table);
        client.tableOperations().flush(table, null, null, true);

        // validate no transactions
        ProcessInfo p = execAdminFateCommand(command);
        assertEquals(0, p.getProcess().waitFor());
        String result = p.readStdOut();
        assertTrue(noTransactions(result, command));

        // We want transactions which take some time to complete since we don't want them
        // to complete before the call to print (hence the sleep time iterator), but we
        // also don't want them to take too long to complete since in that case we
        // may not see transactions complete mid-print

        // create 250 txns each taking >= 20ms to complete >= 5 seconds total
        for (int i = 0; i < numTxns; i++) {
          // Initiate compaction to create txn. This compaction will take >= 20ms to complete
          // ((10 key values) * (2ms sleep time / key value))
          client.tableOperations().compact(table, null, null, false, false);
        }

        // Keep printing until we see a transaction complete mid-print or until we run out of
        // transactions (they all complete).
        // Realistically, should only take 1 or 2 iterations to see a transaction complete
        // mid-print.
        do {
          // Execute the command when transactions are currently running and may complete mid-print
          p = execAdminFateCommand(command);
          // Previously, this check could fail due to a ZK NoNodeException
          assertEquals(0, p.getProcess().waitFor());
          result = p.readStdOut();
          // A transaction should have completed mid-print and been ignored
        } while (!result.contains("Tried to get info on a since completed transaction - ignoring")
            && !noTransactions(result, command));

        if (noTransactions(result, command)) {
          // Fail since we printed until all transactions have completed and didn't see a
          // transaction complete mid-print.
          // This is highly unlikely to have occurred.
          fail();
        }
        // Otherwise, we saw 'Tried to get info on a since completed transaction - ignoring', so
        // test passes
        client.tableOperations().delete(table);
      }
    }
  }

  private boolean noTransactions(String result, String command) {
    if (command.equals("--print")) {
      return result.contains(" 0 transactions");
    } else { // --summary
      result = result.substring(result.indexOf("{"), result.lastIndexOf("}") + 1);
      FateSummaryReport report = FateSummaryReport.fromJson(result);
      return report != null && report.getReportTime() != 0 && report.getStatusCounts().isEmpty()
          && report.getStepCounts().isEmpty() && report.getCmdCounts().isEmpty()
          && report.getStatusFilterNames().isEmpty() && report.getFateDetails().isEmpty();
    }
  }

  private ProcessInfo execAdminFateCommand(String command) throws Exception {
    if (command.equals("--print")) {
      return getCluster().exec(Admin.class, "fate", command);
    } else { // --summary
      return getCluster().exec(Admin.class, "fate", command, "-j");
    }
  }
}

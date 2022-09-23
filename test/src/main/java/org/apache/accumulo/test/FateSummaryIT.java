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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

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
import org.apache.accumulo.core.iterators.IteratorUtil.IteratorScope;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloClusterImpl.ProcessInfo;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloConfigImpl;
import org.apache.accumulo.server.util.Admin;
import org.apache.accumulo.server.util.fateCommand.FateSummaryReport;
import org.apache.accumulo.test.functional.ConfigurableMacBase;
import org.apache.accumulo.test.functional.ReadWriteIT;
import org.apache.accumulo.test.functional.SlowIterator;
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
      client.tableOperations().flush(table);

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

    }
  }

}

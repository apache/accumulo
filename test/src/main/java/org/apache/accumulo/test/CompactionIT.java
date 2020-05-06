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

package org.apache.accumulo.test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.CompactionConfig;
import org.apache.accumulo.core.client.admin.NewTableConfiguration;
import org.apache.accumulo.core.client.admin.PluginConfig;
import org.apache.accumulo.core.client.admin.compaction.CompactableFile;
import org.apache.accumulo.core.client.admin.compaction.TooManyDeletesSelector;
import org.apache.accumulo.core.client.summary.SummarizerConfiguration;
import org.apache.accumulo.core.client.summary.summarizers.DeletesSummarizer;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.metadata.StoredTabletFile;
import org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType;
import org.apache.accumulo.core.metadata.schema.TabletsMetadata;
import org.apache.accumulo.core.spi.compaction.CompactionExecutorId;
import org.apache.accumulo.core.spi.compaction.CompactionKind;
import org.apache.accumulo.core.spi.compaction.CompactionPlan;
import org.apache.accumulo.core.spi.compaction.CompactionPlanner;
import org.apache.accumulo.harness.SharedMiniClusterBase;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class CompactionIT extends SharedMiniClusterBase {

  public static class TestPlanner implements CompactionPlanner {

    private int filesPerCompaction;
    private List<CompactionExecutorId> executorIds;
    private EnumSet<CompactionKind> kindsToProcess = EnumSet.noneOf(CompactionKind.class);
    private Random rand = new Random();

    @Override
    public void init(InitParameters params) {
      var executors = Integer.parseInt(params.getOptions().get("executors"));
      this.filesPerCompaction = Integer.parseInt(params.getOptions().get("filesPerCompaction"));
      this.executorIds = new ArrayList<>();
      for (String kind : params.getOptions().get("process").split(",")) {
        kindsToProcess.add(CompactionKind.valueOf(kind.toUpperCase()));
      }

      for (int i = 0; i < executors; i++) {
        var ceid = params.getExecutorManager().createExecutor("e" + i, 2);
        executorIds.add(ceid);
      }

    }

    static String getFirstChar(CompactableFile cf) {
      return cf.getFileName().substring(0, 1);
    }

    @Override
    public CompactionPlan makePlan(PlanningParameters params) {

      if (Boolean.parseBoolean(params.getExecutionHints().getOrDefault("compact_all", "false"))) {
        return params.createPlanBuilder()
            .addJob(1, executorIds.get(rand.nextInt(executorIds.size())), params.getCandidates())
            .build();
      }

      if (kindsToProcess.contains(params.getKind())) {
        var planBuilder = params.createPlanBuilder();

        // Group files by first char, like F for flush files or C for compaction produced files.
        // This prevents F and C files from compacting together, which makes it easy to reason about
        // the number of expected files produced by compactions from known number of F files.
        params.getCandidates().stream().collect(Collectors.groupingBy(TestPlanner::getFirstChar))
            .values().forEach(files -> {
              for (int i = filesPerCompaction; i <= files.size(); i += filesPerCompaction) {
                planBuilder.addJob(1, executorIds.get(rand.nextInt(executorIds.size())),
                    files.subList(i - filesPerCompaction, i));
              }
            });

        return planBuilder.build();
      } else {
        return params.createPlanBuilder().build();
      }
    }
  }

  @BeforeClass
  public static void setup() throws Exception {
    SharedMiniClusterBase.startMiniClusterWithConfig((miniCfg, coreSite) -> {
      Map<String,String> siteCfg = new HashMap<>();

      var csp = Property.TSERV_COMPACTION_SERVICE_PREFIX.getKey();
      siteCfg.put(csp + "cs1.planner", TestPlanner.class.getName());
      siteCfg.put(csp + "cs1.planner.opts.executors", "3");
      siteCfg.put(csp + "cs1.planner.opts.filesPerCompaction", "5");
      siteCfg.put(csp + "cs1.planner.opts.process", "SYSTEM");

      siteCfg.put(csp + "cs2.planner", TestPlanner.class.getName());
      siteCfg.put(csp + "cs2.planner.opts.executors", "2");
      siteCfg.put(csp + "cs2.planner.opts.filesPerCompaction", "7");
      siteCfg.put(csp + "cs2.planner.opts.process", "SYSTEM");

      siteCfg.put(csp + "cs3.planner", TestPlanner.class.getName());
      siteCfg.put(csp + "cs3.planner.opts.executors", "1");
      siteCfg.put(csp + "cs3.planner.opts.filesPerCompaction", "3");
      siteCfg.put(csp + "cs3.planner.opts.process", "USER");

      siteCfg.put(csp + "cs4.planner", TestPlanner.class.getName());
      siteCfg.put(csp + "cs4.planner.opts.executors", "2");
      siteCfg.put(csp + "cs4.planner.opts.filesPerCompaction", "11");
      siteCfg.put(csp + "cs4.planner.opts.process", "USER");

      miniCfg.setSiteConfig(siteCfg);
    });
  }

  @AfterClass
  public static void teardown() {
    SharedMiniClusterBase.stopMiniCluster();
  }

  @After
  public void cleanup() {
    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
      client.tableOperations().list().stream()
          .filter(tableName -> !tableName.startsWith("accumulo.")).forEach(tableName -> {
            try {
              client.tableOperations().delete(tableName);
            } catch (AccumuloException | AccumuloSecurityException | TableNotFoundException e) {
              throw new RuntimeException(e);
            }
          });
    }
  }

  // TODO test compacting half of tablets with filtering iter... then other half with different
  // filtering iter... then scan
  // TODO test setting incorrect plugin types and ensure there is an error

  /**
   * Test ensures that system compactions are dispatched to a configured compaction service. The
   * compaction services produce a very specific number of files, so the test indirectly checks
   * dispatching worked by observing how many files a tablet ends up with.
   */
  @Test
  public void testDispatchSystem() throws Exception {
    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
      createTable(client, "dst1", "cs1");
      createTable(client, "dst2", "cs2");

      addFiles(client, "dst1", 14);
      addFiles(client, "dst2", 13);

      assertTrue(getFiles(client, "dst1").size() >= 6);
      assertTrue(getFiles(client, "dst2").size() >= 7);

      addFiles(client, "dst1", 1);
      addFiles(client, "dst2", 1);

      while (getFiles(client, "dst1").size() > 3 || getFiles(client, "dst2").size() > 2) {
        Thread.sleep(100);
      }

      assertEquals(3, getFiles(client, "dst1").size());
      assertEquals(2, getFiles(client, "dst2").size());
    }
  }

  @Test
  public void testDispatchUser() throws Exception {
    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
      createTable(client, "dut1", "cs3");
      createTable(client, "dut2", "cs3", "special", "cs4");

      addFiles(client, "dut1", 6);
      addFiles(client, "dut2", 33);

      assertEquals(6, getFiles(client, "dut1").size());
      assertEquals(33, getFiles(client, "dut2").size());

      client.tableOperations().compact("dut1", new CompactionConfig().setWait(false));

      // The hint should cause the compaction to dispatch to service cs4 which will produce a
      // different number of files.
      client.tableOperations().compact("dut2", new CompactionConfig().setWait(false)
          .setExecutionHints(Map.of("compaction_type", "special")));

      while (getFiles(client, "dut1").size() > 2 || getFiles(client, "dut2").size() > 3) {
        Thread.sleep(100);
      }

      assertEquals(2, getFiles(client, "dut1").size());
      assertEquals(3, getFiles(client, "dut2").size());

      // The way the compaction services were configured, they would never converge to one file for
      // the user compactions. However Accumulo will keep asking the planner for a plan until a user
      // compaction converges to one file. So cancel the compactions.
      client.tableOperations().cancelCompaction("dut1");
      client.tableOperations().cancelCompaction("dut2");

      assertEquals(2, getFiles(client, "dut1").size());
      assertEquals(3, getFiles(client, "dut2").size());

      client.tableOperations().compact("dut1",
          new CompactionConfig().setWait(true).setExecutionHints(Map.of("compact_all", "true")));
      client.tableOperations().compact("dut2",
          new CompactionConfig().setWait(true).setExecutionHints(Map.of("compact_all", "true")));

      assertEquals(1, getFiles(client, "dut1").size());
      assertEquals(1, getFiles(client, "dut2").size());
    }

  }

  @Test
  public void testTooManyDeletes() throws Exception {
    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
      Map<String,
          String> props = Map.of(Property.TABLE_COMPACTION_SELECTOR.getKey(),
              TooManyDeletesSelector.class.getName(),
              Property.TABLE_COMPACTION_SELECTOR_OPTS.getKey() + "threshold", ".4");
      var deleteSummarizerCfg =
          SummarizerConfiguration.builder(DeletesSummarizer.class.getName()).build();
      client.tableOperations().create("tmd_selector", new NewTableConfiguration()
          .setProperties(props).enableSummarization(deleteSummarizerCfg));
      client.tableOperations().create("tmd_control1",
          new NewTableConfiguration().enableSummarization(deleteSummarizerCfg));
      client.tableOperations().create("tmd_control2",
          new NewTableConfiguration().enableSummarization(deleteSummarizerCfg));
      client.tableOperations().create("tmd_control3",
          new NewTableConfiguration().enableSummarization(deleteSummarizerCfg));

      addFile(client, "tmd_selector", 1, 1000, false);
      addFile(client, "tmd_selector", 1, 1000, true);

      addFile(client, "tmd_control1", 1, 1000, false);
      addFile(client, "tmd_control1", 1, 1000, true);

      addFile(client, "tmd_control2", 1, 1000, false);
      addFile(client, "tmd_control2", 1000, 2000, false);

      addFile(client, "tmd_control3", 1, 2000, false);
      addFile(client, "tmd_control3", 1, 1000, true);

      assertEquals(2, getFiles(client, "tmd_control1").size());
      assertEquals(2, getFiles(client, "tmd_control2").size());
      assertEquals(2, getFiles(client, "tmd_control3").size());

      while (getFiles(client, "tmd_selector").size() != 0) {
        Thread.sleep(100);
      }

      assertEquals(2, getFiles(client, "tmd_control1").size());
      assertEquals(2, getFiles(client, "tmd_control2").size());
      assertEquals(2, getFiles(client, "tmd_control3").size());

      var cc1 = new CompactionConfig()
          .setSelector(new PluginConfig(TooManyDeletesSelector.class.getName())
              .setOptions(Map.of("threshold", ".99")))
          .setWait(true);

      client.tableOperations().compact("tmd_control1", cc1);
      client.tableOperations().compact("tmd_control2", cc1);
      client.tableOperations().compact("tmd_control3", cc1);

      assertEquals(0, getFiles(client, "tmd_control1").size());
      assertEquals(2, getFiles(client, "tmd_control2").size());
      assertEquals(2, getFiles(client, "tmd_control3").size());

      var cc2 = new CompactionConfig()
          .setSelector(new PluginConfig(TooManyDeletesSelector.class.getName())
              .setOptions(Map.of("threshold", ".40")))
          .setWait(true);

      client.tableOperations().compact("tmd_control1", cc2);
      client.tableOperations().compact("tmd_control2", cc2);
      client.tableOperations().compact("tmd_control3", cc2);

      assertEquals(0, getFiles(client, "tmd_control1").size());
      assertEquals(2, getFiles(client, "tmd_control2").size());
      assertEquals(1, getFiles(client, "tmd_control3").size());

      client.tableOperations().compact("tmd_control2", new CompactionConfig().setWait(true));

      assertEquals(1, getFiles(client, "tmd_control2").size());

    }
  }

  private Set<String> getFiles(AccumuloClient client, String name) {
    var tableId = TableId.of(client.tableOperations().tableIdMap().get(name));

    try (var tabletsMeta =
        TabletsMetadata.builder().forTable(tableId).fetch(ColumnType.FILES).build(client)) {
      return tabletsMeta.stream().flatMap(tm -> tm.getFiles().stream())
          .map(StoredTabletFile::getFileName).collect(Collectors.toSet());
    }
  }

  private void addFile(AccumuloClient client, String table, int startRow, int endRow,
      boolean delete) throws Exception {
    try (var writer = client.createBatchWriter(table)) {
      for (int i = startRow; i < endRow; i++) {
        Mutation mut = new Mutation(String.format("%09d", i));
        if (delete)
          mut.putDelete("f1", "q1");
        else
          mut.put("f1", "q1", "v" + i);
        writer.addMutation(mut);

      }
    }
    client.tableOperations().flush(table, null, null, true);
  }

  private void addFiles(AccumuloClient client, String table, int num) throws Exception {
    try (var writer = client.createBatchWriter(table)) {
      for (int i = 0; i < num; i++) {
        Mutation mut = new Mutation("r" + i);
        mut.put("f1", "q1", "v" + i);
        writer.addMutation(mut);
        writer.flush();
        client.tableOperations().flush(table, null, null, true);
      }
    }
  }

  private void createTable(AccumuloClient client, String name, String compactionService)
      throws Exception {
    NewTableConfiguration ntc = new NewTableConfiguration().setProperties(
        Map.of(Property.TABLE_COMPACTION_DISPATCHER_OPTS.getKey() + "service", compactionService));
    client.tableOperations().create(name, ntc);
  }

  private void createTable(AccumuloClient client, String name, String compactionService,
      String userType, String userService) throws Exception {
    var tcdo = Property.TABLE_COMPACTION_DISPATCHER_OPTS.getKey();

    NewTableConfiguration ntc = new NewTableConfiguration().setProperties(Map.of(tcdo + "service",
        compactionService, tcdo + "service.user." + userType, userService));

    client.tableOperations().create(name, ntc);
  }

}

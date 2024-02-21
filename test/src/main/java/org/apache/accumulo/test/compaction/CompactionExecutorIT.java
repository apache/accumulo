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
package org.apache.accumulo.test.compaction;

import static org.apache.accumulo.core.util.LazySingletons.GSON;
import static org.apache.accumulo.core.util.LazySingletons.RANDOM;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.CompactionConfig;
import org.apache.accumulo.core.client.admin.NewTableConfiguration;
import org.apache.accumulo.core.client.admin.PluginConfig;
import org.apache.accumulo.core.client.admin.compaction.CompactableFile;
import org.apache.accumulo.core.client.admin.compaction.CompressionConfigurer;
import org.apache.accumulo.core.client.admin.compaction.TooManyDeletesSelector;
import org.apache.accumulo.core.client.summary.SummarizerConfiguration;
import org.apache.accumulo.core.client.summary.summarizers.DeletesSummarizer;
import org.apache.accumulo.core.clientImpl.Namespace;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.user.RegExFilter;
import org.apache.accumulo.core.metadata.StoredTabletFile;
import org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType;
import org.apache.accumulo.core.metadata.schema.TabletsMetadata;
import org.apache.accumulo.core.spi.compaction.CompactionKind;
import org.apache.accumulo.core.spi.compaction.CompactionPlan;
import org.apache.accumulo.core.spi.compaction.CompactionPlanner;
import org.apache.accumulo.core.spi.compaction.CompactorGroupId;
import org.apache.accumulo.harness.MiniClusterConfigurationCallback;
import org.apache.accumulo.harness.SharedMiniClusterBase;
import org.apache.accumulo.minicluster.ServerType;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloConfigImpl;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;

public class CompactionExecutorIT extends SharedMiniClusterBase {
  public static final List<String> compactionGroups = new LinkedList<>();
  public static final Logger log = LoggerFactory.getLogger(CompactionExecutorIT.class);

  public static class TestPlanner implements CompactionPlanner {

    private static class GroupConfig {
      String name;
    }

    private int filesPerCompaction;
    private List<CompactorGroupId> groupIds;
    private EnumSet<CompactionKind> kindsToProcess = EnumSet.noneOf(CompactionKind.class);

    @Override
    public void init(InitParameters params) {
      var groups = params.getOptions().get("groups");
      this.filesPerCompaction = Integer.parseInt(params.getOptions().get("filesPerCompaction"));
      this.groupIds = new ArrayList<>();
      for (String kind : params.getOptions().get("process").split(",")) {
        kindsToProcess.add(CompactionKind.valueOf(kind.toUpperCase()));
      }

      for (JsonElement element : GSON.get().fromJson(groups, JsonArray.class)) {
        GroupConfig groupConfig = GSON.get().fromJson(element, GroupConfig.class);
        var cgid = params.getGroupManager().getGroup(groupConfig.name);
        groupIds.add(cgid);
      }

    }

    static String getFirstChar(CompactableFile cf) {
      return cf.getFileName().substring(0, 1);
    }

    @Override
    public CompactionPlan makePlan(PlanningParameters params) {
      if (Boolean.parseBoolean(params.getExecutionHints().getOrDefault("compact_all", "false"))) {
        return params
            .createPlanBuilder().addJob((short) 1,
                groupIds.get(RANDOM.get().nextInt(groupIds.size())), params.getCandidates())
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
                planBuilder.addJob((short) 1, groupIds.get(RANDOM.get().nextInt(groupIds.size())),
                    files.subList(i - filesPerCompaction, i));
              }
            });

        return planBuilder.build();
      } else {
        return params.createPlanBuilder().build();
      }
    }
  }

  public static class CompactionExecutorITConfig implements MiniClusterConfigurationCallback {
    @Override
    public void configureMiniCluster(MiniAccumuloConfigImpl cfg, Configuration conf) {
      var csp = Property.COMPACTION_SERVICE_PREFIX.getKey();
      cfg.setProperty(csp + "cs1.planner", TestPlanner.class.getName());
      cfg.setProperty(csp + "cs1.planner.opts.groups",
          "[{'name':'e1'},{'name':'e2'},{'name':'e3'}]");
      cfg.setProperty(csp + "cs1.planner.opts.filesPerCompaction", "5");
      cfg.setProperty(csp + "cs1.planner.opts.process", "SYSTEM");

      cfg.setProperty(csp + "cs2.planner", TestPlanner.class.getName());
      cfg.setProperty(csp + "cs2.planner.opts.groups", "[{'name':'f1'},{'name':'f2'}]");
      cfg.setProperty(csp + "cs2.planner.opts.filesPerCompaction", "7");
      cfg.setProperty(csp + "cs2.planner.opts.process", "SYSTEM");

      cfg.setProperty(csp + "cs3.planner", TestPlanner.class.getName());
      cfg.setProperty(csp + "cs3.planner.opts.groups", "[{'name':'g1'}]");
      cfg.setProperty(csp + "cs3.planner.opts.filesPerCompaction", "3");
      cfg.setProperty(csp + "cs3.planner.opts.process", "USER");

      cfg.setProperty(csp + "cs4.planner", TestPlanner.class.getName());
      cfg.setProperty(csp + "cs4.planner.opts.groups", "[{'name':'h1'},{'name':'h2'}]");
      cfg.setProperty(csp + "cs4.planner.opts.filesPerCompaction", "11");
      cfg.setProperty(csp + "cs4.planner.opts.process", "USER");

      // this is meant to be dynamically reconfigured
      cfg.setProperty(csp + "recfg.planner", TestPlanner.class.getName());
      cfg.setProperty(csp + "recfg.planner.opts.groups", "[{'name':'i1'},{'name':'i2'}]");
      cfg.setProperty(csp + "recfg.planner.opts.filesPerCompaction", "11");
      cfg.setProperty(csp + "recfg.planner.opts.process", "SYSTEM");

      Stream.of("e1", "e2", "e3", "f1", "f2", "g1", "h1", "h2", "i1", "i2")
          .forEach(s -> cfg.getClusterServerConfiguration().addCompactorResourceGroup(s, 0));

      // use raw local file system
      conf.set("fs.file.impl", RawLocalFileSystem.class.getName());
    }
  }

  @BeforeAll
  public static void setup() throws Exception {
    SharedMiniClusterBase
        .startMiniClusterWithConfig(new CompactionExecutorIT.CompactionExecutorITConfig());
  }

  @AfterAll
  public static void teardown() {
    SharedMiniClusterBase.stopMiniCluster();
  }

  @AfterEach
  public void cleanup() {
    var iter = compactionGroups.iterator();
    while (iter.hasNext()) {
      var group = iter.next();
      getCluster().getConfig().getClusterServerConfiguration().addCompactorResourceGroup(group, 0);
      try {
        getCluster().getClusterControl().stopCompactorGroup(group);
      } catch (Exception e) {
        log.warn("Compaction group: {} failed to fully stop", group, e);
      } finally {
        iter.remove();
      }
    }
    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
      client.tableOperations().list().stream()
          .filter(
              tableName -> !tableName.startsWith(Namespace.ACCUMULO.name() + Namespace.SEPARATOR))
          .forEach(tableName -> {
            try {
              client.tableOperations().delete(tableName);
            } catch (AccumuloException | AccumuloSecurityException | TableNotFoundException e) {
              throw new RuntimeException(e);
            }
          });
    }
  }

  @Test
  public void testReconfigureCompactionService() throws Exception {
    Stream.of("i1", "i2").forEach(g -> {
      getCluster().getConfig().getClusterServerConfiguration().addCompactorResourceGroup(g, 1);
      compactionGroups.add(g);
    });
    getCluster().getClusterControl().start(ServerType.COMPACTOR);

    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
      createTable(client, "rctt", "recfg");

      addFiles(client, "rctt", 22);

      while (getFiles(client, "rctt").size() > 2) {
        Thread.sleep(100);
      }

      assertEquals(2, getFiles(client, "rctt").size());

      client.instanceOperations().setProperty(
          Property.COMPACTION_SERVICE_PREFIX.getKey() + "recfg.planner.opts.filesPerCompaction",
          "5");
      client.instanceOperations().setProperty(
          Property.COMPACTION_SERVICE_PREFIX.getKey() + "recfg.planner.opts.groups",
          "[{'name':'group1'}]");
      getCluster().getConfig().getClusterServerConfiguration().addCompactorResourceGroup("group1",
          1);
      compactionGroups.add("group1");
      getCluster().getClusterControl().start(ServerType.COMPACTOR);

      addFiles(client, "rctt", 10);

      while (getFiles(client, "rctt").size() > 4) {
        Thread.sleep(100);
      }

      assertEquals(4, getFiles(client, "rctt").size());
    }
  }

  @Test
  public void testAddCompactionService() throws Exception {
    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
      client.instanceOperations().setProperty(
          Property.COMPACTION_SERVICE_PREFIX.getKey() + "newcs.planner.opts.filesPerCompaction",
          "7");
      client.instanceOperations().setProperty(
          Property.COMPACTION_SERVICE_PREFIX.getKey() + "newcs.planner.opts.process", "SYSTEM");
      client.instanceOperations().setProperty(
          Property.COMPACTION_SERVICE_PREFIX.getKey() + "newcs.planner.opts.groups",
          "[{'name':'add1'},{'name':'add2'}, {'name':'add3'}]");
      client.instanceOperations().setProperty(
          Property.COMPACTION_SERVICE_PREFIX.getKey() + "newcs.planner",
          TestPlanner.class.getName());

      Stream.of("add1", "add2", "add3").forEach(s -> {
        getCluster().getConfig().getClusterServerConfiguration().addCompactorResourceGroup(s, 1);
        compactionGroups.add(s);
      });
      getCluster().getClusterControl().start(ServerType.COMPACTOR);

      createTable(client, "acst", "newcs");

      addFiles(client, "acst", 42);

      while (getFiles(client, "acst").size() > 6) {
        Thread.sleep(100);
      }

      assertEquals(6, getFiles(client, "acst").size());
    }
  }

  /**
   * Test ensures that system compactions are dispatched to a configured compaction service. The
   * compaction services produce a very specific number of files, so the test indirectly checks
   * dispatching worked by observing how many files a tablet ends up with.
   */
  @Test
  public void testDispatchSystem() throws Exception {
    Stream.of("e1", "e2", "e3", "f1", "f2").forEach(s -> {
      getCluster().getConfig().getClusterServerConfiguration().addCompactorResourceGroup(s, 1);
      compactionGroups.add(s);
    });
    getCluster().getClusterControl().start(ServerType.COMPACTOR);

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
    Stream.of("h1", "h2", "g1").forEach(s -> {
      getCluster().getConfig().getClusterServerConfiguration().addCompactorResourceGroup(s, 1);
      compactionGroups.add(s);
    });
    getCluster().getClusterControl().start(ServerType.COMPACTOR);

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
      var deleteSummarizerCfg =
          SummarizerConfiguration.builder(DeletesSummarizer.class.getName()).build();
      client.tableOperations().create("tmd_control1",
          new NewTableConfiguration().enableSummarization(deleteSummarizerCfg));
      client.tableOperations().create("tmd_control2",
          new NewTableConfiguration().enableSummarization(deleteSummarizerCfg));
      client.tableOperations().create("tmd_control3",
          new NewTableConfiguration().enableSummarization(deleteSummarizerCfg));

      addFile(client, "tmd_control1", 1, 1000, false);
      addFile(client, "tmd_control1", 1, 1000, true);

      addFile(client, "tmd_control2", 1, 1000, false);
      addFile(client, "tmd_control2", 1000, 2000, false);

      addFile(client, "tmd_control3", 1, 2000, false);
      addFile(client, "tmd_control3", 1, 1000, true);

      assertEquals(2, getFiles(client, "tmd_control1").size());
      assertEquals(2, getFiles(client, "tmd_control2").size());
      assertEquals(2, getFiles(client, "tmd_control3").size());

      assertEquals(2, getFiles(client, "tmd_control1").size());
      assertEquals(2, getFiles(client, "tmd_control2").size());
      assertEquals(2, getFiles(client, "tmd_control3").size());

      var cc1 = new CompactionConfig()
          .setSelector(
              new PluginConfig(TooManyDeletesSelector.class.getName(), Map.of("threshold", ".99")))
          .setWait(true);

      client.tableOperations().compact("tmd_control1", cc1);
      client.tableOperations().compact("tmd_control2", cc1);
      client.tableOperations().compact("tmd_control3", cc1);

      assertEquals(0, getFiles(client, "tmd_control1").size());
      assertEquals(2, getFiles(client, "tmd_control2").size());
      assertEquals(2, getFiles(client, "tmd_control3").size());

      var cc2 = new CompactionConfig()
          .setSelector(
              new PluginConfig(TooManyDeletesSelector.class.getName(), Map.of("threshold", ".40")))
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

  @Test
  public void testIteratorsWithRange() throws Exception {

    String tableName = "tiwr";

    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {

      SortedSet<Text> splits = new TreeSet<>();
      for (String s : List.of("f", "m", "r", "t")) {
        splits.add(new Text(s));
      }

      NewTableConfiguration ntc = new NewTableConfiguration().withSplits(splits);
      client.tableOperations().create(tableName, ntc);

      Map<String,String> expected = new TreeMap<>();

      try (var writer = client.createBatchWriter(tableName)) {
        int v = 0;
        for (String row : List.of("a", "h", "o", "s", "x")) {
          Mutation m = new Mutation(row);
          for (int q = 0; q < 10; q++) {
            String qual = String.format("%03d", q);
            String val = "v" + v++;
            m.at().family("f").qualifier(qual).put(val);
            expected.put(row + ":f:" + qual, val);
          }
          writer.addMutation(m);
        }
      }

      IteratorSetting iterSetting = new IteratorSetting(20, "rf", RegExFilter.class.getName());
      RegExFilter.setRegexs(iterSetting, null, null, "004|007", null, false);
      RegExFilter.setNegate(iterSetting, true);
      client.tableOperations().compact(tableName,
          new CompactionConfig().setStartRow(new Text("b")).setEndRow(new Text("m"))
              .setIterators(List.of(iterSetting)).setWait(true).setFlush(true));

      for (String row : List.of("a", "h")) {
        assertNotNull(expected.remove(row + ":f:004"));
        assertNotNull(expected.remove(row + ":f:007"));
      }

      Map<String,String> actual = scanTable(client, tableName);
      assertEquals(expected, actual);

      iterSetting = new IteratorSetting(20, "rf", RegExFilter.class.getName());
      RegExFilter.setRegexs(iterSetting, null, null, "002|005|009", null, false);
      RegExFilter.setNegate(iterSetting, true);
      client.tableOperations().compact(tableName, new CompactionConfig().setStartRow(new Text("m"))
          .setEndRow(new Text("u")).setIterators(List.of(iterSetting)).setWait(true));

      for (String row : List.of("o", "s", "x")) {
        assertNotNull(expected.remove(row + ":f:002"));
        assertNotNull(expected.remove(row + ":f:005"));
        assertNotNull(expected.remove(row + ":f:009"));
      }

      actual = scanTable(client, tableName);
      assertEquals(expected, actual);

      iterSetting = new IteratorSetting(20, "rf", RegExFilter.class.getName());
      RegExFilter.setRegexs(iterSetting, null, null, "00[18]", null, false);
      RegExFilter.setNegate(iterSetting, true);
      client.tableOperations().compact(tableName,
          new CompactionConfig().setIterators(List.of(iterSetting)).setWait(true));

      for (String row : List.of("a", "h", "o", "s", "x")) {
        assertNotNull(expected.remove(row + ":f:001"));
        assertNotNull(expected.remove(row + ":f:008"));
      }

      actual = scanTable(client, tableName);
      assertEquals(expected, actual);

      // add all data back and force a compaction to ensure iters do not run again
      try (var writer = client.createBatchWriter(tableName)) {
        int v = 1000;
        for (String row : List.of("a", "h", "o", "s", "x")) {
          Mutation m = new Mutation(row);
          for (int q = 0; q < 10; q++) {
            String qual = String.format("%03d", q);
            String val = "v" + v++;
            m.at().family("f").qualifier(qual).put(val);
            expected.put(row + ":f:" + qual, val);
          }
          writer.addMutation(m);
        }
      }

      client.tableOperations().compact(tableName,
          new CompactionConfig().setWait(true).setFlush(true));

      actual = scanTable(client, tableName);
      assertEquals(expected, actual);

    }
  }

  @Test
  public void testConfigurer() throws Exception {
    String tableName = "tcc";

    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
      var ntc = new NewTableConfiguration()
          .setProperties(Map.of(Property.TABLE_FILE_COMPRESSION_TYPE.getKey(), "none"));
      client.tableOperations().create(tableName, ntc);

      byte[] data = new byte[100000];
      Arrays.fill(data, (byte) 65);

      try (var writer = client.createBatchWriter(tableName)) {
        for (int row = 0; row < 10; row++) {
          Mutation m = new Mutation(row + "");
          m.at().family("big").qualifier("stuff").put(data);
          writer.addMutation(m);
        }
      }

      client.tableOperations().flush(tableName, null, null, true);

      // without compression, expect file to be large
      long sizes = getFileSizes(client, tableName);
      assertTrue(sizes > data.length * 10 && sizes < data.length * 11,
          "Unexpected files sizes : " + sizes);

      client.tableOperations().compact(tableName,
          new CompactionConfig().setWait(true)
              .setConfigurer(new PluginConfig(CompressionConfigurer.class.getName(),
                  Map.of(CompressionConfigurer.LARGE_FILE_COMPRESSION_TYPE, "gz",
                      CompressionConfigurer.LARGE_FILE_COMPRESSION_THRESHOLD, data.length + ""))));

      // after compacting with compression, expect small file
      sizes = getFileSizes(client, tableName);
      assertTrue(sizes < data.length, "Unexpected files sizes : " + sizes);

      client.tableOperations().compact(tableName, new CompactionConfig().setWait(true));

      // after compacting without compression, expect big files again
      sizes = getFileSizes(client, tableName);
      assertTrue(sizes > data.length * 10 && sizes < data.length * 11,
          "Unexpected files sizes : " + sizes);

    }
  }

  public static long getFileSizes(AccumuloClient client, String tableName) {
    var tableId = TableId.of(client.tableOperations().tableIdMap().get(tableName));

    try (var tabletsMeta =
        TabletsMetadata.builder(client).forTable(tableId).fetch(ColumnType.FILES).build()) {
      return tabletsMeta.stream().flatMap(tm -> tm.getFiles().stream()).mapToLong(stf -> {
        try {
          return FileSystem.getLocal(new Configuration()).getFileStatus(stf.getPath()).getLen();
        } catch (IOException e) {
          throw new UncheckedIOException(e);
        }
      }

      ).sum();
    }
  }

  @Test
  public void testIncorrectSelectorType() throws Exception {
    String tableName = "tist";

    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
      client.tableOperations().create(tableName);
      addFiles(client, tableName, 5);

      var e = assertThrows(AccumuloException.class,
          () -> client.tableOperations().compact(tableName, new CompactionConfig()
              .setSelector(new PluginConfig(CompressionConfigurer.class.getName())).setWait(true)));
      final String msg = e.getMessage();
      assertTrue(msg.contains("TabletServer could not load CompactionSelector"),
          "Unexpected message : " + msg);

    }
  }

  @Test
  public void testIncorrectConfigurerType() throws Exception {
    String tableName = "tict";

    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
      client.tableOperations().create(tableName);
      addFiles(client, tableName, 5);

      var e = assertThrows(AccumuloException.class,
          () -> client.tableOperations().compact(tableName,
              new CompactionConfig()
                  .setConfigurer(new PluginConfig(TooManyDeletesSelector.class.getName()))
                  .setWait(true)));
      final String msg = e.getMessage();
      assertTrue(msg.contains("TabletServer could not load CompactionConfigurer"),
          "Unexpected message : " + msg);
    }
  }

  private Map<String,String> scanTable(AccumuloClient client, String tableName)
      throws TableNotFoundException, AccumuloSecurityException, AccumuloException {
    Map<String,String> actual = new TreeMap<>();
    try (var scanner = client.createScanner(tableName)) {
      for (Entry<Key,Value> e : scanner) {
        var k = e.getKey();
        actual.put(
            k.getRowData() + ":" + k.getColumnFamilyData() + ":" + k.getColumnQualifierData(),
            e.getValue().toString());
      }
    }
    return actual;
  }

  private Set<String> getFiles(AccumuloClient client, String name) {
    var tableId = TableId.of(client.tableOperations().tableIdMap().get(name));

    try (var tabletsMeta =
        TabletsMetadata.builder(client).forTable(tableId).fetch(ColumnType.FILES).build()) {
      return tabletsMeta.stream().flatMap(tm -> tm.getFiles().stream())
          .map(StoredTabletFile::getFileName).collect(Collectors.toSet());
    }
  }

  private void addFile(AccumuloClient client, String table, int startRow, int endRow,
      boolean delete) throws Exception {
    try (var writer = client.createBatchWriter(table)) {
      for (int i = startRow; i < endRow; i++) {
        Mutation mut = new Mutation(String.format("%09d", i));
        if (delete) {
          mut.putDelete("f1", "q1");
        } else {
          mut.put("f1", "q1", "v" + i);
        }
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

    NewTableConfiguration ntc = new NewTableConfiguration().setProperties(
        Map.of(tcdo + "service", compactionService, tcdo + "service.user." + userType, userService,
            Property.TABLE_MAJC_RATIO.getKey(), "100"));

    client.tableOperations().create(name, ntc);
  }
}

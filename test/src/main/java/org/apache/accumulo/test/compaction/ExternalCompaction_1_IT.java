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

import static org.apache.accumulo.test.compaction.ExternalCompactionTestUtils.GROUP1;
import static org.apache.accumulo.test.compaction.ExternalCompactionTestUtils.GROUP2;
import static org.apache.accumulo.test.compaction.ExternalCompactionTestUtils.GROUP3;
import static org.apache.accumulo.test.compaction.ExternalCompactionTestUtils.GROUP4;
import static org.apache.accumulo.test.compaction.ExternalCompactionTestUtils.GROUP6;
import static org.apache.accumulo.test.compaction.ExternalCompactionTestUtils.GROUP8;
import static org.apache.accumulo.test.compaction.ExternalCompactionTestUtils.MAX_DATA;
import static org.apache.accumulo.test.compaction.ExternalCompactionTestUtils.assertNoCompactionMetadata;
import static org.apache.accumulo.test.compaction.ExternalCompactionTestUtils.compact;
import static org.apache.accumulo.test.compaction.ExternalCompactionTestUtils.createTable;
import static org.apache.accumulo.test.compaction.ExternalCompactionTestUtils.row;
import static org.apache.accumulo.test.compaction.ExternalCompactionTestUtils.verify;
import static org.apache.accumulo.test.compaction.ExternalCompactionTestUtils.writeData;
import static org.apache.accumulo.test.util.FileMetadataUtil.countFencedFiles;
import static org.apache.accumulo.test.util.FileMetadataUtil.splitFilesIntoRanges;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.stream.Collectors;

import org.apache.accumulo.compactor.ExtCEnv.CompactorIterEnv;
import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.admin.CompactionConfig;
import org.apache.accumulo.core.client.admin.NewTableConfiguration;
import org.apache.accumulo.core.client.admin.PluginConfig;
import org.apache.accumulo.core.client.admin.compaction.CompactableFile;
import org.apache.accumulo.core.client.admin.compaction.CompactionSelector;
import org.apache.accumulo.core.client.admin.compaction.CompressionConfigurer;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.DevNull;
import org.apache.accumulo.core.iterators.Filter;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.IteratorUtil.IteratorScope;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.spi.compaction.SimpleCompactionDispatcher;
import org.apache.accumulo.harness.MiniClusterConfigurationCallback;
import org.apache.accumulo.harness.SharedMiniClusterBase;
import org.apache.accumulo.minicluster.ServerType;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloConfigImpl;
import org.apache.accumulo.server.util.FindCompactionTmpFiles;
import org.apache.accumulo.test.functional.CompactionIT.ErrorThrowingSelector;
import org.apache.accumulo.test.util.Wait;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.google.common.base.Preconditions;

public class ExternalCompaction_1_IT extends SharedMiniClusterBase {

  public static class ExternalCompaction1Config implements MiniClusterConfigurationCallback {
    @Override
    public void configureMiniCluster(MiniAccumuloConfigImpl cfg, Configuration coreSite) {
      ExternalCompactionTestUtils.configureMiniCluster(cfg, coreSite);
    }
  }

  @BeforeAll
  public static void beforeTests() throws Exception {
    startMiniClusterWithConfig(new ExternalCompaction1Config());
  }

  public static class TestFilter extends Filter {

    int modulus = 1;

    @Override
    public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options,
        IteratorEnvironment env) throws IOException {
      super.init(source, options, env);

      // this cast should fail if the compaction is running in the tserver
      CompactorIterEnv cienv = (CompactorIterEnv) env;

      Preconditions.checkArgument(!cienv.getQueueName().isEmpty());
      Preconditions
          .checkArgument(options.getOrDefault("expectedQ", "").equals(cienv.getQueueName()));
      Preconditions.checkArgument(cienv.isUserCompaction());
      Preconditions.checkArgument(cienv.getIteratorScope() == IteratorScope.majc);
      Preconditions.checkArgument(!cienv.isSamplingEnabled());

      // if the init function is never called at all, then not setting the modulus option should
      // cause the test to fail
      if (options.containsKey("modulus")) {
        Preconditions.checkArgument(!options.containsKey("pmodulus"));
        Preconditions.checkArgument(cienv.isFullMajorCompaction());
        modulus = Integer.parseInt(options.get("modulus"));
      }

      // use when partial compaction is expected
      if (options.containsKey("pmodulus")) {
        Preconditions.checkArgument(!options.containsKey("modulus"));
        Preconditions.checkArgument(!cienv.isFullMajorCompaction());
        modulus = Integer.parseInt(options.get("pmodulus"));
      }
    }

    @Override
    public boolean accept(Key k, Value v) {
      return Integer.parseInt(v.toString()) % modulus == 0;
    }

  }

  @Test
  public void testBadSelector() throws Exception {
    try (AccumuloClient c = Accumulo.newClient().from(getClientProps()).build()) {
      final String tableName = getUniqueNames(1)[0];
      NewTableConfiguration tc = new NewTableConfiguration();
      // Ensure compactions don't kick off
      tc.setProperties(Map.of(Property.TABLE_MAJC_RATIO.getKey(), "10.0"));
      c.tableOperations().create(tableName, tc);
      // Create multiple RFiles
      try (BatchWriter bw = c.createBatchWriter(tableName)) {
        for (int i = 1; i <= 4; i++) {
          Mutation m = new Mutation(Integer.toString(i));
          m.put("cf", "cq", new Value());
          bw.addMutation(m);
          bw.flush();
          // flush often to create multiple files to compact
          c.tableOperations().flush(tableName, null, null, true);
        }
      }

      CompactionConfig config = new CompactionConfig()
          .setSelector(new PluginConfig(ErrorThrowingSelector.class.getName(), Map.of()))
          .setWait(true);
      assertThrows(AccumuloException.class, () -> c.tableOperations().compact(tableName, config));

      List<String> rows = new ArrayList<>();
      c.createScanner(tableName).forEach((k, v) -> rows.add(k.getRow().toString()));
      assertEquals(List.of("1", "2", "3", "4"), rows);

      assertNoCompactionMetadata(getCluster().getServerContext(), tableName);
    }
  }

  @Test
  public void testExternalCompaction() throws Exception {
    String[] names = this.getUniqueNames(2);
    try (AccumuloClient client =
        Accumulo.newClient().from(getCluster().getClientProperties()).build()) {

      String table1 = names[0];
      createTable(client, table1, "cs1");

      String table2 = names[1];
      createTable(client, table2, "cs2");

      writeData(client, table1);
      writeData(client, table2);

      compact(client, table1, 2, GROUP1, true);
      verify(client, table1, 2);

      SortedSet<Text> splits = new TreeSet<>();
      splits.add(new Text(row(MAX_DATA / 2)));
      client.tableOperations().addSplits(table2, splits);

      compact(client, table2, 3, GROUP2, true);
      verify(client, table2, 3);

    }
  }

  @Test
  public void testCompactionAndCompactorDies() throws Exception {
    String table1 = this.getUniqueNames(1)[0];
    try (AccumuloClient client =
        Accumulo.newClient().from(getCluster().getClientProperties()).build()) {

      createTable(client, table1, "cs3", 2);
      writeData(client, table1);
      verify(client, table1, 1);

      getCluster().getClusterControl().stopAllServers(ServerType.COMPACTOR);
      getCluster().getClusterControl().start(ServerType.COMPACTOR, null, 1,
          ExternalDoNothingCompactor.class);

      compact(client, table1, 2, GROUP3, false);
      TableId tid = getCluster().getServerContext().getTableId(table1);

      // Wait for the compaction to start by waiting for 1 external compaction column
      var ecids = ExternalCompactionTestUtils
          .waitForCompactionStartAndReturnEcids(getCluster().getServerContext(), tid);

      assertFalse(ecids.isEmpty());

      // Verify that a tmp file is created
      Wait.waitFor(() -> FindCompactionTmpFiles
          .findTempFiles(getCluster().getServerContext(), tid.canonical()).size() == 1);

      // Kill the compactor
      getCluster().getClusterControl().stop(ServerType.COMPACTOR);

      // DeadCompactionDetector in the CompactionCoordinator should fail the compaction and delete
      // it from the tablet.
      ExternalCompactionTestUtils.waitForRunningCompactions(getCluster().getServerContext(), tid,
          ecids);

      // Verify that the tmp file are cleaned up
      Wait.waitFor(() -> FindCompactionTmpFiles
          .findTempFiles(getCluster().getServerContext(), tid.canonical()).size() == 0);

      // If the compaction actually ran it would have filtered data, so lets make sure all the data
      // written is there. This check provides evidence the compaction did not run.
      verify(client, table1, 1);

      // We need to cancel the compaction or delete the table here because we initiate a user
      // compaction above in the test. Even though the external compaction was cancelled
      // because we split the table, FaTE will continue to queue up a compaction
      client.tableOperations().cancelCompaction(table1);
      getCluster().getClusterControl().stop(ServerType.TABLET_SERVER);
    } finally {
      // We stopped the TServer and started our own, restart the original TabletServers
      getCluster().getClusterControl().start(ServerType.TABLET_SERVER);
      // Restart the regular compactors
      getCluster().getClusterControl().stopAllServers(ServerType.COMPACTOR);
      getCluster().getClusterControl().start(ServerType.COMPACTOR);

    }

  }

  @Test
  public void testManytablets() throws Exception {
    String table1 = this.getUniqueNames(1)[0];
    try (AccumuloClient client =
        Accumulo.newClient().from(getCluster().getClientProperties()).build()) {

      createTable(client, table1, "cs4", 200);

      writeData(client, table1);

      compact(client, table1, 3, GROUP4, true);

      verify(client, table1, 3);
    }
  }

  @Test
  public void testConfigurer() throws Exception {
    String tableName = this.getUniqueNames(1)[0];

    try (AccumuloClient client =
        Accumulo.newClient().from(getCluster().getClientProperties()).build()) {

      Map<String,String> props = Map.of("table.compaction.dispatcher",
          SimpleCompactionDispatcher.class.getName(), "table.compaction.dispatcher.opts.service",
          "cs5", Property.TABLE_FILE_COMPRESSION_TYPE.getKey(), "none");
      NewTableConfiguration ntc = new NewTableConfiguration().setProperties(props);
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
      long sizes = CompactionExecutorIT.getFileSizes(client, tableName);
      assertTrue(sizes > data.length * 10 && sizes < data.length * 11,
          "Unexpected files sizes : " + sizes);

      client.tableOperations().compact(tableName,
          new CompactionConfig().setWait(true)
              .setConfigurer(new PluginConfig(CompressionConfigurer.class.getName(),
                  Map.of(CompressionConfigurer.LARGE_FILE_COMPRESSION_TYPE, "gz",
                      CompressionConfigurer.LARGE_FILE_COMPRESSION_THRESHOLD, data.length + ""))));
      assertNoCompactionMetadata(getCluster().getServerContext(), tableName);

      // after compacting with compression, expect small file
      sizes = CompactionExecutorIT.getFileSizes(client, tableName);
      assertTrue(sizes < data.length,
          "Unexpected files sizes: data: " + data.length + ", file:" + sizes);

      client.tableOperations().compact(tableName, new CompactionConfig().setWait(true));
      assertNoCompactionMetadata(getCluster().getServerContext(), tableName);

      // after compacting without compression, expect big files again
      sizes = CompactionExecutorIT.getFileSizes(client, tableName);
      assertTrue(sizes > data.length * 10 && sizes < data.length * 11,
          "Unexpected files sizes : " + sizes);

      // We need to cancel the compaction or delete the table here because we initiate a user
      // compaction above in the test. Even though the external compaction was cancelled
      // because we split the table, FaTE will continue to queue up a compaction
      client.tableOperations().cancelCompaction(tableName);
    }
  }

  @Test
  public void testConfigurerSetOnTable() throws Exception {
    String tableName = this.getUniqueNames(1)[0];

    try (AccumuloClient client =
        Accumulo.newClient().from(getCluster().getClientProperties()).build()) {

      byte[] data = new byte[100000];

      Map<String,String> props = Map.of("table.compaction.dispatcher",
          SimpleCompactionDispatcher.class.getName(), "table.compaction.dispatcher.opts.service",
          "cs5", Property.TABLE_FILE_COMPRESSION_TYPE.getKey(), "none",
          Property.TABLE_COMPACTION_CONFIGURER.getKey(), CompressionConfigurer.class.getName(),
          Property.TABLE_COMPACTION_CONFIGURER_OPTS.getKey()
              + CompressionConfigurer.LARGE_FILE_COMPRESSION_TYPE,
          "gz", Property.TABLE_COMPACTION_CONFIGURER_OPTS.getKey()
              + CompressionConfigurer.LARGE_FILE_COMPRESSION_THRESHOLD,
          "" + data.length);
      NewTableConfiguration ntc = new NewTableConfiguration().setProperties(props);
      client.tableOperations().create(tableName, ntc);

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
      long sizes = CompactionExecutorIT.getFileSizes(client, tableName);
      assertTrue(sizes > data.length * 10 && sizes < data.length * 11,
          "Unexpected files sizes : " + sizes);

      client.tableOperations().compact(tableName, new CompactionConfig().setWait(true));
      assertNoCompactionMetadata(getCluster().getServerContext(), tableName);

      // after compacting with compression, expect small file
      sizes = CompactionExecutorIT.getFileSizes(client, tableName);
      assertTrue(sizes < data.length,
          "Unexpected files sizes: data: " + data.length + ", file:" + sizes);

      assertNoCompactionMetadata(getCluster().getServerContext(), tableName);

      // We need to cancel the compaction or delete the table here because we initiate a user
      // compaction above in the test. Even though the external compaction was cancelled
      // because we split the table, FaTE will continue to queue up a compaction
      client.tableOperations().cancelCompaction(tableName);
    }
  }

  public static class ExtDevNull extends DevNull {
    @Override
    public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options,
        IteratorEnvironment env) throws IOException {
      super.init(source, options, env);

      // this cast should fail if the compaction is running in the tserver
      CompactorIterEnv cienv = (CompactorIterEnv) env;

      Preconditions.checkArgument(!cienv.getQueueName().isEmpty());
    }
  }

  @Test
  public void testExternalCompactionWithTableIterator() throws Exception {
    // in addition to testing table configured iters w/ external compaction, this also tests an
    // external compaction that deletes everything

    String table1 = this.getUniqueNames(1)[0];
    try (AccumuloClient client =
        Accumulo.newClient().from(getCluster().getClientProperties()).build()) {
      createTable(client, table1, "cs6");
      writeData(client, table1);
      compact(client, table1, 2, GROUP6, true);
      verify(client, table1, 2);

      IteratorSetting setting = new IteratorSetting(50, "delete", ExtDevNull.class);
      client.tableOperations().attachIterator(table1, setting, EnumSet.of(IteratorScope.majc));
      client.tableOperations().compact(table1, new CompactionConfig().setWait(true));

      try (Scanner s = client.createScanner(table1)) {
        assertFalse(s.iterator().hasNext());
      }

      assertNoCompactionMetadata(getCluster().getServerContext(), table1);

      // We need to cancel the compaction or delete the table here because we initiate a user
      // compaction above in the test. Even though the external compaction was cancelled
      // because we split the table, FaTE will continue to queue up a compaction
      client.tableOperations().cancelCompaction(table1);
    }
  }

  @Test
  public void testExternalCompactionWithFencedFiles() throws Exception {
    String[] names = this.getUniqueNames(2);
    try (AccumuloClient client =
        Accumulo.newClient().from(getCluster().getClientProperties()).build()) {

      String table1 = names[0];
      createTable(client, table1, "cs1");

      String table2 = names[1];
      createTable(client, table2, "cs2");

      writeData(client, table1);
      writeData(client, table2);

      // Verify that all data can be seen
      verify(client, table1, 1, MAX_DATA);
      verify(client, table2, 1, MAX_DATA);

      // Split file in table1 into two files each fenced off by 100 rows for a total of 200
      splitFilesIntoRanges(getCluster().getServerContext(), table1,
          Set.of(new Range(new Text(row(100)), new Text(row(199))),
              new Range(new Text(row(300)), new Text(row(399)))));
      assertEquals(2, countFencedFiles(getCluster().getServerContext(), table1));

      // Fence file in table2 to 600 rows
      splitFilesIntoRanges(getCluster().getServerContext(), table2,
          Set.of(new Range(new Text(row(200)), new Text(row(799)))));
      assertEquals(1, countFencedFiles(getCluster().getServerContext(), table2));

      // Verify that a subset of the data is now seen after fencing
      verify(client, table1, 1, 200);
      verify(client, table2, 1, 600);

      // Compact and verify previousy fenced data didn't come back
      compact(client, table1, 2, GROUP1, true);
      verify(client, table1, 2, 200);

      SortedSet<Text> splits = new TreeSet<>();
      splits.add(new Text(row(MAX_DATA / 2)));
      client.tableOperations().addSplits(table2, splits);

      // Compact and verify previousy fenced data didn't come back
      compact(client, table2, 3, GROUP2, true);
      verify(client, table2, 3, 600);

      // should be no more fenced files after compaction
      assertEquals(0, countFencedFiles(getCluster().getServerContext(), table1));
      assertEquals(0, countFencedFiles(getCluster().getServerContext(), table2));
    }
  }

  public static class FSelector implements CompactionSelector {

    @Override
    public void init(InitParameters iparams) {}

    @Override
    public Selection select(SelectionParameters sparams) {
      List<CompactableFile> toCompact = sparams.getAvailableFiles().stream()
          .filter(cf -> cf.getFileName().startsWith("F")).collect(Collectors.toList());
      return new Selection(toCompact);
    }

  }

  @Test
  public void testPartialCompaction() throws Exception {
    String tableName = getUniqueNames(1)[0];
    try (final AccumuloClient client =
        Accumulo.newClient().from(getCluster().getClientProperties()).build()) {

      createTable(client, tableName, "cs8");

      writeData(client, tableName);
      // This should create an A file
      compact(client, tableName, 17, GROUP8, true);
      verify(client, tableName, 17);

      try (BatchWriter bw = client.createBatchWriter(tableName)) {
        for (int i = MAX_DATA; i < MAX_DATA * 2; i++) {
          Mutation m = new Mutation(row(i));
          m.put("", "", "" + i);
          bw.addMutation(m);
        }
      }

      // this should create an F file
      client.tableOperations().flush(tableName);

      // run a compaction that only compacts F files
      IteratorSetting iterSetting = new IteratorSetting(100, TestFilter.class);
      // make sure iterator options make it to compactor process
      iterSetting.addOption("expectedQ", GROUP8);
      // compact F file w/ different modulus and user pmodulus option for partial compaction
      iterSetting.addOption("pmodulus", 19 + "");
      CompactionConfig config = new CompactionConfig().setIterators(List.of(iterSetting))
          .setWait(true).setSelector(new PluginConfig(FSelector.class.getName()));
      client.tableOperations().compact(tableName, config);
      assertNoCompactionMetadata(getCluster().getServerContext(), tableName);

      try (Scanner scanner = client.createScanner(tableName)) {
        int count = 0;
        for (Entry<Key,Value> entry : scanner) {

          int v = Integer.parseInt(entry.getValue().toString());
          int modulus = v < MAX_DATA ? 17 : 19;

          assertEquals(0, Integer.parseInt(entry.getValue().toString()) % modulus,
              String.format("%s %s %d != 0", entry.getValue(), "%", modulus));
          count++;
        }

        int expectedCount = 0;
        for (int i = 0; i < MAX_DATA * 2; i++) {
          int modulus = i < MAX_DATA ? 17 : 19;
          if (i % modulus == 0) {
            expectedCount++;
          }
        }

        assertEquals(expectedCount, count);
      }

      // We need to cancel the compaction or delete the table here because we initiate a user
      // compaction above in the test. Even though the external compaction was cancelled
      // because we split the table, FaTE will continue to queue up a compaction
      client.tableOperations().cancelCompaction(tableName);
    }

  }

}

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

import static org.apache.accumulo.minicluster.ServerType.TABLET_SERVER;
import static org.apache.accumulo.test.compaction.ExternalCompactionTestUtils.MAX_DATA;
import static org.apache.accumulo.test.compaction.ExternalCompactionTestUtils.QUEUE1;
import static org.apache.accumulo.test.compaction.ExternalCompactionTestUtils.QUEUE2;
import static org.apache.accumulo.test.compaction.ExternalCompactionTestUtils.QUEUE3;
import static org.apache.accumulo.test.compaction.ExternalCompactionTestUtils.QUEUE4;
import static org.apache.accumulo.test.compaction.ExternalCompactionTestUtils.QUEUE5;
import static org.apache.accumulo.test.compaction.ExternalCompactionTestUtils.QUEUE6;
import static org.apache.accumulo.test.compaction.ExternalCompactionTestUtils.QUEUE7;
import static org.apache.accumulo.test.compaction.ExternalCompactionTestUtils.QUEUE8;
import static org.apache.accumulo.test.compaction.ExternalCompactionTestUtils.compact;
import static org.apache.accumulo.test.compaction.ExternalCompactionTestUtils.createTable;
import static org.apache.accumulo.test.compaction.ExternalCompactionTestUtils.getFinalStatesForTable;
import static org.apache.accumulo.test.compaction.ExternalCompactionTestUtils.row;
import static org.apache.accumulo.test.compaction.ExternalCompactionTestUtils.verify;
import static org.apache.accumulo.test.compaction.ExternalCompactionTestUtils.writeData;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.accumulo.compactor.Compactor;
import org.apache.accumulo.compactor.ExtCEnv.CompactorIterEnv;
import org.apache.accumulo.coordinator.CompactionCoordinator;
import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
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
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.DevNull;
import org.apache.accumulo.core.iterators.Filter;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.IteratorUtil.IteratorScope;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.metadata.schema.ExternalCompactionFinalState;
import org.apache.accumulo.core.metadata.schema.ExternalCompactionFinalState.FinalState;
import org.apache.accumulo.core.metadata.schema.ExternalCompactionId;
import org.apache.accumulo.core.metadata.schema.ExternalCompactionMetadata;
import org.apache.accumulo.core.metadata.schema.TabletMetadata;
import org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType;
import org.apache.accumulo.core.metadata.schema.TabletsMetadata;
import org.apache.accumulo.core.spi.compaction.SimpleCompactionDispatcher;
import org.apache.accumulo.core.util.UtilWaitThread;
import org.apache.accumulo.harness.MiniClusterConfigurationCallback;
import org.apache.accumulo.harness.SharedMiniClusterBase;
import org.apache.accumulo.minicluster.ServerType;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloClusterImpl.ProcessInfo;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloConfigImpl;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

public class ExternalCompaction_1_IT extends SharedMiniClusterBase {

  public static class ExternalCompaction1Config implements MiniClusterConfigurationCallback {
    @Override
    public void configureMiniCluster(MiniAccumuloConfigImpl cfg, Configuration coreSite) {
      ExternalCompactionTestUtils.configureMiniCluster(cfg, coreSite);
      cfg.setNumCompactors(2);
    }
  }

  private static final Logger LOG = LoggerFactory.getLogger(ExternalCompaction_1_IT.class);

  @BeforeAll
  public static void beforeTests() throws Exception {
    startMiniClusterWithConfig(new ExternalCompaction1Config());
  }

  @AfterEach
  public void tearDown() throws Exception {
    // The ExternalDoNothingCompactor needs to be restarted between tests
    getCluster().getClusterControl().stop(ServerType.COMPACTOR);
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

      getCluster().getClusterControl().startCoordinator(CompactionCoordinator.class);
      getCluster().getClusterControl().startCompactors(Compactor.class, 1, QUEUE1);
      getCluster().getClusterControl().startCompactors(Compactor.class, 1, QUEUE2);

      compact(client, table1, 2, QUEUE1, true);
      verify(client, table1, 2);

      SortedSet<Text> splits = new TreeSet<>();
      splits.add(new Text(row(MAX_DATA / 2)));
      client.tableOperations().addSplits(table2, splits);

      compact(client, table2, 3, QUEUE2, true);
      verify(client, table2, 3);

    }
  }

  @Test
  public void testCompactionAndCompactorDies() throws Exception {
    String table1 = this.getUniqueNames(1)[0];
    try (AccumuloClient client =
        Accumulo.newClient().from(getCluster().getClientProperties()).build()) {
      // Stop the TabletServer so that it does not commit the compaction
      getCluster().getProcesses().get(TABLET_SERVER).forEach(p -> {
        try {
          getCluster().killProcess(TABLET_SERVER, p);
        } catch (Exception e) {
          fail("Failed to shutdown tablet server");
        }
      });
      // Start our TServer that will not commit the compaction
      ProcessInfo tserverProcess = getCluster().exec(ExternalCompactionTServer.class);

      createTable(client, table1, "cs3", 2);
      writeData(client, table1);

      getCluster().getClusterControl().startCompactors(ExternalDoNothingCompactor.class, 1, QUEUE3);
      getCluster().getClusterControl().startCoordinator(CompactionCoordinator.class);

      compact(client, table1, 2, QUEUE3, false);
      TableId tid = getCluster().getServerContext().getTableId(table1);

      // Wait for the compaction to start by waiting for 1 external compaction column
      ExternalCompactionTestUtils
          .waitForCompactionStartAndReturnEcids(getCluster().getServerContext(), tid);

      // Kill the compactor
      getCluster().getClusterControl().stop(ServerType.COMPACTOR);

      // DeadCompactionDetector in the CompactionCoordinator should fail the compaction.
      long count = 0;
      while (count == 0) {
        count = getFinalStatesForTable(getCluster(), tid)
            .filter(state -> state.getFinalState().equals(FinalState.FAILED)).count();
        UtilWaitThread.sleep(250);
      }

      // We need to cancel the compaction or delete the table here because we initiate a user
      // compaction above in the test. Even though the external compaction was cancelled
      // because we split the table, FaTE will continue to queue up a compaction
      client.tableOperations().cancelCompaction(table1);
      getCluster().stopProcessWithTimeout(tserverProcess.getProcess(), 30, TimeUnit.SECONDS);
      getCluster().getClusterControl().stop(ServerType.TABLET_SERVER);
    } finally {
      // We stopped the TServer and started our own, restart the original TabletServers
      getCluster().getClusterControl().start(ServerType.TABLET_SERVER);
    }

  }

  @Test
  public void testManytablets() throws Exception {
    String table1 = this.getUniqueNames(1)[0];
    try (AccumuloClient client =
        Accumulo.newClient().from(getCluster().getClientProperties()).build()) {

      createTable(client, table1, "cs4", 200);

      writeData(client, table1);

      getCluster().getClusterControl().startCompactors(Compactor.class, 2, QUEUE4);
      getCluster().getClusterControl().startCoordinator(CompactionCoordinator.class);

      compact(client, table1, 3, QUEUE4, true);

      verify(client, table1, 3);
    }
  }

  @Test
  public void testConfigurer() throws Exception {
    String tableName = this.getUniqueNames(1)[0];

    getCluster().getClusterControl().startCompactors(Compactor.class, 1, QUEUE5);
    getCluster().getClusterControl().startCoordinator(CompactionCoordinator.class);

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

      // after compacting with compression, expect small file
      sizes = CompactionExecutorIT.getFileSizes(client, tableName);
      assertTrue(sizes < data.length,
          "Unexpected files sizes: data: " + data.length + ", file:" + sizes);

      client.tableOperations().compact(tableName, new CompactionConfig().setWait(true));

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
      getCluster().getClusterControl().startCompactors(Compactor.class, 1, QUEUE6);
      getCluster().getClusterControl().startCoordinator(CompactionCoordinator.class);
      compact(client, table1, 2, QUEUE6, true);
      verify(client, table1, 2);

      IteratorSetting setting = new IteratorSetting(50, "delete", ExtDevNull.class);
      client.tableOperations().attachIterator(table1, setting, EnumSet.of(IteratorScope.majc));
      client.tableOperations().compact(table1, new CompactionConfig().setWait(true));

      try (Scanner s = client.createScanner(table1)) {
        assertFalse(s.iterator().hasNext());
      }

      // We need to cancel the compaction or delete the table here because we initiate a user
      // compaction above in the test. Even though the external compaction was cancelled
      // because we split the table, FaTE will continue to queue up a compaction
      client.tableOperations().cancelCompaction(table1);
    }
  }

  @Test
  public void testExternalCompactionDeadTServer() throws Exception {
    // Shut down the normal TServers
    getCluster().getProcesses().get(TABLET_SERVER).forEach(p -> {
      try {
        getCluster().killProcess(TABLET_SERVER, p);
      } catch (Exception e) {
        fail("Failed to shutdown tablet server");
      }
    });
    // Start our TServer that will not commit the compaction
    ProcessInfo tserverProcess = getCluster().exec(ExternalCompactionTServer.class);

    final String table3 = this.getUniqueNames(1)[0];

    try (final AccumuloClient client =
        Accumulo.newClient().from(getCluster().getClientProperties()).build()) {
      createTable(client, table3, "cs7");
      writeData(client, table3);
      getCluster().getClusterControl().startCompactors(Compactor.class, 1, QUEUE7);
      getCluster().getClusterControl().startCoordinator(CompactionCoordinator.class);
      compact(client, table3, 2, QUEUE7, false);

      // ExternalCompactionTServer will not commit the compaction. Wait for the
      // metadata table entries to show up.
      LOG.info("Waiting for external compaction to complete.");
      TableId tid = getCluster().getServerContext().getTableId(table3);
      Stream<ExternalCompactionFinalState> fs = getFinalStatesForTable(getCluster(), tid);
      while (fs.findAny().isEmpty()) {
        LOG.info("Waiting for compaction completed marker to appear");
        UtilWaitThread.sleep(250);
        fs = getFinalStatesForTable(getCluster(), tid);
      }

      LOG.info("Validating metadata table contents.");
      TabletsMetadata tm = getCluster().getServerContext().getAmple().readTablets().forTable(tid)
          .fetch(ColumnType.ECOMP).build();
      List<TabletMetadata> md = new ArrayList<>();
      tm.forEach(t -> md.add(t));
      assertEquals(1, md.size());
      TabletMetadata m = md.get(0);
      Map<ExternalCompactionId,ExternalCompactionMetadata> em = m.getExternalCompactions();
      assertEquals(1, em.size());
      List<ExternalCompactionFinalState> finished = new ArrayList<>();
      getFinalStatesForTable(getCluster(), tid).forEach(f -> finished.add(f));
      assertEquals(1, finished.size());
      assertEquals(em.entrySet().iterator().next().getKey(),
          finished.get(0).getExternalCompactionId());
      tm.close();

      // Force a flush on the metadata table before killing our tserver
      client.tableOperations().flush(MetadataTable.NAME);

      // Stop our TabletServer. Need to perform a normal shutdown so that the WAL is closed
      // normally.
      LOG.info("Stopping our tablet server");
      getCluster().stopProcessWithTimeout(tserverProcess.getProcess(), 30, TimeUnit.SECONDS);
      getCluster().getClusterControl().stop(ServerType.TABLET_SERVER);

      // Start a TabletServer to commit the compaction.
      LOG.info("Starting normal tablet server");
      getCluster().getClusterControl().start(ServerType.TABLET_SERVER);

      // Wait for the compaction to be committed.
      LOG.info("Waiting for compaction completed marker to disappear");
      Stream<ExternalCompactionFinalState> fs2 = getFinalStatesForTable(getCluster(), tid);
      while (fs2.findAny().isPresent()) {
        LOG.info("Waiting for compaction completed marker to disappear");
        UtilWaitThread.sleep(500);
        fs2 = getFinalStatesForTable(getCluster(), tid);
      }
      verify(client, table3, 2);

      // We need to cancel the compaction or delete the table here because we initiate a user
      // compaction above in the test. Even though the external compaction was cancelled
      // because we split the table, FaTE will continue to queue up a compaction
      client.tableOperations().cancelCompaction(table3);
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

      getCluster().getClusterControl().startCompactors(Compactor.class, 1, QUEUE8);
      getCluster().getClusterControl().startCoordinator(CompactionCoordinator.class);

      createTable(client, tableName, "cs8");

      writeData(client, tableName);
      // This should create an A file
      compact(client, tableName, 17, QUEUE8, true);
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
      iterSetting.addOption("expectedQ", QUEUE8);
      // compact F file w/ different modulus and user pmodulus option for partial compaction
      iterSetting.addOption("pmodulus", 19 + "");
      CompactionConfig config = new CompactionConfig().setIterators(List.of(iterSetting))
          .setWait(true).setSelector(new PluginConfig(FSelector.class.getName()));
      client.tableOperations().compact(tableName, config);

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

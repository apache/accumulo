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
package org.apache.accumulo.test.compaction;

import static org.apache.accumulo.minicluster.ServerType.TABLET_SERVER;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
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
import org.apache.accumulo.core.clientImpl.Tables;
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
import org.apache.accumulo.core.metadata.schema.ExternalCompactionFinalState;
import org.apache.accumulo.core.metadata.schema.ExternalCompactionFinalState.FinalState;
import org.apache.accumulo.core.metadata.schema.ExternalCompactionId;
import org.apache.accumulo.core.metadata.schema.ExternalCompactionMetadata;
import org.apache.accumulo.core.metadata.schema.TabletMetadata;
import org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType;
import org.apache.accumulo.core.metadata.schema.TabletsMetadata;
import org.apache.accumulo.core.spi.compaction.SimpleCompactionDispatcher;
import org.apache.accumulo.fate.util.UtilWaitThread;
import org.apache.accumulo.harness.AccumuloClusterHarness;
import org.apache.accumulo.harness.MiniClusterConfigurationCallback;
import org.apache.accumulo.minicluster.ServerType;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloClusterImpl;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloClusterImpl.ProcessInfo;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloConfigImpl;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

public class ExternalCompaction_1_IT extends AccumuloClusterHarness
    implements MiniClusterConfigurationCallback {

  private static final Logger LOG = LoggerFactory.getLogger(ExternalCompaction_1_IT.class);

  @Override
  public void configureMiniCluster(MiniAccumuloConfigImpl cfg, Configuration coreSite) {
    ExternalCompactionUtils.configureMiniCluster(cfg, coreSite);
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
    ProcessInfo c1 = null, c2 = null, coord = null;
    String[] names = this.getUniqueNames(2);
    try (AccumuloClient client =
        Accumulo.newClient().from(getCluster().getClientProperties()).build()) {

      String table1 = names[0];
      ExternalCompactionUtils.createTable(client, table1, "cs1");

      String table2 = names[1];
      ExternalCompactionUtils.createTable(client, table2, "cs2");

      ExternalCompactionUtils.writeData(client, table1);
      ExternalCompactionUtils.writeData(client, table2);

      c1 = ((MiniAccumuloClusterImpl) getCluster()).exec(Compactor.class, "-q", "DCQ1");
      c2 = ((MiniAccumuloClusterImpl) getCluster()).exec(Compactor.class, "-q", "DCQ2");
      coord = ((MiniAccumuloClusterImpl) getCluster()).exec(CompactionCoordinator.class);

      ExternalCompactionUtils.compact(client, table1, 2, "DCQ1", true);
      ExternalCompactionUtils.verify(client, table1, 2);

      SortedSet<Text> splits = new TreeSet<>();
      splits.add(new Text(ExternalCompactionUtils.row(ExternalCompactionUtils.MAX_DATA / 2)));
      client.tableOperations().addSplits(table2, splits);

      ExternalCompactionUtils.compact(client, table2, 3, "DCQ2", true);
      ExternalCompactionUtils.verify(client, table2, 3);

    } finally {
      // Stop the Compactor and Coordinator that we started
      ExternalCompactionUtils.stopProcesses(c1, c2, coord);
    }
  }

  @Test
  public void testCompactionAndCompactorDies() throws Exception {
    String table1 = this.getUniqueNames(1)[0];
    try (AccumuloClient client =
        Accumulo.newClient().from(getCluster().getClientProperties()).build()) {
      // Stop the TabletServer so that it does not commit the compaction
      ((MiniAccumuloClusterImpl) getCluster()).getProcesses().get(TABLET_SERVER).forEach(p -> {
        try {
          ((MiniAccumuloClusterImpl) getCluster()).killProcess(TABLET_SERVER, p);
        } catch (Exception e) {
          fail("Failed to shutdown tablet server");
        }
      });
      // Start our TServer that will not commit the compaction
      ProcessInfo tserv =
          ((MiniAccumuloClusterImpl) getCluster()).exec(ExternalCompactionTServer.class);

      ExternalCompactionUtils.createTable(client, table1, "cs1", 2);
      ExternalCompactionUtils.writeData(client, table1);
      ProcessInfo c1 = ((MiniAccumuloClusterImpl) getCluster())
          .exec(ExternalDoNothingCompactor.class, "-q", "DCQ1");
      ProcessInfo coord =
          ((MiniAccumuloClusterImpl) getCluster()).exec(CompactionCoordinator.class);
      ExternalCompactionUtils.compact(client, table1, 2, "DCQ1", false);
      TableId tid = Tables.getTableId(getCluster().getServerContext(), table1);
      // Wait for the compaction to start by waiting for 1 external compaction column
      Set<ExternalCompactionId> ecids = new HashSet<>();
      do {
        UtilWaitThread.sleep(250);
        try (TabletsMetadata tm = getCluster().getServerContext().getAmple().readTablets()
            .forTable(tid).fetch(ColumnType.ECOMP).build()) {
          tm.stream().flatMap(t -> t.getExternalCompactions().keySet().stream())
              .forEach(ecids::add);
        }
      } while (ecids.isEmpty());

      // Kill the compactor
      ExternalCompactionUtils.stopProcesses(c1);

      // DeadCompactionDetector in the CompactionCoordinator should fail the compaction.
      long count = 0;
      while (count == 0) {
        count = ExternalCompactionUtils.getFinalStatesForTable(getCluster(), tid)
            .filter(state -> state.getFinalState().equals(FinalState.FAILED)).count();
        UtilWaitThread.sleep(250);
      }

      // Stop the processes we started
      ExternalCompactionUtils.stopProcesses(tserv, coord);
    } finally {
      // We stopped the TServer and started our own, restart the original TabletServers
      ((MiniAccumuloClusterImpl) getCluster()).getClusterControl().start(ServerType.TABLET_SERVER);
    }

  }

  @Test
  public void testManytablets() throws Exception {
    ProcessInfo c1 = null, c2 = null, c3 = null, c4 = null, coord = null;
    String table1 = this.getUniqueNames(1)[0];
    try (AccumuloClient client =
        Accumulo.newClient().from(getCluster().getClientProperties()).build()) {

      ExternalCompactionUtils.createTable(client, table1, "cs1", 200);

      ExternalCompactionUtils.writeData(client, table1);

      c1 = ((MiniAccumuloClusterImpl) getCluster()).exec(Compactor.class, "-q", "DCQ1");
      c2 = ((MiniAccumuloClusterImpl) getCluster()).exec(Compactor.class, "-q", "DCQ1");
      c3 = ((MiniAccumuloClusterImpl) getCluster()).exec(Compactor.class, "-q", "DCQ1");
      c4 = ((MiniAccumuloClusterImpl) getCluster()).exec(Compactor.class, "-q", "DCQ1");
      coord = ((MiniAccumuloClusterImpl) getCluster()).exec(CompactionCoordinator.class);

      ExternalCompactionUtils.compact(client, table1, 3, "DCQ1", true);

      ExternalCompactionUtils.verify(client, table1, 3);
    } finally {
      ExternalCompactionUtils.stopProcesses(c1, c2, c3, c4, coord);
    }
  }

  @Test
  public void testConfigurer() throws Exception {
    String tableName = this.getUniqueNames(1)[0];

    ProcessInfo c1 = ((MiniAccumuloClusterImpl) getCluster()).exec(Compactor.class, "-q", "DCQ1");
    ProcessInfo coord = ((MiniAccumuloClusterImpl) getCluster()).exec(CompactionCoordinator.class);

    try (AccumuloClient client =
        Accumulo.newClient().from(getCluster().getClientProperties()).build()) {

      Map<String,String> props = Map.of("table.compaction.dispatcher",
          SimpleCompactionDispatcher.class.getName(), "table.compaction.dispatcher.opts.service",
          "cs1", Property.TABLE_FILE_COMPRESSION_TYPE.getKey(), "none");
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
      assertTrue("Unexpected files sizes : " + sizes,
          sizes > data.length * 10 && sizes < data.length * 11);

      client.tableOperations().compact(tableName,
          new CompactionConfig().setWait(true)
              .setConfigurer(new PluginConfig(CompressionConfigurer.class.getName(),
                  Map.of(CompressionConfigurer.LARGE_FILE_COMPRESSION_TYPE, "gz",
                      CompressionConfigurer.LARGE_FILE_COMPRESSION_THRESHOLD, data.length + ""))));

      // after compacting with compression, expect small file
      sizes = CompactionExecutorIT.getFileSizes(client, tableName);
      assertTrue("Unexpected files sizes: data: " + data.length + ", file:" + sizes,
          sizes < data.length);

      client.tableOperations().compact(tableName, new CompactionConfig().setWait(true));

      // after compacting without compression, expect big files again
      sizes = CompactionExecutorIT.getFileSizes(client, tableName);
      assertTrue("Unexpected files sizes : " + sizes,
          sizes > data.length * 10 && sizes < data.length * 11);

    } finally {
      ExternalCompactionUtils.stopProcesses(c1, coord);
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

    ProcessInfo c1 = null, coord = null;
    String table1 = this.getUniqueNames(1)[0];
    try (AccumuloClient client =
        Accumulo.newClient().from(getCluster().getClientProperties()).build()) {
      ExternalCompactionUtils.createTable(client, table1, "cs1");
      ExternalCompactionUtils.writeData(client, table1);
      c1 = ((MiniAccumuloClusterImpl) getCluster()).exec(Compactor.class, "-q", "DCQ1");
      coord = ((MiniAccumuloClusterImpl) getCluster()).exec(CompactionCoordinator.class);
      ExternalCompactionUtils.compact(client, table1, 2, "DCQ1", true);
      ExternalCompactionUtils.verify(client, table1, 2);

      IteratorSetting setting = new IteratorSetting(50, "delete", ExtDevNull.class);
      client.tableOperations().attachIterator(table1, setting, EnumSet.of(IteratorScope.majc));
      client.tableOperations().compact(table1, new CompactionConfig().setWait(true));

      try (Scanner s = client.createScanner(table1)) {
        assertFalse(s.iterator().hasNext());
      }
    } finally {
      ExternalCompactionUtils.stopProcesses(c1, coord);
    }
  }

  @Test
  public void testExternalCompactionDeadTServer() throws Exception {
    // Shut down the normal TServers
    ((MiniAccumuloClusterImpl) getCluster()).getProcesses().get(TABLET_SERVER).forEach(p -> {
      try {
        ((MiniAccumuloClusterImpl) getCluster()).killProcess(TABLET_SERVER, p);
      } catch (Exception e) {
        fail("Failed to shutdown tablet server");
      }
    });
    // Start our TServer that will not commit the compaction
    ProcessInfo tserv =
        ((MiniAccumuloClusterImpl) getCluster()).exec(ExternalCompactionTServer.class);

    final String table3 = this.getUniqueNames(1)[0];
    ProcessInfo c1 = null, coord = null;
    try (final AccumuloClient client =
        Accumulo.newClient().from(getCluster().getClientProperties()).build()) {
      ExternalCompactionUtils.createTable(client, table3, "cs1");
      ExternalCompactionUtils.writeData(client, table3);
      c1 = ((MiniAccumuloClusterImpl) getCluster()).exec(Compactor.class, "-q", "DCQ1");
      coord = ((MiniAccumuloClusterImpl) getCluster()).exec(CompactionCoordinator.class);
      ExternalCompactionUtils.compact(client, table3, 2, "DCQ1", false);

      // ExternalCompactionTServer will not commit the compaction. Wait for the
      // metadata table entries to show up.
      LOG.info("Waiting for external compaction to complete.");
      TableId tid = Tables.getTableId(getCluster().getServerContext(), table3);
      Stream<ExternalCompactionFinalState> fs =
          ExternalCompactionUtils.getFinalStatesForTable(getCluster(), tid);
      while (fs.count() == 0) {
        LOG.info("Waiting for compaction completed marker to appear");
        UtilWaitThread.sleep(250);
        fs = ExternalCompactionUtils.getFinalStatesForTable(getCluster(), tid);
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
      ExternalCompactionUtils.getFinalStatesForTable(getCluster(), tid)
          .forEach(f -> finished.add(f));
      assertEquals(1, finished.size());
      assertEquals(em.entrySet().iterator().next().getKey(),
          finished.get(0).getExternalCompactionId());
      tm.close();

      // Force a flush on the metadata table before killing our tserver
      client.tableOperations().flush("accumulo.metadata");

      // Stop our TabletServer. Need to perform a normal shutdown so that the WAL is closed
      // normally.
      LOG.info("Stopping our tablet server");
      ExternalCompactionUtils.stopProcesses(tserv);

      // Start a TabletServer to commit the compaction.
      LOG.info("Starting normal tablet server");
      ((MiniAccumuloClusterImpl) getCluster()).getClusterControl().start(ServerType.TABLET_SERVER);

      // Wait for the compaction to be committed.
      LOG.info("Waiting for compaction completed marker to disappear");
      Stream<ExternalCompactionFinalState> fs2 =
          ExternalCompactionUtils.getFinalStatesForTable(getCluster(), tid);
      while (fs2.count() != 0) {
        LOG.info("Waiting for compaction completed marker to disappear");
        UtilWaitThread.sleep(500);
        fs2 = ExternalCompactionUtils.getFinalStatesForTable(getCluster(), tid);
      }
      ExternalCompactionUtils.verify(client, table3, 2);
    } finally {
      ExternalCompactionUtils.stopProcesses(c1, coord);
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
    ProcessInfo c1 = null, coord = null;
    String tableName = getUniqueNames(1)[0];
    try (final AccumuloClient client =
        Accumulo.newClient().from(getCluster().getClientProperties()).build()) {

      c1 = ((MiniAccumuloClusterImpl) getCluster()).exec(Compactor.class, "-q", "DCQ1");
      coord = ((MiniAccumuloClusterImpl) getCluster()).exec(CompactionCoordinator.class);

      ExternalCompactionUtils.createTable(client, tableName, "cs1");

      ExternalCompactionUtils.writeData(client, tableName);
      // This should create an A file
      ExternalCompactionUtils.compact(client, tableName, 17, "DCQ1", true);
      ExternalCompactionUtils.verify(client, tableName, 17);

      try (BatchWriter bw = client.createBatchWriter(tableName)) {
        for (int i = ExternalCompactionUtils.MAX_DATA; i < ExternalCompactionUtils.MAX_DATA * 2;
            i++) {
          Mutation m = new Mutation(ExternalCompactionUtils.row(i));
          m.put("", "", "" + i);
          bw.addMutation(m);
        }
      }

      // this should create an F file
      client.tableOperations().flush(tableName);

      // run a compaction that only compacts F files
      IteratorSetting iterSetting = new IteratorSetting(100, TestFilter.class);
      // make sure iterator options make it to compactor process
      iterSetting.addOption("expectedQ", "DCQ1");
      // compact F file w/ different modulus and user pmodulus option for partial compaction
      iterSetting.addOption("pmodulus", 19 + "");
      CompactionConfig config = new CompactionConfig().setIterators(List.of(iterSetting))
          .setWait(true).setSelector(new PluginConfig(FSelector.class.getName()));
      client.tableOperations().compact(tableName, config);

      try (Scanner scanner = client.createScanner(tableName)) {
        int count = 0;
        for (Entry<Key,Value> entry : scanner) {

          int v = Integer.parseInt(entry.getValue().toString());
          int modulus = v < ExternalCompactionUtils.MAX_DATA ? 17 : 19;

          assertTrue(String.format("%s %s %d != 0", entry.getValue(), "%", modulus),
              Integer.parseInt(entry.getValue().toString()) % modulus == 0);
          count++;
        }

        int expectedCount = 0;
        for (int i = 0; i < ExternalCompactionUtils.MAX_DATA * 2; i++) {
          int modulus = i < ExternalCompactionUtils.MAX_DATA ? 17 : 19;
          if (i % modulus == 0) {
            expectedCount++;
          }
        }

        assertEquals(expectedCount, count);
      }

    } finally {
      ExternalCompactionUtils.stopProcesses(c1, coord);
    }
  }

}

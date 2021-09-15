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

import static org.apache.accumulo.minicluster.ServerType.TABLET_SERVER;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpClient;
import java.net.http.HttpClient.Redirect;
import java.net.http.HttpClient.Version;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpResponse.BodyHandlers;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.accumulo.compactor.Compactor;
import org.apache.accumulo.compactor.ExtCEnv.CompactorIterEnv;
import org.apache.accumulo.coordinator.CompactionCoordinator;
import org.apache.accumulo.coordinator.ExternalCompactionMetrics;
import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
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
import org.apache.accumulo.core.metadata.schema.Ample.DataLevel;
import org.apache.accumulo.core.metadata.schema.ExternalCompactionFinalState;
import org.apache.accumulo.core.metadata.schema.ExternalCompactionFinalState.FinalState;
import org.apache.accumulo.core.metadata.schema.ExternalCompactionId;
import org.apache.accumulo.core.metadata.schema.ExternalCompactionMetadata;
import org.apache.accumulo.core.metadata.schema.TabletMetadata;
import org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType;
import org.apache.accumulo.core.metadata.schema.TabletsMetadata;
import org.apache.accumulo.core.spi.compaction.DefaultCompactionPlanner;
import org.apache.accumulo.core.spi.compaction.SimpleCompactionDispatcher;
import org.apache.accumulo.fate.util.UtilWaitThread;
import org.apache.accumulo.harness.MiniClusterConfigurationCallback;
import org.apache.accumulo.harness.SharedMiniClusterBase;
import org.apache.accumulo.minicluster.ServerType;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloClusterImpl.ProcessInfo;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloConfigImpl;
import org.apache.accumulo.miniclusterImpl.ProcessReference;
import org.apache.commons.io.input.Tailer;
import org.apache.commons.io.input.TailerListenerAdapter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.io.Text;
import org.bouncycastle.util.Arrays;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.gson.Gson;

public class ExternalCompactionIT extends SharedMiniClusterBase
    implements MiniClusterConfigurationCallback {

  private static final Logger LOG = LoggerFactory.getLogger(ExternalCompactionIT.class);

  private static final int MAX_DATA = 1000;

  private HttpRequest req = null;
  {
    try {
      req = HttpRequest.newBuilder().GET().uri(new URI("http://localhost:9099/metrics")).build();
    } catch (URISyntaxException e) {
      throw new RuntimeException(e);
    }
  }
  private final HttpClient hc =
      HttpClient.newBuilder().version(Version.HTTP_1_1).followRedirects(Redirect.NORMAL).build();

  private static String row(int r) {
    return String.format("r:%04d", r);
  }

  @Override
  public void configureMiniCluster(MiniAccumuloConfigImpl cfg, Configuration coreSite) {
    cfg.setProperty("tserver.compaction.major.service.cs1.planner",
        DefaultCompactionPlanner.class.getName());
    cfg.setProperty("tserver.compaction.major.service.cs1.planner.opts.executors",
        "[{'name':'all', 'type': 'external', 'queue': 'DCQ1'}]");
    cfg.setProperty("tserver.compaction.major.service.cs2.planner",
        DefaultCompactionPlanner.class.getName());
    cfg.setProperty("tserver.compaction.major.service.cs2.planner.opts.executors",
        "[{'name':'all', 'type': 'external','queue': 'DCQ2'}]");
    cfg.setProperty(Property.COMPACTION_COORDINATOR_DEAD_COMPACTOR_CHECK_INTERVAL.getKey(), "5s");
    cfg.setProperty(Property.COMPACTION_COORDINATOR_TSERVER_COMPACTION_CHECK_INTERVAL, "3s");
    cfg.setProperty(Property.COMPACTOR_PORTSEARCH, "true");
    // use raw local file system so walogs sync and flush will work
    coreSite.set("fs.file.impl", RawLocalFileSystem.class.getName());
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

  @Before
  public void setUp() throws Exception {
    if (SharedMiniClusterBase.getCluster() == null) {
      SharedMiniClusterBase.startMiniClusterWithConfig(this);
    }
  }

  @After
  public void tearDown() throws Exception {
    // The tables need to be deleted between tests because MAC
    // is not being restarted and it's possible that a test
    // will not get the expected compaction. The compaction that
    // is run during a test could be for a table from the previous
    // test due to the way the previous test ended.
    cleanupTables();
  }

  private void stopProcesses(ProcessInfo... processes) throws Exception {
    for (ProcessInfo p : processes) {
      if (p != null) {
        Process proc = p.getProcess();
        if (proc.supportsNormalTermination()) {
          proc.destroyForcibly();
        } else {
          LOG.info("Stopping process manually");
          new ProcessBuilder("kill", Long.toString(proc.pid())).start();
          proc.waitFor();
        }
      }
    }
  }

  private void cleanupTables() {
    try (AccumuloClient client = Accumulo.newClient()
        .from(SharedMiniClusterBase.getCluster().getClientProperties()).build()) {
      for (String table : client.tableOperations().list()) {
        try {
          if (!table.startsWith("accumulo")) {
            client.tableOperations().cancelCompaction(table);
            client.tableOperations().delete(table);
          }
        } catch (Exception e) {
          fail("Error deleting table: " + table + ", msg: " + e.getMessage());
        }
      }
    }
  }

  private Stream<ExternalCompactionFinalState> getFinalStatesForTable(TableId tid) {
    return getCluster().getServerContext().getAmple().getExternalCompactionFinalStates()
        .filter(state -> state.getExtent().tableId().equals(tid));
  }

  @Test
  public void testExternalCompaction() throws Exception {
    ProcessInfo c1 = null, c2 = null, coord = null;
    String[] names = this.getUniqueNames(2);
    try (AccumuloClient client = Accumulo.newClient()
        .from(SharedMiniClusterBase.getCluster().getClientProperties()).build()) {

      String table1 = names[0];
      createTable(client, table1, "cs1");

      String table2 = names[1];
      createTable(client, table2, "cs2");

      writeData(client, table1);
      writeData(client, table2);

      c1 = SharedMiniClusterBase.getCluster().exec(Compactor.class, "-q", "DCQ1");
      c2 = SharedMiniClusterBase.getCluster().exec(Compactor.class, "-q", "DCQ2");
      coord = SharedMiniClusterBase.getCluster().exec(CompactionCoordinator.class);

      compact(client, table1, 2, "DCQ1", true);
      verify(client, table1, 2);

      SortedSet<Text> splits = new TreeSet<>();
      splits.add(new Text(row(MAX_DATA / 2)));
      client.tableOperations().addSplits(table2, splits);

      compact(client, table2, 3, "DCQ2", true);
      verify(client, table2, 3);

    } finally {
      // Stop the Compactor and Coordinator that we started
      stopProcesses(c1, c2, coord);
    }
  }

  @Test
  public void testSplitDuringExternalCompaction() throws Exception {
    ProcessInfo c1 = null, coord = null;
    String table1 = this.getUniqueNames(1)[0];
    try (AccumuloClient client = Accumulo.newClient()
        .from(SharedMiniClusterBase.getCluster().getClientProperties()).build()) {
      createTable(client, table1, "cs1");
      TableId tid = Tables.getTableId(getCluster().getServerContext(), table1);
      writeData(client, table1);

      c1 = SharedMiniClusterBase.getCluster().exec(ExternalDoNothingCompactor.class, "-q", "DCQ1");
      coord = SharedMiniClusterBase.getCluster().exec(TestCompactionCoordinator.class);
      compact(client, table1, 2, "DCQ1", false);

      // Wait for the compaction to start by waiting for 1 external compaction column
      Set<ExternalCompactionId> ecids = new HashSet<>();
      do {
        UtilWaitThread.sleep(50);
        try (TabletsMetadata tm = getCluster().getServerContext().getAmple().readTablets()
            .forTable(tid).fetch(ColumnType.ECOMP).build()) {
          tm.stream().flatMap(t -> t.getExternalCompactions().keySet().stream())
              .forEach(ecids::add);
        }
      } while (ecids.isEmpty());

      // ExternalDoNothingCompactor will not compact, it will wait, split the table.
      SortedSet<Text> splits = new TreeSet<>();
      int jump = MAX_DATA / 5;
      for (int r = jump; r < MAX_DATA; r += jump) {
        splits.add(new Text(row(r)));
      }

      assertEquals(0, getCoordinatorMetrics().getFailed());

      client.tableOperations().addSplits(table1, splits);

      // wait for failure or test timeout
      ExternalCompactionMetrics metrics = getCoordinatorMetrics();
      while (metrics.getFailed() == 0) {
        UtilWaitThread.sleep(250);
        metrics = getCoordinatorMetrics();
      }

      // Check that there is one failed compaction in the coordinator metrics
      assertTrue(metrics.getStarted() > 0);
      assertEquals(0, metrics.getCompleted());
      assertEquals(1, metrics.getFailed());

      // ensure compaction ids were deleted by split operation from metadata table
      try (TabletsMetadata tm = getCluster().getServerContext().getAmple().readTablets()
          .forTable(tid).fetch(ColumnType.ECOMP).build()) {
        Set<ExternalCompactionId> ecids2 = tm.stream()
            .flatMap(t -> t.getExternalCompactions().keySet().stream()).collect(Collectors.toSet());
        assertTrue(Collections.disjoint(ecids, ecids2));
      }
    } finally {
      stopProcesses(c1, coord);
    }
  }

  @Test
  public void testCoordinatorRestartsDuringCompaction() throws Exception {
    ProcessInfo c1 = null, coord = null;
    String table1 = this.getUniqueNames(1)[0];
    try (AccumuloClient client = Accumulo.newClient()
        .from(SharedMiniClusterBase.getCluster().getClientProperties()).build()) {
      createTable(client, table1, "cs1", 2);
      writeData(client, table1);
      c1 = SharedMiniClusterBase.getCluster().exec(ExternalDoNothingCompactor.class, "-q", "DCQ1");
      coord = SharedMiniClusterBase.getCluster().exec(CompactionCoordinator.class);
      compact(client, table1, 2, "DCQ1", false);
      TableId tid = Tables.getTableId(getCluster().getServerContext(), table1);
      // Wait for the compaction to start by waiting for 1 external compaction column
      Set<ExternalCompactionId> ecids = new HashSet<>();
      do {
        UtilWaitThread.sleep(50);
        try (TabletsMetadata tm = getCluster().getServerContext().getAmple().readTablets()
            .forTable(tid).fetch(ColumnType.ECOMP).build()) {
          tm.stream().flatMap(t -> t.getExternalCompactions().keySet().stream())
              .forEach(ecids::add);
        }
      } while (ecids.isEmpty());

      // Stop the Coordinator
      stopProcesses(coord);

      // Start the TestCompactionCoordinator so that we have
      // access to the metrics.
      coord = SharedMiniClusterBase.getCluster().exec(TestCompactionCoordinator.class);

      // Wait for coordinator to start
      ExternalCompactionMetrics metrics = null;
      while (null == metrics) {
        try {
          metrics = getCoordinatorMetrics();
        } catch (Exception e) {
          UtilWaitThread.sleep(250);
        }
      }

      // wait for failure or test timeout
      metrics = getCoordinatorMetrics();
      while (metrics.getRunning() == 0) {
        UtilWaitThread.sleep(250);
        metrics = getCoordinatorMetrics();
      }
    } finally {
      stopProcesses(c1, coord);
    }
  }

  @Test
  public void testCompactionAndCompactorDies() throws Exception {
    String table1 = this.getUniqueNames(1)[0];
    try (AccumuloClient client = Accumulo.newClient()
        .from(SharedMiniClusterBase.getCluster().getClientProperties()).build()) {
      // Stop the TabletServer so that it does not commit the compaction
      getCluster().getProcesses().get(TABLET_SERVER).forEach(p -> {
        try {
          getCluster().killProcess(TABLET_SERVER, p);
        } catch (Exception e) {
          fail("Failed to shutdown tablet server");
        }
      });
      // Start our TServer that will not commit the compaction
      ProcessInfo tserv = SharedMiniClusterBase.getCluster().exec(ExternalCompactionTServer.class);

      createTable(client, table1, "cs1", 2);
      writeData(client, table1);
      ProcessInfo c1 =
          SharedMiniClusterBase.getCluster().exec(ExternalDoNothingCompactor.class, "-q", "DCQ1");
      ProcessInfo coord = SharedMiniClusterBase.getCluster().exec(CompactionCoordinator.class);
      compact(client, table1, 2, "DCQ1", false);
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
      stopProcesses(c1);

      // DeadCompactionDetector in the CompactionCoordinator should fail the compaction.
      long count = 0;
      while (count == 0) {
        count = this.getFinalStatesForTable(tid)
            .filter(state -> state.getFinalState().equals(FinalState.FAILED)).count();
        UtilWaitThread.sleep(250);
      }

      // Stop the processes we started
      stopProcesses(tserv, coord);
    } finally {
      // We stopped the TServer and started our own, restart the original TabletServers
      getCluster().getClusterControl().start(ServerType.TABLET_SERVER);
    }

  }

  @Test
  public void testMergeDuringExternalCompaction() throws Exception {
    ProcessInfo c1 = null, coord = null;
    String table1 = this.getUniqueNames(1)[0];
    try (AccumuloClient client = Accumulo.newClient()
        .from(SharedMiniClusterBase.getCluster().getClientProperties()).build()) {

      createTable(client, table1, "cs1", 2);
      // set compaction ratio to 1 so that majc occurs naturally, not user compaction
      // user compaction blocks merge
      client.tableOperations().setProperty(table1, Property.TABLE_MAJC_RATIO.toString(), "1.0");
      // cause multiple rfiles to be created
      writeData(client, table1);
      writeData(client, table1);
      writeData(client, table1);
      writeData(client, table1);

      TableId tid = Tables.getTableId(getCluster().getServerContext(), table1);

      c1 = SharedMiniClusterBase.getCluster().exec(ExternalDoNothingCompactor.class, "-q", "DCQ1");
      coord = SharedMiniClusterBase.getCluster().exec(TestCompactionCoordinator.class);

      // Wait for the compaction to start by waiting for 1 external compaction column
      Set<ExternalCompactionId> ecids = new HashSet<>();
      do {
        UtilWaitThread.sleep(50);
        try (TabletsMetadata tm = getCluster().getServerContext().getAmple().readTablets()
            .forTable(tid).fetch(ColumnType.ECOMP).build()) {
          tm.stream().flatMap(t -> t.getExternalCompactions().keySet().stream())
              .forEach(ecids::add);
        }
      } while (ecids.isEmpty());

      var md = new ArrayList<TabletMetadata>();
      try (TabletsMetadata tm = getCluster().getServerContext().getAmple().readTablets()
          .forTable(tid).fetch(ColumnType.PREV_ROW).build()) {
        tm.forEach(t -> md.add(t));
        assertEquals(2, md.size());
      }

      assertEquals(0, getCoordinatorMetrics().getFailed());

      // Merge - blocking operation
      Text start = md.get(0).getPrevEndRow();
      Text end = md.get(1).getEndRow();
      client.tableOperations().merge(table1, start, end);

      // wait for failure or test timeout
      ExternalCompactionMetrics metrics = getCoordinatorMetrics();
      while (metrics.getFailed() == 0) {
        UtilWaitThread.sleep(250);
        metrics = getCoordinatorMetrics();
      }

      // Check that there is one failed compaction in the coordinator metrics
      assertTrue(metrics.getStarted() > 0);
      assertEquals(0, metrics.getCompleted());
      assertTrue(metrics.getFailed() > 0);

      // ensure compaction ids were deleted by merge operation from metadata table
      try (TabletsMetadata tm = getCluster().getServerContext().getAmple().readTablets()
          .forTable(tid).fetch(ColumnType.ECOMP).build()) {
        Set<ExternalCompactionId> ecids2 = tm.stream()
            .flatMap(t -> t.getExternalCompactions().keySet().stream()).collect(Collectors.toSet());
        // keep checking until test times out
        while (!Collections.disjoint(ecids, ecids2)) {
          UtilWaitThread.sleep(25);
          ecids2 = tm.stream().flatMap(t -> t.getExternalCompactions().keySet().stream())
              .collect(Collectors.toSet());
        }
      } finally {
        stopProcesses(c1, coord);
      }
    }
  }

  @Test
  public void testManytablets() throws Exception {
    ProcessInfo c1 = null, c2 = null, c3 = null, c4 = null, coord = null;
    String table1 = this.getUniqueNames(1)[0];
    try (AccumuloClient client = Accumulo.newClient()
        .from(SharedMiniClusterBase.getCluster().getClientProperties()).build()) {

      createTable(client, table1, "cs1", 200);

      writeData(client, table1);

      c1 = SharedMiniClusterBase.getCluster().exec(Compactor.class, "-q", "DCQ1");
      c2 = SharedMiniClusterBase.getCluster().exec(Compactor.class, "-q", "DCQ1");
      c3 = SharedMiniClusterBase.getCluster().exec(Compactor.class, "-q", "DCQ1");
      c4 = SharedMiniClusterBase.getCluster().exec(Compactor.class, "-q", "DCQ1");
      coord = SharedMiniClusterBase.getCluster().exec(CompactionCoordinator.class);

      compact(client, table1, 3, "DCQ1", true);

      verify(client, table1, 3);
    } finally {
      stopProcesses(c1, c2, c3, c4, coord);
    }
  }

  @Test
  public void testExternalCompactionsRunWithTableOffline() throws Exception {
    ProcessInfo c1 = null, coord = null;
    String table1 = this.getUniqueNames(1)[0];
    try (AccumuloClient client = Accumulo.newClient()
        .from(SharedMiniClusterBase.getCluster().getClientProperties()).build()) {
      createTable(client, table1, "cs1");
      // set compaction ratio to 1 so that majc occurs naturally, not user compaction
      // user compaction blocks merge
      client.tableOperations().setProperty(table1, Property.TABLE_MAJC_RATIO.toString(), "1.0");
      // cause multiple rfiles to be created
      writeData(client, table1);
      writeData(client, table1);
      writeData(client, table1);
      writeData(client, table1);

      c1 = SharedMiniClusterBase.getCluster().exec(TestCompactionCoordinatorForOfflineTable.class);

      // Wait for coordinator to start
      ExternalCompactionMetrics metrics = null;
      while (null == metrics) {
        try {
          metrics = getCoordinatorMetrics();
        } catch (Exception e) {
          UtilWaitThread.sleep(250);
        }
      }

      // Offline the table when the compaction starts
      Thread t = new Thread(() -> {
        try {
          ExternalCompactionMetrics metrics2 = getCoordinatorMetrics();
          while (metrics2.getStarted() == 0) {
            metrics2 = getCoordinatorMetrics();
          }
          client.tableOperations().offline(table1, false);
        } catch (Exception e) {
          LOG.error("Error: ", e);
          fail("Failed to offline table");
        }
      });
      t.start();

      TableId tid = Tables.getTableId(getCluster().getServerContext(), table1);
      // Confirm that no final state is in the metadata table
      assertEquals(0, this.getFinalStatesForTable(tid).count());

      // Start the compactor
      coord = SharedMiniClusterBase.getCluster().exec(Compactor.class, "-q", "DCQ1");

      t.join();

      // wait for completed or test timeout
      metrics = getCoordinatorMetrics();
      while (metrics.getCompleted() == 0) {
        UtilWaitThread.sleep(250);
        metrics = getCoordinatorMetrics();
      }

      // Confirm that final state is in the metadata table
      assertEquals(1, this.getFinalStatesForTable(tid).count());

      // Online the table
      client.tableOperations().online(table1);

      // wait for compaction to be committed by tserver or test timeout
      long finalStateCount = this.getFinalStatesForTable(tid).count();
      while (finalStateCount > 0) {
        UtilWaitThread.sleep(250);
        finalStateCount = this.getFinalStatesForTable(tid).count();
      }
    } finally {
      stopProcesses(c1, coord);
    }
  }

  @Test
  public void testUserCompactionCancellation() throws Exception {
    ProcessInfo c1 = null, coord = null;
    String table1 = this.getUniqueNames(1)[0];
    try (AccumuloClient client = Accumulo.newClient()
        .from(SharedMiniClusterBase.getCluster().getClientProperties()).build()) {

      createTable(client, table1, "cs1");
      writeData(client, table1);

      // The ExternalDoNothingCompactor creates a compaction thread that
      // sleeps for 5 minutes.
      // Wait for the coordinator to insert the running compaction metadata
      // entry into the metadata table, then cancel the compaction
      c1 = SharedMiniClusterBase.getCluster().exec(ExternalDoNothingCompactor.class, "-q", "DCQ1");
      coord = SharedMiniClusterBase.getCluster().exec(TestCompactionCoordinator.class);

      compact(client, table1, 2, "DCQ1", false);

      TableId tid = Tables.getTableId(getCluster().getServerContext(), table1);
      List<TabletMetadata> md = new ArrayList<>();
      TabletsMetadata tm = getCluster().getServerContext().getAmple().readTablets().forTable(tid)
          .fetch(ColumnType.ECOMP).build();
      tm.forEach(t -> md.add(t));

      while (md.size() == 0) {
        tm.close();
        tm = getCluster().getServerContext().getAmple().readTablets().forTable(tid)
            .fetch(ColumnType.ECOMP).build();
        tm.forEach(t -> md.add(t));
      }

      assertEquals(0, getCoordinatorMetrics().getFailed());

      client.tableOperations().cancelCompaction(table1);

      // wait for failure or test timeout
      ExternalCompactionMetrics metrics = getCoordinatorMetrics();
      while (metrics.getFailed() == 0) {
        UtilWaitThread.sleep(250);
        metrics = getCoordinatorMetrics();
      }

      assertEquals(1, metrics.getStarted());
      assertEquals(0, metrics.getRunning());
      assertEquals(0, metrics.getCompleted());
      assertEquals(1, metrics.getFailed());
    } finally {
      stopProcesses(c1, coord);
    }
  }

  @Test
  public void testDeleteTableDuringUserExternalCompaction() throws Exception {
    ProcessInfo c1 = null, coord = null;
    try (AccumuloClient client = Accumulo.newClient()
        .from(SharedMiniClusterBase.getCluster().getClientProperties()).build()) {

      String table1 = "ectt6";
      createTable(client, table1, "cs1");
      writeData(client, table1);

      // The ExternalDoNothingCompactor creates a compaction thread that
      // sleeps for 5 minutes.
      // Wait for the coordinator to insert the running compaction metadata
      // entry into the metadata table, then cancel the compaction
      c1 = SharedMiniClusterBase.getCluster().exec(ExternalDoNothingCompactor.class, "-q", "DCQ1");
      coord = SharedMiniClusterBase.getCluster().exec(TestCompactionCoordinator.class);

      compact(client, table1, 2, "DCQ1", false);

      List<TabletMetadata> md = new ArrayList<>();
      TabletsMetadata tm = getCluster().getServerContext().getAmple().readTablets()
          .forLevel(DataLevel.USER).fetch(ColumnType.ECOMP).build();
      tm.forEach(t -> md.add(t));

      while (md.size() == 0) {
        tm.close();
        tm = getCluster().getServerContext().getAmple().readTablets().forLevel(DataLevel.USER)
            .fetch(ColumnType.ECOMP).build();
        tm.forEach(t -> md.add(t));
      }

      assertEquals(0, getCoordinatorMetrics().getFailed());

      client.tableOperations().delete(table1);

      // wait for failure or test timeout
      ExternalCompactionMetrics metrics = getCoordinatorMetrics();
      while (metrics.getFailed() == 0) {
        UtilWaitThread.sleep(250);
        metrics = getCoordinatorMetrics();
      }

      assertEquals(1, metrics.getStarted());
      assertEquals(0, metrics.getRunning());
      assertEquals(0, metrics.getCompleted());
      assertEquals(1, metrics.getFailed());
    } finally {
      stopProcesses(c1, coord);
    }
  }

  @Test
  public void testDeleteTableDuringExternalCompaction() throws Exception {
    ProcessInfo c1 = null, coord = null;
    String table1 = this.getUniqueNames(1)[0];
    try (AccumuloClient client = Accumulo.newClient()
        .from(SharedMiniClusterBase.getCluster().getClientProperties()).build()) {

      createTable(client, table1, "cs1");
      // set compaction ratio to 1 so that majc occurs naturally, not user compaction
      // user compaction blocks delete
      client.tableOperations().setProperty(table1, Property.TABLE_MAJC_RATIO.toString(), "1.0");
      // cause multiple rfiles to be created
      writeData(client, table1);
      writeData(client, table1);
      writeData(client, table1);
      writeData(client, table1);

      // The ExternalDoNothingCompactor creates a compaction thread that
      // sleeps for 5 minutes. The compaction should occur naturally.
      // Wait for the coordinator to insert the running compaction metadata
      // entry into the metadata table, then delete the table.
      c1 = SharedMiniClusterBase.getCluster().exec(ExternalDoNothingCompactor.class, "-q", "DCQ1");
      coord = SharedMiniClusterBase.getCluster().exec(TestCompactionCoordinator.class);

      TableId tid = Tables.getTableId(getCluster().getServerContext(), table1);
      LOG.warn("Tid for Table {} is {}", table1, tid);
      List<TabletMetadata> md = new ArrayList<>();
      TabletsMetadata tm = getCluster().getServerContext().getAmple().readTablets().forTable(tid)
          .fetch(ColumnType.ECOMP).build();
      tm.forEach(t -> md.add(t));

      while (md.size() == 0) {
        tm.close();
        tm = getCluster().getServerContext().getAmple().readTablets().forTable(tid)
            .fetch(ColumnType.ECOMP).build();
        tm.forEach(t -> md.add(t));
        UtilWaitThread.sleep(250);
      }

      assertEquals(0, getCoordinatorMetrics().getFailed());

      client.tableOperations().delete(table1);

      // wait for failure or test timeout
      ExternalCompactionMetrics metrics = getCoordinatorMetrics();
      while (metrics.getFailed() == 0) {
        UtilWaitThread.sleep(50);
        metrics = getCoordinatorMetrics();
      }

      tm = getCluster().getServerContext().getAmple().readTablets().forTable(tid)
          .fetch(ColumnType.ECOMP).build();
      assertEquals(0, tm.stream().count());
      tm.close();

      // The metadata tablets will be deleted from the metadata table because we have deleted the
      // table. Verify that the compaction failed by looking at the metrics in the Coordinator.
      assertEquals(1, metrics.getStarted());
      assertEquals(0, metrics.getRunning());
      assertEquals(0, metrics.getCompleted());
      assertEquals(1, metrics.getFailed());
    } finally {
      stopProcesses(c1, coord);
    }
  }

  @Test
  public void testConfigurer() throws Exception {
    String tableName = this.getUniqueNames(1)[0];

    ProcessInfo c1 = SharedMiniClusterBase.getCluster().exec(Compactor.class, "-q", "DCQ1");
    ProcessInfo coord = SharedMiniClusterBase.getCluster().exec(CompactionCoordinator.class);

    try (AccumuloClient client = Accumulo.newClient()
        .from(SharedMiniClusterBase.getCluster().getClientProperties()).build()) {

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
      stopProcesses(c1, coord);
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
    try (AccumuloClient client = Accumulo.newClient()
        .from(SharedMiniClusterBase.getCluster().getClientProperties()).build()) {
      createTable(client, table1, "cs1");
      writeData(client, table1);
      c1 = SharedMiniClusterBase.getCluster().exec(Compactor.class, "-q", "DCQ1");
      coord = SharedMiniClusterBase.getCluster().exec(CompactionCoordinator.class);
      compact(client, table1, 2, "DCQ1", true);
      verify(client, table1, 2);

      IteratorSetting setting = new IteratorSetting(50, "delete", ExtDevNull.class);
      client.tableOperations().attachIterator(table1, setting, EnumSet.of(IteratorScope.majc));
      client.tableOperations().compact(table1, new CompactionConfig().setWait(true));

      try (Scanner s = client.createScanner(table1)) {
        assertFalse(s.iterator().hasNext());
      }
    } finally {
      stopProcesses(c1, coord);
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
    ProcessInfo tserv = SharedMiniClusterBase.getCluster().exec(ExternalCompactionTServer.class);

    final String table3 = this.getUniqueNames(1)[0];
    ProcessInfo c1 = null, coord = null;
    try (final AccumuloClient client = Accumulo.newClient()
        .from(SharedMiniClusterBase.getCluster().getClientProperties()).build()) {
      createTable(client, table3, "cs1");
      writeData(client, table3);
      c1 = SharedMiniClusterBase.getCluster().exec(Compactor.class, "-q", "DCQ1");
      coord = SharedMiniClusterBase.getCluster().exec(CompactionCoordinator.class);
      compact(client, table3, 2, "DCQ1", false);

      // ExternalCompactionTServer will not commit the compaction. Wait for the
      // metadata table entries to show up.
      LOG.info("Waiting for external compaction to complete.");
      TableId tid = Tables.getTableId(getCluster().getServerContext(), table3);
      Stream<ExternalCompactionFinalState> fs = this.getFinalStatesForTable(tid);
      while (fs.count() == 0) {
        LOG.info("Waiting for compaction completed marker to appear");
        UtilWaitThread.sleep(250);
        fs = this.getFinalStatesForTable(tid);
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
      this.getFinalStatesForTable(tid).forEach(f -> finished.add(f));
      assertEquals(1, finished.size());
      assertEquals(em.entrySet().iterator().next().getKey(),
          finished.get(0).getExternalCompactionId());
      tm.close();

      // Force a flush on the metadata table before killing our tserver
      client.tableOperations().flush("accumulo.metadata");

      // Stop our TabletServer. Need to perform a normal shutdown so that the WAL is closed
      // normally.
      LOG.info("Stopping our tablet server");
      stopProcesses(tserv);

      // Start a TabletServer to commit the compaction.
      LOG.info("Starting normal tablet server");
      getCluster().getClusterControl().start(ServerType.TABLET_SERVER);

      // Wait for the compaction to be committed.
      LOG.info("Waiting for compaction completed marker to disappear");
      Stream<ExternalCompactionFinalState> fs2 = this.getFinalStatesForTable(tid);
      while (fs2.count() != 0) {
        LOG.info("Waiting for compaction completed marker to disappear");
        UtilWaitThread.sleep(500);
        fs2 = this.getFinalStatesForTable(tid);
      }
      verify(client, table3, 2);
    } finally {
      stopProcesses(c1, coord);
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
    try (final AccumuloClient client = Accumulo.newClient()
        .from(SharedMiniClusterBase.getCluster().getClientProperties()).build()) {

      c1 = SharedMiniClusterBase.getCluster().exec(Compactor.class, "-q", "DCQ1");
      coord = SharedMiniClusterBase.getCluster().exec(CompactionCoordinator.class);

      createTable(client, tableName, "cs1");

      writeData(client, tableName);
      // This should create an A file
      compact(client, tableName, 17, "DCQ1", true);
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
          int modulus = v < MAX_DATA ? 17 : 19;

          assertTrue(String.format("%s %s %d != 0", entry.getValue(), "%", modulus),
              Integer.parseInt(entry.getValue().toString()) % modulus == 0);
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

    } finally {
      stopProcesses(c1, coord);
    }
  }

  private static Optional<String> extract(String input, String regex) {
    Pattern pattern = Pattern.compile(regex);
    Matcher matcher = pattern.matcher(input);
    if (matcher.matches()) {
      return Optional.of(matcher.group(1));
    }

    return Optional.empty();
  }

  @Test
  public void testMetrics() throws Exception {
    Collection<ProcessReference> tservers =
        getCluster().getProcesses().get(ServerType.TABLET_SERVER);
    assertEquals(2, tservers.size());
    // kill one tserver so that queue metrics are not spread across tservers
    getCluster().killProcess(TABLET_SERVER, tservers.iterator().next());
    ProcessInfo c1 = null, c2 = null, coord = null;
    String[] names = getUniqueNames(2);
    try (final AccumuloClient client = Accumulo.newClient()
        .from(SharedMiniClusterBase.getCluster().getClientProperties()).build()) {
      String table1 = names[0];
      createTable(client, table1, "cs1", 5);

      String table2 = names[1];
      createTable(client, table2, "cs2", 10);

      writeData(client, table1);
      writeData(client, table2);

      LinkedBlockingQueue<String> queueMetrics = new LinkedBlockingQueue<>();

      Tailer tailer =
          Tailer.create(new File("./target/tserver.metrics"), new TailerListenerAdapter() {
            @Override
            public void handle(final String line) {
              extract(line, ".*(DCQ1_queued=[0-9]+).*").ifPresent(queueMetrics::add);
              extract(line, ".*(DCQ2_queued=[0-9]+).*").ifPresent(queueMetrics::add);
            }
          });

      compact(client, table1, 7, "DCQ1", false);
      compact(client, table2, 13, "DCQ2", false);

      boolean sawDCQ1_5 = false;
      boolean sawDCQ2_10 = false;

      // wait until expected number of queued are seen in metrics
      while (!sawDCQ1_5 || !sawDCQ2_10) {
        String qm = queueMetrics.take();
        sawDCQ1_5 |= qm.equals("DCQ1_queued=5");
        sawDCQ2_10 |= qm.equals("DCQ2_queued=10");
      }

      // start compactors
      c1 = SharedMiniClusterBase.getCluster().exec(Compactor.class, "-q", "DCQ1");
      c2 = SharedMiniClusterBase.getCluster().exec(Compactor.class, "-q", "DCQ2");
      coord = SharedMiniClusterBase.getCluster().exec(CompactionCoordinator.class);

      boolean sawDCQ1_0 = false;
      boolean sawDCQ2_0 = false;

      // wait until queued goes to zero in metrics
      while (!sawDCQ1_0 || !sawDCQ2_0) {
        String qm = queueMetrics.take();
        sawDCQ1_0 |= qm.equals("DCQ1_queued=0");
        sawDCQ2_0 |= qm.equals("DCQ2_queued=0");
      }

      tailer.stop();

      // Wait for all external compactions to complete
      long count;
      do {
        UtilWaitThread.sleep(100);
        try (TabletsMetadata tm = getCluster().getServerContext().getAmple().readTablets()
            .forLevel(DataLevel.USER).fetch(ColumnType.ECOMP).build()) {
          count = tm.stream().flatMap(t -> t.getExternalCompactions().keySet().stream()).count();
        }
      } while (count > 0);

      verify(client, table1, 7);
      verify(client, table2, 13);

    } finally {
      stopProcesses(c1, c2, coord);
      // We stopped the TServer and started our own, restart the original TabletServers
      getCluster().getClusterControl().start(ServerType.TABLET_SERVER);
    }
  }

  private ExternalCompactionMetrics getCoordinatorMetrics() throws Exception {
    HttpResponse<String> res = hc.send(req, BodyHandlers.ofString());
    assertEquals(200, res.statusCode());
    String metrics = res.body();
    assertNotNull(metrics);
    return new Gson().fromJson(metrics, ExternalCompactionMetrics.class);
  }

  private void verify(AccumuloClient client, String table1, int modulus)
      throws TableNotFoundException, AccumuloSecurityException, AccumuloException {
    try (Scanner scanner = client.createScanner(table1)) {
      int count = 0;
      for (Entry<Key,Value> entry : scanner) {
        assertTrue(String.format("%s %s %d != 0", entry.getValue(), "%", modulus),
            Integer.parseInt(entry.getValue().toString()) % modulus == 0);
        count++;
      }

      int expectedCount = 0;
      for (int i = 0; i < MAX_DATA; i++) {
        if (i % modulus == 0)
          expectedCount++;
      }

      assertEquals(expectedCount, count);
    }
  }

  private void compact(final AccumuloClient client, String table1, int modulus,
      String expectedQueue, boolean wait)
      throws AccumuloSecurityException, TableNotFoundException, AccumuloException {
    IteratorSetting iterSetting = new IteratorSetting(100, TestFilter.class);
    // make sure iterator options make it to compactor process
    iterSetting.addOption("expectedQ", expectedQueue);
    iterSetting.addOption("modulus", modulus + "");
    CompactionConfig config =
        new CompactionConfig().setIterators(List.of(iterSetting)).setWait(wait);
    client.tableOperations().compact(table1, config);
  }

  private void createTable(AccumuloClient client, String tableName, String service)
      throws Exception {
    Map<String,String> props =
        Map.of("table.compaction.dispatcher", SimpleCompactionDispatcher.class.getName(),
            "table.compaction.dispatcher.opts.service", service);
    NewTableConfiguration ntc = new NewTableConfiguration().setProperties(props);

    client.tableOperations().create(tableName, ntc);

  }

  private void createTable(AccumuloClient client, String tableName, String service, int numTablets)
      throws Exception {
    SortedSet<Text> splits = new TreeSet<>();
    int jump = MAX_DATA / numTablets;

    for (int r = jump; r < MAX_DATA; r += jump) {
      splits.add(new Text(row(r)));
    }

    createTable(client, tableName, service, splits);
  }

  private void createTable(AccumuloClient client, String tableName, String service,
      SortedSet<Text> splits) throws Exception {
    Map<String,String> props =
        Map.of("table.compaction.dispatcher", SimpleCompactionDispatcher.class.getName(),
            "table.compaction.dispatcher.opts.service", service);
    NewTableConfiguration ntc = new NewTableConfiguration().setProperties(props).withSplits(splits);

    client.tableOperations().create(tableName, ntc);

  }

  private void writeData(AccumuloClient client, String table1) throws MutationsRejectedException,
      TableNotFoundException, AccumuloException, AccumuloSecurityException {
    try (BatchWriter bw = client.createBatchWriter(table1)) {
      for (int i = 0; i < MAX_DATA; i++) {
        Mutation m = new Mutation(row(i));
        m.put("", "", "" + i);
        bw.addMutation(m);
      }
    }

    client.tableOperations().flush(table1);
  }
}

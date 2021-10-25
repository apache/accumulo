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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.stream.Stream;

import org.apache.accumulo.cluster.AccumuloCluster;
import org.apache.accumulo.coordinator.CompactionCoordinator;
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
import org.apache.accumulo.core.clientImpl.ClientContext;
import org.apache.accumulo.core.compaction.thrift.CompactionCoordinatorService;
import org.apache.accumulo.core.compaction.thrift.TCompactionState;
import org.apache.accumulo.core.compaction.thrift.TExternalCompaction;
import org.apache.accumulo.core.compaction.thrift.TExternalCompactionList;
import org.apache.accumulo.core.conf.ClientProperty;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.metadata.schema.ExternalCompactionFinalState;
import org.apache.accumulo.core.metadata.schema.ExternalCompactionId;
import org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType;
import org.apache.accumulo.core.metadata.schema.TabletsMetadata;
import org.apache.accumulo.core.rpc.ThriftUtil;
import org.apache.accumulo.core.spi.compaction.DefaultCompactionPlanner;
import org.apache.accumulo.core.spi.compaction.SimpleCompactionDispatcher;
import org.apache.accumulo.core.trace.TraceUtil;
import org.apache.accumulo.core.util.HostAndPort;
import org.apache.accumulo.core.util.compaction.ExternalCompactionUtil;
import org.apache.accumulo.fate.util.UtilWaitThread;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloClusterImpl;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloClusterImpl.ProcessInfo;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloConfigImpl;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.test.compaction.ExternalCompaction_1_IT.TestFilter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.io.Text;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.beust.jcommander.internal.Maps;

public class ExternalCompactionTestUtils {

  private static final Logger LOG = LoggerFactory.getLogger(ExternalCompactionTestUtils.class);

  public static final int MAX_DATA = 1000;

  public static String row(int r) {
    return String.format("r:%04d", r);
  }

  public static Stream<ExternalCompactionFinalState> getFinalStatesForTable(AccumuloCluster cluster,
      TableId tid) {
    return cluster.getServerContext().getAmple().getExternalCompactionFinalStates()
        .filter(state -> state.getExtent().tableId().equals(tid));
  }

  public static void compact(final AccumuloClient client, String table1, int modulus,
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

  public static void createTable(AccumuloClient client, String tableName, String service)
      throws Exception {
    Map<String,String> props =
        Map.of("table.compaction.dispatcher", SimpleCompactionDispatcher.class.getName(),
            "table.compaction.dispatcher.opts.service", service);
    NewTableConfiguration ntc = new NewTableConfiguration().setProperties(props);

    client.tableOperations().create(tableName, ntc);

  }

  public static void createTable(AccumuloClient client, String tableName, String service,
      int numTablets) throws Exception {
    SortedSet<Text> splits = new TreeSet<>();
    int jump = MAX_DATA / numTablets;

    for (int r = jump; r < MAX_DATA; r += jump) {
      splits.add(new Text(row(r)));
    }

    createTable(client, tableName, service, splits);
  }

  public static void createTable(AccumuloClient client, String tableName, String service,
      SortedSet<Text> splits) throws Exception {
    Map<String,String> props =
        Map.of("table.compaction.dispatcher", SimpleCompactionDispatcher.class.getName(),
            "table.compaction.dispatcher.opts.service", service);
    NewTableConfiguration ntc = new NewTableConfiguration().setProperties(props).withSplits(splits);

    client.tableOperations().create(tableName, ntc);

  }

  public static void writeData(AccumuloClient client, String table1)
      throws MutationsRejectedException, TableNotFoundException, AccumuloException,
      AccumuloSecurityException {
    try (BatchWriter bw = client.createBatchWriter(table1)) {
      for (int i = 0; i < MAX_DATA; i++) {
        Mutation m = new Mutation(row(i));
        m.put("", "", "" + i);
        bw.addMutation(m);
      }
    }

    client.tableOperations().flush(table1);
  }

  public static void verify(AccumuloClient client, String table1, int modulus)
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

  public static void stopProcesses(ProcessInfo... processes) throws Exception {
    for (ProcessInfo p : processes) {
      if (p != null) {
        Process proc = p.getProcess();
        if (proc.supportsNormalTermination()) {
          LOG.info("Stopping process {}", proc.pid());
          proc.destroyForcibly().waitFor();
        } else {
          LOG.info("Stopping process {} manually", proc.pid());
          new ProcessBuilder("kill", Long.toString(proc.pid())).start();
          proc.waitFor();
        }
      }
    }
  }

  public static void configureMiniCluster(MiniAccumuloConfigImpl cfg, Configuration coreSite) {

    // ecomp writes from the TabletServer are not being written to the metadata
    // table, they are being queued up instead.
    Map<String,String> clProps = Maps.newHashMap();
    clProps.put(ClientProperty.BATCH_WRITER_LATENCY_MAX.getKey(), "2s");
    cfg.setClientProps(clProps);

    cfg.setProperty("tserver.compaction.major.service.cs1.planner",
        DefaultCompactionPlanner.class.getName());
    cfg.setProperty("tserver.compaction.major.service.cs1.planner.opts.executors",
        "[{'name':'all', 'type': 'external', 'queue': 'DCQ1'}]");
    cfg.setProperty("tserver.compaction.major.service.cs2.planner",
        DefaultCompactionPlanner.class.getName());
    cfg.setProperty("tserver.compaction.major.service.cs2.planner.opts.executors",
        "[{'name':'all', 'type': 'external','queue': 'DCQ2'}]");
    cfg.setProperty(Property.COMPACTION_COORDINATOR_FINALIZER_COMPLETION_CHECK_INTERVAL, "5s");
    cfg.setProperty(Property.COMPACTION_COORDINATOR_DEAD_COMPACTOR_CHECK_INTERVAL, "5s");
    cfg.setProperty(Property.COMPACTION_COORDINATOR_TSERVER_COMPACTION_CHECK_INTERVAL, "3s");
    cfg.setProperty(Property.COMPACTOR_PORTSEARCH, "true");
    cfg.setProperty(Property.GENERAL_SIMPLETIMER_THREADPOOL_SIZE, "10");
    cfg.setProperty(Property.MANAGER_FATE_THREADPOOL_SIZE, "10");
    // use raw local file system so walogs sync and flush will work
    coreSite.set("fs.file.impl", RawLocalFileSystem.class.getName());
  }

  public static ProcessInfo startCoordinator(MiniAccumuloClusterImpl cluster,
      Class<? extends CompactionCoordinator> coord, ClientContext context) throws IOException {
    ProcessInfo pi = cluster.exec(coord);
    UtilWaitThread.sleep(1000);
    // Wait for coordinator to start
    TExternalCompactionList metrics = null;
    while (null == metrics) {
      try {
        metrics = getRunningCompactions(context);
      } catch (Exception e) {
        UtilWaitThread.sleep(250);
      }
    }
    return pi;
  }

  public static TExternalCompactionList getRunningCompactions(ClientContext context)
      throws Exception {
    HostAndPort coordinatorHost = ExternalCompactionUtil.findCompactionCoordinator(context);
    if (null == coordinatorHost) {
      throw new TTransportException("Unable to get CompactionCoordinator address from ZooKeeper");
    }
    CompactionCoordinatorService.Client client = ThriftUtil
        .getClient(new CompactionCoordinatorService.Client.Factory(), coordinatorHost, context);
    try {
      TExternalCompactionList running =
          client.getRunningCompactions(TraceUtil.traceInfo(), context.rpcCreds());
      return running;
    } finally {
      ThriftUtil.returnClient(client, context);
    }
  }

  private static TExternalCompactionList getCompletedCompactions(ClientContext context)
      throws Exception {
    HostAndPort coordinatorHost = ExternalCompactionUtil.findCompactionCoordinator(context);
    if (null == coordinatorHost) {
      throw new TTransportException("Unable to get CompactionCoordinator address from ZooKeeper");
    }
    CompactionCoordinatorService.Client client = ThriftUtil
        .getClient(new CompactionCoordinatorService.Client.Factory(), coordinatorHost, context);
    try {
      TExternalCompactionList completed =
          client.getCompletedCompactions(TraceUtil.traceInfo(), context.rpcCreds());
      return completed;
    } finally {
      ThriftUtil.returnClient(client, context);
    }
  }

  public static TCompactionState getLastState(TExternalCompaction status) {
    ArrayList<Long> timestamps = new ArrayList<>(status.getUpdates().size());
    status.getUpdates().keySet().forEach(k -> timestamps.add(k));
    Collections.sort(timestamps);
    return status.getUpdates().get(timestamps.get(timestamps.size() - 1)).getState();
  }

  public static Set<ExternalCompactionId> waitForCompactionStartAndReturnEcids(ServerContext ctx,
      TableId tid) {
    Set<ExternalCompactionId> ecids = new HashSet<>();
    do {
      UtilWaitThread.sleep(50);
      try (TabletsMetadata tm =
          ctx.getAmple().readTablets().forTable(tid).fetch(ColumnType.ECOMP).build()) {
        tm.stream().flatMap(t -> t.getExternalCompactions().keySet().stream()).forEach(ecids::add);
      }
    } while (ecids.isEmpty());
    return ecids;
  }

  public static int confirmCompactionRunning(ServerContext ctx, Set<ExternalCompactionId> ecids)
      throws Exception {
    int matches = 0;
    while (matches == 0) {
      TExternalCompactionList running = ExternalCompactionTestUtils.getRunningCompactions(ctx);
      if (running.getCompactions() != null) {
        for (ExternalCompactionId ecid : ecids) {
          TExternalCompaction tec = running.getCompactions().get(ecid.canonical());
          if (tec != null && tec.getUpdates() != null && !tec.getUpdates().isEmpty()) {
            matches++;
            assertEquals(TCompactionState.STARTED, ExternalCompactionTestUtils.getLastState(tec));
          }
        }
      }
      UtilWaitThread.sleep(250);
    }
    return matches;
  }

  public static void confirmCompactionCompleted(ServerContext ctx, Set<ExternalCompactionId> ecids,
      TCompactionState expectedState) throws Exception {
    // The running compaction should be removed
    TExternalCompactionList running = ExternalCompactionTestUtils.getRunningCompactions(ctx);
    while (running.getCompactions() != null) {
      UtilWaitThread.sleep(250);
      running = ExternalCompactionTestUtils.getRunningCompactions(ctx);
    }
    // The compaction should be in the completed list with the expected state
    TExternalCompactionList completed = ExternalCompactionTestUtils.getCompletedCompactions(ctx);
    while (completed.getCompactions() == null) {
      UtilWaitThread.sleep(250);
      completed = ExternalCompactionTestUtils.getCompletedCompactions(ctx);
    }
    for (ExternalCompactionId e : ecids) {
      TExternalCompaction tec = completed.getCompactions().get(e.canonical());
      assertNotNull(tec);
      assertEquals(expectedState, ExternalCompactionTestUtils.getLastState(tec));
    }

  }
}

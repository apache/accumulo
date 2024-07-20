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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.CompactionConfig;
import org.apache.accumulo.core.client.admin.NewTableConfiguration;
import org.apache.accumulo.core.clientImpl.ClientContext;
import org.apache.accumulo.core.conf.ClientProperty;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.metadata.schema.ExternalCompactionId;
import org.apache.accumulo.core.metadata.schema.TabletMetadata;
import org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType;
import org.apache.accumulo.core.metadata.schema.TabletsMetadata;
import org.apache.accumulo.core.rpc.grpc.GrpcUtil;
import org.apache.accumulo.core.spi.compaction.RatioBasedCompactionPlanner;
import org.apache.accumulo.core.spi.compaction.SimpleCompactionDispatcher;
import org.apache.accumulo.core.trace.TraceUtil;
import org.apache.accumulo.core.util.UtilWaitThread;
import org.apache.accumulo.core.util.compaction.ExternalCompactionUtil;
import org.apache.accumulo.grpc.compaction.protobuf.CompactionCoordinatorServiceGrpc;
import org.apache.accumulo.grpc.compaction.protobuf.CompactionCoordinatorServiceGrpc.CompactionCoordinatorServiceBlockingStub;
import org.apache.accumulo.grpc.compaction.protobuf.GetCompletedCompactionsRequest;
import org.apache.accumulo.grpc.compaction.protobuf.GetRunningCompactionsRequest;
import org.apache.accumulo.grpc.compaction.protobuf.PCompactionState;
import org.apache.accumulo.grpc.compaction.protobuf.PExternalCompaction;
import org.apache.accumulo.grpc.compaction.protobuf.PExternalCompactionList;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloConfigImpl;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.test.compaction.ExternalCompaction_1_IT.TestFilter;
import org.apache.accumulo.test.util.Wait;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.io.Text;
import org.apache.thrift.transport.TTransportException;

import com.beust.jcommander.internal.Maps;
import com.google.common.net.HostAndPort;

public class ExternalCompactionTestUtils {

  public static final int MAX_DATA = 1000;
  public static final String GROUP1 = "DCQ1";
  public static final String GROUP2 = "DCQ2";
  public static final String GROUP3 = "DCQ3";
  public static final String GROUP4 = "DCQ4";
  public static final String GROUP5 = "DCQ5";
  public static final String GROUP6 = "DCQ6";
  public static final String GROUP7 = "DCQ7";
  public static final String GROUP8 = "DCQ8";

  public static String row(int r) {
    return String.format("r:%04d", r);
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

  public static void writeData(AccumuloClient client, String table1, int rows)
      throws TableNotFoundException, AccumuloException, AccumuloSecurityException {
    try (BatchWriter bw = client.createBatchWriter(table1)) {
      for (int i = 0; i < rows; i++) {
        Mutation m = new Mutation(row(i));
        m.put("", "", "" + i);
        bw.addMutation(m);
      }
    }

    client.tableOperations().flush(table1);
  }

  public static void writeData(AccumuloClient client, String table1)
      throws TableNotFoundException, AccumuloException, AccumuloSecurityException {
    writeData(client, table1, MAX_DATA);
  }

  public static void verify(AccumuloClient client, String table1, int modulus)
      throws TableNotFoundException, AccumuloSecurityException, AccumuloException {
    verify(client, table1, modulus, MAX_DATA);
  }

  public static void verify(AccumuloClient client, String table1, int modulus, int rows)
      throws TableNotFoundException, AccumuloSecurityException, AccumuloException {
    try (Scanner scanner = client.createScanner(table1)) {
      int count = 0;
      for (Entry<Key,Value> entry : scanner) {
        assertEquals(0, Integer.parseInt(entry.getValue().toString()) % modulus,
            String.format("%s %s %d != 0", entry.getValue(), "%", modulus));
        count++;
      }

      int expectedCount = 0;
      for (int i = 0; i < rows; i++) {
        if (i % modulus == 0) {
          expectedCount++;
        }
      }

      assertEquals(expectedCount, count);
    }
  }

  public static void configureMiniCluster(MiniAccumuloConfigImpl cfg, Configuration coreSite) {

    // ecomp writes from the TabletServer are not being written to the metadata
    // table, they are being queued up instead.
    Map<String,String> clProps = Maps.newHashMap();
    clProps.put(ClientProperty.BATCH_WRITER_LATENCY_MAX.getKey(), "2s");
    cfg.setClientProps(clProps);

    // configure the compaction services to use the queues
    cfg.setProperty(Property.COMPACTION_SERVICE_PREFIX.getKey() + "cs1.planner",
        RatioBasedCompactionPlanner.class.getName());
    cfg.setProperty(Property.COMPACTION_SERVICE_PREFIX.getKey() + "cs1.planner.opts.groups",
        "[{'group':'" + GROUP1 + "'}]");
    cfg.setProperty(Property.COMPACTION_SERVICE_PREFIX.getKey() + "cs2.planner",
        RatioBasedCompactionPlanner.class.getName());
    cfg.setProperty(Property.COMPACTION_SERVICE_PREFIX.getKey() + "cs2.planner.opts.groups",
        "[{'group':'" + GROUP2 + "'}]");
    cfg.setProperty(Property.COMPACTION_SERVICE_PREFIX.getKey() + "cs3.planner",
        RatioBasedCompactionPlanner.class.getName());
    cfg.setProperty(Property.COMPACTION_SERVICE_PREFIX.getKey() + "cs3.planner.opts.groups",
        "[{'group':'" + GROUP3 + "'}]");
    cfg.setProperty(Property.COMPACTION_SERVICE_PREFIX.getKey() + "cs4.planner",
        RatioBasedCompactionPlanner.class.getName());
    cfg.setProperty(Property.COMPACTION_SERVICE_PREFIX.getKey() + "cs4.planner.opts.groups",
        "[{'group':'" + GROUP4 + "'}]");
    cfg.setProperty(Property.COMPACTION_SERVICE_PREFIX.getKey() + "cs5.planner",
        RatioBasedCompactionPlanner.class.getName());
    cfg.setProperty(Property.COMPACTION_SERVICE_PREFIX.getKey() + "cs5.planner.opts.groups",
        "[{'group':'" + GROUP5 + "'}]");
    cfg.setProperty(Property.COMPACTION_SERVICE_PREFIX.getKey() + "cs6.planner",
        RatioBasedCompactionPlanner.class.getName());
    cfg.setProperty(Property.COMPACTION_SERVICE_PREFIX.getKey() + "cs6.planner.opts.groups",
        "[{'group':'" + GROUP6 + "'}]");
    cfg.setProperty(Property.COMPACTION_SERVICE_PREFIX.getKey() + "cs7.planner",
        RatioBasedCompactionPlanner.class.getName());
    cfg.setProperty(Property.COMPACTION_SERVICE_PREFIX.getKey() + "cs7.planner.opts.groups",
        "[{'group':'" + GROUP7 + "'}]");
    cfg.setProperty(Property.COMPACTION_SERVICE_PREFIX.getKey() + "cs8.planner",
        RatioBasedCompactionPlanner.class.getName());
    cfg.setProperty(Property.COMPACTION_SERVICE_PREFIX.getKey() + "cs8.planner.opts.groups",
        "[{'group':'" + GROUP8 + "'}]");
    cfg.setProperty(Property.COMPACTION_COORDINATOR_FINALIZER_COMPLETION_CHECK_INTERVAL, "5s");
    cfg.setProperty(Property.COMPACTION_COORDINATOR_DEAD_COMPACTOR_CHECK_INTERVAL, "5s");
    cfg.setProperty(Property.COMPACTION_COORDINATOR_TSERVER_COMPACTION_CHECK_INTERVAL, "3s");
    cfg.setProperty(Property.COMPACTOR_CANCEL_CHECK_INTERVAL, "5s");
    cfg.setProperty(Property.COMPACTOR_PORTSEARCH, "true");
    cfg.setProperty(Property.COMPACTOR_MIN_JOB_WAIT_TIME, "100ms");
    cfg.setProperty(Property.COMPACTOR_MAX_JOB_WAIT_TIME, "1s");
    cfg.setProperty(Property.GENERAL_THREADPOOL_SIZE, "10");
    cfg.setProperty(Property.MANAGER_FATE_THREADPOOL_SIZE, "10");
    cfg.setProperty(Property.MANAGER_TABLET_GROUP_WATCHER_INTERVAL, "1s");
    // use raw local file system so walogs sync and flush will work
    coreSite.set("fs.file.impl", RawLocalFileSystem.class.getName());
  }

  public static PExternalCompactionList getRunningCompactions(ClientContext context,
      Optional<HostAndPort> coordinatorHost) {
    // TODO: coordinatorHost contains the Thrift port so right now only host is used.
    // we eventually need the gRPC port and will need to store than in Zk.
    // GrpcUtil for now just uses the property in the context for the port
    CompactionCoordinatorServiceBlockingStub client = CompactionCoordinatorServiceGrpc
        .newBlockingStub(GrpcUtil.getChannel(coordinatorHost.orElseThrow(), context));
    try {
      PExternalCompactionList running =
          client.getRunningCompactions(GetRunningCompactionsRequest.newBuilder()
              .setPtinfo(TraceUtil.protoTraceInfo()).setCredentials(context.gRpcCreds()).build());
      return running;
    } finally {
      // TODO return gRpc client if needed
      // ThriftUtil.returnClient(client, context);
    }
  }

  private static PExternalCompactionList getCompletedCompactions(ClientContext context,
      Optional<HostAndPort> coordinatorHost) {
    // TODO: coordinatorHost contains the Thrift port so right now only host is used.
    // we eventually need the gRPC port and will need to store than in Zk.
    // GrpcUtil for now just uses the property in the context for the port
    CompactionCoordinatorServiceBlockingStub client = CompactionCoordinatorServiceGrpc
        .newBlockingStub(GrpcUtil.getChannel(coordinatorHost.orElseThrow(), context));
    try {
      PExternalCompactionList completed =
          client.getCompletedCompactions(GetCompletedCompactionsRequest.newBuilder()
              .setPtinfo(TraceUtil.protoTraceInfo()).setCredentials(context.gRpcCreds()).build());
      return completed;
    } finally {
      // TODO return gRpc client if needed
      // ThriftUtil.returnClient(client, context);
    }
  }

  public static PCompactionState getLastState(PExternalCompaction status) {
    ArrayList<Long> timestamps = new ArrayList<>(status.getUpdatesMap().size());
    status.getUpdatesMap().keySet().forEach(k -> timestamps.add(k));
    Collections.sort(timestamps);
    return status.getUpdatesMap().get(timestamps.get(timestamps.size() - 1)).getState();
  }

  public static Set<ExternalCompactionId> waitForCompactionStartAndReturnEcids(ServerContext ctx,
      TableId tid) {
    Set<ExternalCompactionId> ecids = new HashSet<>();
    do {
      try (TabletsMetadata tm =
          ctx.getAmple().readTablets().forTable(tid).fetch(ColumnType.ECOMP).build()) {
        tm.stream().flatMap(t -> t.getExternalCompactions().keySet().stream()).forEach(ecids::add);
      }
      if (ecids.isEmpty()) {
        UtilWaitThread.sleep(50);
      }
    } while (ecids.isEmpty());
    return ecids;
  }

  public static long countTablets(ServerContext ctx, String tableName,
      Predicate<TabletMetadata> tabletTest) {
    var tableId = TableId.of(ctx.tableOperations().tableIdMap().get(tableName));
    try (var tabletsMetadata = ctx.getAmple().readTablets().forTable(tableId).build()) {
      return tabletsMetadata.stream().filter(tabletTest).count();
    }
  }

  public static void waitForRunningCompactions(ServerContext ctx, TableId tid,
      Set<ExternalCompactionId> idsToWaitFor) throws Exception {

    Wait.waitFor(() -> {
      Set<ExternalCompactionId> seen;
      try (TabletsMetadata tm =
          ctx.getAmple().readTablets().forTable(tid).fetch(ColumnType.ECOMP).build()) {
        seen = tm.stream().flatMap(t -> t.getExternalCompactions().keySet().stream())
            .collect(Collectors.toSet());
      }

      return Collections.disjoint(seen, idsToWaitFor);
    });
  }

  public static int confirmCompactionRunning(ServerContext ctx, Set<ExternalCompactionId> ecids)
      throws Exception {
    int matches = 0;
    Optional<HostAndPort> coordinatorHost = ExternalCompactionUtil.findCompactionCoordinator(ctx);
    if (coordinatorHost.isEmpty()) {
      throw new TTransportException("Unable to get CompactionCoordinator address from ZooKeeper");
    }
    while (matches == 0) {
      PExternalCompactionList running =
          ExternalCompactionTestUtils.getRunningCompactions(ctx, coordinatorHost);
      if (!running.getCompactionsMap().isEmpty()) {
        for (ExternalCompactionId ecid : ecids) {
          PExternalCompaction tec = running.getCompactionsMap().get(ecid.canonical());
          if (tec != null && !tec.getUpdatesMap().isEmpty()) {
            matches++;
            assertEquals(PCompactionState.STARTED, ExternalCompactionTestUtils.getLastState(tec));
          }
        }
      }
      if (matches == 0) {
        UtilWaitThread.sleep(50);
      }
    }
    return matches;
  }

  public static void confirmCompactionCompleted(ServerContext ctx, Set<ExternalCompactionId> ecids,
      PCompactionState expectedState) throws Exception {
    Optional<HostAndPort> coordinatorHost = ExternalCompactionUtil.findCompactionCoordinator(ctx);
    if (coordinatorHost.isEmpty()) {
      throw new TTransportException("Unable to get CompactionCoordinator address from ZooKeeper");
    }

    // The running compaction should be removed
    PExternalCompactionList running =
        ExternalCompactionTestUtils.getRunningCompactions(ctx, coordinatorHost);
    while (running.getCompactionsMap().keySet().stream()
        .anyMatch((e) -> ecids.contains(ExternalCompactionId.of(e)))) {
      running = ExternalCompactionTestUtils.getRunningCompactions(ctx, coordinatorHost);
    }
    // The compaction should be in the completed list with the expected state
    PExternalCompactionList completed =
        ExternalCompactionTestUtils.getCompletedCompactions(ctx, coordinatorHost);
    while (completed.getCompactionsMap().isEmpty()) {
      UtilWaitThread.sleep(50);
      completed = ExternalCompactionTestUtils.getCompletedCompactions(ctx, coordinatorHost);
    }
    for (ExternalCompactionId e : ecids) {
      PExternalCompaction tec = completed.getCompactionsMap().get(e.canonical());
      assertNotNull(tec);
      assertEquals(expectedState, ExternalCompactionTestUtils.getLastState(tec));
    }

  }

  public static void assertNoCompactionMetadata(ServerContext ctx, String tableName) {
    var tableId = TableId.of(ctx.tableOperations().tableIdMap().get(tableName));
    try (var tabletsMetadata = ctx.getAmple().readTablets().forTable(tableId).build()) {

      int count = 0;

      for (var tabletMetadata : tabletsMetadata) {
        assertEquals(Set.of(), tabletMetadata.getCompacted());
        assertNull(tabletMetadata.getSelectedFiles());
        assertEquals(Set.of(), tabletMetadata.getExternalCompactions().keySet());
        assertEquals(Set.of(), tabletMetadata.getUserCompactionsRequested());
        count++;
      }

      assertTrue(count > 0);
    }
  }
}

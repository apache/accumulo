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

import static org.apache.accumulo.test.compaction.ExternalCompactionTestUtils.GROUP2;
import static org.apache.accumulo.test.compaction.ExternalCompactionTestUtils.compact;
import static org.apache.accumulo.test.compaction.ExternalCompactionTestUtils.confirmCompactionCompleted;
import static org.apache.accumulo.test.compaction.ExternalCompactionTestUtils.createTable;
import static org.apache.accumulo.test.compaction.ExternalCompactionTestUtils.getRunningCompactions;
import static org.apache.accumulo.test.compaction.ExternalCompactionTestUtils.waitForCompactionStartAndReturnEcids;
import static org.apache.accumulo.test.compaction.ExternalCompactionTestUtils.writeData;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.compaction.thrift.CompactionCoordinatorService;
import org.apache.accumulo.core.compaction.thrift.TCompactionState;
import org.apache.accumulo.core.compaction.thrift.TCompactionStatusUpdate;
import org.apache.accumulo.core.compaction.thrift.TExternalCompaction;
import org.apache.accumulo.core.compaction.thrift.TExternalCompactionList;
import org.apache.accumulo.core.compaction.thrift.TExternalCompactionMap;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.iterators.IteratorUtil.IteratorScope;
import org.apache.accumulo.core.metadata.schema.ExternalCompactionId;
import org.apache.accumulo.core.metadata.schema.TabletMetadata;
import org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType;
import org.apache.accumulo.core.metadata.schema.TabletsMetadata;
import org.apache.accumulo.core.rpc.ThriftUtil;
import org.apache.accumulo.core.rpc.clients.ThriftClientTypes;
import org.apache.accumulo.core.trace.TraceUtil;
import org.apache.accumulo.core.util.UtilWaitThread;
import org.apache.accumulo.core.util.compaction.ExternalCompactionUtil;
import org.apache.accumulo.core.util.compaction.RunningCompactionInfo;
import org.apache.accumulo.harness.MiniClusterConfigurationCallback;
import org.apache.accumulo.harness.SharedMiniClusterBase;
import org.apache.accumulo.manager.compaction.coordinator.CompactionCoordinator;
import org.apache.accumulo.minicluster.ServerType;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloConfigImpl;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.util.FindCompactionTmpFiles;
import org.apache.accumulo.test.functional.SlowIterator;
import org.apache.accumulo.test.util.Wait;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransportException;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.google.common.net.HostAndPort;

public class ExternalCompaction_3_IT extends SharedMiniClusterBase {

  public static class ExternalCompaction3Config implements MiniClusterConfigurationCallback {
    @Override
    public void configureMiniCluster(MiniAccumuloConfigImpl cfg, Configuration coreSite) {
      ExternalCompactionTestUtils.configureMiniCluster(cfg, coreSite);
    }
  }

  @BeforeAll
  public static void beforeTests() throws Exception {
    startMiniClusterWithConfig(new ExternalCompaction3Config());
  }

  @Test
  public void testMergeCancelsExternalCompaction() throws Exception {

    String table1 = this.getUniqueNames(1)[0];
    try (AccumuloClient client =
        Accumulo.newClient().from(getCluster().getClientProperties()).build()) {

      getCluster().getClusterControl().stop(ServerType.COMPACTOR);
      getCluster().getClusterControl().start(ServerType.COMPACTOR, null, 1,
          ExternalDoNothingCompactor.class);

      createTable(client, table1, "cs1", 2);
      // set compaction ratio to 1 so that majc occurs naturally, not user compaction
      // user compaction blocks merge
      client.tableOperations().setProperty(table1, Property.TABLE_MAJC_RATIO.toString(), "1.0");
      // cause multiple rfiles to be created
      writeData(client, table1);
      writeData(client, table1);
      writeData(client, table1);
      writeData(client, table1);

      TableId tid = getCluster().getServerContext().getTableId(table1);

      // Wait for the compaction to start by waiting for 1 external compaction column
      Set<ExternalCompactionId> ecids =
          waitForCompactionStartAndReturnEcids(getCluster().getServerContext(), tid);

      var md = new ArrayList<TabletMetadata>();
      try (TabletsMetadata tm = getCluster().getServerContext().getAmple().readTablets()
          .forTable(tid).fetch(ColumnType.PREV_ROW).build()) {
        tm.forEach(t -> md.add(t));
        assertEquals(2, md.size());
      }

      // Verify that a tmp file is created
      Wait.waitFor(() -> FindCompactionTmpFiles
          .findTempFiles(getCluster().getServerContext(), tid.canonical()).size() == 1);

      // Merge - blocking operation
      Text start = md.get(0).getPrevEndRow();
      Text end = md.get(1).getEndRow();
      client.tableOperations().merge(table1, start, end);

      confirmCompactionCompleted(getCluster().getServerContext(), ecids,
          TCompactionState.CANCELLED);

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
      }
      // We need to cancel the compaction or delete the table here because we initiate a user
      // compaction above in the test. Even though the external compaction was cancelled
      // because we split the table, FaTE will continue to queue up a compaction
      client.tableOperations().delete(table1);

      // Verify that the tmp file are cleaned up
      Wait.waitFor(() -> FindCompactionTmpFiles
          .findTempFiles(getCluster().getServerContext(), tid.canonical()).size() == 0);
    } finally {
      getCluster().getClusterControl().stop(ServerType.COMPACTOR);
      getCluster().getClusterControl().start(ServerType.COMPACTOR);

    }
  }

  @Test
  public void testCoordinatorRestartsDuringCompaction() throws Exception {

    String table1 = this.getUniqueNames(1)[0];
    try (AccumuloClient client =
        Accumulo.newClient().from(getCluster().getClientProperties()).build()) {

      createTable(client, table1, "cs2", 2);
      writeData(client, table1);

      IteratorSetting setting = new IteratorSetting(50, "slow", SlowIterator.class);
      SlowIterator.setSeekSleepTime(setting, 5000);
      SlowIterator.setSleepTime(setting, 5000);
      client.tableOperations().attachIterator(table1, setting, EnumSet.of(IteratorScope.majc));

      compact(client, table1, 2, GROUP2, false);

      TableId tid = getCluster().getServerContext().getTableId(table1);

      // Wait for the compaction to start by waiting for 1 external compaction column
      Set<ExternalCompactionId> ecids =
          waitForCompactionStartAndReturnEcids(getCluster().getServerContext(), tid);

      ServerContext ctx = getCluster().getServerContext();

      // Wait for all compactions to start
      Map<ExternalCompactionId,RunningCompactionInfo> originalRunningInfo = null;
      do {
        originalRunningInfo = getRunningCompactionInformation(ctx, ecids);
      } while (originalRunningInfo == null
          || originalRunningInfo.values().stream().allMatch(rci -> rci.duration == 0));

      Map<String,TExternalCompactionList> longestRunning = getLongRunningCompactions(ctx);
      assertTrue(longestRunning.containsKey(GROUP2));
      assertEquals(originalRunningInfo.size(), longestRunning.get(GROUP2).getCompactions().size());

      // Stop the Manager (Coordinator)
      getCluster().getClusterControl().stop(ServerType.MANAGER);

      // Restart the Manager while the compaction is running
      getCluster().getClusterControl().start(ServerType.MANAGER);

      Map<ExternalCompactionId,RunningCompactionInfo> postRestartRunningInfo =
          getRunningCompactionInformation(ctx, ecids);

      for (Entry<ExternalCompactionId,RunningCompactionInfo> post : postRestartRunningInfo
          .entrySet()) {
        if (originalRunningInfo.containsKey(post.getKey())) {
          assertTrue(
              (post.getValue().duration - originalRunningInfo.get(post.getKey()).duration) > 0);
        }
        final String lastState = post.getValue().status;
        assertTrue(lastState.equals(TCompactionState.IN_PROGRESS.name()));
      }

      longestRunning = getLongRunningCompactions(ctx);
      assertTrue(longestRunning.containsKey(GROUP2));
      assertEquals(postRestartRunningInfo.size(),
          longestRunning.get(GROUP2).getCompactions().size());

      // We need to cancel the compaction or delete the table here because we initiate a user
      // compaction above in the test. Even though the external compaction was cancelled
      // because we split the table, FaTE will continue to queue up a compaction
      client.tableOperations().cancelCompaction(table1);

    }
  }

  private Map<ExternalCompactionId,RunningCompactionInfo> getRunningCompactionInformation(
      ServerContext ctx, Set<ExternalCompactionId> ecids) throws InterruptedException {

    final Map<ExternalCompactionId,RunningCompactionInfo> results = new HashMap<>();

    while (results.isEmpty()) {
      TExternalCompactionMap running = null;
      while (running == null || running.getCompactions() == null) {
        try {
          Optional<HostAndPort> coordinatorHost =
              ExternalCompactionUtil.findCompactionCoordinator(ctx);
          if (coordinatorHost.isEmpty()) {
            throw new TTransportException(
                "Unable to get CompactionCoordinator address from ZooKeeper");
          }
          running = getRunningCompactions(ctx, coordinatorHost);
        } catch (TException t) {
          running = null;
          Thread.sleep(2000);
        }
      }
      for (ExternalCompactionId ecid : ecids) {
        final TExternalCompaction tec = running.getCompactions().get(ecid.canonical());
        if (tec != null && tec.getUpdatesSize() > 0) {
          // When the coordinator restarts it inserts a message into the updates. If this
          // is the last message, then don't insert this into the results. We want to get
          // an actual update from the Compactor.
          TreeMap<Long,TCompactionStatusUpdate> sorted = new TreeMap<>(tec.getUpdates());
          var lastEntry = sorted.lastEntry();
          if (lastEntry.getValue().getMessage().equals(CompactionCoordinator.RESTART_UPDATE_MSG)) {
            continue;
          }
          results.put(ecid, new RunningCompactionInfo(tec));
        }
      }
    }
    return results;
  }

  private Map<String,TExternalCompactionList> getLongRunningCompactions(ServerContext ctx)
      throws InterruptedException {

    Map<String,TExternalCompactionList> results = new HashMap<>();

    while (results.isEmpty()) {
      try {
        Optional<HostAndPort> coordinatorHost =
            ExternalCompactionUtil.findCompactionCoordinator(ctx);
        if (coordinatorHost.isEmpty()) {
          throw new TTransportException(
              "Unable to get CompactionCoordinator address from ZooKeeper");
        }
        CompactionCoordinatorService.Client client =
            ThriftUtil.getClient(ThriftClientTypes.COORDINATOR, coordinatorHost.orElseThrow(), ctx);
        try {
          results = client.getLongRunningCompactions(TraceUtil.traceInfo(), ctx.rpcCreds());
        } finally {
          ThriftUtil.returnClient(client, ctx);
        }
      } catch (TException t) {
        Thread.sleep(2000);
      }
    }
    return results;
  }

}

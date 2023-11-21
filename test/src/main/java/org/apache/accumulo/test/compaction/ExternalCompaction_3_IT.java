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
import static org.apache.accumulo.test.compaction.ExternalCompactionTestUtils.getLastState;
import static org.apache.accumulo.test.compaction.ExternalCompactionTestUtils.getRunningCompactions;
import static org.apache.accumulo.test.compaction.ExternalCompactionTestUtils.waitForCompactionStartAndReturnEcids;
import static org.apache.accumulo.test.compaction.ExternalCompactionTestUtils.writeData;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.compaction.thrift.TCompactionState;
import org.apache.accumulo.core.compaction.thrift.TExternalCompaction;
import org.apache.accumulo.core.compaction.thrift.TExternalCompactionList;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.metadata.schema.ExternalCompactionId;
import org.apache.accumulo.core.metadata.schema.TabletMetadata;
import org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType;
import org.apache.accumulo.core.metadata.schema.TabletsMetadata;
import org.apache.accumulo.core.util.UtilWaitThread;
import org.apache.accumulo.core.util.compaction.ExternalCompactionUtil;
import org.apache.accumulo.harness.MiniClusterConfigurationCallback;
import org.apache.accumulo.harness.SharedMiniClusterBase;
import org.apache.accumulo.minicluster.ServerType;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloConfigImpl;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.util.FindCompactionTmpFiles;
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
    getCluster().getClusterControl().stop(ServerType.COMPACTOR);
    getCluster().getClusterControl().start(ServerType.COMPACTOR, null, 1,
        ExternalDoNothingCompactor.class);
  }

  @Test
  public void testMergeCancelsExternalCompaction() throws Exception {

    String table1 = this.getUniqueNames(1)[0];
    try (AccumuloClient client =
        Accumulo.newClient().from(getCluster().getClientProperties()).build()) {

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
    }
  }

  @Test
  public void testCoordinatorRestartsDuringCompaction() throws Exception {

    String table1 = this.getUniqueNames(1)[0];
    try (AccumuloClient client =
        Accumulo.newClient().from(getCluster().getClientProperties()).build()) {

      createTable(client, table1, "cs2", 2);
      writeData(client, table1);
      compact(client, table1, 2, GROUP2, false);

      TableId tid = getCluster().getServerContext().getTableId(table1);

      // Wait for the compaction to start by waiting for 1 external compaction column
      Set<ExternalCompactionId> ecids =
          waitForCompactionStartAndReturnEcids(getCluster().getServerContext(), tid);

      // Stop the Manager (Coordinator)
      getCluster().getClusterControl().stop(ServerType.MANAGER);

      // Restart the Manager while the compaction is running
      getCluster().getClusterControl().start(ServerType.MANAGER);

      ServerContext ctx = getCluster().getServerContext();

      // Confirm compaction is still running
      int matches = 0;
      while (matches == 0) {
        TExternalCompactionList running = null;
        while (running == null) {
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
        if (running.getCompactions() != null) {
          for (ExternalCompactionId ecid : ecids) {
            TExternalCompaction tec = running.getCompactions().get(ecid.canonical());
            if (tec != null && tec.getUpdates() != null && !tec.getUpdates().isEmpty()) {
              matches++;
              assertEquals(TCompactionState.IN_PROGRESS, getLastState(tec));
            }
          }
        }
        UtilWaitThread.sleep(250);
      }
      assertTrue(matches > 0);

      // We need to cancel the compaction or delete the table here because we initiate a user
      // compaction above in the test. Even though the external compaction was cancelled
      // because we split the table, FaTE will continue to queue up a compaction
      client.tableOperations().cancelCompaction(table1);

    }
  }

}

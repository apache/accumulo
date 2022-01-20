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
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.accumulo.coordinator.CompactionCoordinator;
import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.clientImpl.Tables;
import org.apache.accumulo.core.compaction.thrift.TCompactionState;
import org.apache.accumulo.core.compaction.thrift.TExternalCompaction;
import org.apache.accumulo.core.compaction.thrift.TExternalCompactionList;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.metadata.schema.ExternalCompactionId;
import org.apache.accumulo.core.metadata.schema.TabletMetadata;
import org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType;
import org.apache.accumulo.core.metadata.schema.TabletsMetadata;
import org.apache.accumulo.fate.util.UtilWaitThread;
import org.apache.accumulo.harness.AccumuloClusterHarness;
import org.apache.accumulo.harness.MiniClusterConfigurationCallback;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloClusterImpl;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloClusterImpl.ProcessInfo;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloConfigImpl;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.junit.After;
import org.junit.Test;

public class ExternalCompaction_3_IT extends AccumuloClusterHarness
    implements MiniClusterConfigurationCallback {

  private ProcessInfo testCompactor = null;
  private ProcessInfo testCoordinator = null;

  @Override
  public void configureMiniCluster(MiniAccumuloConfigImpl cfg, Configuration coreSite) {
    ExternalCompactionTestUtils.configureMiniCluster(cfg, coreSite);
  }

  @After
  public void tearDown() throws Exception {
    super.teardownCluster();
    ExternalCompactionTestUtils.stopProcesses(testCompactor, testCoordinator);
    testCompactor = null;
    testCoordinator = null;
  }

  @Test
  public void testMergeCancelsExternalCompaction() throws Exception {
    String table1 = this.getUniqueNames(1)[0];
    try (AccumuloClient client =
        Accumulo.newClient().from(getCluster().getClientProperties()).build()) {

      ExternalCompactionTestUtils.createTable(client, table1, "cs1", 2);
      // set compaction ratio to 1 so that majc occurs naturally, not user compaction
      // user compaction blocks merge
      client.tableOperations().setProperty(table1, Property.TABLE_MAJC_RATIO.toString(), "1.0");
      // cause multiple rfiles to be created
      ExternalCompactionTestUtils.writeData(client, table1);
      ExternalCompactionTestUtils.writeData(client, table1);
      ExternalCompactionTestUtils.writeData(client, table1);
      ExternalCompactionTestUtils.writeData(client, table1);

      TableId tid = Tables.getTableId(getCluster().getServerContext(), table1);

      testCoordinator =
          ExternalCompactionTestUtils.startCoordinator(((MiniAccumuloClusterImpl) getCluster()),
              CompactionCoordinator.class, getCluster().getServerContext());

      testCompactor = ((MiniAccumuloClusterImpl) getCluster())
          .exec(ExternalDoNothingCompactor.class, "-q", "DCQ1");

      // Wait for the compaction to start by waiting for 1 external compaction column
      Set<ExternalCompactionId> ecids = ExternalCompactionTestUtils
          .waitForCompactionStartAndReturnEcids(getCluster().getServerContext(), tid);

      var md = new ArrayList<TabletMetadata>();
      try (TabletsMetadata tm = getCluster().getServerContext().getAmple().readTablets()
          .forTable(tid).fetch(ColumnType.PREV_ROW).build()) {
        tm.forEach(t -> md.add(t));
        assertEquals(2, md.size());
      }

      // Merge - blocking operation
      Text start = md.get(0).getPrevEndRow();
      Text end = md.get(1).getEndRow();
      client.tableOperations().merge(table1, start, end);

      ExternalCompactionTestUtils.confirmCompactionCompleted(getCluster().getServerContext(), ecids,
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
    }
  }

  @Test
  public void testCoordinatorRestartsDuringCompaction() throws Exception {
    String table1 = this.getUniqueNames(1)[0];
    try (AccumuloClient client =
        Accumulo.newClient().from(getCluster().getClientProperties()).build()) {

      ExternalCompactionTestUtils.createTable(client, table1, "cs1", 2);
      ExternalCompactionTestUtils.writeData(client, table1);

      testCoordinator =
          ExternalCompactionTestUtils.startCoordinator(((MiniAccumuloClusterImpl) getCluster()),
              CompactionCoordinator.class, getCluster().getServerContext());

      testCompactor = ((MiniAccumuloClusterImpl) getCluster())
          .exec(ExternalDoNothingCompactor.class, "-q", "DCQ1");

      ExternalCompactionTestUtils.compact(client, table1, 2, "DCQ1", false);

      TableId tid = Tables.getTableId(getCluster().getServerContext(), table1);

      // Wait for the compaction to start by waiting for 1 external compaction column
      Set<ExternalCompactionId> ecids = ExternalCompactionTestUtils
          .waitForCompactionStartAndReturnEcids(getCluster().getServerContext(), tid);

      // Stop the Coordinator
      ExternalCompactionTestUtils.stopProcesses(testCoordinator);

      // Restart the coordinator while the compaction is running
      testCoordinator =
          ExternalCompactionTestUtils.startCoordinator(((MiniAccumuloClusterImpl) getCluster()),
              CompactionCoordinator.class, getCluster().getServerContext());

      // Confirm compaction is still running
      int matches = 0;
      while (matches == 0) {
        TExternalCompactionList running =
            ExternalCompactionTestUtils.getRunningCompactions(getCluster().getServerContext());
        if (running.getCompactions() != null) {
          for (ExternalCompactionId ecid : ecids) {
            TExternalCompaction tec = running.getCompactions().get(ecid.canonical());
            if (tec != null && tec.getUpdates() != null && !tec.getUpdates().isEmpty()) {
              matches++;
              assertEquals(TCompactionState.IN_PROGRESS,
                  ExternalCompactionTestUtils.getLastState(tec));
            }
          }
        }
        UtilWaitThread.sleep(250);
      }
      assertTrue(matches > 0);

      // Delete the table to cancel the running compaction, else the ExternalDoNothingCompactor
      // will run forever
      client.tableOperations().delete(table1);

    }
  }

}

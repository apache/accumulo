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

import static org.apache.accumulo.test.compaction.ExternalCompactionTestUtils.MAX_DATA;
import static org.apache.accumulo.test.compaction.ExternalCompactionTestUtils.QUEUE1;
import static org.apache.accumulo.test.compaction.ExternalCompactionTestUtils.QUEUE2;
import static org.apache.accumulo.test.compaction.ExternalCompactionTestUtils.QUEUE3;
import static org.apache.accumulo.test.compaction.ExternalCompactionTestUtils.QUEUE4;
import static org.apache.accumulo.test.compaction.ExternalCompactionTestUtils.QUEUE5;
import static org.apache.accumulo.test.compaction.ExternalCompactionTestUtils.compact;
import static org.apache.accumulo.test.compaction.ExternalCompactionTestUtils.confirmCompactionCompleted;
import static org.apache.accumulo.test.compaction.ExternalCompactionTestUtils.confirmCompactionRunning;
import static org.apache.accumulo.test.compaction.ExternalCompactionTestUtils.createTable;
import static org.apache.accumulo.test.compaction.ExternalCompactionTestUtils.getFinalStatesForTable;
import static org.apache.accumulo.test.compaction.ExternalCompactionTestUtils.getRunningCompactions;
import static org.apache.accumulo.test.compaction.ExternalCompactionTestUtils.row;
import static org.apache.accumulo.test.compaction.ExternalCompactionTestUtils.waitForCompactionStartAndReturnEcids;
import static org.apache.accumulo.test.compaction.ExternalCompactionTestUtils.writeData;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.Collections;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import org.apache.accumulo.compactor.Compactor;
import org.apache.accumulo.coordinator.CompactionCoordinator;
import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.compaction.thrift.TCompactionState;
import org.apache.accumulo.core.compaction.thrift.TExternalCompactionList;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.metadata.schema.ExternalCompactionId;
import org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType;
import org.apache.accumulo.core.metadata.schema.TabletsMetadata;
import org.apache.accumulo.core.util.UtilWaitThread;
import org.apache.accumulo.harness.MiniClusterConfigurationCallback;
import org.apache.accumulo.harness.SharedMiniClusterBase;
import org.apache.accumulo.minicluster.ServerType;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloConfigImpl;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ExternalCompaction_2_IT extends SharedMiniClusterBase {

  public static class ExternalCompaction2Config implements MiniClusterConfigurationCallback {
    @Override
    public void configureMiniCluster(MiniAccumuloConfigImpl cfg, Configuration coreSite) {
      ExternalCompactionTestUtils.configureMiniCluster(cfg, coreSite);
    }
  }

  private static final Logger LOG = LoggerFactory.getLogger(ExternalCompaction_2_IT.class);

  @BeforeAll
  public static void beforeTests() throws Exception {
    startMiniClusterWithConfig(new ExternalCompaction2Config());
  }

  @AfterEach
  public void tearDown() throws Exception {
    // The ExternalDoNothingCompactor needs to be restarted between tests
    getCluster().getClusterControl().stop(ServerType.COMPACTOR);
  }

  @Test
  public void testSplitCancelsExternalCompaction() throws Exception {

    getCluster().getClusterControl().startCoordinator(CompactionCoordinator.class);
    getCluster().getClusterControl().startCompactors(ExternalDoNothingCompactor.class, 1, QUEUE1);

    String table1 = this.getUniqueNames(1)[0];
    try (AccumuloClient client =
        Accumulo.newClient().from(getCluster().getClientProperties()).build()) {

      createTable(client, table1, "cs1");
      TableId tid = getCluster().getServerContext().getTableId(table1);
      writeData(client, table1);
      compact(client, table1, 2, QUEUE1, false);

      // Wait for the compaction to start by waiting for 1 external compaction column
      Set<ExternalCompactionId> ecids = ExternalCompactionTestUtils
          .waitForCompactionStartAndReturnEcids(getCluster().getServerContext(), tid);

      // Confirm that this ECID shows up in RUNNING set
      int matches = ExternalCompactionTestUtils
          .confirmCompactionRunning(getCluster().getServerContext(), ecids);
      assertTrue(matches > 0);

      // ExternalDoNothingCompactor will not compact, it will wait, split the table.
      SortedSet<Text> splits = new TreeSet<>();
      int jump = MAX_DATA / 5;
      for (int r = jump; r < MAX_DATA; r += jump) {
        splits.add(new Text(row(r)));
      }

      client.tableOperations().addSplits(table1, splits);

      confirmCompactionCompleted(getCluster().getServerContext(), ecids,
          TCompactionState.CANCELLED);

      // ensure compaction ids were deleted by split operation from metadata table
      try (TabletsMetadata tm = getCluster().getServerContext().getAmple().readTablets()
          .forTable(tid).fetch(ColumnType.ECOMP).build()) {
        Set<ExternalCompactionId> ecids2 = tm.stream()
            .flatMap(t -> t.getExternalCompactions().keySet().stream()).collect(Collectors.toSet());
        assertTrue(Collections.disjoint(ecids, ecids2));
      }

      // We need to cancel the compaction or delete the table here because we initiate a user
      // compaction above in the test. Even though the external compaction was cancelled
      // because we split the table, FaTE will continue to queue up a compaction
      client.tableOperations().cancelCompaction(table1);
    }
  }

  @Test
  public void testExternalCompactionsSucceedsRunWithTableOffline() throws Exception {

    getCluster().getClusterControl().stop(ServerType.COMPACTION_COORDINATOR);
    getCluster().getClusterControl().stop(ServerType.COMPACTOR);

    String table1 = this.getUniqueNames(1)[0];
    try (AccumuloClient client =
        Accumulo.newClient().from(getCluster().getClientProperties()).build()) {

      createTable(client, table1, "cs2");
      // set compaction ratio to 1 so that majc occurs naturally, not user compaction
      // user compaction blocks merge
      client.tableOperations().setProperty(table1, Property.TABLE_MAJC_RATIO.toString(), "1.0");
      // cause multiple rfiles to be created
      writeData(client, table1);
      writeData(client, table1);
      writeData(client, table1);
      writeData(client, table1);

      getCluster().getClusterControl()
          .startCoordinator(TestCompactionCoordinatorForOfflineTable.class);

      TableId tid = getCluster().getServerContext().getTableId(table1);
      // Confirm that no final state is in the metadata table
      assertEquals(0, getFinalStatesForTable(getCluster(), tid).count());

      // Offline the table when the compaction starts
      final AtomicBoolean succeededInTakingOffline = new AtomicBoolean(false);
      Thread t = new Thread(() -> {
        try (AccumuloClient client2 =
            Accumulo.newClient().from(getCluster().getClientProperties()).build()) {
          TExternalCompactionList metrics2 = getRunningCompactions(getCluster().getServerContext());
          while (metrics2.getCompactions() == null) {
            metrics2 = getRunningCompactions(getCluster().getServerContext());
            if (metrics2.getCompactions() == null) {
              UtilWaitThread.sleep(50);
            }
          }
          LOG.info("Taking table offline");
          client2.tableOperations().offline(table1, false);
          succeededInTakingOffline.set(true);
        } catch (Exception e) {
          LOG.error("Error: ", e);
        }
      });
      t.start();

      // Start the compactor
      getCluster().getClusterControl().startCompactors(Compactor.class, 1, QUEUE2);

      // Wait for the compaction to start by waiting for 1 external compaction column
      Set<ExternalCompactionId> ecids = ExternalCompactionTestUtils
          .waitForCompactionStartAndReturnEcids(getCluster().getServerContext(), tid);

      // Confirm that this ECID shows up in RUNNING set
      int matches = ExternalCompactionTestUtils
          .confirmCompactionRunning(getCluster().getServerContext(), ecids);
      assertTrue(matches > 0);

      t.join();
      if (!succeededInTakingOffline.get()) {
        fail("Failed to offline table");
      }

      confirmCompactionCompleted(getCluster().getServerContext(), ecids,
          TCompactionState.SUCCEEDED);

      // Confirm that final state is in the metadata table
      assertEquals(1, getFinalStatesForTable(getCluster(), tid).count());

      // Online the table
      client.tableOperations().online(table1);

      // wait for compaction to be committed by tserver or test timeout
      long finalStateCount = getFinalStatesForTable(getCluster(), tid).count();
      while (finalStateCount > 0) {
        finalStateCount = getFinalStatesForTable(getCluster(), tid).count();
        if (finalStateCount > 0) {
          UtilWaitThread.sleep(50);
        }
      }

      // We need to cancel the compaction or delete the table here because we initiate a user
      // compaction above in the test. Even though the external compaction was cancelled
      // because we split the table, FaTE will continue to queue up a compaction
      client.tableOperations().delete(table1);

      getCluster().getClusterControl().stop(ServerType.COMPACTION_COORDINATOR);
      getCluster().getClusterControl().stop(ServerType.COMPACTOR);
    }
  }

  @Test
  public void testUserCompactionCancellation() throws Exception {

    getCluster().getClusterControl().startCoordinator(CompactionCoordinator.class);
    getCluster().getClusterControl().startCompactors(ExternalDoNothingCompactor.class, 1, QUEUE3);

    String table1 = this.getUniqueNames(1)[0];
    try (AccumuloClient client =
        Accumulo.newClient().from(getCluster().getClientProperties()).build()) {

      createTable(client, table1, "cs3");
      TableId tid = getCluster().getServerContext().getTableId(table1);
      writeData(client, table1);
      compact(client, table1, 2, QUEUE3, false);

      // Wait for the compaction to start by waiting for 1 external compaction column
      Set<ExternalCompactionId> ecids = ExternalCompactionTestUtils
          .waitForCompactionStartAndReturnEcids(getCluster().getServerContext(), tid);

      // Confirm that this ECID shows up in RUNNING set
      int matches = ExternalCompactionTestUtils
          .confirmCompactionRunning(getCluster().getServerContext(), ecids);
      assertTrue(matches > 0);

      client.tableOperations().cancelCompaction(table1);

      confirmCompactionCompleted(getCluster().getServerContext(), ecids,
          TCompactionState.CANCELLED);

      // We need to cancel the compaction or delete the table here because we initiate a user
      // compaction above in the test. Even though the external compaction was cancelled
      // because we split the table, FaTE will continue to queue up a compaction
      client.tableOperations().cancelCompaction(table1);
    }
  }

  @Test
  public void testDeleteTableCancelsUserExternalCompaction() throws Exception {

    getCluster().getClusterControl().startCoordinator(CompactionCoordinator.class);
    getCluster().getClusterControl().startCompactors(ExternalDoNothingCompactor.class, 1, QUEUE4);

    String table1 = this.getUniqueNames(1)[0];
    try (AccumuloClient client =
        Accumulo.newClient().from(getCluster().getClientProperties()).build()) {

      createTable(client, table1, "cs4");
      TableId tid = getCluster().getServerContext().getTableId(table1);
      writeData(client, table1);
      compact(client, table1, 2, QUEUE4, false);

      // Wait for the compaction to start by waiting for 1 external compaction column
      Set<ExternalCompactionId> ecids =
          waitForCompactionStartAndReturnEcids(getCluster().getServerContext(), tid);

      // Confirm that this ECID shows up in RUNNING set
      int matches = confirmCompactionRunning(getCluster().getServerContext(), ecids);
      assertTrue(matches > 0);

      client.tableOperations().delete(table1);

      confirmCompactionCompleted(getCluster().getServerContext(), ecids,
          TCompactionState.CANCELLED);

    }
  }

  @Test
  public void testDeleteTableCancelsExternalCompaction() throws Exception {
    getCluster().getClusterControl().startCoordinator(CompactionCoordinator.class);
    getCluster().getClusterControl().startCompactors(ExternalDoNothingCompactor.class, 1, QUEUE5);

    String table1 = this.getUniqueNames(1)[0];
    try (AccumuloClient client =
        Accumulo.newClient().from(getCluster().getClientProperties()).build()) {

      createTable(client, table1, "cs5");
      // set compaction ratio to 1 so that majc occurs naturally, not user compaction
      // user compaction blocks delete
      client.tableOperations().setProperty(table1, Property.TABLE_MAJC_RATIO.toString(), "1.0");
      // cause multiple rfiles to be created
      writeData(client, table1);
      writeData(client, table1);
      writeData(client, table1);
      writeData(client, table1);

      TableId tid = getCluster().getServerContext().getTableId(table1);

      // Wait for the compaction to start by waiting for 1 external compaction column
      Set<ExternalCompactionId> ecids = ExternalCompactionTestUtils
          .waitForCompactionStartAndReturnEcids(getCluster().getServerContext(), tid);

      // Confirm that this ECID shows up in RUNNING set
      int matches = ExternalCompactionTestUtils
          .confirmCompactionRunning(getCluster().getServerContext(), ecids);
      assertTrue(matches > 0);

      client.tableOperations().delete(table1);

      confirmCompactionCompleted(getCluster().getServerContext(), ecids,
          TCompactionState.CANCELLED);

      TabletsMetadata tm = getCluster().getServerContext().getAmple().readTablets().forTable(tid)
          .fetch(ColumnType.ECOMP).build();
      assertEquals(0, tm.stream().count());
      tm.close();

    }
  }

}

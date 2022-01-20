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
import static org.junit.Assert.fail;

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
import org.apache.accumulo.core.clientImpl.Tables;
import org.apache.accumulo.core.compaction.thrift.TCompactionState;
import org.apache.accumulo.core.compaction.thrift.TExternalCompactionList;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.metadata.schema.ExternalCompactionId;
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
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ExternalCompaction_2_IT extends AccumuloClusterHarness
    implements MiniClusterConfigurationCallback {

  private static final Logger LOG = LoggerFactory.getLogger(ExternalCompaction_2_IT.class);

  @Override
  public void configureMiniCluster(MiniAccumuloConfigImpl cfg, Configuration coreSite) {
    ExternalCompactionTestUtils.configureMiniCluster(cfg, coreSite);
  }

  private ProcessInfo testCompactor = null;
  private ProcessInfo testCoordinator = null;

  @Before
  public void setUp() throws Exception {
    super.setupCluster();
    testCompactor = ((MiniAccumuloClusterImpl) getCluster()).exec(ExternalDoNothingCompactor.class,
        "-q", "DCQ1");
    testCoordinator =
        ExternalCompactionTestUtils.startCoordinator(((MiniAccumuloClusterImpl) getCluster()),
            CompactionCoordinator.class, getCluster().getServerContext());
  }

  @After
  public void tearDown() throws Exception {
    super.teardownCluster();
    ExternalCompactionTestUtils.stopProcesses(testCompactor, testCoordinator);
    testCompactor = null;
    testCoordinator = null;
  }

  @Test
  public void testSplitCancelsExternalCompaction() throws Exception {
    String table1 = this.getUniqueNames(1)[0];
    try (AccumuloClient client =
        Accumulo.newClient().from(getCluster().getClientProperties()).build()) {

      ExternalCompactionTestUtils.createTable(client, table1, "cs1");
      TableId tid = Tables.getTableId(getCluster().getServerContext(), table1);
      ExternalCompactionTestUtils.writeData(client, table1);
      ExternalCompactionTestUtils.compact(client, table1, 2, "DCQ1", false);

      // Wait for the compaction to start by waiting for 1 external compaction column
      Set<ExternalCompactionId> ecids = ExternalCompactionTestUtils
          .waitForCompactionStartAndReturnEcids(getCluster().getServerContext(), tid);

      // Confirm that this ECID shows up in RUNNING set
      int matches = ExternalCompactionTestUtils
          .confirmCompactionRunning(getCluster().getServerContext(), ecids);
      assertTrue(matches > 0);

      // ExternalDoNothingCompactor will not compact, it will wait, split the table.
      SortedSet<Text> splits = new TreeSet<>();
      int jump = ExternalCompactionTestUtils.MAX_DATA / 5;
      for (int r = jump; r < ExternalCompactionTestUtils.MAX_DATA; r += jump) {
        splits.add(new Text(ExternalCompactionTestUtils.row(r)));
      }

      client.tableOperations().addSplits(table1, splits);

      ExternalCompactionTestUtils.confirmCompactionCompleted(getCluster().getServerContext(), ecids,
          TCompactionState.CANCELLED);

      // ensure compaction ids were deleted by split operation from metadata table
      try (TabletsMetadata tm = getCluster().getServerContext().getAmple().readTablets()
          .forTable(tid).fetch(ColumnType.ECOMP).build()) {
        Set<ExternalCompactionId> ecids2 = tm.stream()
            .flatMap(t -> t.getExternalCompactions().keySet().stream()).collect(Collectors.toSet());
        assertTrue(Collections.disjoint(ecids, ecids2));
      }
    }
  }

  @Test
  public void testExternalCompactionsSucceedsRunWithTableOffline() throws Exception {
    ExternalCompactionTestUtils.stopProcesses(testCoordinator, testCompactor);
    String table1 = this.getUniqueNames(1)[0];
    try (AccumuloClient client =
        Accumulo.newClient().from(getCluster().getClientProperties()).build()) {

      ExternalCompactionTestUtils.createTable(client, table1, "cs1");
      // set compaction ratio to 1 so that majc occurs naturally, not user compaction
      // user compaction blocks merge
      client.tableOperations().setProperty(table1, Property.TABLE_MAJC_RATIO.toString(), "1.0");
      // cause multiple rfiles to be created
      ExternalCompactionTestUtils.writeData(client, table1);
      ExternalCompactionTestUtils.writeData(client, table1);
      ExternalCompactionTestUtils.writeData(client, table1);
      ExternalCompactionTestUtils.writeData(client, table1);

      ProcessInfo coord =
          ExternalCompactionTestUtils.startCoordinator(((MiniAccumuloClusterImpl) getCluster()),
              TestCompactionCoordinatorForOfflineTable.class, getCluster().getServerContext());

      TableId tid = Tables.getTableId(getCluster().getServerContext(), table1);
      // Confirm that no final state is in the metadata table
      assertEquals(0,
          ExternalCompactionTestUtils.getFinalStatesForTable(getCluster(), tid).count());

      // Offline the table when the compaction starts
      final AtomicBoolean succeededInTakingOffline = new AtomicBoolean(false);
      Thread t = new Thread(() -> {
        try (AccumuloClient client2 =
            Accumulo.newClient().from(getCluster().getClientProperties()).build()) {
          TExternalCompactionList metrics2 =
              ExternalCompactionTestUtils.getRunningCompactions(getCluster().getServerContext());
          while (metrics2.getCompactions() == null) {
            UtilWaitThread.sleep(50);
            metrics2 =
                ExternalCompactionTestUtils.getRunningCompactions(getCluster().getServerContext());
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
      ProcessInfo comp =
          ((MiniAccumuloClusterImpl) getCluster()).exec(Compactor.class, "-q", "DCQ1");

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

      ExternalCompactionTestUtils.confirmCompactionCompleted(getCluster().getServerContext(), ecids,
          TCompactionState.SUCCEEDED);

      // Confirm that final state is in the metadata table
      assertEquals(1,
          ExternalCompactionTestUtils.getFinalStatesForTable(getCluster(), tid).count());

      // Online the table
      client.tableOperations().online(table1);

      // wait for compaction to be committed by tserver or test timeout
      long finalStateCount =
          ExternalCompactionTestUtils.getFinalStatesForTable(getCluster(), tid).count();
      while (finalStateCount > 0) {
        UtilWaitThread.sleep(250);
        finalStateCount =
            ExternalCompactionTestUtils.getFinalStatesForTable(getCluster(), tid).count();
      }

      ExternalCompactionTestUtils.stopProcesses(comp, coord);
    }
  }

  @Test
  public void testUserCompactionCancellation() throws Exception {
    String table1 = this.getUniqueNames(1)[0];
    try (AccumuloClient client =
        Accumulo.newClient().from(getCluster().getClientProperties()).build()) {

      ExternalCompactionTestUtils.createTable(client, table1, "cs1");
      ExternalCompactionTestUtils.writeData(client, table1);
      ExternalCompactionTestUtils.compact(client, table1, 2, "DCQ1", false);

      TableId tid = Tables.getTableId(getCluster().getServerContext(), table1);

      // Wait for the compaction to start by waiting for 1 external compaction column
      Set<ExternalCompactionId> ecids = ExternalCompactionTestUtils
          .waitForCompactionStartAndReturnEcids(getCluster().getServerContext(), tid);

      // Confirm that this ECID shows up in RUNNING set
      int matches = ExternalCompactionTestUtils
          .confirmCompactionRunning(getCluster().getServerContext(), ecids);
      assertTrue(matches > 0);

      client.tableOperations().cancelCompaction(table1);

      ExternalCompactionTestUtils.confirmCompactionCompleted(getCluster().getServerContext(), ecids,
          TCompactionState.CANCELLED);

    }
  }

  @Test
  public void testDeleteTableCancelsUserExternalCompaction() throws Exception {
    String table1 = this.getUniqueNames(1)[0];
    try (AccumuloClient client =
        Accumulo.newClient().from(getCluster().getClientProperties()).build()) {

      ExternalCompactionTestUtils.createTable(client, table1, "cs1");
      ExternalCompactionTestUtils.writeData(client, table1);
      ExternalCompactionTestUtils.compact(client, table1, 2, "DCQ1", false);

      TableId tid = Tables.getTableId(getCluster().getServerContext(), table1);

      // Wait for the compaction to start by waiting for 1 external compaction column
      Set<ExternalCompactionId> ecids = ExternalCompactionTestUtils
          .waitForCompactionStartAndReturnEcids(getCluster().getServerContext(), tid);

      // Confirm that this ECID shows up in RUNNING set
      int matches = ExternalCompactionTestUtils
          .confirmCompactionRunning(getCluster().getServerContext(), ecids);
      assertTrue(matches > 0);

      client.tableOperations().delete(table1);

      ExternalCompactionTestUtils.confirmCompactionCompleted(getCluster().getServerContext(), ecids,
          TCompactionState.CANCELLED);
    }
  }

  @Test
  public void testDeleteTableCancelsExternalCompaction() throws Exception {
    String table1 = this.getUniqueNames(1)[0];
    try (AccumuloClient client =
        Accumulo.newClient().from(getCluster().getClientProperties()).build()) {

      ExternalCompactionTestUtils.createTable(client, table1, "cs1");
      // set compaction ratio to 1 so that majc occurs naturally, not user compaction
      // user compaction blocks delete
      client.tableOperations().setProperty(table1, Property.TABLE_MAJC_RATIO.toString(), "1.0");
      // cause multiple rfiles to be created
      ExternalCompactionTestUtils.writeData(client, table1);
      ExternalCompactionTestUtils.writeData(client, table1);
      ExternalCompactionTestUtils.writeData(client, table1);
      ExternalCompactionTestUtils.writeData(client, table1);

      TableId tid = Tables.getTableId(getCluster().getServerContext(), table1);

      // Wait for the compaction to start by waiting for 1 external compaction column
      Set<ExternalCompactionId> ecids = ExternalCompactionTestUtils
          .waitForCompactionStartAndReturnEcids(getCluster().getServerContext(), tid);

      // Confirm that this ECID shows up in RUNNING set
      int matches = ExternalCompactionTestUtils
          .confirmCompactionRunning(getCluster().getServerContext(), ecids);
      assertTrue(matches > 0);

      client.tableOperations().delete(table1);

      ExternalCompactionTestUtils.confirmCompactionCompleted(getCluster().getServerContext(), ecids,
          TCompactionState.CANCELLED);

      TabletsMetadata tm = getCluster().getServerContext().getAmple().readTablets().forTable(tid)
          .fetch(ColumnType.ECOMP).build();
      assertEquals(0, tm.stream().count());
      tm.close();

    }
  }

}

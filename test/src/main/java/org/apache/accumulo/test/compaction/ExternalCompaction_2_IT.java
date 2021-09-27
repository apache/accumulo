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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.stream.Collectors;

import org.apache.accumulo.compactor.Compactor;
import org.apache.accumulo.coordinator.ExternalCompactionMetrics;
import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.clientImpl.Tables;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.metadata.schema.Ample.DataLevel;
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
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ExternalCompaction_2_IT extends AccumuloClusterHarness
    implements MiniClusterConfigurationCallback {

  private static final Logger LOG = LoggerFactory.getLogger(ExternalCompaction_2_IT.class);

  @Override
  public void configureMiniCluster(MiniAccumuloConfigImpl cfg, Configuration coreSite) {
    ExternalCompactionUtils.configureMiniCluster(cfg, coreSite);
  }

  private ProcessInfo testCompactor = null;
  private ProcessInfo testCoordinator = null;

  @Before
  public void setUp() throws Exception {
    super.setupCluster();
    testCompactor = ((MiniAccumuloClusterImpl) getCluster()).exec(ExternalDoNothingCompactor.class,
        "-q", "DCQ1");
    testCoordinator = ExternalCompactionUtils.startCoordinator(
        ((MiniAccumuloClusterImpl) getCluster()), TestCompactionCoordinator.class);
  }

  @After
  public void tearDown() throws Exception {
    super.teardownCluster();
    ExternalCompactionUtils.stopProcesses(testCompactor, testCoordinator);
    testCompactor = null;
    testCoordinator = null;
  }

  @Test
  public void testSplitDuringExternalCompaction() throws Exception {
    String table1 = this.getUniqueNames(1)[0];
    try (AccumuloClient client =
        Accumulo.newClient().from(getCluster().getClientProperties()).build()) {

      ExternalCompactionMetrics startingMetrics = ExternalCompactionUtils.getCoordinatorMetrics();

      ExternalCompactionUtils.createTable(client, table1, "cs1");
      TableId tid = Tables.getTableId(getCluster().getServerContext(), table1);
      ExternalCompactionUtils.writeData(client, table1);
      ExternalCompactionUtils.compact(client, table1, 2, "DCQ1", false);

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
      int jump = ExternalCompactionUtils.MAX_DATA / 5;
      for (int r = jump; r < ExternalCompactionUtils.MAX_DATA; r += jump) {
        splits.add(new Text(ExternalCompactionUtils.row(r)));
      }

      client.tableOperations().addSplits(table1, splits);

      // wait for failure or test timeout
      ExternalCompactionMetrics metrics = ExternalCompactionUtils.getCoordinatorMetrics();
      while (metrics.getFailed() == startingMetrics.getFailed()) {
        UtilWaitThread.sleep(250);
        metrics = ExternalCompactionUtils.getCoordinatorMetrics();
      }

      // Check that there is one failed compaction in the coordinator metrics
      assertTrue(metrics.getStarted() > 0);
      assertEquals(startingMetrics.getCompleted(), metrics.getCompleted());
      assertTrue(metrics.getFailed() > startingMetrics.getFailed());

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
  public void testExternalCompactionsRunWithTableOffline() throws Exception {
    ExternalCompactionMetrics startingMetrics = ExternalCompactionUtils.getCoordinatorMetrics();
    ExternalCompactionUtils.stopProcesses(testCoordinator, testCompactor);
    String table1 = this.getUniqueNames(1)[0];
    try (AccumuloClient client =
        Accumulo.newClient().from(getCluster().getClientProperties()).build()) {

      ExternalCompactionUtils.createTable(client, table1, "cs1");
      // set compaction ratio to 1 so that majc occurs naturally, not user compaction
      // user compaction blocks merge
      client.tableOperations().setProperty(table1, Property.TABLE_MAJC_RATIO.toString(), "1.0");
      // cause multiple rfiles to be created
      ExternalCompactionUtils.writeData(client, table1);
      ExternalCompactionUtils.writeData(client, table1);
      ExternalCompactionUtils.writeData(client, table1);
      ExternalCompactionUtils.writeData(client, table1);

      ProcessInfo coord = ExternalCompactionUtils.startCoordinator(
          ((MiniAccumuloClusterImpl) getCluster()), TestCompactionCoordinatorForOfflineTable.class);

      ExternalCompactionMetrics metrics = ExternalCompactionUtils.getCoordinatorMetrics();

      final long started = metrics.getStarted();
      // Offline the table when the compaction starts
      Thread t = new Thread(() -> {
        try {
          ExternalCompactionMetrics metrics2 = ExternalCompactionUtils.getCoordinatorMetrics();
          while (metrics2.getStarted() == started) {
            metrics2 = ExternalCompactionUtils.getCoordinatorMetrics();
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
      assertEquals(0, ExternalCompactionUtils.getFinalStatesForTable(getCluster(), tid).count());

      // Start the compactor
      ProcessInfo comp =
          ((MiniAccumuloClusterImpl) getCluster()).exec(Compactor.class, "-q", "DCQ1");

      t.join();

      // wait for completed or test timeout
      ExternalCompactionMetrics metrics3 = ExternalCompactionUtils.getCoordinatorMetrics();
      while (metrics3.getCompleted() == metrics.getCompleted()) {
        UtilWaitThread.sleep(250);
        metrics3 = ExternalCompactionUtils.getCoordinatorMetrics();
      }

      // Confirm that final state is in the metadata table
      assertEquals(1, ExternalCompactionUtils.getFinalStatesForTable(getCluster(), tid).count());

      // Online the table
      client.tableOperations().online(table1);

      // wait for compaction to be committed by tserver or test timeout
      long finalStateCount =
          ExternalCompactionUtils.getFinalStatesForTable(getCluster(), tid).count();
      while (finalStateCount > 0) {
        UtilWaitThread.sleep(250);
        finalStateCount = ExternalCompactionUtils.getFinalStatesForTable(getCluster(), tid).count();
      }

      // Check that the compaction succeeded
      metrics = ExternalCompactionUtils.getCoordinatorMetrics();
      assertTrue(metrics.getStarted() > startingMetrics.getStarted());
      assertTrue(metrics.getCompleted() > startingMetrics.getCompleted());
      assertEquals(metrics.getFailed(), startingMetrics.getFailed());
      assertEquals(0, metrics.getRunning());

      ExternalCompactionUtils.stopProcesses(comp, coord);
    }
  }

  @Test
  public void testUserCompactionCancellation() throws Exception {
    String table1 = this.getUniqueNames(1)[0];
    try (AccumuloClient client =
        Accumulo.newClient().from(getCluster().getClientProperties()).build()) {

      ExternalCompactionMetrics startingMetrics = ExternalCompactionUtils.getCoordinatorMetrics();

      ExternalCompactionUtils.createTable(client, table1, "cs1");
      ExternalCompactionUtils.writeData(client, table1);
      ExternalCompactionUtils.compact(client, table1, 2, "DCQ1", false);

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

      client.tableOperations().cancelCompaction(table1);

      // wait for failure or test timeout
      ExternalCompactionMetrics metrics = ExternalCompactionUtils.getCoordinatorMetrics();
      while (metrics.getFailed() == 0) {
        UtilWaitThread.sleep(250);
        metrics = ExternalCompactionUtils.getCoordinatorMetrics();
      }

      // Confirm that compaction failed
      assertTrue(metrics.getStarted() > startingMetrics.getStarted());
      assertEquals(0, metrics.getRunning());
      assertEquals(startingMetrics.getCompleted(), metrics.getCompleted());
      assertTrue(metrics.getFailed() > startingMetrics.getFailed());
    }
  }

  @Test
  public void testDeleteTableDuringUserExternalCompaction() throws Exception {
    String table1 = this.getUniqueNames(1)[0];
    try (AccumuloClient client =
        Accumulo.newClient().from(getCluster().getClientProperties()).build()) {

      ExternalCompactionMetrics startingMetrics = ExternalCompactionUtils.getCoordinatorMetrics();

      ExternalCompactionUtils.createTable(client, table1, "cs1");
      ExternalCompactionUtils.writeData(client, table1);
      ExternalCompactionUtils.compact(client, table1, 2, "DCQ1", false);

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

      client.tableOperations().delete(table1);

      // wait for failure or test timeout
      ExternalCompactionMetrics metrics = ExternalCompactionUtils.getCoordinatorMetrics();
      while (metrics.getFailed() == startingMetrics.getFailed()) {
        UtilWaitThread.sleep(250);
        metrics = ExternalCompactionUtils.getCoordinatorMetrics();
      }

      // Confirm that compaction failed
      assertTrue(metrics.getStarted() > startingMetrics.getStarted());
      assertEquals(startingMetrics.getCompleted(), metrics.getCompleted());
      assertTrue(metrics.getFailed() > startingMetrics.getFailed());
    }
  }

  @Test
  public void testDeleteTableDuringExternalCompaction() throws Exception {
    String table1 = this.getUniqueNames(1)[0];
    try (AccumuloClient client =
        Accumulo.newClient().from(getCluster().getClientProperties()).build()) {

      ExternalCompactionMetrics startingMetrics = ExternalCompactionUtils.getCoordinatorMetrics();

      ExternalCompactionUtils.createTable(client, table1, "cs1");
      // set compaction ratio to 1 so that majc occurs naturally, not user compaction
      // user compaction blocks delete
      client.tableOperations().setProperty(table1, Property.TABLE_MAJC_RATIO.toString(), "1.0");
      // cause multiple rfiles to be created
      ExternalCompactionUtils.writeData(client, table1);
      ExternalCompactionUtils.writeData(client, table1);
      ExternalCompactionUtils.writeData(client, table1);
      ExternalCompactionUtils.writeData(client, table1);

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

      client.tableOperations().delete(table1);

      // wait for failure or test timeout
      ExternalCompactionMetrics metrics = ExternalCompactionUtils.getCoordinatorMetrics();
      while (metrics.getFailed() == startingMetrics.getFailed()) {
        UtilWaitThread.sleep(50);
        metrics = ExternalCompactionUtils.getCoordinatorMetrics();
      }

      tm = getCluster().getServerContext().getAmple().readTablets().forTable(tid)
          .fetch(ColumnType.ECOMP).build();
      assertEquals(0, tm.stream().count());
      tm.close();

      // The metadata tablets will be deleted from the metadata table because we have deleted the
      // table. Verify that the compaction failed by looking at the metrics in the Coordinator.
      assertTrue(metrics.getStarted() > startingMetrics.getStarted());
      assertEquals(0, metrics.getRunning());
      assertEquals(startingMetrics.getCompleted(), metrics.getCompleted());
      assertTrue(metrics.getFailed() > startingMetrics.getFailed());
    }
  }

}

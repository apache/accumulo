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

import static org.apache.accumulo.test.compaction.ExternalCompactionTestUtils.GROUP1;
import static org.apache.accumulo.test.compaction.ExternalCompactionTestUtils.GROUP3;
import static org.apache.accumulo.test.compaction.ExternalCompactionTestUtils.GROUP4;
import static org.apache.accumulo.test.compaction.ExternalCompactionTestUtils.MAX_DATA;
import static org.apache.accumulo.test.compaction.ExternalCompactionTestUtils.compact;
import static org.apache.accumulo.test.compaction.ExternalCompactionTestUtils.confirmCompactionCompleted;
import static org.apache.accumulo.test.compaction.ExternalCompactionTestUtils.confirmCompactionRunning;
import static org.apache.accumulo.test.compaction.ExternalCompactionTestUtils.countTablets;
import static org.apache.accumulo.test.compaction.ExternalCompactionTestUtils.createTable;
import static org.apache.accumulo.test.compaction.ExternalCompactionTestUtils.row;
import static org.apache.accumulo.test.compaction.ExternalCompactionTestUtils.waitForCompactionStartAndReturnEcids;
import static org.apache.accumulo.test.compaction.ExternalCompactionTestUtils.writeData;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Collections;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.clientImpl.TableOperationsImpl;
import org.apache.accumulo.core.compaction.thrift.TCompactionState;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.metadata.schema.ExternalCompactionId;
import org.apache.accumulo.core.metadata.schema.MetadataSchema;
import org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType;
import org.apache.accumulo.core.metadata.schema.TabletsMetadata;
import org.apache.accumulo.harness.MiniClusterConfigurationCallback;
import org.apache.accumulo.harness.SharedMiniClusterBase;
import org.apache.accumulo.minicluster.ServerType;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloConfigImpl;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.util.FindCompactionTmpFiles;
import org.apache.accumulo.test.util.Wait;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;

public class ExternalCompaction_2_IT extends SharedMiniClusterBase {

  public static class ExternalCompaction2Config implements MiniClusterConfigurationCallback {
    @Override
    public void configureMiniCluster(MiniAccumuloConfigImpl cfg, Configuration coreSite) {
      ExternalCompactionTestUtils.configureMiniCluster(cfg, coreSite);
    }
  }

  @BeforeAll
  public static void beforeTests() throws Exception {
    startMiniClusterWithConfig(new ExternalCompaction2Config());
    getCluster().getClusterControl().stop(ServerType.COMPACTOR);
    getCluster().getClusterControl().start(ServerType.COMPACTOR, null, 1,
        ExternalDoNothingCompactor.class);
  }

  @Test
  public void testSplitCancelsExternalCompaction() throws Exception {

    String table1 = this.getUniqueNames(1)[0];
    try (AccumuloClient client =
        Accumulo.newClient().from(getCluster().getClientProperties()).build()) {

      createTable(client, table1, "cs1");
      TableId tid = getCluster().getServerContext().getTableId(table1);
      writeData(client, table1);
      compact(client, table1, 2, GROUP1, false);

      // Wait for the compaction to start by waiting for 1 external compaction column
      Set<ExternalCompactionId> ecids = ExternalCompactionTestUtils
          .waitForCompactionStartAndReturnEcids(getCluster().getServerContext(), tid);

      // Confirm that this ECID shows up in RUNNING set
      int matches = ExternalCompactionTestUtils
          .confirmCompactionRunning(getCluster().getServerContext(), ecids);
      assertTrue(matches > 0);

      // Verify that a tmp file is created
      Wait.waitFor(() -> FindCompactionTmpFiles
          .findTempFiles(getCluster().getServerContext(), tid.canonical()).size() == 1);

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

      // Verify that the tmp file are cleaned up
      Wait.waitFor(() -> FindCompactionTmpFiles
          .findTempFiles(getCluster().getServerContext(), tid.canonical()).size() == 0, 60_000);
    }
  }

  @Test
  public void testUserCompactionCancellation() throws Exception {

    String table1 = this.getUniqueNames(1)[0];
    try (AccumuloClient client =
        Accumulo.newClient().from(getCluster().getClientProperties()).build()) {

      createTable(client, table1, "cs3");
      TableId tid = getCluster().getServerContext().getTableId(table1);
      writeData(client, table1);

      AtomicReference<Throwable> error = new AtomicReference<>();
      Thread t = new Thread(() -> {
        try {
          compact(client, table1, 2, GROUP3, true);
        } catch (AccumuloSecurityException | TableNotFoundException | AccumuloException e) {
          error.set(e);
        }
      });
      t.start();

      // Wait for the compaction to start by waiting for 1 external compaction column
      Set<ExternalCompactionId> ecids = ExternalCompactionTestUtils
          .waitForCompactionStartAndReturnEcids(getCluster().getServerContext(), tid);

      // Confirm that this ECID shows up in RUNNING set
      int matches = ExternalCompactionTestUtils
          .confirmCompactionRunning(getCluster().getServerContext(), ecids);
      assertTrue(matches > 0);

      // Verify that a tmp file is created
      Wait.waitFor(() -> FindCompactionTmpFiles
          .findTempFiles(getCluster().getServerContext(), tid.canonical()).size() == 1);

      // when the compaction starts it will create a selected files column in the tablet, wait for
      // that to happen
      while (countTablets(getCluster().getServerContext(), table1,
          tm -> tm.getSelectedFiles() != null) == 0) {
        Thread.sleep(1000);
      }

      client.tableOperations().cancelCompaction(table1);

      t.join();
      Throwable e = error.get();
      assertNotNull(e);
      assertEquals(TableOperationsImpl.COMPACTION_CANCELED_MSG, e.getMessage());

      confirmCompactionCompleted(getCluster().getServerContext(), ecids,
          TCompactionState.CANCELLED);

      // ensure the canceled compaction deletes any tablet metadata related to the compaction
      while (countTablets(getCluster().getServerContext(), table1,
          tm -> tm.getSelectedFiles() != null || !tm.getCompacted().isEmpty()) > 0) {
        Thread.sleep(1000);
      }

      // Verify that the tmp file are cleaned up
      Wait.waitFor(() -> FindCompactionTmpFiles
          .findTempFiles(getCluster().getServerContext(), tid.canonical()).size() == 0);
    }
  }

  @Test
  public void testDeleteTableCancelsUserExternalCompaction() throws Exception {

    String table1 = this.getUniqueNames(1)[0];
    try (AccumuloClient client =
        Accumulo.newClient().from(getCluster().getClientProperties()).build()) {

      createTable(client, table1, "cs4");
      TableId tid = getCluster().getServerContext().getTableId(table1);
      writeData(client, table1);
      compact(client, table1, 2, GROUP4, false);

      // Wait for the compaction to start by waiting for 1 external compaction column
      Set<ExternalCompactionId> ecids =
          waitForCompactionStartAndReturnEcids(getCluster().getServerContext(), tid);

      // Confirm that this ECID shows up in RUNNING set
      int matches = confirmCompactionRunning(getCluster().getServerContext(), ecids);
      assertTrue(matches > 0);

      // when the compaction starts it will create a selected files column in the tablet, wait for
      // that to happen
      while (countTablets(getCluster().getServerContext(), table1,
          tm -> tm.getSelectedFiles() != null) == 0) {
        Thread.sleep(1000);
      }

      client.tableOperations().delete(table1);

      confirmCompactionCompleted(getCluster().getServerContext(), ecids,
          TCompactionState.CANCELLED);

      // ELASTICITY_TODO make delete table fate op get operation ids before deleting
      // there should be no metadata for the table, check to see if the compaction wrote anything
      // after table delete
      try (var scanner = client.createScanner(MetadataTable.NAME)) {
        scanner.setRange(MetadataSchema.TabletsSection.getRange(tid));
        assertEquals(0, scanner.stream().count());
      }

    }
  }

  @Test
  public void testDeleteTableCancelsExternalCompaction() throws Exception {

    String table1 = this.getUniqueNames(1)[0];
    try (AccumuloClient client =
        Accumulo.newClient().from(getCluster().getClientProperties()).build()) {

      createTable(client, table1, "cs5");

      ServerContext ctx = getCluster().getServerContext();
      TableId tid = ctx.getTableId(table1);

      // set compaction ratio to 1 so that majc occurs naturally, not user compaction
      // user compaction blocks delete
      client.tableOperations().setProperty(table1, Property.TABLE_MAJC_RATIO.toString(), "1.0");

      AtomicReference<Throwable> error = new AtomicReference<>();
      CountDownLatch latch = new CountDownLatch(1);
      Runnable r = () -> {
        try {
          // cause multiple rfiles to be created
          latch.countDown();
          writeData(client, table1);
          writeData(client, table1);
          writeData(client, table1);
          writeData(client, table1);
        } catch (TableNotFoundException | AccumuloException | AccumuloSecurityException e) {
          error.set(e);
        }
      };
      Thread t = new Thread(r);
      t.start();
      latch.await();
      // Wait for the compaction to start by waiting for 1 external compaction column
      Set<ExternalCompactionId> ecids =
          ExternalCompactionTestUtils.waitForCompactionStartAndReturnEcids(ctx, tid);

      // Confirm that this ECID shows up in RUNNING set
      int matches = ExternalCompactionTestUtils.confirmCompactionRunning(ctx, ecids);
      assertTrue(matches > 0);

      client.tableOperations().delete(table1);

      LoggerFactory.getLogger(getClass()).debug("Table deleted.");

      confirmCompactionCompleted(ctx, ecids, TCompactionState.CANCELLED);

      LoggerFactory.getLogger(getClass()).debug("Confirmed compaction cancelled.");

      TabletsMetadata tm =
          ctx.getAmple().readTablets().forTable(tid).fetch(ColumnType.PREV_ROW).build();
      assertEquals(0, tm.stream().count());
      tm.close();

      t.join();
      assertNull(error.get());
    }
  }

}

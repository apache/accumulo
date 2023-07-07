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
import static org.apache.accumulo.test.compaction.ExternalCompactionTestUtils.GROUP5;
import static org.apache.accumulo.test.compaction.ExternalCompactionTestUtils.MAX_DATA;
import static org.apache.accumulo.test.compaction.ExternalCompactionTestUtils.compact;
import static org.apache.accumulo.test.compaction.ExternalCompactionTestUtils.confirmCompactionCompleted;
import static org.apache.accumulo.test.compaction.ExternalCompactionTestUtils.confirmCompactionRunning;
import static org.apache.accumulo.test.compaction.ExternalCompactionTestUtils.createTable;
import static org.apache.accumulo.test.compaction.ExternalCompactionTestUtils.row;
import static org.apache.accumulo.test.compaction.ExternalCompactionTestUtils.waitForCompactionStartAndReturnEcids;
import static org.apache.accumulo.test.compaction.ExternalCompactionTestUtils.writeData;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Collections;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.stream.Collectors;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.compaction.thrift.TCompactionState;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.metadata.schema.ExternalCompactionId;
import org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType;
import org.apache.accumulo.core.metadata.schema.TabletsMetadata;
import org.apache.accumulo.harness.MiniClusterConfigurationCallback;
import org.apache.accumulo.harness.SharedMiniClusterBase;
import org.apache.accumulo.minicluster.ServerType;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloConfigImpl;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

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
  }

  @AfterEach
  public void tearDown() throws Exception {
    // The ExternalDoNothingCompactor needs to be restarted between tests
    getCluster().getClusterControl().stop(ServerType.COMPACTOR);
    getCluster().getConfig().getClusterServerConfiguration().clearCompactorResourceGroups();
  }

  @Test
  public void testSplitCancelsExternalCompaction() throws Exception {

    getCluster().getConfig().getClusterServerConfiguration().addCompactorResourceGroup(GROUP1, 1);
    getCluster().getClusterControl().start(ServerType.COMPACTOR, null, 1,
        ExternalDoNothingCompactor.class);

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
  public void testUserCompactionCancellation() throws Exception {

    getCluster().getConfig().getClusterServerConfiguration().addCompactorResourceGroup(GROUP3, 1);
    getCluster().getClusterControl().start(ServerType.COMPACTOR, null, 1,
        ExternalDoNothingCompactor.class);

    String table1 = this.getUniqueNames(1)[0];
    try (AccumuloClient client =
        Accumulo.newClient().from(getCluster().getClientProperties()).build()) {

      createTable(client, table1, "cs3");
      TableId tid = getCluster().getServerContext().getTableId(table1);
      writeData(client, table1);
      compact(client, table1, 2, GROUP3, false);

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

    getCluster().getConfig().getClusterServerConfiguration().addCompactorResourceGroup(GROUP4, 1);
    getCluster().getClusterControl().start(ServerType.COMPACTOR, null, 1,
        ExternalDoNothingCompactor.class);

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

      client.tableOperations().delete(table1);

      confirmCompactionCompleted(getCluster().getServerContext(), ecids,
          TCompactionState.CANCELLED);

    }
  }

  @Test
  public void testDeleteTableCancelsExternalCompaction() throws Exception {

    getCluster().getConfig().getClusterServerConfiguration().addCompactorResourceGroup(GROUP5, 1);
    getCluster().getClusterControl().start(ServerType.COMPACTOR, null, 1,
        ExternalDoNothingCompactor.class);

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

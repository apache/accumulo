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
package org.apache.accumulo.test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.accumulo.coordinator.CompactionCoordinator;
import org.apache.accumulo.coordinator.ExternalCompactionMetrics;
import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.clientImpl.Tables;
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
import org.junit.Test;

public class ExternalCompaction_3_IT extends AccumuloClusterHarness
    implements MiniClusterConfigurationCallback {

  @Override
  public void configureMiniCluster(MiniAccumuloConfigImpl cfg, Configuration coreSite) {
    ExternalCompactionUtils.configureMiniCluster(cfg, coreSite);
  }

  @Test
  public void testMergeDuringExternalCompaction() throws Exception {
    String table1 = this.getUniqueNames(1)[0];
    try (AccumuloClient client =
        Accumulo.newClient().from(getCluster().getClientProperties()).build()) {

      ExternalCompactionUtils.createTable(client, table1, "cs1", 2);
      // set compaction ratio to 1 so that majc occurs naturally, not user compaction
      // user compaction blocks merge
      client.tableOperations().setProperty(table1, Property.TABLE_MAJC_RATIO.toString(), "1.0");
      // cause multiple rfiles to be created
      ExternalCompactionUtils.writeData(client, table1);
      ExternalCompactionUtils.writeData(client, table1);
      ExternalCompactionUtils.writeData(client, table1);
      ExternalCompactionUtils.writeData(client, table1);

      TableId tid = Tables.getTableId(getCluster().getServerContext(), table1);

      ((MiniAccumuloClusterImpl) getCluster()).exec(TestCompactionCoordinator.class);
      // Wait for coordinator to start
      ExternalCompactionMetrics metrics = null;
      while (null == metrics) {
        try {
          metrics = ExternalCompactionUtils.getCoordinatorMetrics();
        } catch (Exception e) {
          UtilWaitThread.sleep(250);
        }
      }

      ((MiniAccumuloClusterImpl) getCluster()).exec(ExternalDoNothingCompactor.class, "-q", "DCQ1");

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

      var md = new ArrayList<TabletMetadata>();
      try (TabletsMetadata tm = getCluster().getServerContext().getAmple().readTablets()
          .forTable(tid).fetch(ColumnType.PREV_ROW).build()) {
        tm.forEach(t -> md.add(t));
        assertEquals(2, md.size());
      }

      assertTrue(ExternalCompactionUtils.getCoordinatorMetrics().getFailed() == 0);

      // Merge - blocking operation
      Text start = md.get(0).getPrevEndRow();
      Text end = md.get(1).getEndRow();
      client.tableOperations().merge(table1, start, end);

      // wait for failure or test timeout
      metrics = ExternalCompactionUtils.getCoordinatorMetrics();
      while (metrics.getFailed() == 0) {
        UtilWaitThread.sleep(250);
        metrics = ExternalCompactionUtils.getCoordinatorMetrics();
      }

      // Check that there is one more failed compaction in the coordinator metrics
      assertTrue(metrics.getStarted() > 0);
      assertEquals(0, metrics.getCompleted());
      assertTrue(metrics.getFailed() > 0);

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

      ExternalCompactionUtils.createTable(client, table1, "cs1", 2);
      ExternalCompactionUtils.writeData(client, table1);

      ProcessInfo coord = ExternalCompactionUtils
          .startCoordinator(((MiniAccumuloClusterImpl) getCluster()), CompactionCoordinator.class);
      ProcessInfo compactor = ((MiniAccumuloClusterImpl) getCluster())
          .exec(ExternalDoNothingCompactor.class, "-q", "DCQ1");
      ExternalCompactionUtils.compact(client, table1, 2, "DCQ1", false);
      TableId tid = Tables.getTableId(getCluster().getServerContext(), table1);
      // Wait for the compaction to start by waiting for 1 external compaction column
      Set<ExternalCompactionId> ecids = new HashSet<>();
      do {
        // getCluster().getServerContext().getAmple().readTablets().forTable(tid).saveKeyValues().build().forEach(tm
        // -> tm.getKeyValues().forEach((k,v) -> System.out.println(k + " -> " + v)));
        try (TabletsMetadata tm = getCluster().getServerContext().getAmple().readTablets()
            .forTable(tid).fetch(ColumnType.ECOMP).build()) {
          tm.stream().flatMap(t -> t.getExternalCompactions().keySet().stream())
              .forEach(ecids::add);
          UtilWaitThread.sleep(50);
        }
      } while (ecids.isEmpty());

      // Stop the Coordinator
      ExternalCompactionUtils.stopProcesses(coord);

      // Start the TestCompactionCoordinator so that we have
      // access to the metrics.
      ProcessInfo testCoordinator = ExternalCompactionUtils.startCoordinator(
          ((MiniAccumuloClusterImpl) getCluster()), TestCompactionCoordinator.class);

      // wait for failure or test timeout
      ExternalCompactionMetrics metrics = ExternalCompactionUtils.getCoordinatorMetrics();
      while (metrics.getRunning() == 0) {
        UtilWaitThread.sleep(250);
        metrics = ExternalCompactionUtils.getCoordinatorMetrics();
      }
      ExternalCompactionUtils.stopProcesses(testCoordinator, compactor);

    }
  }

}

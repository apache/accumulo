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
package org.apache.accumulo.test.functional;

import static com.google.common.collect.MoreCollectors.onlyElement;
import static org.apache.accumulo.core.metadata.schema.Ample.ConditionalResult.Status.ACCEPTED;
import static org.apache.accumulo.core.metadata.schema.Ample.ConditionalResult.Status.REJECTED;
import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.SUSPEND;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.List;
import java.util.Map;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.admin.NewTableConfiguration;
import org.apache.accumulo.core.client.admin.TabletAvailability;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.metadata.schema.TabletMetadata;
import org.apache.accumulo.core.metadata.schema.TabletsMetadata;
import org.apache.accumulo.core.spi.balancer.TableLoadBalancer;
import org.apache.accumulo.minicluster.ServerType;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloClusterControl;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloConfigImpl;
import org.apache.accumulo.test.util.Wait;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Test;

public class SuspendMarkerIT extends ConfigurableMacBase {

  final String SUSPEND_RG = "SUSPEND";

  @Override
  protected void configure(MiniAccumuloConfigImpl cfg, Configuration hadoopCoreSite) {
    cfg.getClusterServerConfiguration().setNumDefaultTabletServers(1);
    cfg.getClusterServerConfiguration().addTabletServerResourceGroup(SUSPEND_RG, 1);

  }

  @Test
  public void testSuspendMarker() throws Exception {
    try (AccumuloClient c = Accumulo.newClient().from(getClientProperties()).build()) {

      MiniAccumuloClusterControl clusterControl = getCluster().getClusterControl();

      List<Process> processes = clusterControl.getTabletServers(SUSPEND_RG);
      assertNotNull(processes);
      assertEquals(1, processes.size());

      String tableName = getUniqueNames(1)[0];
      NewTableConfiguration ntc = new NewTableConfiguration();
      ntc.withInitialTabletAvailability(TabletAvailability.HOSTED);
      ntc.setProperties(Map.of(Property.TABLE_SUSPEND_DURATION.getKey(), "30s",
          TableLoadBalancer.TABLE_ASSIGNMENT_GROUP_PROPERTY, SUSPEND_RG));
      c.tableOperations().create(tableName, ntc);

      c.instanceOperations().waitForBalance();

      TableId suspendTableTid = TableId.of(c.tableOperations().tableIdMap().get(tableName));

      TabletMetadata originalTM;
      try (TabletsMetadata tms = TabletsMetadata.builder(c).forTable(suspendTableTid).build()) {
        originalTM = tms.stream().collect(onlyElement());
        assertNull(originalTM.getSuspend());
      }

      clusterControl.stopTabletServerGroup(SUSPEND_RG);

      Wait.waitFor(() -> suspendColumnExists(c, suspendTableTid), 60_000);

      try (var tabletsMutator =
          getCluster().getServerContext().getAmple().conditionallyMutateTablets()) {
        tabletsMutator.mutateTablet(originalTM.getExtent()).requireAbsentOperation()
            .requireSame(originalTM, SUSPEND).putTabletAvailability(TabletAvailability.ONDEMAND)
            .submit(tabletMetadata -> false);

        // This should fail because the original tablet metadata does not have a suspend column
        // and the current tablet metadata does.
        assertEquals(REJECTED, tabletsMutator.process().get(originalTM.getExtent()).getStatus());
      }

      // test require same when the tablet metadata passed in has a suspend column set
      var suspendedTM =
          getCluster().getServerContext().getAmple().readTablet(originalTM.getExtent());
      assertNotNull(suspendedTM.getSuspend());
      try (var tabletsMutator =
          getCluster().getServerContext().getAmple().conditionallyMutateTablets()) {
        tabletsMutator.mutateTablet(originalTM.getExtent()).requireAbsentOperation()
            .requireSame(suspendedTM, SUSPEND).putFlushNonce(6789).submit(tabletMetadata -> false);

        // This should succeed because the tablet metadata does have a suspend column and the
        // current tablet metadata does.
        assertEquals(ACCEPTED, tabletsMutator.process().get(originalTM.getExtent()).getStatus());
      }

      clusterControl.start(ServerType.TABLET_SERVER);

      Wait.waitFor(() -> !suspendColumnExists(c, suspendTableTid), 60_000);

      try (var tabletsMutator =
          getCluster().getServerContext().getAmple().conditionallyMutateTablets()) {
        tabletsMutator.mutateTablet(originalTM.getExtent()).requireAbsentOperation()
            .requireSame(originalTM, SUSPEND).putTabletAvailability(TabletAvailability.ONDEMAND)
            .submit(tabletMetadata -> false);

        // This should succeed because the original tablet metadata does not have a suspend column
        // and the current tablet metadata does not also because the tablet server for the SUSPEND
        // resource group was restarted.
        assertEquals(ACCEPTED, tabletsMutator.process().get(originalTM.getExtent()).getStatus());
      }
    }
  }

  private static boolean suspendColumnExists(AccumuloClient c, TableId tid) {
    try (TabletsMetadata tms = TabletsMetadata.builder(c).forTable(tid).build()) {
      TabletMetadata tm = tms.stream().collect(onlyElement());
      return tm.getSuspend() != null;
    }
  }
}

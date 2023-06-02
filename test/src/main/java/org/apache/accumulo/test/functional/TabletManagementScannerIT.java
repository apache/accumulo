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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.Collections;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.stream.IntStream;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.admin.Locations;
import org.apache.accumulo.core.client.admin.NewTableConfiguration;
import org.apache.accumulo.core.client.admin.TabletHostingGoal;
import org.apache.accumulo.core.clientImpl.ClientContext;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.manager.state.TabletManagement;
import org.apache.accumulo.core.manager.state.TabletManagement.ManagementAction;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.metadata.RootTable;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection;
import org.apache.accumulo.core.metadata.schema.TabletMetadata;
import org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType;
import org.apache.accumulo.core.metadata.schema.TabletsMetadata;
import org.apache.accumulo.core.spi.ondemand.DefaultOnDemandTabletUnloader;
import org.apache.accumulo.harness.MiniClusterConfigurationCallback;
import org.apache.accumulo.harness.SharedMiniClusterBase;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloConfigImpl;
import org.apache.accumulo.server.manager.state.TabletManagementScanner;
import org.apache.accumulo.test.util.Wait;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.google.common.collect.Iterators;

public class TabletManagementScannerIT extends SharedMiniClusterBase {

  public static class TMSIT_Config implements MiniClusterConfigurationCallback {

    @Override
    public void configureMiniCluster(MiniAccumuloConfigImpl cfg, Configuration coreSite) {
      cfg.setNumTservers(1);
      cfg.setProperty(Property.TSERV_ASSIGNMENT_MAXCONCURRENT, "10");
      cfg.setProperty(Property.GENERAL_THREADPOOL_SIZE, "10");
      cfg.setProperty(Property.MANAGER_TABLET_GROUP_WATCHER_INTERVAL, "180");
      cfg.setProperty(Property.TSERV_ONDEMAND_UNLOADER_INTERVAL, "10s");
      cfg.setProperty(DefaultOnDemandTabletUnloader.INACTIVITY_THRESHOLD, "15");
    }
  }

  @Override
  protected Duration defaultTimeout() {
    return Duration.ofMinutes(2);
  }

  @BeforeAll
  public static void beforeAll() throws Exception {
    SharedMiniClusterBase.startMiniClusterWithConfig(new TMSIT_Config());
  }

  @AfterAll
  public static void afterAll() throws Exception {
    SharedMiniClusterBase.stopMiniCluster();
  }

  @Test
  public void testKnownTabletModificationsTakePriority() throws Exception {

    final ConcurrentLinkedQueue<TabletManagement> mods = new ConcurrentLinkedQueue<>();

    try (AccumuloClient c = Accumulo.newClient().from(getClientProps()).build()) {

      // Confirm that the root and metadata tables are hosted
      Locations rootLocations =
          c.tableOperations().locate(RootTable.NAME, Collections.singletonList(new Range()));
      rootLocations.groupByTablet().keySet()
          .forEach(tid -> assertNotNull(rootLocations.getTabletLocation(tid)));

      Locations metadataLocations =
          c.tableOperations().locate(MetadataTable.NAME, Collections.singletonList(new Range()));
      metadataLocations.groupByTablet().keySet()
          .forEach(tid -> assertNotNull(metadataLocations.getTabletLocation(tid)));

      String tableName = super.getUniqueNames(1)[0];
      NewTableConfiguration ntc = new NewTableConfiguration();
      SortedSet<Text> splits = new TreeSet<>();
      IntStream.range(97, 122).forEach((i) -> {
        splits.add(new Text(String.valueOf((char) i)));
      });
      ntc.withSplits(splits);
      c.tableOperations().create(tableName, ntc);

      TableId tableId = TableId.of(c.tableOperations().tableIdMap().get(tableName));

      // wait for the tablets to exist in the metadata table. The tablets
      // will not be hosted so the current location will be empty.
      try (TabletManagementScanner s = new TabletManagementScanner((ClientContext) c,
          new Range(TabletsSection.encodeRow(tableId, null)), null, MetadataTable.NAME, mods)) {
        Wait.waitFor(() -> Iterators.size(s) == 26, 10000, 250);
      }

      // set the tablet hosting goal on the first half of the tablets
      // so they will need a location update action
      c.tableOperations().setTabletHostingGoal(tableName, new Range(null, "m"),
          TabletHostingGoal.ALWAYS);

      // let's add a known modification for the last tablet
      TabletsMetadata tms = getCluster().getServerContext().getAmple().readTablets()
          .forTablet(new KeyExtent(tableId, null, new Text("y")))
          .fetch(ColumnType.PREV_ROW, ColumnType.LOCATION, ColumnType.SUSPEND, ColumnType.LOGS,
              ColumnType.HOSTING_GOAL, ColumnType.HOSTING_REQUESTED, ColumnType.FILES,
              ColumnType.LAST, ColumnType.OPID)
          .build();
      assertEquals(1, Iterators.size(tms.iterator()));
      TabletMetadata tm = tms.iterator().next();
      mods.add(new TabletManagement(EnumSet.of(ManagementAction.NEEDS_SPLITTING), tm));

      // check the contents of the TabletManagementScanner
      try (TabletManagementScanner s = new TabletManagementScanner((ClientContext) c,
          new Range(TabletsSection.getRange(tableId)), null, MetadataTable.NAME, mods)) {

        @SuppressWarnings("resource")
        Iterator<TabletManagement> iter = s;
        // Confirm that the first object is the one we added to the known mods set.
        assertFalse(mods.isEmpty());
        assertTrue(iter.hasNext());
        TabletManagement tman = iter.next();
        assertEquals(1, tman.getActions().size());
        assertTrue(tman.getActions().contains(ManagementAction.NEEDS_SPLITTING));
        assertEquals(tm, tman.getTabletMetadata());

        assertTrue(mods.isEmpty());

        // The TabletManagementScanner is going to return all of the tablets because
        // we passed null for the state. Confirm that the hosting goal is ALWAYS for
        // for the first 13 and ondemand for the latter half. When running in the Manager
        // the latter half of the results would not be returned.
        for (int i = 0; i < 26; i++) {
          assertTrue(iter.hasNext());
          tman = iter.next();
          assertEquals(1, tman.getActions().size());
          assertTrue(tman.getActions().contains(ManagementAction.NEEDS_LOCATION_UPDATE));
          if (i < 13) {
            assertEquals(TabletHostingGoal.ALWAYS, tman.getTabletMetadata().getHostingGoal());
          } else {
            assertEquals(TabletHostingGoal.ONDEMAND, tman.getTabletMetadata().getHostingGoal());
          }
        }

        assertFalse(iter.hasNext());
      }

    }
  }

}

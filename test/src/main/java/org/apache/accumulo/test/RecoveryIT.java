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
package org.apache.accumulo.test;

import static org.apache.accumulo.harness.AccumuloITBase.MINI_CLUSTER_ONLY;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.stream.IntStream;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.admin.NewTableConfiguration;
import org.apache.accumulo.core.client.admin.TabletAvailability;
import org.apache.accumulo.core.clientImpl.ClientContext;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.metadata.schema.TabletMetadata;
import org.apache.accumulo.core.metadata.schema.TabletsMetadata;
import org.apache.accumulo.core.spi.balancer.TableLoadBalancer;
import org.apache.accumulo.core.tabletserver.log.LogEntry;
import org.apache.accumulo.harness.AccumuloClusterHarness;
import org.apache.accumulo.minicluster.ServerType;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloClusterControl;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloClusterImpl;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloConfigImpl;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.log.SortedLogState;
import org.apache.accumulo.server.manager.recovery.RecoveryPath;
import org.apache.accumulo.test.functional.ReadWriteIT;
import org.apache.accumulo.test.util.Wait;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

@Tag(MINI_CLUSTER_ONLY)
public class RecoveryIT extends AccumuloClusterHarness {

  private static final String RESOURCE_GROUP = "RG1";

  private volatile boolean disableTabletServerLogSorting = false;

  @Override
  protected Duration defaultTimeout() {
    return Duration.ofMinutes(3);
  }

  @Override
  public void configureMiniCluster(MiniAccumuloConfigImpl cfg, Configuration hadoopCoreSite) {
    cfg.getClusterServerConfiguration().setNumDefaultTabletServers(1);
    cfg.setProperty(Property.MANAGER_TABLET_GROUP_WATCHER_INTERVAL, "5s");
    cfg.getClusterServerConfiguration().addTabletServerResourceGroup(RESOURCE_GROUP, 3);
    cfg.setProperty(Property.INSTANCE_ZK_TIMEOUT, "15s");
    if (disableTabletServerLogSorting) {
      cfg.setProperty(Property.TSERV_WAL_SORT_MAX_CONCURRENT, "0");
    }
    // file system supports recovery
    hadoopCoreSite.set("fs.file.impl", RawLocalFileSystem.class.getName());
  }

  @Override
  public void setupCluster() throws Exception {
    // Do *NOT* startup the cluster here. We are doing it in the test
    // method so that we can modify the properties for each test run
  }

  @ParameterizedTest
  @ValueSource(strings = {"TSERVER", "SSERVER", "COMPACTOR"})
  public void test(String serverForSorting) throws Exception {

    // Determine whether or not we need to disable log sorting
    // in the TabletServer. We want to do this when the serverForSorting
    // parameter is SSERVER or COMPACTOR
    switch (serverForSorting) {
      case "TSERVER":
        disableTabletServerLogSorting = false;
        break;
      case "SSERVER":
      case "COMPACTOR":
      default:
        disableTabletServerLogSorting = true;
    }

    // Start the cluster
    super.setupCluster();

    // create a table
    String tableName = getUniqueNames(1)[0];
    try (AccumuloClient c = Accumulo.newClient().from(getClientProps()).build()) {

      SortedSet<Text> splits = new TreeSet<>();
      IntStream.range(97, 122).forEach(i -> splits.add(new Text(new String("" + i))));

      NewTableConfiguration ntc = new NewTableConfiguration();
      Map<String,String> tableProps = new HashMap<>();
      tableProps.put(Property.TABLE_MAJC_RATIO.getKey(), "100");
      tableProps.put(Property.TABLE_FILE_MAX.getKey(), "3");
      tableProps.put(TableLoadBalancer.TABLE_ASSIGNMENT_GROUP_PROPERTY, RESOURCE_GROUP);
      ntc.setProperties(tableProps);
      ntc.withSplits(splits);
      ntc.withInitialTabletAvailability(TabletAvailability.ONDEMAND);
      c.tableOperations().create(tableName, ntc);

      c.instanceOperations().waitForBalance();

      TableId tid = TableId.of(c.tableOperations().tableIdMap().get(tableName));

      ReadWriteIT.ingest(c, 1000, 1, 1, 0, "", tableName, 100);

      // Confirm that there are no walog entries for this table
      assertEquals(0, countWaLogEntries(c, tid));

      MiniAccumuloClusterImpl mini = (MiniAccumuloClusterImpl) cluster;
      MiniAccumuloClusterControl control = (MiniAccumuloClusterControl) cluster.getClusterControl();

      // Stop any running Compactors and ScanServers
      control.stopAllServers(ServerType.COMPACTOR);
      control.stopAllServers(ServerType.SCAN_SERVER);

      // Kill the TabletServer in resource group that is hosting the table
      List<Process> procs = control.getTabletServers(RESOURCE_GROUP);
      assertEquals(3, procs.size());
      for (int i = 0; i < 3; i++) {
        procs.get(i).destroyForcibly().waitFor();
      }
      control.getTabletServers(RESOURCE_GROUP).clear();

      // The TabletGroupWatcher in the Manager will notice that the TabletServer is dead
      // and will assign the TabletServer's walog to all of the tablets that were assigned
      // to that server. Confirm that walog entries exist for this tablet
      if (!serverForSorting.equals("TSERVER")) {
        Wait.waitFor(() -> countWaLogEntries(c, tid) > 0, 60_000);
      }

      // Start the process that will do the log sorting
      switch (serverForSorting) {
        case "TSERVER":
          // We don't need to re-start the TabletServers here, there is
          // already a TabletServer running in the default group that
          // is hosting the root and metadata tables. It should perform
          // the log sorting.
          break;
        case "SSERVER":
          mini.getConfig().getClusterServerConfiguration().setNumDefaultScanServers(1);
          getClusterControl().startAllServers(ServerType.SCAN_SERVER);
          break;
        case "COMPACTOR":
          mini.getConfig().getClusterServerConfiguration().setNumDefaultCompactors(1);
          getClusterControl().startAllServers(ServerType.COMPACTOR);
          break;
        case "ALL":
        default:
          fail("Unhandled server type: " + serverForSorting);
      }

      // Confirm sorting completed
      Wait.waitFor(() -> logSortingCompleted(c, tid) == true, 60_000);

      // Start the tablet servers so that the Manager
      // can assign the table and so that recovery can be completed.
      getClusterControl().startAllServers(ServerType.TABLET_SERVER);

      c.instanceOperations().waitForBalance();

      // When the tablet is hosted, the sorted walogs will be applied
      // Confirm the 3 walog entries are gone for this tablet
      assertEquals(0, countWaLogEntries(c, tid));

      // Scan the data and make sure its there.
      ReadWriteIT.verify(c, 1000, 1, 1, 0, "", tableName);

    }
  }

  private long countWaLogEntries(AccumuloClient c, TableId tableId) throws Exception {
    try (TabletsMetadata tabletsMetadata = ((ClientContext) c).getAmple().readTablets()
        .forTable(tableId).fetch(TabletMetadata.ColumnType.LOGS).build()) {
      return tabletsMetadata.stream().filter(tabletMetadata -> tabletMetadata.getLogs() != null)
          .count();
    }
  }

  private boolean logSortingCompleted(AccumuloClient c, TableId tableId) throws Exception {
    try (TabletsMetadata tabletsMetadata = ((ClientContext) c).getAmple().readTablets()
        .forTable(tableId).fetch(TabletMetadata.ColumnType.LOGS).build()) {
      ServerContext ctx = getCluster().getServerContext();
      for (TabletMetadata tm : tabletsMetadata) {
        for (LogEntry walog : tm.getLogs()) {
          String sortId = walog.getUniqueID().toString();
          String filename = walog.getPath();
          String dest = RecoveryPath.getRecoveryPath(new Path(filename)).toString();

          if (ctx.getZooCache().get(ctx.getZooKeeperRoot() + Constants.ZRECOVERY + "/" + sortId)
              != null
              || !ctx.getVolumeManager().exists(SortedLogState.getFinishedMarkerPath(dest))) {
            return false;
          }
        }
      }
      return true;
    }
  }

}

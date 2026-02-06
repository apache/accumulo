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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.stream.IntStream;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.ScannerBase.ConsistencyLevel;
import org.apache.accumulo.core.client.admin.CompactionConfig;
import org.apache.accumulo.core.client.admin.NewTableConfiguration;
import org.apache.accumulo.core.client.admin.servers.ServerId;
import org.apache.accumulo.core.conf.ClientProperty;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.ResourceGroupId;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.lock.ServiceLock;
import org.apache.accumulo.core.lock.ServiceLockData;
import org.apache.accumulo.core.lock.ServiceLockData.ThriftService;
import org.apache.accumulo.core.lock.ServiceLockPaths.AddressSelector;
import org.apache.accumulo.core.lock.ServiceLockPaths.ResourceGroupPredicate;
import org.apache.accumulo.core.lock.ServiceLockPaths.ServiceLockPath;
import org.apache.accumulo.core.metadata.schema.TabletsMetadata;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.spi.compaction.RatioBasedCompactionPlanner;
import org.apache.accumulo.core.spi.compaction.SimpleCompactionDispatcher;
import org.apache.accumulo.harness.MiniClusterConfigurationCallback;
import org.apache.accumulo.harness.SharedMiniClusterBase;
import org.apache.accumulo.minicluster.ServerType;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloClusterControl;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloConfigImpl;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.util.adminCommand.StopServers;
import org.apache.accumulo.test.compaction.ExternalCompactionTestUtils;
import org.apache.accumulo.test.util.Wait;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.google.common.net.HostAndPort;

public class GracefulShutdownIT extends SharedMiniClusterBase {

  private static final String GROUP_NAME = "graceful";

  // @formatter:off
  private static final String clientConfiguration =
     "["+
     " {"+
     "   \"isDefault\": true,"+
     "   \"maxBusyTimeout\": \"5m\","+
     "   \"busyTimeoutMultiplier\": 8,"+
     "   \"group\":" + GROUP_NAME + "," +
     "   \"scanTypeActivations\": [graceful],"+
     "   \"attemptPlans\": ["+
     "     {"+
     "       \"servers\": \"3\","+
     "       \"busyTimeout\": \"33ms\","+
     "       \"salt\": \"one\""+
     "     }"+
     "   ]"+
     "  }"+
     "]";
  // @formatter:on

  private static class GracefulShutdownITConfig implements MiniClusterConfigurationCallback {

    @Override
    public void configureMiniCluster(MiniAccumuloConfigImpl cfg, Configuration coreSite) {
      cfg.getClusterServerConfiguration().setNumDefaultCompactors(0);
      cfg.getClusterServerConfiguration().setNumDefaultScanServers(0);
      cfg.getClusterServerConfiguration().setNumDefaultTabletServers(2);
      cfg.setProperty(Property.COMPACTION_COORDINATOR_DEAD_COMPACTOR_CHECK_INTERVAL, "5s");
      cfg.setProperty(Property.COMPACTOR_CANCEL_CHECK_INTERVAL, "5s");
      cfg.setProperty(Property.COMPACTOR_PORTSEARCH, "true");
      cfg.setProperty(Property.COMPACTION_SERVICE_PREFIX.getKey() + GROUP_NAME + ".planner",
          RatioBasedCompactionPlanner.class.getName());
      cfg.setProperty(
          Property.COMPACTION_SERVICE_PREFIX.getKey() + GROUP_NAME + ".planner.opts.groups",
          "[{'group': '" + GROUP_NAME + "'}]");
      cfg.setClientProperty(ClientProperty.SCAN_SERVER_SELECTOR_OPTS_PREFIX.getKey() + "profiles",
          clientConfiguration);
      // Timeout scan sessions after being idle for 3 seconds
      cfg.setProperty(Property.TSERV_SESSION_MAXIDLE, "3s");
    }
  }

  @BeforeAll
  public static void startup() throws Exception {
    SharedMiniClusterBase.startMiniClusterWithConfig(new GracefulShutdownITConfig());
  }

  @AfterAll
  public static void shutdown() throws Exception {
    SharedMiniClusterBase.stopMiniCluster();
  }

  @Test
  public void testGracefulShutdown() throws Exception {

    // Start ScanServers and Compactors using named groups
    final MiniAccumuloClusterControl control = getCluster().getClusterControl();

    try (final AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
      final ServerContext ctx = getCluster().getServerContext();
      final String tableName = getUniqueNames(1)[0];

      final NewTableConfiguration ntc = new NewTableConfiguration();
      ntc.setProperties(Map.of(Property.TABLE_MAJC_RATIO.getKey(), "10",
          "table.compaction.dispatcher", SimpleCompactionDispatcher.class.getName(),
          "table.compaction.dispatcher.opts.service", GROUP_NAME));

      client.tableOperations().create(tableName, ntc);
      final TableId tid = ctx.getTableId(tableName);

      // Insert 10 rows, flush after every row to create 10 files
      try (BatchWriter writer = client.createBatchWriter(tableName)) {
        for (int i : IntStream.rangeClosed(1, 10).toArray()) {
          String val = i + "";
          Mutation m = new Mutation(val);
          m.put(val, val, val);
          writer.addMutation(m);
          writer.flush();
          client.tableOperations().flush(tableName, null, null, true);
        }
      }
      long numFiles = getNumFilesForTable(ctx, tid);
      assertEquals(10, numFiles);
      client.instanceOperations().waitForBalance();

      // Restart Garbage Collector
      final ServiceLockPath gcLockPath =
          getCluster().getServerContext().getServerPaths().getGarbageCollector(true);
      Optional<ServiceLockData> data = ServiceLock.getLockData(ctx.getZooSession(), gcLockPath);
      assertTrue(data.isPresent());
      final HostAndPort gcAddress = data.orElseThrow().getAddress(ThriftService.GC);
      assertTrue(!control.getProcesses(ServerType.GARBAGE_COLLECTOR).isEmpty());
      // Don't call `new Admin().execute(new String[] {"signalShutdown", "-h ", host, "-p ",
      // Integer.toString(port)})`
      // because this poisons the SingletonManager and puts it into SERVER mode
      StopServers.signalGracefulShutdown(ctx, gcAddress);
      Wait.waitFor(() -> {
        control.refreshProcesses(ServerType.GARBAGE_COLLECTOR);
        return control.getProcesses(ServerType.GARBAGE_COLLECTOR).isEmpty();
      });

      // Restart Tablet Server
      final Set<ServiceLockPath> tservers = getCluster().getServerContext().getServerPaths()
          .getTabletServer((rg) -> rg.equals(ResourceGroupId.DEFAULT), AddressSelector.all(), true);
      assertEquals(2, tservers.size());
      final HostAndPort tserverAddress =
          HostAndPort.fromString(tservers.iterator().next().getServer());
      StopServers.signalGracefulShutdown(ctx, tserverAddress);
      Wait.waitFor(() -> {
        control.refreshProcesses(ServerType.TABLET_SERVER);
        return control.getProcesses(ServerType.TABLET_SERVER).size() == 1;
      });
      client.instanceOperations().waitForBalance();
      control.start(ServerType.TABLET_SERVER);
      Wait.waitFor(() -> control.getProcesses(ServerType.TABLET_SERVER).size() == 2);
      client.instanceOperations().waitForBalance();

      // Restart Manager
      final ServiceLockPath manager =
          getCluster().getServerContext().getServerPaths().getManager(true);
      assertNotNull(manager);
      Set<ServerId> managerLocations =
          client.instanceOperations().getServers(ServerId.Type.MANAGER);
      assertNotNull(managerLocations);
      assertEquals(1, managerLocations.size());
      final HostAndPort managerAddress =
          HostAndPort.fromString(managerLocations.iterator().next().toHostPortString());
      StopServers.signalGracefulShutdown(ctx, managerAddress);
      Wait.waitFor(() -> {
        control.refreshProcesses(ServerType.MANAGER);
        return control.getProcesses(ServerType.MANAGER).isEmpty();
      });
      control.start(ServerType.MANAGER);
      Wait.waitFor(() -> control.getProcesses(ServerType.MANAGER).size() == 1);
      client.instanceOperations().waitForBalance();

      // Compact table and shutdown compactor
      getCluster().getConfig().getClusterServerConfiguration().addCompactorResourceGroup(GROUP_NAME,
          1);
      getCluster().getClusterControl().start(ServerType.COMPACTOR);

      Wait.waitFor(() -> getCluster()
          .getServerContext().getServerPaths().getCompactor(
              (rg) -> rg.equals(ResourceGroupId.of(GROUP_NAME)), AddressSelector.all(), true)
          .size() == 1);
      final Set<ServiceLockPath> compactors = getCluster().getServerContext().getServerPaths()
          .getCompactor(ResourceGroupPredicate.exact(ResourceGroupId.of(GROUP_NAME)),
              AddressSelector.all(), true);
      final HostAndPort compactorAddress =
          HostAndPort.fromString(compactors.iterator().next().getServer());

      final CompactionConfig cc = new CompactionConfig();
      final IteratorSetting is = new IteratorSetting(100, SlowIterator.class);
      SlowIterator.setSeekSleepTime(is, 1000);
      SlowIterator.setSleepTime(is, 1000);
      cc.setIterators(List.of(is));
      cc.setWait(false);

      final long numFiles2 = getNumFilesForTable(ctx, tid);
      assertEquals(numFiles2, numFiles);
      Set<ServerId> newManagerLocations =
          client.instanceOperations().getServers(ServerId.Type.MANAGER);
      assertNotNull(newManagerLocations);
      assertEquals(1, newManagerLocations.size());
      final HostAndPort newManagerAddress =
          HostAndPort.fromString(newManagerLocations.iterator().next().toHostPortString());
      assertEquals(0, ExternalCompactionTestUtils
          .getRunningCompactions(ctx, Optional.of(newManagerAddress)).getCompactionsSize());
      client.tableOperations().compact(tableName, cc);
      Wait.waitFor(() -> ExternalCompactionTestUtils
          .getRunningCompactions(ctx, Optional.of(newManagerAddress)).getCompactionsSize() > 0);
      StopServers.signalGracefulShutdown(ctx, compactorAddress);
      Wait.waitFor(() -> {
        control.refreshProcesses(ServerType.COMPACTOR);
        return control.getProcesses(ServerType.COMPACTOR).isEmpty();
      });
      final long numFiles3 = getNumFilesForTable(ctx, tid);
      assertTrue(numFiles3 < numFiles2);
      assertEquals(1, numFiles3);

      getCluster().getConfig().getClusterServerConfiguration()
          .addScanServerResourceGroup(GROUP_NAME, 1);
      getCluster().getClusterControl().start(ServerType.SCAN_SERVER);

      Wait.waitFor(() -> getCluster()
          .getServerContext().getServerPaths().getScanServer(
              (rg) -> rg.equals(ResourceGroupId.of(GROUP_NAME)), AddressSelector.all(), true)
          .size() == 1);
      final Set<ServiceLockPath> sservers =
          getCluster().getServerContext().getServerPaths().getScanServer(
              (rg) -> rg.equals(ResourceGroupId.of(GROUP_NAME)), AddressSelector.all(), true);
      final HostAndPort sserver = HostAndPort.fromString(sservers.iterator().next().getServer());
      try (final Scanner scanner = client.createScanner(tableName, Authorizations.EMPTY)) {
        scanner.setRange(new Range());
        scanner.setConsistencyLevel(ConsistencyLevel.EVENTUAL);
        scanner.setExecutionHints(Map.of("scan_type", "graceful"));
        scanner.addScanIterator(is); // add the slow iterator
        scanner.setBatchSize(1);
        int count = 0;
        for (Entry<Key,Value> e : scanner) {
          assertNotNull(e);
          count++;
          if (count == 2) {
            StopServers.signalGracefulShutdown(ctx, sserver);
          }
        }
        assertEquals(10, count);
        Wait.waitFor(() -> {
          control.refreshProcesses(ServerType.SCAN_SERVER);
          return control.getProcesses(ServerType.SCAN_SERVER).isEmpty();
        });

      }

    }

  }

  long getNumFilesForTable(ServerContext ctx, TableId tid) {
    try (TabletsMetadata tablets = ctx.getAmple().readTablets().forTable(tid).build()) {
      return tablets.stream().mapToLong(tm -> tm.getFiles().size()).sum();
    }
  }
}

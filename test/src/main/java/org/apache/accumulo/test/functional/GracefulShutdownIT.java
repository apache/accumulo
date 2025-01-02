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

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.IntStream;

import org.apache.accumulo.compactor.Compactor;
import org.apache.accumulo.coordinator.CompactionCoordinator;
import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.ScannerBase.ConsistencyLevel;
import org.apache.accumulo.core.client.admin.CompactionConfig;
import org.apache.accumulo.core.client.admin.NewTableConfiguration;
import org.apache.accumulo.core.conf.ClientProperty;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.fate.zookeeper.ServiceLock;
import org.apache.accumulo.core.fate.zookeeper.ServiceLock.ServiceLockPath;
import org.apache.accumulo.core.metadata.schema.TabletsMetadata;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.spi.compaction.DefaultCompactionPlanner;
import org.apache.accumulo.core.spi.compaction.SimpleCompactionDispatcher;
import org.apache.accumulo.core.util.HostAndPort;
import org.apache.accumulo.core.util.ServerServices;
import org.apache.accumulo.core.util.ServerServices.Service;
import org.apache.accumulo.harness.MiniClusterConfigurationCallback;
import org.apache.accumulo.harness.SharedMiniClusterBase;
import org.apache.accumulo.minicluster.ServerType;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloClusterControl;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloConfigImpl;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.util.Admin;
import org.apache.accumulo.test.compaction.ExternalCompactionTestUtils;
import org.apache.accumulo.test.util.Wait;
import org.apache.accumulo.tserver.ScanServer;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

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
      cfg.setNumCompactors(0);
      cfg.setNumScanServers(0);
      cfg.setNumTservers(2);
      cfg.setProperty(Property.COMPACTION_COORDINATOR_FINALIZER_COMPLETION_CHECK_INTERVAL, "5s");
      cfg.setProperty(Property.COMPACTION_COORDINATOR_DEAD_COMPACTOR_CHECK_INTERVAL, "5s");
      cfg.setProperty(Property.COMPACTION_COORDINATOR_TSERVER_COMPACTION_CHECK_INTERVAL, "3s");
      cfg.setProperty(Property.COMPACTION_COORDINATOR_THRIFTCLIENT_PORTSEARCH, "true");
      cfg.setProperty(Property.COMPACTOR_CANCEL_CHECK_INTERVAL, "5s");
      cfg.setProperty(Property.COMPACTOR_PORTSEARCH, "true");
      cfg.setProperty("tserver.compaction.major.service." + GROUP_NAME + ".planner",
          DefaultCompactionPlanner.class.getName());
      cfg.setProperty("tserver.compaction.major.service." + GROUP_NAME + ".planner.opts.executors",
          "[{'name':'all', 'type': 'external', 'queue': '" + GROUP_NAME + "'}]");
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
          ServiceLock.path(ctx.getZooKeeperRoot() + Constants.ZGC_LOCK);
      byte[] data = ServiceLock.getLockData(ctx.getZooReaderWriter().getZooKeeper(), gcLockPath);
      assertNotNull(data);
      final HostAndPort gcAddress =
          new ServerServices(new String(data, UTF_8)).getAddress(Service.GC_CLIENT);
      assertTrue(!control.getProcesses(ServerType.GARBAGE_COLLECTOR).isEmpty());
      // Don't call `new Admin().execute(new String[] {"signalShutdown", "-h ", host, "-p ",
      // Integer.toString(port)})`
      // because this poisons the SingletonManager and puts it into SERVER mode
      Admin.signalGracefulShutdown(ctx, gcAddress.getHost(), gcAddress.getPort());
      Wait.waitFor(() -> {
        control.refreshProcesses(ServerType.GARBAGE_COLLECTOR);
        return control.getProcesses(ServerType.GARBAGE_COLLECTOR).isEmpty();
      });

      // Restart Tablet Server
      final List<String> tservers = client.instanceOperations().getTabletServers();
      assertEquals(2, tservers.size());
      final HostAndPort tserverAddress = HostAndPort.fromString(tservers.get(0));
      Admin.signalGracefulShutdown(ctx, tserverAddress.getHost(), tserverAddress.getPort());
      Wait.waitFor(() -> {
        control.refreshProcesses(ServerType.TABLET_SERVER);
        return control.getProcesses(ServerType.TABLET_SERVER).size() == 1;
      });
      client.instanceOperations().waitForBalance();
      control.start(ServerType.TABLET_SERVER);
      Wait.waitFor(() -> control.getProcesses(ServerType.TABLET_SERVER).size() == 2);
      client.instanceOperations().waitForBalance();

      // Restart Manager
      final List<String> managers = client.instanceOperations().getManagerLocations();
      assertEquals(1, managers.size());
      final HostAndPort managerAddress = HostAndPort.fromString(managers.get(0));
      Admin.signalGracefulShutdown(ctx, managerAddress.getHost(), managerAddress.getPort());
      Wait.waitFor(() -> {
        control.refreshProcesses(ServerType.MANAGER);
        return control.getProcesses(ServerType.MANAGER).isEmpty();
      });
      control.start(ServerType.MANAGER);
      Wait.waitFor(() -> control.getProcesses(ServerType.MANAGER).size() == 1);
      client.instanceOperations().waitForBalance();

      // Compact table and shutdown compactor
      control.startCoordinator(CompactionCoordinator.class);
      getCluster().getConfig().setNumCompactors(1);
      control.startCompactors(Compactor.class, 1, GROUP_NAME);
      Wait.waitFor(() -> client.instanceOperations().getCompactors().size() == 1);
      final Set<String> compactors = client.instanceOperations().getCompactors();
      final HostAndPort compactorAddress = HostAndPort.fromString(compactors.iterator().next());

      final CompactionConfig cc = new CompactionConfig();
      final IteratorSetting is = new IteratorSetting(100, SlowIterator.class);
      SlowIterator.setSeekSleepTime(is, 1000);
      SlowIterator.setSleepTime(is, 1000);
      cc.setIterators(List.of(is));
      cc.setWait(false);

      final long numFiles2 = getNumFilesForTable(ctx, tid);
      assertEquals(numFiles2, numFiles);
      assertEquals(0, ExternalCompactionTestUtils.getRunningCompactions(ctx).getCompactionsSize());
      client.tableOperations().compact(tableName, cc);
      Wait.waitFor(
          () -> ExternalCompactionTestUtils.getRunningCompactions(ctx).getCompactionsSize() > 0);
      Admin.signalGracefulShutdown(ctx, compactorAddress.getHost(), compactorAddress.getPort());
      Wait.waitFor(() -> {
        control.refreshProcesses(ServerType.COMPACTOR);
        return control.getProcesses(ServerType.COMPACTOR).isEmpty();
      });
      final long numFiles3 = getNumFilesForTable(ctx, tid);
      assertTrue(numFiles3 < numFiles2);
      assertEquals(1, numFiles3);

      getCluster().getConfig().setNumScanServers(1);
      control.startScanServer(ScanServer.class, 1, GROUP_NAME);
      Wait.waitFor(() -> client.instanceOperations().getScanServers().size() == 1);
      final Set<String> sservers = client.instanceOperations().getScanServers();
      final HostAndPort sserver = HostAndPort.fromString(sservers.iterator().next());
      try (final Scanner scanner = client.createScanner(tableName, Authorizations.EMPTY)) {
        scanner.setRange(new Range());
        scanner.setConsistencyLevel(ConsistencyLevel.EVENTUAL);
        scanner.setExecutionHints(Map.of("scan_type", "graceful"));
        scanner.addScanIterator(is); // add the slow iterator
        scanner.setBatchSize(1);
        int count = 0;
        for (Entry<Key,Value> e : scanner) {
          count++;
          if (count == 2) {
            Admin.signalGracefulShutdown(ctx, sserver.getHost(), sserver.getPort());
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

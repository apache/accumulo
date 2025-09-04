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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiFunction;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.cli.ConfigOpts;
import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.admin.NewTableConfiguration;
import org.apache.accumulo.core.client.admin.TabletAvailability;
import org.apache.accumulo.core.client.admin.servers.ServerId;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.conf.SiteConfiguration;
import org.apache.accumulo.core.data.ResourceGroupId;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.fate.zookeeper.ZooUtil.NodeMissingPolicy;
import org.apache.accumulo.core.lock.ServiceLockPaths.AddressSelector;
import org.apache.accumulo.core.lock.ServiceLockPaths.ServiceLockPath;
import org.apache.accumulo.core.util.UtilWaitThread;
import org.apache.accumulo.harness.AccumuloClusterHarness;
import org.apache.accumulo.minicluster.ServerType;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloConfigImpl;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.test.util.Wait;
import org.apache.accumulo.tserver.TabletServer;
import org.apache.hadoop.conf.Configuration;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.net.HostAndPort;

/**
 * Test that validates that the TabletServer will be terminated when the lock is removed in
 * ZooKeeper, but a Watcher in the TabletServer is preventing the LockWatcher to be invoked.
 */
public class HalfDeadServerWatcherIT extends AccumuloClusterHarness {

  public static class HalfDeadTabletServer extends TabletServer {

    private static final Logger LOG = LoggerFactory.getLogger(HalfDeadTabletServer.class);

    public static void main(String[] args) throws Exception {
      try (HalfDeadTabletServer tserver =
          new HalfDeadTabletServer(new ConfigOpts(), ServerContext::new, args)) {
        tserver.runServer();
      }
    }

    public static class StuckWatcher implements Watcher {
      private static final Logger LOG = LoggerFactory.getLogger(StuckWatcher.class);

      @Override
      public void process(WatchedEvent event) {
        LOG.info("started sleeping...");
        while (true) {
          LOG.info("still sleeping...");
          UtilWaitThread.sleep(2000);
        }
      }

    }

    protected HalfDeadTabletServer(ConfigOpts opts,
        BiFunction<SiteConfiguration,ResourceGroupId,ServerContext> serverContextFactory,
        String[] args) {
      super(opts, serverContextFactory, args);
    }

    @Override
    public void evaluateOnDemandTabletsForUnload() {
      super.evaluateOnDemandTabletsForUnload();
      getOnlineTablets().keySet().forEach(ke -> {
        if (!ke.isMeta()) {
          final TableId tid = ke.tableId();
          final String tableZPath = Constants.ZTABLES + "/" + tid.canonical();
          try {
            this.getContext().getZooSession().asReader().exists(tableZPath, new StuckWatcher());
          } catch (KeeperException | InterruptedException e) {
            LOG.error("Error setting watch at: {}", tableZPath, e);
          }
          LOG.info("Set StuckWatcher at: {}", tableZPath);
        }
      });
    }
  }

  private static final AtomicBoolean USE_VERIFICATION_THREAD = new AtomicBoolean(false);

  @Override
  public void configureMiniCluster(MiniAccumuloConfigImpl cfg, Configuration hadoopCoreSite) {
    if (USE_VERIFICATION_THREAD.get()) {
      cfg.setProperty(Property.GENERAL_SERVER_LOCK_VERIFICATION_INTERVAL, "10s");
    } else {
      cfg.setProperty(Property.GENERAL_SERVER_LOCK_VERIFICATION_INTERVAL, "0");
    }
    cfg.setServerClass(ServerType.TABLET_SERVER, rg -> HalfDeadTabletServer.class);
    cfg.setProperty(Property.TSERV_ONDEMAND_UNLOADER_INTERVAL, "30s");
    cfg.getClusterServerConfiguration().setNumDefaultCompactors(1);
    cfg.getClusterServerConfiguration().setNumDefaultScanServers(0);
    cfg.getClusterServerConfiguration().setNumDefaultTabletServers(1);
  }

  @AfterEach
  public void afterTest() throws Exception {
    getCluster().getClusterControl().stopAllServers(ServerType.TABLET_SERVER);
    USE_VERIFICATION_THREAD.set(!USE_VERIFICATION_THREAD.get());
  }

  @Test
  public void testOne() throws Exception {
    if (USE_VERIFICATION_THREAD.get()) {
      // This test should use the verification thread, which should
      // end the TabletServer, throw an Exception on the ping call,
      // and return true
      assertTrue(testTabletServerWithStuckWatcherDies());
    } else {
      // This test should time out
      IllegalStateException e =
          assertThrows(IllegalStateException.class, () -> testTabletServerWithStuckWatcherDies());
      assertTrue(e.getMessage().contains("Timeout exceeded"));
    }
  }

  @Test
  public void testTwo() throws Exception {
    if (USE_VERIFICATION_THREAD.get()) {
      // This test should use the verification thread, which should
      // end the TabletServer, throw an Exception on the ping call,
      // and return true
      assertTrue(testTabletServerWithStuckWatcherDies());
    } else {
      // This test should time out
      IllegalStateException e =
          assertThrows(IllegalStateException.class, () -> testTabletServerWithStuckWatcherDies());
      assertTrue(e.getMessage().contains("Timeout exceeded"));
    }
  }

  public boolean testTabletServerWithStuckWatcherDies() throws Exception {
    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
      String tableName = getUniqueNames(1)[0];

      client.tableOperations().create(tableName,
          new NewTableConfiguration().withInitialTabletAvailability(TabletAvailability.HOSTED));

      // Wait a minute, the evaluator thread runs on a 30s interval to set the StuckWatcher
      Thread.sleep(60_000);

      // delete the table, which should invoke the watcher
      client.tableOperations().delete(tableName);

      final Set<ServerId> tservers =
          client.instanceOperations().getServers(ServerId.Type.TABLET_SERVER);
      assertEquals(1, tservers.size());

      ServerId tserver = tservers.iterator().next();

      // Delete the lock for the TabletServer
      final ServerContext ctx = getServerContext();
      Set<ServiceLockPath> serverPaths =
          ctx.getServerPaths().getTabletServer((rg) -> rg.equals(ResourceGroupId.DEFAULT),
              AddressSelector.exact(HostAndPort.fromString(tserver.toHostPortString())), true);
      assertEquals(1, serverPaths.size());
      ctx.getZooSession().asReaderWriter().recursiveDelete(serverPaths.iterator().next().toString(),
          NodeMissingPolicy.FAIL);

      Wait.waitFor(() -> pingServer(client, tserver.toHostPortString()) == false, 60_000);
      return true;
    }

  }

  private boolean pingServer(AccumuloClient client, String server) {
    final boolean lockVerificationThreadInUse = USE_VERIFICATION_THREAD.get();
    try {
      client.instanceOperations().ping(server);
      return true;
    } catch (AccumuloException e) {
      if (lockVerificationThreadInUse) {
        // If the lock verification thread is in use, the the TabletServer
        // should shut down and the call to ping will throw an Exception
        return false;
      } else {
        // With the lock verification thread disabled, the StuckWatcher
        // should prevent the TabletServer from shutting down during
        // this test method.
        fail("TabletServer unexpectedly shut down");
        return false;
      }
    }

  }

}

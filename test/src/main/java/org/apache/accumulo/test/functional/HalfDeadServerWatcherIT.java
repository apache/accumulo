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

import java.io.IOException;
import java.util.List;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.fate.zookeeper.ZooUtil.NodeMissingPolicy;
import org.apache.accumulo.core.process.thrift.ServerProcessService;
import org.apache.accumulo.core.util.UtilWaitThread;
import org.apache.accumulo.harness.AccumuloClusterHarness;
import org.apache.accumulo.minicluster.ServerType;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloConfigImpl;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.ServerOpts;
import org.apache.accumulo.test.util.Wait;
import org.apache.accumulo.tserver.TabletServer;
import org.apache.accumulo.tserver.tablet.Tablet;
import org.apache.accumulo.tserver.tablet.TabletData;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test that validates that the TabletServer will be terminated when the lock is removed in
 * ZooKeeper, but a Watcher in the TabletServer is preventing the LockWatcher to be invoked.
 */
public class HalfDeadServerWatcherIT extends AccumuloClusterHarness {

  public static class HalfDeadTabletServer extends TabletServer
      implements ServerProcessService.Iface {

    private static final Logger LOG = LoggerFactory.getLogger(HalfDeadTabletServer.class);

    public static void main(String[] args) throws Exception {
      try (HalfDeadTabletServer tserver = new HalfDeadTabletServer(new ServerOpts(), args)) {
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

    protected HalfDeadTabletServer(ServerOpts opts, String[] args) {
      super(opts, args);
    }

    @Override
    protected TreeMap<KeyExtent,TabletData> splitTablet(Tablet tablet, byte[] splitPoint)
        throws IOException {
      LOG.info("In HalfDeadServerWatcherIT::splitTablet");
      TreeMap<KeyExtent,TabletData> results = super.splitTablet(tablet, splitPoint);
      if (!tablet.getExtent().isMeta()) {
        final TableId tid = tablet.getExtent().tableId();
        final String zooRoot = this.getContext().getZooKeeperRoot();
        final String tableZPath = zooRoot + Constants.ZTABLES + "/" + tid.canonical();
        try {
          this.getContext().getZooReaderWriter().exists(tableZPath, new StuckWatcher());
        } catch (KeeperException | InterruptedException e) {
          LOG.error("Error setting watch at: {}", tableZPath, e);
        }
        LOG.info("Set StuckWatcher at: {}", tableZPath);
      }
      return results;
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
    cfg.setServerClass(ServerType.TABLET_SERVER, HalfDeadTabletServer.class);
    cfg.setNumCompactors(0);
    cfg.setNumScanServers(0);
    cfg.setNumTservers(1);
  }

  @AfterEach
  public void afterTest() throws Exception {
    getCluster().getClusterControl().stopAllServers(ServerType.TABLET_SERVER);
    super.teardownCluster();
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
      client.tableOperations().create(tableName);

      // add splits to the table, which should set a StuckWatcher on the table node in zookeeper
      TreeSet<Text> splits = new TreeSet<>();
      splits.add(new Text("j"));
      splits.add(new Text("t"));
      client.tableOperations().addSplits(tableName, splits);

      // delete the table, which should invoke the watcher
      client.tableOperations().delete(tableName);

      final List<String> tservers = client.instanceOperations().getTabletServers();
      assertEquals(1, tservers.size());

      // Delete the lock for the TabletServer
      final ServerContext ctx = getServerContext();
      final String zooRoot = ctx.getZooKeeperRoot();
      ctx.getZooReaderWriter().recursiveDelete(
          zooRoot + Constants.ZTSERVERS + "/" + tservers.get(0), NodeMissingPolicy.FAIL);

      Wait.waitFor(() -> pingServer(client, tservers.get(0)) == false, 60_000);
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

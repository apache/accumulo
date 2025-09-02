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
package org.apache.accumulo.test.upgrade;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.cli.ConfigOpts;
import org.apache.accumulo.core.client.admin.servers.ServerId;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.fate.zookeeper.ZooReaderWriter;
import org.apache.accumulo.core.fate.zookeeper.ZooUtil.NodeExistsPolicy;
import org.apache.accumulo.core.lock.ServiceLock;
import org.apache.accumulo.core.lock.ServiceLockPaths.AddressSelector;
import org.apache.accumulo.core.lock.ServiceLockPaths.ResourceGroupPredicate;
import org.apache.accumulo.core.zookeeper.ZooSession;
import org.apache.accumulo.harness.AccumuloClusterHarness;
import org.apache.accumulo.manager.Manager;
import org.apache.accumulo.minicluster.ServerType;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloConfigImpl;
import org.apache.accumulo.server.AbstractServer;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.test.util.Wait;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Test;

public class UpgradeIT extends AccumuloClusterHarness {

  private class ServerThatWontStart extends AbstractServer {

    protected ServerThatWontStart(String[] args) {
      super(ServerId.Type.TABLET_SERVER, new ConfigOpts(), (conf, rgid) -> new ServerContext(conf),
          args);
    }

    @Override
    public void run() {}

    @Override
    public ServiceLock getLock() {
      return null;
    }

    @Override
    public void startServiceLockVerificationThread() {}

    @Override
    public void close() {}

  }

  // This class exists because Manager constructor is not visible
  private class TestManager extends Manager {

    protected TestManager(String[] args) throws IOException {
      super(new ConfigOpts(), (conf, rgid) -> new ServerContext(conf), args);
    }

    @Override
    public void startServiceLockVerificationThread() {}

    @Override
    public void close() {}

  }

  @Override
  protected Duration defaultTimeout() {
    return Duration.ofMinutes(5);
  }

  @Override
  public void configureMiniCluster(MiniAccumuloConfigImpl cfg, Configuration hadoopCoreSite) {
    cfg.setProperty(Property.INSTANCE_ZK_TIMEOUT, "15s");
  }

  @Test
  public void testServersWontStart() throws Exception {
    // Constants.ZPREPARE_FOR_UPGRADE is created by 'accumulo upgrade --prepare'
    // which is run when the user wants to shutdown an instance in preparation
    // to upgrade it. When this node exists, no servers should start. There is
    // no ability to create this node in ZooKeeper before MAC starts for this
    // test, so we will create the node after MAC starts, then try to restart
    // MAC.

    final ZooSession zs = getServerContext().getZooSession();
    final ZooReaderWriter zrw = zs.asReaderWriter();
    final String upgradePath = Constants.ZPREPARE_FOR_UPGRADE;
    zrw.putPersistentData(upgradePath, new byte[0], NodeExistsPolicy.SKIP);

    getCluster().stop();

    getCluster().getClusterControl().startAllServers(ServerType.ZOOKEEPER);

    // Confirm all servers down
    Wait.waitFor(() -> getServerContext().getServerPaths()
        .getCompactor(ResourceGroupPredicate.ANY, AddressSelector.all(), true).size() == 0);
    Wait.waitFor(() -> getServerContext().getServerPaths().getGarbageCollector(true) == null);
    Wait.waitFor(() -> getServerContext().getServerPaths().getManager(true) == null);
    Wait.waitFor(() -> getServerContext().getServerPaths()
        .getScanServer(ResourceGroupPredicate.ANY, AddressSelector.all(), true).size() == 0);
    Wait.waitFor(() -> getServerContext().getServerPaths()
        .getTabletServer(ResourceGroupPredicate.ANY, AddressSelector.all(), true).size() == 0);

    assertThrows(IllegalStateException.class,
        () -> assertTimeoutPreemptively(Duration.ofMinutes(2), () -> getCluster().start()));

    // Confirm no servers started
    Wait.waitFor(() -> getServerContext().getServerPaths()
        .getCompactor(ResourceGroupPredicate.ANY, AddressSelector.all(), true).size() == 0);
    Wait.waitFor(() -> getServerContext().getServerPaths().getGarbageCollector(true) == null);
    Wait.waitFor(() -> getServerContext().getServerPaths().getManager(true) == null);
    Wait.waitFor(() -> getServerContext().getServerPaths()
        .getScanServer(ResourceGroupPredicate.ANY, AddressSelector.all(), true).size() == 0);
    Wait.waitFor(() -> getServerContext().getServerPaths()
        .getTabletServer(ResourceGroupPredicate.ANY, AddressSelector.all(), true).size() == 0);

    // Validate the exception from the servers
    List<String> args = new ArrayList<>();
    args.add("--props");
    args.add(getCluster().getAccumuloPropertiesPath());
    IllegalStateException ise = assertThrows(IllegalStateException.class,
        () -> new ServerThatWontStart(args.toArray(new String[0])));
    assertTrue(ise.getMessage()
        .startsWith("Instance has been prepared for upgrade to a minor or major version"));

  }

  @Test
  public void testUpgradeManagerFailure() throws Exception {
    // Test that the Manager fails when an upgrade is needed,
    // but UpgradeProgressTracker.initialize was never called.

    getCluster().stop();
    getCluster().getClusterControl().startAllServers(ServerType.ZOOKEEPER);

    ServerContext ctx = getCluster().getServerContext();
    UpgradeUtilIT.downgradePersistentVersion(ctx);

    List<String> args = new ArrayList<>();
    args.add("--props");
    args.add(getCluster().getAccumuloPropertiesPath());
    try (TestManager mgr = new TestManager(args.toArray(new String[0]))) {
      IllegalStateException ise = assertThrows(IllegalStateException.class, () -> mgr.runServer());
      assertTrue(ise.getMessage().equals("Upgrade not started, " + Constants.ZUPGRADE_PROGRESS
          + " node does not exist. Did you run 'accumulo upgrade --start'?"));
    }
  }

}

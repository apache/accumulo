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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.cli.ServerOpts;
import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.admin.servers.ServerId;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.conf.SiteConfiguration;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.fate.zookeeper.ZooUtil.NodeExistsPolicy;
import org.apache.accumulo.core.lock.ServiceLock;
import org.apache.accumulo.core.lock.ServiceLockPaths;
import org.apache.accumulo.core.lock.ServiceLockPaths.AddressSelector;
import org.apache.accumulo.core.lock.ServiceLockPaths.ResourceGroupPredicate;
import org.apache.accumulo.manager.Manager;
import org.apache.accumulo.manager.upgrade.Upgrader11to12;
import org.apache.accumulo.minicluster.ServerType;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloConfigImpl;
import org.apache.accumulo.server.AbstractServer;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.test.harness.AccumuloClusterHarness;
import org.apache.accumulo.test.util.Wait;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Test;

public class UpgradeIT extends AccumuloClusterHarness {

  private static class ServerThatWontStart extends AbstractServer {

    protected ServerThatWontStart(String[] args) {
      super(ServerId.Type.TABLET_SERVER, new ServerOpts(), (conf, rgid) -> new ServerContext(conf),
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
  private static class TestManager extends Manager {

    protected TestManager(String[] args) throws IOException {
      super(new ServerOpts(), (conf, rgid) -> new ServerContext(conf), args);
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

    getServerContext().getZooSession().asReaderWriter()
        .putPersistentData(Constants.ZPREPARE_FOR_UPGRADE, new byte[0], NodeExistsPolicy.SKIP);

    getCluster().stop();

    getCluster().getClusterControl().startAllServers(ServerType.ZOOKEEPER);

    // Confirm all servers down
    waitForNoServers();

    var startIse = assertThrows(IllegalStateException.class,
        () -> assertTimeoutPreemptively(Duration.ofMinutes(2), () -> getCluster().start()));
    assertTrue(startIse.getMessage().startsWith("Cannot start, instance is prepared for upgrade"));

    // Confirm no servers started
    waitForNoServers();

    // Validate the exception from the servers
    System.setProperty(SiteConfiguration.ACCUMULO_PROPERTIES_PROPERTY,
        "file://" + getCluster().getAccumuloPropertiesPath());
    try {
      IllegalStateException ise =
          assertThrows(IllegalStateException.class, () -> new ServerThatWontStart(new String[0]));
      assertTrue(ise.getMessage()
          .startsWith("Instance has been prepared for upgrade to a minor or major version"));
    } finally {
      System.clearProperty(SiteConfiguration.ACCUMULO_PROPERTIES_PROPERTY);
    }

  }

  private static void waitForNoServers() {
    final ServiceLockPaths paths = getServerContext().getServerPaths();
    Wait.waitFor(() -> paths.getCompactor(ResourceGroupPredicate.ANY, AddressSelector.all(), true)
        .isEmpty());
    Wait.waitFor(() -> paths.getGarbageCollector(true) == null);
    Wait.waitFor(() -> paths.getManager(true) == null);
    Wait.waitFor(() -> paths.getAssistantManagers(AddressSelector.all(), true).isEmpty());
    Wait.waitFor(() -> paths.getScanServer(ResourceGroupPredicate.ANY, AddressSelector.all(), true)
        .isEmpty());
    Wait.waitFor(() -> paths
        .getTabletServer(ResourceGroupPredicate.ANY, AddressSelector.all(), true).isEmpty());
  }

  @Test
  public void testUpgradeManagerFailure() throws Exception {
    // Test that the Manager fails when an upgrade is needed,
    // but UpgradeProgressTracker.initialize was never called.

    getCluster().stop();
    getCluster().getClusterControl().startAllServers(ServerType.ZOOKEEPER);

    ServerContext ctx = getCluster().getServerContext();
    UpgradeUtilIT.downgradePersistentVersion(ctx);

    System.setProperty(SiteConfiguration.ACCUMULO_PROPERTIES_PROPERTY,
        "file://" + getCluster().getAccumuloPropertiesPath());

    try (TestManager mgr = new TestManager(new String[0])) {
      IllegalStateException ise = assertThrows(IllegalStateException.class, mgr::runServer);
      assertEquals("Upgrade not started, " + Constants.ZUPGRADE_PROGRESS
          + " node does not exist. Did you run 'accumulo upgrade --start'?", ise.getMessage());
    } finally {
      System.clearProperty(SiteConfiguration.ACCUMULO_PROPERTIES_PROPERTY);
    }
  }

  @Test
  public void testGcCandidateUpgrade() throws Exception {
    String table = getUniqueNames(1)[0];
    try (AccumuloClient c = Accumulo.newClient().from(getClientProps()).build()) {
      c.tableOperations().create(table);

      // Some GC candidates from 2.1
      Map<String,String> testData = new HashMap<>();
      testData.put("~del6c383c59agcav:/tables/1/t-0000001", "skewed");
      testData.put("~del6fb76650agcav:/tables/1/t-0000007", "skewed");
      testData.put("~del17571942file:/v1/accumulo/tables/1/t-000003u/F000003x.rf", "skewed");
      testData.put("~dela6d36bfffile:/v2/accumulo/tables/1/t-000003t/A000003w.rf", "skewed");
      testData.put("~delb131253dfile:/v1/accumulo/tables/1/b-000003g", "skewed");

      try (var writer = c.createBatchWriter(table)) {
        for (var entry : testData.entrySet()) {
          writer.addMutation(
              new Mutation(entry.getKey()).at().family("").qualifier("").put(entry.getValue()));
        }
      }

      Upgrader11to12.upgradeGcCandidates(c, table);

      Map<String,String> output = new HashMap<>();

      try (var scanner = c.createScanner(table)) {
        scanner.forEach((k, v) -> output.put(k.getRow().toString(), v.toString()));
      }

      Map<String,String> expected = new HashMap<>();
      expected.put("~delfdccbbd9/1/t-0000001", "DIR");
      expected.put("~del6e94a332/1/t-0000007", "DIR");
      expected.put("~del17571942file:/v1/accumulo/tables/1/t-000003u/F000003x.rf", "FILE");
      expected.put("~dela6d36bfffile:/v2/accumulo/tables/1/t-000003t/A000003w.rf", "FILE");
      expected.put("~delb131253dfile:/v1/accumulo/tables/1/b-000003g", "BDIR");

      assertEquals(expected, output);

      // Run a second time, should not fail or make any changes
      Upgrader11to12.upgradeGcCandidates(c, table);
      output.clear();
      try (var scanner = c.createScanner(table)) {
        scanner.forEach((k, v) -> output.put(k.getRow().toString(), v.toString()));
      }
      assertEquals(expected, output);
    }

  }
}

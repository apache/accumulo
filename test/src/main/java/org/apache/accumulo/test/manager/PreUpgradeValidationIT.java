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
package org.apache.accumulo.test.manager;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.accumulo.harness.AccumuloITBase.MINI_CLUSTER_ONLY;
import static org.apache.accumulo.server.AccumuloDataVersion.REMOVE_DEPRECATIONS_FOR_VERSION_3;
import static org.apache.accumulo.server.AccumuloDataVersion.ROOT_TABLET_META_CHANGES;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mockStatic;

import java.time.Duration;
import java.util.List;
import java.util.Map;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.data.InstanceId;
import org.apache.accumulo.core.fate.zookeeper.ZooReaderWriter;
import org.apache.accumulo.core.fate.zookeeper.ZooUtil;
import org.apache.accumulo.harness.MiniClusterConfigurationCallback;
import org.apache.accumulo.harness.SharedMiniClusterBase;
import org.apache.accumulo.manager.upgrade.PreUpgradeValidation;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloConfigImpl;
import org.apache.accumulo.server.AccumuloDataVersion;
import org.apache.accumulo.server.ServerContext;
import org.apache.hadoop.conf.Configuration;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.cli.AclParser;
import org.apache.zookeeper.data.ACL;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Tag(MINI_CLUSTER_ONLY)
public class PreUpgradeValidationIT extends SharedMiniClusterBase {

  private static final Logger log = LoggerFactory.getLogger(PreUpgradeValidationIT.class);

  private static class ConfigCallback implements MiniClusterConfigurationCallback {
    @Override
    public void configureMiniCluster(MiniAccumuloConfigImpl cfg, Configuration coreSite) {
      // Only one tserver to avoid race conditions on ZK propagation (auths and configuration)
      cfg.setNumTservers(3);
      // Set the min span to 0 so we will definitely get all the traces back. See ACCUMULO-4365
      Map<String,String> siteConf = cfg.getSiteConfig();
      cfg.setSiteConfig(siteConf);
    }
  }

  @Override
  protected Duration defaultTimeout() {
    return Duration.ofMinutes(1);
  }

  @BeforeAll
  public static void setupMiniCluster() throws Exception {
    SharedMiniClusterBase.startMiniClusterWithConfig(new PreUpgradeValidationIT.ConfigCallback());

    String userDir = System.getProperty("user.dir");

    System.setProperty("hadoop.tmp.dir", userDir + "/target/hadoop-tmp");

  }

  /**
   * Wrap PreUpgradeValidation and override the fail method so that it throws an exception instead
   * of the default behaviour of calling system.exit()
   */
  private static class PreUpgradeValidationWrapper extends PreUpgradeValidation {
    @Override
    protected void fail(Exception e) {
      log.error("FATAL: Error performing pre-upgrade checks", e);
      throw new IllegalStateException("validation failed");
    }
  }

  /**
   * Upgrade should be skipped mini will be running the current version. The test inserts an node
   * with an invalid ACL, but it will not be checked because the upgrade check should not run.
   */
  @Test
  public void noUpdateNeededTest() throws Exception {
    final InstanceId IID = getCluster().getServerContext().getInstanceID();

    final String zkInstRootPath = "/accumulo/" + IID;

    ZooReaderWriter zrw = getCluster().getServerContext().getZooReaderWriter();
    ZooKeeper zooKeeper = zrw.getZooKeeper();

    ServerContext context = getCluster().getServerContext();

    ZooUtil.auth(zooKeeper, "digest", "stranger:abcde123".getBytes(UTF_8));
    List<ACL> pub = AclParser.parse("digest:stranger:abcde123:cdrwa");
    zooKeeper.create(zkInstRootPath + "/strange_node1", new byte[0], pub, CreateMode.PERSISTENT);
    zooKeeper.create(zkInstRootPath + "/strange_node2", new byte[0], pub, CreateMode.PERSISTENT);

    PreUpgradeValidation pcheck = new PreUpgradeValidationWrapper();
    // if invalid acls are detected, this is expected to throw an IllegalStateException.

    pcheck.validate(context, null);

    assertNotNull(zooKeeper.exists(zkInstRootPath + "/strange_node1", false));
    assertNotNull(zooKeeper.exists(zkInstRootPath + "/strange_node2", false));

    // clean-up
    zooKeeper.delete(zkInstRootPath + "/strange_node1", -1);
    zooKeeper.delete(zkInstRootPath + "/strange_node2", -1);

  }

  /**
   * Force pre upgrade check to run - it will verify ACLs and locks that exist with the running
   * mini-cluster.
   */
  @Test
  public void aclCheckTest() {
    ServerContext context = getCluster().getServerContext();

    try (var dataVersion = mockStatic(AccumuloDataVersion.class)) {
      dataVersion.when(() -> AccumuloDataVersion.getCurrentVersion(context))
          .thenReturn(ROOT_TABLET_META_CHANGES);
      dataVersion.when(AccumuloDataVersion::get).thenReturn(REMOVE_DEPRECATIONS_FOR_VERSION_3);

      PreUpgradeValidation pcheck = new PreUpgradeValidation();
      pcheck.validate(context, null);
    }
  }

  /**
   * Ingest nodes with invalid ACL and force upgrade to run.
   */
  @Test
  public void aclCheckFailTest() throws Exception {
    final InstanceId IID = getCluster().getServerContext().getInstanceID();

    final String zkInstRootPath = "/accumulo/" + IID;

    ZooReaderWriter zrw = getCluster().getServerContext().getZooReaderWriter();
    ZooKeeper zooKeeper = zrw.getZooKeeper();

    ServerContext context = getCluster().getServerContext();

    ZooUtil.auth(zooKeeper, "digest", "stranger:abcde123".getBytes(UTF_8));
    List<ACL> pub = AclParser.parse("digest:stranger:abcde123:cdrwa");
    zooKeeper.create(zkInstRootPath + "/strange_node1", new byte[0], pub, CreateMode.PERSISTENT);
    zooKeeper.create(zkInstRootPath + "/strange_node2", new byte[0], pub, CreateMode.PERSISTENT);

    try (var dataVersion = mockStatic(AccumuloDataVersion.class)) {
      dataVersion.when(() -> AccumuloDataVersion.getCurrentVersion(context))
          .thenReturn(ROOT_TABLET_META_CHANGES);
      dataVersion.when(AccumuloDataVersion::get).thenReturn(REMOVE_DEPRECATIONS_FOR_VERSION_3);

      PreUpgradeValidation pcheck = new PreUpgradeValidationWrapper();
      assertThrows(IllegalStateException.class, () -> pcheck.validate(context, null));
    }
    // clean-up
    zooKeeper.delete(zkInstRootPath + "/strange_node1", -1);
    zooKeeper.delete(zkInstRootPath + "/strange_node2", -1);

  }

  @Test
  public void aclTserverLocksTest() throws Exception {
    final InstanceId IID = getCluster().getServerContext().getInstanceID();

    final String zkInstRootPath = "/accumulo/" + IID;

    ZooReaderWriter zrw = getCluster().getServerContext().getZooReaderWriter();
    ZooKeeper zooKeeper = zrw.getZooKeeper();

    ServerContext context = getCluster().getServerContext();

    try (var dataVersion = mockStatic(AccumuloDataVersion.class)) {
      dataVersion.when(() -> AccumuloDataVersion.getCurrentVersion(context))
          .thenReturn(ROOT_TABLET_META_CHANGES);
      dataVersion.when(AccumuloDataVersion::get).thenReturn(REMOVE_DEPRECATIONS_FOR_VERSION_3);

      String zkTserver = zkInstRootPath + Constants.ZTSERVERS;
      String hostA = "localhost:19998";
      zrw.putPersistentData(zkTserver + "/" + hostA, new byte[0], ZooUtil.NodeExistsPolicy.SKIP);
      // write legacy lock data
      zrw.putPersistentData(
          zkTserver + "/" + hostA + "/zlock#74a2f2ef-8d1d-4ff0-a5de-6e8cfd8bb046#0000000000",
          hostA.getBytes(UTF_8), ZooUtil.NodeExistsPolicy.SKIP);

      String hostB = "localhost:19999";
      zrw.putPersistentData(zkTserver + "/" + hostB, new byte[0], ZooUtil.NodeExistsPolicy.SKIP);
      // write legacy lock data
      String lockDataB =
          "{\"descriptors\":[{\"uuid\":\"74a2f2ef-8d1d-4ff0-a5de-6e8cfd8bb046\",\"service\":\"TABLET_MANAGEMENT\",\"address\":\"localhost:9997\",\"group\":\"default\"},{\"uuid\":\"74a2f2ef-8d1d-4ff0-a5de-6e8cfd8bb046\",\"service\":\"TABLET_SCAN\",\"address\":\"localhost:9997\",\"group\":\"default\"},{\"uuid\":\"74a2f2ef-8d1d-4ff0-a5de-6e8cfd8bb046\",\"service\":\"CLIENT\",\"address\":\"localhost:9997\",\"group\":\"default\"},{\"uuid\":\"74a2f2ef-8d1d-4ff0-a5de-6e8cfd8bb046\",\"service\":\"TSERV\",\"address\":\"localhost:9997\",\"group\":\"default\"},{\"uuid\":\"74a2f2ef-8d1d-4ff0-a5de-6e8cfd8bb046\",\"service\":\"TABLET_INGEST\",\"address\":\"localhost:9997\",\"group\":\"default\"}]}\n";
      zrw.putPersistentData(
          zkTserver + "/" + hostB + "/zlock#74a2f2ef-8d1d-4ff0-a5de-6e8cfd8bb046#0000000000",
          lockDataB.getBytes(UTF_8), ZooUtil.NodeExistsPolicy.SKIP);

      log.info("BEFORE: root:{}", zooKeeper.getChildren(zkTserver + "/" + hostA, false));

      assertNotNull(zooKeeper.exists(
          zkTserver + "/" + hostA + "/zlock#74a2f2ef-8d1d-4ff0-a5de-6e8cfd8bb046#0000000000",
          false));
      assertNotNull(zooKeeper.exists(
          zkTserver + "/" + hostB + "/zlock#74a2f2ef-8d1d-4ff0-a5de-6e8cfd8bb046#0000000000",
          false));
      PreUpgradeValidation pcheck = new PreUpgradeValidation();
      pcheck.validate(context, null);

      log.info("AFTER: root:{}", zooKeeper.getChildren(zkTserver + "/" + hostA, false));
      // can't be parsed - should be removed
      assertNull(zooKeeper.exists(
          zkTserver + "/" + hostA + "/zlock#74a2f2ef-8d1d-4ff0-a5de-6e8cfd8bb046#0000000000",
          false));
      // parsable - but no thrift should be deleted
      assertNull(zooKeeper.exists(
          zkTserver + "/" + hostB + "/zlock#74a2f2ef-8d1d-4ff0-a5de-6e8cfd8bb046#0000000000",
          false));

      // validate that valid tserver locks were not removed and tservers remain active.
      try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
        List<String> activeTservers = client.instanceOperations().getTabletServers();
        log.warn("ACTIVE: {}", activeTservers);
        assertEquals(3, activeTservers.size());
      }
    }
  }
}

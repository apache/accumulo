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
import static org.apache.accumulo.harness.AccumuloITBase.ZOOKEEPER_TESTING_SERVER;
import static org.apache.accumulo.server.AccumuloDataVersion.REMOVE_DEPRECATIONS_FOR_VERSION_3;
import static org.apache.accumulo.server.AccumuloDataVersion.ROOT_TABLET_META_CHANGES;
import static org.junit.Assert.assertThrows;

import java.io.File;
import java.util.List;
import java.util.UUID;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.data.InstanceId;
import org.apache.accumulo.core.fate.zookeeper.ZooReaderWriter;
import org.apache.accumulo.core.fate.zookeeper.ZooUtil;
import org.apache.accumulo.manager.upgrade.PreUpgradeValidation;
import org.apache.accumulo.server.AccumuloDataVersion;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.conf.store.PropStore;
import org.apache.accumulo.server.conf.store.impl.ZooPropStore;
import org.apache.accumulo.server.init.ZooKeeperInitializer;
import org.apache.accumulo.test.zookeeper.ZooKeeperTestingServer;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.cli.AclParser;
import org.apache.zookeeper.data.ACL;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Tag(ZOOKEEPER_TESTING_SERVER)
public class PreUpgradeValidationIT {
  private static final Logger log = LoggerFactory.getLogger(PreUpgradeValidationIT.class);

  private static ZooKeeperTestingServer testZk = null;
  private static ZooReaderWriter zrw;
  private static ZooKeeper zooKeeper;
  @TempDir
  private static File tempDir;

  @BeforeAll
  public static void setupZk() {
    // using default zookeeper port - we don't have a full configuration
    testZk = new ZooKeeperTestingServer(tempDir);
    zooKeeper = testZk.getZooKeeper();
    ZooUtil.digestAuth(zooKeeper, ZooKeeperTestingServer.SECRET);
    zrw = testZk.getZooReaderWriter();
  }

  @AfterAll
  public static void shutdownZK() throws Exception {
    testZk.close();
  }

  @Test
  public void noUpdateNeededTest() {
    ServerContext context = Mockito.mock(ServerContext.class);

    try (var dataVersion = Mockito.mockStatic(AccumuloDataVersion.class)) {
      dataVersion.when(() -> AccumuloDataVersion.getCurrentVersion(context))
          .thenReturn(REMOVE_DEPRECATIONS_FOR_VERSION_3);
      dataVersion.when(AccumuloDataVersion::get).thenReturn(REMOVE_DEPRECATIONS_FOR_VERSION_3);

      PreUpgradeValidation pcheck = new PreUpgradeValidation();
      pcheck.validate(context, null);

      var unused = Mockito.verify(context);
      // verifyNoMoreInteractions(context);
    }
  }

  @Test
  public void aclCheckTest() {
    final InstanceId IID = InstanceId.of(UUID.randomUUID());
    final String zkInstRootPath = "/accumulo/" + IID;

    ServerContext context = Mockito.mock(ServerContext.class);

    try (var dataVersion = Mockito.mockStatic(AccumuloDataVersion.class)) {
      dataVersion.when(() -> AccumuloDataVersion.getCurrentVersion(context))
          .thenReturn(ROOT_TABLET_META_CHANGES);
      dataVersion.when(AccumuloDataVersion::get).thenReturn(REMOVE_DEPRECATIONS_FOR_VERSION_3);

      Mockito.when(context.getInstanceID()).thenReturn(IID);
      Mockito.when(context.getZooReaderWriter()).thenReturn(zrw);
      Mockito.when(context.getZooKeeperRoot()).thenReturn(zkInstRootPath);

      ZooKeeperInitializer zki = new ZooKeeperInitializer();
      try {
        // creates config path (required by prop store to exist)
        zki.initializeConfig(IID, zrw);

        final PropStore propStore = ZooPropStore.initialize(IID, zrw);
        Mockito.when(context.getPropStore()).thenReturn(propStore);

        zki.initialize(context, true, Constants.ZROOT + Constants.ZINSTANCES, "dir", "url");
      } catch (Exception ex) {
        log.warn("Test ZooKeeper initialization failed", ex);
      }

      PreUpgradeValidation pcheck = new PreUpgradeValidation();
      pcheck.validate(context, null);
    }
  }

  @Test
  public void aclCheckFailTest() throws Exception {
    final InstanceId IID = InstanceId.of(UUID.randomUUID());
    final String zkInstRootPath = "/accumulo/" + IID;

    ServerContext context = Mockito.mock(ServerContext.class);

    try (var dataVersion = Mockito.mockStatic(AccumuloDataVersion.class)) {
      dataVersion.when(() -> AccumuloDataVersion.getCurrentVersion(context))
          .thenReturn(ROOT_TABLET_META_CHANGES);
      dataVersion.when(AccumuloDataVersion::get).thenReturn(REMOVE_DEPRECATIONS_FOR_VERSION_3);

      Mockito.when(context.getInstanceID()).thenReturn(IID);
      Mockito.when(context.getZooReaderWriter()).thenReturn(zrw);
      Mockito.when(context.getZooKeeperRoot()).thenReturn(zkInstRootPath);

      ZooKeeperInitializer zki = new ZooKeeperInitializer();
      try {
        // creates config path (required by prop store to exist)
        zki.initializeConfig(IID, zrw);

        final PropStore propStore = ZooPropStore.initialize(IID, zrw);
        Mockito.when(context.getPropStore()).thenReturn(propStore);

        zki.initialize(context, true, Constants.ZROOT + Constants.ZINSTANCES, "dir", "url");
      } catch (Exception ex) {
        log.warn("Test ZooKeeper initialization failed", ex);
      }

      ZooUtil.auth(zooKeeper, "digest", "stranger:abcde123".getBytes(UTF_8));
      List<ACL> pub = AclParser.parse("digest:stranger:abcde123:cdrwa");
      zooKeeper.create(zkInstRootPath + "/strange_node1", new byte[0], pub, CreateMode.PERSISTENT);
      zooKeeper.create(zkInstRootPath + "/strange_node2", new byte[0], pub, CreateMode.PERSISTENT);

      PreUpgradeValidation pcheck = new PreUpgradeValidationWrapper();
      assertThrows(RuntimeException.class, () -> pcheck.validate(context, null));
    }
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
}

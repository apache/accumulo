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

import static org.apache.accumulo.harness.AccumuloITBase.ZOOKEEPER_TESTING_SERVER;
import static org.easymock.EasyMock.createNiceMock;
import static org.easymock.EasyMock.expect;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.data.InstanceId;
import org.apache.accumulo.core.fate.zookeeper.ZooReaderWriter;
import org.apache.accumulo.core.fate.zookeeper.ZooUtil;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.conf.store.PropStore;
import org.apache.accumulo.server.conf.store.SystemPropKey;
import org.apache.accumulo.server.conf.store.impl.ZooPropStore;
import org.apache.accumulo.server.conf.util.ConfigPropertyUpgrader;
import org.apache.accumulo.test.conf.store.PropStoreZooKeeperIT;
import org.apache.accumulo.test.conf.util.LegacyPropData;
import org.apache.accumulo.test.zookeeper.ZooKeeperTestingServer;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZKUtil;
import org.apache.zookeeper.ZooKeeper;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Tag(ZOOKEEPER_TESTING_SERVER)
public class ConfigPropertyUpgraderIT {

  private static final Logger log = LoggerFactory.getLogger(PropStoreZooKeeperIT.class);
  private static ZooKeeperTestingServer testZk = null;
  private static ZooKeeper zooKeeper;
  private static ZooReaderWriter zrw;

  private InstanceId instanceId = null;

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

  @BeforeEach
  public void setupZnodes() throws Exception {

    instanceId = InstanceId.of(UUID.randomUUID());

    testZk.initPaths(ZooUtil.getRoot(instanceId));

    ServerContext context = createNiceMock(ServerContext.class);
    expect(context.getZooReaderWriter()).andReturn(zrw).anyTimes();
    expect(context.getZooKeepersSessionTimeOut()).andReturn(zooKeeper.getSessionTimeout())
        .anyTimes();
    expect(context.getInstanceID()).andReturn(instanceId).anyTimes();

    List<LegacyPropData.PropNode> nodes = LegacyPropData.getData(instanceId);
    for (LegacyPropData.PropNode node : nodes) {
      zrw.putPersistentData(node.getPath(), node.getData(), ZooUtil.NodeExistsPolicy.SKIP);
    }

    try {
      zrw.putPersistentData(ZooUtil.getRoot(instanceId) + Constants.ZCONFIG, new byte[0],
          ZooUtil.NodeExistsPolicy.SKIP);
    } catch (KeeperException ex) {
      log.trace("Issue during zk initialization, skipping", ex);
    } catch (InterruptedException ex) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException("Interrupted during zookeeper path initialization", ex);
    }
  }

  @AfterEach
  public void cleanupZnodes() {
    try {
      ZKUtil.deleteRecursive(zooKeeper, Constants.ZROOT);
    } catch (KeeperException | InterruptedException ex) {
      throw new IllegalStateException("Failed to clean-up test zooKeeper nodes.", ex);
    }
  }

  @Test
  void doUpgrade() {
    ConfigPropertyUpgrader upgrader = new ConfigPropertyUpgrader();
    upgrader.doUpgrade(instanceId, zrw);

    PropStore propStore = ZooPropStore.initialize(instanceId, zrw);

    var sysKey = SystemPropKey.of(instanceId);
    log.info("PropStore: {}", propStore.get(sysKey));

    var vProps = propStore.get(sysKey);
    if (vProps == null) {
      fail("unexpected null returned from prop store get for " + sysKey);
      return; // keep spotbugs happy
    }

    Map<String,String> props = vProps.asMap();

    // also validates that rname from deprecated master to manager occured.
    assertEquals(5, props.size());
    assertEquals("4", props.get("manager.bulk.retries"));
    assertEquals("10m", props.get("manager.bulk.timeout"));
    assertEquals("10", props.get("manager.bulk.rename.threadpool.size"));
    assertEquals("4", props.get("manager.bulk.threadpool.size"));

    assertEquals("true", props.get("table.bloom.enabled"));

  }
}

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
package org.apache.accumulo.test.conf.util;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.accumulo.harness.AccumuloITBase.ZOOKEEPER_TESTING_SERVER;
import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.util.List;
import java.util.UUID;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.data.InstanceId;
import org.apache.accumulo.core.fate.zookeeper.ZooReaderWriter;
import org.apache.accumulo.core.fate.zookeeper.ZooUtil;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.conf.store.SystemPropKey;
import org.apache.accumulo.server.conf.store.impl.PropStoreWatcher;
import org.apache.accumulo.server.conf.store.impl.ZooPropStore;
import org.apache.accumulo.server.conf.util.TransformToken;
import org.apache.accumulo.test.zookeeper.ZooKeeperTestingServer;
import org.apache.zookeeper.ZKUtil;
import org.apache.zookeeper.ZooKeeper;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

@Tag(ZOOKEEPER_TESTING_SERVER)
public class TransformTokenIT {

  @TempDir
  private static File tempDir;

  private static ZooKeeperTestingServer testZk = null;
  private static ZooKeeper zooKeeper;
  private static ZooReaderWriter zrw;
  private InstanceId instanceId = null;

  private ServerContext context = null;
  private PropStoreWatcher watcher = null;

  @BeforeAll
  public static void setupZk() {

    // using default zookeeper port - we don't have a full configuration
    testZk = new ZooKeeperTestingServer(tempDir);
    zooKeeper = testZk.getZooKeeper();
    zrw = testZk.getZooReaderWriter();
  }

  @AfterAll
  public static void shutdownZK() throws Exception {
    testZk.close();
  }

  @BeforeEach
  public void testSetup() throws Exception {
    instanceId = InstanceId.of(UUID.randomUUID());

    List<LegacyPropData.PropNode> nodes = LegacyPropData.getData(instanceId);
    for (LegacyPropData.PropNode node : nodes) {
      zrw.putPersistentData(node.getPath(), node.getData(), ZooUtil.NodeExistsPolicy.SKIP);
    }

    ZooPropStore propStore = ZooPropStore.initialize(instanceId, zrw);

    context = createMock(ServerContext.class);
    expect(context.getInstanceID()).andReturn(instanceId).anyTimes();
    expect(context.getZooReaderWriter()).andReturn(zrw).anyTimes();
    expect(context.getPropStore()).andReturn(propStore).anyTimes();

    watcher = createMock(PropStoreWatcher.class);
  }

  @AfterEach
  public void cleanupZnodes() throws Exception {
    ZooUtil.digestAuth(zooKeeper, ZooKeeperTestingServer.SECRET);
    ZKUtil.deleteRecursive(zooKeeper, Constants.ZROOT);
    verify(context, watcher);
  }

  @Test
  public void tokenGoPathTest() {
    replay(context, watcher);

    var sysPropKey = SystemPropKey.of(instanceId);

    TransformToken token = TransformToken.createToken(sysPropKey.getPath(), zrw);

    assertTrue(token.haveTokenOwnership());
    token.releaseToken();
    assertFalse(token.haveTokenOwnership());

    // relock by getting a new lock
    TransformToken lock2 = TransformToken.createToken(sysPropKey.getPath(), zrw);
    assertTrue(lock2.haveTokenOwnership());

    // fail with a current lock node present
    TransformToken lock3 = TransformToken.createToken(sysPropKey.getPath(), zrw);
    assertFalse(lock3.haveTokenOwnership());
    // and confirm lock still present
    assertTrue(lock2.haveTokenOwnership());
  }

  @Test
  public void failOnInvalidLockTest() throws Exception {

    replay(context, watcher);

    var sysPropKey = SystemPropKey.of(instanceId);
    var tokenPath = sysPropKey.getPath() + TransformToken.TRANSFORM_TOKEN;

    TransformToken lock = TransformToken.createToken(sysPropKey.getPath(), zrw);

    // force change in lock
    assertTrue(lock.haveTokenOwnership());
    zrw.mutateExisting(tokenPath, v -> UUID.randomUUID().toString().getBytes(UTF_8));
    assertThrows(IllegalStateException.class, lock::releaseToken,
        "Expected unlock to fail on different UUID");

    // clean-up and get new lock
    zrw.delete(tokenPath);
    TransformToken lock3 = TransformToken.createToken(sysPropKey.getPath(), zrw);
    assertTrue(lock3.haveTokenOwnership());
    zrw.delete(tokenPath);
    assertThrows(IllegalStateException.class, lock::releaseToken,
        "Expected unlock to fail when no lock present");

  }
}

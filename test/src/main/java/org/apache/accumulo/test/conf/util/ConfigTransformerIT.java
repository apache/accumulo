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

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.accumulo.harness.AccumuloITBase.ZOOKEEPER_TESTING_SERVER;
import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.File;
import java.util.List;
import java.util.UUID;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.data.InstanceId;
import org.apache.accumulo.core.fate.zookeeper.ZooReaderWriter;
import org.apache.accumulo.core.fate.zookeeper.ZooUtil;
import org.apache.accumulo.core.util.Retry;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.conf.codec.VersionedPropCodec;
import org.apache.accumulo.server.conf.store.SystemPropKey;
import org.apache.accumulo.server.conf.store.impl.PropStoreWatcher;
import org.apache.accumulo.server.conf.store.impl.ZooPropStore;
import org.apache.accumulo.server.conf.util.ConfigTransformer;
import org.apache.accumulo.server.conf.util.TransformToken;
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
public class ConfigTransformerIT {

  private static final Logger log = LoggerFactory.getLogger(ConfigTransformerIT.class);
  private final static VersionedPropCodec codec = VersionedPropCodec.getDefault();
  @TempDir
  private static File tempDir;
  private static ZooKeeperTestingServer testZk = null;
  private static ZooKeeper zooKeeper;
  private static ZooReaderWriter zrw;

  private InstanceId instanceId = null;
  private ZooPropStore propStore = null;
  private ServerContext context = null;
  private PropStoreWatcher watcher = null;

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
  public void testSetup() throws Exception {
    instanceId = InstanceId.of(UUID.randomUUID());

    List<LegacyPropData.PropNode> nodes = LegacyPropData.getData(instanceId);
    for (LegacyPropData.PropNode node : nodes) {
      zrw.putPersistentData(node.getPath(), node.getData(), ZooUtil.NodeExistsPolicy.SKIP);
    }
    propStore = ZooPropStore.initialize(instanceId, zrw);

    context = createMock(ServerContext.class);
    expect(context.getInstanceID()).andReturn(instanceId).anyTimes();
    expect(context.getZooReaderWriter()).andReturn(zrw).anyTimes();
    expect(context.getPropStore()).andReturn(propStore).anyTimes();

    watcher = createMock(PropStoreWatcher.class);
    watcher.process(anyObject());
    expectLastCall().anyTimes();

    replay(context, watcher);

  }

  @AfterEach
  public void cleanupZnodes() {
    try {
      ZKUtil.deleteRecursive(zooKeeper, Constants.ZROOT);
    } catch (KeeperException | InterruptedException ex) {
      throw new IllegalStateException("Failed to clean-up test zooKeeper nodes.", ex);
    }
    verify(context, watcher);
  }

  @Test
  public void propStoreConversionTest() throws Exception {

    var sysPropKey = SystemPropKey.of(instanceId);

    List<String> sysLegacy = zrw.getChildren(sysPropKey.getPath());
    log.info("Before: {}", sysLegacy);

    var vProps = propStore.get(sysPropKey);
    assertNotNull(vProps);
    log.info("Converted: {}", vProps);

    sysLegacy = zrw.getChildren(sysPropKey.getPath());
    log.info("After: {}", sysLegacy);

  }

  @Test
  public void transformTest() throws Exception {

    var sysPropKey = SystemPropKey.of(instanceId);

    ConfigTransformer transformer = new ConfigTransformer(zrw, codec, watcher);
    List<String> sysLegacy = zrw.getChildren(sysPropKey.getPath());
    log.info("Before: {}", sysLegacy);

    var converted = transformer.transform(sysPropKey, sysPropKey.getPath(), false);

    assertEquals(sysLegacy.size(), converted.asMap().size());
  }

  @Test
  public void failToGetLock() throws Exception {
    var sysPropKey = SystemPropKey.of(instanceId);

    Retry retry =
        Retry.builder().maxRetries(3).retryAfter(250, MILLISECONDS).incrementBy(500, MILLISECONDS)
            .maxWait(5, SECONDS).backOffFactor(1.75).logInterval(3, MINUTES).createRetry();

    ConfigTransformer transformer = new ConfigTransformer(zrw, codec, watcher, retry);
    // manually create a lock so transformer fails
    zrw.putEphemeralData(sysPropKey.getPath() + TransformToken.TRANSFORM_TOKEN, new byte[0]);

    assertThrows(IllegalStateException.class,
        () -> transformer.transform(sysPropKey, sysPropKey.getPath(), false));

  }

  @Test
  public void continueOnLockRelease() {

  }

  @Test
  public void createdByAnother() {

  }

}

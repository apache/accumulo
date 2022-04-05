/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
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
import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.File;
import java.util.List;
import java.util.UUID;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.data.InstanceId;
import org.apache.accumulo.fate.util.Retry;
import org.apache.accumulo.fate.zookeeper.ZooReaderWriter;
import org.apache.accumulo.fate.zookeeper.ZooUtil;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.conf.codec.VersionedPropCodec;
import org.apache.accumulo.server.conf.store.PropCacheKey;
import org.apache.accumulo.server.conf.store.PropStoreException;
import org.apache.accumulo.server.conf.store.impl.PropStoreWatcher;
import org.apache.accumulo.server.conf.store.impl.ZooPropStore;
import org.apache.accumulo.server.conf.util.ConfigTransformer;
import org.apache.accumulo.server.conf.util.TransformLock;
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
public class ConfigTransformerTest {

  private static final Logger log = LoggerFactory.getLogger(ConfigTransformerTest.class);
  private final static VersionedPropCodec codec = VersionedPropCodec.getDefault();
  @TempDir
  private static File tempDir;
  private static ZooKeeperTestingServer testZk = null;
  private static ZooKeeper zooKeeper;
  private static ZooReaderWriter zrw;

  private InstanceId instanceId = null;
  private ZooPropStore propStore = null;
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

    propStore = new ZooPropStore.Builder(instanceId, zrw, 30_000).build();

    ServerContext context = createMock(ServerContext.class);
    expect(context.getInstanceID()).andReturn(instanceId).anyTimes();
    expect(context.getZooReaderWriter()).andReturn(zrw).anyTimes();
    expect(context.getPropStore()).andReturn(propStore).anyTimes();

    watcher = createMock(PropStoreWatcher.class);

    replay(context, watcher);

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
  public void propStoreConversionTest() throws Exception {

    var sysPropKey = PropCacheKey.forSystem(instanceId);

    List<String> sysLegacy = zrw.getChildren(sysPropKey.getBasePath());
    log.info("Before: {}", sysLegacy);

    var vProps = propStore.get(sysPropKey);
    assertNotNull(vProps);
    log.info("Converted: {}", vProps);

    sysLegacy = zrw.getChildren(sysPropKey.getBasePath());
    log.info("After: {}", sysLegacy);

  }

  @Test
  public void transformTest() throws Exception {

    var sysPropKey = PropCacheKey.forSystem(instanceId);

    ConfigTransformer transformer = new ConfigTransformer(zrw, codec, watcher);
    List<String> sysLegacy = zrw.getChildren(sysPropKey.getBasePath());
    log.info("Before: {}", sysLegacy);

    var converted = transformer.transform(sysPropKey);

    assertEquals(sysLegacy.size(), converted.getProperties().size());
  }

  @Test
  public void failToGetLock() throws Exception {
    var sysPropKey = PropCacheKey.forSystem(instanceId);

    Retry retry =
        Retry.builder().maxRetries(3).retryAfter(250, MILLISECONDS).incrementBy(500, MILLISECONDS)
            .maxWait(5, SECONDS).backOffFactor(1.75).logInterval(3, MINUTES).createRetry();

    ConfigTransformer transformer = new ConfigTransformer(zrw, codec, watcher, retry);
    // manually create a lock so transformer fails
    zrw.putEphemeralData(sysPropKey.getBasePath() + TransformLock.LOCK_NAME, new byte[0]);

    assertThrows(PropStoreException.class, () -> transformer.transform(sysPropKey));

  }

  @Test
  public void continueOnLockRelease() {

  }

  @Test
  public void createdByAnother() {

  }

}

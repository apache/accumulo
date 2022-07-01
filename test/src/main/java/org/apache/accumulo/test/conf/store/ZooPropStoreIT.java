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
package org.apache.accumulo.test.conf.store;

import static com.google.common.base.Suppliers.memoize;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.accumulo.harness.AccumuloITBase.ZOOKEEPER_TESTING_SERVER;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.conf.SiteConfiguration;
import org.apache.accumulo.core.data.InstanceId;
import org.apache.accumulo.core.data.NamespaceId;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.fate.zookeeper.ZooUtil;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.conf.codec.VersionedProperties;
import org.apache.accumulo.server.conf.store.SystemPropKey;
import org.apache.accumulo.server.conf.store.impl.TestZooPropStore;
import org.apache.accumulo.test.zookeeper.ZooKeeperTestingServer;
import org.apache.curator.test.TestingZooKeeperMain;
import org.apache.curator.test.TestingZooKeeperMain.TestZooKeeperServer;
import org.apache.curator.test.TestingZooKeeperServer;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Tag(ZOOKEEPER_TESTING_SERVER)
public class ZooPropStoreIT {

  /**
   * Override the propStore to use the TestZooPropStore
   *
   */
  private static class TestServerContext extends ServerContext {

    private final long zkClientSessionId;

    public TestServerContext(SiteConfiguration siteConfig) {
      super(siteConfig);
      zkClientSessionId = this.getZooReaderWriter().getZooKeeper().getSessionId();
      propStore = memoize(() -> new TestZooPropStore(INSTANCE_ID, this.getZooReaderWriter(), null,
          null, null, () -> {
            closeClientSession(zkClientSessionId);
          }));
    }

  }

  @TempDir
  private static File tempDir;

  private static final Logger LOG = LoggerFactory.getLogger(ZooPropStoreIT.class);
  private static final InstanceId INSTANCE_ID = InstanceId.of(UUID.randomUUID());
  // fake ids
  private static final NamespaceId nsId = NamespaceId.of("nsIdForTest");
  private static final TableId tidA = TableId.of("A");
  private static final TableId tidB = TableId.of("B");

  private static ZooKeeperTestingServer zkTestServerWrapper = null;
  private static TestingZooKeeperServer zkServer = null;
  private static TestServerContext context;

  @BeforeAll
  public static void setupZk() throws Exception {

    // Create the ZooKeeper Testing Server
    zkTestServerWrapper = new ZooKeeperTestingServer(tempDir);
    zkServer = zkTestServerWrapper.getZooKeeperTestServer().getTestingZooKeeperServer();
    ZooKeeper zkClient = zkTestServerWrapper.getZooKeeper();
    // Initialize ZK Paths
    initPaths(zkClient);

    // Simulate an instance file on the filesystem
    File instanceIdDir = new File(tempDir, Constants.INSTANCE_ID_DIR);
    instanceIdDir.mkdir();
    File instanceIdFile = new File(instanceIdDir, INSTANCE_ID.toString());
    assertTrue(instanceIdFile.createNewFile());

    // Create the server context
    Map<String,String> properties = new HashMap<>();
    properties.put(Property.INSTANCE_ZK_HOST.getKey(), zkTestServerWrapper.getConn());
    properties.put(Property.INSTANCE_ZK_TIMEOUT.getKey(), "5s");
    properties.put(Property.INSTANCE_SECRET.getKey(), ZooKeeperTestingServer.SECRET);
    properties.put(Property.INSTANCE_VOLUMES.getKey(), tempDir.toURI().toString());
    SiteConfiguration site = SiteConfiguration.empty().withOverrides(properties).build();
    context = new TestServerContext(site);

  }

  @AfterAll
  public static void shutdownZK() throws Exception {
    zkTestServerWrapper.close();
  }

  private static void initPaths(ZooKeeper zkClient) {

    zkTestServerWrapper.initPaths(ZooUtil.getRoot(INSTANCE_ID) + Constants.ZCONFIG);

    try {
      zkClient.create(Constants.ZROOT + Constants.ZINSTANCES, new byte[0],
          ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
      zkClient.create(Constants.ZROOT + Constants.ZINSTANCES + "/test",
          INSTANCE_ID.canonical().getBytes(UTF_8), ZooDefs.Ids.OPEN_ACL_UNSAFE,
          CreateMode.PERSISTENT);

      zkClient.create(ZooUtil.getRoot(INSTANCE_ID) + Constants.ZTABLES, new byte[0],
          ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
      zkClient.create(ZooUtil.getRoot(INSTANCE_ID) + Constants.ZTABLES + "/" + tidA.canonical(),
          new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
      zkClient.create(
          ZooUtil.getRoot(INSTANCE_ID) + Constants.ZTABLES + "/" + tidA.canonical() + "/conf",
          new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

      zkClient.create(ZooUtil.getRoot(INSTANCE_ID) + Constants.ZTABLES + "/" + tidB.canonical(),
          new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
      zkClient.create(
          ZooUtil.getRoot(INSTANCE_ID) + Constants.ZTABLES + "/" + tidB.canonical() + "/conf",
          new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
      zkClient.create(ZooUtil.getRoot(INSTANCE_ID) + Constants.ZNAMESPACES, new byte[0],
          ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
      zkClient.create(ZooUtil.getRoot(INSTANCE_ID) + Constants.ZNAMESPACES + "/" + nsId.canonical(),
          new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
      zkClient.create(
          ZooUtil.getRoot(INSTANCE_ID) + Constants.ZNAMESPACES + "/" + nsId.canonical() + "/conf",
          new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

    } catch (KeeperException ex) {
      LOG.trace("Issue during zk initialization, skipping", ex);
    } catch (InterruptedException ex) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException("Interrupted during zookeeper path initialization", ex);
    }

  }

  private static void closeClientSession(long sessionId) {
    try {
      Method m = zkServer.getClass().getDeclaredMethod("getMain");
      m.setAccessible(true);
      TestingZooKeeperMain main = (TestingZooKeeperMain) m.invoke(zkServer);
      Method m2 = main.getClass().getDeclaredMethod("getZkServer");
      m2.setAccessible(true);
      TestZooKeeperServer svr = (TestZooKeeperServer) m2.invoke(main);
      svr.closeSession(sessionId);
    } catch (NoSuchMethodException | SecurityException | IllegalAccessException
        | IllegalArgumentException | InvocationTargetException e) {
      LOG.error("Error closing client session", e);
    }
  }

  @Test
  public void testGetClosesConnection() throws Exception {
    ((TestZooPropStore) context.getPropStore()).create(SystemPropKey.of(context),
        Map.of(Property.TABLE_BLOOM_ENABLED.getKey(), "true"));

    VersionedProperties props =
        ((TestZooPropStore) context.getPropStore()).get(SystemPropKey.of(INSTANCE_ID));
    assertNotNull(props);
  }
}

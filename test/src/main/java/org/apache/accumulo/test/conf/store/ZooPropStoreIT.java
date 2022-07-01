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

import static org.apache.accumulo.harness.AccumuloITBase.ZOOKEEPER_TESTING_SERVER;
import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.reset;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.File;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Map;
import java.util.UUID;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.DefaultConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.conf.SiteConfiguration;
import org.apache.accumulo.core.data.InstanceId;
import org.apache.accumulo.core.data.NamespaceId;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.fate.zookeeper.ZooReaderWriter;
import org.apache.accumulo.fate.zookeeper.ZooUtil;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.conf.SystemConfiguration;
import org.apache.accumulo.server.conf.ZooBasedConfiguration;
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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Tag(ZOOKEEPER_TESTING_SERVER)
public class ZooPropStoreIT {

  @TempDir
  private static File tempDir;

  private static final Logger LOG = LoggerFactory.getLogger(ZooPropStoreIT.class);
  private static final InstanceId INSTANCE_ID = InstanceId.of(UUID.randomUUID());
  private static ZooKeeperTestingServer zkTestServerWrapper = null;
  private static TestingZooKeeperServer zkServer = null;
  private static ZooReaderWriter zrw;
  private static ZooKeeper zkClient;
  private ServerContext context;

  // fake ids
  private final NamespaceId nsId = NamespaceId.of("nsIdForTest");
  private final TableId tidA = TableId.of("A");
  private final TableId tidB = TableId.of("B");

  private TestZooPropStore propStore;
  private AccumuloConfiguration parent;

  @BeforeAll
  public static void setupZk() {
    zkTestServerWrapper = new ZooKeeperTestingServer(tempDir);
    zkServer = zkTestServerWrapper.getZooKeeperTestServer().getTestingZooKeeperServer();
    zkClient = zkTestServerWrapper.getZooKeeper();
    ZooUtil.digestAuth(zkClient, ZooKeeperTestingServer.SECRET);
    zrw = zkTestServerWrapper.getZooReaderWriter();
  }

  @AfterAll
  public static void shutdownZK() throws Exception {
    zkTestServerWrapper.close();
  }

  @BeforeEach
  public void initPaths() {
    context = createMock(ServerContext.class);
    zkTestServerWrapper.initPaths(ZooUtil.getRoot(INSTANCE_ID) + Constants.ZCONFIG);

    try {
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

    reset(context);

    // setup context mock with enough to create prop store
    expect(context.getInstanceID()).andReturn(INSTANCE_ID).anyTimes();
    expect(context.getZooReaderWriter()).andReturn(zrw).anyTimes();
    expect(context.getZooKeepersSessionTimeOut()).andReturn(zrw.getSessionTimeout()).anyTimes();

    replay(context);

    long zkClientSessionId = zkClient.getSessionId();
    propStore = new TestZooPropStore(context.getInstanceID(), zrw, null, null, null, () -> {
      closeClientSession(zkClientSessionId);
    });

    reset(context);

    // parent = createMock(AccumuloConfiguration.class);
    parent = DefaultConfiguration.getInstance();

    // setup context mock with prop store and the rest of the env needed.
    expect(context.getInstanceID()).andReturn(INSTANCE_ID).anyTimes();
    expect(context.getZooReaderWriter()).andReturn(zrw).anyTimes();
    expect(context.getZooKeepersSessionTimeOut()).andReturn(zkClient.getSessionTimeout())
        .anyTimes();
    expect(context.getPropStore()).andReturn(propStore).anyTimes();
    expect(context.getSiteConfiguration()).andReturn(SiteConfiguration.empty().build()).anyTimes();
  }

  private void closeClientSession(long sessionId) {
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
  public void testCheckZkConnection() throws Exception {
    replay(context);
    propStore.checkZkConnection();
    closeClientSession(zkClient.getSessionId());
    Thread.sleep(2000);
    propStore.checkZkConnection();
  }

  @Test
  public void testGetWithFailedConnection() throws Exception {
    replay(context);
    propStore.checkZkConnection();
    propStore.create(SystemPropKey.of(context),
        Map.of(Property.TABLE_BLOOM_ENABLED.getKey(), "true"));
    SystemPropKey sysPropKey = SystemPropKey.of(INSTANCE_ID);
    ZooBasedConfiguration zbc = new SystemConfiguration(context, sysPropKey, parent);
    assertEquals("true", zbc.get(Property.TABLE_BLOOM_ENABLED));
    closeClientSession(zkClient.getSessionId());
    Thread.sleep(2000);
    assertEquals("true", zbc.get(Property.TABLE_BLOOM_ENABLED));
  }

  // @Test
  // public void connectionLossTest() throws Exception {
  //
  // // expect(parent.getUpdateCount()).andReturn(123L).anyTimes();
  // replay(context);
  //
  // propStore.create(SystemPropKey.of(context),
  // Map.of(Property.TABLE_BLOOM_ENABLED.getKey(), "true"));
  //
  // var sysPropKey = SystemPropKey.of(INSTANCE_ID);
  //
  // TestListener testListener = new TestListener();
  // propStore.registerAsListener(sysPropKey, testListener);
  //
  // ZooBasedConfiguration zbc = new SystemConfiguration(context, sysPropKey, parent);
  //
  // assertNotNull(zbc.getSnapshot());
  // assertEquals("true", zbc.get(Property.TABLE_BLOOM_ENABLED));
  //
  // long updateCount = zbc.getUpdateCount();
  //
  // var tableBPropKey = TablePropKey.of(INSTANCE_ID, tidB);
  // propStore.create(tableBPropKey, Map.of());
  // Thread.sleep(150);
  //
  // int changeCount = testListener.getZkChangeCount();
  //
  // // force an "external update" directly in ZK - emulates a change external to the prop store.
  // // just echoing the same data - but it will update the ZooKeeper node data version.
  // Stat stat = new Stat();
  // byte[] bytes = zrw.getData(sysPropKey.getPath(), stat);
  // zrw.overwritePersistentData(sysPropKey.getPath(), bytes, stat.getVersion());
  //
  // // allow ZooKeeper notification time to propagate
  //
  // int retries = 5;
  // do {
  // Thread.sleep(25);
  // } while (changeCount >= testListener.getZkChangeCount() && --retries > 0);
  //
  // assertTrue(changeCount < testListener.getZkChangeCount());
  //
  // // prop changed - but will not be loaded in cache.
  // long updateCount2 = zbc.getUpdateCount();
  // assertNotEquals(updateCount, updateCount2);
  //
  // // read will repopulate the cache.
  // assertNotNull(zbc.getSnapshot());
  // assertEquals("true", zbc.get(Property.TABLE_BLOOM_ENABLED));
  //
  // assertNotEquals(updateCount, zbc.getUpdateCount());
  // assertEquals(updateCount2, zbc.getUpdateCount());
  // }

}

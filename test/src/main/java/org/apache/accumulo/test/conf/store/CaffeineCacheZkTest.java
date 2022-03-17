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
package org.apache.accumulo.test.conf.store;

import static org.apache.accumulo.core.conf.Property.GC_PORT;
import static org.apache.accumulo.core.conf.Property.MANAGER_CLIENTPORT;
import static org.apache.accumulo.core.conf.Property.TSERV_CLIENTPORT;
import static org.apache.accumulo.core.conf.Property.TSERV_NATIVEMAP_ENABLED;
import static org.apache.accumulo.core.conf.Property.TSERV_SCAN_MAX_OPENFILES;
import static org.apache.accumulo.harness.AccumuloITBase.ZOOKEEPER_TESTING_SERVER;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.data.InstanceId;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.metrics.MetricsUtil;
import org.apache.accumulo.fate.zookeeper.ZooReaderWriter;
import org.apache.accumulo.fate.zookeeper.ZooUtil;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.conf.codec.VersionedProperties;
import org.apache.accumulo.server.conf.store.PropCacheKey;
import org.apache.accumulo.server.conf.store.impl.PropCacheCaffeineImpl;
import org.apache.accumulo.server.conf.store.impl.PropStoreMetrics;
import org.apache.accumulo.server.conf.store.impl.PropStoreWatcher;
import org.apache.accumulo.server.conf.store.impl.ReadyMonitor;
import org.apache.accumulo.server.conf.store.impl.ZooPropLoader;
import org.apache.accumulo.server.conf.store.impl.ZooPropStore;
import org.apache.accumulo.test.zookeeper.ZooKeeperTestingServer;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZKUtil;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.easymock.EasyMock;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.jupiter.api.Tag;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Tag(ZOOKEEPER_TESTING_SERVER)
public class CaffeineCacheZkTest {

  private static final Logger log = LoggerFactory.getLogger(CaffeineCacheZkTest.class);
  private static final InstanceId INSTANCE_ID = InstanceId.of(UUID.randomUUID());

  private static ZooKeeperTestingServer testZk = null;
  private static ZooReaderWriter zrw;
  private static ZooKeeper zooKeeper;

  private final TableId tIdA = TableId.of("A");
  private final TableId tIdB = TableId.of("B");
  private final PropStoreMetrics cacheMetrics = new PropStoreMetrics();

  @ClassRule
  public static final TemporaryFolder TEMP =
      new TemporaryFolder(new File(System.getProperty("user.dir") + "/target"));

  @BeforeClass
  public static void setupZk() throws Exception {
    // using default zookeeper port - we don't have a full configuration
    testZk = new ZooKeeperTestingServer(TEMP.newFolder());
    zooKeeper = testZk.getZooKeeper();

    zrw = testZk.getZooReaderWriter();
    ServerContext context = EasyMock.createNiceMock(ServerContext.class);
    EasyMock.expect(context.getInstanceID()).andReturn(INSTANCE_ID).anyTimes();
    EasyMock.expect(context.getZooReaderWriter()).andReturn(zrw).anyTimes();

    EasyMock.replay(context);
  }

  @AfterClass
  public static void shutdownZK() throws Exception {
    testZk.close();
  }

  @Before
  public void setupZnodes() {
    testZk.initPaths(ZooUtil.getRoot(INSTANCE_ID) + Constants.ZCONFIG);
    try {
      zooKeeper.create(ZooUtil.getRoot(INSTANCE_ID) + Constants.ZTABLES, new byte[0],
          ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
      zooKeeper.create(ZooUtil.getRoot(INSTANCE_ID) + Constants.ZTABLES + "/" + tIdA.canonical(),
          new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
      zooKeeper.create(
          ZooUtil.getRoot(INSTANCE_ID) + Constants.ZTABLES + "/" + tIdA.canonical() + "/conf",
          new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

      zooKeeper.create(ZooUtil.getRoot(INSTANCE_ID) + Constants.ZTABLES + "/" + tIdB.canonical(),
          new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
      zooKeeper.create(
          ZooUtil.getRoot(INSTANCE_ID) + Constants.ZTABLES + "/" + tIdB.canonical() + "/conf",
          new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

    } catch (KeeperException ex) {
      log.trace("Issue during zk initialization, skipping", ex);
    } catch (InterruptedException ex) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException("Interrupted during zookeeper path initialization", ex);
    }
  }

  @After
  public void cleanupZnodes() {
    try {
      ZKUtil.deleteRecursive(zooKeeper, "/accumulo");
    } catch (KeeperException | InterruptedException ex) {
      throw new IllegalStateException("Failed to clean-up test zooKeeper nodes.", ex);
    }
  }

  @Test
  public void init() throws Exception {
    Map<String,String> props = new HashMap<>();
    props.put(TSERV_CLIENTPORT.getKey(), "1234");
    props.put(TSERV_NATIVEMAP_ENABLED.getKey(), "false");
    props.put(TSERV_SCAN_MAX_OPENFILES.getKey(), "2345");
    props.put(MANAGER_CLIENTPORT.getKey(), "3456");
    props.put(GC_PORT.getKey(), "4567");
    VersionedProperties vProps = new VersionedProperties(props);

    // directly create prop node - simulate existing properties.
    PropCacheKey propCacheKey = PropCacheKey.forTable(INSTANCE_ID, tIdA);
    var created = zrw.putPersistentData(propCacheKey.getPath(),
        ZooPropStore.getCodec().toBytes(vProps), ZooUtil.NodeExistsPolicy.FAIL);

    assertTrue("expected properties to be created", created);

    ReadyMonitor readyMonitor = new ReadyMonitor("test", zooKeeper.getSessionTimeout());

    PropStoreWatcher propStoreWatcher = new PropStoreWatcher(readyMonitor);

    MetricsUtil.initializeProducers(cacheMetrics);

    ZooPropLoader propLoader =
        new ZooPropLoader(zrw, ZooPropStore.getCodec(), propStoreWatcher, cacheMetrics);
    PropCacheCaffeineImpl cache = new PropCacheCaffeineImpl.Builder(propLoader, cacheMetrics).build();

    VersionedProperties readProps = cache.get(propCacheKey);

    if (readProps == null) {
      fail("Received null for versioned properties");
    }

    log.info("Props read from cache: {}", readProps.print(true));
  }

  // TODO - remove - this is not testing but was used for development?
  @Test
  public void watcherTest() throws Exception {
    ZooKeeper zk = zrw.getZooKeeper();

    PropCacheKey propCacheKey = PropCacheKey.forTable(INSTANCE_ID, tIdA);

    log.trace("add watcher");
    Watcher watcherA = new TestWatcher(zooKeeper, "WATCHER_A");
    zk.exists(propCacheKey.getPath(), watcherA);

    log.trace("create");
    zk.create(propCacheKey.getPath(), new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE,
        CreateMode.PERSISTENT);

    try {
      Thread.sleep(100);
    } catch (InterruptedException ex) {
      // empty
    }

    log.trace("Set data to empty byte array - version 0");
    zk.setData(propCacheKey.getPath(), new byte[0], 0);

    try {
      Thread.sleep(100);
    } catch (InterruptedException ex) {
      // empty
    }

    log.trace("Set data to empty byte array - version 1");
    zk.setData(propCacheKey.getPath(), new byte[0], 1);
    try {
      Thread.sleep(100);
    } catch (InterruptedException ex) {
      // empty
    }
    try {
      log.debug("Deleting watchers.");
      zk.removeWatches(propCacheKey.getPath(), watcherA, Watcher.WatcherType.Data, false);
    } catch (Exception ex) {
      log.info("error on watcher delete");
    }
    try {
      Thread.sleep(100);
    } catch (InterruptedException ex) {
      // empty
    }

    log.info("Set data to empty byte array - version 2");
    zk.setData(propCacheKey.getPath(), new byte[0], 2);

    try {
      Thread.sleep(500);
    } catch (InterruptedException ex) {
      // empty
    }

  }

  private static class TestWatcher implements Watcher {

    private final ZooKeeper zk;
    private final String id;

    public TestWatcher(final ZooKeeper zk, final String id) {
      this.zk = zk;
      this.id = id;
    }

    @Override
    public void process(WatchedEvent watchedEvent) {
      log.info("ZooKeeper event: watcher: {}, process: {}", id, watchedEvent);
      switch (watchedEvent.getType()) {
        case NodeCreated:
        case NodeDataChanged:
          log.info("Data change on {}", watchedEvent.getPath());
          try {
            log.info("adding watcher - {}", this);
            zk.exists(watchedEvent.getPath(), this);
          } catch (KeeperException ex) {
            throw new IllegalStateException("ZooKeeper exception thrown by watcher", ex);
          } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("ZooKeeper watcher interrupted", ex);
          }
          break;
        default:
          log.info("ignoring watcher - {}", this);
      }
    }
  }
}

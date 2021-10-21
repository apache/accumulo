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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.fate.zookeeper.ZooReaderWriter;
import org.apache.accumulo.fate.zookeeper.ZooUtil;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.conf.codec.VersionedPropCodec;
import org.apache.accumulo.server.conf.codec.VersionedPropGzipCodec;
import org.apache.accumulo.server.conf.codec.VersionedProperties;
import org.apache.accumulo.server.conf.store.PropCacheId;
import org.apache.accumulo.server.conf.store.PropChangeListener;
import org.apache.accumulo.server.conf.store.PropStore;
import org.apache.accumulo.server.conf.store.PropStoreException;
import org.apache.accumulo.server.conf.store.impl.ZooPropStore;
import org.apache.accumulo.test.categories.ZooKeeperTestingServerTests;
import org.apache.accumulo.test.zookeeper.ZooKeeperTestingServer;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZKUtil;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.easymock.EasyMock;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Category({ZooKeeperTestingServerTests.class})
public class PropStoreZooKeeperIT {

  private static final Logger log = LoggerFactory.getLogger(PropStoreZooKeeperIT.class);
  private static final String INSTANCE_ID = UUID.randomUUID().toString();
  private static final VersionedPropCodec propCodec = VersionedPropGzipCodec.codec(true);
  private static ZooKeeperTestingServer testZk = null;
  private static ZooKeeper zooKeeper;
  private static ServerContext context;
  private final TableId tIdA = TableId.of("A");
  private final TableId tIdB = TableId.of("B");

  @BeforeClass
  public static void setupZk() {

    // using default zookeeper port - we don't have a full configuration
    testZk = new ZooKeeperTestingServer("test");
    zooKeeper = testZk.getZooKeeper();

    ZooReaderWriter zooReaderWriter = new ZooReaderWriter(testZk.getConn(), 10_000, "test");

    context = EasyMock.createNiceMock(ServerContext.class);
    EasyMock.expect(context.getInstanceID()).andReturn(INSTANCE_ID).anyTimes();
    EasyMock.expect(context.getZooReaderWriter()).andReturn(zooReaderWriter).anyTimes();
    EasyMock.expect(context.getVersionedPropertiesCodec()).andReturn(propCodec).anyTimes();

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
  public void readFixed() {
    ZooPropStore.initSysProps(context, Map.of());
    PropStore propStore = new ZooPropStore.Builder(context).build();
    Map<String,String> fixed = propStore.readFixed();
    assertNotNull(fixed);
  }

  @Test
  public void createNoProps() throws PropStoreException, InterruptedException, KeeperException {

    ZooPropStore propStore = new ZooPropStore.Builder(context).build();

    PropCacheId tableA = PropCacheId.forTable(INSTANCE_ID, tIdA);

    // read from ZK
    assertNull(zooKeeper.exists(tableA.getPath(), false));
    assertTrue(propStore.create(tableA, null));
    // read from cache
    assertNotNull(zooKeeper.exists(tableA.getPath(), false));
    assertNotNull(propStore.get(tableA));

    assertEquals(1, propStore.getMetrics().getLoadCounter());
  }

  @Test
  public void failOnDuplicate() throws PropStoreException, InterruptedException, KeeperException {

    PropStore propStore = new ZooPropStore.Builder(context).build();

    PropCacheId tableA = PropCacheId.forTable(INSTANCE_ID, tIdA);

    assertNull(zooKeeper.exists(tableA.getPath(), false)); // check node does not exist in ZK
    assertTrue(propStore.create(tableA, null));

    assertNotNull(zooKeeper.exists(tableA.getPath(), false)); // check nod created
    assertThrows(PropStoreException.class, () -> propStore.create(tableA, null));

    assertNotNull(propStore.get(tableA));
  }

  @Test
  public void createWithProps()
      throws PropStoreException, InterruptedException, KeeperException, IOException {

    PropStore propStore = new ZooPropStore.Builder(context).build();

    PropCacheId tableA = PropCacheId.forTable(INSTANCE_ID, TableId.of("A"));
    Map<String,String> initialProps = new HashMap<>();
    initialProps.put(Property.TABLE_BLOOM_ENABLED.getKey(), "true");
    propStore.create(tableA, initialProps);

    VersionedProperties vProps = propStore.get(tableA);
    assertNotNull(vProps);
    assertEquals("true", vProps.getProperties().get(Property.TABLE_BLOOM_ENABLED.getKey()));

    // check using direct read from ZK
    byte[] bytes = zooKeeper.getData(tableA.getPath(), false, new Stat());
    var readFromZk = propCodec.fromBytes(bytes);
    assertEquals(readFromZk.getProperties(), propStore.get(tableA).getProperties());
  }

  @Test
  public void update() throws PropStoreException, InterruptedException {

    PropStore propStore = new ZooPropStore.Builder(context).build();

    TestChangeListener listener = new TestChangeListener();

    PropCacheId tableA = PropCacheId.forTable(INSTANCE_ID, TableId.of("A"));
    propStore.registerAsListener(tableA, listener);

    Map<String,String> initialProps = new HashMap<>();
    initialProps.put(Property.TABLE_BLOOM_ENABLED.getKey(), "true");
    propStore.create(tableA, initialProps);

    assertNotNull(propStore.get(tableA));
    assertEquals("true",
        propStore.get(tableA).getProperties().get(Property.TABLE_BLOOM_ENABLED.getKey()));

    int version0 = propStore.get(tableA).getDataVersion();

    Map<String,String> updateProps = new HashMap<>();
    updateProps.put(Property.TABLE_BLOOM_ENABLED.getKey(), "false");
    updateProps.put(Property.TABLE_MAJC_RATIO.getKey(), "5");

    log.trace("calling update()");

    propStore.putAll(tableA, updateProps);

    // allow change notification to propagate
    Thread.sleep(150);

    log.trace("calling get()");

    // validate version changed on write.
    int version1 = propStore.get(tableA).getDataVersion();

    log.trace("V0: {}, V1: {}", version0, version1);

    assertTrue(version0 < version1);

    assertNotNull(propStore.get(tableA));
    assertEquals(2, propStore.get(tableA).getProperties().size());
    assertEquals("false",
        propStore.get(tableA).getProperties().get(Property.TABLE_BLOOM_ENABLED.getKey()));
    assertEquals("5",
        propStore.get(tableA).getProperties().get(Property.TABLE_MAJC_RATIO.getKey()));

    propStore.removeProperties(tableA,
        Collections.singletonList(Property.TABLE_MAJC_RATIO.getKey()));
    Thread.sleep(150);
    // validate version changed on write

    log.trace("current props: {}", propStore.get(tableA).print(true));

    int version2 = propStore.get(tableA).getDataVersion();
    log.trace("versions created by test: v0: {}, v1: {}, v2: {}", version0, version1, version2);

    assertTrue(version0 < version2);
    assertTrue(version1 < version2);

    // allow change to propagate
    Thread.sleep(150);

    assertEquals(1, propStore.get(tableA).getProperties().size());
    assertEquals("false",
        propStore.get(tableA).getProperties().get(Property.TABLE_BLOOM_ENABLED.getKey()));
    assertNull(propStore.get(tableA).getProperties().get(Property.TABLE_MAJC_RATIO.getKey()));

    log.trace("changed count: {}", listener.changeCounts);

    assertEquals(2, (int) listener.getChangeCounts().get(tableA));
    assertNull(listener.getDeleteCounts().get(tableA));

  }

  @Test
  public void deleteTest() {
    PropStore propStore = new ZooPropStore.Builder(context).build();

    PropCacheId tableA = PropCacheId.forTable(INSTANCE_ID, TableId.of("A"));
    PropCacheId tableB = PropCacheId.forTable(INSTANCE_ID, TableId.of("B"));

    Map<String,String> initialProps = new HashMap<>();
    initialProps.put(Property.TABLE_BLOOM_ENABLED.getKey(), "true");
    propStore.create(tableA, initialProps);
    propStore.create(tableB, initialProps);

    assertNotNull(propStore.get(tableA));
    assertNotNull(propStore.get(tableB));
    assertEquals("true",
        propStore.get(tableA).getProperties().get(Property.TABLE_BLOOM_ENABLED.getKey()));

  }

  /**
   * Delete a node and validate delete is propogated via ZooKeeper watcher. Uses multiple caches
   * that should only be coordinating via ZooKeeper events. When a node is deleted, the ZooKeeper
   * node deleted event should also clear the node from all caches.
   *
   * @throws PropStoreException
   *           Any exception is a test failure.
   * @throws InterruptedException
   *           Any exception is a test failure.
   */
  @Test
  public void deleteThroughWatcher() throws PropStoreException, InterruptedException {

    PropStore propStore = new ZooPropStore.Builder(context).build();

    TestChangeListener listener = new TestChangeListener();

    PropCacheId tableA = PropCacheId.forTable(INSTANCE_ID, TableId.of("A"));
    PropCacheId tableB = PropCacheId.forTable(INSTANCE_ID, TableId.of("B"));

    propStore.registerAsListener(tableA, listener);
    propStore.registerAsListener(tableB, listener);

    Map<String,String> initialProps = new HashMap<>();
    initialProps.put(Property.TABLE_BLOOM_ENABLED.getKey(), "true");
    propStore.create(tableA, initialProps);
    propStore.create(tableB, initialProps);

    assertNotNull(propStore.get(tableA));
    assertNotNull(propStore.get(tableB));
    assertEquals("true",
        propStore.get(tableA).getProperties().get(Property.TABLE_BLOOM_ENABLED.getKey()));

    // use 3nd prop store - change will propagate via ZooKeeper
    PropStore propStore2 = new ZooPropStore.Builder(context).build();
    propStore2.delete(tableA);

    log.trace("--- delete --- {}", tableA);

    Thread.sleep(150);

    assertNull(propStore.get(tableA));
    assertNotNull(propStore.get(tableB));

    // validate change count not triggered
    assertNull(listener.getChangeCounts().get(tableA));
    assertNull(listener.getChangeCounts().get(tableB));

    // validate delete only for table A
    assertEquals(1, (int) listener.getDeleteCounts().get(tableA));
    assertNull(listener.getChangeCounts().get(tableA));
  }

  /**
   * Simulate change in props by process external to the prop store instance.
   */
  @Test
  public void externalChange()
      throws IOException, PropStoreException, InterruptedException, KeeperException {

    PropStore propStore = new ZooPropStore.Builder(context).build();

    TestChangeListener listener = new TestChangeListener();

    PropCacheId tableA = PropCacheId.forTable(INSTANCE_ID, TableId.of("A"));
    PropCacheId tableB = PropCacheId.forTable(INSTANCE_ID, TableId.of("B"));

    propStore.registerAsListener(tableA, listener);
    propStore.registerAsListener(tableB, listener);

    Map<String,String> initialProps = new HashMap<>();
    initialProps.put(Property.TABLE_BLOOM_ENABLED.getKey(), "true");
    propStore.create(tableA, initialProps);
    propStore.create(tableB, initialProps);

    assertNotNull(propStore.get(tableA));
    assertNotNull(propStore.get(tableB));

    VersionedProperties firstRead = propStore.get(tableA);

    assertEquals("true", firstRead.getProperties().get(Property.TABLE_BLOOM_ENABLED.getKey()));

    // This assumes default is resolved at a higher level
    assertNull(firstRead.getProperties().get(Property.TABLE_BLOOM_SIZE.getKey()));

    Map<String,String> update = new HashMap<>();
    var bloomSize = "1_000_000";
    update.put(Property.TABLE_BLOOM_SIZE.getKey(), bloomSize);
    VersionedProperties pendingProps = firstRead.addOrUpdate(update);

    log.debug("Writing props to trigger change notification {}", pendingProps.print(true));

    byte[] updatedBytes = propCodec.toBytes(pendingProps);
    // force external write to ZooKeeper
    zooKeeper.setData(tableA.getPath(), updatedBytes, firstRead.getDataVersion());

    Thread.sleep(150);

    log.trace("Test - waiting for event");

    VersionedProperties updateRead = propStore.get(tableA);

    log.trace("Re-read: {}", updateRead.print(true));

    // original values
    assertNull(firstRead.getProperties().get(Property.TABLE_BLOOM_SIZE.getKey()));

    log.trace("Updated: {}", updateRead.print(true));
    // values after update
    assertNotNull(updateRead.getProperties().get(Property.TABLE_BLOOM_SIZE.getKey()));
    assertEquals(bloomSize, updateRead.getProperties().get(Property.TABLE_BLOOM_SIZE.getKey()));

    log.trace("Prop changes {}", listener.getChangeCounts());
    log.trace("Prop deletes {}", listener.getDeleteCounts());

  }

  private static class TestChangeListener implements PropChangeListener {

    private final Map<PropCacheId,Integer> changeCounts = new ConcurrentHashMap<>();
    private final Map<PropCacheId,Integer> deleteCounts = new ConcurrentHashMap<>();

    @Override
    public void zkChangeEvent(PropCacheId id) {
      changeCounts.merge(id, 1, Integer::sum);
    }

    @Override
    public void cacheChangeEvent(PropCacheId id) {
      changeCounts.merge(id, 1, Integer::sum);
    }

    @Override
    public void deleteEvent(PropCacheId id) {
      deleteCounts.merge(id, 1, Integer::sum);
    }

    @Override
    public void connectionEvent() {

    }

    public Map<PropCacheId,Integer> getChangeCounts() {
      return changeCounts;
    }

    public Map<PropCacheId,Integer> getDeleteCounts() {
      return deleteCounts;
    }
  }
}

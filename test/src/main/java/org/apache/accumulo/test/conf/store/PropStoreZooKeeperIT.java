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

import static org.apache.accumulo.harness.AccumuloITBase.ZOOKEEPER_TESTING_SERVER;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.InstanceId;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.fate.zookeeper.ZooReaderWriter;
import org.apache.accumulo.fate.zookeeper.ZooUtil;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.conf.codec.VersionedPropCodec;
import org.apache.accumulo.server.conf.codec.VersionedProperties;
import org.apache.accumulo.server.conf.store.PropCacheKey;
import org.apache.accumulo.server.conf.store.PropChangeListener;
import org.apache.accumulo.server.conf.store.PropStore;
import org.apache.accumulo.server.conf.store.PropStoreException;
import org.apache.accumulo.server.conf.store.impl.ZooPropStore;
import org.apache.accumulo.test.zookeeper.ZooKeeperTestingServer;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZKUtil;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.easymock.EasyMock;
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
public class PropStoreZooKeeperIT {

  private static final Logger log = LoggerFactory.getLogger(PropStoreZooKeeperIT.class);
  private static final InstanceId INSTANCE_ID = InstanceId.of(UUID.randomUUID());
  private static final VersionedPropCodec propCodec = VersionedPropCodec.getDefault();
  private static ZooKeeperTestingServer testZk = null;
  private static ZooKeeper zooKeeper;
  private static ServerContext context;
  private final TableId tIdA = TableId.of("A");
  private final TableId tIdB = TableId.of("B");

  @TempDir
  private static File tempDir;

  @BeforeAll
  public static void setupZk() {

    // using default zookeeper port - we don't have a full configuration
    testZk = new ZooKeeperTestingServer(tempDir);
    zooKeeper = testZk.getZooKeeper();
    ZooReaderWriter zrw = testZk.getZooReaderWriter();

    context = EasyMock.createNiceMock(ServerContext.class);
    expect(context.getInstanceID()).andReturn(INSTANCE_ID).anyTimes();
    expect(context.getZooReaderWriter()).andReturn(zrw).anyTimes();

    replay(context);
  }

  @AfterAll
  public static void shutdownZK() throws Exception {
    testZk.close();
  }

  @BeforeEach
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

  @AfterEach
  public void cleanupZnodes() {
    try {
      ZKUtil.deleteRecursive(zooKeeper, "/accumulo");
    } catch (KeeperException | InterruptedException ex) {
      throw new IllegalStateException("Failed to clean-up test zooKeeper nodes.", ex);
    }
  }

  /**
   * Verify that when a config node does not exist, null is returned instead of an exception.
   */
  @Test
  public void createNoProps() throws PropStoreException, InterruptedException, KeeperException {

    ZooPropStore propStore = new ZooPropStore.Builder(context).build();

    PropCacheKey propKey = PropCacheKey.forTable(INSTANCE_ID, tIdA);

    // read from ZK
    assertNull(zooKeeper.exists(propKey.getPath(), false));
    // read from store
    assertNull(propStore.get(propKey));

  }

  @Test
  public void failOnDuplicate() throws PropStoreException, InterruptedException, KeeperException {

    PropStore propStore = new ZooPropStore.Builder(context).build();

    PropCacheKey propKey = PropCacheKey.forTable(INSTANCE_ID, tIdA);

    assertNull(zooKeeper.exists(propKey.getPath(), false)); // check node does not exist in ZK

    propStore.create(propKey, Map.of());
    Thread.sleep(25); // yield.

    assertNotNull(zooKeeper.exists(propKey.getPath(), false)); // check not created
    assertThrows(PropStoreException.class, () -> propStore.create(propKey, null));

    assertNotNull(propStore.get(propKey));
  }

  @Test
  public void createWithProps()
      throws PropStoreException, InterruptedException, KeeperException, IOException {

    PropStore propStore = new ZooPropStore.Builder(context).build();

    PropCacheKey propKey = PropCacheKey.forTable(INSTANCE_ID, TableId.of("A"));
    Map<String,String> initialProps = new HashMap<>();
    initialProps.put(Property.TABLE_BLOOM_ENABLED.getKey(), "true");
    propStore.create(propKey, initialProps);

    VersionedProperties vProps = propStore.get(propKey);
    assertNotNull(vProps);
    assertEquals("true", vProps.getProperties().get(Property.TABLE_BLOOM_ENABLED.getKey()));

    // check using direct read from ZK
    byte[] bytes = zooKeeper.getData(propKey.getPath(), false, new Stat());
    var readFromZk = propCodec.fromBytes(0, bytes);
    var propsA = propStore.get(propKey);
    if (propsA == null) {
      throw new IllegalStateException("unexpected null for versioned properties for propKey");
    }
    assertEquals(readFromZk.getProperties(), propsA.getProperties());
  }

  @Test
  public void update() throws PropStoreException, InterruptedException {

    PropStore propStore = new ZooPropStore.Builder(context).build();

    TestChangeListener listener = new TestChangeListener();

    PropCacheKey propKey = PropCacheKey.forTable(INSTANCE_ID, TableId.of("A"));
    propStore.registerAsListener(propKey, listener);

    Map<String,String> initialProps = new HashMap<>();
    initialProps.put(Property.TABLE_BLOOM_ENABLED.getKey(), "true");
    propStore.create(propKey, initialProps);

    var props1 = propStore.get(propKey);
    if (props1 == null) {
      throw new IllegalStateException("unexpected null for versioned properties for propKey");
    }

    assertEquals("true", props1.getProperties().get(Property.TABLE_BLOOM_ENABLED.getKey()));

    int version0 = props1.getDataVersion();

    Map<String,String> updateProps = new HashMap<>();
    updateProps.put(Property.TABLE_BLOOM_ENABLED.getKey(), "false");
    updateProps.put(Property.TABLE_MAJC_RATIO.getKey(), "5");

    log.trace("calling update()");

    propStore.putAll(propKey, updateProps);

    // allow change notification to propagate
    Thread.sleep(150);

    log.trace("calling get()");

    var props2 = propStore.get(propKey);
    if (props2 == null) {
      throw new IllegalStateException("unexpected null for versioned properties for propKey");
    }
    // validate version changed on write.
    int version1 = props2.getDataVersion();

    log.trace("V0: {}, V1: {}", version0, version1);

    assertTrue(version0 < version1);

    assertNotNull(propStore.get(propKey));
    assertEquals(2, props2.getProperties().size());
    assertEquals("false", props2.getProperties().get(Property.TABLE_BLOOM_ENABLED.getKey()));
    assertEquals("5", props2.getProperties().get(Property.TABLE_MAJC_RATIO.getKey()));

    propStore.removeProperties(propKey,
        Collections.singletonList(Property.TABLE_MAJC_RATIO.getKey()));
    Thread.sleep(150);
    // validate version changed on write

    var props3 = propStore.get(propKey);
    if (props3 == null) {
      throw new IllegalStateException("unexpected null for versioned properties for propKey");
    }
    log.trace("current props: {}", props3.print(true));

    int version2 = props3.getDataVersion();
    log.trace("versions created by test: v0: {}, v1: {}, v2: {}", version0, version1, version2);

    assertTrue(version0 < version2);
    assertTrue(version1 < version2);

    // allow change to propagate
    Thread.sleep(150);

    var props4 = propStore.get(propKey);
    if (props4 == null) {
      throw new IllegalStateException("unexpected null for versioned properties for propKey");
    }
    assertEquals(1, props4.getProperties().size());
    assertEquals("false", props4.getProperties().get(Property.TABLE_BLOOM_ENABLED.getKey()));
    assertNull(props4.getProperties().get(Property.TABLE_MAJC_RATIO.getKey()));

    log.trace("changed count: {}", listener.changeCounts);

    assertEquals(2, (int) listener.getChangeCounts().get(propKey));
    assertNull(listener.getDeleteCounts().get(propKey));

  }

  @Test
  public void deleteTest() {
    PropStore propStore = new ZooPropStore.Builder(context).build();

    PropCacheKey tableAPropKey = PropCacheKey.forTable(INSTANCE_ID, TableId.of("A"));
    PropCacheKey tableBPropKey = PropCacheKey.forTable(INSTANCE_ID, TableId.of("B"));

    Map<String,String> initialProps = new HashMap<>();
    initialProps.put(Property.TABLE_BLOOM_ENABLED.getKey(), "true");
    propStore.create(tableAPropKey, initialProps);
    propStore.create(tableBPropKey, initialProps);

    assertNotNull(propStore.get(tableAPropKey));
    assertNotNull(propStore.get(tableBPropKey));

    var props1 = propStore.get(tableAPropKey);
    if (props1 == null) {
      throw new IllegalStateException("unexpected null for versioned properties for tableAPropKey");
    }
    assertEquals("true", props1.getProperties().get(Property.TABLE_BLOOM_ENABLED.getKey()));
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

    PropCacheKey tableAPropKey = PropCacheKey.forTable(INSTANCE_ID, TableId.of("A"));
    PropCacheKey tableBPropKey = PropCacheKey.forTable(INSTANCE_ID, TableId.of("B"));

    propStore.registerAsListener(tableAPropKey, listener);
    propStore.registerAsListener(tableBPropKey, listener);

    Map<String,String> initialProps = new HashMap<>();
    initialProps.put(Property.TABLE_BLOOM_ENABLED.getKey(), "true");
    propStore.create(tableAPropKey, initialProps);
    propStore.create(tableBPropKey, initialProps);

    var propsA = propStore.get(tableAPropKey);
    if (propsA == null) {
      throw new IllegalStateException("unexpected null for versioned properties for tableAPropKey");
    }

    var propsB = propStore.get(tableBPropKey);
    if (propsB == null) {
      throw new IllegalStateException("unexpected null for versioned properties for tableBPropKey");
    }

    assertEquals("true", propsA.getProperties().get(Property.TABLE_BLOOM_ENABLED.getKey()));

    // use 3nd prop store - change will propagate via ZooKeeper
    PropStore propStore2 = new ZooPropStore.Builder(context).build();
    propStore2.delete(tableAPropKey);

    log.trace("After delete on 2nd store for table: {}", tableAPropKey);

    Thread.sleep(150);

    assertNull(propStore.get(tableAPropKey));
    assertNotNull(propStore.get(tableBPropKey));

    // validate change count not triggered
    assertNull(listener.getChangeCounts().get(tableAPropKey));
    assertNull(listener.getChangeCounts().get(tableBPropKey));

    // validate delete only for table A
    assertEquals(1, (int) listener.getDeleteCounts().get(tableAPropKey));
    assertNull(listener.getChangeCounts().get(tableAPropKey));
  }

  /**
   * Simulate change in props by process external to the prop store instance.
   */
  @Test
  public void externalChange()
      throws IOException, PropStoreException, InterruptedException, KeeperException {

    PropStore propStore = new ZooPropStore.Builder(context).build();

    TestChangeListener listener = new TestChangeListener();

    PropCacheKey tableAPropKey = PropCacheKey.forTable(INSTANCE_ID, TableId.of("A"));
    PropCacheKey tableBPropKey = PropCacheKey.forTable(INSTANCE_ID, TableId.of("B"));

    propStore.registerAsListener(tableAPropKey, listener);
    propStore.registerAsListener(tableBPropKey, listener);

    Map<String,String> initialProps = new HashMap<>();
    initialProps.put(Property.TABLE_BLOOM_ENABLED.getKey(), "true");
    propStore.create(tableAPropKey, initialProps);
    propStore.create(tableBPropKey, initialProps);

    assertNotNull(propStore.get(tableAPropKey));
    assertNotNull(propStore.get(tableBPropKey));

    VersionedProperties firstRead = propStore.get(tableAPropKey);
    if (firstRead == null) {
      throw new IllegalStateException("first read unexpected returned null");
    }
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
    context.getZooReaderWriter().overwritePersistentData(tableAPropKey.getPath(), updatedBytes,
        firstRead.getDataVersion());

    Thread.sleep(150);

    log.trace("Test - waiting for event");

    VersionedProperties updateRead = propStore.get(tableAPropKey);
    if (updateRead == null) {
      throw new IllegalStateException("update read failed with unexpected null");
    }
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

    private final Map<PropCacheKey,Integer> changeCounts = new ConcurrentHashMap<>();
    private final Map<PropCacheKey,Integer> deleteCounts = new ConcurrentHashMap<>();

    @Override
    public void zkChangeEvent(PropCacheKey propCacheKey) {
      changeCounts.merge(propCacheKey, 1, Integer::sum);
    }

    @Override
    public void cacheChangeEvent(PropCacheKey propCacheKey) {
      changeCounts.merge(propCacheKey, 1, Integer::sum);
    }

    @Override
    public void deleteEvent(PropCacheKey propCacheKey) {
      deleteCounts.merge(propCacheKey, 1, Integer::sum);
    }

    @Override
    public void connectionEvent() {

    }

    public Map<PropCacheKey,Integer> getChangeCounts() {
      return changeCounts;
    }

    public Map<PropCacheKey,Integer> getDeleteCounts() {
      return deleteCounts;
    }
  }
}

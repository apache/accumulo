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
package org.apache.accumulo.server.conf.store.impl;

import static org.apache.accumulo.core.conf.Property.TABLE_BULK_MAX_TABLETS;
import static org.apache.accumulo.core.conf.Property.TABLE_FILE_BLOCK_SIZE;
import static org.apache.accumulo.core.conf.Property.TABLE_SPLIT_THRESHOLD;
import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.capture;
import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.eq;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.isA;
import static org.easymock.EasyMock.newCapture;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.time.Instant;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.InstanceId;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.fate.zookeeper.ZooReaderWriter;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.conf.codec.VersionedPropCodec;
import org.apache.accumulo.server.conf.codec.VersionedProperties;
import org.apache.accumulo.server.conf.store.PropStore;
import org.apache.accumulo.server.conf.store.TablePropKey;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;
import org.easymock.Capture;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class ZooPropStoreTest {

  private final VersionedPropCodec propCodec = VersionedPropCodec.getDefault();
  private InstanceId instanceId;

  // mocks
  private ServerContext context;
  private ZooReaderWriter zrw;

  @BeforeEach
  public void init() throws Exception {
    instanceId = InstanceId.of(UUID.randomUUID());
    context = createMock(ServerContext.class);
    zrw = createMock(ZooReaderWriter.class);
    expect(context.getZooReaderWriter()).andReturn(zrw).anyTimes();
    expect(zrw.getSessionTimeout()).andReturn(2_000).anyTimes();
    expect(context.getInstanceID()).andReturn(instanceId).anyTimes();

    expect(zrw.exists(eq("/accumulo/" + instanceId), anyObject())).andReturn(true).anyTimes();
  }

  @AfterEach
  public void verifyMock() {
    verify(context, zrw);
  }

  @Test
  public void create() throws Exception {

    var propStoreKey = TablePropKey.of(instanceId, TableId.of("propStoreKey"));

    Capture<byte[]> bytes = newCapture();
    expect(zrw.putPrivatePersistentData(eq(propStoreKey.getPath()), capture(bytes), anyObject()))
        .andReturn(true).once();

    replay(context, zrw);

    PropStore propStore = ZooPropStore.initialize(instanceId, zrw);

    propStore.create(propStoreKey,
        Map.of(TABLE_BULK_MAX_TABLETS.getKey(), "1234", TABLE_FILE_BLOCK_SIZE.getKey(), "512M"));

    var decoded = propCodec.fromBytes(0, bytes.getValue());
    assertNotNull(decoded);
    assertEquals(0, decoded.getDataVersion());
    assertEquals("1234", decoded.asMap().get(TABLE_BULK_MAX_TABLETS.getKey()));
    assertEquals("512M", decoded.asMap().get(TABLE_FILE_BLOCK_SIZE.getKey()));
  }

  /**
   * Simple get tests load from ZooKeeper and then retrieved from cache.
   *
   * @throws Exception any exception is a test error.
   */
  @Test
  public void getTest() throws Exception {

    var propStoreKey = TablePropKey.of(instanceId, TableId.of("propStoreKey"));

    var vProps = new VersionedProperties(Map.of(Property.TABLE_BLOOM_ENABLED.getKey(), "true"));

    // expect one ZooKeeper call - subsequent calls should load from cache.
    Capture<Stat> stat = newCapture();
    expect(zrw.getData(eq(propStoreKey.getPath()), isA(PropStoreWatcher.class), capture(stat)))
        .andAnswer(() -> {
          Stat s = stat.getValue();
          s.setCtime(System.currentTimeMillis());
          s.setMtime(System.currentTimeMillis());
          s.setVersion((int) vProps.getDataVersion());
          s.setDataLength(propCodec.toBytes(vProps).length);
          stat.setValue(s);
          return propCodec.toBytes(vProps);
        }).once();

    replay(context, zrw);

    PropStore propStore = ZooPropStore.initialize(instanceId, zrw);

    assertNotNull(propStore.get(propStoreKey)); // first call will fetch from ZooKeeper
    assertNotNull(propStore.get(propStoreKey)); // next call will fetch from cache.

    var p = propStore.get(propStoreKey);

    assertEquals("true", p.asMap().get(Property.TABLE_BLOOM_ENABLED.getKey()));
  }

  @Test
  public void versionTest() throws Exception {

    var propStoreKey = TablePropKey.of(instanceId, TableId.of("table1"));
    Map<String,String> props =
        Map.of(TABLE_BULK_MAX_TABLETS.getKey(), "1234", TABLE_FILE_BLOCK_SIZE.getKey(), "512M");

    Capture<PropStoreWatcher> propStoreWatcherCapture = newCapture();
    Capture<Stat> stat = newCapture();

    // force version mismatch between zk and props.
    var expectedVersion = 99;
    expect(zrw.getData(eq(propStoreKey.getPath()), capture(propStoreWatcherCapture), capture(stat)))
        .andAnswer(() -> {
          Stat s = stat.getValue();
          s.setCtime(System.currentTimeMillis());
          s.setMtime(System.currentTimeMillis());
          s.setVersion(expectedVersion);
          s.setDataLength(propCodec.toBytes(new VersionedProperties(props)).length);
          stat.setValue(s);
          return propCodec.toBytes(new VersionedProperties(props));
        }).once();

    replay(context, zrw);

    PropStore propStore = ZooPropStore.initialize(instanceId, zrw);
    var vProps = propStore.get(propStoreKey);
    assertNotNull(vProps);
    assertEquals(expectedVersion, vProps.getDataVersion());
  }

  /**
   * Test that putAll will add and also overwrite properties.
   *
   * @throws Exception any exception is a test error.
   */
  @Test
  public void putAllTest() throws Exception {

    var propStoreKey = TablePropKey.of(instanceId, TableId.of("table1"));

    var initialProps = new VersionedProperties(0, Instant.now(),
        Map.of(TABLE_BULK_MAX_TABLETS.getKey(), "1234", TABLE_FILE_BLOCK_SIZE.getKey(), "512M"));

    // not cached - first will load from ZooKeeper
    Capture<Stat> stat = newCapture();

    expect(zrw.getData(eq(propStoreKey.getPath()), capture(stat))).andAnswer(() -> {
      Stat s = stat.getValue();
      s.setCtime(System.currentTimeMillis());
      s.setMtime(System.currentTimeMillis());
      s.setVersion((int) initialProps.getDataVersion());
      s.setDataLength(propCodec.toBytes(initialProps).length);
      stat.setValue(s);
      return propCodec.toBytes(initialProps);
    }).once();

    Capture<byte[]> bytes = newCapture();
    expect(zrw.overwritePersistentData(eq(propStoreKey.getPath()), capture(bytes), eq(0)))
        .andAnswer(() -> {
          var stored = propCodec.fromBytes(0, bytes.getValue());
          assertEquals(3, stored.asMap().size());
          // overwritten
          assertEquals("4321", stored.asMap().get(TABLE_BULK_MAX_TABLETS.getKey()));
          // unchanged
          assertEquals("512M", stored.asMap().get(TABLE_FILE_BLOCK_SIZE.getKey()));
          // new
          assertEquals("123M", stored.asMap().get(TABLE_SPLIT_THRESHOLD.getKey()));
          return true;
        }).once();

    replay(context, zrw);

    PropStore propStore = ZooPropStore.initialize(instanceId, zrw);

    Map<String,String> updateProps =
        Map.of(TABLE_BULK_MAX_TABLETS.getKey(), "4321", TABLE_SPLIT_THRESHOLD.getKey(), "123M");

    propStore.putAll(propStoreKey, updateProps);

    verify(zrw);
  }

  @Test
  public void removeTest() throws Exception {

    var propStoreKey = TablePropKey.of(instanceId, TableId.of("table1"));

    var initialProps = new VersionedProperties(123, Instant.now(),
        Map.of(TABLE_BULK_MAX_TABLETS.getKey(), "1234", TABLE_FILE_BLOCK_SIZE.getKey(), "512M"));

    // not cached - will load from ZooKeeper
    Capture<Stat> stat = newCapture();

    expect(zrw.getData(eq(propStoreKey.getPath()), capture(stat))).andAnswer(() -> {
      Stat s = stat.getValue();
      s.setVersion(123);
      s.setDataLength(propCodec.toBytes(initialProps).length);
      stat.setValue(s);
      return propCodec.toBytes(initialProps);
    }).once();

    Capture<byte[]> bytes = newCapture();
    expect(zrw.overwritePersistentData(eq(propStoreKey.getPath()), capture(bytes), eq(123)))
        .andAnswer(() -> {
          var stored = propCodec.fromBytes(124, bytes.getValue());
          assertEquals(1, stored.asMap().size());
          // deleted
          assertNull(stored.asMap().get(TABLE_BULK_MAX_TABLETS.getKey()));
          // unchanged
          assertEquals("512M", stored.asMap().get(TABLE_FILE_BLOCK_SIZE.getKey()));
          // never existed
          assertNull(stored.asMap().get(TABLE_SPLIT_THRESHOLD.getKey()));
          return true;
        }).once();

    replay(context, zrw);

    PropStore propStore = ZooPropStore.initialize(instanceId, zrw);

    Set<String> deleteNames =
        Set.of(TABLE_BULK_MAX_TABLETS.getKey(), TABLE_SPLIT_THRESHOLD.getKey());

    propStore.removeProperties(propStoreKey, deleteNames);
    verify(zrw);
  }

  @Test
  public void removeWithExceptionsTest() throws Exception {

    var propStoreKey = TablePropKey.of(instanceId, TableId.of("table1"));

    // return "bad data"
    Capture<Stat> stat = newCapture();
    expect(zrw.getData(eq(propStoreKey.getPath()), capture(stat))).andAnswer(() -> {
      Stat s = stat.getValue();
      s.setCtime(System.currentTimeMillis());
      s.setMtime(System.currentTimeMillis());
      s.setCzxid(1234);
      s.setVersion(19);
      s.setDataLength(12345);
      stat.setValue(s);
      return new byte[100];
    }).once();

    // mock throwing exceptions
    expect(zrw.getData(eq(propStoreKey.getPath()), anyObject(Stat.class)))
        .andThrow(new KeeperException.NoNodeException("mock forced no node")).once();
    expect(zrw.getData(eq(propStoreKey.getPath()), anyObject(Stat.class)))
        .andThrow(new InterruptedException("mock forced interrupt exception")).once();

    replay(context, zrw);

    PropStore propStore = ZooPropStore.initialize(instanceId, zrw);

    Set<String> deleteNames =
        Set.of(TABLE_BULK_MAX_TABLETS.getKey(), TABLE_SPLIT_THRESHOLD.getKey());

    // Parse error converted to IllegalStateException
    assertThrows(IllegalStateException.class,
        () -> propStore.removeProperties(propStoreKey, deleteNames));
    // ZK exception converted to IllegalStateException
    assertThrows(IllegalStateException.class,
        () -> propStore.removeProperties(propStoreKey, deleteNames));
    // InterruptException converted to IllegalStateException
    assertThrows(IllegalStateException.class,
        () -> propStore.removeProperties(propStoreKey, deleteNames));
  }

  @Test
  public void validateWatcherSetTest() throws Exception {

    var tablePropKey = TablePropKey.of(instanceId, TableId.of("table1"));
    Map<String,String> props =
        Map.of(TABLE_BULK_MAX_TABLETS.getKey(), "1234", TABLE_FILE_BLOCK_SIZE.getKey(), "512M");

    Capture<PropStoreWatcher> propStoreWatcherCapture = newCapture();
    Capture<Stat> stat = newCapture();

    // first call loads cache
    expect(zrw.getData(eq(tablePropKey.getPath()), capture(propStoreWatcherCapture), capture(stat)))
        .andAnswer(() -> {
          Stat s = stat.getValue();
          s.setCtime(System.currentTimeMillis());
          s.setMtime(System.currentTimeMillis());
          s.setVersion(13);
          stat.setValue(s);
          return propCodec.toBytes(new VersionedProperties(12, Instant.now(), props));
        }).once();

    // watcher change (re)loads cache on next get call.
    expect(zrw.getData(eq(tablePropKey.getPath()), capture(propStoreWatcherCapture), capture(stat)))
        .andAnswer(() -> {
          Stat s = stat.getValue();
          s.setCtime(System.currentTimeMillis());
          s.setMtime(System.currentTimeMillis());
          s.setVersion(14);
          stat.setValue(s);
          return propCodec.toBytes(new VersionedProperties(13, Instant.now(), props));
        }).once();

    replay(context, zrw);

    ReadyMonitor monitor = new TestReadyMonitor("testmon", 2000);
    PropStoreWatcher watcher = new TestWatcher(monitor);

    ZooPropStore propStore = new ZooPropStore(instanceId, zrw, monitor, watcher, null);

    assertNotNull(propStore.get(tablePropKey));

    // second call should return from cache - not zookeeper.
    assertNotNull(propStore.get(tablePropKey));
    PropStoreWatcher capturedMonitor = propStoreWatcherCapture.getValue();
    capturedMonitor.signalZkChangeEvent(tablePropKey);
    Thread.sleep(150);

    assertNotNull(propStore.get(tablePropKey));

  }

  private static class TestReadyMonitor extends ReadyMonitor {

    /**
     * Create an instance of a ready monitor.
     *
     * @param resourceName the resource name guarded by this monitor (used by logging)
     * @param timeout the max time in milliseconds this will block waiting.
     */
    public TestReadyMonitor(String resourceName, long timeout) {
      super(resourceName, timeout);
      setReady();
    }
  }

  private static class TestWatcher extends PropStoreWatcher {
    public TestWatcher(ReadyMonitor zkReadyMonitor) {
      super(zkReadyMonitor);
    }
  }

  @Test
  public void deleteTest() throws Exception {

    var propStoreKey = TablePropKey.of(instanceId, TableId.of("propStoreKey"));

    var vProps = new VersionedProperties(Map.of(Property.TABLE_BLOOM_ENABLED.getKey(), "true"));

    // expect first call to load cache.
    // expect one ZooKeeper call - subsequent calls should load from cache.
    Capture<Stat> stat = newCapture();
    expect(zrw.getData(eq(propStoreKey.getPath()), isA(PropStoreWatcher.class), capture(stat)))
        .andAnswer(() -> {
          Stat s = stat.getValue();
          s.setCtime(System.currentTimeMillis());
          s.setMtime(System.currentTimeMillis());
          s.setVersion((int) vProps.getDataVersion());
          s.setDataLength(propCodec.toBytes(vProps).length);
          stat.setValue(s);
          return propCodec.toBytes(vProps);
        }).once();

    zrw.delete(eq(propStoreKey.getPath()));
    expectLastCall().once();

    replay(context, zrw);

    PropStore propStore = ZooPropStore.initialize(instanceId, zrw);

    assertNotNull(propStore.get(propStoreKey)); // first call will fetch from ZooKeeper
    assertNotNull(propStore.get(propStoreKey)); // next call will fetch from cache.
    var p = propStore.get(propStoreKey);

    assertEquals("true", p.asMap().get(Property.TABLE_BLOOM_ENABLED.getKey()));

    propStore.delete(propStoreKey);
    Thread.sleep(50);
  }
}

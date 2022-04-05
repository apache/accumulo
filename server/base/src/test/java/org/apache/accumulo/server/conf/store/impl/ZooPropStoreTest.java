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
package org.apache.accumulo.server.conf.store.impl;

import static java.nio.charset.StandardCharsets.UTF_8;
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
import static org.junit.jupiter.api.Assertions.fail;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.InstanceId;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.fate.zookeeper.ZooReaderWriter;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.conf.codec.VersionedPropCodec;
import org.apache.accumulo.server.conf.codec.VersionedProperties;
import org.apache.accumulo.server.conf.store.PropCacheKey;
import org.apache.accumulo.server.conf.store.PropStore;
import org.apache.accumulo.server.conf.store.PropStoreException;
import org.apache.accumulo.server.conf.util.TransformLock;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;
import org.easymock.Capture;
import org.easymock.CaptureType;
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
    expect(context.getZooKeepersSessionTimeOut()).andReturn(500).anyTimes();
    expect(context.getInstanceID()).andReturn(instanceId).anyTimes();

    expect(zrw.exists(eq("/accumulo/" + instanceId), anyObject())).andReturn(true).anyTimes();
  }

  @AfterEach
  public void verifyMock() {
    verify(context, zrw);
  }

  @Test
  public void initSysPropsTest() throws Exception {
    // checks for system props existence
    expect(zrw.exists(PropCacheKey.forSystem(instanceId).getPath())).andReturn(false).once();

    Capture<byte[]> bytes = newCapture();
    expect(zrw.putPersistentData(eq(PropCacheKey.forSystem(instanceId).getPath()), capture(bytes),
        anyObject())).andReturn(true).once();

    replay(context, zrw);

    ZooPropStore.initSysProps(context,
        Map.of(TABLE_BULK_MAX_TABLETS.getKey(), "1234", TABLE_FILE_BLOCK_SIZE.getKey(), "512M"));

    var decoded = propCodec.fromBytes(0, bytes.getValue());
    assertNotNull(decoded);
    assertEquals(0, decoded.getDataVersion());
    assertEquals("1234", decoded.getProperties().get(TABLE_BULK_MAX_TABLETS.getKey()));
    assertEquals("512M", decoded.getProperties().get(TABLE_FILE_BLOCK_SIZE.getKey()));
  }

  @Test
  public void create() throws Exception {

    PropCacheKey propCacheKey = PropCacheKey.forTable(instanceId, TableId.of("propCacheKey"));

    expect(zrw.putPersistentData(eq(propCacheKey.getPath()), anyObject(), anyObject()))
        .andReturn(true).once();

    replay(context, zrw);

    PropStore propStore = new ZooPropStore.Builder(context).build();

    propStore.create(propCacheKey, Map.of());
  }

  /**
   * Simple get tests load from ZooKeeper and then retrieved from cache.
   *
   * @throws Exception
   *           any exception is a test error.
   */
  @Test
  public void getTest() throws Exception {

    PropCacheKey propCacheKey = PropCacheKey.forTable(instanceId, TableId.of("propCacheKey"));

    var vProps = new VersionedProperties(Map.of(Property.TABLE_BLOOM_ENABLED.getKey(), "true"));

    // expect one ZooKeeper call - subsequent calls should load from cache.
    expect(zrw.getData(eq(propCacheKey.getPath()), anyObject(PropStoreWatcher.class), anyObject()))
        .andReturn(VersionedPropCodec.getDefault().toBytes(vProps)).once();

    replay(context, zrw);

    PropStore propStore = new ZooPropStore.Builder(context).build();

    assertNotNull(propStore.get(propCacheKey)); // first call will fetch from ZooKeeper
    assertNotNull(propStore.get(propCacheKey)); // next call will fetch from cache.

    var p = propStore.get(propCacheKey);
    if (p == null) {
      fail("Did not get expected versioned properties - instead got null;");
    } else {
      assertEquals("true", p.getProperties().get(Property.TABLE_BLOOM_ENABLED.getKey()));
    }
  }

  @Test
  public void versionTest() throws Exception {

    PropCacheKey propCacheKey = PropCacheKey.forTable(instanceId, TableId.of("table1"));
    Map<String,String> props =
        Map.of(TABLE_BULK_MAX_TABLETS.getKey(), "1234", TABLE_FILE_BLOCK_SIZE.getKey(), "512M");

    Capture<PropStoreWatcher> propStoreWatcherCapture = newCapture();
    Capture<Stat> stat = newCapture();

    // force version mismatch between zk and props.
    var expectedVersion = 99;
    expect(zrw.getData(eq(propCacheKey.getPath()), capture(propStoreWatcherCapture), capture(stat)))
        .andAnswer(() -> {
          Stat s = stat.getValue();
          s.setCtime(System.currentTimeMillis());
          s.setMtime(System.currentTimeMillis());
          s.setVersion(expectedVersion);
          stat.setValue(s);
          return propCodec.toBytes(new VersionedProperties(props));
        }).once();

    replay(context, zrw);

    PropStore propStore = new ZooPropStore.Builder(context).build();
    var vProps = propStore.get(propCacheKey);
    assertNotNull(vProps);
    assertEquals(expectedVersion, vProps.getDataVersion());
  }

  /**
   * Test that putAll will add and also overwrite properties.
   *
   * @throws Exception
   *           any exception is a test error.
   */
  @Test
  public void putAllTest() throws Exception {

    PropCacheKey propCacheKey = PropCacheKey.forTable(instanceId, TableId.of("table1"));

    var initialProps = new VersionedProperties(0, Instant.now(),
        Map.of(TABLE_BULK_MAX_TABLETS.getKey(), "1234", TABLE_FILE_BLOCK_SIZE.getKey(), "512M"));

    // not cached - will load from ZooKeeper
    expect(zrw.getData(eq(propCacheKey.getPath()), anyObject(Stat.class)))
        .andReturn(propCodec.toBytes(initialProps)).once();

    Capture<byte[]> bytes = newCapture();
    expect(zrw.overwritePersistentData(eq(propCacheKey.getPath()), capture(bytes), eq(0)))
        .andAnswer(() -> {
          var stored = propCodec.fromBytes(0, bytes.getValue());
          assertEquals(3, stored.getProperties().size());
          // overwritten
          assertEquals("4321", stored.getProperties().get(TABLE_BULK_MAX_TABLETS.getKey()));
          // unchanged
          assertEquals("512M", stored.getProperties().get(TABLE_FILE_BLOCK_SIZE.getKey()));
          // new
          assertEquals("123M", stored.getProperties().get(TABLE_SPLIT_THRESHOLD.getKey()));
          return true;
        }).once();

    replay(context, zrw);

    PropStore propStore = new ZooPropStore.Builder(context).build();

    Map<String,String> updateProps =
        Map.of(TABLE_BULK_MAX_TABLETS.getKey(), "4321", TABLE_SPLIT_THRESHOLD.getKey(), "123M");

    propStore.putAll(propCacheKey, updateProps);

    verify(zrw);
  }

  @Test
  public void removeTest() throws Exception {

    PropCacheKey propCacheKey = PropCacheKey.forTable(instanceId, TableId.of("table1"));

    var initialProps = new VersionedProperties(123, Instant.now(),
        Map.of(TABLE_BULK_MAX_TABLETS.getKey(), "1234", TABLE_FILE_BLOCK_SIZE.getKey(), "512M"));

    // not cached - will load from ZooKeeper
    Capture<Stat> stat = newCapture();

    expect(zrw.getData(eq(propCacheKey.getPath()), capture(stat))).andAnswer(() -> {
      Stat s = stat.getValue();
      s.setVersion(123);
      stat.setValue(s);
      return propCodec.toBytes(initialProps);
    }).once();

    Capture<byte[]> bytes = newCapture();
    expect(zrw.overwritePersistentData(eq(propCacheKey.getPath()), capture(bytes), eq(123)))
        .andAnswer(() -> {
          var stored = propCodec.fromBytes(124, bytes.getValue());
          assertEquals(1, stored.getProperties().size());
          // deleted
          assertNull(stored.getProperties().get(TABLE_BULK_MAX_TABLETS.getKey()));
          // unchanged
          assertEquals("512M", stored.getProperties().get(TABLE_FILE_BLOCK_SIZE.getKey()));
          // never existed
          assertNull(stored.getProperties().get(TABLE_SPLIT_THRESHOLD.getKey()));
          return true;
        }).once();

    replay(context, zrw);

    PropStore propStore = new ZooPropStore.Builder(context).build();

    Set<String> deleteNames =
        Set.of(TABLE_BULK_MAX_TABLETS.getKey(), TABLE_SPLIT_THRESHOLD.getKey());

    propStore.removeProperties(propCacheKey, deleteNames);
    verify(zrw);
  }

  @Test
  public void removeWithExceptionsTest() throws Exception {

    PropCacheKey propCacheKey = PropCacheKey.forTable(instanceId, TableId.of("table1"));

    // return "bad data"
    Capture<Stat> stat = newCapture();
    expect(zrw.getData(eq(propCacheKey.getPath()), capture(stat))).andAnswer(() -> {
      Stat s = stat.getValue();
      s.setCtime(System.currentTimeMillis());
      s.setMtime(System.currentTimeMillis());
      s.setCzxid(1234);
      s.setVersion(19);
      stat.setValue(s);
      return new byte[100];
    }).once();

    // mock throwing exceptions
    expect(zrw.getData(eq(propCacheKey.getPath()), anyObject(Stat.class)))
        .andThrow(new KeeperException.NoNodeException("mock forced no node")).once();
    expect(zrw.getData(eq(propCacheKey.getPath()), anyObject(Stat.class)))
        .andThrow(new InterruptedException("mock forced interrupt exception")).once();

    replay(context, zrw);

    PropStore propStore = new ZooPropStore.Builder(context).build();

    Set<String> deleteNames =
        Set.of(TABLE_BULK_MAX_TABLETS.getKey(), TABLE_SPLIT_THRESHOLD.getKey());

    // Parse error converted to PropStoreException
    assertThrows(PropStoreException.class,
        () -> propStore.removeProperties(propCacheKey, deleteNames));
    // ZK exception converted to PropStoreException
    assertThrows(PropStoreException.class,
        () -> propStore.removeProperties(propCacheKey, deleteNames));
    // InterruptException converted to PropStoreException
    assertThrows(PropStoreException.class,
        () -> propStore.removeProperties(propCacheKey, deleteNames));
  }

  @Test
  public void validateWatcherSetTest() throws Exception {

    PropCacheKey propCacheKey = PropCacheKey.forTable(instanceId, TableId.of("table1"));
    Map<String,String> props =
        Map.of(TABLE_BULK_MAX_TABLETS.getKey(), "1234", TABLE_FILE_BLOCK_SIZE.getKey(), "512M");

    Capture<PropStoreWatcher> propStoreWatcherCapture = newCapture();
    Capture<Stat> stat = newCapture();

    // first call loads cache
    expect(zrw.getData(eq(propCacheKey.getPath()), capture(propStoreWatcherCapture), capture(stat)))
        .andAnswer(() -> {
          Stat s = stat.getValue();
          s.setCtime(System.currentTimeMillis());
          s.setMtime(System.currentTimeMillis());
          s.setVersion(13);
          stat.setValue(s);
          return propCodec.toBytes(new VersionedProperties(12, Instant.now(), props));
        }).once();

    // watcher change (re)loads cache on next get call.

    expect(zrw.getData(eq(propCacheKey.getPath()), capture(propStoreWatcherCapture), capture(stat)))
        .andAnswer(() -> {
          Stat s = stat.getValue();
          s.setCtime(System.currentTimeMillis());
          s.setMtime(System.currentTimeMillis());
          s.setVersion(14);
          stat.setValue(s);
          return propCodec.toBytes(new VersionedProperties(13, Instant.now(), props));
        }).once();

    replay(context, zrw);

    PropStore propStore = new ZooPropStore.Builder(context).build();

    assertNotNull(propStore.get(propCacheKey));

    // second call should return from cache - not zookeeper.
    assertNotNull(propStore.get(propCacheKey));

    PropStoreWatcher capturedMonitor = propStoreWatcherCapture.getValue();
    capturedMonitor.signalZkChangeEvent(propCacheKey);
    Thread.sleep(150);

    assertNotNull(propStore.get(propCacheKey));

  }

  /**
   * Verify that a node is created when it does not exist.
   *
   * @throws Exception
   *           any exception is a test failure.
   */
  // @Test
  public void getNoNodeTest() {
    // TODO - implementation requires mocking transform locking
  }

  @Test
  public void deleteTest() throws Exception {

    PropCacheKey propCacheKey = PropCacheKey.forTable(instanceId, TableId.of("propCacheKey"));

    var vProps = new VersionedProperties(Map.of(Property.TABLE_BLOOM_ENABLED.getKey(), "true"));

    // expect first call to load cache.
    expect(zrw.getData(eq(propCacheKey.getPath()), anyObject(PropStoreWatcher.class), anyObject()))
        .andReturn(VersionedPropCodec.getDefault().toBytes(vProps)).once();

    zrw.delete(eq(propCacheKey.getPath()));
    expectLastCall().once();

    replay(context, zrw);

    PropStore propStore = new ZooPropStore.Builder(context).build();

    assertNotNull(propStore.get(propCacheKey)); // first call will fetch from ZooKeeper
    assertNotNull(propStore.get(propCacheKey)); // next call will fetch from cache.
    var p = propStore.get(propCacheKey);
    if (p == null) {
      fail("Did not get expected versioned properties - instead got null;");
    } else {
      assertEquals("true", p.getProperties().get(Property.TABLE_BLOOM_ENABLED.getKey()));
    }
    propStore.delete(propCacheKey);
    Thread.sleep(50);
  }

  // TODO - test invalid until mock can return UUID to validate lock
  // @Test
  public void getUpgradeTest() throws KeeperException, InterruptedException {
    PropCacheKey sysKey = PropCacheKey.forSystem(instanceId);

    String retryPropPath = sysKey.getBasePath() + "/master.bulk.retries";
    String timeoutPropPath = sysKey.getBasePath() + "/master.bulk.timeout";

    expect(zrw.getData(eq(sysKey.getPath()), anyObject(PropStoreWatcher.class), anyObject()))
        .andThrow(new KeeperException.NoNodeException("test sys id does not exist")).once();
    expect(zrw.getChildren(eq(sysKey.getBasePath())))
        .andReturn(List.of("master.bulk.retries", "master.bulk.timeout")).once();

    // transform lock calls
    String lockPath = sysKey.getBasePath() + TransformLock.LOCK_NAME;
    expect(zrw.exists(lockPath)).andReturn(false).once();
    Capture<byte[]> lockId = newCapture();
    zrw.putEphemeralData(eq(lockPath), capture(lockId));
    expectLastCall().once();

    final byte[] saveVal = new byte[64];
    Capture<Stat> lockStat = newCapture();
    expect(zrw.getData(eq(lockPath), capture(lockStat))).andAnswer(() -> {
      byte[] val = lockId.getValue();
      System.arraycopy(val, 0, saveVal, 0, val.length);
      Stat s = lockStat.getValue();
      s.setCtime(System.currentTimeMillis());
      s.setMtime(System.currentTimeMillis());
      s.setVersion(123);
      s.setDataLength(val.length);
      lockStat.setValue(s);
      return val;
    }).once();

    expect(zrw.getData(eq(lockPath))).andReturn(saveVal).once();

    zrw.deleteStrict(eq(lockPath), eq(123));
    expectLastCall().once();

    Capture<Stat> stat1 = newCapture();
    expect(zrw.getData(eq(retryPropPath), capture(stat1))).andAnswer(() -> {
      byte[] val = "4".getBytes(UTF_8);
      Stat s = stat1.getValue();
      s.setCtime(System.currentTimeMillis());
      s.setMtime(System.currentTimeMillis());
      s.setVersion(123);
      s.setDataLength(val.length);
      stat1.setValue(s);
      return val;
    }).once();

    Capture<Stat> stat2 = newCapture();
    expect(zrw.getData(eq(timeoutPropPath), capture(stat2))).andAnswer(() -> {
      byte[] val = "10m".getBytes(UTF_8);
      Stat s = stat2.getValue();
      s.setCtime(System.currentTimeMillis());
      s.setMtime(System.currentTimeMillis());
      s.setVersion(99);
      s.setDataLength(val.length);
      stat2.setValue(s);
      return val;
    }).once();

    Capture<byte[]> bytes = newCapture(CaptureType.ALL);
    expect(zrw.putPersistentData(eq(PropCacheKey.forSystem(instanceId).getPath()), capture(bytes),
        anyObject())).andReturn(true).once();

    expect(zrw.getStatus(eq(sysKey.getPath()), isA(PropStoreWatcher.class))).andAnswer(() -> {
      Stat s = new Stat();
      s.setCtime(System.currentTimeMillis());
      s.setMtime(System.currentTimeMillis());
      s.setVersion(0);
      return s;
    }).once();

    zrw.deleteStrict(eq(retryPropPath), eq(123));
    expectLastCall().once();

    zrw.deleteStrict(eq(timeoutPropPath), eq(99));
    expectLastCall().once();

    replay(context, zrw);

    ZooPropStore propStore = new ZooPropStore.Builder(context).build();
    var vProps = propStore.get(PropCacheKey.forSystem(instanceId));
    assertNull(vProps);

    // assertEquals(0, "4".compareTo(vProps.getProperties().get("master.bulk.retries")));
    // assertEquals(0, "10m".compareTo(vProps.getProperties().get("master.bulk.timeout")));
  }
}

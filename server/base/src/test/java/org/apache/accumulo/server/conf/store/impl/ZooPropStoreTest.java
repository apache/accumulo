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

import static org.apache.accumulo.core.conf.Property.GC_PORT;
import static org.apache.accumulo.core.conf.Property.MANAGER_CLIENTPORT;
import static org.apache.accumulo.core.conf.Property.TABLE_BULK_MAX_TABLETS;
import static org.apache.accumulo.core.conf.Property.TABLE_FILE_BLOCK_SIZE;
import static org.apache.accumulo.core.conf.Property.TABLE_SPLIT_THRESHOLD;
import static org.apache.accumulo.core.conf.Property.TSERV_CLIENTPORT;
import static org.apache.accumulo.core.conf.Property.TSERV_NATIVEMAP_ENABLED;
import static org.apache.accumulo.core.conf.Property.TSERV_SCAN_MAX_OPENFILES;
import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.anyString;
import static org.easymock.EasyMock.capture;
import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.eq;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.newCapture;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.time.Instant;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.fate.zookeeper.ZooReaderWriter;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.conf.codec.VersionedPropCodec;
import org.apache.accumulo.server.conf.codec.VersionedPropGzipCodec;
import org.apache.accumulo.server.conf.codec.VersionedProperties;
import org.apache.accumulo.server.conf.store.PropCacheId;
import org.apache.accumulo.server.conf.store.PropStore;
import org.apache.accumulo.server.conf.store.PropStoreException;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;
import org.easymock.Capture;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZooPropStoreTest {

  private static final Logger log = LoggerFactory.getLogger(ZooPropStoreTest.class);

  private final VersionedPropCodec propCodec = VersionedPropGzipCodec.codec(true);
  private String IID;

  // mocks
  private ServerContext context;
  private ZooReaderWriter zrw;

  @Before
  public void init() throws Exception {
    IID = UUID.randomUUID().toString();
    context = createMock(ServerContext.class);
    zrw = createMock(ZooReaderWriter.class);
    expect(context.getZooReaderWriter()).andReturn(zrw).anyTimes();
    expect(context.getZooKeepersSessionTimeOut()).andReturn(500).anyTimes();
    expect(context.getVersionedPropertiesCodec()).andReturn(propCodec).anyTimes();
    expect(context.getInstanceID()).andReturn(IID).anyTimes();

    expect(zrw.exists(eq("/accumulo/" + IID), anyObject())).andReturn(true).anyTimes();
  }

  @After
  public void verifyMock() {
    verify(context, zrw);
  }

  @Test
  public void initSysPropsTest() throws Exception {
    // checks for system props existence
    expect(zrw.exists(PropCacheId.forSystem(IID).getPath())).andReturn(false).once();

    Capture<byte[]> bytes = newCapture();
    expect(zrw.putPersistentData(eq(PropCacheId.forSystem(IID).getPath()), capture(bytes),
        anyObject())).andReturn(true).once();

    replay(context, zrw);

    ZooPropStore.initSysProps(context,
        Map.of(TABLE_BULK_MAX_TABLETS.getKey(), "1234", TABLE_FILE_BLOCK_SIZE.getKey(), "512M"));

    var decoded = propCodec.fromBytes(bytes.getValue());
    assertNotNull(decoded);
    assertEquals(0, decoded.getDataVersion());
    assertEquals("1234", decoded.getProperties().get(TABLE_BULK_MAX_TABLETS.getKey()));
    assertEquals("512M", decoded.getProperties().get(TABLE_FILE_BLOCK_SIZE.getKey()));
  }

  @Test
  public void create() throws Exception {

    PropCacheId id1 = PropCacheId.forTable(IID, TableId.of("id1"));

    expect(zrw.putPersistentData(eq(id1.getPath()), anyObject(), anyObject())).andReturn(true)
        .once();

    Stat statCheck = new Stat();
    statCheck.setVersion(0); // on create, expect dataVersion == 0;
    expect(zrw.getStatus(anyObject(), anyObject())).andReturn(statCheck).anyTimes();

    replay(context, zrw);

    PropStore propStore = new ZooPropStore.Builder(context).build();

    propStore.create(id1, Map.of());
  }

  @Test
  public void createInvalidVersion() throws Exception {

    expect(zrw.putPersistentData(anyString(), anyObject(), anyObject())).andReturn(true).once();

    Stat statCheck = new Stat();
    statCheck.setVersion(9); // on create, expect dataVersion == 0;
    expect(zrw.getStatus(anyObject(), anyObject())).andReturn(statCheck).anyTimes();

    replay(context, zrw);

    PropStore propStore = new ZooPropStore.Builder(context).build();

    PropCacheId id1 = PropCacheId.forTable(IID, TableId.of("id1"));

    assertThrows(PropStoreException.class, () -> propStore.create(id1, Map.of()));
  }

  /**
   * Simple get tests load from ZooKeeper and then retrieved from cache.
   *
   * @throws Exception
   *           any exception is a test error.
   */
  @Test
  public void getTest() throws Exception {

    PropCacheId id1 = PropCacheId.forTable(IID, TableId.of("id1"));

    var vProps = new VersionedProperties(Map.of(Property.TABLE_BLOOM_ENABLED.getKey(), "true"));

    // expect one ZooKeeper call - subsequent calls should load from cache.
    expect(zrw.getData(eq(id1.getPath()), anyObject(PropStoreWatcher.class), anyObject()))
        .andReturn(VersionedPropGzipCodec.codec(true).toBytes(vProps)).once();

    replay(context, zrw);

    PropStore propStore = new ZooPropStore.Builder(context).build();

    assertNotNull(propStore.get(id1)); // first call will fetch from ZooKeeper
    assertNotNull(propStore.get(id1)); // next call will fetch from cache.
    assertEquals("true",
        propStore.get(id1).getProperties().get(Property.TABLE_BLOOM_ENABLED.getKey()));
  }

  @Test
  public void badVersionTest() throws Exception {

    PropCacheId tid = PropCacheId.forTable(IID, TableId.of("table1"));
    Map<String,String> props =
        Map.of(TABLE_BULK_MAX_TABLETS.getKey(), "1234", TABLE_FILE_BLOCK_SIZE.getKey(), "512M");

    Capture<PropStoreWatcher> propStoreWatcherCapture = newCapture();
    Capture<Stat> stat = newCapture();

    // force version mismatch between zk and props.
    expect(zrw.getData(eq(tid.getPath()), capture(propStoreWatcherCapture), capture(stat)))
        .andAnswer(() -> {
          Stat s = stat.getValue();
          s.setCtime(System.currentTimeMillis());
          s.setMtime(System.currentTimeMillis());
          s.setVersion(99);
          stat.setValue(s);
          return propCodec.toBytes(new VersionedProperties(12, Instant.now(), props));
        }).once();

    replay(context, zrw);

    PropStore propStore = new ZooPropStore.Builder(context).build();
    assertNull(propStore.get(tid));
  }

  /**
   * Test that putAll will add and also overwrite properties.
   *
   * @throws Exception
   *           any exception is a test error.
   */
  @Test
  public void putAllTest() throws Exception {

    PropCacheId tid = PropCacheId.forTable(IID, TableId.of("table1"));

    var initialProps = new VersionedProperties(0, Instant.now(),
        Map.of(TABLE_BULK_MAX_TABLETS.getKey(), "1234", TABLE_FILE_BLOCK_SIZE.getKey(), "512M"));

    // not cached - will load from ZooKeeper
    expect(zrw.getData(eq(tid.getPath()))).andReturn(propCodec.toBytes(initialProps)).once();

    Capture<byte[]> bytes = newCapture();
    expect(zrw.overwritePersistentData(eq(tid.getPath()), capture(bytes), eq(1))).andAnswer(() -> {
      var stored = propCodec.fromBytes(bytes.getValue());
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

    assertTrue(propStore.putAll(tid, updateProps));
  }

  @Test
  public void removeTest() throws Exception {

    PropCacheId tid = PropCacheId.forTable(IID, TableId.of("table1"));

    var initialProps = new VersionedProperties(0, Instant.now(),
        Map.of(TABLE_BULK_MAX_TABLETS.getKey(), "1234", TABLE_FILE_BLOCK_SIZE.getKey(), "512M"));

    // not cached - will load from ZooKeeper
    expect(zrw.getData(eq(tid.getPath()))).andReturn(propCodec.toBytes(initialProps)).once();

    Capture<byte[]> bytes = newCapture();
    expect(zrw.overwritePersistentData(eq(tid.getPath()), capture(bytes), eq(1))).andAnswer(() -> {
      var stored = propCodec.fromBytes(bytes.getValue());
      assertEquals(1, stored.getProperties().size());
      // deleted
      assertNull(stored.getProperties().get(TABLE_BULK_MAX_TABLETS.getKey()));
      // unchanged
      assertEquals("512M", stored.getProperties().get(TABLE_FILE_BLOCK_SIZE.getKey()));
      // never existed
      assertNull("123M", stored.getProperties().get(TABLE_SPLIT_THRESHOLD.getKey()));
      return true;
    }).once();

    replay(context, zrw);

    PropStore propStore = new ZooPropStore.Builder(context).build();

    Set<String> deleteNames =
        Set.of(TABLE_BULK_MAX_TABLETS.getKey(), TABLE_SPLIT_THRESHOLD.getKey());

    assertTrue(propStore.removeProperties(tid, deleteNames));
  }

  @Test
  public void validateWatcherSetTest() throws Exception {

    PropCacheId tid = PropCacheId.forTable(IID, TableId.of("table1"));
    Map<String,String> props =
        Map.of(TABLE_BULK_MAX_TABLETS.getKey(), "1234", TABLE_FILE_BLOCK_SIZE.getKey(), "512M");

    Capture<PropStoreWatcher> propStoreWatcherCapture = newCapture();
    Capture<Stat> stat = newCapture();

    // first call loads cache
    expect(zrw.getData(eq(tid.getPath()), capture(propStoreWatcherCapture), capture(stat)))
        .andAnswer(() -> {
          Stat s = stat.getValue();
          s.setCtime(System.currentTimeMillis());
          s.setMtime(System.currentTimeMillis());
          s.setVersion(13);
          stat.setValue(s);
          return propCodec.toBytes(new VersionedProperties(12, Instant.now(), props));
        }).once();

    // watcher change (re)loads cache on next get call.

    expect(zrw.getData(eq(tid.getPath()), capture(propStoreWatcherCapture), capture(stat)))
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

    assertNotNull(propStore.get(tid));

    // second call should return from cache - not zookeeper.
    assertNotNull(propStore.get(tid));

    PropStoreWatcher capturedMonitor = propStoreWatcherCapture.getValue();
    capturedMonitor.signalZkChangeEvent(tid);
    Thread.sleep(150);

    assertNotNull(propStore.get(tid));

  }

  /**
   * Verify that null is returned and not an exception. No node exception will be caught by loader -
   * and return null on get.
   *
   * @throws Exception
   *           any exception is a test failure.
   */
  @Test
  public void getNoNodeTest() throws Exception {
    PropCacheId id1 = PropCacheId.forTable(IID, TableId.of("id1"));

    expect(zrw.getData(eq(id1.getPath()), anyObject(PropStoreWatcher.class), anyObject()))
        .andThrow(new KeeperException.NoNodeException("testing - forced no node")).anyTimes();

    replay(context, zrw);
    PropStore propStore = new ZooPropStore.Builder(context).build();
    assertNull(propStore.get(id1));

  }

  @Test
  public void readFixed() throws IOException, InterruptedException, KeeperException {
    // test data
    Map<String,
        String> props = Map.of(TSERV_CLIENTPORT.getKey(), "1234", TSERV_NATIVEMAP_ENABLED.getKey(),
            "false", TSERV_SCAN_MAX_OPENFILES.getKey(), "2345", MANAGER_CLIENTPORT.getKey(), "3456",
            GC_PORT.getKey(), "4567");

    VersionedProperties vProps = new VersionedProperties(props);

    // checks for initialization by reading instance id
    expect(zrw.exists(eq("/accumulo/" + IID), anyObject(PropStoreWatcher.class))).andReturn(true)
        .anyTimes();

    expect(zrw.getData(PropCacheId.forSystem(IID).getPath())).andReturn(propCodec.toBytes(vProps))
        .anyTimes();

    replay(context, zrw);

    PropStore propStore = new ZooPropStore.Builder(context).build();

    Map<String,String> fixed = propStore.readFixed();
    log.trace("fixed properties: {}", fixed);

    assertEquals(Property.fixedProperties.size(), fixed.size());
    assertEquals("1234", fixed.get(TSERV_CLIENTPORT.getKey()));
    assertEquals("false", fixed.get(TSERV_NATIVEMAP_ENABLED.getKey()));
  }

  @Test
  public void readFixedDefaults() throws Exception {
    VersionedProperties vProps = new VersionedProperties();

    expect(zrw.getData(eq(PropCacheId.forSystem(IID).getPath())))
        .andReturn(propCodec.toBytes(vProps));
    replay(context, zrw);

    PropStore propStore = new ZooPropStore.Builder(context).build();

    Map<String,String> fixed = propStore.readFixed();
    log.trace("fixed properties: {}", fixed);
    assertEquals(Property.fixedProperties.size(), fixed.size());
    assertEquals(TSERV_CLIENTPORT.getDefaultValue(), fixed.get(TSERV_CLIENTPORT.getKey()));
    assertEquals(TSERV_NATIVEMAP_ENABLED.getDefaultValue(),
        fixed.get(TSERV_NATIVEMAP_ENABLED.getKey()));

  }

  @Test
  public void deleteTest() throws Exception {

    PropCacheId id1 = PropCacheId.forTable(IID, TableId.of("id1"));

    var vProps = new VersionedProperties(Map.of(Property.TABLE_BLOOM_ENABLED.getKey(), "true"));

    // expect first call to load cache.
    expect(zrw.getData(eq(id1.getPath()), anyObject(PropStoreWatcher.class), anyObject()))
        .andReturn(VersionedPropGzipCodec.codec(true).toBytes(vProps)).once();

    zrw.delete(eq(id1.getPath()));
    expectLastCall().once();

    replay(context, zrw);

    PropStore propStore = new ZooPropStore.Builder(context).build();

    assertNotNull(propStore.get(id1)); // first call will fetch from ZooKeeper
    assertNotNull(propStore.get(id1)); // next call will fetch from cache.
    assertEquals("true",
        propStore.get(id1).getProperties().get(Property.TABLE_BLOOM_ENABLED.getKey()));

    propStore.delete(id1);
    Thread.sleep(50);
  }

}

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
import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.capture;
import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.eq;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.newCapture;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;

import org.apache.accumulo.core.data.InstanceId;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.fate.zookeeper.ZooReaderWriter;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.conf.codec.VersionedPropCodec;
import org.apache.accumulo.server.conf.codec.VersionedProperties;
import org.apache.accumulo.server.conf.store.PropChangeListener;
import org.apache.accumulo.server.conf.store.PropStore;
import org.apache.accumulo.server.conf.store.PropStoreKey;
import org.apache.accumulo.server.conf.store.TablePropKey;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.Stat;
import org.easymock.Capture;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class PropStoreEventTest {
  private final VersionedPropCodec propCodec = VersionedPropCodec.getDefault();
  private InstanceId instanceId;

  // mocks
  private ServerContext context;
  private ZooReaderWriter zrw;
  private ReadyMonitor readyMonitor;

  @BeforeEach
  public void initCommonMocks() throws Exception {
    instanceId = InstanceId.of(UUID.randomUUID());
    context = createMock(ServerContext.class);
    zrw = createMock(ZooReaderWriter.class);
    expect(context.getZooReaderWriter()).andReturn(zrw).anyTimes();
    expect(context.getZooKeepersSessionTimeOut()).andReturn(500).anyTimes();
    expect(context.getInstanceID()).andReturn(instanceId).anyTimes();

    expect(zrw.exists(eq("/accumulo/" + instanceId), anyObject())).andReturn(true).anyTimes();

    readyMonitor = createMock(ReadyMonitor.class);
  }

  @AfterEach
  public void verifyMocks() {
    verify(context, zrw, readyMonitor);
  }

  @Test
  public void zkChangeEventTest() throws Exception {

    var tablePropKey = TablePropKey.of(instanceId, TableId.of("a1"));

    PropStoreWatcher watcher = new PropStoreWatcher(readyMonitor);

    WatchedEvent zkEvent = createMock(WatchedEvent.class);
    expect(zkEvent.getPath()).andReturn(tablePropKey.getPath()).once();
    expect(zkEvent.getType()).andReturn(Watcher.Event.EventType.NodeDataChanged);
    readyMonitor.setReady();
    expectLastCall().once();

    replay(context, zrw, readyMonitor, zkEvent);

    PropStore propStore = new ZooPropStore(instanceId, zrw, readyMonitor, watcher, null);
    StoreTestListener listener = new StoreTestListener();

    propStore.registerAsListener(tablePropKey, listener);

    watcher.process(zkEvent);

    Thread.sleep(150);

    assertEquals(1, listener.getZkChangeEventCount());

    verify(zkEvent);
  }

  @Test
  public void deleteEventTest() throws Exception {

    var tablePropKey = TablePropKey.of(instanceId, TableId.of("a1"));

    PropStoreWatcher watcher = new PropStoreWatcher(readyMonitor);

    WatchedEvent zkEvent = createMock(WatchedEvent.class);
    expect(zkEvent.getPath()).andReturn(tablePropKey.getPath()).once();
    expect(zkEvent.getType()).andReturn(Watcher.Event.EventType.NodeDeleted);

    readyMonitor.setReady();
    expectLastCall().once();

    replay(context, zrw, readyMonitor, zkEvent);

    PropStore propStore = new ZooPropStore(instanceId, zrw, readyMonitor, watcher, null);

    StoreTestListener listener = new StoreTestListener();

    propStore.registerAsListener(tablePropKey, listener);

    watcher.process(zkEvent);

    Thread.sleep(150);
    assertEquals(1, listener.getDeleteEventCount());
    verify(zkEvent);

  }

  @Test
  public void disconnectEventTest() throws Exception {

    var tablePropKey = TablePropKey.of(instanceId, TableId.of("a1"));

    PropStoreWatcher watcher = new PropStoreWatcher(readyMonitor);

    WatchedEvent zkEvent = createMock(WatchedEvent.class);
    expect(zkEvent.getType()).andReturn(Watcher.Event.EventType.None);
    expect(zkEvent.getState()).andReturn(Watcher.Event.KeeperState.Disconnected).once();

    readyMonitor.setReady();
    expectLastCall().once();
    readyMonitor.clearReady();
    expectLastCall();

    replay(context, zrw, readyMonitor, zkEvent);

    PropStore propStore = new ZooPropStore(instanceId, zrw, readyMonitor, watcher, null);

    StoreTestListener listener = new StoreTestListener();

    propStore.registerAsListener(tablePropKey, listener);

    watcher.process(zkEvent);

    Thread.sleep(150);
    assertEquals(1, listener.getConnectionEventCount());
    verify(zkEvent);

  }

  @Test
  public void closedEventTest() throws Exception {

    var tablePropKey = TablePropKey.of(instanceId, TableId.of("a1"));

    PropStoreWatcher watcher = new PropStoreWatcher(readyMonitor);

    WatchedEvent zkEvent = createMock(WatchedEvent.class);
    expect(zkEvent.getType()).andReturn(Watcher.Event.EventType.None);
    expect(zkEvent.getState()).andReturn(Watcher.Event.KeeperState.Closed).once();

    readyMonitor.setReady();
    expectLastCall();
    readyMonitor.clearReady();
    expectLastCall();
    readyMonitor.setClosed();
    expectLastCall();

    replay(context, zrw, readyMonitor, zkEvent);

    PropStore propStore = new ZooPropStore(instanceId, zrw, readyMonitor, watcher, null);

    StoreTestListener listener = new StoreTestListener();

    propStore.registerAsListener(tablePropKey, listener);

    watcher.process(zkEvent);

    Thread.sleep(150);
    assertEquals(1, listener.getConnectionEventCount());
    verify(zkEvent);
  }

  @Test
  public void cacheChangeEventTest() throws Exception {

    var tablePropKey = TablePropKey.of(instanceId, TableId.of("a1"));

    PropStoreWatcher watcher = new PropStoreWatcher(readyMonitor);
    readyMonitor.setReady();
    expectLastCall().once();

    replay(context, zrw, readyMonitor);

    PropStore propStore = new ZooPropStore(instanceId, zrw, readyMonitor, watcher, null);

    StoreTestListener listener = new StoreTestListener();

    propStore.registerAsListener(tablePropKey, listener);

    watcher.signalCacheChangeEvent(tablePropKey);

    Thread.sleep(250);
    assertEquals(1, listener.getCacheChangeEventCount());
  }

  @Test
  public void validateWatcherSetTest() throws Exception {
    var tablePropKey = TablePropKey.of(instanceId, TableId.of("a1"));

    Map<String,String> props1 =
        Map.of(TABLE_BULK_MAX_TABLETS.getKey(), "1234", TABLE_FILE_BLOCK_SIZE.getKey(), "512M");

    VersionedProperties vProps = new VersionedProperties(11, Instant.now(), props1);
    Capture<Stat> stat = newCapture();

    // first call loads cache
    expect(zrw.getData(eq(tablePropKey.getPath()), anyObject(), capture(stat))).andAnswer(() -> {
      Stat s = stat.getValue();
      s.setCtime(System.currentTimeMillis());
      s.setMtime(System.currentTimeMillis());
      s.setVersion(12);
      s.setDataLength(propCodec.toBytes(vProps).length);
      stat.setValue(s);
      return propCodec.toBytes(vProps);
    }).once();

    PropStoreWatcher watcher = new PropStoreWatcher(readyMonitor);

    replay(context, zrw, readyMonitor);

    ZooPropLoader loader = new ZooPropLoader(zrw, propCodec, watcher);

    PropCacheCaffeineImpl cache = new PropCacheCaffeineImpl.Builder(loader).build();

    // load cache
    var read1 = cache.get(tablePropKey);
    assertNotNull(read1);
    assertEquals("1234", read1.asMap().get(TABLE_BULK_MAX_TABLETS.getKey()));
    assertEquals("512M", read1.asMap().get(TABLE_FILE_BLOCK_SIZE.getKey()));

    watcher.process(
        new WatchedEvent(Watcher.Event.EventType.NodeDataChanged, null, tablePropKey.getPath()));

  }

  private static class StoreTestListener implements PropChangeListener {

    private int zkChangeEventCount = 0;
    private int cacheChangeEventCount = 0;
    private int deleteEventCount = 0;
    private int connectionEventCount = 0;

    @Override
    public void zkChangeEvent(PropStoreKey<?> propStoreKey) {
      zkChangeEventCount++;
    }

    @Override
    public void cacheChangeEvent(PropStoreKey<?> propStoreKey) {
      cacheChangeEventCount++;
    }

    @Override
    public void deleteEvent(PropStoreKey<?> propStoreKey) {
      deleteEventCount++;
    }

    @Override
    public void connectionEvent() {
      connectionEventCount++;
    }

    public int getZkChangeEventCount() {
      return zkChangeEventCount;
    }

    public int getCacheChangeEventCount() {
      return cacheChangeEventCount;
    }

    public int getDeleteEventCount() {
      return deleteEventCount;
    }

    public int getConnectionEventCount() {
      return connectionEventCount;
    }
  }
}

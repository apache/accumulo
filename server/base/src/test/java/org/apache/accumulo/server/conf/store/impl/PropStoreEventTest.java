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

import static org.apache.accumulo.core.conf.Property.TABLE_BULK_MAX_TABLETS;
import static org.apache.accumulo.core.conf.Property.TABLE_FILE_BLOCK_SIZE;
import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.capture;
import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.eq;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.newCapture;
import static org.easymock.EasyMock.replay;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;

import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.fate.zookeeper.ZooReaderWriter;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.conf.codec.VersionedPropCodec;
import org.apache.accumulo.server.conf.codec.VersionedPropGzipCodec;
import org.apache.accumulo.server.conf.codec.VersionedProperties;
import org.apache.accumulo.server.conf.store.PropCacheId;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.Stat;
import org.easymock.Capture;
import org.junit.Before;
import org.junit.Test;

//TODO - test stub. Modify to use PropStore, not loader

/**
 * Exercise asynchronous events in ZooPropStore.
 */
public class PropStoreEventTest {

  // common test properties
  private CaffeineCacheTest.TestTicker ticker;
  private ServerContext context;
  private PropCacheId propCacheId;
  private VersionedPropCodec propCodec;
  private PropStoreWatcher propStoreWatcher;

  // mocks
  private PropStoreMetrics cacheMetrics;
  private ZooReaderWriter zrw;
  private ReadyMonitor zkReadyMonitor;

  @Before
  public void initCommonMocks() {

    ticker = new CaffeineCacheTest.TestTicker();
    String IID = UUID.randomUUID().toString();
    cacheMetrics = new PropStoreMetrics();

    propCacheId = PropCacheId.forTable(UUID.randomUUID().toString(), TableId.of("t1"));
    propCodec = VersionedPropGzipCodec.codec(true);

    context = createMock(ServerContext.class);
    expect(context.getInstanceID()).andReturn(IID).anyTimes();
    expect(context.getVersionedPropertiesCodec()).andReturn(VersionedPropGzipCodec.codec(true))
        .anyTimes();

    zkReadyMonitor = createMock(ReadyMonitor.class);
    zrw = createMock(ZooReaderWriter.class);

    propStoreWatcher = new PropStoreWatcher(zkReadyMonitor);

  }

  @Test
  public void getChangeMonitorTest() {
    replay(context, zrw);
    // TODO implement test
    // fail("Implement test");
  }

  @Test
  public void validateWatcherSetTest() throws Exception {

    Map<String,String> props1 =
        Map.of(TABLE_BULK_MAX_TABLETS.getKey(), "1234", TABLE_FILE_BLOCK_SIZE.getKey(), "512M");

    VersionedProperties vProps = new VersionedProperties(11, Instant.now(), props1);
    Capture<Stat> stat = newCapture();

    // first call loads cache
    expect(zrw.getData(eq(propCacheId.getPath()), anyObject(), capture(stat))).andAnswer(() -> {
      Stat s = stat.getValue();
      s.setCtime(System.currentTimeMillis());
      s.setMtime(System.currentTimeMillis());
      s.setVersion(12);
      stat.setValue(s);
      return propCodec.toBytes(vProps);
    }).once();

    //
    // expect(zrw.getData(eq(eq(tid.getPath())), changeMonitor, capture(stat))).andAnswer(() -> {
    // Stat s = stat.getValue();
    // s.setCtime(System.currentTimeMillis());
    // s.setMtime(System.currentTimeMillis());
    // s.setVersion(12);
    // stat.setValue(s);
    // return propCodec.toBytes(vProps);
    // }).once();

    // watcher change (re)loads cache on next get call.
    // expect(zrw.getData(eq(tid.getPath()), capture(changeMonitorCapture),
    // capture(stat))).andAnswer(() -> {
    // Stat s = stat.getValue();
    // s.setCtime(System.currentTimeMillis());
    // s.setMtime(System.currentTimeMillis());
    // s.setVersion(13);
    // stat.setValue(s);
    // return propCodec.toBytes(vProps);
    // }).once();

    replay(context, zrw, zkReadyMonitor);

    ZooPropLoader loader = new ZooPropLoader(zrw, propCodec, propStoreWatcher, cacheMetrics);

    CaffeineCache cache =
        new CaffeineCache.Builder(loader, cacheMetrics).withTicker(ticker).build();

    // load cache
    var read1 = cache.get(propCacheId);
    assertNotNull(read1);
    assertEquals("1234", read1.getProperties().get(TABLE_BULK_MAX_TABLETS.getKey()));
    assertEquals("512M", read1.getProperties().get(TABLE_FILE_BLOCK_SIZE.getKey()));

    propStoreWatcher.process(
        new WatchedEvent(Watcher.Event.EventType.NodeDataChanged, null, propCacheId.getPath()));

    // PropStore propStore = new ZooPropStore(context);

    // assertNotNull(propStore.get(tid));

    // second call should return from cache - not zookeeper.
    // assertNotNull(propStore.get(tid));

    // PropStoreChangeMonitor capturedMonitor = changeMonitor.getValue();
    // capturedMonitor.signalZkChangeEvent(tid);
    // Thread.sleep(50);

    // assertNotNull(propStore.get(tid));
  }

}

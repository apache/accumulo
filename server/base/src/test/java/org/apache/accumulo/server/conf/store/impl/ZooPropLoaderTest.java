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

import static org.apache.accumulo.core.conf.Property.GC_PORT;
import static org.apache.accumulo.core.conf.Property.MANAGER_CLIENTPORT;
import static org.apache.accumulo.core.conf.Property.TSERV_CLIENTPORT;
import static org.apache.accumulo.core.conf.Property.TSERV_NATIVEMAP_ENABLED;
import static org.apache.accumulo.core.conf.Property.TSERV_SCAN_MAX_OPENFILES;
import static org.easymock.EasyMock.anyLong;
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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.data.InstanceId;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.fate.zookeeper.ZooReaderWriter;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.conf.codec.VersionedPropCodec;
import org.apache.accumulo.server.conf.codec.VersionedProperties;
import org.apache.accumulo.server.conf.store.PropStoreKey;
import org.apache.accumulo.server.conf.store.SystemPropKey;
import org.apache.accumulo.server.conf.store.TablePropKey;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;
import org.easymock.Capture;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZooPropLoaderTest {

  private static final Logger log = LoggerFactory.getLogger(ZooPropLoaderTest.class);

  private PropCacheCaffeineImplTest.TestTicker ticker;
  private InstanceId instanceId;
  private ServerContext context;
  private PropStoreKey<?> propStoreKey;
  private VersionedPropCodec propCodec;

  // mocks
  private PropStoreMetrics cacheMetrics;
  private PropStoreWatcher propStoreWatcher;
  private ZooReaderWriter zrw;

  private ZooPropLoader loader;

  @BeforeEach
  public void initCommonMocks() {
    ticker = new PropCacheCaffeineImplTest.TestTicker();
    instanceId = InstanceId.of(UUID.randomUUID());

    propStoreKey = TablePropKey.of(instanceId, TableId.of("1abc"));
    propCodec = VersionedPropCodec.getDefault();

    // mocks
    context = createMock(ServerContext.class);
    expect(context.getInstanceID()).andReturn(instanceId).anyTimes();

    zrw = createMock(ZooReaderWriter.class);

    cacheMetrics = createMock(PropStoreMetrics.class);

    propStoreWatcher = createMock(PropStoreWatcher.class);

    // loader used in tests
    loader = new ZooPropLoader(zrw, propCodec, propStoreWatcher, cacheMetrics);

  }

  @AfterEach
  public void verifyCommonMocks() {
    verify(context, zrw, propStoreWatcher, cacheMetrics);
  }

  @Test
  public void loadTest() throws Exception {

    final VersionedProperties defaultProps = new VersionedProperties();
    final byte[] bytes = propCodec.toBytes(defaultProps);
    Capture<Stat> stat = newCapture();
    expect(zrw.getData(eq(propStoreKey.getPath()), anyObject(), capture(stat))).andAnswer(() -> {
      Stat s = stat.getValue();
      s.setCtime(System.currentTimeMillis());
      s.setMtime(System.currentTimeMillis());
      s.setVersion((int) defaultProps.getDataVersion());
      s.setDataLength(bytes.length);
      stat.setValue(s);
      return (bytes);
    }).once();

    cacheMetrics.addLoadTime(anyLong());
    expectLastCall().times(1);

    replay(context, zrw, propStoreWatcher, cacheMetrics);

    assertNotNull(loader.load(propStoreKey));
  }

  // from cache loader

  /**
   * Verify that first call loads from ZooKeeper, then second call returns from the cache.
   *
   * @throws Exception any exception is a test failure.
   */
  @Test
  public void loadAndCacheTest() throws Exception {

    final VersionedProperties defaultProps = new VersionedProperties();
    final byte[] bytes = propCodec.toBytes(defaultProps);

    expect(zrw.getStatus(propStoreKey.getPath())).andThrow(new KeeperException.NoNodeException())
        .anyTimes();

    Capture<Stat> stat = newCapture();
    expect(zrw.getData(eq(propStoreKey.getPath()), isA(PropStoreWatcher.class), capture(stat)))
        .andAnswer(() -> {
          Stat s = stat.getValue();
          s.setCtime(System.currentTimeMillis());
          s.setMtime(System.currentTimeMillis());
          s.setVersion((int) defaultProps.getDataVersion());
          s.setDataLength(bytes.length);
          stat.setValue(s);
          return (bytes);
        }).once();

    cacheMetrics.addLoadTime(anyLong());
    expectLastCall().times(1);

    replay(context, zrw, propStoreWatcher, cacheMetrics);

    PropCacheCaffeineImpl cache =
        new PropCacheCaffeineImpl.Builder(loader, cacheMetrics).forTests(ticker).build();

    // load into cache
    assertNotNull(cache.get(propStoreKey));

    // read cached entry - load count should not change.
    ticker.advance(1, TimeUnit.MINUTES);
    assertNotNull(cache.get(propStoreKey));
  }

  /**
   * Verify that an exception on load result in null value and that the exception does not escape
   * the load call.
   * <p>
   * throws Exception any exception is a test failure.
   */
  @Test
  public void loadFailTest() throws Exception {

    expect(zrw.getData(eq(propStoreKey.getPath()), anyObject(), anyObject()))
        .andThrow(new KeeperException.NoNodeException("force no node exception")).once();

    propStoreWatcher.signalZkChangeEvent(eq(propStoreKey));
    expectLastCall();

    cacheMetrics.incrZkError();
    expectLastCall().once();

    replay(context, zrw, propStoreWatcher, cacheMetrics);

    PropCacheCaffeineImpl cache =
        new PropCacheCaffeineImpl.Builder(loader, cacheMetrics).forTests(ticker).build();

    assertNull(cache.get(propStoreKey));

    log.debug("Metrics: {}", cacheMetrics);
  }

  /**
   * Validate that cache expiration functions as expected.
   * <p>
   * throws Exception any exception is a test failure.
   */
  @Test
  public void expireTest() throws Exception {

    VersionedProperties defaultProps = new VersionedProperties();
    byte[] bytes = propCodec.toBytes(defaultProps);

    Capture<Stat> stat = newCapture();
    expect(zrw.getData(eq(propStoreKey.getPath()), isA(PropStoreWatcher.class), capture(stat)))
        .andAnswer(() -> {
          Stat s = stat.getValue();
          s.setCtime(System.currentTimeMillis());
          s.setMtime(System.currentTimeMillis());
          s.setVersion((int) defaultProps.getDataVersion());
          s.setDataLength(bytes.length);
          stat.setValue(s);
          return (bytes);
        }).times(2);

    cacheMetrics.addLoadTime(anyLong());
    expectLastCall().times(2);

    cacheMetrics.incrEviction();
    expectLastCall().once();

    replay(context, zrw, propStoreWatcher, cacheMetrics);

    PropCacheCaffeineImpl cache =
        new PropCacheCaffeineImpl.Builder(loader, cacheMetrics).forTests(ticker).build();

    // load cache
    assertNotNull(cache.get(propStoreKey));

    ticker.advance(70, TimeUnit.MINUTES);

    assertNotNull(cache.get(propStoreKey));

  }

  /**
   * Test that a ZooKeeper exception on an async reload task is correctly handed and that the value
   * is removed from the cache when the refresh fails and the next get.
   *
   * @throws Exception if a test error occurs.
   */
  @Test
  public void reloadExceptionTest() throws Exception {

    final VersionedProperties defaultProps = new VersionedProperties();
    final byte[] bytes = propCodec.toBytes(defaultProps);

    Capture<Stat> stat = newCapture();
    expect(zrw.getData(eq(propStoreKey.getPath()), isA(PropStoreWatcher.class), capture(stat)))
        .andAnswer(() -> {
          Stat s = stat.getValue();
          s.setCtime(System.currentTimeMillis());
          s.setMtime(System.currentTimeMillis());
          s.setVersion((int) defaultProps.getDataVersion());
          s.setDataLength(bytes.length);
          stat.setValue(s);
          return (bytes);
        }).once();

    expect(zrw.getData(eq(propStoreKey.getPath()), anyObject(), anyObject()))
        .andThrow(new KeeperException.NoNodeException("forced no node")).anyTimes();

    propStoreWatcher.signalZkChangeEvent(anyObject());
    expectLastCall().anyTimes();

    propStoreWatcher.signalCacheChangeEvent(anyObject());
    expectLastCall().anyTimes();

    cacheMetrics.addLoadTime(anyLong());
    expectLastCall().times(1);
    cacheMetrics.incrEviction();
    expectLastCall().times(1);
    cacheMetrics.incrZkError();
    expectLastCall().times(1);

    replay(context, zrw, propStoreWatcher, cacheMetrics);

    PropCacheCaffeineImpl cache =
        new PropCacheCaffeineImpl.Builder(loader, cacheMetrics).forTests(ticker).build();

    // prime cache
    assertNotNull(cache.get(propStoreKey));

    ticker.advance(5, TimeUnit.MINUTES);

    // read cached value
    assertNotNull(cache.get(propStoreKey));

    // advance so expire called.
    ticker.advance(120, TimeUnit.MINUTES);

    assertNull(cache.get(propStoreKey));
  }

  @Test
  public void getIfCachedTest() {

    replay(context, zrw, propStoreWatcher, cacheMetrics);

    PropCacheCaffeineImpl cache =
        new PropCacheCaffeineImpl.Builder(loader, cacheMetrics).forTests(ticker).build();

    assertNull(cache.getIfCached(propStoreKey));

  }

  @Test
  public void removeTest() throws Exception {
    final var sysPropKey = SystemPropKey.of(instanceId);
    final var tablePropKey = TablePropKey.of(instanceId, TableId.of("t1"));

    final VersionedProperties defaultProps = new VersionedProperties();
    final byte[] bytes = propCodec.toBytes(defaultProps);

    Capture<Stat> stat = newCapture();
    expect(zrw.getData(eq(sysPropKey.getPath()), isA(PropStoreWatcher.class), capture(stat)))
        .andAnswer(() -> {
          Stat s = stat.getValue();
          s.setCtime(System.currentTimeMillis());
          s.setMtime(System.currentTimeMillis());
          s.setVersion((int) defaultProps.getDataVersion());
          s.setDataLength(bytes.length);
          stat.setValue(s);
          return (bytes);
        }).once();

    Capture<Stat> stat1 = newCapture();
    expect(zrw.getData(eq(tablePropKey.getPath()), isA(PropStoreWatcher.class), capture(stat1)))
        .andAnswer(() -> {
          Stat s = stat1.getValue();
          s.setCtime(System.currentTimeMillis());
          s.setMtime(System.currentTimeMillis());
          s.setVersion((int) defaultProps.getDataVersion());
          s.setDataLength(bytes.length);
          stat1.setValue(s);
          return (bytes);
        }).once();

    cacheMetrics.addLoadTime(anyLong());
    expectLastCall().times(2);

    replay(context, zrw, propStoreWatcher, cacheMetrics);

    PropCacheCaffeineImpl cache =
        new PropCacheCaffeineImpl.Builder(loader, cacheMetrics).forTests(ticker).build();

    // load into cache
    assertNotNull(cache.get(sysPropKey));
    assertNotNull(cache.get(tablePropKey));

    cache.remove(tablePropKey);

    // verify retrieved from cache without loading.
    assertNotNull(cache.getIfCached(sysPropKey));
    assertNull(cache.getIfCached(tablePropKey));
  }

  @Test
  public void removeAllTest() throws Exception {
    final var sysPropKey = SystemPropKey.of(instanceId);
    final var tablePropKey = TablePropKey.of(instanceId, TableId.of("t1"));

    final VersionedProperties defaultProps = new VersionedProperties();
    final byte[] bytes = propCodec.toBytes(defaultProps);

    Capture<Stat> stat = newCapture();
    expect(zrw.getData(eq(sysPropKey.getPath()), isA(PropStoreWatcher.class), capture(stat)))
        .andAnswer(() -> {
          Stat s = stat.getValue();
          s.setCtime(System.currentTimeMillis());
          s.setMtime(System.currentTimeMillis());
          s.setVersion((int) defaultProps.getDataVersion());
          s.setDataLength(bytes.length);
          stat.setValue(s);
          return (bytes);
        }).once();

    Capture<Stat> stat1 = newCapture();
    expect(zrw.getData(eq(tablePropKey.getPath()), isA(PropStoreWatcher.class), capture(stat1)))
        .andAnswer(() -> {
          Stat s = stat1.getValue();
          s.setCtime(System.currentTimeMillis());
          s.setMtime(System.currentTimeMillis());
          s.setVersion((int) defaultProps.getDataVersion());
          s.setDataLength(bytes.length);
          stat1.setValue(s);
          return (bytes);
        }).once();

    cacheMetrics.addLoadTime(anyLong());
    expectLastCall().times(2);

    replay(context, zrw, propStoreWatcher, cacheMetrics);

    PropCacheCaffeineImpl cache =
        new PropCacheCaffeineImpl.Builder(loader, cacheMetrics).forTests(ticker).build();

    // load into cache
    assertNotNull(cache.get(sysPropKey));
    assertNotNull(cache.get(tablePropKey));

    cache.removeAll();

    // verify retrieved from cache without loading.
    assertNull(cache.getIfCached(sysPropKey));
    assertNull(cache.getIfCached(tablePropKey));
  }

  @Test
  public void getIfCachedNotPresentTest() {
    replay(context, zrw, propStoreWatcher, cacheMetrics);

    PropCacheCaffeineImpl cache =
        new PropCacheCaffeineImpl.Builder(loader, cacheMetrics).forTests(ticker).build();

    // load into cache
    assertNull(cache.getIfCached(propStoreKey));
  }

  @Test
  public void captureExampleTest() throws Exception {

    Map<String,String> props = new HashMap<>();
    props.put(TSERV_CLIENTPORT.getKey(), "1234");
    props.put(TSERV_NATIVEMAP_ENABLED.getKey(), "false");
    props.put(TSERV_SCAN_MAX_OPENFILES.getKey(), "2345");
    props.put(MANAGER_CLIENTPORT.getKey(), "3456");
    props.put(GC_PORT.getKey(), "4567");
    VersionedProperties vProps = new VersionedProperties(8, Instant.now(), props);

    Capture<String> path = newCapture();
    Capture<Stat> stat = newCapture();

    expect(zrw.getData(capture(path), anyObject(), capture(stat))).andAnswer(() -> {
      Stat r = stat.getValue();
      r.setCzxid(1234);
      r.setVersion(9);
      stat.setValue(r);
      return propCodec.toBytes(vProps);
    }).anyTimes();

    replay(context, zrw, propStoreWatcher, cacheMetrics);

    Stat statCheck = new Stat();
    statCheck.setVersion(9);

  }

}

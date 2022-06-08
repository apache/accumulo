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
import static org.apache.accumulo.core.conf.Property.TSERV_CLIENTPORT;
import static org.apache.accumulo.core.conf.Property.TSERV_NATIVEMAP_ENABLED;
import static org.apache.accumulo.core.conf.Property.TSERV_SCAN_MAX_OPENFILES;
import static org.apache.accumulo.server.conf.store.impl.PropCacheCaffeineImpl.REFRESH_MIN;
import static org.easymock.EasyMock.anyLong;
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
import static org.junit.jupiter.api.Assertions.assertNull;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.InstanceId;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.fate.zookeeper.ZooReaderWriter;
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

    VersionedProperties defaultProps = new VersionedProperties();

    expect(zrw.getData(eq(propStoreKey.getPath()), anyObject(), anyObject()))
        .andReturn(propCodec.toBytes(defaultProps)).anyTimes();

    cacheMetrics.addLoadTime(anyLong());
    expectLastCall().times(1);

    replay(context, zrw, propStoreWatcher, cacheMetrics);

    assertNotNull(loader.load(propStoreKey));
  }

  // from cache loader

  /**
   * Verify that first call loads from ZooKeeper, then second call returns from the cache.
   *
   * @throws Exception
   *           any exception is a test failure.
   */
  @Test
  public void loadAndCacheTest() throws Exception {

    VersionedProperties defaultProps = new VersionedProperties();

    expect(zrw.getStatus(propStoreKey.getPath())).andThrow(new KeeperException.NoNodeException())
        .anyTimes();
    expect(zrw.getData(eq(propStoreKey.getPath()), anyObject(), anyObject()))
        .andReturn(propCodec.toBytes(defaultProps)).once();

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

  @Test
  public void getExpireTimeoutTest() {
    replay(context, zrw, propStoreWatcher, cacheMetrics);
    // TODO check into implementing an explicit test.
    // fail("Implement test");
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

    expect(zrw.getData(eq(propStoreKey.getPath()), anyObject(), anyObject()))
        .andReturn(propCodec.toBytes(defaultProps)).times(2);

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
   * @throws Exception
   *           if a test error occurs.
   */
  @Test
  public void reloadExceptionTest() throws Exception {

    final VersionedProperties defaultProps = new VersionedProperties();

    expect(zrw.getData(eq(propStoreKey.getPath()), anyObject(), anyObject()))
        .andReturn(propCodec.toBytes(defaultProps)).once();

    Stat stat = new Stat();
    stat.setVersion(123); // set different version so reload triggered
    expect(zrw.getStatus(propStoreKey.getPath())).andReturn(stat).once();

    expect(zrw.getData(eq(propStoreKey.getPath()), anyObject(), anyObject()))
        .andThrow(new KeeperException.NoNodeException("forced no node")).anyTimes();

    propStoreWatcher.signalZkChangeEvent(anyObject());
    expectLastCall().anyTimes();

    propStoreWatcher.signalCacheChangeEvent(anyObject());
    expectLastCall().anyTimes();

    cacheMetrics.addLoadTime(anyLong());
    expectLastCall().times(1);
    cacheMetrics.incrRefresh();
    expectLastCall().times(1);
    cacheMetrics.incrRefreshLoad();
    expectLastCall().times(1);
    cacheMetrics.incrZkError();
    expectLastCall().times(2);

    replay(context, zrw, propStoreWatcher, cacheMetrics);

    PropCacheCaffeineImpl cache =
        new PropCacheCaffeineImpl.Builder(loader, cacheMetrics).forTests(ticker).build();

    // prime cache
    assertNotNull(cache.get(propStoreKey));

    ticker.advance(5, TimeUnit.MINUTES);

    // read cached value
    assertNotNull(cache.get(propStoreKey));

    // advance so refresh called.
    ticker.advance(20, TimeUnit.MINUTES);

    assertNotNull(cache.get(propStoreKey));

    assertNull(cache.get(propStoreKey));
  }

  @Test
  public void getWithoutCachingTest() {

    replay(context, zrw, propStoreWatcher, cacheMetrics);

    PropCacheCaffeineImpl cache =
        new PropCacheCaffeineImpl.Builder(loader, cacheMetrics).forTests(ticker).build();

    assertNull(cache.getWithoutCaching(propStoreKey));

  }

  @Test
  public void removeTest() throws Exception {
    final var sysPropKey = SystemPropKey.of(instanceId);
    final var tablePropKey = TablePropKey.of(instanceId, TableId.of("t1"));

    VersionedProperties defaultProps = new VersionedProperties();

    expect(zrw.getData(eq(sysPropKey.getPath()), anyObject(), anyObject()))
        .andReturn(propCodec.toBytes(defaultProps)).once();
    expect(zrw.getData(eq(tablePropKey.getPath()), anyObject(), anyObject()))
        .andReturn(propCodec.toBytes(defaultProps)).once();

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
    assertNotNull(cache.getWithoutCaching(sysPropKey));
    assertNull(cache.getWithoutCaching(tablePropKey));
  }

  @Test
  public void removeAllTest() throws Exception {
    final var sysPropKey = SystemPropKey.of(instanceId);
    final var tablePropKey = TablePropKey.of(instanceId, TableId.of("t1"));

    VersionedProperties defaultProps = new VersionedProperties();

    expect(zrw.getData(eq(sysPropKey.getPath()), anyObject(), anyObject()))
        .andReturn(propCodec.toBytes(defaultProps)).once();
    expect(zrw.getData(eq(tablePropKey.getPath()), anyObject(), anyObject()))
        .andReturn(propCodec.toBytes(defaultProps)).once();

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
    assertNull(cache.getWithoutCaching(sysPropKey));
    assertNull(cache.getWithoutCaching(tablePropKey));
  }

  @Test
  public void getWithoutCachingNotPresentTest() {
    replay(context, zrw, propStoreWatcher, cacheMetrics);

    PropCacheCaffeineImpl cache =
        new PropCacheCaffeineImpl.Builder(loader, cacheMetrics).forTests(ticker).build();

    // load into cache
    assertNull(cache.getWithoutCaching(propStoreKey));
  }

  @Test
  public void refreshTest() throws Exception {

    VersionedProperties defaultProps = new VersionedProperties();

    // first call loads cache
    Capture<Stat> stat = newCapture();
    expect(zrw.getData(eq(propStoreKey.getPath()), anyObject(), capture(stat))).andAnswer(() -> {
      Stat s = stat.getValue();
      s.setCtime(System.currentTimeMillis());
      s.setMtime(System.currentTimeMillis());
      s.setCzxid(1234);
      s.setVersion(0);
      stat.setValue(s);
      return propCodec.toBytes(defaultProps);
    }).times(1);

    Stat expectedStat = new Stat();
    expectedStat.setVersion(0);
    expect(zrw.getStatus(propStoreKey.getPath())).andReturn(expectedStat).times(2);

    cacheMetrics.addLoadTime(anyLong());
    expectLastCall().times(1);
    cacheMetrics.incrRefresh();
    expectLastCall().times(2);

    replay(context, zrw, propStoreWatcher, cacheMetrics);

    PropCacheCaffeineImpl cache =
        new PropCacheCaffeineImpl.Builder(loader, cacheMetrics).forTests(ticker).build();

    // load cache
    log.debug("received: {}", cache.get(propStoreKey));

    ticker.advance(REFRESH_MIN + 1, TimeUnit.MINUTES);

    assertNotNull(cache.get(propStoreKey));

    ticker.advance(REFRESH_MIN / 2, TimeUnit.MINUTES);

    assertNotNull(cache.get(propStoreKey));

    ticker.advance(REFRESH_MIN + 1, TimeUnit.MINUTES);

    assertNotNull(cache.get(propStoreKey));

    ticker.advance(1, TimeUnit.MINUTES);

    assertNotNull(cache.get(propStoreKey));

  }

  /**
   * Test that when the refreshAfterWrite period expires that the data version is checked against
   * stored value - and on mismatch, rereads the values from ZooKeeper.
   */
  @Test
  public void refreshDifferentVersionTest() throws Exception {

    final int initialVersion = 123;
    Capture<PropStoreWatcher> propStoreWatcherCapture = newCapture();

    Capture<Stat> stat = newCapture();

    expect(zrw.getData(eq(propStoreKey.getPath()), capture(propStoreWatcherCapture), capture(stat)))
        .andAnswer(() -> {
          Stat s = stat.getValue();
          s.setCtime(System.currentTimeMillis());
          s.setMtime(System.currentTimeMillis());
          s.setVersion(initialVersion + 1);
          stat.setValue(s);
          return propCodec.toBytes(new VersionedProperties(initialVersion + 1, Instant.now(),
              Map.of(Property.TABLE_SPLIT_THRESHOLD.getKey(), "7G")));
        }).once();

    // make it look like version on ZK has advanced.
    Stat stat2 = new Stat();
    stat2.setVersion(initialVersion + 3); // initSysProps 123, on write 124
    expect(zrw.getStatus(propStoreKey.getPath())).andReturn(stat2).once();

    Capture<Stat> stat3 = newCapture();

    expect(
        zrw.getData(eq(propStoreKey.getPath()), capture(propStoreWatcherCapture), capture(stat3)))
        .andAnswer(() -> {
          Stat s = stat3.getValue();
          s.setCtime(System.currentTimeMillis());
          s.setMtime(System.currentTimeMillis());
          s.setVersion(initialVersion + 4);
          stat3.setValue(s);
          return propCodec.toBytes(new VersionedProperties(initialVersion + 3, Instant.now(),
              Map.of(Property.TABLE_SPLIT_THRESHOLD.getKey(), "12G")));
        }).once();

    propStoreWatcher.signalCacheChangeEvent(eq(propStoreKey));
    expectLastCall();

    cacheMetrics.addLoadTime(anyLong());
    expectLastCall().times(2);

    cacheMetrics.incrRefresh();
    expectLastCall().times(1);

    cacheMetrics.incrRefreshLoad();
    expectLastCall().times(1);

    replay(context, zrw, propStoreWatcher, cacheMetrics);

    PropCacheCaffeineImpl cache =
        new PropCacheCaffeineImpl.Builder(loader, cacheMetrics).forTests(ticker).build();

    // prime cache
    var origProps = cache.get(propStoreKey);
    assertNotNull(origProps);
    assertEquals("7G", origProps.asMap().get(Property.TABLE_SPLIT_THRESHOLD.getKey()));

    ticker.advance(REFRESH_MIN + 1, TimeUnit.MINUTES);
    // first call after refresh return original and schedules update
    var originalProps = cache.get(propStoreKey);
    assertNotNull(originalProps);
    assertNotNull(originalProps.asMap().get(Property.TABLE_SPLIT_THRESHOLD.getKey()));

    // refresh should have loaded updated value;
    var updatedProps = cache.get(propStoreKey);
    log.debug("Updated props: {}", updatedProps == null ? "null" : updatedProps.print(true));

    assertNotNull(updatedProps);

    assertEquals("12G", updatedProps.asMap().get(Property.TABLE_SPLIT_THRESHOLD.getKey()));
  }

  /**
   * Test that when the refreshAfterWrite period expires that the data version is checked against
   * stored value - and on match, returns the current value without rereading the values from
   * ZooKeeper.
   *
   * @throws Exception
   *           any exception is a test failure
   */
  @Test
  public void refreshSameVersionTest() throws Exception {

    final int expectedVersion = 123;

    VersionedProperties mockProps = createMock(VersionedProperties.class);
    expect(mockProps.getTimestamp()).andReturn(Instant.now()).once();
    expect(mockProps.asMap()).andReturn(Map.of());

    Capture<Stat> stat = newCapture();

    // first call loads cache
    expect(zrw.getData(eq(propStoreKey.getPath()), anyObject(), capture(stat))).andAnswer(() -> {
      Stat s = stat.getValue();
      s.setCtime(System.currentTimeMillis());
      s.setMtime(System.currentTimeMillis());
      s.setVersion(expectedVersion);
      stat.setValue(s);
      return propCodec.toBytes(mockProps);
    }).times(1);

    Stat stat2 = new Stat();
    stat2.setCtime(System.currentTimeMillis());
    stat2.setMtime(System.currentTimeMillis());
    stat2.setVersion(expectedVersion);

    expect(zrw.getStatus(propStoreKey.getPath())).andReturn(stat2).once();

    cacheMetrics.addLoadTime(anyLong());
    expectLastCall().times(1);
    cacheMetrics.incrRefresh();
    expectLastCall().times(1);

    replay(context, zrw, propStoreWatcher, cacheMetrics, mockProps);

    PropCacheCaffeineImpl cache =
        new PropCacheCaffeineImpl.Builder(loader, cacheMetrics).forTests(ticker).build();

    // prime cache
    cache.get(propStoreKey);

    ticker.advance(30, TimeUnit.MINUTES);

    VersionedProperties vPropsRead = cache.get(propStoreKey);

    assertNotNull(vPropsRead);

    cache.get(propStoreKey);

    verify(mockProps);
  }

  /**
   * reload exception - exception thrown reading Stat to check version.
   *
   * @throws Exception
   *           any exception is a test failure.
   */
  @Test
  public void refreshExceptionTest() throws Exception {
    VersionedProperties defaultProps = new VersionedProperties();

    // first call loads cache
    Capture<Stat> stat = newCapture();
    expect(zrw.getData(eq(propStoreKey.getPath()), anyObject(), capture(stat))).andAnswer(() -> {
      Stat s = stat.getValue();
      s.setCtime(System.currentTimeMillis());
      s.setMtime(System.currentTimeMillis());
      s.setCzxid(1234);
      s.setVersion(0);
      stat.setValue(s);
      return propCodec.toBytes(defaultProps);
    }).times(1);

    Stat expectedStat = new Stat();
    expectedStat.setVersion(0);
    expect(zrw.getStatus(propStoreKey.getPath()))
        .andThrow(new KeeperException.NoNodeException("force no node exception")).once();

    propStoreWatcher.signalZkChangeEvent(eq(propStoreKey));
    expectLastCall().anyTimes();

    expect(zrw.getData(eq(propStoreKey.getPath()), anyObject(), anyObject()))
        .andThrow(new KeeperException.NoNodeException("force no node exception")).once();

    cacheMetrics.addLoadTime(anyLong());
    expectLastCall().times(1);
    cacheMetrics.incrRefresh();
    expectLastCall().times(1);
    cacheMetrics.incrZkError();
    expectLastCall().times(2);

    replay(context, zrw, propStoreWatcher, cacheMetrics);

    PropCacheCaffeineImpl cache =
        new PropCacheCaffeineImpl.Builder(loader, cacheMetrics).forTests(ticker).build();

    // load cache
    log.debug("received: {}", cache.get(propStoreKey));

    ticker.advance(REFRESH_MIN + 1, TimeUnit.MINUTES);

    assertNotNull(cache.get(propStoreKey)); // returns current and queues async refresh

    assertNull(cache.get(propStoreKey)); // on exception, the loader should return null

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

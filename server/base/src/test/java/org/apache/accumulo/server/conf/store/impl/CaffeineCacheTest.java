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
import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.eq;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.conf.codec.VersionedPropGzipCodec;
import org.apache.accumulo.server.conf.codec.VersionedProperties;
import org.apache.accumulo.server.conf.store.PropCacheId;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.github.benmanes.caffeine.cache.Ticker;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

public class CaffeineCacheTest {

  private String IID;
  private ServerContext context;
  private PropStoreWatcher propStoreWatcher;
  private ZooPropLoader zooPropLoader;
  private TestTicker ticker;

  private PropCacheId tableId;
  private VersionedProperties vProps;

  private CaffeineCache cache = null;

  @Before
  @SuppressFBWarnings(value = "PREDICTABLE_RANDOM",
      justification = "random used for testing with variable names")
  public void init() {
    ticker = new TestTicker();
    IID = UUID.randomUUID().toString();
    PropStoreMetrics cacheMetrics = new PropStoreMetrics();

    tableId =
        PropCacheId.forTable(IID, TableId.of("t" + ThreadLocalRandom.current().nextInt(1, 1000)));

    Map<String,String> props =
        Map.of(TABLE_BULK_MAX_TABLETS.getKey(), "1234", TABLE_FILE_BLOCK_SIZE.getKey(), "512M");

    vProps = new VersionedProperties(props);

    // mocks
    context = createMock(ServerContext.class);
    propStoreWatcher = createMock(PropStoreWatcher.class);
    zooPropLoader = createMock(ZooPropLoader.class);

    expect(context.getInstanceID()).andReturn(IID).anyTimes();
    expect(context.getVersionedPropertiesCodec()).andReturn(VersionedPropGzipCodec.codec(true))
        .anyTimes();

    cache = new CaffeineCache.Builder(zooPropLoader, cacheMetrics).withTicker(ticker).build();

  }

  @After
  public void verifyMocks() {
    verify(context, propStoreWatcher, zooPropLoader);
  }

  @Test
  public void getTest() {
    expect(zooPropLoader.load(eq(tableId))).andReturn(vProps).once();
    replay(context, propStoreWatcher, zooPropLoader);
    assertNotNull(cache.get(tableId)); // will call load
    assertNotNull(cache.get(tableId)); // will read from cache
  }

  /**
   * Verifies that value is not cached
   */
  @SuppressFBWarnings(value = "PREDICTABLE_RANDOM",
      justification = "random used for testing with variable names")
  @Test
  public void getNoCacheTest() {
    PropCacheId tableId2 =
        PropCacheId.forTable(IID, TableId.of("t2" + ThreadLocalRandom.current().nextInt(1, 1000)));

    expect(zooPropLoader.load(eq(tableId2))).andReturn(vProps).once();

    replay(context, propStoreWatcher, zooPropLoader);
    var props = cache.getWithoutCaching(tableId);
    assertNull(props);

    assertNotNull(cache.get(tableId2)); // load into cache
    assertNotNull(cache.getWithoutCaching(tableId2)); // read from cache - no load call.
  }

  @Test
  public void removeTest() {
    expect(zooPropLoader.load(eq(tableId))).andReturn(vProps).times(2);
    replay(context, propStoreWatcher, zooPropLoader);
    assertNotNull(cache.get(tableId)); // will call load
    cache.remove(tableId);
    assertNotNull(cache.get(tableId)); // will call load again
  }

  @SuppressFBWarnings(value = "PREDICTABLE_RANDOM",
      justification = "random used for testing with variable names")
  @Test
  public void removeAllTest() {
    PropCacheId tableId2 =
        PropCacheId.forTable(IID, TableId.of("t2" + ThreadLocalRandom.current().nextInt(1, 1000)));

    expect(zooPropLoader.load(eq(tableId))).andReturn(vProps).once();
    expect(zooPropLoader.load(eq(tableId2))).andReturn(vProps).once();

    replay(context, propStoreWatcher, zooPropLoader);

    assertNotNull(cache.get(tableId)); // will call load
    assertNotNull(cache.get(tableId2)); // will call load

    cache.removeAll();

    // check that values are not in cache - will not call load
    assertNull(cache.getWithoutCaching(tableId));
    assertNull(cache.getWithoutCaching(tableId2));
  }

  VersionedProperties asyncProps() {
    return vProps;
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  @Test
  public void refreshTest() throws Exception {

    expect(zooPropLoader.load(eq(tableId))).andReturn(vProps).once();

    CompletableFuture future = CompletableFuture.supplyAsync(this::asyncProps);

    expect(zooPropLoader.asyncReload(eq(tableId), eq(vProps), anyObject())).andReturn(future)
        .once();

    replay(context, propStoreWatcher, zooPropLoader);
    assertNotNull(cache.get(tableId)); // will call load and place into cache

    ticker.advance(30, TimeUnit.MINUTES);
    cache.cleanUp();
    assertNotNull(cache.get(tableId)); // will async check stat and then reload
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  @Test
  public void expireTest() throws Exception {
    expect(zooPropLoader.load(eq(tableId))).andReturn(vProps).times(2);

    CompletableFuture future = CompletableFuture.supplyAsync(this::asyncProps);

    replay(context, propStoreWatcher, zooPropLoader);
    assertNotNull(cache.get(tableId)); // will call load

    ticker.advance(90, TimeUnit.MINUTES);
    cache.cleanUp();
    assertNotNull(cache.get(tableId)); // expired - will call load.
  }

  @Test
  public void getExceptionTest() {
    expect(zooPropLoader.load(eq(tableId)))
        .andThrow(new IllegalStateException("forced test exception")).once();
    replay(context, propStoreWatcher, zooPropLoader);
    assertNull(cache.get(tableId)); // will call load
  }

  public static class TestTicker implements Ticker {

    private final long startTime;
    private long elapsed;

    public TestTicker() {
      startTime = System.nanoTime();
      elapsed = 0L;
    }

    public void advance(final long value, final TimeUnit units) {
      elapsed += TimeUnit.NANOSECONDS.convert(value, units);
    }

    @Override
    public long read() {
      return startTime + elapsed;
    }
  }

}

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
import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.eq;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.data.InstanceId;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.conf.codec.VersionedProperties;
import org.apache.accumulo.server.conf.store.TablePropKey;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.github.benmanes.caffeine.cache.Ticker;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

public class PropCacheCaffeineImplTest {

  private InstanceId instanceId;
  private ServerContext context;
  private PropStoreWatcher propStoreWatcher;
  private ZooPropLoader zooPropLoader;
  private TestTicker ticker;

  private TablePropKey tablePropKey;
  private VersionedProperties vProps;

  private PropCacheCaffeineImpl cache = null;

  @BeforeEach
  @SuppressFBWarnings(value = "PREDICTABLE_RANDOM",
      justification = "random used for testing with variable names")
  public void init() {
    ticker = new TestTicker();
    instanceId = InstanceId.of(UUID.randomUUID());

    tablePropKey =
        TablePropKey.of(instanceId, TableId.of("t" + ThreadLocalRandom.current().nextInt(1, 1000)));

    Map<String,String> props =
        Map.of(TABLE_BULK_MAX_TABLETS.getKey(), "1234", TABLE_FILE_BLOCK_SIZE.getKey(), "512M");

    vProps = new VersionedProperties(props);

    // mocks
    context = createMock(ServerContext.class);
    propStoreWatcher = createMock(PropStoreWatcher.class);
    zooPropLoader = createMock(ZooPropLoader.class);

    expect(context.getInstanceID()).andReturn(instanceId).anyTimes();

    cache = new PropCacheCaffeineImpl.Builder(zooPropLoader).forTests(ticker).build();

  }

  @AfterEach
  public void verifyMocks() {
    verify(context, propStoreWatcher, zooPropLoader);
  }

  @Test
  public void getTest() {
    expect(zooPropLoader.load(eq(tablePropKey))).andReturn(vProps).once();
    replay(context, propStoreWatcher, zooPropLoader);
    assertNotNull(cache.get(tablePropKey)); // will call load
    assertNotNull(cache.get(tablePropKey)); // will read from cache
  }

  /**
   * Verifies that value is not cached
   */
  @SuppressFBWarnings(value = "PREDICTABLE_RANDOM",
      justification = "random used for testing with variable names")
  @Test
  public void getNoCacheTest() {
    var table2PropKey = TablePropKey.of(instanceId,
        TableId.of("t2" + ThreadLocalRandom.current().nextInt(1, 1000)));

    expect(zooPropLoader.load(eq(table2PropKey))).andReturn(vProps).once();

    replay(context, propStoreWatcher, zooPropLoader);
    var props = cache.getIfCached(tablePropKey);
    assertNull(props);

    assertNotNull(cache.get(table2PropKey)); // load into cache
    assertNotNull(cache.getIfCached(table2PropKey)); // read from cache - no load call.
  }

  @Test
  public void removeTest() {
    expect(zooPropLoader.load(eq(tablePropKey))).andReturn(vProps).times(2);
    replay(context, propStoreWatcher, zooPropLoader);
    assertNotNull(cache.get(tablePropKey)); // will call load
    cache.remove(tablePropKey);
    assertNotNull(cache.get(tablePropKey)); // will call load again
  }

  @SuppressFBWarnings(value = "PREDICTABLE_RANDOM",
      justification = "random used for testing with variable names")
  @Test
  public void removeAllTest() {
    var table2PropKey = TablePropKey.of(instanceId,
        TableId.of("t2" + ThreadLocalRandom.current().nextInt(1, 1000)));

    expect(zooPropLoader.load(eq(tablePropKey))).andReturn(vProps).once();
    expect(zooPropLoader.load(eq(table2PropKey))).andReturn(vProps).once();

    replay(context, propStoreWatcher, zooPropLoader);

    assertNotNull(cache.get(tablePropKey)); // will call load
    assertNotNull(cache.get(table2PropKey)); // will call load

    cache.removeAll();

    // check that values are not in cache - will not call load
    assertNull(cache.getIfCached(tablePropKey));
    assertNull(cache.getIfCached(table2PropKey));
  }

  @Test
  public void expireTest() {
    expect(zooPropLoader.load(eq(tablePropKey))).andReturn(vProps).times(2);

    replay(context, propStoreWatcher, zooPropLoader);
    assertNotNull(cache.get(tablePropKey)); // will call load

    ticker.advance(90, TimeUnit.MINUTES);
    assertNotNull(cache.get(tablePropKey)); // expired - will call load.
  }

  @Test
  public void getExceptionTest() {
    expect(zooPropLoader.load(eq(tablePropKey)))
        .andThrow(new IllegalStateException("forced test exception")).once();
    replay(context, propStoreWatcher, zooPropLoader);
    assertNull(cache.get(tablePropKey)); // will call load
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

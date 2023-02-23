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
package org.apache.accumulo.test.functional;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.accumulo.harness.AccumuloITBase.ZOOKEEPER_TESTING_SERVER;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.clientImpl.AcceptableThriftTableOperationException;
import org.apache.accumulo.core.data.InstanceId;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.fate.zookeeper.ZooReaderWriter;
import org.apache.accumulo.core.fate.zookeeper.ZooUtil;
import org.apache.accumulo.core.metadata.schema.Ample;
import org.apache.accumulo.core.metadata.schema.AmpleImpl;
import org.apache.accumulo.core.metadata.schema.TabletMetadata;
import org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.metadata.TabletMetadataCache;
import org.apache.accumulo.test.zookeeper.ZooKeeperTestingServer;
import org.apache.hadoop.io.Text;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.easymock.EasyMock;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.benmanes.caffeine.cache.stats.CacheStats;

@Tag(ZOOKEEPER_TESTING_SERVER)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class TabletMetadataCacheIT {

  /**
   * Class for test that exists to expose protected methods for test
   */
  public static class TestTabletMetadataCache extends TabletMetadataCache {

    public TestTabletMetadataCache(InstanceId iid, ZooReaderWriter zrw, Ample ample) {
      super(iid, zrw, ample);
    }

    @Override
    public KeyExtent getExtent(String path) {
      return super.getExtent(path);
    }

    public AtomicBoolean getConnected() {
      return connected;
    }

    /**
     * Return size of the cache
     *
     * @return size
     */
    protected int getTabletMetadataCacheSize() {
      return tabletMetadataCache.asMap().size();
    }

    /**
     * Return Cache stats object
     *
     * @return cache stats
     */
    protected CacheStats getTabletMetadataCacheStats() {
      return tabletMetadataCache.stats();
    }

  }

  @TempDir
  private static File tempDir;

  private static final InstanceId IID = InstanceId.of(UUID.randomUUID());

  private static ZooKeeperTestingServer szk = null;
  private static ZooKeeper zooKeeper;

  private ServerContext context = null;
  private TestTabletMetadataCache cache = null;
  private TableId tid = TableId.of("2");
  private KeyExtent ke1 = new KeyExtent(tid, new Text("b"), null);
  private KeyExtent ke2 = new KeyExtent(tid, new Text("d"), new Text("b"));
  private KeyExtent ke3 = new KeyExtent(tid, new Text("g"), new Text("d"));
  private KeyExtent ke4 = new KeyExtent(tid, new Text("m"), new Text("g"));
  private TabletMetadata tm1 = TabletMetadata.create(tid.canonical(), null, "b");
  private TabletMetadata tm2 = TabletMetadata.create(tid.canonical(), "b", "d");
  private TabletMetadata tm3 = TabletMetadata.create(tid.canonical(), "d", "g");
  private TabletMetadata tm4 = TabletMetadata.create(tid.canonical(), "g", "m");

  @BeforeAll
  public static void setup() throws Exception {
    szk = new ZooKeeperTestingServer(tempDir);
    szk.initPaths(ZooUtil.getRoot(IID) + Constants.ZTABLET_CACHE);
    zooKeeper = szk.getZooKeeper();
    ZooUtil.digestAuth(zooKeeper, ZooKeeperTestingServer.SECRET);
  }

  @AfterAll
  public static void teardown() throws Exception {
    szk.close();
  }

  @BeforeEach
  public void before() {
    context = EasyMock.createNiceMock(ServerContext.class);
    AmpleImpl ample = EasyMock.createStrictMock(AmpleImpl.class);
    expect(ample.readTablet(ke1, ColumnType.values())).andReturn(tm1).anyTimes();
    expect(ample.readTablet(ke2, ColumnType.values())).andReturn(tm2).anyTimes();
    expect(ample.readTablet(ke3, ColumnType.values())).andReturn(tm3).anyTimes();
    expect(ample.readTablet(ke4, ColumnType.values())).andReturn(tm4).anyTimes();
    cache = new TestTabletMetadataCache(IID, szk.getZooReaderWriter(), ample);
    expect(context.getInstanceID()).andReturn(IID).anyTimes();
    expect(context.getZooReaderWriter()).andReturn(szk.getZooReaderWriter()).anyTimes();
    expect(context.getAmple()).andReturn(ample).anyTimes();
    expect(context.getZooKeepers()).andReturn(szk.getConn());
    expect(context.getZooKeepersSessionTimeOut()).andReturn((30000));
    expect(context.isTabletMetadataCacheInitialized()).andReturn(Boolean.TRUE).anyTimes();
    expect(context.getTabletMetadataCache()).andReturn(cache).anyTimes();
    replay(context, ample);
  }

  @AfterEach
  public void after() {
    cache.close();
  }

  @Test
  @Order(1)
  public void testPathParsing() {
    String path = TabletMetadataCache.getPath(IID, ke1);
    assertEquals("/accumulo/" + IID + Constants.ZTABLET_CACHE + "/2;b;", path);
    KeyExtent result = cache.getExtent(path);
    assertEquals(ke1, result);
    String path2 = TabletMetadataCache.getPath(IID, ke2);
    assertEquals("/accumulo/" + IID + Constants.ZTABLET_CACHE + "/2;d;b", path2);
    KeyExtent result2 = cache.getExtent(path2);
    assertEquals(ke2.toMetaRow(), result2.toMetaRow());
  }

  @Test
  @Order(2)
  public void testCachingLocalTabletChange() {
    TabletMetadataCache.tabletMetadataChanged(context, ke1); // This will perform a create in ZK
    assertEquals(0, cache.getTabletMetadataCacheStats().loadCount());
    assertEquals(0, cache.getTabletMetadataCacheSize());
    TabletMetadata result = cache.get(ke1);
    assertEquals(1, cache.getTabletMetadataCacheSize());
    assertEquals(1, cache.getTabletMetadataCacheStats().requestCount());
    assertEquals(1, cache.getTabletMetadataCacheStats().loadSuccessCount());
    assertEquals(tm1, result);
    TabletMetadataCache.tabletMetadataChanged(context, ke1); // This will perform an update and
                                                             // trigger invalidation
    assertEquals(0, cache.getTabletMetadataCacheSize());
    result = cache.get(ke1);
    assertEquals(1, cache.getTabletMetadataCacheSize());
    assertEquals(2, cache.getTabletMetadataCacheStats().requestCount());
    assertEquals(2, cache.getTabletMetadataCacheStats().loadSuccessCount());
    assertEquals(tm1, result);
  }

  public class TabletChangedThread implements Runnable {

    private final Logger LOG = LoggerFactory.getLogger(TabletChangedThread.class);

    private Long deserialize(byte[] value) {
      return Long.parseLong(new String(value, UTF_8));
    }

    private byte[] serialize(Long value) {
      return value.toString().getBytes(UTF_8);
    }

    @Override
    public void run() {

      // mutate ZK entry for this tablet
      String path = Constants.ZROOT + "/" + IID + Constants.ZTABLET_CACHE + "/"
          + TabletMetadataCache.serializeKeyExtent(ke2);
      try {
        LOG.info("Mutating node at path: {}", path);
        szk.getZooReaderWriter().mutateOrCreate(path, serialize(Long.valueOf(0)), (currVal) -> {
          return serialize(deserialize(currVal) + 1);
        });
      } catch (AcceptableThriftTableOperationException | KeeperException | InterruptedException e) {
        LOG.error("***ERROR***", e);
      }
    }
  }

  @Test
  @Order(3)
  public void testCachingThreadTabletChange() throws InterruptedException {
    TabletMetadataCache.tabletMetadataChanged(context, ke2); // This will perform a create in ZK

    assertEquals(0, cache.getTabletMetadataCacheStats().loadCount());
    assertEquals(0, cache.getTabletMetadataCacheSize());
    TabletMetadata result = cache.get(ke2);
    assertEquals(1, cache.getTabletMetadataCacheSize());
    assertEquals(1, cache.getTabletMetadataCacheStats().requestCount());
    assertEquals(1, cache.getTabletMetadataCacheStats().loadSuccessCount());
    assertEquals(tm2, result);

    Thread t = new Thread(new TabletChangedThread());
    t.start();
    t.join();

    // wait for zk watcher to remove cache entry
    while (cache.getTabletMetadataCacheSize() != 0) {
      Thread.sleep(100);
    }

    assertEquals(0, cache.getTabletMetadataCacheSize());
    result = cache.get(ke2);
    assertEquals(1, cache.getTabletMetadataCacheSize());
    assertEquals(2, cache.getTabletMetadataCacheStats().requestCount());
    assertEquals(2, cache.getTabletMetadataCacheStats().loadSuccessCount());
    assertEquals(tm2, result);
  }

  @Test
  @Order(4)
  public void testZooKeeperConnectionRestart() throws Exception {
    TabletMetadataCache.tabletMetadataChanged(context, ke1); // This will perform a create in ZK
    assertEquals(0, cache.getTabletMetadataCacheStats().loadCount());
    assertEquals(0, cache.getTabletMetadataCacheSize());
    TabletMetadata result = cache.get(ke1);
    assertEquals(1, cache.getTabletMetadataCacheSize());
    assertEquals(1, cache.getTabletMetadataCacheStats().requestCount());
    assertEquals(1, cache.getTabletMetadataCacheStats().loadSuccessCount());
    assertEquals(tm1, result);
    TabletMetadataCache.tabletMetadataChanged(context, ke1); // This will perform an update and
                                                             // trigger invalidation
    assertEquals(0, cache.getTabletMetadataCacheSize());
    result = cache.get(ke1);
    assertEquals(1, cache.getTabletMetadataCacheSize());
    assertEquals(2, cache.getTabletMetadataCacheStats().requestCount());
    assertEquals(2, cache.getTabletMetadataCacheStats().loadSuccessCount());
    assertEquals(tm1, result);

    // Close ZK Server
    szk.close();
    LoggerFactory.getLogger(TabletMetadataCacheIT.class).info("ZK Server closed");

    long requestCount = 0L;
    // Wait to be disconnected
    while (cache.getConnected().get()) {
      // TODO: The ZooKeeper connection does not realize that it's disconnected
      // immediately. Depending on the type of connection failure there is some
      // amount of time before the Watcher gets notified. For example, it appears
      // that if the server closes, like in this test, then the Watcher gets notified
      // rather quickly (less than 100ms). In the case where there is a network partition
      // or the ZK Server is restarted, the Watcher might not get the event for some time.
      // This means that because the ZooKeeper client does not know immediately that it's
      // disconnected, that it's possible that the cache will return a possibly stale result
      result = cache.get(ke1);
      requestCount = cache.getTabletMetadataCacheStats().requestCount();
      assertEquals(1, cache.getTabletMetadataCacheSize());
      assertTrue(requestCount > 2);
      assertEquals(3, cache.getTabletMetadataCacheStats().loadSuccessCount());
      assertEquals(tm1, result);
      Thread.sleep(10);
    }

    // Start ZK Server
    szk.restart();

    // Wait to be reconnected
    while (!cache.getConnected().get()) {
      Thread.sleep(10);
    }

    assertEquals(0, cache.getTabletMetadataCacheSize());
    result = cache.get(ke1);
    assertEquals(1, cache.getTabletMetadataCacheSize());
    assertEquals(requestCount + 1, cache.getTabletMetadataCacheStats().requestCount());
    assertEquals(4, cache.getTabletMetadataCacheStats().loadSuccessCount());
    assertEquals(tm1, result);

  }

  @Test
  @Order(5)
  public void testZooKeeperDies() throws Exception {
    TabletMetadataCache.tabletMetadataChanged(context, ke1); // This will perform a create in ZK
    assertEquals(0, cache.getTabletMetadataCacheStats().loadCount());
    assertEquals(0, cache.getTabletMetadataCacheSize());
    TabletMetadata result = cache.get(ke1);
    assertEquals(1, cache.getTabletMetadataCacheSize());
    assertEquals(1, cache.getTabletMetadataCacheStats().requestCount());
    assertEquals(1, cache.getTabletMetadataCacheStats().loadSuccessCount());
    assertEquals(tm1, result);
    TabletMetadataCache.tabletMetadataChanged(context, ke1); // This will perform an update and
                                                             // trigger invalidation
    assertEquals(0, cache.getTabletMetadataCacheSize());
    result = cache.get(ke1);
    assertEquals(1, cache.getTabletMetadataCacheSize());
    assertEquals(2, cache.getTabletMetadataCacheStats().requestCount());
    assertEquals(2, cache.getTabletMetadataCacheStats().loadSuccessCount());
    assertEquals(tm1, result);

    // close ZooKeeper
    szk.close();

    // Wait to be disconnected
    while (cache.getConnected().get()) {
      Thread.sleep(10);
    }

    assertEquals(0, cache.getTabletMetadataCacheSize());
    result = cache.get(ke1);
    assertEquals(0, cache.getTabletMetadataCacheSize());

  }

}

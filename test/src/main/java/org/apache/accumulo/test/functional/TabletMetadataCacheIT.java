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

import java.io.File;
import java.util.UUID;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.clientImpl.AcceptableThriftTableOperationException;
import org.apache.accumulo.core.data.InstanceId;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.fate.zookeeper.ZooUtil;
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
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.benmanes.caffeine.cache.stats.CacheStats;

@Tag(ZOOKEEPER_TESTING_SERVER)
public class TabletMetadataCacheIT {

  /**
   * Class for test that exists to expose protected methods for test
   */
  public static class TestTabletMetadataCache extends TabletMetadataCache {

    public TestTabletMetadataCache(ServerContext ctx) {
      super(ctx);
    }

    @Override
    public KeyExtent getExtent(String path) {
      return super.getExtent(path);
    }

    @Override
    public String getPath(KeyExtent extent) {
      return super.getPath(extent);
    }

    @Override
    public int getTabletMetadataCacheSize() {
      return super.getTabletMetadataCacheSize();
    }

    @Override
    protected CacheStats getTabletMetadataCacheStats() {
      return super.getTabletMetadataCacheStats();
    }

  }

  @TempDir
  private static File tempDir;

  private static final InstanceId IID = InstanceId.of(UUID.randomUUID());

  private static ZooKeeperTestingServer szk = null;
  private static ZooKeeper zooKeeper;

  private ServerContext context;
  private KeyExtent ke1, ke2, ke3, ke4;
  private TabletMetadata tm1, tm2, tm3, tm4;

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
    expect(context.getInstanceID()).andReturn(IID).anyTimes();
    expect(context.getZooReaderWriter()).andReturn(szk.getZooReaderWriter()).anyTimes();
    expect(context.getAmple()).andReturn(ample).anyTimes();
    expect(context.getZooKeepers()).andReturn(szk.getConn());
    expect(context.getZooKeepersSessionTimeOut()).andReturn((30000));

    TableId tid = TableId.of("2");
    ke1 = new KeyExtent(tid, new Text("b"), null);
    ke2 = new KeyExtent(tid, new Text("d"), new Text("b"));
    ke3 = new KeyExtent(tid, new Text("g"), new Text("d"));
    ke4 = new KeyExtent(tid, new Text("m"), new Text("g"));

    tm1 = TabletMetadata.create(tid.canonical(), null, "b");
    tm2 = TabletMetadata.create(tid.canonical(), "b", "d");
    tm3 = TabletMetadata.create(tid.canonical(), "d", "g");
    tm4 = TabletMetadata.create(tid.canonical(), "g", "m");

    expect(ample.readTablet(ke1, ColumnType.values())).andReturn(tm1).anyTimes();
    expect(ample.readTablet(ke2, ColumnType.values())).andReturn(tm2).anyTimes();
    expect(ample.readTablet(ke3, ColumnType.values())).andReturn(tm3).anyTimes();
    expect(ample.readTablet(ke4, ColumnType.values())).andReturn(tm4).anyTimes();

    replay(context, ample);
  }

  @Test
  public void testPathParsing() {
    try (TestTabletMetadataCache cache = new TestTabletMetadataCache(context)) {
      String path = cache.getPath(ke1);
      assertEquals("/accumulo/" + IID + Constants.ZTABLET_CACHE + "/2;b;", path);
      KeyExtent result = cache.getExtent(path);
      assertEquals(ke1, result);
      String path2 = cache.getPath(ke2);
      assertEquals("/accumulo/" + IID + Constants.ZTABLET_CACHE + "/2;d;b", path2);
      KeyExtent result2 = cache.getExtent(path2);
      assertEquals(ke2.toMetaRow(), result2.toMetaRow());
    }
  }

  @Test
  public void testCachingLocalTabletChange() {
    try (TestTabletMetadataCache cache = new TestTabletMetadataCache(context)) {
      cache.tabletMetadataChanged(ke1); // This will perform a create in ZK
      assertEquals(0, cache.getTabletMetadataCacheStats().loadCount());
      assertEquals(0, cache.getTabletMetadataCacheSize());
      TabletMetadata result = cache.get(ke1);
      assertEquals(1, cache.getTabletMetadataCacheSize());
      assertEquals(1, cache.getTabletMetadataCacheStats().requestCount());
      assertEquals(1, cache.getTabletMetadataCacheStats().loadSuccessCount());
      assertEquals(tm1, result);
      cache.tabletMetadataChanged(ke1); // This will perform an update and trigger invalidation
      assertEquals(0, cache.getTabletMetadataCacheSize());
      result = cache.get(ke1);
      assertEquals(1, cache.getTabletMetadataCacheSize());
      assertEquals(2, cache.getTabletMetadataCacheStats().requestCount());
      assertEquals(2, cache.getTabletMetadataCacheStats().loadSuccessCount());
      assertEquals(tm1, result);
    }
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
  public void testCachingThreadTabletChange() throws InterruptedException {
    try (TestTabletMetadataCache cache = new TestTabletMetadataCache(context)) {
      cache.tabletMetadataChanged(ke2); // This will perform a create in ZK

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
  }

}

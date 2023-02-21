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
package org.apache.accumulo.server.metadata;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.Closeable;
import java.util.concurrent.CountDownLatch;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.clientImpl.AcceptableThriftTableOperationException;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.fate.zookeeper.ZooReaderWriter;
import org.apache.accumulo.core.metadata.schema.TabletMetadata;
import org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType;
import org.apache.accumulo.server.ServerContext;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.WatcherRemoveCuratorFramework;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.CuratorCache;
import org.apache.curator.framework.recipes.cache.CuratorCacheBuilder;
import org.apache.curator.framework.recipes.cache.CuratorCacheListener;
import org.apache.curator.framework.recipes.cache.CuratorCacheStorage;
import org.apache.curator.retry.RetryNTimes;
import org.apache.hadoop.io.Text;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.github.benmanes.caffeine.cache.Scheduler;
import com.github.benmanes.caffeine.cache.stats.CacheStats;

/**
 * Object that uses ZooKeeper for signaling Tablet metadata changes to keep a cache of Tablet
 * metadata up to date.
 *
 */
public class TabletMetadataCache implements Closeable {

  private static final Logger LOG = LoggerFactory.getLogger(TabletMetadataCache.class);
  private static final Long INITIAL_VALUE = Long.valueOf(0);

  private final String znodeBasePath;
  private final ZooReaderWriter zrw;
  private final CuratorFramework cf;
  private final WatcherRemoveCuratorFramework watcherWrapper;
  private final CuratorCache tabletStateCache;
  private final LoadingCache<KeyExtent,TabletMetadata> tabletMetadataCache;

  public TabletMetadataCache(final ServerContext ctx) {

    this.zrw = ctx.getZooReaderWriter();

    final String rootPath = Constants.ZROOT + "/" + ctx.getInstanceID() + Constants.ZTABLET_CACHE;
    znodeBasePath = rootPath + "/";

    // TODO: Likely want to set a max size and eviction parameters
    tabletMetadataCache =
        Caffeine.newBuilder().recordStats().scheduler(Scheduler.systemScheduler()).build((k) -> {
          try {
            TabletMetadata tm = ctx.getAmple().readTablet(k, ColumnType.values());
            LOG.info("Loading tablet metadata for extent: {}. Returned: {}", k, tm);
            return tm;
          } catch (Exception e) {
            LOG.error("Error loading tablet metadata for extent: {}", k, e);
            throw e;
          }
        });

    CuratorFrameworkFactory.Builder curatorBuilder = CuratorFrameworkFactory.builder();
    curatorBuilder.connectString(ctx.getZooKeepers());
    curatorBuilder.sessionTimeoutMs(ctx.getZooKeepersSessionTimeOut());
    curatorBuilder.retryPolicy(new RetryNTimes(5, 2000));
    cf = curatorBuilder.build();
    cf.start();
    watcherWrapper = cf.newWatcherRemoveCuratorFramework();

    CuratorCacheBuilder cacheBuilder = CuratorCache.builder(watcherWrapper, rootPath);
    cacheBuilder.withStorage(CuratorCacheStorage.standard());
    this.tabletStateCache = cacheBuilder.build();
    CountDownLatch initializedLatch = new CountDownLatch(1);
    CuratorCacheListener listener = new CuratorCacheListener() {
      @Override
      public void event(Type type, ChildData oldData, ChildData newData) {
        LOG.info("Received event type: {}, old: {}, new: {}", type, oldData, newData);
        String path = newData.getPath();
        switch (type) {
          case NODE_CREATED:
            // Do nothing, cache will be populated on clients first call to get()
            break;
          case NODE_CHANGED:
          case NODE_DELETED:
          default:
            // Remove the tablet metadata cache entry on update or delete.
            KeyExtent extent = getExtent(path);
            LOG.info("Invalidating extent: {}", extent);
            tabletMetadataCache.invalidate(extent);
            break;

        }
      }

      @Override
      public void initialized() {
        initializedLatch.countDown();
      }
    };
    tabletStateCache.listenable()
        .addListener(CuratorCacheListener.builder().forAll(listener).afterInitialized().build());
    tabletStateCache.start();
    try {
      initializedLatch.await();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException("Thread interrupted waiting for tabletStateCache to initialize",
          e);
    }
  }

  protected KeyExtent getExtent(String path) {
    return deserializeKeyExtent(path.substring(znodeBasePath.length()));
  }

  protected String getPath(KeyExtent extent) {
    return znodeBasePath + serializeKeyExtent(extent);
  }

  public TabletMetadata get(KeyExtent extent) {
    return tabletMetadataCache.get(extent);
  }

  protected int getTabletMetadataCacheSize() {
    return tabletMetadataCache.asMap().size();
  }

  protected CacheStats getTabletMetadataCacheStats() {
    return tabletMetadataCache.stats();
  }

  /**
   * Updates the data of the znode for this extent which will trigger TabletMetadataCache watchers
   * to reload the TabletMetadata for this extent.
   *
   * @param extent
   */
  public void tabletMetadataChanged(KeyExtent extent) {
    final String path = getPath(extent);
    try {
      // remove entry from local cache
      LOG.info("Invalidating extent due to local tablet change: {}", extent);
      tabletMetadataCache.invalidate(extent);
      // mutate ZK entry for this tablet
      this.zrw.mutateOrCreate(path, serializeData(INITIAL_VALUE), (currVal) -> {
        return serializeData(deserializeData(currVal) + 1);
      });
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException("Interrupted updating ZooKeeper at: " + path, e);

    } catch (AcceptableThriftTableOperationException | KeeperException e) {
      throw new RuntimeException("Error updating ZooKeeper at: " + path, e);
    }
  }

  public static String serializeKeyExtent(KeyExtent ke) {
    Text entry = new Text(ke.tableId().canonical());
    entry.append(new byte[] {';'}, 0, 1);
    if (ke.endRow() != null) {
      entry.append(ke.endRow().getBytes(), 0, ke.endRow().getLength());
    }
    entry.append(new byte[] {';'}, 0, 1);
    if (ke.prevEndRow() != null) {
      entry.append(ke.prevEndRow().getBytes(), 0, ke.prevEndRow().getLength());
    }
    return entry.toString();
  }

  private static KeyExtent deserializeKeyExtent(String zkNode) {
    String[] parts = zkNode.split(";");
    assert (parts.length == 3);
    String tid = parts[0];
    String end = parts[1];
    String prev = parts[2];
    return new KeyExtent(TableId.of(tid), end == null ? null : new Text(end),
        prev == null ? null : new Text(prev));
  }

  private static Long deserializeData(byte[] value) {
    return Long.parseLong(new String(value, UTF_8));
  }

  private static byte[] serializeData(Long value) {
    return value.toString().getBytes(UTF_8);
  }

  @Override
  public void close() {
    tabletMetadataCache.invalidateAll();
    tabletMetadataCache.cleanUp();
    tabletStateCache.close();
    watcherWrapper.removeWatchers();
    cf.close();
  }

}

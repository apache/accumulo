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

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.clientImpl.AcceptableThriftTableOperationException;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.fate.zookeeper.ZooReaderWriter;
import org.apache.accumulo.core.metadata.schema.TabletMetadata;
import org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType;
import org.apache.accumulo.server.ServerContext;
import org.apache.hadoop.io.Text;
import org.apache.zookeeper.AddWatchMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.WatcherType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.github.benmanes.caffeine.cache.Scheduler;
import com.github.benmanes.caffeine.cache.stats.CacheStats;

/**
 * Object that uses ZooKeeper for signaling Tablet metadata changes to keep a local cache of Tablet
 * metadata up to date.
 */
public class TabletMetadataCache implements Closeable {

  private Watcher tabletCacheZNodeWatcher = new Watcher() {
    @Override
    public void process(WatchedEvent event) {
      switch (event.getType()) {
        case NodeCreated:
          // Do nothing, cache will be populated on clients first call to get()
          break;
        case NodeDataChanged:
        case NodeDeleted:
          // Remove the tablet metadata cache entry on update or delete.
          KeyExtent extent = getExtent(event.getPath());
          LOG.info("Invalidating extent: {}", extent);
          tabletMetadataCache.invalidate(extent);
          break;
        case None:
        case ChildWatchRemoved:
        case DataWatchRemoved:
        case NodeChildrenChanged:
        case PersistentWatchRemoved:
        default:
          break;

      }
    }
  };

  private static final Logger LOG = LoggerFactory.getLogger(TabletMetadataCache.class);
  private static final Long INITIAL_VALUE = Long.valueOf(0);

  private final String watcherPath;
  private final String znodePath;
  private final ZooReaderWriter zrw;
  private final LoadingCache<KeyExtent,TabletMetadata> tabletMetadataCache;

  public TabletMetadataCache(final ServerContext ctx) {

    this.zrw = ctx.getZooReaderWriter();

    watcherPath = Constants.ZROOT + "/" + ctx.getInstanceID() + Constants.ZTABLET_CACHE;
    znodePath = watcherPath + "/";

    // TODO: Likely want to set a max size and eviction parameters
    tabletMetadataCache =
        Caffeine.newBuilder().recordStats().scheduler(Scheduler.systemScheduler()).build((k) -> {
          TabletMetadata tm = ctx.getAmple().readTablet(k, ColumnType.values());
          LOG.debug("Loading tablet metadata for extent: {}. Returned: {}", k, tm);
          return tm;
        });

    try {
      this.zrw.getZooKeeper().addWatch(watcherPath, tabletCacheZNodeWatcher,
          AddWatchMode.PERSISTENT_RECURSIVE);
    } catch (KeeperException e) {
      throw new RuntimeException("Error setting watch", e);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException("Thread interrupted setting watch", e);
    }

  }

  /**
   * Return a KeyExtent object from the tablet_cache znode entry
   *
   * @param path tablet_cache znode path
   * @return KeyExtent object
   */
  protected KeyExtent getExtent(String path) {
    return deserializeKeyExtent(path.substring(znodePath.length()));
  }

  /**
   * Return the tablet_cache znode path for a KeyExtent
   *
   * @param extent KeyExtent
   * @return tablet_cache znode path
   */
  protected String getPath(KeyExtent extent) {
    return znodePath + serializeKeyExtent(extent);
  }

  /**
   * Cached TabletMetadata for the KeyExtent. If it does not exist, then it will be loaded into the
   * cache via the CacheLoader which uses Ample to read the TabletMetadata and all of its columns.
   *
   * @param extent KeyExtent
   * @return TabletMetadata for the KeyExtent
   */
  public TabletMetadata get(KeyExtent extent) {
    return tabletMetadataCache.get(extent);
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

  /**
   * Updates the data of the znode for this extent which will trigger TabletMetadataCache watchers
   * to reload the TabletMetadata for this extent.
   *
   * @param extent KeyExtent
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

  /**
   * Returns a String that represents this KeyExtent's TableId, endRow, and prevEndRow.
   * TabletsSection#encodeRow(TableId, Text) does not preserve prevEndRow when serializing the
   * KeyExtent.
   *
   * @param ke KeyExtent
   * @return string representation of KeyExtent
   */
  public static String serializeKeyExtent(KeyExtent ke) {
    Text entry = new Text(ke.tableId().canonical());
    entry.append(new byte[] {';'}, 0, 1);
    // TODO: May need to do semi-colon escaping in endRow and prevEndRow
    // like KeyExtent.toString()
    if (ke.endRow() != null) {
      entry.append(ke.endRow().getBytes(), 0, ke.endRow().getLength());
    }
    entry.append(new byte[] {';'}, 0, 1);
    if (ke.prevEndRow() != null) {
      entry.append(ke.prevEndRow().getBytes(), 0, ke.prevEndRow().getLength());
    }
    return entry.toString();
  }

  /**
   * Parses a String created by {@link #serializeKeyExtent(KeyExtent)} and returns a KeyExtent.
   *
   * @param serializedKeyExtent serialized KeyExtent
   * @return KeyExtent
   */
  private static KeyExtent deserializeKeyExtent(String serializedKeyExtent) {
    String[] parts = serializedKeyExtent.split(";");
    assert (parts.length == 2 || parts.length == 3);
    String tid = parts[0];
    String end = parts[1];
    String prev = parts.length == 3 ? parts[2] : null;
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
    try {
      this.zrw.getZooKeeper().removeWatches(watcherPath, tabletCacheZNodeWatcher, WatcherType.Any,
          false);
    } catch (InterruptedException | KeeperException e) {
      LOG.error("Error removing persistent watcher", e);
    }
  }

}

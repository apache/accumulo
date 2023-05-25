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
package org.apache.accumulo.manager.split;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.metadata.StoredTabletFile;
import org.apache.accumulo.core.metadata.TabletFile;
import org.apache.accumulo.core.metadata.schema.Ample;
import org.apache.accumulo.core.metadata.schema.TabletMetadata;
import org.apache.accumulo.manager.Manager;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.conf.TableConfiguration;
import org.apache.accumulo.server.util.FileUtil;
import org.apache.accumulo.server.util.FileUtil.FileInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.CacheLoader;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.github.benmanes.caffeine.cache.Weigher;
import com.google.common.base.Preconditions;
import com.google.common.hash.HashCode;
import com.google.common.hash.Hashing;

public class Splitter {

  private static final Logger log = LoggerFactory.getLogger(Splitter.class);

  private final ServerContext context;
  private final Ample.DataLevel level;

  private final ExecutorService splitExecutor;

  private final ScheduledExecutorService scanExecutor;
  private final Manager manager;
  private ScheduledFuture<?> scanFuture;

  Cache<KeyExtent,KeyExtent> splitsStarting;

  Cache<KeyExtent,HashCode> unsplittable;

  private static class CacheKey {

    final TableId tableId;
    final TabletFile tabletFile;

    public CacheKey(TableId tableId, TabletFile tabletFile) {
      this.tableId = tableId;
      this.tabletFile = tabletFile;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      CacheKey cacheKey = (CacheKey) o;
      return Objects.equals(tableId, cacheKey.tableId)
          && Objects.equals(tabletFile, cacheKey.tabletFile);
    }

    @Override
    public int hashCode() {
      return Objects.hash(tableId, tabletFile);
    }

  }

  LoadingCache<CacheKey,FileInfo> splitFileCache;

  public static int weigh(KeyExtent keyExtent) {
    int size = 0;
    size += keyExtent.tableId().toString().length();
    if (keyExtent.endRow() != null) {
      size += keyExtent.endRow().getLength();
    }
    if (keyExtent.prevEndRow() != null) {
      size += keyExtent.prevEndRow().getLength();
    }
    return size;
  }

  public Splitter(ServerContext context, Ample.DataLevel level, Manager manager) {
    this.context = context;
    this.level = level;
    this.manager = manager;
    this.splitExecutor = context.threadPools().createExecutorService(context.getConfiguration(),
        Property.MANAGER_SPLIT_WORKER_THREADS, true);
    this.scanExecutor =
        context.threadPools().createScheduledExecutorService(1, "Tablet Split Scanner", true);

    Weigher<CacheKey,FileInfo> weigher =
        (key, info) -> key.tableId.canonical().length() + key.tabletFile.getPathStr().length()
            + info.getFirstRow().getLength() + info.getLastRow().getLength();

    CacheLoader<CacheKey,FileInfo> loader = new CacheLoader<>() {
      @Override
      public FileInfo load(CacheKey key) throws Exception {
        TableConfiguration tableConf = context.getTableConfiguration(key.tableId);
        return FileUtil.tryToGetFirstAndLastRows(context, tableConf, Set.of(key.tabletFile))
            .get(key.tabletFile);
      }
    };

    splitFileCache = Caffeine.newBuilder().expireAfterAccess(10, TimeUnit.MINUTES)
        .maximumWeight(10_000_000L).weigher(weigher).build(loader);

    Weigher<KeyExtent,KeyExtent> weigher2 = (keyExtent, keyExtent2) -> weigh(keyExtent);

    // Tracks splits starting, but not forever in case something in the code does not remove it.
    splitsStarting = Caffeine.newBuilder().expireAfterAccess(3, TimeUnit.HOURS)
        .maximumWeight(10_000_000L).weigher(weigher2).build();

    Weigher<KeyExtent,HashCode> weigher3 = (keyExtent, hc) -> {
      return weigh(keyExtent) + hc.bits() / 8;
    };

    unsplittable = Caffeine.newBuilder().expireAfterAccess(24, TimeUnit.HOURS)
        .maximumWeight(10_000_000L).weigher(weigher3).build();
  }

  public synchronized void start() {
    Preconditions.checkState(scanFuture == null);
    Preconditions.checkState(!scanExecutor.isShutdown());
    // ELASTICITY_TODO make this configurable if functionality is not moved elsewhere
    scanFuture = scanExecutor.scheduleWithFixedDelay(new SplitScanner(context, level, manager), 1,
        10, TimeUnit.SECONDS);
  }

  public synchronized void stop() {
    scanFuture.cancel(true);
    scanExecutor.shutdownNow();
    splitExecutor.shutdownNow();
  }

  public FileInfo getCachedFileInfo(TableId tableId, TabletFile tabletFile) {
    return splitFileCache.get(new CacheKey(tableId, tabletFile));
  }

  private HashCode caclulateFilesHash(TabletMetadata tabletMetadata) {
    var hasher = Hashing.goodFastHash(128).newHasher();
    tabletMetadata.getFiles().stream().map(StoredTabletFile::getPathStr).sorted()
        .forEach(path -> hasher.putString(path, UTF_8));
    return hasher.hash();
  }

  /**
   * This tablet met the criteria for split but inspection could not find a split point. Remember
   * this to avoid wasting time on future inspections until its files change.
   */
  public void rememberUnsplittable(TabletMetadata tablet) {
    unsplittable.put(tablet.getExtent(), caclulateFilesHash(tablet));
  }

  /**
   * Determines if further inspection should be done on a tablet that meets the criteria for splits.
   */
  public boolean shouldInspect(TabletMetadata tablet) {
    if (splitsStarting.getIfPresent(tablet.getExtent()) != null) {
      return false;
    }

    var hashCode = unsplittable.getIfPresent(tablet.getExtent());

    if (hashCode != null) {
      if (hashCode.equals(caclulateFilesHash(tablet))) {
        return false;
      }
    }

    return true;
  }

  /**
   * Temporarily remember that the process of splitting is starting for this tablet making
   * {@link #shouldInspect(TabletMetadata)} return false in the future.
   */
  public boolean addSplitStarting(KeyExtent extent) {
    Objects.requireNonNull(extent);
    return splitsStarting.asMap().put(extent, extent) == null;
  }

  public void removeSplitStarting(KeyExtent extent) {
    splitsStarting.invalidate(extent);
  }

  public void executeSplit(SplitTask splitTask) {
    splitExecutor.execute(splitTask);
  }
}

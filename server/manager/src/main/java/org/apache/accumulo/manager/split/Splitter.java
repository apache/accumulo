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

import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.metadata.TabletFile;
import org.apache.accumulo.core.util.cache.Caches.CacheName;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.conf.TableConfiguration;
import org.apache.accumulo.server.util.FileUtil;
import org.apache.accumulo.server.util.FileUtil.FileInfo;

import com.github.benmanes.caffeine.cache.CacheLoader;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.github.benmanes.caffeine.cache.Weigher;

public class Splitter {

  private final ThreadPoolExecutor splitExecutor;

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

  public Splitter(ServerContext context) {
    int numThreads = context.getConfiguration().getCount(Property.MANAGER_SPLIT_WORKER_THREADS);
    // Set up thread pool that constrains the amount of task it queues and when full discards task.
    // The purpose of this is to avoid reading lots of data into memory if lots of tablets need to
    // split.
    BlockingQueue<Runnable> queue = new ArrayBlockingQueue<>(10000);
    this.splitExecutor = context.threadPools().createThreadPool(numThreads, numThreads, 0,
        TimeUnit.MILLISECONDS, "split_seeder", queue, true);

    // Discard task when the queue is full, this allows the TGW to continue processing task other
    // than splits.
    this.splitExecutor.setRejectedExecutionHandler(new ThreadPoolExecutor.DiscardPolicy());

    Weigher<CacheKey,
        FileInfo> weigher = (key, info) -> key.tableId.canonical().length()
            + key.tabletFile.getPath().toString().length() + info.getFirstRow().getLength()
            + info.getLastRow().getLength();

    CacheLoader<CacheKey,FileInfo> loader = key -> {
      TableConfiguration tableConf = context.getTableConfiguration(key.tableId);
      return FileUtil.tryToGetFirstAndLastRows(context, tableConf, Set.of(key.tabletFile))
          .get(key.tabletFile);
    };

    splitFileCache = context.getCaches().createNewBuilder(CacheName.SPLITTER_FILES, true)
        .expireAfterAccess(10, TimeUnit.MINUTES).maximumWeight(10_000_000L).weigher(weigher)
        .build(loader);

  }

  public synchronized void start() {}

  public synchronized void stop() {
    splitExecutor.shutdownNow();
  }

  public FileInfo getCachedFileInfo(TableId tableId, TabletFile tabletFile) {
    return splitFileCache.get(new CacheKey(tableId, tabletFile));
  }

  public void initiateSplit(SeedSplitTask seedSplitTask) {
    splitExecutor.execute(seedSplitTask);
  }
}

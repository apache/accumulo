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

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.file.FileOperations;
import org.apache.accumulo.core.file.FileSKVIterator;
import org.apache.accumulo.core.metadata.TabletFile;
import org.apache.accumulo.core.util.cache.Caches.CacheName;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.conf.TableConfiguration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.benmanes.caffeine.cache.CacheLoader;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.github.benmanes.caffeine.cache.Weigher;

public class Splitter {

  private static final Logger LOG = LoggerFactory.getLogger(Splitter.class);

  private final ThreadPoolExecutor splitExecutor;

  public static class FileInfo {
    final Text firstRow;
    final Text lastRow;

    public FileInfo(Text firstRow, Text lastRow) {
      this.firstRow = firstRow;
      this.lastRow = lastRow;
    }

    public Text getFirstRow() {
      return firstRow;
    }

    public Text getLastRow() {
      return lastRow;
    }
  }

  public static <T extends TabletFile> Map<T,FileInfo> tryToGetFirstAndLastRows(
      ServerContext context, TableConfiguration tableConf, Set<T> dataFiles) {

    HashMap<T,FileInfo> dataFilesInfo = new HashMap<>();

    long t1 = System.currentTimeMillis();

    for (T dataFile : dataFiles) {

      FileSKVIterator reader = null;
      FileSystem ns = context.getVolumeManager().getFileSystemByPath(dataFile.getPath());
      try {
        reader = FileOperations.getInstance().newReaderBuilder()
            .forFile(dataFile, ns, ns.getConf(), tableConf.getCryptoService())
            .withTableConfiguration(tableConf).build();

        Text firstRow = reader.getFirstRow();
        if (firstRow != null) {
          dataFilesInfo.put(dataFile, new FileInfo(firstRow, reader.getLastRow()));
        }

      } catch (IOException ioe) {
        LOG.warn("Failed to read data file to determine first and last key : " + dataFile, ioe);
      } finally {
        if (reader != null) {
          try {
            reader.close();
          } catch (IOException ioe) {
            LOG.warn("failed to close " + dataFile, ioe);
          }
        }
      }

    }

    long t2 = System.currentTimeMillis();

    String message = String.format("Found first and last keys for %d data files in %6.2f secs",
        dataFiles.size(), (t2 - t1) / 1000.0);
    if (t2 - t1 > 500) {
      LOG.debug(message);
    } else {
      LOG.trace(message);
    }

    return dataFilesInfo;
  }

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

  final LoadingCache<CacheKey,FileInfo> splitFileCache;

  public Splitter(ServerContext context) {
    int numThreads = context.getConfiguration().getCount(Property.MANAGER_SPLIT_WORKER_THREADS);
    // Set up thread pool that constrains the amount of task it queues and when full discards task.
    // The purpose of this is to avoid reading lots of data into memory if lots of tablets need to
    // split.
    BlockingQueue<Runnable> queue = new ArrayBlockingQueue<>(10000);
    this.splitExecutor = context.threadPools().getPoolBuilder("split_seeder")
        .numCoreThreads(numThreads).numMaxThreads(numThreads).withTimeOut(0L, TimeUnit.MILLISECONDS)
        .withQueue(queue).enableThreadPoolMetrics().build();

    // Discard task when the queue is full, this allows the TGW to continue processing task other
    // than splits.
    this.splitExecutor.setRejectedExecutionHandler(new ThreadPoolExecutor.DiscardPolicy());

    Weigher<CacheKey,
        FileInfo> weigher = (key, info) -> key.tableId.canonical().length()
            + key.tabletFile.getPath().toString().length() + info.getFirstRow().getLength()
            + info.getLastRow().getLength();

    CacheLoader<CacheKey,FileInfo> loader = key -> {
      TableConfiguration tableConf = context.getTableConfiguration(key.tableId);
      return tryToGetFirstAndLastRows(context, tableConf, Set.of(key.tabletFile))
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

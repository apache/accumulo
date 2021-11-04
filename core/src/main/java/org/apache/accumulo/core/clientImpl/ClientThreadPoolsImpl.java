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
package org.apache.accumulo.core.clientImpl;

import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.conf.ConfigurationCopy;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.util.threads.ThreadPools;

public class ClientThreadPoolsImpl {

  public static class ThreadPoolConfig {

    public static final ThreadPoolConfig EMPTY_CONFIG =
        new ThreadPoolConfig(Optional.empty(), Optional.empty(), Optional.empty());

    private final Optional<Iterable<Entry<String,String>>> configuration;
    private final Optional<Integer> numThreads;
    private final Optional<String> threadName;

    public ThreadPoolConfig(Iterable<Entry<String,String>> configuration) {
      this(Optional.of(configuration), Optional.empty(), Optional.empty());
    }

    public ThreadPoolConfig(Iterable<Entry<String,String>> configuration, int numThreads) {
      this(Optional.of(configuration), Optional.of(numThreads), Optional.empty());
    }

    public ThreadPoolConfig(Iterable<Entry<String,String>> configuration, int numThreads,
        String threadName) {
      this(Optional.of(configuration), Optional.of(numThreads), Optional.of(threadName));
    }

    private ThreadPoolConfig(Optional<Iterable<Entry<String,String>>> configuration,
        Optional<Integer> numThreads, Optional<String> threadName) {
      this.configuration = configuration;
      this.numThreads = numThreads;
      this.threadName = threadName;
    }

    public Optional<Iterable<Entry<String,String>>> getConfiguration() {
      return configuration;
    }

    public Optional<Integer> getNumThreads() {
      return numThreads;
    }

    public Optional<String> getThreadName() {
      return threadName;
    }
  }

  public static enum ThreadPoolUsage {
    /**
     * ThreadPoolExecutor that runs bulk import tasks
     */
    BULK_IMPORT_POOL,
    /**
     * ThreadPoolExecutor that runs tasks to contact Compactors to get running compaction
     * information
     */
    ACTIVE_EXTERNAL_COMPACTION_POOL,
    /**
     * ThreadPoolExecutor used for fetching data from the TabletServers
     */
    SCANNER_READ_AHEAD_POOL,
    /**
     * ThreadPoolExecutor used for adding splits to a table
     */
    ADD_SPLITS_THREAD_POOL,
    /**
     * ThreadPoolExecutor used for fetching data from the TabletServers
     */
    BATCH_SCANNER_READ_AHEAD_POOL,
    /**
     * ThreadPoolExecutor that runs the tasks of binning mutations
     */
    BATCH_WRITER_BINNING_POOL,
    /**
     * ThreadPoolExecutor that runs the tasks of sending mutations to TabletServers
     */
    BATCH_WRITER_SEND_POOL,
    /**
     * ThreadPoolExecutor that runs clean up tasks when close is called on the ConditionalWriter
     */
    CONDITIONAL_WRITER_CLEANUP_TASK_POOL,
    /**
     * ThreadPoolExecutor responsible for loading bloom filters
     */
    BLOOM_FILTER_LAYER_LOADER_POOL
  }

  public static enum ScheduledThreadPoolUsage {
    /**
     * shared scheduled executor for trivial tasks
     */
    SHARED_GENERAL_SCHEDULED_TASK_POOL,
    /**
     * ScheduledThreadPoolExecutor that runs tasks for the BatchWriter to meet the users latency
     * goals.
     */
    BATCH_WRITER_LATENCY_TASK_POOL,
    /**
     * ScheduledThreadPoolExecutor that periodically runs tasks to handle failed write mutations and
     * send mutations to TabletServers
     */
    CONDITIONAL_WRITER_RETRY_POOL
  }

  private ScheduledThreadPoolExecutor sharedScheduledThreadPool = null;

  public ThreadPoolExecutor getThreadPool(ThreadPoolUsage usage, ThreadPoolConfig config) {
    switch (usage) {
      case BULK_IMPORT_POOL:
        Objects.requireNonNull(config.getNumThreads().get(), "Number of threads must be set");
        return ThreadPools.createFixedThreadPool(config.getNumThreads().get(), "BulkImportThread",
            false);
      case ACTIVE_EXTERNAL_COMPACTION_POOL:
        Objects.requireNonNull(config.getNumThreads().get(), "Number of threads must be set");
        return ThreadPools.createFixedThreadPool(config.getNumThreads().get(),
            "getactivecompactions", false);
      case SCANNER_READ_AHEAD_POOL:
        return ThreadPools.createThreadPool(0, Integer.MAX_VALUE, 3L, TimeUnit.SECONDS,
            "Accumulo scanner read ahead thread", new SynchronousQueue<>(), OptionalInt.empty(),
            false);
      case ADD_SPLITS_THREAD_POOL:
        return ThreadPools.createFixedThreadPool(16, "addSplits", false);
      case BATCH_SCANNER_READ_AHEAD_POOL:
        Objects.requireNonNull(config.getNumThreads().get(), "Number of threads must be set");
        Objects.requireNonNull(config.getThreadName().get(), "Name of threads must be set");
        return ThreadPools.createFixedThreadPool(config.getNumThreads().get(),
            "batch scanner " + config.getThreadName().get() + "-", false);
      case BATCH_WRITER_BINNING_POOL:
        return ThreadPools.createFixedThreadPool(1, "BinMutations", new SynchronousQueue<>(),
            false);
      case BATCH_WRITER_SEND_POOL:
        Objects.requireNonNull(config.getNumThreads().get(), "Number of threads must be set");
        return ThreadPools.createFixedThreadPool(config.getNumThreads().get(), "MutationWriter",
            false);
      case CONDITIONAL_WRITER_CLEANUP_TASK_POOL:
        return ThreadPools.createFixedThreadPool(1, 3, TimeUnit.SECONDS,
            "Conditional Writer Cleanup Thread", false);
      case BLOOM_FILTER_LAYER_LOADER_POOL:
        Objects.requireNonNull(config.getConfiguration().get(), "Configuration must be set");
        return ThreadPools.createThreadPool(0,
            new ConfigurationCopy(config.getConfiguration().get())
                .getCount(Property.TSERV_BLOOM_LOAD_MAXCONCURRENT),
            60, TimeUnit.SECONDS, "bloom-loader", false);
      default:
        throw new IllegalArgumentException("Unhandled thread pool usage value: " + usage.name());
    }
  }

  public ScheduledThreadPoolExecutor getScheduledThreadPool(ScheduledThreadPoolUsage usage,
      ThreadPoolConfig config) {
    switch (usage) {
      case SHARED_GENERAL_SCHEDULED_TASK_POOL:
        Objects.requireNonNull(config.getConfiguration().get(), "Configuration must be set");
        if (sharedScheduledThreadPool == null) {
          sharedScheduledThreadPool = (ScheduledThreadPoolExecutor) ThreadPools
              .createExecutorService(new ConfigurationCopy(config.getConfiguration().get()),
                  Property.GENERAL_SIMPLETIMER_THREADPOOL_SIZE);
        }
        return sharedScheduledThreadPool;
      case BATCH_WRITER_LATENCY_TASK_POOL:
        Objects.requireNonNull(config.getConfiguration().get(), "Configuration must be set");
        return ThreadPools.createGeneralScheduledExecutorService(
            new ConfigurationCopy(config.getConfiguration().get()));
      case CONDITIONAL_WRITER_RETRY_POOL:
        Objects.requireNonNull(config.getNumThreads().get(), "Number of threads must be set");
        return ThreadPools.createScheduledExecutorService(config.getNumThreads().get(),
            "ConiditionalWriterImpl", false);
      default:
        throw new IllegalArgumentException("Unhandled thread pool usage value: " + usage.name());

    }
  }

  public void close() {
    if (sharedScheduledThreadPool != null) {
      sharedScheduledThreadPool.shutdownNow();
      sharedScheduledThreadPool = null;
    }
  }
}

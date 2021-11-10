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

import static java.util.Objects.requireNonNull;

import java.util.Optional;
import java.util.OptionalInt;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.ConfigurationCopy;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.util.threads.ThreadPools;

public class ClientThreadPools {

  public static class ThreadPoolConfig {

    public static final ThreadPoolConfig EMPTY_CONFIG =
        new ThreadPoolConfig(Optional.empty(), Optional.empty(), Optional.empty());

    private final Optional<AccumuloConfiguration> configuration;
    private final Optional<Integer> numThreads;
    private final Optional<String> threadName;

    public ThreadPoolConfig(AccumuloConfiguration configuration) {
      this(Optional.of(configuration), Optional.empty(), Optional.empty());
    }

    public ThreadPoolConfig(AccumuloConfiguration configuration, int numThreads) {
      this(Optional.of(configuration), Optional.of(numThreads), Optional.empty());
    }

    public ThreadPoolConfig(AccumuloConfiguration configuration, int numThreads,
        String threadName) {
      this(Optional.of(configuration), Optional.of(numThreads), Optional.of(threadName));
    }

    private ThreadPoolConfig(Optional<AccumuloConfiguration> configuration,
        Optional<Integer> numThreads, Optional<String> threadName) {
      this.configuration = configuration;
      this.numThreads = numThreads;
      this.threadName = threadName;
    }

    public Optional<AccumuloConfiguration> getConfiguration() {
      return configuration;
    }

    public Optional<Integer> getNumThreads() {
      return numThreads;
    }

    public Optional<String> getThreadName() {
      return threadName;
    }
  }

  public static enum ThreadPoolType {
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
     * ThreadPoolExecutor responsible for loading bloom filters
     */
    BLOOM_FILTER_LAYER_LOADER_POOL
  }

  public static enum ScheduledThreadPoolType {
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

  public ThreadPoolExecutor newThreadPool(ThreadPoolType usage, ThreadPoolConfig config) {
    switch (usage) {
      case BULK_IMPORT_POOL:
        requireNonNull(config.getNumThreads().get(), "Number of threads must be set");
        return ThreadPools.createFixedThreadPool(config.getNumThreads().get(), "BulkImportThread");
      case ACTIVE_EXTERNAL_COMPACTION_POOL:
        requireNonNull(config.getNumThreads().get(), "Number of threads must be set");
        return ThreadPools.createFixedThreadPool(config.getNumThreads().get(),
            "getactivecompactions");
      case SCANNER_READ_AHEAD_POOL:
        return ThreadPools.createThreadPool(0, Integer.MAX_VALUE, 3L, TimeUnit.SECONDS,
            "Accumulo scanner read ahead thread", new SynchronousQueue<>(), OptionalInt.empty());
      case ADD_SPLITS_THREAD_POOL:
        return ThreadPools.createFixedThreadPool(16, "addSplits");
      case BATCH_SCANNER_READ_AHEAD_POOL:
        requireNonNull(config.getNumThreads().get(), "Number of threads must be set");
        requireNonNull(config.getThreadName().get(), "Name of threads must be set");
        return ThreadPools.createFixedThreadPool(config.getNumThreads().get(),
            "batch scanner " + config.getThreadName().get() + "-");
      case BATCH_WRITER_BINNING_POOL:
        return ThreadPools.createFixedThreadPool(1, "BinMutations", new SynchronousQueue<>());
      case BATCH_WRITER_SEND_POOL:
        requireNonNull(config.getNumThreads().get(), "Number of threads must be set");
        return ThreadPools.createFixedThreadPool(config.getNumThreads().get(), "MutationWriter");
      case BLOOM_FILTER_LAYER_LOADER_POOL:
        requireNonNull(config.getConfiguration().get(), "Configuration must be set");
        return ThreadPools.createThreadPool(0,
            new ConfigurationCopy(config.getConfiguration().get()).getCount(
                Property.TSERV_BLOOM_LOAD_MAXCONCURRENT),
            60, TimeUnit.SECONDS, "bloom-loader");
      default:
        throw new IllegalArgumentException("Unhandled thread pool usage value: " + usage.name());
    }
  }

  public ScheduledThreadPoolExecutor newScheduledThreadPool(ScheduledThreadPoolType usage,
      ThreadPoolConfig config) {
    switch (usage) {
      case SHARED_GENERAL_SCHEDULED_TASK_POOL:
        requireNonNull(config.getConfiguration().get(), "Configuration must be set");
        synchronized (this) {
          if (sharedScheduledThreadPool == null) {
            sharedScheduledThreadPool = (ScheduledThreadPoolExecutor) ThreadPools
                .createExecutorService(new ConfigurationCopy(config.getConfiguration().get()),
                    Property.GENERAL_SIMPLETIMER_THREADPOOL_SIZE);
          }
        }
        return sharedScheduledThreadPool;
      case BATCH_WRITER_LATENCY_TASK_POOL:
        requireNonNull(config.getConfiguration().get(), "Configuration must be set");
        return ThreadPools.createGeneralScheduledExecutorService(
            new ConfigurationCopy(config.getConfiguration().get()));
      case CONDITIONAL_WRITER_RETRY_POOL:
        requireNonNull(config.getNumThreads().get(), "Number of threads must be set");
        return ThreadPools.createScheduledExecutorService(config.getNumThreads().get(),
            "ConiditionalWriterImpl");
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

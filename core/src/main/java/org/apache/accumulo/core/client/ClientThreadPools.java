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
package org.apache.accumulo.core.client;

import java.util.Map.Entry;
import java.util.Optional;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor;

public interface ClientThreadPools {

  class ThreadPoolConfig {

    public static ThreadPoolConfig EMPTY_CONFIG =
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

  enum ThreadPoolUsage {
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

  enum ScheduledThreadPoolUsage {
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

  /**
   * return a ThreadPoolExecutor configured for the specified usage
   *
   * @param usage
   *          thread pool usage
   * @param config
   *          thread pool configuration
   * @return ThreadPoolExecutor
   */
  ThreadPoolExecutor getThreadPool(ThreadPoolUsage usage, ThreadPoolConfig config);

  /**
   * return a ScheduledThreadPoolExecutor configured for the specified usage
   *
   * @param usage
   *          thread pool usage
   * @param config
   *          thread pool configuration
   * @return ScheduledThreadPoolExecutor
   */
  ScheduledThreadPoolExecutor getScheduledThreadPool(ScheduledThreadPoolUsage usage,
      ThreadPoolConfig config);

}

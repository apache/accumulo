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
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor;

public interface ClientThreadPools {

  /**
   * return a shared scheduled executor for trivial tasks
   *
   * @param conf
   *          configuration properties
   * @return ScheduledThreadPoolExecutor
   */
  ScheduledThreadPoolExecutor getSharedScheduledExecutor(Iterable<Entry<String,String>> conf);

  /**
   * ThreadPoolExecutor that runs bulk import tasks
   *
   * @param conf
   *          configuration properties
   * @param numThreads
   *          number of threads for the pool
   * @return ThreadPoolExecutor
   */
  ThreadPoolExecutor getBulkImportThreadPool(Iterable<Entry<String,String>> conf, int numThreads);

  /**
   * ThreadPoolExecutor that runs tasks to contact Compactors to get running compaction information
   *
   * @param conf
   *          configuration properties
   * @param numThreads
   *          number of threads for the pool
   * @return ThreadPoolExecutor
   */
  ThreadPoolExecutor getExternalCompactionActiveCompactionsPool(Iterable<Entry<String,String>> conf,
      int numThreads);

  /**
   * ThreadPoolExecutor used for fetching data from the TabletServers
   *
   * @param conf
   *          configuration properties
   * @return ThreadPoolExecutor
   */
  ThreadPoolExecutor getScannerReadAheadPool(Iterable<Entry<String,String>> conf);

  /**
   * ThreadPoolExecutor used for adding splits to a table
   *
   * @param conf
   *          configuration properties
   * @return ThreadPoolExecutor
   */
  ThreadPoolExecutor getAddSplitsThreadPool(Iterable<Entry<String,String>> conf);

  /**
   * ThreadPoolExecutor used for fetching data from the TabletServers
   *
   * @param conf
   *          configuration properties
   * @param numQueryThreads
   *          number of threads for the pool
   * @param batchReaderInstance
   *          instance name
   * @return ThreadPoolExecutor
   */
  ThreadPoolExecutor getBatchReaderThreadPool(Iterable<Entry<String,String>> conf,
      int numQueryThreads, int batchReaderInstance);

  /**
   * ScheduledThreadPoolExecutor that runs tasks for the BatchWriter to meet the users latency
   * goals.
   *
   * @param conf
   *          configuration properties
   * @return ScheduledThreadPoolExecutor
   */
  ScheduledThreadPoolExecutor
      getBatchWriterLatencyTasksThreadPool(Iterable<Entry<String,String>> conf);

  /**
   * ThreadPoolExecutor that runs the tasks of binning mutations
   *
   * @param conf
   *          configuration properties
   * @return ThreadPoolExecutor
   */
  ThreadPoolExecutor getBatchWriterBinningThreadPool(Iterable<Entry<String,String>> conf);

  /**
   * ThreadPoolExecutor that runs the tasks of sending mutations to TabletServers
   *
   * @param conf
   *          configuration properties
   * @return ThreadPoolExecutor
   */
  ThreadPoolExecutor getBatchWriterSendThreadPool(Iterable<Entry<String,String>> conf,
      int numSendThreads);

  /**
   * ThreadPoolExecutor that runs clean up tasks when close is called on the ConditionalWriter
   *
   * @param conf
   *          configuration properties
   * @return ThreadPoolExecutor
   */
  ThreadPoolExecutor getConditionalWriterCleanupTaskThreadPool(Iterable<Entry<String,String>> conf);

  /**
   * ScheduledThreadPoolExecutor that periodically runs tasks to handle failed write mutations and
   * send mutations to TabletServers
   *
   * @param conf
   *          configuration properties
   * @param config
   *          conditional writer config
   * @return ScheduledThreadPoolExecutor
   */
  ScheduledThreadPoolExecutor getConditionalWriterThreadPool(Iterable<Entry<String,String>> conf,
      ConditionalWriterConfig config);

  /**
   * ThreadPoolExecutor responsible for loading bloom filters
   *
   * @param conf
   *          configuration properties
   * @return ThreadPoolExecutor
   */
  ThreadPoolExecutor getBloomFilterLayerLoadThreadPool(Iterable<Entry<String,String>> conf);

}

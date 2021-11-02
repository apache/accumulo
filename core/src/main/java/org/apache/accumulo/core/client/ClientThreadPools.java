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

import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor;

import org.apache.accumulo.core.clientImpl.ClientContext;

public interface ClientThreadPools {

  /**
   * return a shared scheduled executor for trivial tasks
   *
   * @param ctx
   * @return ScheduledThreadPoolExecutor
   */
  ScheduledThreadPoolExecutor getSharedScheduledExecutor(ClientContext context);

  /**
   * ThreadPoolExecutor that runs bulk import tasks
   *
   * @param ctx
   * @param numThreads
   * @return ThreadPoolExecutor
   */
  ThreadPoolExecutor getBulkImportThreadPool(ClientContext ctx, int numThreads);

  /**
   * ThreadPoolExecutor that runs tasks to contact Compactors to get running compaction information
   *
   * @param numThreads
   * @return ThreadPoolExecutor
   */
  ThreadPoolExecutor getExternalCompactionActiveCompactionsPool(ClientContext ctx, int numThreads);

  /**
   * ThreadPoolExecutor used for fetching data from the TabletServers
   *
   * @param ctx
   * @return ThreadPoolExecutor
   */
  ThreadPoolExecutor getScannerReadAheadPool(ClientContext ctx);

  /**
   * ThreadPoolExecutor used for adding splits to a table
   *
   * @param ctx
   * @return ThreadPoolExecutor
   */
  ThreadPoolExecutor getAddSplitsThreadPool(ClientContext ctx);

  /**
   * ThreadPoolExecutor used for fetching data from the TabletServers
   *
   * @param ctx
   * @param numQueryThreads
   * @param batchReaderInstance
   * @return ThreadPoolExecutor
   */
  ThreadPoolExecutor getBatchReaderThreadPool(ClientContext ctx, int numQueryThreads,
      int batchReaderInstance);

  /**
   * ScheduledThreadPoolExecutor that runs tasks for the BatchWriter to meet the users latency
   * goals.
   * 
   * @param ctx
   *          client context object
   * @return ScheduledThreadPoolExecutor
   */
  ScheduledThreadPoolExecutor getBatchWriterLatencyTasksThreadPool(ClientContext ctx);

  /**
   * ThreadPoolExecutor that runs the tasks of binning mutations
   * 
   * @param ctx
   *          client context object
   * @return ThreadPoolExecutor
   */
  ThreadPoolExecutor getBatchWriterBinningThreadPool(ClientContext ctx);

  /**
   * ThreadPoolExecutor that runs the tasks of sending mutations to TabletServers
   *
   * @param ctx
   *          client context object
   * @return ThreadPoolExecutor
   */
  ThreadPoolExecutor getBatchWriterSendThreadPool(ClientContext ctx, int numSendThreads);

  /**
   * ThreadPoolExecutor that runs clean up tasks when close is called on the ConditionalWriter
   *
   * @param ctx
   * @return ThreadPoolExecutor
   */
  ThreadPoolExecutor getConditionalWriterCleanupTaskThreadPool(ClientContext ctx);

  /**
   * ScheduledThreadPoolExecutor that periodically runs tasks to handle failed write mutations and
   * send mutations to TabletServers
   *
   * @param ctx
   * @param config
   * @return ScheduledThreadPoolExecutor
   */
  ScheduledThreadPoolExecutor getConditionalWriterThreadPool(ClientContext ctx,
      ConditionalWriterConfig config);

  /**
   * ThreadPoolExecutor responsible for loading bloom filters
   * 
   * @param ctx
   *          client context object
   * @return ThreadPoolExecutor
   */
  ThreadPoolExecutor getBloomFilterLayerLoadThreadPool(ClientContext ctx);

}

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
import java.util.OptionalInt;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.client.ClientThreadPools;
import org.apache.accumulo.core.client.ConditionalWriterConfig;
import org.apache.accumulo.core.conf.ConfigurationCopy;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.util.threads.ThreadPools;

public class ClientThreadPoolsImpl implements ClientThreadPools {

  private ScheduledThreadPoolExecutor sharedScheduledThreadPool = null;

  public synchronized ScheduledThreadPoolExecutor
      getSharedScheduledExecutor(Iterable<Entry<String,String>> conf) {
    if (sharedScheduledThreadPool == null) {
      sharedScheduledThreadPool = (ScheduledThreadPoolExecutor) ThreadPools.createExecutorService(
          new ConfigurationCopy(conf), Property.GENERAL_SIMPLETIMER_THREADPOOL_SIZE);
    }
    return sharedScheduledThreadPool;
  }

  public ThreadPoolExecutor getBulkImportThreadPool(Iterable<Entry<String,String>> conf,
      int numThreads) {
    return ThreadPools.createFixedThreadPool(numThreads, "BulkImportThread", false);
  }

  public ThreadPoolExecutor getExternalCompactionActiveCompactionsPool(
      Iterable<Entry<String,String>> conf, int numThreads) {
    return ThreadPools.createFixedThreadPool(numThreads, "getactivecompactions", false);
  }

  public ThreadPoolExecutor getScannerReadAheadPool(Iterable<Entry<String,String>> conf) {
    return ThreadPools.createThreadPool(0, Integer.MAX_VALUE, 3L, TimeUnit.SECONDS,
        "Accumulo scanner read ahead thread", new SynchronousQueue<>(), OptionalInt.empty(), false);
  }

  public ThreadPoolExecutor getAddSplitsThreadPool(Iterable<Entry<String,String>> conf) {
    return ThreadPools.createFixedThreadPool(16, "addSplits", false);
  }

  public ThreadPoolExecutor getBatchReaderThreadPool(Iterable<Entry<String,String>> conf,
      int numQueryThreads, int batchReaderInstance) {
    return ThreadPools.createFixedThreadPool(numQueryThreads,
        "batch scanner " + batchReaderInstance + "-", false);
  }

  public ScheduledThreadPoolExecutor
      getBatchWriterLatencyTasksThreadPool(Iterable<Entry<String,String>> conf) {
    return ThreadPools.createGeneralScheduledExecutorService(new ConfigurationCopy(conf));
  }

  @Override
  public ThreadPoolExecutor getBatchWriterBinningThreadPool(Iterable<Entry<String,String>> conf) {
    return ThreadPools.createFixedThreadPool(1, "BinMutations", new SynchronousQueue<>(), false);
  }

  @Override
  public ThreadPoolExecutor getBatchWriterSendThreadPool(Iterable<Entry<String,String>> conf,
      int numSendThreads) {
    return ThreadPools.createFixedThreadPool(numSendThreads, "MutationWriter", false);
  }

  @Override
  public ThreadPoolExecutor
      getConditionalWriterCleanupTaskThreadPool(Iterable<Entry<String,String>> conf) {
    return ThreadPools.createFixedThreadPool(1, 3, TimeUnit.SECONDS,
        "Conditional Writer Cleanup Thread", false);
  }

  @Override
  public ScheduledThreadPoolExecutor getConditionalWriterThreadPool(
      Iterable<Entry<String,String>> conf, ConditionalWriterConfig config) {
    return ThreadPools.createScheduledExecutorService(config.getMaxWriteThreads(),
        "ConiditionalWriterImpl", false);
  }

  @Override
  public ThreadPoolExecutor getBloomFilterLayerLoadThreadPool(Iterable<Entry<String,String>> conf) {
    return ThreadPools.createThreadPool(0,
        new ConfigurationCopy(conf).getCount(Property.TSERV_BLOOM_LOAD_MAXCONCURRENT), 60,
        TimeUnit.SECONDS, "bloom-loader", false);
  }

}

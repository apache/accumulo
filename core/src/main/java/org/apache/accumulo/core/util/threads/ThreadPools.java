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
package org.apache.accumulo.core.util.threads;

import java.util.OptionalInt;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;

public class ThreadPools {

  // the number of seconds before we allow a thread to terminate with non-use.
  public static final long DEFAULT_TIMEOUT_MILLISECS = 180000L;

  /**
   * Create a thread pool based on a thread pool related property
   *
   * @param conf
   *          accumulo configuration
   * @param p
   *          thread pool related property
   * @return ExecutorService impl
   * @throws RuntimeException
   *           if property is not handled
   */
  public static ExecutorService createExecutorService(AccumuloConfiguration conf, Property p) {

    switch (p) {
      case GENERAL_SIMPLETIMER_THREADPOOL_SIZE:
        return createScheduledExecutorService(conf.getCount(p), "SimpleTimer", false);
      case MASTER_BULK_THREADPOOL_SIZE:
        return createFixedThreadPool(conf.getCount(p),
            conf.getTimeInMillis(Property.MASTER_BULK_THREADPOOL_TIMEOUT), TimeUnit.MILLISECONDS,
            "bulk import", true);
      case MASTER_RENAME_THREADS:
        return createFixedThreadPool(conf.getCount(p), "bulk move", false);
      case MASTER_FATE_THREADPOOL_SIZE:
        return createFixedThreadPool(conf.getCount(p), "Repo Runner", false);
      case MASTER_STATUS_THREAD_POOL_SIZE:
        int threads = conf.getCount(p);
        if (threads == 0) {
          return createThreadPool(0, Integer.MAX_VALUE, 60L, TimeUnit.SECONDS,
              "GatherTableInformation", new SynchronousQueue<Runnable>(), OptionalInt.empty(),
              false);
        } else {
          return createFixedThreadPool(threads, "GatherTableInformation", false);
        }
      case TSERV_WORKQ_THREADS:
        return createFixedThreadPool(conf.getCount(p), "distributed work queue", false);
      case TSERV_MINC_MAXCONCURRENT:
        return createFixedThreadPool(conf.getCount(p), 0L, TimeUnit.MILLISECONDS, "minor compactor",
            true);
      case TSERV_MIGRATE_MAXCONCURRENT:
        return createFixedThreadPool(conf.getCount(p), 0L, TimeUnit.MILLISECONDS,
            "tablet migration", true);
      case TSERV_ASSIGNMENT_MAXCONCURRENT:
        return createFixedThreadPool(conf.getCount(p), 0L, TimeUnit.MILLISECONDS,
            "tablet assignment", true);
      case TSERV_SUMMARY_RETRIEVAL_THREADS:
        return createThreadPool(conf.getCount(p), conf.getCount(p), 60, TimeUnit.SECONDS,
            "summary file retriever", true);
      case TSERV_SUMMARY_REMOTE_THREADS:
        return createThreadPool(conf.getCount(p), conf.getCount(p), 60, TimeUnit.SECONDS,
            "summary remote", true);
      case TSERV_SUMMARY_PARTITION_THREADS:
        return createThreadPool(conf.getCount(p), conf.getCount(p), 60, TimeUnit.SECONDS,
            "summary partition", true);
      case GC_DELETE_THREADS:
        return createFixedThreadPool(conf.getCount(p), "deleting", false);
      case REPLICATION_WORKER_THREADS:
        return createFixedThreadPool(conf.getCount(p), "replication task", false);
      default:
        throw new RuntimeException("Unhandled thread pool property: " + p);
    }
  }

  public static ThreadPoolExecutor createFixedThreadPool(int numThreads, final String name,
      boolean enableTracing) {
    return createFixedThreadPool(numThreads, DEFAULT_TIMEOUT_MILLISECS, TimeUnit.MILLISECONDS, name,
        enableTracing);
  }

  public static ThreadPoolExecutor createFixedThreadPool(int numThreads, final String name,
      BlockingQueue<Runnable> queue, boolean enableTracing) {
    return createThreadPool(numThreads, numThreads, DEFAULT_TIMEOUT_MILLISECS,
        TimeUnit.MILLISECONDS, name, queue, OptionalInt.empty(), enableTracing);
  }

  public static ThreadPoolExecutor createFixedThreadPool(int numThreads, long timeOut,
      TimeUnit units, final String name, boolean enableTracing) {
    return createThreadPool(numThreads, numThreads, timeOut, units, name,
        new LinkedBlockingQueue<Runnable>(), OptionalInt.empty(), enableTracing);
  }

  public static ThreadPoolExecutor createThreadPool(int coreThreads, int maxThreads, long timeOut,
      TimeUnit units, final String name, boolean enableTracing) {
    return createThreadPool(coreThreads, maxThreads, timeOut, units, name,
        new LinkedBlockingQueue<Runnable>(), OptionalInt.empty(), enableTracing);
  }

  public static ThreadPoolExecutor createThreadPool(int coreThreads, int maxThreads, long timeOut,
      TimeUnit units, final String name, BlockingQueue<Runnable> queue, OptionalInt priority,
      boolean enableTracing) {
    ThreadPoolExecutor result = null;
    if (enableTracing) {
      result = new TracingThreadPoolExecutor(coreThreads, maxThreads, timeOut, units, queue,
          new NamedThreadFactory(name, priority));
    } else {
      result = new ThreadPoolExecutor(coreThreads, maxThreads, timeOut, units, queue,
          new NamedThreadFactory(name, priority));
    }
    if (timeOut > 0) {
      result.allowCoreThreadTimeOut(true);
    }
    return result;
  }

  public static ScheduledThreadPoolExecutor
      createGeneralScheduledExecutorService(AccumuloConfiguration conf) {
    return (ScheduledThreadPoolExecutor) createExecutorService(conf,
        Property.GENERAL_SIMPLETIMER_THREADPOOL_SIZE);
  }

  public static ScheduledThreadPoolExecutor createScheduledExecutorService(int numThreads,
      final String name, boolean enableTracing) {
    return createScheduledExecutorService(numThreads, name, OptionalInt.empty(), enableTracing);
  }

  public static ScheduledThreadPoolExecutor createScheduledExecutorService(int numThreads,
      final String name, OptionalInt priority, boolean enableTracing) {
    if (enableTracing) {
      return new TracingScheduledThreadPoolExecutor(numThreads,
          new NamedThreadFactory(name, priority));
    } else {
      return new ScheduledThreadPoolExecutor(numThreads, new NamedThreadFactory(name, priority));
    }
  }

}

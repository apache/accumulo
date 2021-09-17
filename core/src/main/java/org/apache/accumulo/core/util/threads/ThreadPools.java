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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ThreadPools {

  // the number of seconds before we allow a thread to terminate with non-use.
  public static final long DEFAULT_TIMEOUT_MILLISECS = 180000L;

  private static final Logger log = LoggerFactory.getLogger(ThreadPools.class);

  private static void makeResizeable(final ThreadPoolExecutor pool,
      final AccumuloConfiguration conf, final Property p) {
    final String threadName = p.name().concat("_watcher");
    Threads.createThread(threadName, () -> {
      int count = conf.getCount(p);
      while (Thread.currentThread().isAlive() && !Thread.currentThread().isInterrupted()) {
        try {
          Thread.sleep(1000);
          int newCount = conf.getCount(p);
          if (newCount != count) {
            log.info("Changing max threads for {} from {} to {}", p.getKey(), count, newCount);
            if (newCount > count) {
              // increasing, increase the max first, or the core will fail to be increased
              pool.setMaximumPoolSize(newCount);
              pool.setCorePoolSize(newCount);
            } else {
              // decreasing, lower the core size first, or the max will fail to be lowered
              pool.setCorePoolSize(newCount);
              pool.setMaximumPoolSize(newCount);
            }
            count = newCount;
          }
        } catch (InterruptedException e) {
          // throw a RuntimeException and let the AccumuloUncaughtExceptionHandler deal with it.
          throw new RuntimeException("Thread " + threadName + " was interrupted.");
        }
      }
    }).start();

  }

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
  public static ExecutorService createExecutorService(final AccumuloConfiguration conf,
      final Property p) {

    switch (p) {
      case GENERAL_SIMPLETIMER_THREADPOOL_SIZE:
        return createScheduledExecutorService(conf.getCount(p), "SimpleTimer", false);
      case MANAGER_BULK_THREADPOOL_SIZE:
        return createFixedThreadPool(conf.getCount(p),
            conf.getTimeInMillis(Property.MANAGER_BULK_THREADPOOL_TIMEOUT), TimeUnit.MILLISECONDS,
            "bulk import", true);
      case MANAGER_RENAME_THREADS:
        return createFixedThreadPool(conf.getCount(p), "bulk move", false);
      case MANAGER_FATE_THREADPOOL_SIZE:
        ThreadPoolExecutor pool = createFixedThreadPool(conf.getCount(p), "Repo Runner", false);
        makeResizeable(pool, conf, p);
        return pool;
      case MANAGER_STATUS_THREAD_POOL_SIZE:
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
        ThreadPoolExecutor pool2 =
            createFixedThreadPool(conf.getCount(p), "replication task", false);
        makeResizeable(pool2, conf, p);
        return pool2;
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

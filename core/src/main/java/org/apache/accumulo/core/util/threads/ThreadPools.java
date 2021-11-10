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
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.IntSupplier;

import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.metrics.MetricsUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.opentelemetry.context.Context;

public class ThreadPools {

  private static final Logger LOG = LoggerFactory.getLogger(ThreadPools.class);

  // the number of seconds before we allow a thread to terminate with non-use.
  public static final long DEFAULT_TIMEOUT_MILLISECS = 180000L;

  /**
   * Resize ThreadPoolExecutor based on current value of maxThreads
   *
   * @param pool
   *          the ThreadPoolExecutor to modify
   * @param maxThreads
   *          supplier of maxThreads value
   * @param poolName
   *          name of the thread pool
   */
  public static void resizePool(final ThreadPoolExecutor pool, final IntSupplier maxThreads,
      String poolName) {
    int count = pool.getMaximumPoolSize();
    int newCount = maxThreads.getAsInt();
    if (count == newCount) {
      return;
    }
    LOG.info("Changing max threads for {} from {} to {}", poolName, count, newCount);
    if (newCount > count) {
      // increasing, increase the max first, or the core will fail to be increased
      pool.setMaximumPoolSize(newCount);
      pool.setCorePoolSize(newCount);
    } else {
      // decreasing, lower the core size first, or the max will fail to be lowered
      pool.setCorePoolSize(newCount);
      pool.setMaximumPoolSize(newCount);
    }

  }

  /**
   * Resize ThreadPoolExecutor based on current value of Property p
   *
   * @param pool
   *          the ThreadPoolExecutor to modify
   * @param conf
   *          the AccumuloConfiguration
   * @param p
   *          the property to base the size from
   */
  public static void resizePool(final ThreadPoolExecutor pool, final AccumuloConfiguration conf,
      final Property p) {
    resizePool(pool, () -> conf.getCount(p), p.getKey());
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
        return createScheduledExecutorService(conf.getCount(p), "SimpleTimer");
      case MANAGER_BULK_THREADPOOL_SIZE:
        return createFixedThreadPool(conf.getCount(p),
            conf.getTimeInMillis(Property.MANAGER_BULK_THREADPOOL_TIMEOUT), TimeUnit.MILLISECONDS,
            "bulk import");
      case MANAGER_RENAME_THREADS:
        return createFixedThreadPool(conf.getCount(p), "bulk move");
      case MANAGER_FATE_THREADPOOL_SIZE:
        return createFixedThreadPool(conf.getCount(p), "Repo Runner");
      case MANAGER_STATUS_THREAD_POOL_SIZE:
        int threads = conf.getCount(p);
        if (threads == 0) {
          return createThreadPool(0, Integer.MAX_VALUE, 60L, TimeUnit.SECONDS,
              "GatherTableInformation", new SynchronousQueue<Runnable>(), OptionalInt.empty());
        } else {
          return createFixedThreadPool(threads, "GatherTableInformation");
        }
      case TSERV_WORKQ_THREADS:
        return createFixedThreadPool(conf.getCount(p), "distributed work queue");
      case TSERV_MINC_MAXCONCURRENT:
        return createFixedThreadPool(conf.getCount(p), 0L, TimeUnit.MILLISECONDS,
            "minor compactor");
      case TSERV_MIGRATE_MAXCONCURRENT:
        return createFixedThreadPool(conf.getCount(p), 0L, TimeUnit.MILLISECONDS,
            "tablet migration");
      case TSERV_ASSIGNMENT_MAXCONCURRENT:
        return createFixedThreadPool(conf.getCount(p), 0L, TimeUnit.MILLISECONDS,
            "tablet assignment");
      case TSERV_SUMMARY_RETRIEVAL_THREADS:
        return createThreadPool(conf.getCount(p), conf.getCount(p), 60, TimeUnit.SECONDS,
            "summary file retriever");
      case TSERV_SUMMARY_REMOTE_THREADS:
        return createThreadPool(conf.getCount(p), conf.getCount(p), 60, TimeUnit.SECONDS,
            "summary remote");
      case TSERV_SUMMARY_PARTITION_THREADS:
        return createThreadPool(conf.getCount(p), conf.getCount(p), 60, TimeUnit.SECONDS,
            "summary partition");
      case GC_DELETE_THREADS:
        return createFixedThreadPool(conf.getCount(p), "deleting");
      case REPLICATION_WORKER_THREADS:
        return createFixedThreadPool(conf.getCount(p), "replication task");
      default:
        throw new RuntimeException("Unhandled thread pool property: " + p);
    }
  }

  public static ThreadPoolExecutor createFixedThreadPool(int numThreads, final String name) {
    return createFixedThreadPool(numThreads, DEFAULT_TIMEOUT_MILLISECS, TimeUnit.MILLISECONDS,
        name);
  }

  public static ThreadPoolExecutor createFixedThreadPool(int numThreads, final String name,
      BlockingQueue<Runnable> queue) {
    return createThreadPool(numThreads, numThreads, DEFAULT_TIMEOUT_MILLISECS,
        TimeUnit.MILLISECONDS, name, queue, OptionalInt.empty());
  }

  public static ThreadPoolExecutor createFixedThreadPool(int numThreads, long timeOut,
      TimeUnit units, final String name) {
    return createThreadPool(numThreads, numThreads, timeOut, units, name,
        new LinkedBlockingQueue<Runnable>(), OptionalInt.empty());
  }

  public static ThreadPoolExecutor createThreadPool(int coreThreads, int maxThreads, long timeOut,
      TimeUnit units, final String name) {
    return createThreadPool(coreThreads, maxThreads, timeOut, units, name,
        new LinkedBlockingQueue<Runnable>(), OptionalInt.empty());
  }

  public static ThreadPoolExecutor createThreadPool(int coreThreads, int maxThreads, long timeOut,
      TimeUnit units, final String name, BlockingQueue<Runnable> queue, OptionalInt priority) {
    ThreadPoolExecutor result = new ThreadPoolExecutor(coreThreads, maxThreads, timeOut, units,
        queue, new NamedThreadFactory(name, priority)) {

      @Override
      public void execute(Runnable arg0) {
        super.execute(Context.current().wrap(arg0));
      }

      @Override
      public boolean remove(Runnable task) {
        return super.remove(Context.current().wrap(task));
      }

      @Override
      public <T> Future<T> submit(Callable<T> task) {
        return super.submit(Context.current().wrap(task));
      }

      @Override
      public <T> Future<T> submit(Runnable task, T result) {
        return super.submit(Context.current().wrap(task), result);
      }

      @Override
      public Future<?> submit(Runnable task) {
        return super.submit(Context.current().wrap(task));
      }
    };
    if (timeOut > 0) {
      result.allowCoreThreadTimeOut(true);
    }
    MetricsUtil.addExecutorServiceMetrics(result, name);
    return result;
  }

  /*
   * If you need the server-side shared ScheduledThreadPoolExecutor, then use
   * ServerContext.getScheduledExecutor()
   */
  public static ScheduledThreadPoolExecutor
      createGeneralScheduledExecutorService(AccumuloConfiguration conf) {
    return (ScheduledThreadPoolExecutor) createExecutorService(conf,
        Property.GENERAL_SIMPLETIMER_THREADPOOL_SIZE);
  }

  public static ScheduledThreadPoolExecutor createScheduledExecutorService(int numThreads,
      final String name) {
    return createScheduledExecutorService(numThreads, name, OptionalInt.empty());
  }

  public static ScheduledThreadPoolExecutor createScheduledExecutorService(int numThreads,
      final String name, OptionalInt priority) {
    ScheduledThreadPoolExecutor result =
        new ScheduledThreadPoolExecutor(numThreads, new NamedThreadFactory(name, priority)) {

          @Override
          public void execute(Runnable command) {
            super.execute(Context.current().wrap(command));
          }

          @Override
          public <V> ScheduledFuture<V> schedule(Callable<V> callable, long delay, TimeUnit unit) {
            return super.schedule(Context.current().wrap(callable), delay, unit);
          }

          @Override
          public ScheduledFuture<?> schedule(Runnable command, long delay, TimeUnit unit) {
            return super.schedule(Context.current().wrap(command), delay, unit);
          }

          @Override
          public ScheduledFuture<?> scheduleAtFixedRate(Runnable command, long initialDelay,
              long period, TimeUnit unit) {
            return super.scheduleAtFixedRate(Context.current().wrap(command), initialDelay, period,
                unit);
          }

          @Override
          public ScheduledFuture<?> scheduleWithFixedDelay(Runnable command, long initialDelay,
              long delay, TimeUnit unit) {
            return super.scheduleWithFixedDelay(Context.current().wrap(command), initialDelay,
                delay, unit);
          }

          @Override
          public <T> Future<T> submit(Callable<T> task) {
            return super.submit(Context.current().wrap(task));
          }

          @Override
          public <T> Future<T> submit(Runnable task, T result) {
            return super.submit(Context.current().wrap(task), result);
          }

          @Override
          public Future<?> submit(Runnable task) {
            return super.submit(Context.current().wrap(task));
          }

          @Override
          public boolean remove(Runnable task) {
            return super.remove(Context.current().wrap(task));
          }

        };
    MetricsUtil.addExecutorServiceMetrics(result, name);
    return result;
  }

}

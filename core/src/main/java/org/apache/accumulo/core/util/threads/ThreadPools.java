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
import org.apache.accumulo.core.trace.TraceUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
   * @param emitThreadPoolMetrics
   *          When set to true will emit metrics and register the metrics in a static registry.
   *          After the thread pool is deleted, there will still be metrics objects related to it in
   *          the static registry. There is no way to clean these left over objects up therefore its
   *          recommended that this option only be set true for long lived thread pools. Creating
   *          lots of short lived thread pools and registering them can lead to out of memory errors
   *          over long time periods.
   * @return ExecutorService impl
   * @throws RuntimeException
   *           if property is not handled
   */
  @SuppressWarnings("deprecation")
  public static ThreadPoolExecutor createExecutorService(final AccumuloConfiguration conf,
      final Property p, boolean emitThreadPoolMetrics) {

    switch (p) {
      case GENERAL_SIMPLETIMER_THREADPOOL_SIZE:
        return createScheduledExecutorService(conf.getCount(p), "SimpleTimer",
            emitThreadPoolMetrics);
      case MANAGER_BULK_THREADPOOL_SIZE:
        return createFixedThreadPool(conf.getCount(p),
            conf.getTimeInMillis(Property.MANAGER_BULK_THREADPOOL_TIMEOUT), TimeUnit.MILLISECONDS,
            "bulk import", emitThreadPoolMetrics);
      case MANAGER_RENAME_THREADS:
        return createFixedThreadPool(conf.getCount(p), "bulk move", emitThreadPoolMetrics);
      case MANAGER_FATE_THREADPOOL_SIZE:
        return createFixedThreadPool(conf.getCount(p), "Repo Runner", emitThreadPoolMetrics);
      case MANAGER_STATUS_THREAD_POOL_SIZE:
        int threads = conf.getCount(p);
        if (threads == 0) {
          return createThreadPool(0, Integer.MAX_VALUE, 60L, TimeUnit.SECONDS,
              "GatherTableInformation", new SynchronousQueue<>(), emitThreadPoolMetrics);
        } else {
          return createFixedThreadPool(threads, "GatherTableInformation", emitThreadPoolMetrics);
        }
      case TSERV_WORKQ_THREADS:
        return createFixedThreadPool(conf.getCount(p), "distributed work queue",
            emitThreadPoolMetrics);
      case TSERV_MINC_MAXCONCURRENT:
        return createFixedThreadPool(conf.getCount(p), 0L, TimeUnit.MILLISECONDS, "minor compactor",
            emitThreadPoolMetrics);
      case TSERV_MIGRATE_MAXCONCURRENT:
        return createFixedThreadPool(conf.getCount(p), 0L, TimeUnit.MILLISECONDS,
            "tablet migration", emitThreadPoolMetrics);
      case TSERV_ASSIGNMENT_MAXCONCURRENT:
        return createFixedThreadPool(conf.getCount(p), 0L, TimeUnit.MILLISECONDS,
            "tablet assignment", emitThreadPoolMetrics);
      case TSERV_SUMMARY_RETRIEVAL_THREADS:
        return createThreadPool(conf.getCount(p), conf.getCount(p), 60, TimeUnit.SECONDS,
            "summary file retriever", emitThreadPoolMetrics);
      case TSERV_SUMMARY_REMOTE_THREADS:
        return createThreadPool(conf.getCount(p), conf.getCount(p), 60, TimeUnit.SECONDS,
            "summary remote", emitThreadPoolMetrics);
      case TSERV_SUMMARY_PARTITION_THREADS:
        return createThreadPool(conf.getCount(p), conf.getCount(p), 60, TimeUnit.SECONDS,
            "summary partition", emitThreadPoolMetrics);
      case GC_DELETE_THREADS:
        return createFixedThreadPool(conf.getCount(p), "deleting", emitThreadPoolMetrics);
      case REPLICATION_WORKER_THREADS:
        return createFixedThreadPool(conf.getCount(p), "replication task", emitThreadPoolMetrics);
      default:
        throw new RuntimeException("Unhandled thread pool property: " + p);
    }
  }

  /**
   * Create a named thread pool
   *
   * @param numThreads
   *          number of threads
   * @param name
   *          thread pool name
   * @param emitThreadPoolMetrics
   *          When set to true will emit metrics and register the metrics in a static registry.
   *          After the thread pool is deleted, there will still be metrics objects related to it in
   *          the static registry. There is no way to clean these left over objects up therefore its
   *          recommended that this option only be set true for long lived thread pools. Creating
   *          lots of short lived thread pools and registering them can lead to out of memory errors
   *          over long time periods.
   * @return ThreadPoolExecutor
   */
  public static ThreadPoolExecutor createFixedThreadPool(int numThreads, final String name,
      boolean emitThreadPoolMetrics) {
    return createFixedThreadPool(numThreads, DEFAULT_TIMEOUT_MILLISECS, TimeUnit.MILLISECONDS, name,
        emitThreadPoolMetrics);
  }

  /**
   * Create a named thread pool
   *
   * @param numThreads
   *          number of threads
   * @param name
   *          thread pool name
   * @param queue
   *          queue to use for tasks
   * @param emitThreadPoolMetrics
   *          When set to true will emit metrics and register the metrics in a static registry.
   *          After the thread pool is deleted, there will still be metrics objects related to it in
   *          the static registry. There is no way to clean these left over objects up therefore its
   *          recommended that this option only be set true for long lived thread pools. Creating
   *          lots of short lived thread pools and registering them can lead to out of memory errors
   *          over long time periods.
   * @return ThreadPoolExecutor
   */
  public static ThreadPoolExecutor createFixedThreadPool(int numThreads, final String name,
      BlockingQueue<Runnable> queue, boolean emitThreadPoolMetrics) {
    return createThreadPool(numThreads, numThreads, DEFAULT_TIMEOUT_MILLISECS,
        TimeUnit.MILLISECONDS, name, queue, emitThreadPoolMetrics);
  }

  /**
   * Create a named thread pool
   *
   * @param numThreads
   *          number of threads
   * @param timeOut
   *          core thread time out
   * @param units
   *          core thread time out units
   * @param name
   *          thread pool name
   * @param emitThreadPoolMetrics
   *          When set to true will emit metrics and register the metrics in a static registry.
   *          After the thread pool is deleted, there will still be metrics objects related to it in
   *          the static registry. There is no way to clean these left over objects up therefore its
   *          recommended that this option only be set true for long lived thread pools. Creating
   *          lots of short lived thread pools and registering them can lead to out of memory errors
   *          over long time periods.
   * @return ThreadPoolExecutor
   */
  public static ThreadPoolExecutor createFixedThreadPool(int numThreads, long timeOut,
      TimeUnit units, final String name, boolean emitThreadPoolMetrics) {
    return createThreadPool(numThreads, numThreads, timeOut, units, name, emitThreadPoolMetrics);
  }

  /**
   * Create a named thread pool
   *
   * @param coreThreads
   *          number of threads
   * @param maxThreads
   *          max number of threads
   * @param timeOut
   *          core thread time out
   * @param units
   *          core thread time out units
   * @param name
   *          thread pool name
   * @param emitThreadPoolMetrics
   *          When set to true will emit metrics and register the metrics in a static registry.
   *          After the thread pool is deleted, there will still be metrics objects related to it in
   *          the static registry. There is no way to clean these left over objects up therefore its
   *          recommended that this option only be set true for long lived thread pools. Creating
   *          lots of short lived thread pools and registering them can lead to out of memory errors
   *          over long time periods.
   * @return ThreadPoolExecutor
   */
  public static ThreadPoolExecutor createThreadPool(int coreThreads, int maxThreads, long timeOut,
      TimeUnit units, final String name, boolean emitThreadPoolMetrics) {
    return createThreadPool(coreThreads, maxThreads, timeOut, units, name,
        new LinkedBlockingQueue<>(), emitThreadPoolMetrics);
  }

  /**
   * Create a named thread pool
   *
   * @param coreThreads
   *          number of threads
   * @param maxThreads
   *          max number of threads
   * @param timeOut
   *          core thread time out
   * @param units
   *          core thread time out units
   * @param name
   *          thread pool name
   * @param queue
   *          queue to use for tasks
   * @param emitThreadPoolMetrics
   *          When set to true will emit metrics and register the metrics in a static registry.
   *          After the thread pool is deleted, there will still be metrics objects related to it in
   *          the static registry. There is no way to clean these left over objects up therefore its
   *          recommended that this option only be set true for long lived thread pools. Creating
   *          lots of short lived thread pools and registering them can lead to out of memory errors
   *          over long time periods.
   * @return ThreadPoolExecutor
   */
  public static ThreadPoolExecutor createThreadPool(int coreThreads, int maxThreads, long timeOut,
      TimeUnit units, final String name, BlockingQueue<Runnable> queue,
      boolean emitThreadPoolMetrics) {
    return createThreadPool(coreThreads, maxThreads, timeOut, units, name, queue,
        OptionalInt.empty(), emitThreadPoolMetrics);
  }

  /**
   * Create a named thread pool
   *
   * @param coreThreads
   *          number of threads
   * @param maxThreads
   *          max number of threads
   * @param timeOut
   *          core thread time out
   * @param units
   *          core thread time out units
   * @param name
   *          thread pool name
   * @param queue
   *          queue to use for tasks
   * @param priority
   *          thread priority
   * @param emitThreadPoolMetrics
   *          When set to true will emit metrics and register the metrics in a static registry.
   *          After the thread pool is deleted, there will still be metrics objects related to it in
   *          the static registry. There is no way to clean these left over objects up therefore its
   *          recommended that this option only be set true for long lived thread pools. Creating
   *          lots of short lived thread pools and registering them can lead to out of memory errors
   *          over long time periods.
   * @return ThreadPoolExecutor
   */
  public static ThreadPoolExecutor createThreadPool(int coreThreads, int maxThreads, long timeOut,
      TimeUnit units, final String name, BlockingQueue<Runnable> queue, OptionalInt priority,
      boolean emitThreadPoolMetrics) {
    var result = new ThreadPoolExecutor(coreThreads, maxThreads, timeOut, units, queue,
        new NamedThreadFactory(name, priority)) {

      @Override
      public void execute(Runnable arg0) {
        super.execute(TraceUtil.wrap(arg0));
      }

      @Override
      public boolean remove(Runnable task) {
        return super.remove(TraceUtil.wrap(task));
      }

      @Override
      public <T> Future<T> submit(Callable<T> task) {
        return super.submit(TraceUtil.wrap(task));
      }

      @Override
      public <T> Future<T> submit(Runnable task, T result) {
        return super.submit(TraceUtil.wrap(task), result);
      }

      @Override
      public Future<?> submit(Runnable task) {
        return super.submit(TraceUtil.wrap(task));
      }
    };
    if (timeOut > 0) {
      result.allowCoreThreadTimeOut(true);
    }
    if (emitThreadPoolMetrics) {
      MetricsUtil.addExecutorServiceMetrics(result, name);
    }
    return result;
  }

  /*
   * If you need the server-side shared ScheduledThreadPoolExecutor, then use
   * ServerContext.getScheduledExecutor()
   */
  public static ScheduledThreadPoolExecutor
      createGeneralScheduledExecutorService(AccumuloConfiguration conf) {
    return (ScheduledThreadPoolExecutor) createExecutorService(conf,
        Property.GENERAL_SIMPLETIMER_THREADPOOL_SIZE, true);
  }

  /**
   * Create a named ScheduledThreadPool
   *
   * @param numThreads
   *          number of threads
   * @param name
   *          thread pool name
   * @param emitThreadPoolMetrics
   *          When set to true will emit metrics and register the metrics in a static registry.
   *          After the thread pool is deleted, there will still be metrics objects related to it in
   *          the static registry. There is no way to clean these left over objects up therefore its
   *          recommended that this option only be set true for long lived thread pools. Creating
   *          lots of short lived thread pools and registering them can lead to out of memory errors
   *          over long time periods.
   * @return ScheduledThreadPoolExecutor
   */
  public static ScheduledThreadPoolExecutor createScheduledExecutorService(int numThreads,
      final String name, boolean emitThreadPoolMetrics) {
    var result = new ScheduledThreadPoolExecutor(numThreads, new NamedThreadFactory(name)) {

      @Override
      public void execute(Runnable command) {
        super.execute(TraceUtil.wrap(command));
      }

      @Override
      public <V> ScheduledFuture<V> schedule(Callable<V> callable, long delay, TimeUnit unit) {
        return super.schedule(TraceUtil.wrap(callable), delay, unit);
      }

      @Override
      public ScheduledFuture<?> schedule(Runnable command, long delay, TimeUnit unit) {
        return super.schedule(TraceUtil.wrap(command), delay, unit);
      }

      @Override
      public ScheduledFuture<?> scheduleAtFixedRate(Runnable command, long initialDelay,
          long period, TimeUnit unit) {
        return super.scheduleAtFixedRate(TraceUtil.wrap(command), initialDelay, period, unit);
      }

      @Override
      public ScheduledFuture<?> scheduleWithFixedDelay(Runnable command, long initialDelay,
          long delay, TimeUnit unit) {
        return super.scheduleWithFixedDelay(TraceUtil.wrap(command), initialDelay, delay, unit);
      }

      @Override
      public <T> Future<T> submit(Callable<T> task) {
        return super.submit(TraceUtil.wrap(task));
      }

      @Override
      public <T> Future<T> submit(Runnable task, T result) {
        return super.submit(TraceUtil.wrap(task), result);
      }

      @Override
      public Future<?> submit(Runnable task) {
        return super.submit(TraceUtil.wrap(task));
      }

      @Override
      public boolean remove(Runnable task) {
        return super.remove(TraceUtil.wrap(task));
      }

    };
    if (emitThreadPoolMetrics) {
      MetricsUtil.addExecutorServiceMetrics(result, name);
    }
    return result;
  }

}

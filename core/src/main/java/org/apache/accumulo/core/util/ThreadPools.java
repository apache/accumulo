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
package org.apache.accumulo.core.util;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.OptionalInt;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.util.Threads.NamedRunnable;
import org.apache.htrace.wrappers.TraceCallable;
import org.apache.htrace.wrappers.TraceRunnable;

public class ThreadPools {

  /**
   * ThreadFactory that sets the name and optionally the priority on a newly created Thread.
   */
  private static class NamedThreadFactory implements ThreadFactory {

    private static final String FORMAT = "%s-%s-%d";

    private AtomicInteger threadNum = new AtomicInteger(1);
    private String name;
    private OptionalInt priority;

    private NamedThreadFactory(String name) {
      this(name, OptionalInt.empty());
    }

    private NamedThreadFactory(String name, OptionalInt priority) {
      this.name = name;
      this.priority = priority;
    }

    @Override
    public Thread newThread(Runnable r) {
      String threadName = null;
      if (r instanceof NamedRunnable) {
        NamedRunnable nr = (NamedRunnable) r;
        threadName = String.format(FORMAT, name, nr.getName(), threadNum.getAndIncrement());
      } else {
        threadName =
            String.format(FORMAT, name, r.getClass().getSimpleName(), threadNum.getAndIncrement());
      }
      return Threads.createThread(threadName, priority, r);
    }
  }

  /**
   * ScheduledThreadPoolExecutor that traces executed tasks.
   */
  public static class TracingScheduledThreadPoolExecutor extends ScheduledThreadPoolExecutor {

    private TracingScheduledThreadPoolExecutor(int corePoolSize, ThreadFactory threadFactory) {
      super(corePoolSize, threadFactory);
    }

    @Override
    public void execute(Runnable command) {
      super.execute(new TraceRunnable(command));
    }

    @Override
    public <T> Future<T> submit(Callable<T> task) {
      return super.submit(new TraceCallable<T>(task));
    }

    @Override
    public <T> Future<T> submit(Runnable task, T result) {
      return super.submit(new TraceRunnable(task), result);
    }

    @Override
    public Future<?> submit(Runnable task) {
      return super.submit(new TraceRunnable(task));
    }

    private <T> Collection<? extends Callable<T>>
        wrapCollection(Collection<? extends Callable<T>> tasks) {
      List<Callable<T>> result = new ArrayList<Callable<T>>(tasks.size());
      tasks.forEach(t -> result.add(new TraceCallable<T>(t)));
      return result;
    }

    @Override
    public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks)
        throws InterruptedException {
      return super.invokeAll(wrapCollection(tasks));
    }

    @Override
    public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks, long timeout,
        TimeUnit unit) throws InterruptedException {
      return super.invokeAll(wrapCollection(tasks), timeout, unit);
    }

    @Override
    public <T> T invokeAny(Collection<? extends Callable<T>> tasks)
        throws InterruptedException, ExecutionException {
      return super.invokeAny(wrapCollection(tasks));
    }

    @Override
    public <T> T invokeAny(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit)
        throws InterruptedException, ExecutionException, TimeoutException {
      return super.invokeAny(wrapCollection(tasks), timeout, unit);
    }

    @Override
    public void shutdown() {
      super.shutdown();
    }

    @Override
    public List<Runnable> shutdownNow() {
      return super.shutdownNow();
    }

  }

  /**
   * ThreadPoolExecutor that traces executed tasks.
   */
  public static class TracingThreadPoolExecutor extends ThreadPoolExecutor {

    private TracingThreadPoolExecutor(int corePoolSize, int maximumPoolSize, long keepAliveTime,
        TimeUnit unit, BlockingQueue<Runnable> workQueue, ThreadFactory threadFactory) {
      super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue, threadFactory);
    }

    @Override
    public void execute(Runnable command) {
      super.execute(new TraceRunnable(command));
    }

    @Override
    public <T> Future<T> submit(Callable<T> task) {
      return super.submit(new TraceCallable<T>(task));
    }

    @Override
    public <T> Future<T> submit(Runnable task, T result) {
      return super.submit(new TraceRunnable(task), result);
    }

    @Override
    public Future<?> submit(Runnable task) {
      return super.submit(new TraceRunnable(task));
    }

    private <T> Collection<? extends Callable<T>>
        wrapCollection(Collection<? extends Callable<T>> tasks) {
      List<Callable<T>> result = new ArrayList<Callable<T>>(tasks.size());
      tasks.forEach(t -> result.add(new TraceCallable<T>(t)));
      return result;
    }

    @Override
    public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks)
        throws InterruptedException {
      return super.invokeAll(wrapCollection(tasks));
    }

    @Override
    public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks, long timeout,
        TimeUnit unit) throws InterruptedException {
      return super.invokeAll(wrapCollection(tasks), timeout, unit);
    }

    @Override
    public <T> T invokeAny(Collection<? extends Callable<T>> tasks)
        throws InterruptedException, ExecutionException {
      return super.invokeAny(wrapCollection(tasks));
    }

    @Override
    public <T> T invokeAny(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit)
        throws InterruptedException, ExecutionException, TimeoutException {
      return super.invokeAny(wrapCollection(tasks), timeout, unit);
    }

    @Override
    public void shutdown() {
      super.shutdown();
    }

    @Override
    public List<Runnable> shutdownNow() {
      return super.shutdownNow();
    }

  }

  public static class CloseableThreadPoolExecutor implements AutoCloseable {

    private final ThreadPoolExecutor tpe;

    public CloseableThreadPoolExecutor(ThreadPoolExecutor tpe) {
      this.tpe = tpe;
    }

    @Override
    public void close() throws Exception {
      this.tpe.shutdownNow();
    }

  }

  // the number of seconds before we allow a thread to terminate with non-use.
  public static final long DEFAULT_TIMEOUT_MILLISECS = 180000L;

  /**
   * Get a thread pool based on a thread pool related property
   *
   * @param conf
   *          accumulo configuration
   * @param p
   *          thread pool related property
   * @return ExecutorService impl
   * @throws RuntimeException
   *           if property is not handled
   */
  public static ExecutorService getExecutorService(AccumuloConfiguration conf, Property p) {

    switch (p) {
      case GENERAL_SIMPLETIMER_THREADPOOL_SIZE:
        return getScheduledExecutorService(conf.getCount(p), "SimpleTimer", false);
      case MASTER_BULK_THREADPOOL_SIZE:
        return getFixedThreadPool(conf.getCount(p),
            conf.getTimeInMillis(Property.MASTER_BULK_THREADPOOL_TIMEOUT), TimeUnit.MILLISECONDS,
            "bulk import", true);
      case MASTER_RENAME_THREADS:
        return getFixedThreadPool(conf.getCount(p), "bulk move", false);
      case MASTER_FATE_THREADPOOL_SIZE:
        return getFixedThreadPool(conf.getCount(p), "Repo Runner", false);
      case MASTER_STATUS_THREAD_POOL_SIZE:
        int threads = conf.getCount(p);
        if (threads == 0) {
          return getThreadPool(0, Integer.MAX_VALUE, 60L, TimeUnit.SECONDS,
              "GatherTableInformation", new SynchronousQueue<Runnable>(), OptionalInt.empty(),
              false);
        } else {
          return getFixedThreadPool(threads, "GatherTableInformation", false);
        }
      case TSERV_WORKQ_THREADS:
        return getFixedThreadPool(conf.getCount(p), "distributed work queue", false);
      case TSERV_MINC_MAXCONCURRENT:
        return getFixedThreadPool(conf.getCount(p), 0L, TimeUnit.MILLISECONDS, "minor compactor",
            true);
      case TSERV_MIGRATE_MAXCONCURRENT:
        return getFixedThreadPool(conf.getCount(p), 0L, TimeUnit.MILLISECONDS, "tablet migration",
            true);
      case TSERV_ASSIGNMENT_MAXCONCURRENT:
        return getFixedThreadPool(conf.getCount(p), 0L, TimeUnit.MILLISECONDS, "tablet assignment",
            true);
      case TSERV_SUMMARY_RETRIEVAL_THREADS:
        return getThreadPool(conf.getCount(p), conf.getCount(p), 60, TimeUnit.SECONDS,
            "summary file retriever", true);
      case TSERV_SUMMARY_REMOTE_THREADS:
        return getThreadPool(conf.getCount(p), conf.getCount(p), 60, TimeUnit.SECONDS,
            "summary remote", true);
      case TSERV_SUMMARY_PARTITION_THREADS:
        return getThreadPool(conf.getCount(p), conf.getCount(p), 60, TimeUnit.SECONDS,
            "summary partition", true);
      case GC_DELETE_THREADS:
        return getFixedThreadPool(conf.getCount(p), "deleting", false);
      case REPLICATION_WORKER_THREADS:
        return getFixedThreadPool(conf.getCount(p), "replication task", false);
      default:
        throw new RuntimeException("Unhandled thread pool property: " + p);
    }
  }

  public static ThreadPoolExecutor getFixedThreadPool(int numThreads, final String name,
      boolean enableTracing) {
    return getFixedThreadPool(numThreads, DEFAULT_TIMEOUT_MILLISECS, TimeUnit.MILLISECONDS, name,
        enableTracing);
  }

  public static ThreadPoolExecutor getFixedThreadPool(int numThreads, final String name,
      BlockingQueue<Runnable> queue, boolean enableTracing) {
    return getThreadPool(numThreads, numThreads, DEFAULT_TIMEOUT_MILLISECS, TimeUnit.MILLISECONDS,
        name, queue, OptionalInt.empty(), enableTracing);
  }

  public static ThreadPoolExecutor getFixedThreadPool(int numThreads, long timeOut, TimeUnit units,
      final String name, boolean enableTracing) {
    return getThreadPool(numThreads, numThreads, timeOut, units, name,
        new LinkedBlockingQueue<Runnable>(), OptionalInt.empty(), enableTracing);
  }

  public static ThreadPoolExecutor getThreadPool(int coreThreads, int maxThreads, long timeOut,
      TimeUnit units, final String name, boolean enableTracing) {
    return getThreadPool(coreThreads, maxThreads, timeOut, units, name,
        new LinkedBlockingQueue<Runnable>(), OptionalInt.empty(), enableTracing);
  }

  public static ThreadPoolExecutor getThreadPool(int coreThreads, int maxThreads, long timeOut,
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
      getGeneralScheduledExecutorService(AccumuloConfiguration conf) {
    return (ScheduledThreadPoolExecutor) getExecutorService(conf,
        Property.GENERAL_SIMPLETIMER_THREADPOOL_SIZE);
  }

  public static ScheduledThreadPoolExecutor getScheduledExecutorService(int numThreads,
      final String name, boolean enableTracing) {
    return getScheduledExecutorService(numThreads, name, OptionalInt.empty(), enableTracing);
  }

  public static ScheduledThreadPoolExecutor getScheduledExecutorService(int numThreads,
      final String name, OptionalInt priority, boolean enableTracing) {
    if (enableTracing) {
      return new TracingScheduledThreadPoolExecutor(numThreads,
          new NamedThreadFactory(name, priority));
    } else {
      return new ScheduledThreadPoolExecutor(numThreads, new NamedThreadFactory(name, priority));
    }
  }

}

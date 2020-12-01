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

import java.lang.Thread.UncaughtExceptionHandler;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalInt;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.fate.util.LoggingRunnable;
import org.apache.htrace.wrappers.TraceCallable;
import org.apache.htrace.wrappers.TraceRunnable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ThreadPools {

  public static class CachedScheduledThreadPoolExecutor extends ScheduledThreadPoolExecutor {
    private String name = null;
    private boolean tracingEnabled = false;

    public CachedScheduledThreadPoolExecutor(int corePoolSize, RejectedExecutionHandler handler) {
      super(corePoolSize, handler);
    }

    public CachedScheduledThreadPoolExecutor(int corePoolSize, ThreadFactory threadFactory,
        RejectedExecutionHandler handler) {
      super(corePoolSize, threadFactory, handler);
    }

    public CachedScheduledThreadPoolExecutor(int corePoolSize, ThreadFactory threadFactory) {
      super(corePoolSize, threadFactory);
    }

    public CachedScheduledThreadPoolExecutor(int corePoolSize) {
      super(corePoolSize);
    }

    public void setName(String name) {
      this.name = name;
    }

    public void enableTracing() {
      this.tracingEnabled = true;
    }

    @Override
    public void execute(Runnable command) {
      if (tracingEnabled) {
        super.execute(new TraceRunnable(command));
      } else {
        super.execute(command);
      }
    }

    @Override
    public <T> Future<T> submit(Callable<T> task) {
      if (tracingEnabled) {
        return super.submit(new TraceCallable<T>(task));
      } else {
        return super.submit(task);
      }
    }

    @Override
    public <T> Future<T> submit(Runnable task, T result) {
      if (tracingEnabled) {
        return super.submit(new TraceRunnable(task), result);
      } else {
        return super.submit(task, result);
      }
    }

    @Override
    public Future<?> submit(Runnable task) {
      if (tracingEnabled) {
        return super.submit(new TraceRunnable(task));
      } else {
        return super.submit(task);
      }
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
      if (tracingEnabled) {
        return super.invokeAll(wrapCollection(tasks));
      } else {
        return super.invokeAll(tasks);
      }
    }

    @Override
    public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks, long timeout,
        TimeUnit unit) throws InterruptedException {
      if (tracingEnabled) {
        return super.invokeAll(wrapCollection(tasks), timeout, unit);
      } else {
        return super.invokeAll(tasks, timeout, unit);
      }
    }

    @Override
    public <T> T invokeAny(Collection<? extends Callable<T>> tasks)
        throws InterruptedException, ExecutionException {
      if (tracingEnabled) {
        return super.invokeAny(wrapCollection(tasks));
      } else {
        return super.invokeAny(tasks);
      }
    }

    @Override
    public <T> T invokeAny(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit)
        throws InterruptedException, ExecutionException, TimeoutException {
      if (tracingEnabled) {
        return super.invokeAny(wrapCollection(tasks), timeout, unit);
      } else {
        return super.invokeAny(tasks, timeout, unit);
      }
    }

    @Override
    public void shutdown() {
      THREAD_POOLS.remove(name);
      super.shutdown();
    }

    @Override
    public List<Runnable> shutdownNow() {
      THREAD_POOLS.remove(name);
      return super.shutdownNow();
    }

  }

  public static class CachedThreadPoolExecutor extends ThreadPoolExecutor {
    private String name = null;
    private boolean tracingEnabled = false;

    public CachedThreadPoolExecutor(int corePoolSize, int maximumPoolSize, long keepAliveTime,
        TimeUnit unit, BlockingQueue<Runnable> workQueue, RejectedExecutionHandler handler) {
      super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue, handler);
    }

    public CachedThreadPoolExecutor(int corePoolSize, int maximumPoolSize, long keepAliveTime,
        TimeUnit unit, BlockingQueue<Runnable> workQueue, ThreadFactory threadFactory,
        RejectedExecutionHandler handler) {
      super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue, threadFactory, handler);
    }

    public CachedThreadPoolExecutor(int corePoolSize, int maximumPoolSize, long keepAliveTime,
        TimeUnit unit, BlockingQueue<Runnable> workQueue, ThreadFactory threadFactory) {
      super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue, threadFactory);
    }

    public CachedThreadPoolExecutor(int corePoolSize, int maximumPoolSize, long keepAliveTime,
        TimeUnit unit, BlockingQueue<Runnable> workQueue) {
      super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue);
    }

    public void setName(String name) {
      this.name = name;
    }

    public void enableTracing() {
      this.tracingEnabled = true;
    }

    @Override
    public void execute(Runnable command) {
      if (tracingEnabled) {
        super.execute(new TraceRunnable(command));
      } else {
        super.execute(command);
      }
    }

    @Override
    public <T> Future<T> submit(Callable<T> task) {
      if (tracingEnabled) {
        return super.submit(new TraceCallable<T>(task));
      } else {
        return super.submit(task);
      }
    }

    @Override
    public <T> Future<T> submit(Runnable task, T result) {
      if (tracingEnabled) {
        return super.submit(new TraceRunnable(task), result);
      } else {
        return super.submit(task, result);
      }
    }

    @Override
    public Future<?> submit(Runnable task) {
      if (tracingEnabled) {
        return super.submit(new TraceRunnable(task));
      } else {
        return super.submit(task);
      }
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
      if (tracingEnabled) {
        return super.invokeAll(wrapCollection(tasks));
      } else {
        return super.invokeAll(tasks);
      }
    }

    @Override
    public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks, long timeout,
        TimeUnit unit) throws InterruptedException {
      if (tracingEnabled) {
        return super.invokeAll(wrapCollection(tasks), timeout, unit);
      } else {
        return super.invokeAll(tasks, timeout, unit);
      }
    }

    @Override
    public <T> T invokeAny(Collection<? extends Callable<T>> tasks)
        throws InterruptedException, ExecutionException {
      if (tracingEnabled) {
        return super.invokeAny(wrapCollection(tasks));
      } else {
        return super.invokeAny(tasks);
      }
    }

    @Override
    public <T> T invokeAny(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit)
        throws InterruptedException, ExecutionException, TimeoutException {
      if (tracingEnabled) {
        return super.invokeAny(wrapCollection(tasks), timeout, unit);
      } else {
        return super.invokeAny(tasks, timeout, unit);
      }
    }

    @Override
    public void shutdown() {
      THREAD_POOLS.remove(name);
      super.shutdown();
    }

    @Override
    public List<Runnable> shutdownNow() {
      THREAD_POOLS.remove(name);
      return super.shutdownNow();
    }

  }

  public static class AccumuloUncaughtExceptionHandler implements UncaughtExceptionHandler {

    private static final Logger LOG =
        LoggerFactory.getLogger(AccumuloUncaughtExceptionHandler.class);
    private static final String HALT_PROPERTY = "HaltVMOnThreadError";

    @Override
    public void uncaughtException(Thread t, Throwable e) {
      if (e instanceof Exception) {
        LOG.error("Caught an exception in {}. Thread is dead.", t, e);
      } else {
        if (System.getProperty(HALT_PROPERTY, "false").equals("true")) {
          LOG.error("Caught an exception in {}.", t, e);
          Halt.halt(String.format("Caught an exception in %s. Halting VM, check the logs.", t));
        } else {
          LOG.error("Caught an exception in {}. Thread is dead.", t, e);
        }
      }
    }

  }

  private static class NamedThreadFactory implements ThreadFactory {
    private static final Logger LOG = LoggerFactory.getLogger(NamedThreadFactory.class);

    private static final UncaughtExceptionHandler UEH = new AccumuloUncaughtExceptionHandler();

    private AtomicInteger threadNum = new AtomicInteger(1);
    private String name;
    private OptionalInt priority;

    public NamedThreadFactory(String name, OptionalInt priority) {
      this.name = name;
      this.priority = priority;
    }

    @Override
    public Thread newThread(Runnable r) {
      Thread thread =
          new Daemon(new LoggingRunnable(LOG, r), name + " " + threadNum.getAndIncrement());
      thread.setUncaughtExceptionHandler(UEH);
      if (priority.isPresent()) {
        thread.setPriority(priority.getAsInt());
      }
      return thread;
    }

  }

  private static final Map<String,ExecutorService> THREAD_POOLS =
      Collections.synchronizedMap(new HashMap<>());

  // the number of seconds before we allow a thread to terminate with non-use.
  public static final long DEFAULT_TIMEOUT_MILLISECS = 180000L;

  public static ExecutorService getExecutorService(AccumuloConfiguration conf, Property p) {

    switch (p) {
      case GENERAL_SIMPLETIMER_THREADPOOL_SIZE:
        return getScheduledExecutorService(conf.getCount(p), "SimpleTimer", OptionalInt.empty());
      case MASTER_BULK_THREADPOOL_SIZE:
        CachedThreadPoolExecutor exec = getSimpleThreadPool(conf.getCount(p),
            conf.getTimeInMillis(Property.MASTER_BULK_THREADPOOL_TIMEOUT), TimeUnit.MILLISECONDS,
            "bulk import");
        exec.enableTracing();
        return exec;
      case MASTER_RENAME_THREADS:
        return getSimpleThreadPool(conf.getCount(p), "bulk move");
      case MASTER_FATE_THREADPOOL_SIZE:
        return getSimpleThreadPool(conf.getCount(p), "Repo Runner");
      case MASTER_STATUS_THREAD_POOL_SIZE:
        int threads = conf.getCount(p);
        if (threads == 0) {
          return getSimpleThreadPool(0, Integer.MAX_VALUE, 60L, TimeUnit.SECONDS,
              "GatherTableInformation", new SynchronousQueue<Runnable>(), OptionalInt.empty());
        } else {
          return getSimpleThreadPool(threads, "GatherTableInformation");
        }
      case TSERV_WORKQ_THREADS:
        return getSimpleThreadPool(conf.getCount(p), "distributed work queue");
      case TSERV_MINC_MAXCONCURRENT:
        CachedThreadPoolExecutor exec2 =
            getSimpleThreadPool(conf.getCount(p), 0L, TimeUnit.MILLISECONDS, "minor compactor");
        exec2.enableTracing();
        return exec2;
      case TSERV_MIGRATE_MAXCONCURRENT:
        CachedThreadPoolExecutor exec3 =
            getSimpleThreadPool(conf.getCount(p), 0L, TimeUnit.MILLISECONDS, "tablet migration");
        exec3.enableTracing();
        return exec3;
      case TSERV_ASSIGNMENT_MAXCONCURRENT:
        CachedThreadPoolExecutor exec4 =
            getSimpleThreadPool(conf.getCount(p), 0L, TimeUnit.MILLISECONDS, "tablet assignment");
        exec4.enableTracing();
        return exec4;
      case TSERV_SUMMARY_RETRIEVAL_THREADS:
        CachedThreadPoolExecutor exec5 = getSimpleThreadPool(conf.getCount(p), conf.getCount(p), 60,
            TimeUnit.SECONDS, "summary file retriever");
        exec5.enableTracing();
        return exec5;
      case TSERV_SUMMARY_REMOTE_THREADS:
        CachedThreadPoolExecutor exec6 = getSimpleThreadPool(conf.getCount(p), conf.getCount(p), 60,
            TimeUnit.SECONDS, "summary remote");
        exec6.enableTracing();
        return exec6;
      case TSERV_SUMMARY_PARTITION_THREADS:
        CachedThreadPoolExecutor exec7 = getSimpleThreadPool(conf.getCount(p), conf.getCount(p), 60,
            TimeUnit.SECONDS, "summary partition");
        exec7.enableTracing();
        return exec7;
      case GC_DELETE_THREADS:
        return getSimpleThreadPool(conf.getCount(p), "deleting");
      case REPLICATION_WORKER_THREADS:
        return getSimpleThreadPool(conf.getCount(p), "replication task");
      default:
        throw new RuntimeException("Unhandled thread pool property: " + p);
    }
  }

  public static CachedThreadPoolExecutor getSimpleThreadPool(int numThreads, final String name) {
    return getSimpleThreadPool(numThreads, DEFAULT_TIMEOUT_MILLISECS, TimeUnit.MILLISECONDS, name);
  }

  public static CachedThreadPoolExecutor getSimpleThreadPool(int numThreads, long timeOut,
      TimeUnit units, final String name) {
    return getSimpleThreadPool(numThreads, numThreads, timeOut, units, name,
        new LinkedBlockingQueue<Runnable>(), OptionalInt.empty());
  }

  public static CachedThreadPoolExecutor getSimpleThreadPool(int coreThreads, int maxThreads,
      long timeOut, TimeUnit units, final String name) {
    return getSimpleThreadPool(coreThreads, maxThreads, timeOut, units, name,
        new LinkedBlockingQueue<Runnable>(), OptionalInt.empty());
  }

  public static CachedThreadPoolExecutor getSimpleThreadPool(int coreThreads, int maxThreads,
      long timeOut, TimeUnit units, final String name, BlockingQueue<Runnable> queue,
      OptionalInt priority) {
    return (CachedThreadPoolExecutor) THREAD_POOLS.computeIfAbsent(name, k -> {
      CachedThreadPoolExecutor result = new CachedThreadPoolExecutor(coreThreads, maxThreads,
          timeOut, units, queue, new NamedThreadFactory(name, priority));
      result.setName(name);
      if (timeOut > 0) {
        result.allowCoreThreadTimeOut(true);
      }
      return result;
    });
  }

  public static CachedScheduledThreadPoolExecutor
      getGeneralScheduledExecutorService(AccumuloConfiguration conf) {
    return (CachedScheduledThreadPoolExecutor) getExecutorService(conf,
        Property.GENERAL_SIMPLETIMER_THREADPOOL_SIZE);
  }

  public static CachedScheduledThreadPoolExecutor getScheduledExecutorService(int numThreads,
      final String name, OptionalInt priority) {
    return (CachedScheduledThreadPoolExecutor) THREAD_POOLS.computeIfAbsent(name, k -> {
      CachedScheduledThreadPoolExecutor result =
          new CachedScheduledThreadPoolExecutor(numThreads, new NamedThreadFactory(name, priority));
      result.setName(name);
      return result;
    });
  }

}

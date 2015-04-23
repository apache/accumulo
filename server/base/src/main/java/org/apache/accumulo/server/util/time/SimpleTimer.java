/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.accumulo.server.util.time;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * Generic singleton timer. Don't use this if you are going to do anything that will take very long. Please use it to reduce the number of threads dedicated to
 * simple events.
 *
 */
public class SimpleTimer {
  private static final Logger log = LoggerFactory.getLogger(SimpleTimer.class);

  private static class ExceptionHandler implements Thread.UncaughtExceptionHandler {
    @Override
    public void uncaughtException(Thread t, Throwable e) {
      log.warn("SimpleTimer task failed", e);
    }
  }

  private static int instanceThreadPoolSize = -1;
  private static SimpleTimer instance;
  private ScheduledExecutorService executor;

  private static final int DEFAULT_THREAD_POOL_SIZE = 1;

  /**
   * Gets the timer instance. If an instance has already been created, it will have the number of threads supplied when it was constructed, and the size
   * provided here is ignored.
   *
   * @param threadPoolSize
   *          number of threads
   */
  public static synchronized SimpleTimer getInstance(int threadPoolSize) {
    if (instance == null) {
      instance = new SimpleTimer(threadPoolSize);
      SimpleTimer.instanceThreadPoolSize = threadPoolSize;
    } else {
      if (SimpleTimer.instanceThreadPoolSize != threadPoolSize) {
        log.warn("Asked to create SimpleTimer with thread pool size {}, existing instance has {}", threadPoolSize, instanceThreadPoolSize);
      }
    }
    return instance;
  }

  /**
   * Gets the timer instance. If an instance has already been created, it will have the number of threads supplied when it was constructed, and the size
   * provided by the configuration here is ignored. If a null configuration is supplied, the number of threads defaults to 1.
   *
   * @param conf
   *          configuration from which to get the number of threads
   * @see Property#GENERAL_SIMPLETIMER_THREADPOOL_SIZE
   */
  public static synchronized SimpleTimer getInstance(AccumuloConfiguration conf) {
    int threadPoolSize;
    if (conf != null) {
      threadPoolSize = conf.getCount(Property.GENERAL_SIMPLETIMER_THREADPOOL_SIZE);
    } else {
      threadPoolSize = DEFAULT_THREAD_POOL_SIZE;
    }
    return getInstance(threadPoolSize);
  }

  /**
   * Gets the thread pool size for the timer instance. Use for testing only.
   *
   * @return thread pool size for timer instance, or -1 if not yet constructed
   */
  @VisibleForTesting
  static int getInstanceThreadPoolSize() {
    return instanceThreadPoolSize;
  }

  private SimpleTimer(int threadPoolSize) {
    executor = Executors.newScheduledThreadPool(threadPoolSize, new ThreadFactoryBuilder().setNameFormat("SimpleTimer-%d").setDaemon(true)
        .setUncaughtExceptionHandler(new ExceptionHandler()).build());
  }

  /**
   * Schedules a task to run in the future.
   *
   * @param task
   *          task to run
   * @param delay
   *          number of milliseconds to wait before execution
   * @return future for scheduled task
   */
  public ScheduledFuture<?> schedule(Runnable task, long delay) {
    return executor.schedule(task, delay, TimeUnit.MILLISECONDS);
  }

  /**
   * Schedules a task to run in the future with a fixed delay between repeated executions.
   *
   * @param task
   *          task to run
   * @param delay
   *          number of milliseconds to wait before first execution
   * @param period
   *          number of milliseconds to wait between executions
   * @return future for scheduled task
   */
  public ScheduledFuture<?> schedule(Runnable task, long delay, long period) {
    return executor.scheduleWithFixedDelay(task, delay, period, TimeUnit.MILLISECONDS);
  }
}

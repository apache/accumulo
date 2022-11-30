/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.accumulo.core.util.ratelimit;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

import java.lang.ref.WeakReference;
import java.util.HashMap;
import java.util.Map;
import java.util.WeakHashMap;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.util.threads.ThreadPools;
import org.apache.accumulo.core.util.threads.Threads;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Provides the ability to retrieve a {@link RateLimiter} keyed to a specific string, which will
 * dynamically update its rate according to a specified callback function.
 */
public class SharedRateLimiterFactory {
  private static final long REPORT_RATE = 60000;
  private static final long UPDATE_RATE = 1000;
  private static SharedRateLimiterFactory instance = null;
  private static ScheduledFuture<?> updateTaskFuture;
  private final Logger log = LoggerFactory.getLogger(SharedRateLimiterFactory.class);
  private final WeakHashMap<String,WeakReference<SharedRateLimiter>> activeLimiters =
      new WeakHashMap<>();

  private SharedRateLimiterFactory() {}

  /** Get the singleton instance of the SharedRateLimiterFactory. */
  public static synchronized SharedRateLimiterFactory getInstance(AccumuloConfiguration conf) {
    if (instance == null) {
      instance = new SharedRateLimiterFactory();

      ScheduledThreadPoolExecutor svc =
          ThreadPools.getServerThreadPools().createGeneralScheduledExecutorService(conf);
      updateTaskFuture = svc.scheduleWithFixedDelay(Threads
          .createNamedRunnable("SharedRateLimiterFactory update polling", instance::updateAll),
          UPDATE_RATE, UPDATE_RATE, MILLISECONDS);

      ScheduledFuture<?> future = svc.scheduleWithFixedDelay(Threads
          .createNamedRunnable("SharedRateLimiterFactory report polling", instance::reportAll),
          REPORT_RATE, REPORT_RATE, MILLISECONDS);
      ThreadPools.watchNonCriticalScheduledTask(future);

    }
    return instance;
  }

  /**
   * A callback which provides the current rate for a {@link RateLimiter}.
   */
  public interface RateProvider {
    /**
     * Calculate the current rate for the {@link RateLimiter}.
     *
     * @return Count of permits which should be provided per second. A non-positive count is taken
     *         to indicate that no rate limiting should be performed.
     */
    long getDesiredRate();
  }

  /**
   * Lookup the RateLimiter associated with the specified name, or create a new one for that name.
   *
   * @param name key for the rate limiter
   * @param rateProvider a function which can be called to get what the current rate for the rate
   *        limiter should be.
   */
  public RateLimiter create(String name, RateProvider rateProvider) {
    synchronized (activeLimiters) {
      if (updateTaskFuture.isDone()) {
        log.warn("SharedRateLimiterFactory update task has failed.");
      }
      var limiterRef = activeLimiters.get(name);
      var limiter = limiterRef == null ? null : limiterRef.get();
      if (limiter == null) {
        limiter = new SharedRateLimiter(name, rateProvider, rateProvider.getDesiredRate());
        activeLimiters.put(name, new WeakReference<>(limiter));
      }
      return limiter;
    }
  }

  private void copyAndThen(String actionName, Consumer<SharedRateLimiter> action) {
    Map<String,SharedRateLimiter> limitersCopy = new HashMap<>();
    // synchronize only for copy
    synchronized (activeLimiters) {
      activeLimiters.forEach((name, limiterRef) -> {
        var limiter = limiterRef.get();
        if (limiter != null) {
          limitersCopy.put(name, limiter);
        }
      });
    }
    limitersCopy.forEach((name, limiter) -> {
      try {
        action.accept(limiter);
      } catch (RuntimeException e) {
        log.error("Failed to {} limiter {}", actionName, name, e);
      }
    });
  }

  /**
   * Walk through all of the currently active RateLimiters, having each update its current rate.
   * This is called periodically so that we can dynamically update as configuration changes.
   */
  private void updateAll() {
    copyAndThen("update", SharedRateLimiter::update);
  }

  /**
   * Walk through all of the currently active RateLimiters, having each report its activity to the
   * debug log.
   */
  private void reportAll() {
    copyAndThen("report", SharedRateLimiter::report);
  }

  protected class SharedRateLimiter extends GuavaRateLimiter {
    private AtomicLong permitsAcquired = new AtomicLong();
    private AtomicLong lastUpdate = new AtomicLong();

    private final RateProvider rateProvider;
    private final String name;

    SharedRateLimiter(String name, RateProvider rateProvider, long initialRate) {
      super(initialRate);
      this.name = name;
      this.rateProvider = rateProvider;
      this.lastUpdate.set(System.nanoTime());
    }

    @Override
    public void acquire(long permits) {
      super.acquire(permits);
      permitsAcquired.addAndGet(permits);
    }

    /** Poll the callback, updating the current rate if necessary. */
    public void update() {
      // Reset rate if needed
      long rate = rateProvider.getDesiredRate();
      if (rate != getRate()) {
        setRate(rate);
      }
    }

    /** Report the current throughput and usage of this rate limiter to the debug log. */
    public void report() {
      if (log.isDebugEnabled()) {
        long duration = NANOSECONDS.toMillis(System.nanoTime() - lastUpdate.get());
        if (duration == 0) {
          return;
        }
        lastUpdate.set(System.nanoTime());

        long sum = permitsAcquired.get();
        permitsAcquired.set(0);

        if (sum > 0) {
          log.debug(String.format("RateLimiter '%s': %,d of %,d permits/second", name,
              sum * 1000L / duration, getRate()));
        }
      }
    }
  }
}

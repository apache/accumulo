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
package org.apache.accumulo.core.util.ratelimit;

import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.WeakHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableMap;

/**
 * Provides the ability to retrieve a {@link RateLimiter} keyed to a specific string, which will dynamically update its rate according to a specified callback
 * function.
 */
public class SharedRateLimiterFactory {
  private static final long REPORT_RATE = 60000;
  private static final long UPDATE_RATE = 1000;
  private static SharedRateLimiterFactory instance = null;
  private final Logger log = LoggerFactory.getLogger(SharedRateLimiterFactory.class);
  private final WeakHashMap<String,SharedRateLimiter> activeLimiters = new WeakHashMap<>();

  private SharedRateLimiterFactory() {}

  /** Get the singleton instance of the SharedRateLimiterFactory. */
  public static synchronized SharedRateLimiterFactory getInstance() {
    if (instance == null) {
      instance = new SharedRateLimiterFactory();

      Timer timer = new Timer("SharedRateLimiterFactory update/report polling");

      // Update periodically
      timer.schedule(new TimerTask() {
        @Override
        public void run() {
          instance.update();
        }
      }, UPDATE_RATE, UPDATE_RATE);

      // Report periodically
      timer.schedule(new TimerTask() {
        @Override
        public void run() {
          instance.report();
        }
      }, REPORT_RATE, REPORT_RATE);
    }
    return instance;
  }

  /**
   * A callback which provides the current rate for a {@link RateLimiter}.
   */
  public static interface RateProvider {
    /**
     * Calculate the current rate for the {@link RateLimiter}.
     *
     * @return Count of permits which should be provided per second. A nonpositive count is taken to indicate that no rate limiting should be performed.
     */
    public long getDesiredRate();
  }

  /**
   * Lookup the RateLimiter associated with the specified name, or create a new one for that name.
   *
   * @param name
   *          key for the rate limiter
   * @param rateProvider
   *          a function which can be called to get what the current rate for the rate limiter should be.
   */
  public RateLimiter create(String name, RateProvider rateProvider) {
    synchronized (activeLimiters) {
      if (activeLimiters.containsKey(name)) {
        SharedRateLimiter limiter = activeLimiters.get(name);
        return limiter;
      } else {
        long initialRate;
        initialRate = rateProvider.getDesiredRate();
        SharedRateLimiter limiter = new SharedRateLimiter(name, rateProvider, initialRate);
        activeLimiters.put(name, limiter);
        return limiter;
      }
    }
  }

  /**
   * Walk through all of the currently active RateLimiters, having each update its current rate. This is called periodically so that we can dynamically update
   * as configuration changes.
   */
  protected void update() {
    Map<String,SharedRateLimiter> limitersCopy;
    synchronized (activeLimiters) {
      limitersCopy = ImmutableMap.copyOf(activeLimiters);
    }
    for (Map.Entry<String,SharedRateLimiter> entry : limitersCopy.entrySet()) {
      try {
        entry.getValue().update();
      } catch (Exception ex) {
        log.error(String.format("Failed to update limiter %s", entry.getKey()), ex);
      }
    }
  }

  /** Walk through all of the currently active RateLimiters, having each report its activity to the debug log. */
  protected void report() {
    Map<String,SharedRateLimiter> limitersCopy;
    synchronized (activeLimiters) {
      limitersCopy = ImmutableMap.copyOf(activeLimiters);
    }
    for (Map.Entry<String,SharedRateLimiter> entry : limitersCopy.entrySet()) {
      try {
        entry.getValue().report();
      } catch (Exception ex) {
        log.error(String.format("Failed to report limiter %s", entry.getKey()), ex);
      }
    }
  }

  protected class SharedRateLimiter extends GuavaRateLimiter {
    private volatile long permitsAcquired = 0;
    private volatile long lastUpdate;

    private final RateProvider rateProvider;
    private final String name;

    SharedRateLimiter(String name, RateProvider rateProvider, long initialRate) {
      super(initialRate);
      this.name = name;
      this.rateProvider = rateProvider;
      this.lastUpdate = System.currentTimeMillis();
    }

    @Override
    public void acquire(long permits) {
      super.acquire(permits);
      permitsAcquired += permits;
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
        long duration = System.currentTimeMillis() - lastUpdate;
        if (duration == 0)
          return;
        lastUpdate = System.currentTimeMillis();

        long sum = permitsAcquired;
        permitsAcquired = 0;

        if (sum > 0) {
          log.debug(String.format("RateLimiter '%s': %,d of %,d permits/second", name, sum * 1000L / duration, getRate()));
        }
      }
    }
  }
}

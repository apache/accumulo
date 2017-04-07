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

/** Rate limiter from the Guava library. */
public class GuavaRateLimiter implements RateLimiter {
  private final com.google.common.util.concurrent.RateLimiter rateLimiter;
  private long currentRate;

  /**
   * Constructor
   *
   * @param initialRate
   *          Count of permits which should be made available per second. A nonpositive rate is taken to indicate there should be no limitation on rate.
   */
  public GuavaRateLimiter(long initialRate) {
    this.currentRate = initialRate;
    this.rateLimiter = com.google.common.util.concurrent.RateLimiter.create(initialRate > 0 ? initialRate : Long.MAX_VALUE);
  }

  @Override
  public long getRate() {
    return currentRate;
  }

  /**
   * Change the rate at which permits are made available.
   *
   * @param newRate
   *          Count of permits which should be made available per second. A nonpositive rate is taken to indicate that there should be no limitation on rate.
   */
  public void setRate(long newRate) {
    this.rateLimiter.setRate(newRate > 0 ? newRate : Long.MAX_VALUE);
    this.currentRate = newRate;
  }

  @Override
  public void acquire(long permits) {
    if (this.currentRate > 0) {
      while (permits > Integer.MAX_VALUE) {
        rateLimiter.acquire(Integer.MAX_VALUE);
        permits -= Integer.MAX_VALUE;
      }
      if (permits > 0) {
        rateLimiter.acquire((int) permits);
      }
    }
  }
}

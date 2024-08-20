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
package org.apache.accumulo.core.util;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Preconditions;

/**
 * A utility class that tracks the time remaining from an initial duration. It allows the caller to
 * check how much time is left on the timer and if the countdown has expired.
 * <p>
 * Example usage:
 *
 * <pre>
 * CountDownTimer timer = CountDownTimer.startNew(Duration.ofMillis(100));
 * Thread.sleep(10);
 * long timeLeft = timer.timeLeft(TimeUnit.MILLISECONDS); // approximately 90ms remaining
 * boolean expired = timer.isExpired(); // false
 * Thread.sleep(100);
 * expired = timer.isExpired(); // true
 * </pre>
 */
public class CountDownTimer {
  private final long startNanos;
  private final long durationNanos;

  private CountDownTimer(long durationNanos) {
    this.startNanos = System.nanoTime();
    this.durationNanos = durationNanos;
  }

  /**
   * Starts a new countdown timer with the specified duration.
   *
   * @param duration the countdown duration, must be non-negative.
   */
  public static CountDownTimer startNew(Duration duration) {
    Preconditions.checkArgument(!duration.isNegative());
    return new CountDownTimer(duration.toNanos());
  }

  /**
   * Starts a new countdown timer with the specified duration.
   *
   * @param duration the countdown duration, must be non-negative.
   * @param unit the time unit of the duration.
   */
  public static CountDownTimer startNew(long duration, TimeUnit unit) {
    Preconditions.checkArgument(duration >= 0);
    return new CountDownTimer(unit.toNanos(duration));
  }

  /**
   * @param unit the desired {@link TimeUnit} for the returned time.
   * @return the remaining time in the specified unit, or zero if expired.
   */
  public long timeLeft(TimeUnit unit) {
    var elapsed = (System.nanoTime() - startNanos);
    var timeLeft = durationNanos - elapsed;
    if (timeLeft < 0) {
      timeLeft = 0;
    }

    return unit.convert(timeLeft, TimeUnit.NANOSECONDS);
  }

  /**
   * Checks if the countdown timer has expired.
   *
   * @return true if the elapsed time since creation is greater than or equals to the initial
   *         duration, otherwise return false.
   */
  public boolean isExpired() {
    return timeLeft(TimeUnit.NANOSECONDS) == 0;
  }
}

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

/**
 * This class provides a timer for measuring elapsed time.
 */
public final class Timer {

  private long startNanos;

  private Timer() {
    this.startNanos = System.nanoTime();
  }

  /**
   * Creates and starts a new Timer instance.
   *
   * @return a new Timer instance that is already started.
   */
  public static Timer startNew() {
    return new Timer();
  }

  /**
   * Resets the start point for this timer.
   */
  public void restart() {
    this.startNanos = System.nanoTime();
  }

  private long getElapsedNanos() {
    return System.nanoTime() - startNanos;
  }

  /**
   * Checks if the specified duration has elapsed since the timer was started.
   *
   * @param duration the duration to check.
   * @return true if the specified duration has elapsed, false otherwise.
   */
  public boolean hasElapsed(Duration duration) {
    return getElapsedNanos() >= toNanos(duration);
  }

  /**
   * Checks if the specified duration has elapsed since the timer was started.
   *
   * @param duration the duration to check.
   * @param unit the TimeUnit of the duration.
   * @return true if the specified duration has elapsed, false otherwise.
   */
  public boolean hasElapsed(long duration, TimeUnit unit) {
    return getElapsedNanos() >= unit.toNanos(duration);
  }

  /**
   * @return the elapsed time as a Duration.
   */
  public Duration elapsed() {
    return Duration.ofNanos(getElapsedNanos());
  }

  /**
   * @param unit the TimeUnit to return the elapsed time in.
   * @return the elapsed time in the specified TimeUnit.
   */
  public long elapsed(TimeUnit unit) {
    return unit.convert(getElapsedNanos(), TimeUnit.NANOSECONDS);
  }

  /**
   * @return true if this timer was started/reset after the other timer was started/reset, false
   *         otherwise
   */
  public boolean startedAfter(Timer otherTimer) {
    return (startNanos - otherTimer.startNanos) > 0;
  }

  private static long toNanos(Duration duration) {
    try {
      // This can overflow when very large, such as when the
      // duration is created using Long.MAX_VALUE millis
      return duration.toNanos();
    } catch (ArithmeticException e) {
      return Long.MAX_VALUE;
    }
  }
}

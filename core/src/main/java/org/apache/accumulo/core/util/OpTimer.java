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

import static java.util.concurrent.TimeUnit.NANOSECONDS;

import java.util.concurrent.TimeUnit;

/**
 * Provides a stop watch for timing a single type of event. This code is based on the
 * org.apache.hadoop.util.StopWatch available in hadoop 2.7.0
 */
public class OpTimer {

  private boolean isStarted;
  private long startNanos;
  private long currentElapsedNanos;

  /**
   * Returns timer running state
   *
   * @return true if timer is running
   */
  public boolean isRunning() {
    return isStarted;
  }

  /**
   * Start the timer instance.
   *
   * @return this instance for fluent chaining.
   * @throws IllegalStateException if start is called on running instance.
   */
  public OpTimer start() throws IllegalStateException {
    if (isStarted) {
      throw new IllegalStateException("OpTimer is already running");
    }
    isStarted = true;
    startNanos = System.nanoTime();
    return this;
  }

  /**
   * Stop the timer instance.
   *
   * @return this instance for fluent chaining.
   * @throws IllegalStateException if stop is called on instance that is not running.
   */
  public OpTimer stop() throws IllegalStateException {
    if (!isStarted) {
      throw new IllegalStateException("OpTimer is already stopped");
    }
    long now = System.nanoTime();
    isStarted = false;
    currentElapsedNanos += now - startNanos;
    return this;
  }

  /**
   * Stops timer instance and current elapsed time to 0.
   *
   * @return this instance for fluent chaining
   */
  public OpTimer reset() {
    currentElapsedNanos = 0;
    isStarted = false;
    return this;
  }

  /**
   * Converts current timer value to specific unit. The conversion to courser granularities truncate
   * with loss of precision.
   *
   * @param timeUnit the time unit that will converted to.
   * @return truncated time in unit of specified time unit.
   */
  public long now(TimeUnit timeUnit) {
    return timeUnit.convert(now(), NANOSECONDS);
  }

  /**
   * Returns the current elapsed time scaled to the provided time unit. This method does not
   * truncate like {@link #now(TimeUnit)} but returns the value as a double.
   *
   * <p>
   * Note: this method is not included in the hadoop 2.7 org.apache.hadoop.util.StopWatch class. If
   * that class is adopted, then provisions will be required to replace this method.
   *
   * @param timeUnit the time unit to scale the elapsed time to.
   * @return the elapsed time of this instance scaled to the provided time unit.
   */
  public double scale(TimeUnit timeUnit) {
    return (double) now() / NANOSECONDS.convert(1L, timeUnit);
  }

  /**
   * Returns current timer elapsed time as nanoseconds.
   *
   * @return elapsed time in nanoseconds.
   */
  public long now() {
    return isStarted ? System.nanoTime() - startNanos + currentElapsedNanos : currentElapsedNanos;
  }

  /**
   * Return the current elapsed time in nanoseconds as a string.
   *
   * @return timer elapsed time as nanoseconds.
   */
  @Override
  public String toString() {
    return String.valueOf(now());
  }

}

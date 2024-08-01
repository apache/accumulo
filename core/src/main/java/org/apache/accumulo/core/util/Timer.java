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

/**
 * This class provides a timer for measuring elapsed time.
 */
public final class Timer {

  private long startNanos;
  private long accumulatedNanos;
  private boolean isStarted;

  private Timer() {
    this.accumulatedNanos = 0;
    this.isStarted = false;
  }

  /**
   * Creates and starts a new Timer instance.
   *
   * @return a new Timer instance that is already started.
   */
  public static Timer startNew() {
    Timer timer = new Timer();
    timer.start();
    return timer;
  }

  /**
   * Starts the timer if it is not already running.
   *
   * @throws IllegalStateException if the timer is already running.
   */
  public void start() throws IllegalStateException {
    if (isStarted) {
      throw new IllegalStateException("Timer is already running");
    }
    startNanos = System.nanoTime();
    isStarted = true;
  }

  /**
   * Stops the timer if it is running and accumulates the elapsed time.
   *
   * @throws IllegalStateException if the timer is not running.
   */
  public void stop() throws IllegalStateException {
    if (!isStarted) {
      throw new IllegalStateException("Timer is not running");
    }
    accumulatedNanos += System.nanoTime() - startNanos;
    isStarted = false;
  }

  /**
   * Resets the timer, stopping it if necessary and clearing accumulated time.
   */
  public void reset() {
    accumulatedNanos = 0;
    isStarted = false;
  }

  private long getElapsedNanos() {
    return isStarted ? System.nanoTime() - startNanos + accumulatedNanos : accumulatedNanos;
  }

  /**
   * Checks if the specified duration has elapsed since the timer was started.
   *
   * @param duration the duration to check.
   * @return true if the specified duration has elapsed, false otherwise.
   */
  public boolean hasElapsed(Duration duration) {
    return getElapsedNanos() >= duration.toNanos();
  }

  /**
   * Calculates the elapsed time as a Duration.
   *
   * @return the elapsed time as a Duration.
   */
  public Duration elapsed() {
    return Duration.ofNanos(getElapsedNanos());
  }

  /**
   * @return the elapsed time as a string in nanoseconds.
   */
  @Override
  public String toString() {
    return String.valueOf(elapsed().toNanos());
  }
}

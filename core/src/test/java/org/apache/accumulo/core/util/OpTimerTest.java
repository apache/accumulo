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

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Exercise basic timer (org.apache.hadoop.util.StopWatch) functionality. Current usage requires
 * ability to reset timer.
 */
public class OpTimerTest {

  private static final Logger log = LoggerFactory.getLogger(OpTimerTest.class);

  /**
   * Validate reset functionality
   */
  @Test
  public void verifyReset() throws InterruptedException {

    OpTimer timer = new OpTimer().start();
    Thread.sleep(50);

    timer.stop();

    long tValue = timer.now();

    log.debug("Time value before reset {}", String.format("%.3f ms", timer.scale(MILLISECONDS)));

    timer.reset().start();
    Thread.sleep(1);

    timer.stop();

    assertTrue(timer.now() > 0);

    assertTrue(tValue > timer.now());

    timer.reset();

    log.debug("Time value after reset {}", String.format("%.3f ms", timer.scale(MILLISECONDS)));

    assertEquals(0, timer.now());

  }

  /**
   * Verify that IllegalStateException is thrown when calling stop when timer has not been started.
   */
  @Test
  public void verifyExceptionCallingStopWhenNotStarted() {
    OpTimer timer = new OpTimer();

    assertFalse(timer.isRunning());

    // should throw exception - not running
    assertThrows(IllegalStateException.class, timer::stop,
        "Should not be able to call stop on a timer that is not running");
  }

  /**
   * Verify that IllegalStateException is thrown when calling start on running timer.
   */
  @Test
  public void verifyExceptionCallingStartWhenRunning() throws InterruptedException {
    OpTimer timer = new OpTimer().start();

    Thread.sleep(50);

    assertTrue(timer.isRunning());

    // should throw exception - already running
    assertThrows(IllegalStateException.class, timer::start,
        "Should not be able to call start on a timer that is already running");
  }

  /**
   * Verify that IllegalStateException is thrown when calling stop when not running.
   */
  @Test
  public void verifyExceptionCallingStopWhenNotRunning() throws InterruptedException {
    OpTimer timer = new OpTimer().start();

    Thread.sleep(50);

    assertTrue(timer.isRunning());

    timer.stop();

    assertFalse(timer.isRunning());

    assertThrows(IllegalStateException.class, timer::stop,
        "Should not be able to call stop on a timer that is not running");
  }

  /**
   * Validate that start / stop accumulates time.
   */
  @Test
  public void verifyElapsed() throws InterruptedException {
    OpTimer timer = new OpTimer().start();

    Thread.sleep(50);

    timer.stop();

    long tValue = timer.now();

    log.debug("Time value after first stop {}",
        String.format("%.3f ms", timer.scale(MILLISECONDS)));

    timer.start();

    Thread.sleep(10);

    timer.stop();

    log.debug("Time value after second stop {}",
        String.format("%.3f ms", timer.scale(MILLISECONDS)));

    assertTrue(tValue < timer.now(), "The timer did not increase in value over time");
  }

  /**
   * Validate that scale returns correct values.
   */
  @Test
  public void scale() throws InterruptedException {
    OpTimer timer = new OpTimer().start();

    Thread.sleep(50);

    timer.stop();

    long tValue = timer.now();

    double nanosPerMillisecond = 1_000_000.0;
    assertEquals(tValue / nanosPerMillisecond, timer.scale(MILLISECONDS), 0.00000001);

    double nanosPerSecond = 1_000_000_000.0;
    assertEquals(tValue / nanosPerSecond, timer.scale(SECONDS), 0.00000001);
  }
}

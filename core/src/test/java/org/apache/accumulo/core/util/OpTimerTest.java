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
package org.apache.accumulo.core.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.concurrent.TimeUnit;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Exercise basic timer (org.apache.hadoop.util.StopWatch) functionality. Current usage requires ability to reset timer.
 */
public class OpTimerTest {

  private static Logger log = LoggerFactory.getLogger(OpTimerTest.class);

  /**
   * Validate reset functionality
   */
  @Test
  public void verifyReset() {

    OpTimer timer = new OpTimer().start();

    try {
      Thread.sleep(50);
    } catch (InterruptedException ex) {
      log.info("sleep sleep interrupted");
      Thread.currentThread().interrupt();
    }

    timer.stop();

    long tValue = timer.now();

    log.debug("Time value before reset {}", String.format("%.3f ms", timer.scale(TimeUnit.MILLISECONDS)));

    timer.reset().start();

    try {
      Thread.sleep(1);
    } catch (InterruptedException ex) {
      log.info("sleep sleep interrupted");
      Thread.currentThread().interrupt();
    }

    timer.stop();

    assertTrue(timer.now() > 0);

    assertTrue(tValue > timer.now());

    timer.reset();

    log.debug("Time value after reset {}", String.format("%.3f ms", timer.scale(TimeUnit.MILLISECONDS)));

    assertEquals(0, timer.now());

  }

  /**
   * Verify that IllegalStateException is thrown when calling stop when timer has not been started.
   */
  @Test(expected = IllegalStateException.class)
  public void verifyExceptionCallingStopWhenNotStarted() {

    OpTimer timer = new OpTimer();

    assertFalse(timer.isRunning());

    // should throw exception - not running
    timer.stop();
  }

  /**
   * Verify that IllegalStateException is thrown when calling start on running timer.
   */
  @Test(expected = IllegalStateException.class)
  public void verifyExceptionCallingStartWhenRunning() {

    OpTimer timer = new OpTimer().start();

    try {
      Thread.sleep(50);
    } catch (InterruptedException ex) {
      log.info("sleep sleep interrupted");
      Thread.currentThread().interrupt();
    }

    assertTrue(timer.isRunning());

    // should throw exception - already running
    timer.start();
  }

  /**
   * Verify that IllegalStateException is thrown when calling stop when not running.
   */
  @Test(expected = IllegalStateException.class)
  public void verifyExceptionCallingStopWhenNotRunning() {

    OpTimer timer = new OpTimer().start();

    try {
      Thread.sleep(50);
    } catch (InterruptedException ex) {
      log.info("sleep sleep interrupted");
      Thread.currentThread().interrupt();
    }

    assertTrue(timer.isRunning());

    timer.stop();

    assertFalse(timer.isRunning());

    // should throw exception
    timer.stop();
  }

  /**
   * Validate that start / stop accumulates time.
   */
  @Test
  public void verifyElapsed() {

    OpTimer timer = new OpTimer().start();

    try {
      Thread.sleep(50);
    } catch (InterruptedException ex) {
      log.info("sleep sleep interrupted");
      Thread.currentThread().interrupt();
    }

    timer.stop();

    long tValue = timer.now();

    log.debug("Time value after first stop {}", String.format("%.3f ms", timer.scale(TimeUnit.MILLISECONDS)));

    timer.start();

    try {
      Thread.sleep(10);
    } catch (InterruptedException ex) {
      log.info("sleep sleep interrupted");
      Thread.currentThread().interrupt();
    }

    timer.stop();

    log.debug("Time value after second stop {}", String.format("%.3f ms", timer.scale(TimeUnit.MILLISECONDS)));

    assertTrue(tValue < timer.now());

  }

  /**
   * Validate that scale returns correct values.
   */
  @Test
  public void scale() {
    OpTimer timer = new OpTimer().start();

    try {
      Thread.sleep(50);
    } catch (InterruptedException ex) {
      log.info("sleep sleep interrupted");
      Thread.currentThread().interrupt();
    }

    timer.stop();

    long tValue = timer.now();

    assertEquals(tValue / 1000000.0, timer.scale(TimeUnit.MILLISECONDS), 0.00000001);

    assertEquals(tValue / 1000000000.0, timer.scale(TimeUnit.SECONDS), 0.00000001);

  }
}

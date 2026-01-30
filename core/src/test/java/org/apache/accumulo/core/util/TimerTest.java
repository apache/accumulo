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
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;

public class TimerTest {

  @Test
  public void testRestart() throws InterruptedException {
    Timer timer = Timer.startNew();

    // Perform a longer sleep initially
    Thread.sleep(100);

    Duration firstElapsed = timer.elapsed();

    assertTrue(timer.hasElapsed(Duration.ofMillis(100)),
        "Should see at least the sleep time has elapsed.");

    timer.restart();

    // Perform a shorter sleep
    Thread.sleep(50);

    Duration secondElapsed = timer.elapsed();

    // Assert that the elapsed time after restart is greater than 0
    assertFalse(secondElapsed.isNegative(),
        "Elapsed time should be greater than 0 after restarting the timer.");
    assertTrue(secondElapsed.compareTo(firstElapsed) < 0,
        "Elapsed time after restart should be less than the initial elapsed time.");

  }

  @Test
  public void testHasElapsed() throws InterruptedException {
    Timer timer = Timer.startNew();

    Thread.sleep(50);

    assertTrue(timer.hasElapsed(Duration.ofMillis(50)),
        "The timer should indicate that 50 milliseconds have elapsed.");
  }

  @Test
  public void testHasElapsedWithTimeUnit() throws InterruptedException {
    Timer timer = Timer.startNew();

    Thread.sleep(50);

    assertTrue(timer.hasElapsed(50, MILLISECONDS),
        "The timer should indicate that 50 milliseconds have elapsed.");
  }

  @Test
  public void testElapsedWithTimeUnit() throws InterruptedException {
    Timer timer = Timer.startNew();

    final int sleepMillis = 50;
    Thread.sleep(sleepMillis);

    long elapsedMillis = timer.elapsed(MILLISECONDS);

    assertTrue(elapsedMillis >= sleepMillis, "Elapsed time in milliseconds is not correct.");

    if (elapsedMillis < 1000) {
      long elapsedSeconds = timer.elapsed(TimeUnit.SECONDS);
      assertEquals(0, elapsedSeconds,
          "Elapsed time in seconds should be 0 for 50 milliseconds of sleep.");
    }
  }

  @Test
  public void testStartedAfter() throws Exception {
    var timer1 = Timer.startNew();
    Thread.sleep(3);
    var timer2 = Timer.startNew();

    assertTrue(timer2.startedAfter(timer1));
    assertFalse(timer1.startedAfter(timer2));

    Thread.sleep(3);
    timer1.restart();

    assertTrue(timer1.startedAfter(timer2));
    assertFalse(timer2.startedAfter(timer1));

    Thread.sleep(3);
    timer2.restart();

    assertTrue(timer2.startedAfter(timer1));
    assertFalse(timer1.startedAfter(timer2));
  }
}

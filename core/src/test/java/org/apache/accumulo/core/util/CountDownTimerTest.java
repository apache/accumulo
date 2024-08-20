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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;

public class CountDownTimerTest {
  @Test
  public void testCountDownTimer() throws Exception {

    long start = System.nanoTime();

    var timer1 = CountDownTimer.startNew(Duration.ofMillis(100));
    Thread.sleep(10);
    var timer2 = CountDownTimer.startNew(100, TimeUnit.MILLISECONDS);
    Thread.sleep(10);
    var timer3 = CountDownTimer.startNew(Duration.ofMillis(100));
    Thread.sleep(10);

    boolean expired1 = timer1.isExpired();
    boolean expired2 = timer1.isExpired();
    boolean expired3 = timer1.isExpired();

    var left3 = timer3.timeLeft(TimeUnit.MILLISECONDS);
    var left2 = timer2.timeLeft(TimeUnit.MILLISECONDS);
    var left1 = timer1.timeLeft(TimeUnit.MILLISECONDS);

    long elapsed = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);

    assertTrue(left3 <= 90);
    assertTrue(left2 <= 80);
    assertTrue(left1 <= 70);

    assertTrue(Math.max(left3 - 10, 0) >= left2);
    assertTrue(Math.max(left2 - 10, 0) >= left1);
    assertTrue(left1 >= 100 - elapsed);
    assertTrue(left1 >= 0);

    if (left1 > 0) {
      assertFalse(expired1);
    } else {
      assertTrue(expired1);
    }

    if (left2 > 0) {
      assertFalse(expired2);
    } else {
      assertTrue(expired2);
    }

    if (left3 > 0) {
      assertFalse(expired3);
    } else {
      assertTrue(expired3);
    }

    Thread.sleep(92);
    assertEquals(0, timer1.timeLeft(TimeUnit.MILLISECONDS));
    assertEquals(0, timer2.timeLeft(TimeUnit.MILLISECONDS));
    assertEquals(0, timer3.timeLeft(TimeUnit.MILLISECONDS));

    assertTrue(timer1.isExpired());
    assertTrue(timer2.isExpired());
    assertTrue(timer3.isExpired());
  }

  @Test
  public void testNegative() {
    assertThrows(IllegalArgumentException.class,
        () -> CountDownTimer.startNew(Duration.ofMillis(-1)));
  }
}

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
 * Tracks the amount of time left from an initial duration.
 */
public class CountDownTimer {
  private final long startNanos;
  private final long durationNanos;

  private CountDownTimer(long durationNanos) {
    this.startNanos = System.nanoTime();
    this.durationNanos = durationNanos;
  }

  /**
   * Starts a timer that will track the time left from the initial duration. For example starting a
   * CountDownTimer with a duration of 100ms will return 90ms left after 10ms. After 110ms it should
   * return 0 and always return 0 from that point.
   */
  public static CountDownTimer startNew(Duration duration) {
    return new CountDownTimer(duration.toNanos());
  }

  /**
   * @return the amount of time left in the countdown or zero if the time is up.
   */
  public long timeLeft(TimeUnit unit) {
    var elapsed = (System.nanoTime() - startNanos);
    var timeLeft = durationNanos - elapsed;
    if (timeLeft < 0) {
      timeLeft = 0;
    }

    return unit.convert(timeLeft, TimeUnit.NANOSECONDS);
  }
}

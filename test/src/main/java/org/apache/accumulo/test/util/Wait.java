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
package org.apache.accumulo.test.util;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

import java.util.function.ToIntFunction;

public class Wait {

  public static final long MAX_WAIT_MILLIS = SECONDS.toMillis(30);
  public static final long SLEEP_MILLIS = 1000;

  /**
   * Get the user-specified timeout.factor value from the system properties. The parsed value must
   * be a valid integer greater-than-or-equal-to 1. On a parse error, including parsing values less
   * than 1, the caller can handle the error and substitute in a different value, which could be any
   * integer (even less than 1).
   *
   * @param onError allows parse exceptions to be detected and the value replaced with a substitute
   * @return the parsed value or the value from the onError function, if an error occurred
   */
  public static int getTimeoutFactor(ToIntFunction<NumberFormatException> onError) {
    String timeoutString = System.getProperty("timeout.factor", "1");
    try {
      int factor = Integer.parseInt(timeoutString);
      if (factor < 1) {
        throw new NumberFormatException("timeout.factor must be at least 1");
      }
      return factor;
    } catch (NumberFormatException e) {
      return onError.applyAsInt(e);
    }

  }

  public interface Condition {
    boolean isSatisfied() throws Exception;
  }

  /**
   * Wait for the provided condition - will throw an IllegalStateException is the wait exceeds the
   * default wait period of 30 seconds and a retry period of 1 second.
   *
   * @param condition when condition evaluates true, return from wait
   */
  public static void waitFor(Condition condition) {
    waitFor(condition, MAX_WAIT_MILLIS);
  }

  /**
   * Wait for the provided condition - will throw an IllegalStateException is the wait exceeds the
   * wait duration with a default retry period of 1 second.
   *
   * @param condition when condition evaluates true, return from wait
   * @param duration maximum total time to wait (milliseconds)
   */
  public static void waitFor(final Condition condition, final long duration) {
    waitFor(condition, duration, SLEEP_MILLIS);
  }

  /**
   * Wait for the provided condition - will throw an IllegalStateException is the wait exceeds the
   * wait period.
   *
   * @param condition when condition evaluates true, return from wait
   * @param duration maximum total time to wait (milliseconds)
   * @param sleepMillis time to sleep between condition checks
   */
  public static void waitFor(final Condition condition, final long duration,
      final long sleepMillis) {
    waitFor(condition, duration, sleepMillis, "");
  }

  /**
   * Wait for the provided condition - will throw an IllegalStateException is the wait exceeds the
   * wait period.
   *
   * @param condition when condition evaluates true, return from wait
   * @param duration maximum total time to wait (milliseconds)
   * @param sleepMillis time to sleep between condition checks
   * @param failMessage optional message to include in IllegalStateException if condition not met
   *        before expiration.
   */
  public static void waitFor(final Condition condition, final long duration, final long sleepMillis,
      final String failMessage) {

    final int timeoutFactor = getTimeoutFactor(e -> 1); // default to factor of 1
    final long scaledDurationNanos = MILLISECONDS.toNanos(duration) * timeoutFactor;
    final long scaledSleepMillis = sleepMillis * timeoutFactor;

    final long startNanos = System.nanoTime();
    boolean success;
    try {
      success = condition.isSatisfied();
      while (!success && System.nanoTime() - startNanos < scaledDurationNanos) {
        MILLISECONDS.sleep(scaledSleepMillis);
        success = condition.isSatisfied();
      }
    } catch (InterruptedException ex) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException("Interrupted during wait");
    } catch (Exception ex) {
      throw new IllegalStateException(failMessage + ". Failed because of exception in condition",
          ex);
    }
    if (!success) {
      throw new IllegalStateException(failMessage + ". Timeout exceeded");
    }
  }
}

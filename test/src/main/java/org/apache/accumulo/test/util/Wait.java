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

import java.util.concurrent.TimeUnit;

public class Wait {

  public static final long MAX_WAIT_MILLIS = TimeUnit.SECONDS.toMillis(30);
  public static final long SLEEP_MILLIS = 1000;

  public interface Condition {
    boolean isSatisfied() throws Exception;
  }

  /**
   * Wait for the provided condition - will throw an IllegalStateException is the wait exceeds the
   * default wait period of 30 seconds and a retry period of 1 second.
   *
   * @param condition when condition evaluates ture, return from wait
   */
  public static void waitFor(Condition condition) {
    waitFor(condition, MAX_WAIT_MILLIS);
  }

  /**
   * Wait for the provided condition - will throw an IllegalStateException is the wait exceeds the
   * wait duration with a default retry period of 1 second.
   *
   * @param condition when condition evaluates ture, return from wait
   * @param duration maximum total time to wait (milliseconds)
   */
  public static void waitFor(final Condition condition, final long duration) {
    waitFor(condition, duration, SLEEP_MILLIS);
  }

  /**
   * Wait for the provided condition - will throw an IllegalStateException is the wait exceeds the
   * wait period.
   *
   * @param condition when condition evaluates ture, return from wait
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
   * @param condition when condition evaluates ture, return from wait
   * @param duration maximum total time to wait (milliseconds)
   * @param sleepMillis time to sleep between condition checks
   * @param failMessage optional message to include in IllegalStateException if condition not met
   *        before expiration.
   */
  public static void waitFor(final Condition condition, final long duration, final long sleepMillis,
      final String failMessage) {

    final long expiry = System.currentTimeMillis() + duration;
    boolean success;
    try {
      success = condition.isSatisfied();
      while (!success && System.currentTimeMillis() < expiry) {
        TimeUnit.MILLISECONDS.sleep(sleepMillis);
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

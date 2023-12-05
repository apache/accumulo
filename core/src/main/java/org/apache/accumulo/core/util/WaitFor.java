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

import java.util.concurrent.TimeUnit;

import org.checkerframework.checker.nullness.qual.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

/**
 * Provides a generic capability to sleep until a condition is true. The maximum time to wait for
 * the condition, the delay between checking the condition as well as progress and error messages
 * can be provided. All are optional.
 * <p>
 * If the timeout is reached, an interrupt occurs, or if an Exception is thrown checking the
 * condition an IllegalStateException is thrown.
 * <p>
 * Note: Integration tests should use the @{code Wait.waitFor(...) methods in the test module that
 * allow the delays to be scaled using a timeout factor to allow for variations in test
 * environments.}
 */
public class WaitFor {
  private static final Logger LOG = LoggerFactory.getLogger(WaitFor.class);
  private static final long MAX_DURATION_SEC = 30;
  private static final long MAX_SLEEP_SEC = 1;

  /**
   * Convenience method that sleeps for the specified time (in milliseconds) where callers do not
   * need to catch / handle the InterruptedException. If an interrupt occurs, the interrupt is
   * reasserted so the caller has the option to test if an interrupt occurred and can take
   * appropriate action.
   * <p>
   * Using this method should be discouraged in favor of the caller properly catching and taking
   * appropriate action. At a minimum, callers should test for the interrupt status on return and
   * take action if an interrupt occurred.
   *
   * @param millis the sleep time.
   */
  public static void sleep(long millis) {
    try {
      Thread.sleep(millis);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      LOG.error("{}", e.getMessage(), e);
    }
  }

  public interface Condition {
    boolean isSatisfied() throws Exception;
  }

  private long duration = SECONDS.toMillis(MAX_DURATION_SEC);
  private long sleepMillis = SECONDS.toMillis(MAX_SLEEP_SEC);

  private String progressMsg = "";
  private String failMessage = "";

  private Condition condition = null;

  /**
   * Use a {@link #builder(Condition)} to create an instance;
   */
  private WaitFor() {}

  /**
   * Set the condition required to be true before the {@link #waitFor()} will continue, This creates
   * a fluent-style object to allow for setting optional parameters before calling
   * {@link #waitFor()}.
   *
   * @param condition when condition evaluates true, return from waiting
   * @return return a fluent-style object to configure optional parameters.
   */
  public static WaitFor builder(@NonNull final Condition condition) {
    WaitFor waiter = new WaitFor();
    waiter.condition = condition;
    return waiter;
  }

  /**
   * Set the approximate maximum time the waitFor will wait for the condition to be satisfied. If
   * the condition is not satisfied by this time, an IllegalStateException will be thrown. The
   * absolute max time for the wait also depends on the delay time and could be up to (duration +
   * delay). If not set, the default value is {@value #MAX_DURATION_SEC} seconds.
   *
   * @param duration the approximate max wait time for the condition to be true.
   * @param units the time units of the duration.
   * @return return a fluent-style object to configure other optional parameters.
   */
  public WaitFor upTo(final long duration, TimeUnit units) {
    Preconditions.checkArgument(duration > 0,
        "when supplied, duration must be > 0. Received: " + duration);
    this.duration = units.toMillis(duration);
    return this;
  }

  /**
   * Set the delay time between checking the condition to continue. If not set, the default value of
   * {@value #MAX_SLEEP_SEC} second(s) is used.
   *
   * @param sleep the time to sleep between checks.
   * @param units the time units of the duration.
   * @return return a fluent-style object to configure other optional parameters.
   */
  public WaitFor withDelay(final long sleep, TimeUnit units) {
    Preconditions.checkArgument(sleep > 0, "when supplied, sleep must be > 0. Received: " + sleep);
    this.sleepMillis = units.toMillis(sleep);
    return this;
  }

  /**
   * Add optional text that will be included in a debug level message when the condition is false
   * and the wait will enter a sleep. The sleep time is appended to this text. If the progress
   * message is not provided, no logging will occur.
   *
   * @param msg text to be included with the debug level log statement.
   * @return return a fluent-style object to configure other optional parameters.
   */
  public WaitFor withProgressMsg(final String msg) {
    this.progressMsg = msg;
    return this;
  }

  /**
   * Add optional text that will be included in the IllegalStateException if an Exception occurs
   * checking the condition or if the maximum duration time is reached without the condition being
   * satisfied.
   *
   * @param msg text to be included in the IllegalStateException message
   * @return return a fluent-style object to configure other optional parameters.
   */
  public WaitFor withFailMsg(final String msg) {
    this.failMessage = msg;
    return this;
  }

  /**
   * Start the wait and hold until condition is satisfied or the timeout is reached.
   */
  public void waitFor() {
    final long durationNanos = MILLISECONDS.toNanos(duration);

    final long startNanos = System.nanoTime();
    boolean success;
    try {
      success = condition.isSatisfied();
      while (!success && System.nanoTime() - startNanos < durationNanos) {
        if (!progressMsg.isEmpty()) {
          LOG.debug("{}, pausing for {} mills", progressMsg, sleepMillis);
        }
        MILLISECONDS.sleep(sleepMillis);
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

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
import static java.util.concurrent.TimeUnit.NANOSECONDS;

import java.security.SecureRandom;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

/**
 * Encapsulates the retrying implementation for some operation. Provides bounded retry attempts with
 * a bounded, linear backoff.
 */
public class Retry {
  private long maxRetries; // not final for testing
  private long waitIncrement; // not final for testing
  private long maxWait; // not final for testing
  private final long logIntervalNanoSec;
  private double backOffFactor;
  private long retriesDone;
  private long currentWait;
  private long initialWait;

  private boolean hasNeverLogged;
  private boolean hasLoggedWarn = false;
  private long lastRetryLog;
  private static final SecureRandom random = new SecureRandom();
  private double currentBackOffFactor;
  private boolean doTimeJitter = true;

  /**
   * @param maxRetries Maximum times to retry or MAX_RETRY_DISABLED if no maximum
   * @param startWait The amount of time (ms) to wait for the initial retry
   * @param maxWait The maximum wait (ms)
   * @param waitIncrement The amount of time (ms) to increment next wait time by
   * @param logInterval The amount of time (ms) between logging retries
   */
  private Retry(long maxRetries, long startWait, long waitIncrement, long maxWait, long logInterval,
      double backOffFactor) {
    this.maxRetries = maxRetries;
    this.maxWait = maxWait;
    this.waitIncrement = waitIncrement;
    this.retriesDone = 0;
    this.currentWait = startWait;
    this.initialWait = startWait;
    this.logIntervalNanoSec = MILLISECONDS.toNanos(logInterval);
    this.hasNeverLogged = true;
    this.lastRetryLog = -1;
    this.backOffFactor = backOffFactor;
    this.currentBackOffFactor = this.backOffFactor;

  }

  // Visible for testing
  @VisibleForTesting
  public void setBackOffFactor(double baskOffFactor) {
    this.backOffFactor = baskOffFactor;
    this.currentBackOffFactor = this.backOffFactor;
  }

  // Visible for testing
  @VisibleForTesting
  public double getWaitFactor() {
    return backOffFactor;
  }

  // Visible for testing
  @VisibleForTesting
  long getMaxRetries() {
    return maxRetries;
  }

  // Visible for testing
  @VisibleForTesting
  long getCurrentWait() {
    return currentWait;
  }

  // Visible for testing
  @VisibleForTesting
  long getWaitIncrement() {
    return waitIncrement;
  }

  // Visible for testing
  @VisibleForTesting
  long getMaxWait() {
    return maxWait;
  }

  // Visible for testing
  @VisibleForTesting
  void setMaxRetries(long maxRetries) {
    this.maxRetries = maxRetries;
  }

  // Visible for testing
  @VisibleForTesting
  void setStartWait(long startWait) {
    this.currentWait = startWait;
    this.initialWait = startWait;
  }

  // Visible for testing
  @VisibleForTesting
  void setWaitIncrement(long waitIncrement) {
    this.waitIncrement = waitIncrement;
  }

  // Visible for testing
  @VisibleForTesting
  void setMaxWait(long maxWait) {
    this.maxWait = maxWait;
  }

  // Visible for testing
  @VisibleForTesting
  void setDoTimeJitter(boolean jitter) {
    doTimeJitter = jitter;
  }

  public boolean hasInfiniteRetries() {
    return maxRetries < 0;
  }

  public long getLogInterval() {
    return NANOSECONDS.toMillis(logIntervalNanoSec);
  }

  public boolean canRetry() {
    return hasInfiniteRetries() || (retriesDone < maxRetries);
  }

  public void useRetry() {
    if (!canRetry()) {
      throw new IllegalStateException("No retries left");
    }
    retriesDone++;
  }

  public boolean hasRetried() {
    return retriesDone > 0;
  }

  public long retriesCompleted() {
    return retriesDone;
  }

  public void waitForNextAttempt(Logger log, String operationDescription)
      throws InterruptedException {

    double waitFactor = (1 + (random.nextDouble() - 0.5) / 10.0) * currentBackOffFactor;
    if (!doTimeJitter) {
      waitFactor = currentBackOffFactor;
    }
    currentBackOffFactor = currentBackOffFactor * backOffFactor;

    log.debug("Sleeping for {}ms before retrying operation : {} ", currentWait,
        operationDescription);

    sleep(currentWait);

    if (backOffFactor == 1) {
      currentWait = Math.min(maxWait, currentWait + waitIncrement);
    } else if (backOffFactor > 1.0) {
      waitIncrement = (long) Math.ceil(waitFactor * this.initialWait);
      currentWait = Math.min(maxWait, initialWait + waitIncrement);
    }
  }

  protected void sleep(long wait) throws InterruptedException {
    Thread.sleep(wait);
  }

  public void logRetry(Logger log, String message, Throwable t) {
    // log the first time as debug, and then after every logInterval as a warning
    long now = System.nanoTime();
    if (hasNeverLogged) {
      if (log.isDebugEnabled()) {
        log.debug(getMessage(message, t));
      }
      hasNeverLogged = false;
      lastRetryLog = now;
    } else if ((now - lastRetryLog) > logIntervalNanoSec) {
      log.warn(getMessage(message), t);
      lastRetryLog = now;
      hasLoggedWarn = true;
    } else {
      if (log.isTraceEnabled()) {
        log.trace(getMessage(message, t));
      }
    }
  }

  public void logRetry(Logger log, String message) {
    // log the first time as debug, and then after every logInterval as a warning
    long now = System.nanoTime();
    if (hasNeverLogged) {
      if (log.isDebugEnabled()) {
        log.debug(getMessage(message));
      }
      hasNeverLogged = false;
      lastRetryLog = now;
    } else if ((now - lastRetryLog) > logIntervalNanoSec) {
      log.warn(getMessage(message));
      lastRetryLog = now;
      hasLoggedWarn = true;
    } else {
      if (log.isTraceEnabled()) {
        log.trace(getMessage(message));
      }
    }
  }

  private String getMessage(String message) {
    return message + ", retrying attempt " + (retriesDone + 1) + " (suppressing retry messages for "
        + getLogInterval() + "ms)";
  }

  private String getMessage(String message, Throwable t) {
    return message + ":" + t + ", retrying attempt " + (retriesDone + 1)
        + " (suppressing retry messages for " + getLogInterval() + "ms)";
  }

  public void logCompletion(Logger log, String operationDescription) {
    if (!hasNeverLogged) {
      var message = operationDescription + " completed after " + (retriesDone + 1)
          + " retries and is no longer retrying.";
      if (hasLoggedWarn) {
        log.info(message);
      } else {
        log.debug(message);
      }
    }
  }

  public interface NeedsRetries {
    /**
     * @return this builder with the maximum number of retries set to unlimited
     */
    NeedsRetryDelay infiniteRetries();

    /**
     * @param max the maximum number of retries to set
     * @return this builder with the maximum number of retries set to the provided value
     */
    NeedsRetryDelay maxRetries(long max);
  }

  public interface NeedsRetryDelay {
    /**
     * @param duration the amount of time to wait before the first retry; input is converted to
     *        milliseconds, rounded down to the nearest
     * @return this builder with the initial wait period set
     */
    NeedsTimeIncrement retryAfter(long duration, TimeUnit unit);
  }

  public interface NeedsTimeIncrement {
    /**
     * @param duration the amount of additional time to add before each subsequent retry; input is
     *        converted to milliseconds, rounded down to the nearest
     * @return this builder with the increment amount set
     */
    NeedsMaxWait incrementBy(long duration, TimeUnit unit);
  }

  public interface NeedsMaxWait {
    /**
     * @param duration the maximum amount of time to which the waiting period between retries can be
     *        incremented; input is converted to milliseconds, rounded down to the nearest
     * @return this builder with a maximum time limit set
     */
    NeedsBackOffFactor maxWait(long duration, TimeUnit unit);
  }

  public interface NeedsBackOffFactor {
    /**
     * @param backOffFactor the number that the wait increment will be successively multiplied by to
     *        make the time between retries to be exponentially increasing. The default value will
     *        be one.
     */
    NeedsLogInterval backOffFactor(double backOffFactor);
  }

  public interface NeedsLogInterval {
    /**
     * @param duration the minimum time interval between logging that a retry is occurring; input is
     *        converted to milliseconds, rounded down to the nearest
     * @return this builder with a logging interval set
     */
    BuilderDone logInterval(long duration, TimeUnit unit);
  }

  public interface BuilderDone {
    /**
     * Create a RetryFactory from this builder which can be used to create many Retry objects with
     * the same settings.
     *
     * @return this builder as a factory; intermediate references to this builder cannot be used to
     *         change options after this has been called
     */
    RetryFactory createFactory();

    /**
     * Create a single Retry object with the currently configured builder settings.
     *
     * @return a retry object from this builder's settings
     */
    Retry createRetry();
  }

  public interface RetryFactory {
    /**
     * Create a single Retry object from this factory's settings.
     *
     * @return a retry object from this factory's settings
     */
    Retry createRetry();
  }

  public static NeedsRetries builder() {
    return new RetryFactoryBuilder();
  }

  private static class RetryFactoryBuilder
      implements NeedsRetries, NeedsRetryDelay, NeedsTimeIncrement, NeedsMaxWait, NeedsLogInterval,
      NeedsBackOffFactor, BuilderDone, RetryFactory {

    private boolean modifiable = true;
    private long maxRetries;
    private long initialWait;
    private long maxWait;
    private long waitIncrement;
    private long logInterval;
    private double backOffFactor = 1.5;

    RetryFactoryBuilder() {}

    private void checkState() {
      Preconditions.checkState(modifiable,
          "Cannot modify this builder once 'createFactory()' has been called");
    }

    @Override
    public NeedsRetryDelay infiniteRetries() {
      checkState();
      this.maxRetries = -1;
      return this;
    }

    @Override
    public NeedsRetryDelay maxRetries(long max) {
      checkState();
      Preconditions.checkArgument(max >= 0, "Maximum number of retries must not be negative");
      this.maxRetries = max;
      return this;
    }

    @Override
    public NeedsTimeIncrement retryAfter(long duration, TimeUnit unit) {
      checkState();
      Preconditions.checkArgument(duration >= 0, "Initial waiting period must not be negative");
      this.initialWait = unit.toMillis(duration);
      return this;
    }

    @Override
    public NeedsMaxWait incrementBy(long duration, TimeUnit unit) {
      checkState();
      Preconditions.checkArgument(duration >= 0,
          "Amount of time to increment the wait between each retry must not be negative");
      this.waitIncrement = unit.toMillis(duration);
      return this;
    }

    @Override
    public NeedsLogInterval backOffFactor(double factor) {
      checkState();
      Preconditions.checkArgument(factor >= 1,
          "backOffFactor exponent that increases the wait between each retry and must greater than one");
      this.backOffFactor = factor;
      return this;
    }

    @Override
    public NeedsBackOffFactor maxWait(long duration, TimeUnit unit) {
      checkState();
      this.maxWait = unit.toMillis(duration);
      Preconditions.checkArgument(maxWait >= initialWait,
          "Maximum wait between retries must not be less than the initial delay");
      return this;
    }

    @Override
    public BuilderDone logInterval(long duration, TimeUnit unit) {
      checkState();
      Preconditions.checkArgument(duration >= 0,
          "The amount of time between logging retries must not be negative");
      this.logInterval = unit.toMillis(duration);
      return this;
    }

    @Override
    public RetryFactory createFactory() {
      this.modifiable = false;
      return this;
    }

    @Override
    public Retry createRetry() {
      return new Retry(maxRetries, initialWait, waitIncrement, maxWait, logInterval, backOffFactor);
    }

  }
}

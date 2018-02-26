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
package org.apache.accumulo.fate.zookeeper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;

/**
 * Encapsulates the retrying implementation for some operation. Provides bounded retry attempts with a bounded, linear backoff.
 */
public class Retry {
  private static final Logger log = LoggerFactory.getLogger(Retry.class);

  public static final long MAX_RETRY_DISABLED = -1;

  private long maxRetries, maxWait, waitIncrement;
  private long retriesDone, currentWait;

  private static long NANO_SEC_PER_MILLI_SEC = 1000000L;
  private long logIntervalNanoSec;
  private long lastRetryLog;

  /**
   * @param maxRetries
   *          Maximum times to retry or MAX_RETRY_DISABLED if no maximum
   * @param startWait
   *          The amount of time (ms) to wait for the initial retry
   * @param maxWait
   *          The maximum wait (ms)
   * @param waitIncrement
   *          The amount of time (ms) to increment next wait time by
   * @param logInterval
   *          The amount of time (ms) between logging retries
   */

  public Retry(long maxRetries, long startWait, long waitIncrement, long maxWait, long logInterval) {
    this.maxRetries = maxRetries;
    this.maxWait = maxWait;
    this.waitIncrement = waitIncrement;
    this.retriesDone = 0l;
    this.currentWait = startWait;
    this.logIntervalNanoSec = (logInterval * NANO_SEC_PER_MILLI_SEC);
    this.lastRetryLog = -1;
  }

  /**
   * @param startWait
   *          The amount of time (ms) to wait for the initial retry
   * @param maxWait
   *          The maximum wait (ms)
   * @param waitIncrement
   *          The amount of time (ms) to increment next wait time by
   * @param logInterval
   *          The amount of time (ms) between logging retries
   */
  public Retry(long startWait, long waitIncrement, long maxWait, long logInterval) {
    this(MAX_RETRY_DISABLED, startWait, waitIncrement, maxWait, logInterval);
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

  public boolean isMaxRetryDisabled() {
    return maxRetries < 0;
  }

  // Visible for testing
  void setLogInterval(long logInterval) {
    this.logIntervalNanoSec = logInterval * NANO_SEC_PER_MILLI_SEC;
  }

  public long getLogInterval() {
    return logIntervalNanoSec / NANO_SEC_PER_MILLI_SEC;
  }

  public boolean canRetry() {
    return isMaxRetryDisabled() || (retriesDone < maxRetries);
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

  public void waitForNextAttempt() throws InterruptedException {
    if (log.isDebugEnabled()) {
      log.debug("Sleeping for " + currentWait + "ms before retrying operation");
    }
    sleep(currentWait);
    currentWait = Math.min(maxWait, currentWait + waitIncrement);
  }

  protected void sleep(long wait) throws InterruptedException {
    Thread.sleep(wait);
  }

  public void logRetry(Logger log, String message, Throwable t) {
    // log the first time as debug, and then after every logInterval as a warning
    long now = System.nanoTime();
    if (lastRetryLog < 0) {
      if (log.isDebugEnabled()) {
        log.debug(getMessage(message, t));
      }
      lastRetryLog = now;
    } else if ((now - lastRetryLog) > logIntervalNanoSec) {
      log.warn(getMessage(message), t);
      lastRetryLog = now;
    } else {
      if (log.isTraceEnabled()) {
        log.trace(getMessage(message, t));
      }
    }
  }

  public void logRetry(Logger log, String message) {
    // log the first time as debug, and then after every logInterval as a warning
    long now = System.nanoTime();
    if (lastRetryLog < 0) {
      if (log.isDebugEnabled()) {
        log.debug(getMessage(message));
      }
      lastRetryLog = now;
    } else if ((now - lastRetryLog) > logIntervalNanoSec) {
      log.warn(getMessage(message));
      lastRetryLog = now;
    } else {
      if (log.isTraceEnabled()) {
        log.trace(getMessage(message));
      }
    }
  }

  private String getMessage(String message) {
    return message + ", retrying attempt " + (retriesDone + 1) + " (suppressing retry messages for " + getLogInterval() + "ms)";
  }

  private String getMessage(String message, Throwable t) {
    return message + ":" + t + ", retrying attempt " + (retriesDone + 1) + " (suppressing retry messages for " + getLogInterval() + "ms)";
  }

}

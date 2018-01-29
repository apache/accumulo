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

/**
 * Encapsulates the retrying implementation for some operation. Provides bounded retry attempts with a bounded, linear backoff.
 */
public class Retry {
  private static Logger log = LoggerFactory.getLogger(Retry.class);

  public static final long MAX_RETRY_DISABLED = -1;

  private long maxRetries, maxWait, waitIncrement;
  private long retriesDone, currentWait;

  private long logInterval;
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
    this.logInterval = logInterval;
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
  long getMaxRetries() {
    return maxRetries;
  }

  // Visible for testing
  long getCurrentWait() {
    return currentWait;
  }

  // Visible for testing
  long getWaitIncrement() {
    return waitIncrement;
  }

  // Visible for testing
  long getMaxWait() {
    return maxWait;
  }

  // Visible for testing
  void setMaxRetries(long maxRetries) {
    this.maxRetries = maxRetries;
  }

  // Visible for testing
  void setStartWait(long startWait) {
    this.currentWait = startWait;
  }

  // Visible for testing
  void setWaitIncrement(long waitIncrement) {
    this.waitIncrement = waitIncrement;
  }

  // Visible for testing
  void setMaxWait(long maxWait) {
    this.maxWait = maxWait;
  }

  // Visible for testing
  static void setLogger(Logger log) {
    Retry.log = log;
  }

  public boolean isMaxRetryDisabled() {
    return maxRetries < 0;
  }

  // Visible for testing
  void setLogInterval(long logInterval) {
    this.logInterval = logInterval;
  }

  public long getLogInterval() {
    return logInterval;
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
      log.debug("Sleeping for " + currentWait + "ms before retrying operation: " + getCaller());
    }
    sleep(currentWait);
    currentWait = Math.min(maxWait, currentWait + waitIncrement);
  }

  protected void sleep(long wait) throws InterruptedException {
    Thread.sleep(wait);
  }

  public void logRetry(String message, Throwable t) {
    // log the first time, and then after every logInterval
    if (lastRetryLog < 0 || (System.currentTimeMillis() - lastRetryLog) > logInterval) {
      log.warn(getMessage(message), t);
      lastRetryLog = System.currentTimeMillis();
    }
  }

  public void logRetry(String message) {
    // log the first time, and then after every logInterval
    if (lastRetryLog < 0 || (System.currentTimeMillis() - lastRetryLog) > logInterval) {
      log.warn(getMessage(message));
      lastRetryLog = System.currentTimeMillis();
    }
  }

  private String getMessage(String message) {
    return getCaller() + ": " + message + ", retrying attempt " + (retriesDone + 1) + " (suppressing retry messages for " + logInterval + "ms)";
  }

  private String getCaller() {
    StackTraceElement[] stackTraceElements = Thread.currentThread().getStackTrace();
    // find the first caller that is not the Retry class
    // we can start at 3 because the first one is this method, the second one is getMessage, and the third is logRetry
    StackTraceElement caller = null;
    for (int i = 3; caller == null && i < stackTraceElements.length; i++) {
      if (!stackTraceElements[i].getClassName().equals(this.getClass().getName())) {
        caller = stackTraceElements[i];
      }
    }
    if (caller == null) {
      caller = stackTraceElements[Math.min(4, stackTraceElements.length - 1)];
    }
    return caller.getClassName() + '.' + caller.getMethodName();
  }

}

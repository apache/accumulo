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
package org.apache.accumulo.server.compaction;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.util.Retry;
import org.apache.accumulo.core.util.Retry.NeedsRetryDelay;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RetryableThriftCall<T> {

  public static class RetriesExceededException extends Exception {

    private static final long serialVersionUID = 1L;

    public RetriesExceededException() {}

    public RetriesExceededException(String message, Throwable cause, boolean enableSuppression,
        boolean writableStackTrace) {
      super(message, cause, enableSuppression, writableStackTrace);
    }

    public RetriesExceededException(String message, Throwable cause) {
      super(message, cause);
    }

    public RetriesExceededException(String message) {
      super(message);
    }

    public RetriesExceededException(Throwable cause) {
      super(cause);
    }

  }

  private static final Logger LOG = LoggerFactory.getLogger(RetryableThriftCall.class);
  public static final long MAX_WAIT_TIME = 60000;

  private final RetryableThriftFunction<T> function;
  private final Retry retry;

  /**
   * RetryableThriftCall constructor
   *
   * @param start initial wait time
   * @param maxWaitTime max wait time
   * @param maxNumRetries number of times to retry, 0 to retry forever
   * @param function function to execute
   */
  public RetryableThriftCall(long start, long maxWaitTime, int maxNumRetries,
      RetryableThriftFunction<T> function) {
    this.function = function;
    NeedsRetryDelay builder = null;
    if (maxNumRetries == 0) {
      builder = Retry.builder().infiniteRetries();
    } else {
      builder = Retry.builder().maxRetries(maxNumRetries);
    }
    this.retry = builder.retryAfter(start, MILLISECONDS).incrementBy(0, MILLISECONDS)
        .maxWait(maxWaitTime, MILLISECONDS).backOffFactor(2).logInterval(1, TimeUnit.MINUTES)
        .createRetry();
  }

  /**
   * Attempts to call the function, waiting and retrying when TException is thrown. Wait time is
   * initially set to the start time and doubled each time, up to the maximum wait time. If
   * maxNumRetries is 0, then this will retry forever. If maxNumRetries is non-zero, then a
   * RuntimeException is thrown when it has exceeded he maxNumRetries parameter.
   *
   * @return T
   * @throws RetriesExceededException when maximum number of retries has been exceeded and the cause
   *         is set to the last TException
   */
  public T run() throws RetriesExceededException {
    T result = null;
    do {
      try {
        result = function.execute();
      } catch (TException e) {
        LOG.error("Error in Thrift function, retrying ...", e);
      }
      if (result == null) {
        if (this.retry.canRetry()) {
          this.retry.useRetry();
          try {
            this.retry.waitForNextAttempt(LOG, "making a thrift RPC");
          } catch (InterruptedException e) {
            LOG.error("Error waiting for next attempt: {}, retrying now.", e.getMessage(), e);
          }
        } else {
          throw new RetriesExceededException(
              "Maximum number of retries (" + this.retry.retriesCompleted() + ") attempted.");
        }
      }
    } while (null == result);
    return result;
  }

}

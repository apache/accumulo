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

import org.apache.log4j.Logger;

/**
 *
 */
public class Retry {
  private static final Logger log = Logger.getLogger(Retry.class);

  private final long maxRetries, maxWait, waitIncrement;
  private long retriesDone, currentWait;

  /**
   * @param maxRetries
   *          Maximum times to retry
   * @param startWait
   *          The amount of time (ms) to wait for the initial retry
   * @param maxWait
   *          The maximum wait (ms)
   * @param waitIncrement
   *          The amount of time (ms) to increment next wait time by
   */
  public Retry(long maxRetries, long startWait, long maxWait, long waitIncrement) {
    this.maxRetries = maxRetries;
    this.maxWait = maxWait;
    this.waitIncrement = waitIncrement;
    this.retriesDone = 0l;
    this.currentWait = startWait;
  }

  public boolean canRetry() {
    return retriesDone < maxRetries;
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
    log.debug("Sleeping for " + currentWait + "ms before retrying operation");
    Thread.sleep(currentWait);
    currentWait = Math.min(maxWait, currentWait + waitIncrement);
  }

}

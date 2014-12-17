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

/**
 * Creates {@link Retry} instances with the given parameters
 */
public class RetryFactory {
  public static final long DEFAULT_MAX_RETRIES = 10l, DEFAULT_START_WAIT = 250l, DEFAULT_WAIT_INCREMENT = 250l, DEFAULT_MAX_WAIT = 5000l;
  public static final RetryFactory DEFAULT_INSTANCE = new RetryFactory(DEFAULT_MAX_RETRIES, DEFAULT_START_WAIT, DEFAULT_WAIT_INCREMENT, DEFAULT_MAX_WAIT);

  private final long maxRetries, startWait, maxWait, waitIncrement;

  public RetryFactory(long maxRetries, long startWait, long waitIncrement, long maxWait) {
    this.maxRetries = maxRetries;
    this.startWait = startWait;
    this.maxWait = maxWait;
    this.waitIncrement = waitIncrement;
  }

  public Retry create() {
    return new Retry(maxRetries, startWait, waitIncrement, maxWait);
  }
}

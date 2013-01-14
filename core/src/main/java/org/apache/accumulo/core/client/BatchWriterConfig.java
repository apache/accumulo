/**
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
package org.apache.accumulo.core.client;

import java.util.concurrent.TimeUnit;

/**
 * This object holds configuration settings used to instantiate a {@link BatchWriter}
 */
public class BatchWriterConfig {
  private long maxMemory = 50 * 1024 * 1024;
  private long maxLatency = 120000;
  private long timeout = Long.MAX_VALUE;
  private int maxWriteThreads = 3;
  
  /**
   * 
   * @param maxMemory
   *          size in bytes of the maximum memory to batch before writing. Defaults to 50M.
   * @return
   */

  public BatchWriterConfig setMaxMemory(long maxMemory) {
    this.maxMemory = maxMemory;
    return this;
  }
  
  /**
   * @param maxLatency
   *          The maximum amount of time to hold data in memory before flushing it to servers. For no max set to zero or Long.MAX_VALUE with TimeUnit.MILLIS.
   *          Defaults to 120 seconds.
   * @param timeUnit
   *          Determines how maxLatency will be interpreted.
   * @return this to allow chaining of set methods
   */

  public BatchWriterConfig setMaxLatency(long maxLatency, TimeUnit timeUnit) {
    if (maxLatency < 0)
      throw new IllegalArgumentException("Negative max latency not allowed " + maxLatency);

    if (maxLatency == 0)
      this.maxLatency = Long.MAX_VALUE;
    else
      this.maxLatency = timeUnit.toMillis(maxLatency);
    return this;
  }
  
  /**
   * 
   * @param timeout
   *          The maximum amount of time an unresponsive server will be retried. When this timeout is exceeded, the BatchWriter should throw an exception. For
   *          no timeout set to zero or Long.MAX_VALUE with TimeUnit.MILLIS. Defaults to no timeout.
   * @param timeUnit
   * @return this to allow chaining of set methods
   */

  public BatchWriterConfig setTimeout(long timeout, TimeUnit timeUnit) {
    if (timeout < 0)
      throw new IllegalArgumentException("Negative timeout not allowed " + timeout);
    
    if (timeout == 0)
      timeout = Long.MAX_VALUE;
    else
      this.timeout = timeUnit.toMillis(timeout);
    return this;
  }
  
  /**
   * @param maxWriteThreads
   *          the maximum number of threads to use for writing data to the tablet servers. Defaults to 3.
   * @return this to allow chaining of set methods
   */

  public BatchWriterConfig setMaxWriteThreads(int maxWriteThreads) {
    if (maxWriteThreads <= 0)
      throw new IllegalArgumentException("Max threads must be positive " + maxWriteThreads);

    this.maxWriteThreads = maxWriteThreads;
    return this;
  }

  public long getMaxMemory() {
    return maxMemory;
  }
  
  public long getMaxLatency(TimeUnit timeUnit) {
    return timeUnit.convert(maxLatency, TimeUnit.MILLISECONDS);
  }
  
  public long getTimeout(TimeUnit timeUnit) {
    return timeUnit.convert(timeout, TimeUnit.MILLISECONDS);
  }
  
  public int getMaxWriteThreads() {
    return maxWriteThreads;
  }
}

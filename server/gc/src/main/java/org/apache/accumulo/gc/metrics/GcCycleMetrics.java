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
package org.apache.accumulo.gc.metrics;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.accumulo.core.gc.thrift.GcCycleStats;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Wrapper class for GcCycleStats so that underlying thrift code in GcCycleStats is not modified.
 * Provides Thread safe access to the gc cycle stats for metrics reporting.
 */
public class GcCycleMetrics {

  private static final Logger log = LoggerFactory.getLogger(GcCycleMetrics.class);

  private AtomicReference<GcCycleStats> lastCollect = new AtomicReference<>(new GcCycleStats());
  private AtomicReference<GcCycleStats> lastWalCollect = new AtomicReference<>(new GcCycleStats());

  private AtomicLong postOpDurationNanos = new AtomicLong(0);
  private AtomicLong runCycleCount = new AtomicLong(0);

  public GcCycleMetrics() {}

  /**
   * Get the last gc run statistics.
   *
   * @return the statistics for the last gc run.
   */
  public GcCycleStats getLastCollect() {
    return lastCollect.get();
  }

  /**
   * Set the last gc run statistics. Makes a defensive deep copy so that if the gc implementation
   * modifies the values.
   *
   * @param lastCollect
   *          the last gc run statistics.
   */
  public void setLastCollect(final GcCycleStats lastCollect) {
    this.lastCollect.set(new GcCycleStats(lastCollect));
  }

  /**
   * The statistics from the last wal collection.
   *
   * @return the last wal collection statistics.
   */
  public GcCycleStats getLastWalCollect() {
    return lastWalCollect.get();
  }

  /**
   * Set the lost wal collection statistics
   *
   * @param lastWalCollect
   *          last wal statistics
   */
  public void setLastWalCollect(final GcCycleStats lastWalCollect) {
    this.lastWalCollect.set(new GcCycleStats(lastWalCollect));
  }

  /**
   * Duration of post operation (compact, flush, none) in nanoseconds.
   *
   * @return duration in nanoseconds.
   */
  public long getPostOpDurationNanos() {
    return postOpDurationNanos.get();
  }

  /**
   * Set the duration of post operation (compact, flush, none) in nanoseconds.
   *
   * @param postOpDurationNanos
   *          the duration, in nanoseconds.
   */
  public void setPostOpDurationNanos(long postOpDurationNanos) {
    this.postOpDurationNanos.set(postOpDurationNanos);
  }

  /**
   * The number of gc cycles that have completed since initialization at process start.
   *
   * @return current run cycle count.
   */
  public long getRunCycleCount() {
    return runCycleCount.get();
  }

  /**
   * Set the counter for number of completed gc collection cycles p the provided value. The value is
   * expected to be &gt;= 0. If a negative value is provided, the count is set to zero and a warning
   * is logged rather than throwing an exception.
   *
   * @param runCycleCount
   *          the number of gc collect cycles completed.
   */
  public void setRunCycleCount(long runCycleCount) {

    if (runCycleCount < 0) {
      log.warn("Attempted to set run cycle count to {}.  Value must be => 0. Count set to 0",
          runCycleCount);
    }
    this.runCycleCount.set(runCycleCount);
  }

  /**
   * Increment the gc run cycle count by one.
   */
  public void incrementRunCycleCount() {
    this.runCycleCount.incrementAndGet();
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder("GcMetricsValues{");
    sb.append("lastCollect=").append(lastCollect.get());
    sb.append(", lastWalCollect=").append(lastWalCollect.get());
    sb.append(", postOpDuration=").append(postOpDurationNanos.get());
    sb.append('}');
    return sb.toString();
  }
}

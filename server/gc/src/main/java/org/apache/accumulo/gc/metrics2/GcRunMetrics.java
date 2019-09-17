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
package org.apache.accumulo.gc.metrics2;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.accumulo.core.gc.thrift.GcCycleStats;

/**
 * Wrapper class for GcCycleStats so that underlying thrift code is not modified. Provides Thread
 * safe access to gc metrics for reporting.
 */
public class GcRunMetrics {

  private AtomicReference<GcCycleStats> lastCollect = new AtomicReference<>(new GcCycleStats());
  private AtomicReference<GcCycleStats> lastWalCollect = new AtomicReference<>(new GcCycleStats());

  private AtomicLong postOpDuration = new AtomicLong(0);
  private AtomicLong runCycleCount = new AtomicLong(0);

  public GcRunMetrics() {}

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
  public long getPostOpDuration() {
    return postOpDuration.get();
  }

  /**
   * Duration of post operation (compact, flush, none) in nanoseconds.
   *
   * @param postOpDuration
   *          the duration, in nanoseconds.
   */
  public void setPostOpDuration(long postOpDuration) {
    this.postOpDuration.set(postOpDuration);
  }

  /**
   * Duration of post operation (compact, flush, none) in nanoseconds.
   *
   * @return duration in nanoseconds.
   */
  public long getRunCycleCount() {
    return runCycleCount.get();
  }

  /**
   * Duration of post operation (compact, flush, none) in nanoseconds.
   *
   * @param runCycleCount
   *          the number of gc collect cycles completed.
   */
  public void setRunCycleCount(long runCycleCount) {
    this.runCycleCount.set(runCycleCount);
  }

  public void incrementRunCycleCount() {
    this.runCycleCount.incrementAndGet();
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder("GcMetricsValues{");
    sb.append("lastCollect=").append(lastCollect.get());
    sb.append(", lastWalCollect=").append(lastWalCollect.get());
    sb.append(", postOpDuration=").append(postOpDuration.get());
    sb.append('}');
    return sb.toString();
  }
}

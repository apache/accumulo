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
package org.apache.accumulo.core.spi.scan;

import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.data.Column;

/**
 * Provides information about an active Accumulo scan against a tablet. Accumulo scans operate by
 * repeatedly gathering batches of data and returning those to the client.
 *
 * @since 2.0.0
 */
public interface ScanInfo {

  enum Type {
    SINGLE, MULTI
  }

  public interface Stats {
    long min();

    long max();

    double mean();

    long sum();

    long num();
  }

  Type getScanType();

  String getTableId();

  /**
   * Returns the first time a tablet knew about a scan over its portion of data.
   *
   * @see ScanInfo#getCurrentTime()
   */
  long getCreationTime();

  /**
   * If the scan has run, returns the last run time.
   *
   * @see ScanInfo#getCurrentTime()
   */
  OptionalLong getLastRunTime();

  /**
   * Returns timing statistics about running and gathering a batches of data.
   */
  Optional<Stats> getRunTimeStats();

  /**
   * Returns statistics about the time between running. These stats are only about the idle times
   * before the last run time. The idle time after the last run time are not included. If the scan
   * has never run, then there are not stats.
   */
  Optional<Stats> getIdleTimeStats();

  /**
   * This method is similar to {@link #getIdleTimeStats()}, but it also includes the time period
   * between the last run time and now in the stats. If the scan has never run, then the stats are
   * computed using only {@code currentTime - creationTime}.
   *
   * @see ScanInfo#getCurrentTime()
   */
  Stats getIdleTimeStats(long currentTime);

  Set<Column> getFetchedColumns();

  List<IteratorSetting> getScanIterators();

  /**
   * There are multiple ways to get time in Java. Use this method to get current time in same way
   * that Accumulo obtains time.
   *
   * @return
   */
  public static long getCurrentTime() {
    return System.currentTimeMillis();
  }

}

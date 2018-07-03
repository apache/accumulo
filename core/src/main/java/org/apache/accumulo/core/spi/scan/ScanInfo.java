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

import java.util.Collection;
import java.util.OptionalLong;
import java.util.Set;

import org.apache.accumulo.core.data.Column;
import org.apache.accumulo.core.spi.common.IteratorConfiguration;
import org.apache.accumulo.core.spi.common.Stats;

/**
 * Provides information about an active Accumulo scan against a tablet. Accumulo scans operate by
 * repeatedly gathering batches of data and returning those to the client.
 *
 * <p>
 * All times are in milliseconds and obtained using System.currentTimeMillis().
 *
 * @since 2.0.0
 */
public interface ScanInfo {

  enum Type {
    SINGLE, MULTI
  }

  Type getScanType();

  String getTableId();

  /**
   * Returns the first time a tablet knew about a scan over its portion of data.
   */
  long getCreationTime();

  /**
   * If the scan has run, returns the last run time.
   */
  OptionalLong getLastRunTime();

  /**
   * Returns timing statistics about running and gathering a batches of data.
   */
  Stats getRunTimeStats();

  /**
   * Returns statistics about the time between running. These stats are only about the idle times
   * before the last run time. The idle time after the last run time are not included. If the scan
   * has never run, then there are no stats.
   */
  Stats getIdleTimeStats();

  /**
   * This method is similar to {@link #getIdleTimeStats()}, but it also includes the time period
   * between the last run time and now in the stats. If the scan has never run, then the stats are
   * computed using only {@code currentTime - creationTime}.
   *
   * @see ScanInfo#getCurrentTime()
   */
  Stats getIdleTimeStats(long currentTime);

  /**
   * This method returns what column were fetched by a scan. When a family is fetched, a Column
   * object where everything but the family is null is in the set.
   *
   * <p>
   * The following example code shows how this method can be used to check if a family was fetched
   * or a family+qualifier was fetched. If continually checking for the same column, should probably
   * create a constant.
   *
   * <pre>
   * <code>
   *   boolean wasFamilyFetched(ScanInfo si, byte[] fam) {
   *     Column family = new Column(fam, null, null);
   *     return si.getFetchedColumns().contains(family);
   *   }
   *
   *   boolean wasColumnFetched(ScanInfo si, byte[] fam, byte[] qual) {
   *     Column col = new Column(fam, qual, null);
   *     return si.getFetchedColumns().contains(col);
   *   }
   * </code>
   * </pre>
   *
   *
   * @return The family and family+qualifier pairs fetched.
   */
  Set<Column> getFetchedColumns();

  /**
   * @return iterators that where configured on the client side scanner
   */
  Collection<IteratorConfiguration> getClientScanIterators();
}

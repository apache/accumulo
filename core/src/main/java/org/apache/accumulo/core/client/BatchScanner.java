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
package org.apache.accumulo.core.client;

import org.apache.accumulo.core.data.Range;

import java.util.Collection;
import java.util.concurrent.TimeUnit;

/**
 * Implementations of BatchScanner support efficient lookups of many ranges in accumulo.
 * BatchScanners are also appropriate for large, single ranges,
 * as a BatchScanner will break those ranges up into separate RPCs
 * provided the range spans more than one tablet
 * and there are sufficiently many scan threads available.
 *
 * Only use this when you do not care about returned data being in sorted order.
 * Use a {@link Scanner} instead when sorted order is important.
 *
 * A BatchScanner instance will use no more threads than provided in the construction of the BatchScanner
 * implementation. Multiple invocations of <code>iterator()</code> will all share the same resources of the instance.
 * A new BatchScanner instance should be created to use allocate additional threads.
 */

public interface BatchScanner extends ScannerBase {

  /**
   * Allows scanning over multiple ranges efficiently.
   *
   * @param ranges
   *          specifies the non-overlapping ranges to query
   */
  void setRanges(Collection<Range> ranges);

  /**
   * Cleans up and finalizes the scanner
   */
  void close();

  /**
   * Sets a timeout threshold for a server to respond. The batch scanner will accomplish as much work as possible before throwing an exception. BatchScanner
   * iterators will throw a {@link TimedOutException} when all needed servers timeout. Setting the timeout to zero or Long.MAX_VALUE and TimeUnit.MILLISECONDS
   * means no timeout.
   *
   * <p>
   * If not set, there is not timeout. The BatchScanner will retry forever.
   *
   * @param timeUnit
   *          determines how timeout is interpreted
   * @since 1.5.0
   */
  @Override
  void setTimeout(long timeout, TimeUnit timeUnit);
}

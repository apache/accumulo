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

import java.util.Collection;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.data.Range;

/**
 * In exchange for possibly <b>returning scanned entries out of order</b>, BatchScanner implementations may scan an Accumulo table more efficiently by
 * <ul>
 * <li>Looking up multiple ranges in parallel. Parallelism is constrained by the number of threads available to the BatchScanner, set in its constructor.</li>
 * <li>Breaking up large ranges into subranges. Often the number and boundaries of subranges are determined by a table's split points.</li>
 * <li>Combining multiple ranges into a single RPC call to a tablet server.</li>
 * </ul>
 *
 * The above techniques lead to better performance than a {@link Scanner} in use cases such as
 * <ul>
 * <li>Retrieving many small ranges</li>
 * <li>Scanning a large range that returns many entries</li>
 * <li>Running server-side iterators that perform computation, even if few entries are returned from the scan itself</li>
 * </ul>
 *
 * To re-emphasize, only use a BatchScanner when you do not care whether returned data is in sorted order. Use a {@link Scanner} instead when sorted order is
 * important.
 *
 * <p>
 * A BatchScanner instance will use no more threads than provided in the construction of the BatchScanner implementation. Multiple invocations of
 * <code>iterator()</code> will all share the same resources of the instance. A new BatchScanner instance should be created to use allocate additional threads.
 */
public interface BatchScanner extends ScannerBase {

  /**
   * Allows scanning over multiple ranges efficiently.
   *
   * @param ranges
   *          specifies the non-overlapping ranges to query
   */
  void setRanges(Collection<Range> ranges);

  @Override
  void close();

  /**
   * {@inheritDoc}
   *
   * <p>
   * The batch scanner will accomplish as much work as possible before throwing an exception. BatchScanner iterators will throw a {@link TimedOutException} when
   * all needed servers timeout.
   */
  @Override
  void setTimeout(long timeout, TimeUnit timeUnit);
}

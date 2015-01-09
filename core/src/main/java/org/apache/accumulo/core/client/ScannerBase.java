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

import java.util.Iterator;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.Text;

/**
 * This class hosts configuration methods that are shared between different types of scanners.
 *
 */
public interface ScannerBase extends Iterable<Entry<Key,Value>> {

  /**
   * Add a server-side scan iterator.
   *
   * @param cfg
   *          fully specified scan-time iterator, including all options for the iterator. Any changes to the iterator setting after this call are not propagated
   *          to the stored iterator.
   * @throws IllegalArgumentException
   *           if the setting conflicts with existing iterators
   */
  void addScanIterator(IteratorSetting cfg);

  /**
   * Remove an iterator from the list of iterators.
   *
   * @param iteratorName
   *          nickname used for the iterator
   */
  void removeScanIterator(String iteratorName);

  /**
   * Update the options for an iterator. Note that this does <b>not</b> change the iterator options during a scan, it just replaces the given option on a
   * configured iterator before a scan is started.
   *
   * @param iteratorName
   *          the name of the iterator to change
   * @param key
   *          the name of the option
   * @param value
   *          the new value for the named option
   */
  void updateScanIteratorOption(String iteratorName, String key, String value);

  /**
   * Adds a column family to the list of columns that will be fetched by this scanner. By default when no columns have been added the scanner fetches all
   * columns.
   *
   * @param col
   *          the column family to be fetched
   */
  void fetchColumnFamily(Text col);

  /**
   * Adds a column to the list of columns that will be fetched by this scanner. The column is identified by family and qualifier. By default when no columns
   * have been added the scanner fetches all columns.
   *
   * @param colFam
   *          the column family of the column to be fetched
   * @param colQual
   *          the column qualifier of the column to be fetched
   */
  void fetchColumn(Text colFam, Text colQual);

  /**
   * Clears the columns to be fetched (useful for resetting the scanner for reuse). Once cleared, the scanner will fetch all columns.
   */
  void clearColumns();

  /**
   * Clears scan iterators prior to returning a scanner to the pool.
   */
  void clearScanIterators();

  /**
   * Returns an iterator over an accumulo table. This iterator uses the options that are currently set for its lifetime, so setting options will have no effect
   * on existing iterators.
   *
   * Keys returned by the iterator are not guaranteed to be in sorted order.
   *
   * @return an iterator over Key,Value pairs which meet the restrictions set on the scanner
   */
  Iterator<Entry<Key,Value>> iterator();

  /**
   * This setting determines how long a scanner will automatically retry when a failure occurs. By default a scanner will retry forever.
   *
   * Setting to zero or Long.MAX_VALUE and TimeUnit.MILLISECONDS means to retry forever.
   *
   * @param timeUnit
   *          determines how timeout is interpreted
   * @since 1.5.0
   */
  void setTimeout(long timeOut, TimeUnit timeUnit);

  /**
   * Returns the setting for how long a scanner will automatically retry when a failure occurs.
   *
   * @return the timeout configured for this scanner
   * @since 1.5.0
   */
  long getTimeout(TimeUnit timeUnit);

  /**
   * Closes any underlying connections on the scanner
   *
   * @since 1.5.0
   */
  void close();
}

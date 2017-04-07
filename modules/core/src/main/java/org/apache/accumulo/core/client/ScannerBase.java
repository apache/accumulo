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

import org.apache.accumulo.core.client.IteratorSetting.Column;
import org.apache.accumulo.core.client.sample.SamplerConfiguration;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.io.Text;

/**
 * This class hosts configuration methods that are shared between different types of scanners.
 *
 */
public interface ScannerBase extends Iterable<Entry<Key,Value>>, AutoCloseable {

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
   * columns. To fetch multiple column families call this function multiple times.
   *
   * <p>
   * This can help limit which locality groups are read on the server side.
   *
   * <p>
   * When used in conjunction with custom iterators, the set of column families fetched is passed to the top iterator's seek method. Custom iterators may change
   * this set of column families when calling seek on their source.
   *
   * @param col
   *          the column family to be fetched
   */
  void fetchColumnFamily(Text col);

  /**
   * Adds a column to the list of columns that will be fetched by this scanner. The column is identified by family and qualifier. By default when no columns
   * have been added the scanner fetches all columns.
   *
   * <p>
   * <b>WARNING</b>. Using this method with custom iterators may have unexpected results. Iterators have control over which column families are fetched. However
   * iterators have no control over which column qualifiers are fetched. When this method is called it activates a system iterator that only allows the
   * requested family/qualifier pairs through. This low level filtering prevents custom iterators from requesting additional column families when calling seek.
   *
   * <p>
   * For an example, assume fetchColumns(A, Q1) and fetchColumns(B,Q1) is called on a scanner and a custom iterator is configured. The families (A,B) will be
   * passed to the seek method of the custom iterator. If the custom iterator seeks its source iterator using the families (A,B,C), it will never see any data
   * from C because the system iterator filtering A:Q1 and B:Q1 will prevent the C family from getting through. ACCUMULO-3905 also has an example of the type of
   * problem this method can cause.
   *
   * <p>
   * tl;dr If using a custom iterator with a seek method that adds column families, then may want to avoid using this method.
   *
   * @param colFam
   *          the column family of the column to be fetched
   * @param colQual
   *          the column qualifier of the column to be fetched
   */
  void fetchColumn(Text colFam, Text colQual);

  /**
   * Adds a column to the list of columns that will be fetch by this scanner.
   *
   * @param column
   *          the {@link Column} to fetch
   * @since 1.7.0
   */
  void fetchColumn(Column column);

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
   * <p>
   * Keys returned by the iterator are not guaranteed to be in sorted order.
   *
   * @return an iterator over Key,Value pairs which meet the restrictions set on the scanner
   */
  @Override
  Iterator<Entry<Key,Value>> iterator();

  /**
   * This setting determines how long a scanner will automatically retry when a failure occurs. By default, a scanner will retry forever.
   *
   * <p>
   * Setting the timeout to zero (with any time unit) or {@link Long#MAX_VALUE} (with {@link TimeUnit#MILLISECONDS}) means no timeout.
   *
   * @param timeOut
   *          the length of the timeout
   * @param timeUnit
   *          the units of the timeout
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
   * Closes any underlying connections on the scanner. This may invalidate any iterators derived from the Scanner, causing them to throw exceptions.
   *
   * @since 1.5.0
   */
  @Override
  void close();

  /**
   * Returns the authorizations that have been set on the scanner
   *
   * @since 1.7.0
   * @return The authorizations set on the scanner instance
   */
  Authorizations getAuthorizations();

  /**
   * Setting this will cause the scanner to read sample data, as long as that sample data was generated with the given configuration. By default this is not set
   * and all data is read.
   *
   * <p>
   * One way to use this method is as follows, where the sampler configuration is obtained from the table configuration. Sample data can be generated in many
   * different ways, so its important to verify the sample data configuration meets expectations.
   *
   * <pre>
   * <code>
   *   // could cache this if creating many scanners to avoid RPCs.
   *   SamplerConfiguration samplerConfig = connector.tableOperations().getSamplerConfiguration(table);
   *   // verify table's sample data is generated in an expected way before using
   *   userCode.verifySamplerConfig(samplerConfig);
   *   scanner.setSamplerCongiguration(samplerConfig);
   * </code>
   * </pre>
   *
   * <p>
   * Of course this is not the only way to obtain a {@link SamplerConfiguration}, it could be a constant, configuration, etc.
   *
   * <p>
   * If sample data is not present or sample data was generated with a different configuration, then the scanner iterator will throw a
   * {@link SampleNotPresentException}. Also if a table's sampler configuration is changed while a scanner is iterating over a table, a
   * {@link SampleNotPresentException} may be thrown.
   *
   * @since 1.8.0
   */
  void setSamplerConfiguration(SamplerConfiguration samplerConfig);

  /**
   * @return currently set sampler configuration. Returns null if no sampler configuration is set.
   * @since 1.8.0
   */
  SamplerConfiguration getSamplerConfiguration();

  /**
   * Clears sampler configuration making a scanner read all data. After calling this, {@link #getSamplerConfiguration()} should return null.
   *
   * @since 1.8.0
   */
  void clearSamplerConfiguration();

  /**
   * This setting determines how long a scanner will wait to fill the returned batch. By default, a scanner wait until the batch is full.
   *
   * <p>
   * Setting the timeout to zero (with any time unit) or {@link Long#MAX_VALUE} (with {@link TimeUnit#MILLISECONDS}) means no timeout.
   *
   * @param timeOut
   *          the length of the timeout
   * @param timeUnit
   *          the units of the timeout
   * @since 1.8.0
   */
  void setBatchTimeout(long timeOut, TimeUnit timeUnit);

  /**
   * Returns the timeout to fill a batch in the given TimeUnit.
   *
   * @return the batch timeout configured for this scanner
   * @since 1.8.0
   */
  long getBatchTimeout(TimeUnit timeUnit);

  /**
   * Sets the name of the classloader context on this scanner. See the administration chapter of the user manual for details on how to configure and use
   * classloader contexts.
   *
   * @param classLoaderContext
   *          name of the classloader context
   * @throws NullPointerException
   *           if context is null
   * @since 1.8.0
   */
  void setClassLoaderContext(String classLoaderContext);

  /**
   * Clears the current classloader context set on this scanner
   *
   * @since 1.8.0
   */
  void clearClassLoaderContext();

  /**
   * Returns the name of the current classloader context set on this scanner
   *
   * @return name of the current context
   * @since 1.8.0
   */
  String getClassLoaderContext();
}

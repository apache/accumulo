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

import java.io.IOException;
import java.util.Iterator;
import java.util.Map.Entry;

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
  public void addScanIterator(IteratorSetting cfg);
  
  /**
   * Remove an iterator from the list of iterators.
   * 
   * @param iteratorName
   *          nickname used for the iterator
   */
  public void removeScanIterator(String iteratorName);
  
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
  public void updateScanIteratorOption(String iteratorName, String key, String value);
  
  /**
   * @deprecated since 1.4
   * @see {@link org.apache.accumulo.core.client.ScannerBase#addScanIterator(IteratorSetting)}
   */
  public void setScanIterators(int priority, String iteratorClass, String iteratorName) throws IOException;
  
  /**
   * @deprecated since 1.4
   * @see {@link org.apache.accumulo.core.client.ScannerBase#addScanIterator(IteratorSetting)}
   */
  public void setScanIteratorOption(String iteratorName, String key, String value);
  
  /**
   * Call this method to initialize regular expressions on a scanner. If it is not called, reasonable defaults will be used.
   * 
   * @param iteratorName
   *          a nickname for the iterator
   * @param iteratorPriority
   *          determines the order in which iterators are applied (system iterators are always applied first, then per-table and scan-time, lowest first)
   * @throws IOException
   *           if an exception occurs reading from the iterator stack
   * 
   * @deprecated since 1.4
   * @see {@link org.apache.accumulo.core.client.ScannerBase#addScanIterator(IteratorSetting)}
   * @see {@link org.apache.accumulo.core.iterators.user.RegExFilter}
   */
  public void setupRegex(String iteratorName, int iteratorPriority) throws IOException;
  
  /**
   * 
   * Set a row regular expression that filters non matching entries server side.
   * 
   * @param regex
   *          java regular expression to match
   * 
   * @deprecated since 1.4
   * @see {@link org.apache.accumulo.core.client.ScannerBase#addScanIterator(IteratorSetting)}
   * @see {@link org.apache.accumulo.core.iterators.user.RegExFilter#ROW_REGEX}
   * 
   *      <pre>
   * // Use the more flexible addScanIterator method:
   * ScanIterator cfg = new ScanIterator(&quot;regex&quot;, RegexIterator.class);
   * RegexIterator.setRegexs(cfg, row, null, null, null, false);
   * scanner.addScanIterator(priority, cfg);
   * </pre>
   */
  public void setRowRegex(String regex);
  
  /**
   * 
   * Set a column family regular expression that filters non matching entries server side.
   * 
   * @param regex
   *          java regular expression to match
   * 
   * @deprecated since 1.4
   * @see {@link org.apache.accumulo.core.client.ScannerBase#addScanIterator(IteratorSetting)}
   * @see {@link org.apache.accumulo.core.iterators.user.RegExFilter#COLF_REGEX}
   */
  public void setColumnFamilyRegex(String regex);
  
  /**
   * Use addScanIterator(int, ScanIterator);
   * 
   * Set a column qualifier regular expression that filters non matching entries server side.
   * 
   * @param regex
   *          java regular expression to match
   * 
   * @deprecated since 1.4
   * @see {@link org.apache.accumulo.core.client.ScannerBase#addScanIterator(IteratorSetting)}
   * @see {@link org.apache.accumulo.core.iterators.user.RegExFilter#COLQ_REGEX}.
   * 
   */
  public void setColumnQualifierRegex(String regex);
  
  /**
   * Set a value regular expression that filters non matching entries server side.
   * 
   * @param regex
   *          java regular expression to match
   * 
   * @deprecated since 1.4
   * @see {@link org.apache.accumulo.core.client.ScannerBase#addScanIterator(IteratorSetting)}
   * @see {@link org.apache.accumulo.core.iterators.user.RegExFilter}
   */
  public void setValueRegex(String regex);
  
  /**
   * Adds a column family to the list of columns that will be fetched by this scanner. By default when no columns have been added the scanner fetches all
   * columns.
   * 
   * @param col
   *          the column family to be fetched
   */
  public void fetchColumnFamily(Text col);
  
  /**
   * Adds a column to the list of columns that will be fetched by this scanner. The column is identified by family and qualifier. By default when no columns
   * have been added the scanner fetches all columns.
   * 
   * @param colFam
   *          the column family of the column to be fetched
   * @param colQual
   *          the column qualifier of the column to be fetched
   */
  public void fetchColumn(Text colFam, Text colQual);
  
  /**
   * Clears the columns to be fetched (useful for resetting the scanner for reuse). Once cleared, the scanner will fetch all columns.
   */
  public void clearColumns();
  
  /**
   * Clears scan iterators prior to returning a scanner to the pool.
   */
  public void clearScanIterators();
  
  /**
   * Returns an iterator over an accumulo table. This iterator uses the options that are currently set for its lifetime, so setting options will have no effect
   * on existing iterators.
   * 
   * Keys returned by the iterator are not guaranteed to be in sorted order.
   * 
   * @return an iterator over Key,Value pairs which meet the restrictions set on the scanner
   */
  public Iterator<Entry<Key,Value>> iterator();
}

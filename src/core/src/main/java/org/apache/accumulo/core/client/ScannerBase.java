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

import org.apache.hadoop.io.Text;

/**
 * This class host methods that are shared between all different types of scanners.
 * 
 */
public interface ScannerBase {
  
  /**
   * Sets server side scan iterators.
   * 
   * @param priority
   *          determines the order in which iterators are applied (system iterators are always applied first, then per-table and scan-time, lowest first)
   * @param iteratorClass
   *          the fully qualified class name of the iterator to be applied at scan time
   * @param iteratorName
   *          a nickname for the iterator
   * @throws IOException
   *           if an exception occurs reading from the iterator stack
   * 
   */
  public void setScanIterators(int priority, String iteratorClass, String iteratorName) throws IOException;
  
  /**
   * Sets options for server side scan iterators.
   * 
   * @param iteratorName
   *          a nickname for the iterator
   * @param key
   *          option name (depends on specific iterator)
   * @param value
   *          option value (depends on specific iterator)
   * 
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
   */
  public void setupRegex(String iteratorName, int iteratorPriority) throws IOException;
  
  /**
   * Set a row regular expression that filters non matching entries server side.
   * 
   * @param regex
   *          java regular expression to match
   * 
   */
  public void setRowRegex(String regex);
  
  /**
   * Set a column family regular expression that filters non matching entries server side.
   * 
   * @param regex
   *          java regular expression to match
   * 
   */
  public void setColumnFamilyRegex(String regex);
  
  /**
   * Set a column qualifier regular expression that filters non matching entries server side.
   * 
   * @param regex
   *          java regular expression to match
   * 
   */
  public void setColumnQualifierRegex(String regex);
  
  /**
   * Set a value regular expression that filters non matching entries server side.
   * 
   * @param regex
   *          java regular expression to match
   * 
   */
  public void setValueRegex(String regex);
  
  /**
   * @param col
   *          limit the scan to only this column family (multiple calls appends to the list of column families to limit)
   */
  public void fetchColumnFamily(Text col);
  
  /**
   * Limits the scan to only this column, identified by family and qualifier. Multiple calls appends to the list of columns to be fetched.
   * 
   * @param colFam
   *          the column family of the column to be fetched
   * @param colQual
   *          the column qualifier of the column to be fetched
   */
  public void fetchColumn(Text colFam, Text colQual);
  
  /**
   * Clears the columns to be fetched (useful for resetting the scanner for reuse)
   */
  public void clearColumns();
  
  /**
   * Clears scan iterators prior to returning a scanner to the pool.
   */
  public void clearScanIterators();
}

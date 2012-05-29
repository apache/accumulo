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

/**
 * Scans a table over a given range.
 * 
 * "Clients can iterate over multiple column families, and there are several mechanisms for limiting the rows, columns, and timestamps traversed by a scan. For
 * example, we could restrict [a] scan ... to only produce anchors whose columns match [a] regular expression ..., or to only produce anchors whose timestamps
 * fall within ten days of the current time."
 */
public interface Scanner extends ScannerBase {
  
  /**
   * This setting determines how long a scanner will automatically retry when a failure occurs. By default a scanner will retry forever.
   * 
   * @param timeOut
   *          in seconds
   */
  public void setTimeOut(int timeOut);
  
  /**
   * Returns the setting for how long a scanner will automatically retry when a failure occurs.
   * 
   * @return the timeout configured for this scanner
   */
  public int getTimeOut();
  
  /**
   * Sets the range of keys to scan over.
   * 
   * @param range
   *          key range to begin and end scan
   */
  public void setRange(Range range);
  
  /**
   * Returns the range of keys to scan over.
   * 
   * @return the range configured for this scanner
   */
  public Range getRange();
  
  /**
   * Sets the number of Key/Value pairs that will be fetched at a time from a tablet server.
   * 
   * @param size
   *          the number of Key/Value pairs to fetch per call to Accumulo
   */
  public void setBatchSize(int size);
  
  /**
   * Returns the batch size (number of Key/Value pairs) that will be fetched at a time from a tablet server.
   * 
   * @return the batch size configured for this scanner
   */
  public int getBatchSize();
  
  /**
   * Enables row isolation. Writes that occur to a row after a scan of that row has begun will not be seen if this option is enabled.
   */
  public void enableIsolation();
  
  /**
   * Disables row isolation. Writes that occur to a row after a scan of that row has begun may be seen if this option is enabled.
   */
  void disableIsolation();
}

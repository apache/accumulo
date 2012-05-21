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
package org.apache.accumulo.core.client.impl;

/**
 * provides scanner functionality
 * 
 * "Clients can iterate over multiple column families, and there are several 
 * mechanisms for limiting the rows, columns, and timestamps traversed by a 
 * scan. For example, we could restrict [a] scan ... to only produce anchors 
 * whose columns match [a] regular expression ..., or to only produce 
 * anchors whose timestamps fall within ten days of the current time."
 * 
 */

import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.thrift.AuthInfo;
import org.apache.accumulo.core.util.ArgumentChecker;
import org.apache.hadoop.io.Text;

public class ScannerImpl extends ScannerOptions implements Scanner {
  
  // keep a list of columns over which to scan
  // keep track of the last thing read
  // hopefully, we can track all the state in the scanner on the client
  // and just query for the next highest row from the tablet server
  
  private Instance instance;
  private AuthInfo credentials;
  private Authorizations authorizations;
  private Text table;
  
  private int timeOut;
  
  private Range range;
  private boolean isolated = false;
  
  public ScannerImpl(Instance instance, AuthInfo credentials, String table, Authorizations authorizations) {
    ArgumentChecker.notNull(instance, credentials, table, authorizations);
    this.instance = instance;
    this.credentials = credentials;
    this.table = new Text(table);
    this.range = new Range((Key) null, (Key) null);
    this.authorizations = authorizations;
    
    this.timeOut = Integer.MAX_VALUE;
  }
  
  /**
   * When failure occurs, the scanner automatically retries. This setting determines how long a scanner will retry. By default a scanner will retry forever.
   * 
   * @param timeOut
   *          in milliseconds
   */
  @Override
  public synchronized void setTimeOut(int timeOut) {
    this.timeOut = timeOut;
  }
  
  @Override
  public synchronized int getTimeOut() {
    return timeOut;
  }
  
  @Override
  public synchronized void setRange(Range range) {
    ArgumentChecker.notNull(range);
    this.range = range;
  }
  
  @Override
  public synchronized Range getRange() {
    return range;
  }
  
  /**
   * Returns an iterator over an accumulo table. This iterator uses the options that are currently set on the scanner for its lifetime. So setting options on a
   * Scanner object will have no effect on existing iterators.
   */
  public synchronized Iterator<Entry<Key,Value>> iterator() {
    return new ScannerIterator(instance, credentials, table, authorizations, range, getBatchSize(), timeOut, this, isolated);
  }
  
  @Override
  public synchronized void enableIsolation() {
    this.isolated = true;
  }
  
  @Override
  public synchronized void disableIsolation() {
    this.isolated = false;
  }
}

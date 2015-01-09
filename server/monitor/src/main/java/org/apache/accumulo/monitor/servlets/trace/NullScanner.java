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
package org.apache.accumulo.monitor.servlets.trace;

import java.util.Iterator;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.Text;

public class NullScanner implements Scanner {

  @Override
  public void addScanIterator(IteratorSetting cfg) {}

  @Override
  public void updateScanIteratorOption(String iteratorName, String key, String value) {}

  @Override
  public void fetchColumnFamily(Text col) {}

  @Override
  public void fetchColumn(Text colFam, Text colQual) {}

  @Override
  public void clearColumns() {}

  @Override
  public void clearScanIterators() {}

  @Deprecated
  @Override
  public void setTimeOut(int timeOut) {}

  @Deprecated
  @Override
  public int getTimeOut() {
    return 0;
  }

  @Override
  public void setRange(Range range) {}

  @Override
  public Range getRange() {
    return null;
  }

  @Override
  public void setBatchSize(int size) {

  }

  @Override
  public int getBatchSize() {
    return 0;
  }

  @Override
  public void enableIsolation() {

  }

  @Override
  public void disableIsolation() {

  }

  @Override
  public Iterator<Entry<Key,Value>> iterator() {
    return new NullKeyValueIterator();
  }

  @Override
  public void removeScanIterator(String iteratorName) {}

  @Override
  public void setTimeout(long timeOut, TimeUnit timeUnit) {}

  @Override
  public long getTimeout(TimeUnit timeUnit) {
    return 0;
  }

  @Override
  public void close() {}

  @Override
  public long getReadaheadThreshold() {
    return 0l;
  }

  @Override
  public void setReadaheadThreshold(long batches) {

  }
}

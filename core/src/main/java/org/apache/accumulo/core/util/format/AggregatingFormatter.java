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
package org.apache.accumulo.core.util.format;

import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;

/**
 * Formatter that will aggregate entries for various display purposes.
 */
public abstract class AggregatingFormatter extends DefaultFormatter {
  @Override
  public String next() {
    Iterator<Entry<Key,Value>> si = super.getScannerIterator();
    checkState(true);
    while (si.hasNext())
      aggregateStats(si.next());
    return getStats();
  }

  /**
   * Generate statistics from each {@link Entry}, called for each entry to be iterated over.
   *
   * @param next
   *          the next entry to aggregate
   */
  protected abstract void aggregateStats(Entry<Key,Value> next);

  /**
   * Finalize the aggregation and return the result. Called once at the end.
   *
   * @return the aggregation results, suitable for printing to the console
   */
  protected abstract String getStats();
}

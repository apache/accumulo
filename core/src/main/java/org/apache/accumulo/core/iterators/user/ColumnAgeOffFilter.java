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
package org.apache.accumulo.core.iterators.user;

import java.io.IOException;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.Filter;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.conf.ColumnSet;
import org.apache.accumulo.core.iterators.conf.ColumnToClassMapping;
import org.apache.accumulo.core.util.Pair;
import org.apache.hadoop.io.Text;

/**
 * A filter that ages off key/value pairs based on the Key's column and timestamp. It removes an entry if its timestamp is less than currentTime - threshold.
 * Different thresholds are set for each column.
 */
public class ColumnAgeOffFilter extends Filter {
  public static class TTLSet extends ColumnToClassMapping<Long> {
    public TTLSet(Map<String,String> objectStrings) {
      super();

      for (Entry<String,String> entry : objectStrings.entrySet()) {
        String column = entry.getKey();
        String ttl = entry.getValue();
        Long l = Long.parseLong(ttl);

        Pair<Text,Text> colPair = ColumnSet.decodeColumns(column);

        if (colPair.getSecond() == null) {
          addObject(colPair.getFirst(), l);
        } else {
          addObject(colPair.getFirst(), colPair.getSecond(), l);
        }
      }
    }
  }

  TTLSet ttls;
  long currentTime = 0;

  @Override
  public boolean accept(Key k, Value v) {
    Long threshold = ttls.getObject(k);
    if (threshold == null)
      return true;
    if (currentTime - k.getTimestamp() > threshold)
      return false;
    return true;
  }

  @Override
  public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options, IteratorEnvironment env) throws IOException {
    super.init(source, options, env);
    this.ttls = new TTLSet(options);
    currentTime = System.currentTimeMillis();
  }

  @Override
  public SortedKeyValueIterator<Key,Value> deepCopy(IteratorEnvironment env) {
    ColumnAgeOffFilter copy = (ColumnAgeOffFilter) super.deepCopy(env);
    copy.currentTime = currentTime;
    copy.ttls = ttls;
    return copy;
  }

  public void overrideCurrentTime(long ts) {
    this.currentTime = ts;
  }

  @Override
  public IteratorOptions describeOptions() {
    IteratorOptions io = super.describeOptions();
    io.setName("colageoff");
    io.setDescription("ColumnAgeOffFilter ages off columns at different rates given a time to live in milliseconds for each column");
    io.addUnnamedOption("<col fam>[:<col qual>] <Long> (escape non-alphanum chars using %<hex>)");
    return io;
  }

  @Override
  public boolean validateOptions(Map<String,String> options) {
    if (super.validateOptions(options) == false)
      return false;
    try {
      this.ttls = new TTLSet(options);
    } catch (Exception e) {
      throw new IllegalArgumentException("bad TTL options", e);
    }
    return true;
  }

  /**
   * A convenience method for adding or changing an age off threshold for a column.
   *
   * @param is
   *          IteratorSetting object to configure.
   * @param column
   *          column to encode as a parameter name.
   * @param ttl
   *          age off threshold in milliseconds.
   */
  public static void addTTL(IteratorSetting is, IteratorSetting.Column column, Long ttl) {
    is.addOption(ColumnSet.encodeColumns(column.getFirst(), column.getSecond()), Long.toString(ttl));
  }

  /**
   * A convenience method for removing an age off threshold for a column.
   *
   * @param is
   *          IteratorSetting object to configure.
   * @param column
   *          column to encode as a parameter name.
   */
  public static void removeTTL(IteratorSetting is, IteratorSetting.Column column) {
    is.removeOption(ColumnSet.encodeColumns(column.getFirst(), column.getSecond()));
  }
}

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

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.Filter;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;

public class ColumnSliceFilter extends Filter {
  public static final String START_BOUND = "startBound";
  public static final String START_INCLUSIVE = "startInclusive";
  public static final String END_BOUND = "endBound";
  public static final String END_INCLUSIVE = "endInclusive";

  private String startBound;
  private String endBound;
  private boolean startInclusive;
  private boolean endInclusive;

  @Override
  public boolean accept(Key key, Value value) {
    String colQ = key.getColumnQualifier().toString();
    return (startBound == null || (startInclusive ? (colQ.compareTo(startBound) >= 0) : (colQ.compareTo(startBound) > 0)))
        && (endBound == null || (endInclusive ? (colQ.compareTo(endBound) <= 0) : (colQ.compareTo(endBound) < 0)));
  }

  @Override
  public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options, IteratorEnvironment env) throws IOException {
    super.init(source, options, env);
    if (options.containsKey(START_BOUND)) {
      startBound = options.get(START_BOUND);
    } else {
      startBound = null;
    }

    if (options.containsKey(START_INCLUSIVE)) {
      startInclusive = Boolean.parseBoolean(options.get(START_INCLUSIVE));
    } else {
      startInclusive = true;
    }

    if (options.containsKey(END_BOUND)) {
      endBound = options.get(END_BOUND);
    } else {
      endBound = null;
    }

    if (options.containsKey(END_INCLUSIVE)) {
      endInclusive = Boolean.parseBoolean(options.get(END_INCLUSIVE));
    } else {
      endInclusive = false;
    }
  }

  @Override
  public IteratorOptions describeOptions() {
    IteratorOptions io = super.describeOptions();
    io.setName("columnSlice");
    io.setDescription("The ColumnSliceFilter/Iterator allows you to filter for key/value pairs based on a lexicographic range of column qualifier names");
    io.addNamedOption(START_BOUND, "start string in slice");
    io.addNamedOption(END_BOUND, "end string in slice");
    io.addNamedOption(START_INCLUSIVE, "include the start bound in the result set");
    io.addNamedOption(END_INCLUSIVE, "include the end bound in the result set");
    return io;
  }

  public static void setSlice(IteratorSetting si, String start, String end) {
    setSlice(si, start, true, end, false);
  }

  public static void setSlice(IteratorSetting si, String start, boolean startInclusive, String end, boolean endInclusive) {
    if (start != null && end != null && (start.compareTo(end) > 0 || (start.compareTo(end) == 0 && (!startInclusive || !endInclusive)))) {
      throw new IllegalArgumentException("Start key must be less than end key or equal with both sides inclusive in range (" + start + ", " + end + ")");
    }

    if (start != null) {
      si.addOption(START_BOUND, start);
    }
    if (end != null) {
      si.addOption(END_BOUND, end);
    }
    si.addOption(START_INCLUSIVE, String.valueOf(startInclusive));
    si.addOption(END_INCLUSIVE, String.valueOf(endInclusive));
  }

  @Override
  public SortedKeyValueIterator<Key,Value> deepCopy(IteratorEnvironment env) {
    ColumnSliceFilter result = (ColumnSliceFilter) super.deepCopy(env);
    result.startBound = startBound;
    result.startInclusive = startInclusive;
    result.endBound = endBound;
    result.endInclusive = endInclusive;
    return result;
  }
}

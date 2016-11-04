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

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;

/**
 * Does not show contents from scan, only displays statistics. Beware that this work is being done client side and this was developed as a utility for
 * debugging. If used on large result sets it will likely fail.
 */
public class StatisticsDisplayFormatter extends AggregatingFormatter {
  private Map<String,Long> classifications = new HashMap<>();
  private Map<String,Long> columnFamilies = new HashMap<>();
  private Map<String,Long> columnQualifiers = new HashMap<>();
  private long total = 0;

  @Override
  protected void aggregateStats(Entry<Key,Value> entry) {
    String key;
    Long count;

    key = entry.getKey().getColumnVisibility().toString();
    count = classifications.get(key);
    classifications.put(key, count != null ? count + 1 : 0L);

    key = entry.getKey().getColumnFamily().toString();
    count = columnFamilies.get(key);
    columnFamilies.put(key, count != null ? count + 1 : 0L);

    key = entry.getKey().getColumnQualifier().toString();
    count = columnQualifiers.get(key);
    columnQualifiers.put(key, count != null ? count + 1 : 0L);

    ++total;
  }

  @Override
  protected String getStats() {
    StringBuilder buf = new StringBuilder();
    buf.append("CLASSIFICATIONS:\n");
    buf.append("----------------\n");
    for (String key : classifications.keySet())
      buf.append("\t").append(key).append(": ").append(classifications.get(key)).append("\n");
    buf.append("COLUMN FAMILIES:\n");
    buf.append("----------------\n");
    for (String key : columnFamilies.keySet())
      buf.append("\t").append(key).append(": ").append(columnFamilies.get(key)).append("\n");
    buf.append("COLUMN QUALIFIERS:\n");
    buf.append("------------------\n");
    for (String key : columnQualifiers.keySet())
      buf.append("\t").append(key).append(": ").append(columnQualifiers.get(key)).append("\n");

    buf.append(total).append(" entries matched.");
    total = 0;
    classifications = new HashMap<>();
    columnFamilies = new HashMap<>();
    columnQualifiers = new HashMap<>();

    return buf.toString();
  }
}

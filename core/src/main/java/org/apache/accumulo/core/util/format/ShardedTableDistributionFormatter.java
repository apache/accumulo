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
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;

/**
 * Formats the rows in a METADATA table scan to show distribution of shards over servers per day. This can be used to determine the effectiveness of the
 * ShardedTableLoadBalancer
 *
 * Use this formatter with the following scan command in the shell:
 *
 * scan -b tableId -c ~tab:loc
 */
public class ShardedTableDistributionFormatter extends AggregatingFormatter {

  private Map<String,HashSet<String>> countsByDay = new HashMap<String,HashSet<String>>();

  @Override
  protected void aggregateStats(Entry<Key,Value> entry) {
    if (entry.getKey().getColumnFamily().toString().equals("~tab") && entry.getKey().getColumnQualifier().toString().equals("loc")) {
      // The row for the sharded table should look like: <tableId>;yyyyMMhh_N
      String row = entry.getKey().getRow().toString();
      // Parse the day out of the row
      int semicolon = row.indexOf(";");
      String day = null;
      if (semicolon != -1) {
        semicolon++;
        day = row.substring(semicolon, semicolon + 8);
      } else
        day = "NULL    ";
      String server = entry.getValue().toString();
      if (countsByDay.get(day) == null)
        countsByDay.put(day, new HashSet<String>());
      countsByDay.get(day).add(server);
    }
  }

  @Override
  protected String getStats() {
    StringBuilder buf = new StringBuilder();
    buf.append("DAY   \t\tSERVERS\n");
    buf.append("------\t\t-------\n");
    for (String day : countsByDay.keySet())
      buf.append(day + "\t\t" + countsByDay.get(day).size() + "\n");
    return buf.toString();
  }

}

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
package org.apache.accumulo.test.randomwalk.concurrent;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;

import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.util.MetadataTable;
import org.apache.accumulo.test.randomwalk.State;
import org.apache.accumulo.test.randomwalk.Test;

/**
 * 
 */
public class CheckBalance extends Test {
  
  private static final String LAST_UNBALANCED_TIME = "lastUnbalancedTime";
  private static final String UNBALANCED_COUNT = "unbalancedCount";
  
  @Override
  public void visit(State state, Properties props) throws Exception {
    log.debug("checking balance");
    Map<String,Long> counts = new HashMap<String,Long>();
    Scanner scanner = state.getConnector().createScanner(MetadataTable.NAME, Authorizations.EMPTY);
    scanner.fetchColumnFamily(MetadataTable.CURRENT_LOCATION_COLUMN_FAMILY);
    for (Entry<Key,Value> entry : scanner) {
      String location = entry.getKey().getColumnQualifier().toString();
      Long count = counts.get(location);
      if (count == null)
        count = new Long(0);
      counts.put(location, count + 1);
    }
    double total = 0.;
    for (Long count : counts.values()) {
      total += count.longValue();
    }
    final double average = total / counts.size();
    
    // Check for even # of tablets on each node
    boolean balanced = true;
    for (Entry<String,Long> entry : counts.entrySet()) {
      if (Math.abs(entry.getValue().longValue() - average) > Math.max(1, average / 5)) {
        balanced = false;
        log.debug("unbalanced: " + entry.getKey() + " has " + entry.getValue() + " tablets and the average is " + average);
      }
    }
    
    // It is expected that the number of tablets will be uneven for short
    // periods of time. Don't complain unless we've seen it only unbalanced
    // over a 15 minute period and it's been at least three checks.
    if (!balanced) {
      String last = props.getProperty(LAST_UNBALANCED_TIME);
      if (last != null && System.currentTimeMillis() - Long.parseLong(last) > 15 * 60 * 1000) {
        String countString = props.getProperty(UNBALANCED_COUNT, "0");
        int count = Integer.parseInt(countString);
        if (count > 3)
          throw new Exception("servers are unbalanced!");
        count++;
        props.setProperty(UNBALANCED_COUNT, "" + count);
      }
      if (last == null)
        props.setProperty(LAST_UNBALANCED_TIME, Long.toString(System.currentTimeMillis()));
    } else {
      props.remove(LAST_UNBALANCED_TIME);
      props.remove(UNBALANCED_COUNT);
    }
  }
  
}

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

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;

import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.test.randomwalk.State;
import org.apache.accumulo.test.randomwalk.Test;

/**
 *
 */
public class CheckBalance extends Test {

  static final String LAST_UNBALANCED_TIME = "lastUnbalancedTime";
  static final String UNBALANCED_COUNT = "unbalancedCount";

  @Override
  public void visit(State state, Properties props) throws Exception {
    log.debug("checking balance");
    Map<String,Long> counts = new HashMap<String,Long>();
    Scanner scanner = state.getConnector().createScanner(MetadataTable.NAME, Authorizations.EMPTY);
    scanner.fetchColumnFamily(TabletsSection.CurrentLocationColumnFamily.NAME);
    for (Entry<Key,Value> entry : scanner) {
      String location = entry.getKey().getColumnQualifier().toString();
      Long count = counts.get(location);
      if (count == null)
        count = Long.valueOf(0);
      counts.put(location, count + 1);
    }
    double total = 0.;
    for (Long count : counts.values()) {
      total += count.longValue();
    }
    final double average = total / counts.size();
    final double sd = stddev(counts.values(), average);
    log.debug("average " + average + ", standard deviation " + sd);

    // Check for balanced # of tablets on each node
    double maxDifference = 2.0 * sd;
    String unbalancedLocation = null;
    long lastCount = 0L;
    boolean balanced = true;
    for (Entry<String,Long> entry : counts.entrySet()) {
      long thisCount = entry.getValue().longValue();
      if (Math.abs(thisCount - average) > maxDifference) {
        balanced = false;
        log.debug("unbalanced: " + entry.getKey() + " has " + entry.getValue() + " tablets and the average is " + average);
        unbalancedLocation = entry.getKey();
        lastCount = thisCount;
      }
    }

    // It is expected that the number of tablets will be uneven for short
    // periods of time. Don't complain unless we've seen it only unbalanced
    // over a 15 minute period and it's been at least three checks.
    if (!balanced) {
      Long last = state.getLong(LAST_UNBALANCED_TIME);
      if (last != null && System.currentTimeMillis() - last > 15 * 60 * 1000) {
        Integer count = state.getInteger(UNBALANCED_COUNT);
        if (count == null)
          count = Integer.valueOf(0);
        if (count > 3)
          throw new Exception("servers are unbalanced! location " + unbalancedLocation + " count " + lastCount + " too far from average " + average);
        count++;
        state.set(UNBALANCED_COUNT, count);
      }
      if (last == null)
        state.set(LAST_UNBALANCED_TIME, System.currentTimeMillis());
    } else {
      state.remove(LAST_UNBALANCED_TIME);
      state.remove(UNBALANCED_COUNT);
    }
  }

  private static double stddev(Collection<Long> samples, double avg) {
    int num = samples.size();
    double sqrtotal = 0.0;
    for (Long s : samples) {
      double diff = s.doubleValue() - avg;
      sqrtotal += diff * diff;
    }
    return Math.sqrt(sqrtotal / num);
  }
}

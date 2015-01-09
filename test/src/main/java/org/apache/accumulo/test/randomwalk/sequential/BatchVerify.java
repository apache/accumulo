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
package org.apache.accumulo.test.randomwalk.sequential;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Random;

import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.test.randomwalk.State;
import org.apache.accumulo.test.randomwalk.Test;
import org.apache.hadoop.io.Text;

public class BatchVerify extends Test {

  @Override
  public void visit(State state, Properties props) throws Exception {

    Random rand = new Random();

    long numWrites = state.getLong("numWrites");
    int maxVerify = Integer.parseInt(props.getProperty("maxVerify", "2000"));
    long numVerify = rand.nextInt(maxVerify - 1) + 1;

    if (numVerify > (numWrites / 4)) {
      numVerify = numWrites / 4;
    }

    Connector conn = state.getConnector();
    BatchScanner scanner = conn.createBatchScanner(state.getString("seqTableName"), new Authorizations(), 2);

    try {
      int count = 0;
      List<Range> ranges = new ArrayList<Range>();
      while (count < numVerify) {
        long rangeStart = rand.nextInt((int) numWrites);
        long rangeEnd = rangeStart + 99;
        if (rangeEnd > (numWrites - 1)) {
          rangeEnd = numWrites - 1;
        }
        count += rangeEnd - rangeStart + 1;
        ranges.add(new Range(new Text(String.format("%010d", rangeStart)), new Text(String.format("%010d", rangeEnd))));
      }

      ranges = Range.mergeOverlapping(ranges);
      if (ranges.size() > 1) {
        Collections.sort(ranges);
      }

      if (count == 0 || ranges.size() == 0)
        return;

      log.debug(String.format("scanning %d rows in the following %d ranges:", count, ranges.size()));
      for (Range r : ranges) {
        log.debug(r);
      }

      scanner.setRanges(ranges);

      List<Key> keys = new ArrayList<Key>();
      for (Entry<Key,Value> entry : scanner) {
        keys.add(entry.getKey());
      }

      log.debug("scan returned " + keys.size() + " rows. now verifying...");

      Collections.sort(keys);

      Iterator<Key> iterator = keys.iterator();
      int curKey = Integer.parseInt(iterator.next().getRow().toString());
      boolean done = false;
      for (Range r : ranges) {
        int start = Integer.parseInt(r.getStartKey().getRow().toString());
        int end = Integer.parseInt(String.copyValueOf(r.getEndKey().getRow().toString().toCharArray(), 0, 10));
        for (int i = start; i <= end; i++) {

          if (done) {
            log.error("missing key " + i);
            break;
          }

          while (curKey < i) {
            log.error("extra key " + curKey);
            if (iterator.hasNext() == false) {
              done = true;
              break;
            }
            curKey = Integer.parseInt(iterator.next().getRow().toString());
          }

          if (curKey > i) {
            log.error("missing key " + i);
          }

          if (iterator.hasNext()) {
            curKey = Integer.parseInt(iterator.next().getRow().toString());
          } else {
            done = true;
          }
        }
      }

      log.debug("verify is now complete");
    } finally {
      scanner.close();
    }
  }
}

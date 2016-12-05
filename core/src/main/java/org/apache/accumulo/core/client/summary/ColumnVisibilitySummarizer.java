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

package org.apache.accumulo.core.client.summary;

import java.util.HashMap;
import java.util.Map;

import org.apache.accumulo.core.client.summary.KeyValueSummarizer.SummaryConsumer;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.commons.lang.mutable.MutableLong;

public class ColumnVisibilitySummarizer implements KeyValueSummarizer {

  private static final int MAX = 5000;
  private static final String PREFIX = "cv:";
  private static final String IGNORE_KEY = "ignored";

  // Map used for computing summary incrementally uses ByteSequence for key which is more efficient than converting colvis to String for each Key. The
  // conversion to String is deferred until the summary is requested. This shows how the interface enables users to write efficient summarizers.
  private Map<ByteSequence,MutableLong> summary = new HashMap<>();
  private long ignored = 0;

  @Override
  public String getId() {
    return "accumulo.cvsummary";
  }

  @Override
  public void collect(Key k, Value v) {
    ByteSequence cv = k.getColumnVisibilityData();

    MutableLong ml = summary.get(cv);
    if (ml == null) {
      if (summary.size() >= MAX) {
        // no need to store this counter in the map and get() it... just use instance variable
        ignored++;
      } else {
        // TODO would probably be safest to copy/clone cv
        summary.put(cv, new MutableLong(1));
      }
    } else {
      // using mutable long allows calling put() to be avoided
      ml.increment();
    }
  }

  @Override
  public void summarize(SummaryConsumer sc) {
    summary.forEach((k, v) -> {
      sc.consume(PREFIX + k.toString(), v.longValue());
    });

    sc.consume(IGNORE_KEY, ignored);
  }

  @Override
  public void merge(Map<String,Long> summary1, Map<String,Long> summary2) {
    summary2.forEach((k2, v2) -> {
      Long v1 = summary1.get(k2);
      if (v1 == null) {
        if (summary1.size() > MAX) {
          summary1.merge(IGNORE_KEY, v2, Long::sum);
        } else {
          summary1.put(k2, v2);
        }
      } else {
        summary1.put(k2, v1 + v2);
      }
    });
  }

  @Override
  public void reset() {
    summary.clear();
    ignored = 0;
  }
}

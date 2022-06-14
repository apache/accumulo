/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.accumulo.test.functional;

import org.apache.accumulo.core.client.summary.Summarizer;
import org.apache.accumulo.core.client.summary.SummarizerConfiguration;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;

/**
 * This summarizer collects some very basic statistics about Keys.
 */
public class BasicSummarizer implements Summarizer {

  public static final String DELETES_STAT = "deletes";
  public static final String MIN_TIMESTAMP_STAT = "minTimestamp";
  public static final String MAX_TIMESTAMP_STAT = "maxTimestamp";
  public static final String TOTAL_STAT = "total";

  @Override
  public Collector collector(SummarizerConfiguration sc) {
    return new Collector() {

      private long minStamp = Long.MAX_VALUE;
      private long maxStamp = Long.MIN_VALUE;
      private long deletes = 0;
      private long total = 0;

      @Override
      public void accept(Key k, Value v) {
        if (k.getTimestamp() < minStamp) {
          minStamp = k.getTimestamp();
        }

        if (k.getTimestamp() > maxStamp) {
          maxStamp = k.getTimestamp();
        }

        if (k.isDeleted()) {
          deletes++;
        }

        total++;
      }

      @Override
      public void summarize(StatisticConsumer sc) {
        sc.accept(MIN_TIMESTAMP_STAT, minStamp);
        sc.accept(MAX_TIMESTAMP_STAT, maxStamp);
        sc.accept(DELETES_STAT, deletes);
        sc.accept(TOTAL_STAT, total);
      }
    };
  }

  @Override
  public Combiner combiner(SummarizerConfiguration sc) {
    return (stats1, stats2) -> {
      stats1.merge(DELETES_STAT, stats2.get(DELETES_STAT), Long::sum);
      stats1.merge(TOTAL_STAT, stats2.get(TOTAL_STAT), Long::sum);
      stats1.merge(MIN_TIMESTAMP_STAT, stats2.get(MIN_TIMESTAMP_STAT), Long::min);
      stats1.merge(MAX_TIMESTAMP_STAT, stats2.get(MAX_TIMESTAMP_STAT), Long::max);
    };
  }
}

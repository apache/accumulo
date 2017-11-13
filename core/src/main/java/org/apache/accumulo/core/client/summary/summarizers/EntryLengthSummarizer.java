/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License");you may not use this file except in compliance with
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
package org.apache.accumulo.core.client.summary.summarizers;

import java.math.RoundingMode;
import java.util.Map;

import org.apache.accumulo.core.client.summary.Summarizer;
import org.apache.accumulo.core.client.summary.SummarizerConfiguration;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;

import com.google.common.math.IntMath;

/**
 * Summarizer that computes summary information about field lengths.
 * Specifically key length, row length, family length, qualifier length, visibility length, and value length.
 * Incrementally computes minimum, maximum, count, sum, and log2 histogram of the lengths.
 */
public class EntryLengthSummarizer implements Summarizer {

  /* Helper function that calculates the various statistics that is used for the Collector methods.*/
  private static class LengthStats {
    private long min = Long.MAX_VALUE;
    private long max = Long.MIN_VALUE;
    private long sum = 0;
    private long[] counts = new long[32];

    private void accept(int length) {
      int idx;

      if (length < min) {
        min = length;
      }

      if (length > max) {
        max = length;
      }

      sum += length;

      if (length == 0) {
        idx = 0;
      } else {
        idx = IntMath.log2(length, RoundingMode.HALF_UP);
      }

      counts[idx]++;
    }

    void summarize (String prefix, StatisticConsumer sc) {
      sc.accept(prefix+".min", (min != Long.MAX_VALUE ? min:0));
      sc.accept(prefix+".max", (max != Long.MIN_VALUE ? max:0));
      sc.accept(prefix+".sum", sum);

      for (int i = 0; i < counts.length; i++) {
        if (counts[i] > 0) {
          sc.accept(prefix+".logHist."+i, counts[i]);
        }
      }
    }

  }

  /* Helper function for merging that is used by the Combiner. */
  private static void merge(String prefix, Map<String, Long> stats1, Map<String,Long> stats2 ) {
    if (stats2.containsKey(prefix+".min") && (stats1.containsKey(prefix+".min") == false)) {
      stats1.put(prefix+".min", stats2.get(prefix+".min"));
    } else {
      stats1.merge(prefix+".min", stats2.get(prefix+".min"), Long::min);
    }

    if (stats2.containsKey(prefix+".max") && (stats1.containsKey(prefix+".max") == false)) {
      stats1.put(prefix+".max", stats2.get(prefix+".max"));
    } else {
      stats1.merge(prefix+".max", stats2.get(prefix+".max"), Long::max);
    }

    if (stats2.containsKey(prefix+".sum") && (stats1.containsKey(prefix+".sum") == false)) {
      stats1.put(prefix+".sum", stats2.get(prefix+".sum"));
    } else {
      stats1.merge(prefix+".sum", stats2.get(prefix+".sum"), Long::sum);
    }

    /* i must be less than 32 since counts[] size max is 32. */
    for (int i = 0; i < 32; i++) {
      if (stats1.containsKey(prefix+".logHist."+i) && stats2.containsKey(prefix+".logHist."+i)) {
        stats1.merge(prefix+".logHist."+i, stats2.get(prefix+".logHist."+i), Long::sum);
      }

      if (stats2.containsKey(prefix+".logHist."+i) && (stats1.containsKey(prefix+".logHist."+i) == false)) {
        stats1.put(prefix+".logHist."+i, stats2.get(prefix+".logHist."+i));
      }
    }
  }

  @Override
  public Collector collector(SummarizerConfiguration sc) {
    return new Collector() {

      private LengthStats keyStats = new LengthStats();
      private LengthStats rowStats = new LengthStats();
      private LengthStats familyStats = new LengthStats();
      private LengthStats qualifierStats = new LengthStats();
      private LengthStats visibilityStats = new LengthStats();
      private LengthStats valueStats = new LengthStats();
      private long total = 0;

      @Override
      public void accept(Key k, Value v) {
       keyStats.accept(k.getLength());
       rowStats.accept(k.getRowData().length());
       familyStats.accept(k.getColumnFamilyData().length());
       qualifierStats.accept(k.getColumnQualifierData().length());
       visibilityStats.accept(k.getColumnVisibilityData().length());
       valueStats.accept(v.getSize());
       total++;
      }

      @Override
      public void summarize(StatisticConsumer sc) {
        keyStats.summarize("key", sc);
        rowStats.summarize("row", sc);
        familyStats.summarize("family", sc);
        qualifierStats.summarize("qualifier", sc);
        visibilityStats.summarize("visibility", sc);
        valueStats.summarize("value", sc);
        sc.accept("total", total);
      }
    };
  }

  @Override
  public Combiner combiner(SummarizerConfiguration sc) {
    return (stats1, stats2) -> {
      merge("key", stats1, stats2);
      merge("row", stats1, stats2);
      merge("family", stats1, stats2);
      merge("qualifier", stats1, stats2);
      merge("visibility", stats1, stats2);
      merge("value", stats1, stats2);
      stats1.merge("total", stats2.get("total"), Long::sum);
    };
  }
}
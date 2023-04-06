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
package org.apache.accumulo.core.client.summary.summarizers;

import java.math.RoundingMode;
import java.util.Map;
import java.util.function.BiFunction;

import org.apache.accumulo.core.client.summary.Summarizer;
import org.apache.accumulo.core.client.summary.SummarizerConfiguration;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;

import com.google.common.math.IntMath;

/**
 * Summarizer that computes summary information about field lengths. Specifically key length, row
 * length, family length, qualifier length, visibility length, and value length. Incrementally
 * computes minimum, maximum, count, sum, and log2 histogram of the lengths.
 *
 * @since 2.0.0
 */
public class EntryLengthSummarizer implements Summarizer {

  /*
   * Helper function that calculates the various statistics that is used for the Collector methods.
   */
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

    void summarize(String prefix, StatisticConsumer sc) {
      sc.accept(prefix + ".min", (min != Long.MAX_VALUE ? min : 0));
      sc.accept(prefix + ".max", (max != Long.MIN_VALUE ? max : 0));
      sc.accept(prefix + ".sum", sum);

      for (int i = 0; i < counts.length; i++) {
        if (counts[i] > 0) {
          sc.accept(prefix + ".logHist." + i, counts[i]);
        }
      }
    }

  }

  /* Helper functions for merging that is used by the Combiner. */
  private static void merge(String key, BiFunction<Long,Long,Long> mergeFunc,
      Map<String,Long> stats1, Map<String,Long> stats2) {
    Long mergeVal = stats2.get(key);

    if (mergeVal != null) {
      stats1.merge(key, mergeVal, mergeFunc);
    }
  }

  private static void merge(String prefix, Map<String,Long> stats1, Map<String,Long> stats2) {
    merge(prefix + ".min", Long::min, stats1, stats2);
    merge(prefix + ".max", Long::max, stats1, stats2);
    merge(prefix + ".sum", Long::sum, stats1, stats2);
    for (int i = 0; i < 32; i++) {
      merge(prefix + ".logHist." + i, Long::sum, stats1, stats2);
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

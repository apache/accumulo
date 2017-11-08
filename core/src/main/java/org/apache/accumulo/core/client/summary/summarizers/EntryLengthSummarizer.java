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

  public static final String MIN_KEY_STAT = "minKey";
  public static final String MAX_KEY_STAT = "maxKey";
  public static final String SUM_KEYS_STAT = "sumKeys";
  
  public static final String MIN_ROW_STAT = "minRow";
  public static final String MAX_ROW_STAT = "maxRow";
  public static final String SUM_ROWS_STAT = "sumRows";
  
  public static final String MIN_FAMILY_STAT = "minFamily";
  public static final String MAX_FAMILY_STAT = "maxFamily";
  public static final String SUM_FAMILIES_STAT = "sumFamilies";
  
  public static final String MIN_QUALIFIER_STAT = "minQualifier";
  public static final String MAX_QUALIFIER_STAT = "maxQualifier";
  public static final String SUM_QUALIFIERS_STAT = "sumQualifiers";
  
  public static final String MIN_VISIBILITY_STAT = "minVisibility";
  public static final String MAX_VISIBILITY_STAT = "maxVisibility";
  public static final String SUM_VISIBILITIES_STAT = "sumVisibilities";
  
  public static final String MIN_VALUE_STAT = "minValue";
  public static final String MAX_VALUE_STAT = "maxValue";
  public static final String SUM_VALUES_STAT = "sumValues";
  
  public static final String TOTAL_STAT = "total";// Total number of Keys
  
  @Override
  public Collector collector(SummarizerConfiguration sc) {
    return new Collector() {

      private long minKey = Long.MAX_VALUE;
      private long maxKey = Long.MIN_VALUE;
      private long sumKeys = 0;
      private long[] keyCounts = new long[32];
      
      private long minRow = Long.MAX_VALUE;
      private long maxRow = Long.MIN_VALUE;
      private long sumRows = 0;
      private long[] rowCounts = new long[32];
      
      private long minFamily = Long.MAX_VALUE;
      private long maxFamily = Long.MIN_VALUE;
      private long sumFamilies = 0;
      private long[] familyCounts = new long[32];
      
      private long minQualifier = Long.MAX_VALUE;
      private long maxQualifier = Long.MIN_VALUE;
      private long sumQualifiers = 0;
      private long[] qualifierCounts = new long[32];
      
      private long minVisibility = Long.MAX_VALUE;
      private long maxVisibility = Long.MIN_VALUE;
      private long sumVisibilities = 0;
      private long[] visibilityCounts = new long[32];
      
      private long minValue = Long.MAX_VALUE;
      private long maxValue = Long.MIN_VALUE;
      private long sumValues = 0;
      private long[] valueCounts = new long[32];
      
      private long total = 0;
      
      @Override
      public void accept(Key k, Value v) {
        int idx;
        
        // KEYS
        if (k.getLength() < minKey) {
          minKey = k.getLength();
        }
        
        if (k.getLength() > maxKey) {
          maxKey = k.getLength();
        }
        
        sumKeys += k.getLength();
        
        if (k.getLength() == 0) {
          idx = 0;
        } else {
          idx = IntMath.log2(k.getLength(), RoundingMode.HALF_UP);
        }
        
        keyCounts[idx]++;
        
        // ROWS
        if (k.getRowData().length() < minRow) {
          minRow = k.getRowData().length();
        }
        
        if (k.getRowData().length() > maxRow) {
          maxRow = k.getRowData().length();
        }
        
        sumRows += k.getRowData().length();
        
        if (k.getRowData().length() == 0) {
          idx = 0;
        } else {
          idx = IntMath.log2(k.getRowData().length(), RoundingMode.HALF_UP);
        }
        
        rowCounts[idx]++;
        
        // FAMILIES
        if (k.getColumnFamilyData().length() < minFamily) {
          minFamily = k.getColumnFamilyData().length();
        }
        
        if (k.getColumnFamilyData().length() > maxFamily) {
          maxFamily = k.getColumnFamilyData().length();
        }
        
        sumFamilies += k.getColumnFamilyData().length();
        
        if (k.getColumnFamilyData().length() == 0) {
          idx = 0;
        } else {
          idx = IntMath.log2(k.getColumnFamilyData().length(), RoundingMode.HALF_UP);
        }
        
        familyCounts[idx]++;
        
        // QUALIFIERS
        if (k.getColumnQualifierData().length() < minQualifier) {
          minQualifier = k.getColumnQualifierData().length();
        }
        
        if (k.getColumnQualifierData().length() > maxQualifier) {
          maxQualifier = k.getColumnQualifierData().length();
        }
        
        sumQualifiers += k.getColumnQualifierData().length();
        
        if (k.getColumnQualifierData().length() == 0) {
          idx = 0;
        } else {
          idx = IntMath.log2(k.getColumnQualifierData().length(), RoundingMode.HALF_UP);
        }
        
        qualifierCounts[idx]++;
        
        // VISIBILITIES
        if (k.getColumnVisibilityData().length() < minVisibility) {
          minVisibility = k.getColumnVisibilityData().length();
        }
        
        if (k.getColumnVisibilityData().length() > maxVisibility) {
          maxVisibility = k.getColumnVisibilityData().length();
        }
        
        sumVisibilities += k.getColumnVisibilityData().length();
        
        if (k.getColumnVisibilityData().length() == 0) {
          idx = 0;
        } else {
          idx = IntMath.log2(k.getColumnVisibilityData().length(), RoundingMode.HALF_UP);
        }
        
        visibilityCounts[idx]++;
        
        // VALUES
        if (v.getSize() < minValue) {
          minValue = v.getSize();
        }
        
        if (v.getSize() > maxValue) {
          maxValue = v.getSize();
        }
        
        sumValues += v.getSize();
          
        if (v.getSize() == 0) {
          idx = 0;
        } else {
          idx = IntMath.log2(v.getSize(), RoundingMode.HALF_UP);
        }
        
        valueCounts[idx]++;
        
        // TOTAL
        total++;
      }

      @Override
      public void summarize(StatisticConsumer sc) {
        sc.accept(MIN_KEY_STAT, (minKey != Long.MAX_VALUE ? minKey:0));
        sc.accept(MAX_KEY_STAT, (maxKey != Long.MIN_VALUE ? maxKey:0));
        sc.accept(SUM_KEYS_STAT, sumKeys);
        
        for (int i = 0;i < keyCounts.length;i++) {
          if(keyCounts[i] > 0) {
            sc.accept("key.logHist."+i, keyCounts[i]);
          }
        }
        
        sc.accept(MIN_ROW_STAT, (minRow != Long.MAX_VALUE ? minRow:0));
        sc.accept(MAX_ROW_STAT, (maxRow != Long.MIN_VALUE ? maxRow:0));
        sc.accept(SUM_ROWS_STAT, sumRows);
        
        for (int i = 0;i < rowCounts.length;i++) {
          if(rowCounts[i] > 0) {
            sc.accept("row.logHist."+i, rowCounts[i]);
          }
        }
        
        sc.accept(MIN_FAMILY_STAT, (minFamily != Long.MAX_VALUE ? minFamily:0));
        sc.accept(MAX_FAMILY_STAT, (maxFamily != Long.MIN_VALUE ? maxFamily:0));
        sc.accept(SUM_FAMILIES_STAT, sumFamilies);
        
        for (int i = 0;i < familyCounts.length;i++) {
          if(familyCounts[i] > 0) {
            sc.accept("family.logHist."+i, familyCounts[i]);
          }
        }
        
        sc.accept(MIN_QUALIFIER_STAT, (minQualifier != Long.MAX_VALUE ? minQualifier:0));
        sc.accept(MAX_QUALIFIER_STAT, (maxQualifier != Long.MIN_VALUE ? maxQualifier:0));
        sc.accept(SUM_QUALIFIERS_STAT, sumQualifiers);
        
        for (int i = 0;i < qualifierCounts.length;i++) {
          if(qualifierCounts[i] > 0) {
            sc.accept("qualifier.logHist."+i, qualifierCounts[i]);
          }
        }
        
        sc.accept(MIN_VISIBILITY_STAT, (minVisibility != Long.MAX_VALUE ? minVisibility:0));
        sc.accept(MAX_VISIBILITY_STAT, (maxVisibility != Long.MIN_VALUE ? maxVisibility:0));
        sc.accept(SUM_VISIBILITIES_STAT, sumVisibilities);
        
        for (int i = 0;i < visibilityCounts.length;i++) {
          if(visibilityCounts[i] > 0) {
            sc.accept("visibility.logHist."+i, visibilityCounts[i]);
          }
        }
        
        sc.accept(MIN_VALUE_STAT, (minValue != Long.MAX_VALUE ? minValue:0));
        sc.accept(MAX_VALUE_STAT, (maxValue != Long.MIN_VALUE ? maxValue:0));
        sc.accept(SUM_VALUES_STAT, sumValues);
        
        for (int i = 0;i < valueCounts.length;i++) {
          if(valueCounts[i] > 0) {
            sc.accept("value.logHist."+i, valueCounts[i]);
          }
        }
        
        sc.accept(TOTAL_STAT, total);
      }
      
    };
  }

  @Override
  public Combiner combiner(SummarizerConfiguration sc) {
    return (stats1, stats2) -> {
      stats1.merge(MIN_KEY_STAT, stats2.get(MIN_KEY_STAT), Long::max);
      stats1.merge(MAX_KEY_STAT, stats2.get(MAX_KEY_STAT), Long::max);
      stats1.merge(SUM_KEYS_STAT, stats2.get(SUM_KEYS_STAT), Long::sum);
      // Log2 Histogram for Keys
      stats2.forEach((k,v) -> stats1.merge(k, v, Long::sum));
      
      stats1.merge(MIN_ROW_STAT, stats2.get(MIN_ROW_STAT), Long::max);
      stats1.merge(MAX_ROW_STAT, stats2.get(MAX_ROW_STAT), Long::max);
      stats1.merge(SUM_ROWS_STAT, stats2.get(SUM_ROWS_STAT), Long::sum);
      // Log2 Histogram for Rows
      stats2.forEach((k,v) -> stats1.merge(k, v, Long::sum));
      
      stats1.merge(MIN_FAMILY_STAT, stats2.get(MIN_FAMILY_STAT), Long::max);
      stats1.merge(MAX_FAMILY_STAT, stats2.get(MAX_FAMILY_STAT), Long::max);
      stats1.merge(SUM_FAMILIES_STAT, stats2.get(SUM_FAMILIES_STAT), Long::sum);
      // Log2 Histogram for Families
      stats2.forEach((k,v) -> stats1.merge(k, v, Long::sum));
      
      stats1.merge(MIN_QUALIFIER_STAT, stats2.get(MIN_QUALIFIER_STAT), Long::max);
      stats1.merge(MAX_QUALIFIER_STAT, stats2.get(MAX_QUALIFIER_STAT), Long::max);
      stats1.merge(SUM_QUALIFIERS_STAT, stats2.get(SUM_QUALIFIERS_STAT), Long::sum);
      // Log2 Histogram for Qualifiers
      stats2.forEach((k,v) -> stats1.merge(k, v, Long::sum));
      
      stats1.merge(MIN_VISIBILITY_STAT, stats2.get(MIN_VISIBILITY_STAT), Long::max);
      stats1.merge(MAX_VISIBILITY_STAT, stats2.get(MAX_VISIBILITY_STAT), Long::max);
      stats1.merge(SUM_VISIBILITIES_STAT, stats2.get(SUM_VISIBILITIES_STAT), Long::sum);
      // Log2 Histogram for Visibilities
      stats2.forEach((k,v) -> stats1.merge(k, v, Long::sum));
      
      stats1.merge(MIN_VALUE_STAT, stats2.get(MIN_VALUE_STAT), Long::max);
      stats1.merge(MAX_VALUE_STAT, stats2.get(MAX_VALUE_STAT), Long::max);
      stats1.merge(SUM_VALUES_STAT, stats2.get(SUM_VALUES_STAT), Long::sum);
      // Log2 Histogram for Values
      stats2.forEach((k,v) -> stats1.merge(k, v, Long::sum));

      stats1.merge(TOTAL_STAT, stats2.get(TOTAL_STAT), Long::sum);
    };
  }
}
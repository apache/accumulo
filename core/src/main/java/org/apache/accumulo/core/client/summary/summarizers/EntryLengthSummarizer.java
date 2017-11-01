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
package org.apache.accumulo.core.client.summary.summarizers;

import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.client.summary.Summarizer;
import org.apache.accumulo.core.client.summary.SummarizerConfiguration;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;

/**
 * Summarizer that computes summary information about field lengths. 
 * Specifically key length, row length, family length, qualifier length, visibility length, and value length.
 * Incrementally computes minimum, maximum, count, sum, and log2 histogram of the lengths.
 */
public class EntryLengthSummarizer implements Summarizer {

  public static final String MIN_KEY_STAT = "minKey";
  public static final String MAX_KEY_STAT = "maxKey";
  public static final String SUM_KEYS_STAT = "sumKeys";
  //Log2 Histogram for Keys
  
  public static final String MIN_ROW_STAT = "minRow";
  public static final String MAX_ROW_STAT = "maxRow";
  public static final String SUM_ROWS_STAT = "sumRows";
  //Log2 Histogram for Rows
  
  public static final String MIN_FAMILY_STAT = "minFamily";
  public static final String MAX_FAMILY_STAT = "maxFamily";
  public static final String SUM_FAMILIES_STAT = "sumFamilies";
  //Log2 Histogram for Families
  
  public static final String MIN_QUALIFIER_STAT = "minQualifier";
  public static final String MAX_QUALIFIER_STAT = "maxQualifier";
  public static final String SUM_QUALIFIERS_STAT = "sumQualifiers";
  //Log2 Histogram for Qualifiers
  
  public static final String MIN_VISIBILITY_STAT = "minVisbility";
  public static final String MAX_VISIBILITY_STAT = "maxVisibility";
  public static final String SUM_VISIBILITIES_STAT = "sumVisibilities";
  //Log2 Histogram for Visibilities
  
  public static final String MIN_VALUE_STAT = "minValue";
  public static final String MAX_VALUE_STAT = "maxValue";
  public static final String SUM_VALUES_STAT = "sumValues";
  //Log2 Histogram for Values
  
  public static final String TOTAL_STAT = "total"; //Count
  
  @Override
  public Collector collector(SummarizerConfiguration sc) {
    return new Collector() {

      private long minKey = Long.MIN_VALUE;
      private long maxKey = Long.MAX_VALUE;
      private long sumKeys = 0;
      
      private long minRow = Long.MIN_VALUE;
      private long maxRow = Long.MAX_VALUE;
      private long sumRows = 0;
      
      private long minFamily = Long.MIN_VALUE;
      private long maxFamily = Long.MAX_VALUE;
      private long sumFamilies = 0;
      
      private long minQualifier = Long.MIN_VALUE;
      private long maxQualifier = Long.MAX_VALUE;
      private long sumQualifiers = 0;
      
      private long minVisibility = Long.MIN_VALUE;
      private long maxVisibility = Long.MAX_VALUE;
      private long sumVisibilities = 0;
      
      private long minValue = Long.MIN_VALUE;
      private long maxValue = Long.MAX_VALUE;
      private long sumValues = 0;
      
      private long total = 0;
      
      @Override
      public void accept(Key k, Value v) {
        // Keys
        if (k.getLength() < minKey) {
          minKey = k.getLength();
        }
        
        if (k.getLength() > maxKey) {
          maxKey = k.getLength();
        }
        
        sumKeys += k.getLength();
        
        // Rows
        if (k.getRowData().length() < minRow) {
          minRow = k.getRowData().length();
        }
        
        if (k.getRowData().length() > maxRow) {
          maxRow = k.getRowData().length();
        }
        
        sumRows += k.getRowData().length();
        
        // Families
        if (k.getColumnFamilyData().length() < minFamily) {
          minFamily = k.getColumnFamilyData().length();
        }
        
        if (k.getColumnFamilyData().length() > maxFamily) {
          maxFamily = k.getColumnFamilyData().length();
        }
        
        sumFamilies += k.getColumnFamilyData().length();
        
        // Qualifiers
        if (k.getColumnQualifierData().length() < minQualifier) {
          minQualifier = k.getColumnQualifierData().length();
        }
        
        if (k.getColumnQualifierData().length() > maxQualifier) {
          maxQualifier = k.getColumnQualifierData().length();
        }
        
        sumQualifiers += k.getColumnQualifierData().length();
        
        // Visibilities
        if (k.getColumnVisibilityData().length() < minVisibility) {
          minVisibility = k.getColumnVisibilityData().length();
        }
        
        if (k.getColumnVisibilityData().length() > maxVisibility) {
          maxVisibility = k.getColumnVisibilityData().length();
        }
        
        sumVisibilities += k.getColumnVisibilityData().length();
        
        // Values
        if (v.getSize() < minValue) {
          minValue = v.getSize();
        }
        
        if (v.getSize() > maxValue) {
          maxValue = v.getSize();
        }
        
        sumValues += v.getSize();
        
        // Count
        total++;
      }

      @Override
      public void summarize(StatisticConsumer sc) {
        sc.accept(MIN_KEY_STAT, minKey);
        sc.accept(MAX_KEY_STAT, maxKey);
        sc.accept(SUM_KEYS_STAT, sumKeys);
        
        sc.accept(MIN_ROW_STAT, minRow);
        sc.accept(MAX_ROW_STAT, maxRow);
        sc.accept(SUM_KEYS_STAT, sumRows);
        
        sc.accept(MIN_FAMILY_STAT, minFamily);
        sc.accept(MAX_FAMILY_STAT, minFamily);
        sc.accept(SUM_FAMILIES_STAT, sumFamilies);
        
        sc.accept(MIN_QUALIFIER_STAT, minQualifier);
        sc.accept(MAX_QUALIFIER_STAT, maxQualifier);
        sc.accept(SUM_QUALIFIERS_STAT, sumQualifiers);
        
        sc.accept(MIN_VISIBILITY_STAT, minVisibility);
        sc.accept(MAX_VISIBILITY_STAT, maxVisibility);
        sc.accept(SUM_VISIBILITIES_STAT, sumVisibilities);
        
        sc.accept(MIN_VALUE_STAT, minValue);
        sc.accept(MAX_VALUE_STAT, maxValue);
        sc.accept(SUM_VALUES_STAT, sumValues);
        
        sc.accept(TOTAL_STAT, total);
      }
      
    };
  }

  @Override
  public Combiner combiner(SummarizerConfiguration sc) {
    return (stats1, stats2) -> {
      stats1.merge(MIN_KEY_STAT, stats2.get(MIN_KEY_STAT), Long::min);
      stats1.merge(MAX_KEY_STAT, stats2.get(MAX_KEY_STAT), Long::max);
      stats1.merge(SUM_KEYS_STAT, stats2.get(SUM_KEYS_STAT), Long::sum);
      
      stats1.merge(MIN_ROW_STAT, stats2.get(MIN_ROW_STAT), Long::min);
      stats1.merge(MAX_ROW_STAT, stats2.get(MAX_ROW_STAT), Long::max);
      stats1.merge(SUM_ROWS_STAT, stats2.get(SUM_ROWS_STAT), Long::sum);
      
      stats1.merge(MIN_FAMILY_STAT, stats2.get(MIN_FAMILY_STAT), Long::min);
      stats1.merge(MAX_FAMILY_STAT, stats2.get(MAX_FAMILY_STAT), Long::max);
      stats1.merge(SUM_FAMILIES_STAT, stats2.get(SUM_FAMILIES_STAT), Long::sum);
      
      stats1.merge(MIN_QUALIFIER_STAT, stats2.get(MIN_QUALIFIER_STAT), Long::min);
      stats1.merge(MAX_QUALIFIER_STAT, stats2.get(MAX_QUALIFIER_STAT), Long::max);
      stats1.merge(SUM_QUALIFIERS_STAT, stats2.get(SUM_QUALIFIERS_STAT), Long::sum);
      
      stats1.merge(MIN_VISIBILITY_STAT, stats2.get(MIN_VISIBILITY_STAT), Long::min);
      stats1.merge(MAX_VISIBILITY_STAT, stats2.get(MAX_VISIBILITY_STAT), Long::max);
      stats1.merge(SUM_VISIBILITIES_STAT, stats2.get(SUM_VISIBILITIES_STAT), Long::sum);
      
      stats1.merge(MIN_VALUE_STAT, stats2.get(MIN_VALUE_STAT), Long::min);
      stats1.merge(MAX_VALUE_STAT, stats2.get(MAX_VALUE_STAT), Long::max);
      stats1.merge(SUM_VALUES_STAT, stats2.get(SUM_VALUES_STAT), Long::sum);
      
      stats1.merge(TOTAL_STAT, stats2.get(TOTAL_STAT), Long::sum);
    };
  }
}

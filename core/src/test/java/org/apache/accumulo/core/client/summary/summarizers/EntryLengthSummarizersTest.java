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

import java.util.HashMap;
import org.apache.accumulo.core.client.summary.summarizers.EntryLengthSummarizer;
import org.apache.accumulo.core.client.summary.Summarizer.Collector;
import org.apache.accumulo.core.client.summary.SummarizerConfiguration;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.junit.Assert;
import org.junit.Test;

public class EntryLengthSummarizersTest {
  
  /* Basic Test: Each test adds to the next, all are simple lengths. */
  
  @Test
  public void testBasicRow() {
    SummarizerConfiguration sc = SummarizerConfiguration.builder(EntryLengthSummarizer.class).build();
    EntryLengthSummarizer entrySum = new EntryLengthSummarizer();
    
    Key k1 = new Key("r1");
    Key k2 = new Key("r2");
    Key k3 = new Key("r3");
    
    Collector collector = entrySum.collector(sc);
    collector.accept(k1, new Value(""));
    collector.accept(k2, new Value(""));
    collector.accept(k3, new Value(""));
    
    HashMap<String,Long> stats = new HashMap<>();
    collector.summarize(stats::put);
    
    HashMap<String,Long> expected = new HashMap<>();
    expected.put("minKey", 2L);
    expected.put("maxKey", 2L);
    expected.put("sumKeys", 6L);
    
    // Log2 Histogram
    expected.put("key.logHist.1", 3L);
    
    expected.put("minRow", 2L);
    expected.put("maxRow", 2L);
    expected.put("sumRows", 6L);
    
    // Log2 Histogram
    expected.put("row.logHist.1", 3L);
    
    expected.put("minFamily", 0L);
    expected.put("maxFamily", 0L);
    expected.put("sumFamilies", 0L);
    
    // Log2 Histogram
    expected.put("family.logHist.0", 3L);
    
    expected.put("minQualifier", 0L);
    expected.put("maxQualifier", 0L);
    expected.put("sumQualifiers", 0L);
    
    // Log2 Histogram
    expected.put("qualifier.logHist.0", 3L);
    
    expected.put("minVisibility", 0L);
    expected.put("maxVisibility", 0L);
    expected.put("sumVisibilities", 0L);
    
    // Log2 Histogram
    expected.put("visibility.logHist.0", 3L);
    
    expected.put("minValue", 0L);
    expected.put("maxValue", 0L);
    expected.put("sumValues", 0L);
    
    // Log2 Histogram
    expected.put("value.logHist.0", 3L);
    
    expected.put("total", 3L);
    
    Assert.assertEquals(expected, stats);
  }
  
  @Test
  public void testBasicFamily() {
    SummarizerConfiguration sc = SummarizerConfiguration.builder(EntryLengthSummarizer.class).build();
    EntryLengthSummarizer entrySum = new EntryLengthSummarizer();
    
    Key k1 = new Key("r1", "f1");
    Key k2 = new Key("r2", "f2");
    Key k3 = new Key("r3", "f3");
    
    Collector collector = entrySum.collector(sc);
    collector.accept(k1, new Value(""));
    collector.accept(k2, new Value(""));
    collector.accept(k3, new Value(""));
    
    HashMap<String,Long> stats = new HashMap<>();
    collector.summarize(stats::put);
    
    HashMap<String,Long> expected = new HashMap<>();
    expected.put("minKey", 4L);
    expected.put("maxKey", 4L);
    expected.put("sumKeys", 12L);
    
    // Log2 Histogram
    expected.put("key.logHist.2", 3L);
    
    expected.put("minRow", 2L);
    expected.put("maxRow", 2L);
    expected.put("sumRows", 6L);
    
    // Log2 Histogram
    expected.put("row.logHist.1", 3L);
    
    expected.put("minFamily", 2L);
    expected.put("maxFamily", 2L);
    expected.put("sumFamilies", 6L);
    
    // Log2 Histogram
    expected.put("family.logHist.1", 3L);
    
    expected.put("minQualifier", 0L);
    expected.put("maxQualifier", 0L);
    expected.put("sumQualifiers", 0L);
    
    // Log2 Histogram
    expected.put("qualifier.logHist.0", 3L);
    
    expected.put("minVisibility", 0L);
    expected.put("maxVisibility", 0L);
    expected.put("sumVisibilities", 0L);
    
    // Log2 Histogram
    expected.put("visibility.logHist.0", 3L);
    
    expected.put("minValue", 0L);
    expected.put("maxValue", 0L);
    expected.put("sumValues", 0L);
    
    // Log2 Histogram
    expected.put("value.logHist.0", 3L);
    
    expected.put("total", 3L);
    
    Assert.assertEquals(expected, stats);
  }
  
  @Test
  public void testBasicQualifier() {
    SummarizerConfiguration sc = SummarizerConfiguration.builder(EntryLengthSummarizer.class).build();
    EntryLengthSummarizer entrySum = new EntryLengthSummarizer();
    
    Key k1 = new Key("r1", "f1", "q1");
    Key k2 = new Key("r2", "f2", "q2");
    Key k3 = new Key("r3", "f3", "q3");
    
    Collector collector = entrySum.collector(sc);
    collector.accept(k1, new Value(""));
    collector.accept(k2, new Value(""));
    collector.accept(k3, new Value(""));
    
    HashMap<String,Long> stats = new HashMap<>();
    collector.summarize(stats::put);
    
    HashMap<String,Long> expected = new HashMap<>();
    expected.put("minKey", 6L);
    expected.put("maxKey", 6L);
    expected.put("sumKeys", 18L);
    
    // Log2 Histogram
    expected.put("key.logHist.3", 3L);
    
    expected.put("minRow", 2L);
    expected.put("maxRow", 2L);
    expected.put("sumRows", 6L);
    
    // Log2 Histogram
    expected.put("row.logHist.1", 3L);
    
    expected.put("minFamily", 2L);
    expected.put("maxFamily", 2L);
    expected.put("sumFamilies", 6L);
    
    // Log2 Histogram
    expected.put("family.logHist.1", 3L);
    
    expected.put("minQualifier", 2L);
    expected.put("maxQualifier", 2L);
    expected.put("sumQualifiers", 6L);
    
    // Log2 Histogram
    expected.put("qualifier.logHist.1", 3L);
    
    expected.put("minVisibility", 0L);
    expected.put("maxVisibility", 0L);
    expected.put("sumVisibilities", 0L);
    
    // Log2 Histogram
    expected.put("visibility.logHist.0", 3L);
    
    expected.put("minValue", 0L);
    expected.put("maxValue", 0L);
    expected.put("sumValues", 0L);
    
    // Log2 Histogram
    expected.put("value.logHist.0", 3L);
    
    expected.put("total", 3L);
    
    Assert.assertEquals(expected, stats);
  }
  
  @Test
  public void testBasicVisibility() {
    SummarizerConfiguration sc = SummarizerConfiguration.builder(EntryLengthSummarizer.class).build();
    EntryLengthSummarizer entrySum = new EntryLengthSummarizer();
    
    Key k1 = new Key("r1", "f1", "q1", "v1");
    Key k2 = new Key("r2", "f2", "q2", "v2");
    Key k3 = new Key("r3", "f3", "q3", "v3");
    
    Collector collector = entrySum.collector(sc);
    collector.accept(k1, new Value(""));
    collector.accept(k2, new Value(""));
    collector.accept(k3, new Value(""));
    
    HashMap<String,Long> stats = new HashMap<>();
    collector.summarize(stats::put);
    
    HashMap<String,Long> expected = new HashMap<>();
    expected.put("minKey", 8L);
    expected.put("maxKey", 8L);
    expected.put("sumKeys", 24L);
    
    // Log2 Histogram
    expected.put("key.logHist.3", 3L);
    
    expected.put("minRow", 2L);
    expected.put("maxRow", 2L);
    expected.put("sumRows", 6L);
    
    // Log2 Histogram
    expected.put("row.logHist.1", 3L);
    
    expected.put("minFamily", 2L);
    expected.put("maxFamily", 2L);
    expected.put("sumFamilies", 6L);
    
    // Log2 Histogram
    expected.put("family.logHist.1", 3L);
    
    expected.put("minQualifier", 2L);
    expected.put("maxQualifier", 2L);
    expected.put("sumQualifiers", 6L);
    
    // Log2 Histogram
    expected.put("qualifier.logHist.1", 3L);
    
    expected.put("minVisibility", 2L);
    expected.put("maxVisibility", 2L);
    expected.put("sumVisibilities", 6L);
    
    // Log2 Histogram
    expected.put("visibility.logHist.1", 3L);
    
    expected.put("minValue", 0L);
    expected.put("maxValue", 0L);
    expected.put("sumValues", 0L);
    
    // Log2 Histogram
    expected.put("value.logHist.0", 3L);
    
    expected.put("total", 3L);
    
    Assert.assertEquals(expected, stats);
  }
  
  @Test
  public void testBasicValue() {
    SummarizerConfiguration sc = SummarizerConfiguration.builder(EntryLengthSummarizer.class).build();
    EntryLengthSummarizer entrySum = new EntryLengthSummarizer();
    
    Key k1 = new Key("r1", "f1", "q1", "v1");
    Key k2 = new Key("r2", "f2", "q2", "v2");
    Key k3 = new Key("r3", "f3", "q3", "v3");
    
    Collector collector = entrySum.collector(sc);
    collector.accept(k1, new Value("v1"));
    collector.accept(k2, new Value("v2"));
    collector.accept(k3, new Value("v3"));
    
    HashMap<String,Long> stats = new HashMap<>();
    collector.summarize(stats::put);
    
    HashMap<String,Long> expected = new HashMap<>();
    expected.put("minKey", 8L);
    expected.put("maxKey", 8L);
    expected.put("sumKeys", 24L);
    
    // Log2 Histogram
    expected.put("key.logHist.3", 3L);
    
    expected.put("minRow", 2L);
    expected.put("maxRow", 2L);
    expected.put("sumRows", 6L);
    
    // Log2 Histogram
    expected.put("row.logHist.1", 3L);
    
    expected.put("minFamily", 2L);
    expected.put("maxFamily", 2L);
    expected.put("sumFamilies", 6L);
    
    // Log2 Histogram
    expected.put("family.logHist.1", 3L);
    
    expected.put("minQualifier", 2L);
    expected.put("maxQualifier", 2L);
    expected.put("sumQualifiers", 6L);
    
    // Log2 Histogram
    expected.put("qualifier.logHist.1", 3L);
    
    expected.put("minVisibility", 2L);
    expected.put("maxVisibility", 2L);
    expected.put("sumVisibilities", 6L);
    
    // Log2 Histogram
    expected.put("visibility.logHist.1", 3L);
    
    expected.put("minValue", 2L);
    expected.put("maxValue", 2L);
    expected.put("sumValues", 6L);
    
    // Log2 Histogram
    expected.put("value.logHist.1", 3L);
    
    expected.put("total", 3L);
    
    Assert.assertEquals(expected, stats);
  }
  
  /* Complex Test: Each test adds to the next, all are mixed lengths. */
  
  @Test
  public void testComplexRow() {
    SummarizerConfiguration sc = SummarizerConfiguration.builder(EntryLengthSummarizer.class).build();
    EntryLengthSummarizer entrySum = new EntryLengthSummarizer();
    
    Key k1 = new Key("r1");
    Key k2 = new Key("row2");
    Key k3 = new Key("columnRow3");
    
    Collector collector = entrySum.collector(sc);
    collector.accept(k1, new Value(""));
    collector.accept(k2, new Value(""));
    collector.accept(k3, new Value(""));
    
    HashMap<String,Long> stats = new HashMap<>();
    collector.summarize(stats::put);
    
    HashMap<String,Long> expected = new HashMap<>();
    expected.put("minKey", 2L);
    expected.put("maxKey", 10L);
    expected.put("sumKeys", 16L);
    
    // Log2 Histogram
    expected.put("key.logHist.1", 1L);
    expected.put("key.logHist.2", 1L);
    expected.put("key.logHist.3", 1L);
    
    expected.put("minRow", 2L);
    expected.put("maxRow", 10L);
    expected.put("sumRows", 16L);
    
    // Log2 Histogram
    expected.put("row.logHist.1", 1L);
    expected.put("row.logHist.2", 1L);
    expected.put("row.logHist.3", 1L);
    
    expected.put("minFamily", 0L);
    expected.put("maxFamily", 0L);
    expected.put("sumFamilies", 0L);
    
    // Log2 Histogram
    expected.put("family.logHist.0", 3L);
    
    expected.put("minQualifier", 0L);
    expected.put("maxQualifier", 0L);
    expected.put("sumQualifiers", 0L);
    
    // Log2 Histogram
    expected.put("qualifier.logHist.0", 3L);
    
    expected.put("minVisibility", 0L);
    expected.put("maxVisibility", 0L);
    expected.put("sumVisibilities", 0L);
    
    // Log2 Histogram
    expected.put("visibility.logHist.0", 3L);
    
    expected.put("minValue", 0L);
    expected.put("maxValue", 0L);
    expected.put("sumValues", 0L);
    
    // Log2 Histogram
    expected.put("value.logHist.0", 3L);
    
    expected.put("total", 3L);
    
    Assert.assertEquals(expected, stats);
  }
  
  @Test
  public void testComplexFamily() {
    SummarizerConfiguration sc = SummarizerConfiguration.builder(EntryLengthSummarizer.class).build();
    EntryLengthSummarizer entrySum = new EntryLengthSummarizer();
    
    Key k1 = new Key("r1", "family1");
    Key k2 = new Key("row2", "columnFamily2");
    Key k3 = new Key("columnRow3", "f3");
    
    Collector collector = entrySum.collector(sc);
    collector.accept(k1, new Value(""));
    collector.accept(k2, new Value(""));
    collector.accept(k3, new Value(""));
    
    HashMap<String,Long> stats = new HashMap<>();
    collector.summarize(stats::put);
    
    HashMap<String,Long> expected = new HashMap<>();
    expected.put("minKey", 9L);
    expected.put("maxKey", 17L);
    expected.put("sumKeys", 38L);
    
    // Log2 Histogram
    expected.put("key.logHist.3", 1L);
    expected.put("key.logHist.4", 2L);
    
    expected.put("minRow", 2L);
    expected.put("maxRow", 10L);
    expected.put("sumRows", 16L);
    
    // Log2 Histogram
    expected.put("row.logHist.1", 1L);
    expected.put("row.logHist.2", 1L);
    expected.put("row.logHist.3", 1L);
    
    expected.put("minFamily", 2L);
    expected.put("maxFamily", 13L);
    expected.put("sumFamilies", 22L);
    
    // Log2 Histogram
    expected.put("family.logHist.1", 1L);
    expected.put("family.logHist.3", 1L);
    expected.put("family.logHist.4", 1L);
    
    expected.put("minQualifier", 0L);
    expected.put("maxQualifier", 0L);
    expected.put("sumQualifiers", 0L);
    
    // Log2 Histogram
    expected.put("qualifier.logHist.0", 3L);
    
    expected.put("minVisibility", 0L);
    expected.put("maxVisibility", 0L);
    expected.put("sumVisibilities", 0L);
    
    // Log2 Histogram
    expected.put("visibility.logHist.0", 3L);
    
    expected.put("minValue", 0L);
    expected.put("maxValue", 0L);
    expected.put("sumValues", 0L);
    
    // Log2 Histogram
    expected.put("value.logHist.0", 3L);
    
    expected.put("total", 3L);
    
    Assert.assertEquals(expected, stats);
  }
  
  @Test
  public void testComplexQualifier() {
    SummarizerConfiguration sc = SummarizerConfiguration.builder(EntryLengthSummarizer.class).build();
    EntryLengthSummarizer entrySum = new EntryLengthSummarizer();
    
    Key k1 = new Key("r1", "family1", "columnQualifier1");
    Key k2 = new Key("row2", "columnFamily2", "q2");
    Key k3 = new Key("columnRow3", "f3", "qualifier3");
    
    Collector collector = entrySum.collector(sc);
    collector.accept(k1, new Value(""));
    collector.accept(k2, new Value(""));
    collector.accept(k3, new Value(""));
    
    HashMap<String,Long> stats = new HashMap<>();
    collector.summarize(stats::put);
    
    HashMap<String,Long> expected = new HashMap<>();
    expected.put("minKey", 19L);
    expected.put("maxKey", 25L);
    expected.put("sumKeys", 66L);
    
    // Log2 Histogram
    expected.put("key.logHist.4", 2L);
    expected.put("key.logHist.5", 1L);
    
    expected.put("minRow", 2L);
    expected.put("maxRow", 10L);
    expected.put("sumRows", 16L);
    
    // Log2 Histogram
    expected.put("row.logHist.1", 1L);
    expected.put("row.logHist.2", 1L);
    expected.put("row.logHist.3", 1L);
    
    expected.put("minFamily", 2L);
    expected.put("maxFamily", 13L);
    expected.put("sumFamilies", 22L);
    
    // Log2 Histogram
    expected.put("family.logHist.1", 1L);
    expected.put("family.logHist.3", 1L);
    expected.put("family.logHist.4", 1L);
    
    expected.put("minQualifier", 2L);
    expected.put("maxQualifier", 16L);
    expected.put("sumQualifiers", 28L);
    
    // Log2 Histogram
    expected.put("qualifier.logHist.1", 1L);
    expected.put("qualifier.logHist.3", 1L);
    expected.put("qualifier.logHist.4", 1L);
    
    expected.put("minVisibility", 0L);
    expected.put("maxVisibility", 0L);
    expected.put("sumVisibilities", 0L);
    
    // Log2 Histogram
    expected.put("visibility.logHist.0", 3L);
    
    expected.put("minValue", 0L);
    expected.put("maxValue", 0L);
    expected.put("sumValues", 0L);
    
    // Log2 Histogram
    expected.put("value.logHist.0", 3L);
    
    expected.put("total", 3L);
    
    Assert.assertEquals(expected, stats);
  }
  
  @Test
  public void testComplexVisibility() {
    SummarizerConfiguration sc = SummarizerConfiguration.builder(EntryLengthSummarizer.class).build();
    EntryLengthSummarizer entrySum = new EntryLengthSummarizer();
    
    Key k1 = new Key("r1", "family1", "columnQualifier1", "v1");
    Key k2 = new Key("row2", "columnFamily2", "q2", "visibility2");
    Key k3 = new Key("columnRow3", "f3", "qualifier3", "columnVisibility3");
    
    Collector collector = entrySum.collector(sc);
    collector.accept(k1, new Value(""));
    collector.accept(k2, new Value(""));
    collector.accept(k3, new Value(""));
    
    HashMap<String,Long> stats = new HashMap<>();
    collector.summarize(stats::put);
    
    HashMap<String,Long> expected = new HashMap<>();
    expected.put("minKey", 27L);
    expected.put("maxKey", 39L);
    expected.put("sumKeys", 96L);
    
    // Log2 Histogram
    expected.put("key.logHist.5", 3L);
    
    expected.put("minRow", 2L);
    expected.put("maxRow", 10L);
    expected.put("sumRows", 16L);
    
    // Log2 Histogram
    expected.put("row.logHist.1", 1L);
    expected.put("row.logHist.2", 1L);
    expected.put("row.logHist.3", 1L);
    
    expected.put("minFamily", 2L);
    expected.put("maxFamily", 13L);
    expected.put("sumFamilies", 22L);
    
    // Log2 Histogram
    expected.put("family.logHist.1", 1L);
    expected.put("family.logHist.3", 1L);
    expected.put("family.logHist.4", 1L);
    
    expected.put("minQualifier", 2L);
    expected.put("maxQualifier", 16L);
    expected.put("sumQualifiers", 28L);
    
    // Log2 Histogram
    expected.put("qualifier.logHist.1", 1L);
    expected.put("qualifier.logHist.3", 1L);
    expected.put("qualifier.logHist.4", 1L);
    
    expected.put("minVisibility", 2L);
    expected.put("maxVisibility", 17L);
    expected.put("sumVisibilities", 30L);
    
    // Log2 Histogram
    expected.put("visibility.logHist.1", 1L);
    expected.put("visibility.logHist.3", 1L);
    expected.put("visibility.logHist.4", 1L);
    
    expected.put("minValue", 0L);
    expected.put("maxValue", 0L);
    expected.put("sumValues", 0L);
    
    // Log2 Histogram
    expected.put("value.logHist.0", 3L);
    
    expected.put("total", 3L);
    
    Assert.assertEquals(expected, stats);
  }
  
  @Test
  public void testComplexValue() {
    SummarizerConfiguration sc = SummarizerConfiguration.builder(EntryLengthSummarizer.class).build();
    EntryLengthSummarizer entrySum = new EntryLengthSummarizer();
    
    Key k1 = new Key("r1", "family1", "columnQualifier1", "v1");
    Key k2 = new Key("row2", "columnFamily2", "q2", "visibility2");
    Key k3 = new Key("columnRow3", "f3", "qualifier3", "columnVisibility3");
    
    Collector collector = entrySum.collector(sc);
    collector.accept(k1, new Value("v1"));
    collector.accept(k2, new Value("value2"));
    collector.accept(k3, new Value("keyValue3"));
    
    HashMap<String,Long> stats = new HashMap<>();
    collector.summarize(stats::put);
    
    HashMap<String,Long> expected = new HashMap<>();
    expected.put("minKey", 27L);
    expected.put("maxKey", 39L);
    expected.put("sumKeys", 96L);
    
    // Log2 Histogram
    expected.put("key.logHist.5", 3L);
    
    expected.put("minRow", 2L);
    expected.put("maxRow", 10L);
    expected.put("sumRows", 16L);
    
    // Log2 Histogram
    expected.put("row.logHist.1", 1L);
    expected.put("row.logHist.2", 1L);
    expected.put("row.logHist.3", 1L);
    
    expected.put("minFamily", 2L);
    expected.put("maxFamily", 13L);
    expected.put("sumFamilies", 22L);
    
    // Log2 Histogram
    expected.put("family.logHist.1", 1L);
    expected.put("family.logHist.3", 1L);
    expected.put("family.logHist.4", 1L);
    
    expected.put("minQualifier", 2L);
    expected.put("maxQualifier", 16L);
    expected.put("sumQualifiers", 28L);
    
    // Log2 Histogram
    expected.put("qualifier.logHist.1", 1L);
    expected.put("qualifier.logHist.3", 1L);
    expected.put("qualifier.logHist.4", 1L);
    
    expected.put("minVisibility", 2L);
    expected.put("maxVisibility", 17L);
    expected.put("sumVisibilities", 30L);
    
    // Log2 Histogram
    expected.put("visibility.logHist.1", 1L);
    expected.put("visibility.logHist.3", 1L);
    expected.put("visibility.logHist.4", 1L);
    
    expected.put("minValue", 2L);
    expected.put("maxValue", 9L);
    expected.put("sumValues", 17L);
    
    // Log2 Histogram
    expected.put("value.logHist.1", 1L);
    expected.put("value.logHist.3", 2L);
    
    expected.put("total", 3L);
    
    Assert.assertEquals(expected, stats);
  }
  
  /* Miscellaneous Test */
  
  @Test
  public void testAll() {
    SummarizerConfiguration sc = SummarizerConfiguration.builder(EntryLengthSummarizer.class).build();
    EntryLengthSummarizer entrySum = new EntryLengthSummarizer();
    
    Key k1 = new Key("maximumnoqualifier","f1", "q", "vis1");
    Key k2 = new Key("minkey","fam2", "q2", "visibility2");
    Key k3 = new Key("row3","f3", "qualifier3", "v3");
    Key k4 = new Key("r4", "family4", "qual4", "vis4");
    Key k5 = new Key("fifthrow", "thirdfamily", "q5", "v5");
    Key k6 = new Key("r6", "sixthfamily", "qual6", "visibi6");
    
    Collector collector = entrySum.collector(sc);
    collector.accept(k1, new Value("v1"));
    collector.accept(k2, new Value("value2"));
    collector.accept(k3, new Value("val3"));
    collector.accept(k4, new Value("fourthvalue"));
    collector.accept(k5, new Value(""));
    collector.accept(k6, new Value("value6"));
    
    HashMap<String,Long> stats = new HashMap<>();
    collector.summarize(stats::put);
    
    HashMap<String,Long> expected = new HashMap<>();
    expected.put("minKey", 18L);
    expected.put("maxKey", 25L);
    expected.put("sumKeys", 132L);
    
    // Log2 Histogram
    expected.put("key.logHist.4", 2L);
    expected.put("key.logHist.5", 4L);
    
    expected.put("minRow", 2L);
    expected.put("maxRow", 18L);
    expected.put("sumRows", 40L);
    
    // Log2 Histogram
    expected.put("row.logHist.1", 2L);
    expected.put("row.logHist.2", 1L);
    expected.put("row.logHist.3", 2L);
    expected.put("row.logHist.4", 1L);
    
    expected.put("minFamily", 2L);
    expected.put("maxFamily", 11L);
    expected.put("sumFamilies", 37L);
    
    // Log2 Histogram
    expected.put("family.logHist.1", 2L);
    expected.put("family.logHist.2", 1L);
    expected.put("family.logHist.3", 3L);
    
    expected.put("minQualifier", 1L);
    expected.put("maxQualifier", 10L);
    expected.put("sumQualifiers", 25L);
    
    // Log2 Histogram
    expected.put("qualifier.logHist.0", 1L);
    expected.put("qualifier.logHist.1", 2L);
    expected.put("qualifier.logHist.2", 2L);
    expected.put("qualifier.logHist.3", 1L);
    
    expected.put("minVisibility", 2L);
    expected.put("maxVisibility", 11L);
    expected.put("sumVisibilities", 30L);
    
    // Log2 Histogram
    expected.put("visibility.logHist.1", 2L);
    expected.put("visibility.logHist.2", 2L);
    expected.put("visibility.logHist.3", 2L);
    
    expected.put("minValue", 0L);
    expected.put("maxValue", 11L);
    expected.put("sumValues", 29L);
    
    // Log2 Histogram
    expected.put("value.logHist.0", 1L);
    expected.put("value.logHist.1", 1L);
    expected.put("value.logHist.2", 1L);
    expected.put("value.logHist.3", 3L);
    
    expected.put("total", 6L);
    
    Assert.assertEquals(expected, stats);
  }
  
  @Test
  public void testLog2Histogram() {
    SummarizerConfiguration sc = SummarizerConfiguration.builder(EntryLengthSummarizer.class).build();
    EntryLengthSummarizer entrySum = new EntryLengthSummarizer();
    
    Key k1 = new Key("row1");
    Key k2 = new Key("row2");
    Key k3 = new Key("row3");
    
    Collector collector = entrySum.collector(sc);
    collector.accept(k1, new Value("01"));
    collector.accept(k2, new Value("012345678"));
    collector.accept(k3, new Value("012345679"));
    
    HashMap<String,Long> stats = new HashMap<>();
    collector.summarize(stats::put);
    
    HashMap<String,Long> expected = new HashMap<>();
    expected.put("minKey", 4L);
    expected.put("maxKey", 4L);
    expected.put("sumKeys", 12L);
    
    // Log2 Histogram for Key
    expected.put("key.logHist.2", 3L);
    
    expected.put("minRow", 4L);
    expected.put("maxRow", 4L);
    expected.put("sumRows", 12L);
    
    // Log2 Histogram for Row
    expected.put("row.logHist.2", 3L);
    
    expected.put("minFamily", 0L);
    expected.put("maxFamily", 0L);
    expected.put("sumFamilies", 0L);
    
    // Log2 Histogram for Family
    expected.put("family.logHist.0", 3L);
    
    expected.put("minQualifier", 0L);
    expected.put("maxQualifier", 0L);
    expected.put("sumQualifiers", 0L);
    
    // Log2 Histogram for Qualifier
    expected.put("qualifier.logHist.0", 3L);
    
    expected.put("minVisibility", 0L);
    expected.put("maxVisibility", 0L);
    expected.put("sumVisibilities", 0L);
    
    // Log2 Histogram for Visibility
    expected.put("visibility.logHist.0", 3L);
    
    expected.put("minValue", 2L);
    expected.put("maxValue", 9L);
    expected.put("sumValues", 20L);
    
    // Log2 Histogram for Value
    expected.put("value.logHist.1", 1L);
    expected.put("value.logHist.3", 2L);
    
    expected.put("total", 3L);
    
    Assert.assertEquals(expected, stats);
  }
}

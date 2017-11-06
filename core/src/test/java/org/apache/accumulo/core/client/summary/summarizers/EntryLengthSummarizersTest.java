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


import java.util.Arrays;
import java.util.HashMap;

import org.apache.accumulo.core.client.summary.summarizers.EntryLengthSummarizer;
import org.apache.accumulo.core.client.summary.Summarizer;
import org.apache.accumulo.core.client.summary.Summarizer.Collector;
import org.apache.accumulo.core.client.summary.SummarizerConfiguration;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.junit.Assert;
import org.junit.Test;

public class EntryLengthSummarizersTest {
  
  @Test
  public void testKeyRowFamily() {
    SummarizerConfiguration sc = SummarizerConfiguration.builder(EntryLengthSummarizer.class).build();
    EntryLengthSummarizer entrySum = new EntryLengthSummarizer();
    
    Key k1 = new Key("maximum","f1");
    Key k2 = new Key("min","f2");
    Key k3 = new Key("row3","f3");
    
    Collector collector = entrySum.collector(sc);
    collector.accept(k1, new Value(""));
    collector.accept(k2, new Value(""));
    collector.accept(k3, new Value(""));
    
    HashMap<String,Long> stats = new HashMap<>();
    collector.summarize(stats::put);
    
    HashMap<String,Long> expected = new HashMap<>();
    expected.put("minKey", 5L);
    expected.put("maxKey", 9L);
    expected.put("sumKeys", 20L);
    
    expected.put("minRow", 3L);
    expected.put("maxRow", 7L);
    expected.put("sumRows", 14L);
    
    expected.put("minFamily", 2L);
    expected.put("maxFamily", 2L);
    expected.put("sumFamilies", 6L);
    
    expected.put("minQualifier", 0L);
    expected.put("maxQualifier", 0L);
    expected.put("sumQualifiers", 0L);
    
    expected.put("minVisibility", 0L);
    expected.put("maxVisibility", 0L);
    expected.put("sumVisibilities", 0L);
    
    expected.put("minValue", 0L);
    expected.put("maxValue", 0L);
    expected.put("sumValues", 0L);
    
    expected.put("total", 3L);
    
    Assert.assertEquals(expected, stats);
  }
  
  @Test
  public void testPrevPlusQualifier() {
    SummarizerConfiguration sc = SummarizerConfiguration.builder(EntryLengthSummarizer.class).build();
    EntryLengthSummarizer entrySum = new EntryLengthSummarizer();
    
    Key k1 = new Key("maximumnoqualifier","f1", "q");
    Key k2 = new Key("minkey","fam2", "q2");
    Key k3 = new Key("row3","f3", "qualifier3");
    Key k4 = new Key("r4", "family4", "qual4");
    
    Collector collector = entrySum.collector(sc);
    collector.accept(k1, new Value(""));
    collector.accept(k2, new Value(""));
    collector.accept(k3, new Value(""));
    collector.accept(k4, new Value(""));
    
    HashMap<String,Long> stats = new HashMap<>();
    collector.summarize(stats::put);
    
    HashMap<String,Long> expected = new HashMap<>();
    expected.put("minKey", 12L);
    expected.put("maxKey", 21L);
    expected.put("sumKeys", 63L);
    
    expected.put("minRow", 2L);
    expected.put("maxRow", 18L);
    expected.put("sumRows", 30L);
    
    expected.put("minFamily", 2L);
    expected.put("maxFamily", 7L);
    expected.put("sumFamilies", 15L);
    
    expected.put("minQualifier", 1L);
    expected.put("maxQualifier", 10L);
    expected.put("sumQualifiers", 18L);
    
    expected.put("minVisibility", 0L);
    expected.put("maxVisibility", 0L);
    expected.put("sumVisibilities", 0L);
    
    expected.put("minValue", 0L);
    expected.put("maxValue", 0L);
    expected.put("sumValues", 0L);
    
    expected.put("total", 4L);
    
    Assert.assertEquals(expected, stats);
  }
  
  @Test
  public void testPrevPlusVisibility() {
    SummarizerConfiguration sc = SummarizerConfiguration.builder(EntryLengthSummarizer.class).build();
    EntryLengthSummarizer entrySum = new EntryLengthSummarizer();
    
    Key k1 = new Key("maximumnoqualifier","f1", "q", "vis1");
    Key k2 = new Key("minkey","fam2", "q2", "visibility2");
    Key k3 = new Key("row3","f3", "qualifier3", "v3");
    Key k4 = new Key("r4", "family4", "qual4", "vis4");
    Key k5 = new Key("fifthrow", "thirdfamily", "q5", "v5");
    Key k6 = new Key("r6", "sixthfamily", "qual6", "visibi6");
    
    Collector collector = entrySum.collector(sc);
    collector.accept(k1, new Value(""));
    collector.accept(k2, new Value(""));
    collector.accept(k3, new Value(""));
    collector.accept(k4, new Value(""));
    collector.accept(k5, new Value(""));
    collector.accept(k6, new Value(""));
    
    HashMap<String,Long> stats = new HashMap<>();
    collector.summarize(stats::put);
    
    HashMap<String,Long> expected = new HashMap<>();
    expected.put("minKey", 18L);
    expected.put("maxKey", 25L);
    expected.put("sumKeys", 132L);
    
    expected.put("minRow", 2L);
    expected.put("maxRow", 18L);
    expected.put("sumRows", 40L);
    
    expected.put("minFamily", 2L);
    expected.put("maxFamily", 11L);
    expected.put("sumFamilies", 37L);
    
    expected.put("minQualifier", 1L);
    expected.put("maxQualifier", 10L);
    expected.put("sumQualifiers", 25L);
    
    expected.put("minVisibility", 2L);
    expected.put("maxVisibility", 11L);
    expected.put("sumVisibilities", 30L);
    
    expected.put("minValue", 0L);
    expected.put("maxValue", 0L);
    expected.put("sumValues", 0L);
    
    expected.put("total", 6L);
    
    Assert.assertEquals(expected, stats);
  }
  
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
    
    expected.put("minRow", 2L);
    expected.put("maxRow", 18L);
    expected.put("sumRows", 40L);
    
    expected.put("minFamily", 2L);
    expected.put("maxFamily", 11L);
    expected.put("sumFamilies", 37L);
    
    expected.put("minQualifier", 1L);
    expected.put("maxQualifier", 10L);
    expected.put("sumQualifiers", 25L);
    
    expected.put("minVisibility", 2L);
    expected.put("maxVisibility", 11L);
    expected.put("sumVisibilities", 30L);
    
    expected.put("minValue", 0L);
    expected.put("maxValue", 11L);
    expected.put("sumValues", 29L);
    
    expected.put("total", 6L);
    
    Assert.assertEquals(expected, stats);
  }
}

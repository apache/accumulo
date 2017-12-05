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

import java.util.HashMap;

import org.apache.accumulo.core.client.summary.Summarizer.Collector;
import org.apache.accumulo.core.client.summary.Summarizer.Combiner;
import org.apache.accumulo.core.client.summary.SummarizerConfiguration;
import org.apache.accumulo.core.client.summary.summarizers.EntryLengthSummarizer;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.junit.Assert;
import org.junit.Test;

public class EntryLengthSummarizersTest {

  /* COLLECTOR TEST */
  /* Basic Test: Each test adds to the next, all are simple lengths. */

  @Test
  public void testEmpty() {
    SummarizerConfiguration sc = SummarizerConfiguration.builder(EntryLengthSummarizer.class).build();
    EntryLengthSummarizer entrySum = new EntryLengthSummarizer();

    Collector collector = entrySum.collector(sc);

    HashMap<String,Long> stats = new HashMap<>();
    collector.summarize(stats::put);

    HashMap<String,Long> expected = new HashMap<>();
    expected.put("key.min", 0L);
    expected.put("key.max", 0L);
    expected.put("key.sum", 0L);

    expected.put("row.min", 0L);
    expected.put("row.max", 0L);
    expected.put("row.sum", 0L);

    expected.put("family.min", 0L);
    expected.put("family.max", 0L);
    expected.put("family.sum", 0L);

    expected.put("qualifier.min", 0L);
    expected.put("qualifier.max", 0L);
    expected.put("qualifier.sum", 0L);

    expected.put("visibility.min", 0L);
    expected.put("visibility.max", 0L);
    expected.put("visibility.sum", 0L);

    expected.put("value.min", 0L);
    expected.put("value.max", 0L);
    expected.put("value.sum", 0L);

    expected.put("total", 0L);

    Assert.assertEquals(expected, stats);
  }

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
    expected.put("key.min", 2L);
    expected.put("key.max", 2L);
    expected.put("key.sum", 6L);

    // Log2 Histogram
    expected.put("key.logHist.1", 3L);

    expected.put("row.min", 2L);
    expected.put("row.max", 2L);
    expected.put("row.sum", 6L);

    // Log2 Histogram
    expected.put("row.logHist.1", 3L);

    expected.put("family.min", 0L);
    expected.put("family.max", 0L);
    expected.put("family.sum", 0L);

    // Log2 Histogram
    expected.put("family.logHist.0", 3L);

    expected.put("qualifier.min", 0L);
    expected.put("qualifier.max", 0L);
    expected.put("qualifier.sum", 0L);

    // Log2 Histogram
    expected.put("qualifier.logHist.0", 3L);

    expected.put("visibility.min", 0L);
    expected.put("visibility.max", 0L);
    expected.put("visibility.sum", 0L);

    // Log2 Histogram
    expected.put("visibility.logHist.0", 3L);

    expected.put("value.min", 0L);
    expected.put("value.max", 0L);
    expected.put("value.sum", 0L);

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
    expected.put("key.min", 4L);
    expected.put("key.max", 4L);
    expected.put("key.sum", 12L);

    // Log2 Histogram
    expected.put("key.logHist.2", 3L);

    expected.put("row.min", 2L);
    expected.put("row.max", 2L);
    expected.put("row.sum", 6L);

    // Log2 Histogram
    expected.put("row.logHist.1", 3L);

    expected.put("family.min", 2L);
    expected.put("family.max", 2L);
    expected.put("family.sum", 6L);

    // Log2 Histogram
    expected.put("family.logHist.1", 3L);

    expected.put("qualifier.min", 0L);
    expected.put("qualifier.max", 0L);
    expected.put("qualifier.sum", 0L);

    // Log2 Histogram
    expected.put("qualifier.logHist.0", 3L);

    expected.put("visibility.min", 0L);
    expected.put("visibility.max", 0L);
    expected.put("visibility.sum", 0L);

    // Log2 Histogram
    expected.put("visibility.logHist.0", 3L);

    expected.put("value.min", 0L);
    expected.put("value.max", 0L);
    expected.put("value.sum", 0L);

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
    expected.put("key.min", 6L);
    expected.put("key.max", 6L);
    expected.put("key.sum", 18L);

    // Log2 Histogram
    expected.put("key.logHist.3", 3L);

    expected.put("row.min", 2L);
    expected.put("row.max", 2L);
    expected.put("row.sum", 6L);

    // Log2 Histogram
    expected.put("row.logHist.1", 3L);

    expected.put("family.min", 2L);
    expected.put("family.max", 2L);
    expected.put("family.sum", 6L);

    // Log2 Histogram
    expected.put("family.logHist.1", 3L);

    expected.put("qualifier.min", 2L);
    expected.put("qualifier.max", 2L);
    expected.put("qualifier.sum", 6L);

    // Log2 Histogram
    expected.put("qualifier.logHist.1", 3L);

    expected.put("visibility.min", 0L);
    expected.put("visibility.max", 0L);
    expected.put("visibility.sum", 0L);

    // Log2 Histogram
    expected.put("visibility.logHist.0", 3L);

    expected.put("value.min", 0L);
    expected.put("value.max", 0L);
    expected.put("value.sum", 0L);

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
    expected.put("key.min", 8L);
    expected.put("key.max", 8L);
    expected.put("key.sum", 24L);

    // Log2 Histogram
    expected.put("key.logHist.3", 3L);

    expected.put("row.min", 2L);
    expected.put("row.max", 2L);
    expected.put("row.sum", 6L);

    // Log2 Histogram
    expected.put("row.logHist.1", 3L);

    expected.put("family.min", 2L);
    expected.put("family.max", 2L);
    expected.put("family.sum", 6L);

    // Log2 Histogram
    expected.put("family.logHist.1", 3L);

    expected.put("qualifier.min", 2L);
    expected.put("qualifier.max", 2L);
    expected.put("qualifier.sum", 6L);

    // Log2 Histogram
    expected.put("qualifier.logHist.1", 3L);

    expected.put("visibility.min", 2L);
    expected.put("visibility.max", 2L);
    expected.put("visibility.sum", 6L);

    // Log2 Histogram
    expected.put("visibility.logHist.1", 3L);

    expected.put("value.min", 0L);
    expected.put("value.max", 0L);
    expected.put("value.sum", 0L);

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
    expected.put("key.min", 8L);
    expected.put("key.max", 8L);
    expected.put("key.sum", 24L);

    // Log2 Histogram
    expected.put("key.logHist.3", 3L);

    expected.put("row.min", 2L);
    expected.put("row.max", 2L);
    expected.put("row.sum", 6L);

    // Log2 Histogram
    expected.put("row.logHist.1", 3L);

    expected.put("family.min", 2L);
    expected.put("family.max", 2L);
    expected.put("family.sum", 6L);

    // Log2 Histogram
    expected.put("family.logHist.1", 3L);

    expected.put("qualifier.min", 2L);
    expected.put("qualifier.max", 2L);
    expected.put("qualifier.sum", 6L);

    // Log2 Histogram
    expected.put("qualifier.logHist.1", 3L);

    expected.put("visibility.min", 2L);
    expected.put("visibility.max", 2L);
    expected.put("visibility.sum", 6L);

    // Log2 Histogram
    expected.put("visibility.logHist.1", 3L);

    expected.put("value.min", 2L);
    expected.put("value.max", 2L);
    expected.put("value.sum", 6L);

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
    expected.put("key.min", 2L);
    expected.put("key.max", 10L);
    expected.put("key.sum", 16L);

    // Log2 Histogram
    expected.put("key.logHist.1", 1L);
    expected.put("key.logHist.2", 1L);
    expected.put("key.logHist.3", 1L);

    expected.put("row.min", 2L);
    expected.put("row.max", 10L);
    expected.put("row.sum", 16L);

    // Log2 Histogram
    expected.put("row.logHist.1", 1L);
    expected.put("row.logHist.2", 1L);
    expected.put("row.logHist.3", 1L);

    expected.put("family.min", 0L);
    expected.put("family.max", 0L);
    expected.put("family.sum", 0L);

    // Log2 Histogram
    expected.put("family.logHist.0", 3L);

    expected.put("qualifier.min", 0L);
    expected.put("qualifier.max", 0L);
    expected.put("qualifier.sum", 0L);

    // Log2 Histogram
    expected.put("qualifier.logHist.0", 3L);

    expected.put("visibility.min", 0L);
    expected.put("visibility.max", 0L);
    expected.put("visibility.sum", 0L);

    // Log2 Histogram
    expected.put("visibility.logHist.0", 3L);

    expected.put("value.min", 0L);
    expected.put("value.max", 0L);
    expected.put("value.sum", 0L);

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
    expected.put("key.min", 9L);
    expected.put("key.max", 17L);
    expected.put("key.sum", 38L);

    // Log2 Histogram
    expected.put("key.logHist.3", 1L);
    expected.put("key.logHist.4", 2L);

    expected.put("row.min", 2L);
    expected.put("row.max", 10L);
    expected.put("row.sum", 16L);

    // Log2 Histogram
    expected.put("row.logHist.1", 1L);
    expected.put("row.logHist.2", 1L);
    expected.put("row.logHist.3", 1L);

    expected.put("family.min", 2L);
    expected.put("family.max", 13L);
    expected.put("family.sum", 22L);

    // Log2 Histogram
    expected.put("family.logHist.1", 1L);
    expected.put("family.logHist.3", 1L);
    expected.put("family.logHist.4", 1L);

    expected.put("qualifier.min", 0L);
    expected.put("qualifier.max", 0L);
    expected.put("qualifier.sum", 0L);

    // Log2 Histogram
    expected.put("qualifier.logHist.0", 3L);

    expected.put("visibility.min", 0L);
    expected.put("visibility.max", 0L);
    expected.put("visibility.sum", 0L);

    // Log2 Histogram
    expected.put("visibility.logHist.0", 3L);

    expected.put("value.min", 0L);
    expected.put("value.max", 0L);
    expected.put("value.sum", 0L);

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
    expected.put("key.min", 19L);
    expected.put("key.max", 25L);
    expected.put("key.sum", 66L);

    // Log2 Histogram
    expected.put("key.logHist.4", 2L);
    expected.put("key.logHist.5", 1L);

    expected.put("row.min", 2L);
    expected.put("row.max", 10L);
    expected.put("row.sum", 16L);

    // Log2 Histogram
    expected.put("row.logHist.1", 1L);
    expected.put("row.logHist.2", 1L);
    expected.put("row.logHist.3", 1L);

    expected.put("family.min", 2L);
    expected.put("family.max", 13L);
    expected.put("family.sum", 22L);

    // Log2 Histogram
    expected.put("family.logHist.1", 1L);
    expected.put("family.logHist.3", 1L);
    expected.put("family.logHist.4", 1L);

    expected.put("qualifier.min", 2L);
    expected.put("qualifier.max", 16L);
    expected.put("qualifier.sum", 28L);

    // Log2 Histogram
    expected.put("qualifier.logHist.1", 1L);
    expected.put("qualifier.logHist.3", 1L);
    expected.put("qualifier.logHist.4", 1L);

    expected.put("visibility.min", 0L);
    expected.put("visibility.max", 0L);
    expected.put("visibility.sum", 0L);

    // Log2 Histogram
    expected.put("visibility.logHist.0", 3L);

    expected.put("value.min", 0L);
    expected.put("value.max", 0L);
    expected.put("value.sum", 0L);

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
    expected.put("key.min", 27L);
    expected.put("key.max", 39L);
    expected.put("key.sum", 96L);

    // Log2 Histogram
    expected.put("key.logHist.5", 3L);

    expected.put("row.min", 2L);
    expected.put("row.max", 10L);
    expected.put("row.sum", 16L);

    // Log2 Histogram
    expected.put("row.logHist.1", 1L);
    expected.put("row.logHist.2", 1L);
    expected.put("row.logHist.3", 1L);

    expected.put("family.min", 2L);
    expected.put("family.max", 13L);
    expected.put("family.sum", 22L);

    // Log2 Histogram
    expected.put("family.logHist.1", 1L);
    expected.put("family.logHist.3", 1L);
    expected.put("family.logHist.4", 1L);

    expected.put("qualifier.min", 2L);
    expected.put("qualifier.max", 16L);
    expected.put("qualifier.sum", 28L);

    // Log2 Histogram
    expected.put("qualifier.logHist.1", 1L);
    expected.put("qualifier.logHist.3", 1L);
    expected.put("qualifier.logHist.4", 1L);

    expected.put("visibility.min", 2L);
    expected.put("visibility.max", 17L);
    expected.put("visibility.sum", 30L);

    // Log2 Histogram
    expected.put("visibility.logHist.1", 1L);
    expected.put("visibility.logHist.3", 1L);
    expected.put("visibility.logHist.4", 1L);

    expected.put("value.min", 0L);
    expected.put("value.max", 0L);
    expected.put("value.sum", 0L);

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
    expected.put("key.min", 27L);
    expected.put("key.max", 39L);
    expected.put("key.sum", 96L);

    // Log2 Histogram
    expected.put("key.logHist.5", 3L);

    expected.put("row.min", 2L);
    expected.put("row.max", 10L);
    expected.put("row.sum", 16L);

    // Log2 Histogram
    expected.put("row.logHist.1", 1L);
    expected.put("row.logHist.2", 1L);
    expected.put("row.logHist.3", 1L);

    expected.put("family.min", 2L);
    expected.put("family.max", 13L);
    expected.put("family.sum", 22L);

    // Log2 Histogram
    expected.put("family.logHist.1", 1L);
    expected.put("family.logHist.3", 1L);
    expected.put("family.logHist.4", 1L);

    expected.put("qualifier.min", 2L);
    expected.put("qualifier.max", 16L);
    expected.put("qualifier.sum", 28L);

    // Log2 Histogram
    expected.put("qualifier.logHist.1", 1L);
    expected.put("qualifier.logHist.3", 1L);
    expected.put("qualifier.logHist.4", 1L);

    expected.put("visibility.min", 2L);
    expected.put("visibility.max", 17L);
    expected.put("visibility.sum", 30L);

    // Log2 Histogram
    expected.put("visibility.logHist.1", 1L);
    expected.put("visibility.logHist.3", 1L);
    expected.put("visibility.logHist.4", 1L);

    expected.put("value.min", 2L);
    expected.put("value.max", 9L);
    expected.put("value.sum", 17L);

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
    Key k2 = new Key("minKey","fam2", "q2", "visibility2");
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
    expected.put("key.min", 18L);
    expected.put("key.max", 25L);
    expected.put("key.sum", 132L);

    // Log2 Histogram
    expected.put("key.logHist.4", 2L);
    expected.put("key.logHist.5", 4L);

    expected.put("row.min", 2L);
    expected.put("row.max", 18L);
    expected.put("row.sum", 40L);

    // Log2 Histogram
    expected.put("row.logHist.1", 2L);
    expected.put("row.logHist.2", 1L);
    expected.put("row.logHist.3", 2L);
    expected.put("row.logHist.4", 1L);

    expected.put("family.min", 2L);
    expected.put("family.max", 11L);
    expected.put("family.sum", 37L);

    // Log2 Histogram
    expected.put("family.logHist.1", 2L);
    expected.put("family.logHist.2", 1L);
    expected.put("family.logHist.3", 3L);

    expected.put("qualifier.min", 1L);
    expected.put("qualifier.max", 10L);
    expected.put("qualifier.sum", 25L);

    // Log2 Histogram
    expected.put("qualifier.logHist.0", 1L);
    expected.put("qualifier.logHist.1", 2L);
    expected.put("qualifier.logHist.2", 2L);
    expected.put("qualifier.logHist.3", 1L);

    expected.put("visibility.min", 2L);
    expected.put("visibility.max", 11L);
    expected.put("visibility.sum", 30L);

    // Log2 Histogram
    expected.put("visibility.logHist.1", 2L);
    expected.put("visibility.logHist.2", 2L);
    expected.put("visibility.logHist.3", 2L);

    expected.put("value.min", 0L);
    expected.put("value.max", 11L);
    expected.put("value.sum", 29L);

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
    expected.put("key.min", 4L);
    expected.put("key.max", 4L);
    expected.put("key.sum", 12L);

    // Log2 Histogram for Key
    expected.put("key.logHist.2", 3L);

    expected.put("row.min", 4L);
    expected.put("row.max", 4L);
    expected.put("row.sum", 12L);

    // Log2 Histogram for Row
    expected.put("row.logHist.2", 3L);

    expected.put("family.min", 0L);
    expected.put("family.max", 0L);
    expected.put("family.sum", 0L);

    // Log2 Histogram for Family
    expected.put("family.logHist.0", 3L);

    expected.put("qualifier.min", 0L);
    expected.put("qualifier.max", 0L);
    expected.put("qualifier.sum", 0L);

    // Log2 Histogram for Qualifier
    expected.put("qualifier.logHist.0", 3L);

    expected.put("visibility.min", 0L);
    expected.put("visibility.max", 0L);
    expected.put("visibility.sum", 0L);

    // Log2 Histogram for Visibility
    expected.put("visibility.logHist.0", 3L);

    expected.put("value.min", 2L);
    expected.put("value.max", 9L);
    expected.put("value.sum", 20L);

    // Log2 Histogram for Value
    expected.put("value.logHist.1", 1L);
    expected.put("value.logHist.3", 2L);

    expected.put("total", 3L);

    Assert.assertEquals(expected, stats);
  }

  /* COMBINER TEST */

  @Test
  public void testCombine() {
    SummarizerConfiguration sc = SummarizerConfiguration.builder(EntryLengthSummarizer.class).build();
    EntryLengthSummarizer entrySum = new EntryLengthSummarizer();

    Collector collector1 = entrySum.collector(sc);
    collector1.accept(new Key("1","f1","q1"), new Value("v1"));
    collector1.accept(new Key("1234","f1","q1"), new Value("v111"));
    collector1.accept(new Key("12345678","f1","q1"), new Value("v111111"));

    HashMap<String, Long> stats1 = new HashMap<>();
    collector1.summarize(stats1::put);

    Collector collector2 = entrySum.collector(sc);
    collector2.accept(new Key("5432","f11","q12"), new Value("2"));
    collector2.accept(new Key("12","f11","q1234"), new Value("12"));
    collector2.accept(new Key("12","f11","q11234567"), new Value("4444"));

    HashMap<String, Long> stats2 = new HashMap<>();
    collector2.summarize(stats2::put);

    Combiner combiner = entrySum.combiner(sc);
    combiner.merge(stats1, stats2);

    HashMap<String,Long> expected = new HashMap<>();
    expected.put("key.min", 5L);
    expected.put("key.max", 14L);
    expected.put("key.sum", 59L);

    // Log2 Histogram for Key
    expected.put("key.logHist.2", 1L);
    expected.put("key.logHist.3", 3L);
    expected.put("key.logHist.4", 2L);

    expected.put("row.min", 1L);
    expected.put("row.max", 8L);
    expected.put("row.sum", 21L);

    // Log2 Histogram for Row
    expected.put("row.logHist.0", 1L);
    expected.put("row.logHist.1", 2L);
    expected.put("row.logHist.2", 2L);
    expected.put("row.logHist.3", 1L);

    expected.put("family.min", 2L);
    expected.put("family.max", 3L);
    expected.put("family.sum", 15L);

    // Log2 Histogram for Family
    expected.put("family.logHist.1", 3L);
    expected.put("family.logHist.2", 3L);

    expected.put("qualifier.min", 2L);
    expected.put("qualifier.max", 9L);
    expected.put("qualifier.sum", 23L);

    // Log2 Histogram for Qualifier
    expected.put("qualifier.logHist.1", 3L);
    expected.put("qualifier.logHist.2", 2L);
    expected.put("qualifier.logHist.3", 1L);

    expected.put("visibility.min", 0L);
    expected.put("visibility.max", 0L);
    expected.put("visibility.sum", 0L);

    // Log2 Histogram for Visibility
    expected.put("visibility.logHist.0", 6L);

    expected.put("value.min", 1L);
    expected.put("value.max", 7L);
    expected.put("value.sum", 20L);

    // Log2 Histogram for Value
    expected.put("value.logHist.0", 1L);
    expected.put("value.logHist.1", 2L);
    expected.put("value.logHist.2", 2L);
    expected.put("value.logHist.3", 1L);

    expected.put("total", 6L);

    Assert.assertEquals(expected, stats1);
  }

  @Test
  public void testCombine2() {
    SummarizerConfiguration sc = SummarizerConfiguration.builder(EntryLengthSummarizer.class).build();
    EntryLengthSummarizer entrySum = new EntryLengthSummarizer();

    Collector collector1 = entrySum.collector(sc);
    collector1.accept(new Key("12345678901234567890","f12345","q123456"), new Value("value1234567890"));

    HashMap<String, Long> stats1 = new HashMap<>();
    collector1.summarize(stats1::put);

    Collector collector2 = entrySum.collector(sc);
    collector2.accept(new Key("5432","f11","q12"), new Value("2"));
    collector2.accept(new Key("12","f11","q1234"), new Value("12"));
    collector2.accept(new Key("12","f11","q11234567"), new Value("4444"));

    HashMap<String, Long> stats2 = new HashMap<>();
    collector2.summarize(stats2::put);

    Combiner combiner = entrySum.combiner(sc);
    combiner.merge(stats1, stats2);

    HashMap<String,Long> expected = new HashMap<>();
    expected.put("key.min", 10L);
    expected.put("key.max", 33L);
    expected.put("key.sum", 67L);

    // Log2 Histogram for Key
    expected.put("key.logHist.3", 2L);
    expected.put("key.logHist.4", 1L);
    expected.put("key.logHist.5", 1L);

    expected.put("row.min", 2L);
    expected.put("row.max", 20L);
    expected.put("row.sum", 28L);

    // Log2 Histogram for Row
    expected.put("row.logHist.1", 2L);
    expected.put("row.logHist.2", 1L);
    expected.put("row.logHist.4", 1L);

    expected.put("family.min", 3L);
    expected.put("family.max", 6L);
    expected.put("family.sum", 15L);

    // Log2 Histogram for Family
    expected.put("family.logHist.2", 3L);
    expected.put("family.logHist.3", 1L);

    expected.put("qualifier.min", 3L);
    expected.put("qualifier.max", 9L);
    expected.put("qualifier.sum", 24L);

    // Log2 Histogram for Qualifier
    expected.put("qualifier.logHist.2", 2L);
    expected.put("qualifier.logHist.3", 2L);

    expected.put("visibility.min", 0L);
    expected.put("visibility.max", 0L);
    expected.put("visibility.sum", 0L);

    // Log2 Histogram for Visibility
    expected.put("visibility.logHist.0", 4L);

    expected.put("value.min", 1L);
    expected.put("value.max", 15L);
    expected.put("value.sum", 22L);

    // Log2 Histogram for Value
    expected.put("value.logHist.0", 1L);
    expected.put("value.logHist.1", 1L);
    expected.put("value.logHist.2", 1L);
    expected.put("value.logHist.4", 1L);

    expected.put("total", 4L);

    Assert.assertEquals(expected, stats1);
  }

  @Test
  public void testCombine3() {
    SummarizerConfiguration sc = SummarizerConfiguration.builder(EntryLengthSummarizer.class).build();
    EntryLengthSummarizer entrySum = new EntryLengthSummarizer();

    Collector collector1 = entrySum.collector(sc);
    collector1.accept(new Key("r1","f1"), new Value("v1"));

    HashMap<String, Long> stats1 = new HashMap<>();
    collector1.summarize(stats1::put);

    Collector collector2 = entrySum.collector(sc);
    collector2.accept(new Key("row1","family1","q1"), new Value(""));

    HashMap<String, Long> stats2 = new HashMap<>();
    collector2.summarize(stats2::put);

    Combiner combiner = entrySum.combiner(sc);
    combiner.merge(stats1, stats2);

    HashMap<String,Long> expected = new HashMap<>();
    expected.put("key.min", 4L);
    expected.put("key.max", 13L);
    expected.put("key.sum", 17L);

    // Log2 Histogram for Key
    expected.put("key.logHist.2", 1L);
    expected.put("key.logHist.4", 1L);

    expected.put("row.min", 2L);
    expected.put("row.max", 4L);
    expected.put("row.sum", 6L);

    // Log2 Histogram for Row
    expected.put("row.logHist.1", 1L);
    expected.put("row.logHist.2", 1L);

    expected.put("family.min", 2L);
    expected.put("family.max", 7L);
    expected.put("family.sum", 9L);

    // Log2 Histogram for Family
    expected.put("family.logHist.1", 1L);
    expected.put("family.logHist.3", 1L);

    expected.put("qualifier.min", 0L);
    expected.put("qualifier.max", 2L);
    expected.put("qualifier.sum", 2L);

    // Log2 Histogram for Qualifier
    expected.put("qualifier.logHist.0", 1L);
    expected.put("qualifier.logHist.1", 1L);

    expected.put("visibility.min", 0L);
    expected.put("visibility.max", 0L);
    expected.put("visibility.sum", 0L);

    // Log2 Histogram for Visibility
    expected.put("visibility.logHist.0", 2L);

    expected.put("value.min", 0L);
    expected.put("value.max", 2L);
    expected.put("value.sum", 2L);

    // Log2 Histogram for Value
    expected.put("value.logHist.0", 1L);
    expected.put("value.logHist.1", 1L);

    expected.put("total", 2L);

    Assert.assertEquals(expected, stats1);
  }
}

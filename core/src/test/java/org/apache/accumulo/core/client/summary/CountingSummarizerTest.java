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
package org.apache.accumulo.core.client.summary;

import static org.apache.accumulo.core.client.summary.CountingSummarizer.COUNTER_STAT_PREFIX;
import static org.apache.accumulo.core.client.summary.CountingSummarizer.DELETES_IGNORED_STAT;
import static org.apache.accumulo.core.client.summary.CountingSummarizer.EMITTED_STAT;
import static org.apache.accumulo.core.client.summary.CountingSummarizer.INGNORE_DELETES_OPT;
import static org.apache.accumulo.core.client.summary.CountingSummarizer.MAX_COUNTERS_OPT;
import static org.apache.accumulo.core.client.summary.CountingSummarizer.MAX_COUNTER_LEN_OPT;
import static org.apache.accumulo.core.client.summary.CountingSummarizer.SEEN_STAT;
import static org.apache.accumulo.core.client.summary.CountingSummarizer.TOO_LONG_STAT;
import static org.apache.accumulo.core.client.summary.CountingSummarizer.TOO_MANY_STAT;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Arrays;
import java.util.HashMap;

import org.apache.accumulo.core.client.summary.Summarizer.Collector;
import org.apache.accumulo.core.client.summary.summarizers.FamilySummarizer;
import org.apache.accumulo.core.client.summary.summarizers.VisibilitySummarizer;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.junit.jupiter.api.Test;

public class CountingSummarizerTest {

  public static class MultiSummarizer extends CountingSummarizer<String> {
    @Override
    protected Converter<String> converter() {
      return (k, v, c) -> {
        c.accept("rp:" + k.getRowData().subSequence(0, 2));
        c.accept("fp:" + k.getColumnFamilyData().subSequence(0, 2));
        c.accept("qp:" + k.getColumnQualifierData().subSequence(0, 2));
      };
    }
  }

  public static class ValueSummarizer extends CountingSummarizer<String> {
    @Override
    protected Converter<String> converter() {
      return (k, v, c) -> {
        c.accept("vp:" + v.toString().subSequence(0, 2));
      };
    }
  }

  @Test
  public void testMultipleEmit() {
    SummarizerConfiguration sc = SummarizerConfiguration.builder(MultiSummarizer.class).build();
    MultiSummarizer countSum = new MultiSummarizer();

    Summarizer.Collector collector = countSum.collector(sc);

    Value val = new Value("abc");

    HashMap<String,Long> expected = new HashMap<>();

    for (String row : new String[] {"ask", "asleep", "some", "soul"}) {
      for (String fam : new String[] {"hop", "hope", "nope", "noop"}) {
        for (String qual : new String[] {"mad", "lad", "lab", "map"}) {
          collector.accept(new Key(row, fam, qual), val);

          expected.merge("rp:" + row.substring(0, 2), 1L, Long::sum);
          expected.merge("fp:" + fam.substring(0, 2), 1L, Long::sum);
          expected.merge("qp:" + qual.substring(0, 2), 1L, Long::sum);
        }
      }
    }

    HashMap<String,Long> stats = new HashMap<>();
    collector.summarize(stats::put);

    CounterSummary csum = new CounterSummary(stats);
    assertEquals(expected, csum.getCounters());
    assertEquals(64, csum.getSeen());
    assertEquals(3 * 64, csum.getEmitted());
    assertEquals(0, csum.getIgnored());
    assertEquals(0, csum.getDeletesIgnored());
  }

  @Test
  public void testSummarizing() {
    SummarizerConfiguration sc = SummarizerConfiguration.builder(FamilySummarizer.class)
        .addOptions(MAX_COUNTERS_OPT, "5", MAX_COUNTER_LEN_OPT, "10").build();
    FamilySummarizer countSum = new FamilySummarizer();

    Value val = new Value("abc");

    Summarizer.Collector collector = countSum.collector(sc);
    for (String fam : Arrays.asList("f1", "f1", "f1", "f2", "f1", "f70000000000000000000",
        "f70000000000000000001", "f2", "f3", "f4", "f5", "f6", "f7", "f3", "f7")) {
      collector.accept(new Key("r", fam), val);
    }

    Key dk = new Key("r", "f2");
    dk.setDeleted(true);
    collector.accept(dk, new Value(""));

    HashMap<String,Long> stats = new HashMap<>();
    collector.summarize(stats::put);

    String p = COUNTER_STAT_PREFIX;

    HashMap<String,Long> expected = new HashMap<>();
    expected.put(p + "f1", 4L);
    expected.put(p + "f2", 2L);
    expected.put(p + "f3", 2L);
    expected.put(p + "f4", 1L);
    expected.put(p + "f5", 1L);
    expected.put(TOO_LONG_STAT, 2L);
    expected.put(TOO_MANY_STAT, 3L);
    expected.put(SEEN_STAT, 16L);
    expected.put(EMITTED_STAT, 15L);
    expected.put(DELETES_IGNORED_STAT, 1L);

    assertEquals(expected, stats);

    CounterSummary csum = new CounterSummary(stats);
    assertEquals(5, csum.getIgnored());
    assertEquals(3, csum.getTooMany());
    assertEquals(2, csum.getTooLong());
    assertEquals(16, csum.getSeen());
    assertEquals(15, csum.getEmitted());
    assertEquals(1, csum.getDeletesIgnored());

    expected.clear();
    expected.put("f1", 4L);
    expected.put("f2", 2L);
    expected.put("f3", 2L);
    expected.put("f4", 1L);
    expected.put("f5", 1L);
    assertEquals(expected, csum.getCounters());

  }

  @Test
  public void testMerge() {
    SummarizerConfiguration sc = SummarizerConfiguration.builder(VisibilitySummarizer.class)
        .addOption(MAX_COUNTERS_OPT, "5").build();
    VisibilitySummarizer countSum = new VisibilitySummarizer();

    String p = COUNTER_STAT_PREFIX;

    HashMap<String,Long> sm1 = new HashMap<>();
    sm1.put(p + "f001", 9L);
    sm1.put(p + "f002", 4L);
    sm1.put(p + "f003", 2L);
    sm1.put(p + "f004", 1L);
    sm1.put(p + "f005", 19L);
    sm1.put(EMITTED_STAT, 15L);
    sm1.put(SEEN_STAT, 5L);
    sm1.put(DELETES_IGNORED_STAT, 1L);

    HashMap<String,Long> sm2 = new HashMap<>();
    sm2.put(p + "f001", 1L);
    sm2.put(p + "f002", 2L);
    sm2.put(p + "f00a", 7L);
    sm2.put(p + "f00b", 1L);
    sm2.put(p + "f00c", 17L);
    sm2.put(EMITTED_STAT, 18L);
    sm2.put(SEEN_STAT, 6L);
    sm2.put(DELETES_IGNORED_STAT, 2L);

    countSum.combiner(sc).merge(sm1, sm2);

    HashMap<String,Long> expected = new HashMap<>();
    expected.put(p + "f001", 10L);
    expected.put(p + "f002", 6L);
    expected.put(p + "f005", 19L);
    expected.put(p + "f00a", 7L);
    expected.put(p + "f00c", 17L);
    expected.put(TOO_LONG_STAT, 0L);
    expected.put(TOO_MANY_STAT, 4L);
    expected.put(EMITTED_STAT, 18L + 15L);
    expected.put(SEEN_STAT, 6L + 5L);
    expected.put(DELETES_IGNORED_STAT, 3L);

    assertEquals(expected, sm1);

    sm2.clear();
    sm2.put(p + "f001", 19L);
    sm2.put(p + "f002", 2L);
    sm2.put(p + "f003", 3L);
    sm2.put(p + "f00b", 13L);
    sm2.put(p + "f00c", 2L);
    sm2.put(TOO_LONG_STAT, 1L);
    sm2.put(TOO_MANY_STAT, 3L);
    sm2.put(EMITTED_STAT, 21L);
    sm2.put(SEEN_STAT, 7L);
    sm2.put(DELETES_IGNORED_STAT, 5L);

    countSum.combiner(sc).merge(sm1, sm2);

    expected.clear();
    expected.put(p + "f001", 29L);
    expected.put(p + "f002", 8L);
    expected.put(p + "f005", 19L);
    expected.put(p + "f00b", 13L);
    expected.put(p + "f00c", 19L);
    expected.put(TOO_LONG_STAT, 1L);
    expected.put(TOO_MANY_STAT, 17L);
    expected.put(EMITTED_STAT, 21L + 18 + 15);
    expected.put(SEEN_STAT, 7L + 6 + 5);
    expected.put(DELETES_IGNORED_STAT, 8L);
  }

  @Test
  public void testCountDeletes() {
    SummarizerConfiguration sc = SummarizerConfiguration.builder(FamilySummarizer.class)
        .addOptions(INGNORE_DELETES_OPT, "false").build();
    FamilySummarizer countSum = new FamilySummarizer();

    Key k1 = new Key("r1", "f1");
    Key k2 = new Key("r1", "f1");
    k2.setDeleted(true);
    Key k3 = new Key("r1", "f2");

    Collector collector = countSum.collector(sc);
    collector.accept(k1, new Value(""));
    collector.accept(k2, new Value(""));
    collector.accept(k3, new Value(""));

    String p = COUNTER_STAT_PREFIX;

    HashMap<String,Long> expected = new HashMap<>();
    expected.put(p + "f1", 2L);
    expected.put(p + "f2", 1L);
    expected.put(TOO_LONG_STAT, 0L);
    expected.put(TOO_MANY_STAT, 0L);
    expected.put(SEEN_STAT, 3L);
    expected.put(EMITTED_STAT, 3L);
    expected.put(DELETES_IGNORED_STAT, 0L);

    HashMap<String,Long> stats = new HashMap<>();
    collector.summarize(stats::put);
    assertEquals(expected, stats);

    CounterSummary csum = new CounterSummary(stats);
    assertEquals(0, csum.getIgnored());
    assertEquals(0, csum.getTooMany());
    assertEquals(0, csum.getTooLong());
    assertEquals(3, csum.getSeen());
    assertEquals(3, csum.getEmitted());
    assertEquals(0, csum.getDeletesIgnored());

    expected.clear();
    expected.put("f1", 2L);
    expected.put("f2", 1L);
    assertEquals(expected, csum.getCounters());
  }

  @Test
  public void testConvertValue() {

    SummarizerConfiguration sc = SummarizerConfiguration.builder(ValueSummarizer.class).build();
    ValueSummarizer countSum = new ValueSummarizer();

    Summarizer.Collector collector = countSum.collector(sc);

    HashMap<String,Long> expected = new HashMap<>();

    for (String row : new String[] {"ask", "asleep", "some", "soul"}) {
      for (String fam : new String[] {"hop", "hope", "nope", "noop"}) {
        for (String qual : new String[] {"mad", "lad", "lab", "map"}) {
          for (Value value : new Value[] {new Value("ask"), new Value("asleep"), new Value("some"),
              new Value("soul")}) {
            collector.accept(new Key(row, fam, qual), value);
            expected.merge("vp:" + value.toString().substring(0, 2), 1L, Long::sum);

          }
        }
      }
    }

    HashMap<String,Long> stats = new HashMap<>();
    collector.summarize(stats::put);

    CounterSummary csum = new CounterSummary(stats);
    assertEquals(expected, csum.getCounters());
    assertEquals(256, csum.getSeen());
    assertEquals(256, csum.getEmitted());
    assertEquals(0, csum.getIgnored());
    assertEquals(0, csum.getDeletesIgnored());

  }

}

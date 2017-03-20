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

import static org.apache.accumulo.core.client.summary.CountingSummarizer.COUNTER_STAT_PREFIX;
import static org.apache.accumulo.core.client.summary.CountingSummarizer.DELETES_IGNORED_STAT;
import static org.apache.accumulo.core.client.summary.CountingSummarizer.EMITTED_STAT;
import static org.apache.accumulo.core.client.summary.CountingSummarizer.INGNORE_DELETES_OPT;
import static org.apache.accumulo.core.client.summary.CountingSummarizer.MAX_COUNTERS_OPT;
import static org.apache.accumulo.core.client.summary.CountingSummarizer.MAX_COUNTER_LEN_OPT;
import static org.apache.accumulo.core.client.summary.CountingSummarizer.SEEN_STAT;
import static org.apache.accumulo.core.client.summary.CountingSummarizer.TOO_LONG_STAT;
import static org.apache.accumulo.core.client.summary.CountingSummarizer.TOO_MANY_STAT;

import java.util.Arrays;
import java.util.HashMap;

import org.apache.accumulo.core.client.summary.CounterSummary;
import org.apache.accumulo.core.client.summary.CountingSummarizer;
import org.apache.accumulo.core.client.summary.Summarizer;
import org.apache.accumulo.core.client.summary.Summarizer.Collector;
import org.apache.accumulo.core.client.summary.SummarizerConfiguration;
import org.apache.accumulo.core.client.summary.summarizers.FamilySummarizer;
import org.apache.accumulo.core.client.summary.summarizers.VisibilitySummarizer;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.junit.Assert;
import org.junit.Test;

public class CountingSummarizerTest {

  public static class MultiSummarizer extends CountingSummarizer<String> {
    @Override
    protected Converter<String> converter() {
      return (k, v, c) -> {
        c.accept("rp:" + k.getRowData().subSequence(0, 2).toString());
        c.accept("fp:" + k.getColumnFamilyData().subSequence(0, 2).toString());
        c.accept("qp:" + k.getColumnQualifierData().subSequence(0, 2).toString());
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

          expected.merge("rp:" + row.substring(0, 2), 1l, Long::sum);
          expected.merge("fp:" + fam.substring(0, 2), 1l, Long::sum);
          expected.merge("qp:" + qual.substring(0, 2), 1l, Long::sum);
        }
      }
    }

    HashMap<String,Long> stats = new HashMap<>();
    collector.summarize((k, v) -> stats.put(k, v));

    CounterSummary csum = new CounterSummary(stats);
    Assert.assertEquals(expected, csum.getCounters());
    Assert.assertEquals(64, csum.getSeen());
    Assert.assertEquals(3 * 64, csum.getEmitted());
    Assert.assertEquals(0, csum.getIgnored());
    Assert.assertEquals(0, csum.getDeletesIgnored());
  }

  @Test
  public void testSummarizing() {
    SummarizerConfiguration sc = SummarizerConfiguration.builder(FamilySummarizer.class).addOptions(MAX_COUNTERS_OPT, "5", MAX_COUNTER_LEN_OPT, "10").build();
    FamilySummarizer countSum = new FamilySummarizer();

    Value val = new Value("abc");

    Summarizer.Collector collector = countSum.collector(sc);
    for (String fam : Arrays.asList("f1", "f1", "f1", "f2", "f1", "f70000000000000000000", "f70000000000000000001", "f2", "f3", "f4", "f5", "f6", "f7", "f3",
        "f7")) {
      collector.accept(new Key("r", fam), val);
    }

    Key dk = new Key("r", "f2");
    dk.setDeleted(true);
    collector.accept(dk, new Value(""));

    HashMap<String,Long> stats = new HashMap<>();
    collector.summarize((k, v) -> stats.put(k, v));

    String p = COUNTER_STAT_PREFIX;

    HashMap<String,Long> expected = new HashMap<>();
    expected.put(p + "f1", 4l);
    expected.put(p + "f2", 2l);
    expected.put(p + "f3", 2l);
    expected.put(p + "f4", 1l);
    expected.put(p + "f5", 1l);
    expected.put(TOO_LONG_STAT, 2l);
    expected.put(TOO_MANY_STAT, 3l);
    expected.put(SEEN_STAT, 16l);
    expected.put(EMITTED_STAT, 15l);
    expected.put(DELETES_IGNORED_STAT, 1l);

    Assert.assertEquals(expected, stats);

    CounterSummary csum = new CounterSummary(stats);
    Assert.assertEquals(5, csum.getIgnored());
    Assert.assertEquals(3, csum.getTooMany());
    Assert.assertEquals(2, csum.getTooLong());
    Assert.assertEquals(16, csum.getSeen());
    Assert.assertEquals(15, csum.getEmitted());
    Assert.assertEquals(1, csum.getDeletesIgnored());

    expected.clear();
    expected.put("f1", 4l);
    expected.put("f2", 2l);
    expected.put("f3", 2l);
    expected.put("f4", 1l);
    expected.put("f5", 1l);
    Assert.assertEquals(expected, csum.getCounters());

  }

  @Test
  public void testMerge() {
    SummarizerConfiguration sc = SummarizerConfiguration.builder(VisibilitySummarizer.class).addOption(MAX_COUNTERS_OPT, "5").build();
    VisibilitySummarizer countSum = new VisibilitySummarizer();

    String p = COUNTER_STAT_PREFIX;

    HashMap<String,Long> sm1 = new HashMap<>();
    sm1.put(p + "f001", 9l);
    sm1.put(p + "f002", 4l);
    sm1.put(p + "f003", 2l);
    sm1.put(p + "f004", 1l);
    sm1.put(p + "f005", 19l);
    sm1.put(EMITTED_STAT, 15l);
    sm1.put(SEEN_STAT, 5l);
    sm1.put(DELETES_IGNORED_STAT, 1l);

    HashMap<String,Long> sm2 = new HashMap<>();
    sm2.put(p + "f001", 1l);
    sm2.put(p + "f002", 2l);
    sm2.put(p + "f00a", 7l);
    sm2.put(p + "f00b", 1l);
    sm2.put(p + "f00c", 17l);
    sm2.put(EMITTED_STAT, 18l);
    sm2.put(SEEN_STAT, 6l);
    sm2.put(DELETES_IGNORED_STAT, 2l);

    countSum.combiner(sc).merge(sm1, sm2);

    HashMap<String,Long> expected = new HashMap<>();
    expected.put(p + "f001", 10l);
    expected.put(p + "f002", 6l);
    expected.put(p + "f005", 19l);
    expected.put(p + "f00a", 7l);
    expected.put(p + "f00c", 17l);
    expected.put(TOO_LONG_STAT, 0l);
    expected.put(TOO_MANY_STAT, 4l);
    expected.put(EMITTED_STAT, 18l + 15l);
    expected.put(SEEN_STAT, 6l + 5l);
    expected.put(DELETES_IGNORED_STAT, 3l);

    Assert.assertEquals(expected, sm1);

    sm2.clear();
    sm2.put(p + "f001", 19l);
    sm2.put(p + "f002", 2l);
    sm2.put(p + "f003", 3l);
    sm2.put(p + "f00b", 13l);
    sm2.put(p + "f00c", 2l);
    sm2.put(TOO_LONG_STAT, 1l);
    sm2.put(TOO_MANY_STAT, 3l);
    sm2.put(EMITTED_STAT, 21l);
    sm2.put(SEEN_STAT, 7l);
    sm2.put(DELETES_IGNORED_STAT, 5l);

    countSum.combiner(sc).merge(sm1, sm2);

    expected.clear();
    expected.put(p + "f001", 29l);
    expected.put(p + "f002", 8l);
    expected.put(p + "f005", 19l);
    expected.put(p + "f00b", 13l);
    expected.put(p + "f00c", 19l);
    expected.put(TOO_LONG_STAT, 1l);
    expected.put(TOO_MANY_STAT, 17l);
    expected.put(EMITTED_STAT, 21l + 18 + 15);
    expected.put(SEEN_STAT, 7l + 6 + 5);
    expected.put(DELETES_IGNORED_STAT, 8l);
  }

  @Test
  public void testCountDeletes() {
    SummarizerConfiguration sc = SummarizerConfiguration.builder(FamilySummarizer.class).addOptions(INGNORE_DELETES_OPT, "false").build();
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
    expected.put(p + "f1", 2l);
    expected.put(p + "f2", 1l);
    expected.put(TOO_LONG_STAT, 0l);
    expected.put(TOO_MANY_STAT, 0l);
    expected.put(SEEN_STAT, 3l);
    expected.put(EMITTED_STAT, 3l);
    expected.put(DELETES_IGNORED_STAT, 0l);

    HashMap<String,Long> stats = new HashMap<>();
    collector.summarize(stats::put);
    Assert.assertEquals(expected, stats);

    CounterSummary csum = new CounterSummary(stats);
    Assert.assertEquals(0, csum.getIgnored());
    Assert.assertEquals(0, csum.getTooMany());
    Assert.assertEquals(0, csum.getTooLong());
    Assert.assertEquals(3, csum.getSeen());
    Assert.assertEquals(3, csum.getEmitted());
    Assert.assertEquals(0, csum.getDeletesIgnored());

    expected.clear();
    expected.put("f1", 2l);
    expected.put("f2", 1l);
    Assert.assertEquals(expected, csum.getCounters());
  }
}

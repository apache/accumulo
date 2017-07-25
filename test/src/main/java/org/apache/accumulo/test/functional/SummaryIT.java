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

package org.apache.accumulo.test.functional;

import static java.util.function.Function.identity;
import static java.util.stream.Collectors.counting;
import static java.util.stream.Collectors.groupingBy;
import static org.apache.accumulo.core.client.summary.CountingSummarizer.DELETES_IGNORED_STAT;
import static org.apache.accumulo.core.client.summary.CountingSummarizer.EMITTED_STAT;
import static org.apache.accumulo.core.client.summary.CountingSummarizer.SEEN_STAT;
import static org.apache.accumulo.core.client.summary.CountingSummarizer.TOO_LONG_STAT;
import static org.apache.accumulo.core.client.summary.CountingSummarizer.TOO_MANY_STAT;
import static org.apache.accumulo.test.functional.BasicSummarizer.DELETES_STAT;
import static org.apache.accumulo.test.functional.BasicSummarizer.MAX_TIMESTAMP_STAT;
import static org.apache.accumulo.test.functional.BasicSummarizer.MIN_TIMESTAMP_STAT;
import static org.apache.accumulo.test.functional.BasicSummarizer.TOTAL_STAT;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.LongSummaryStatistics;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.regex.PatternSyntaxException;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.TableOfflineException;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.admin.CompactionConfig;
import org.apache.accumulo.core.client.admin.CompactionStrategyConfig;
import org.apache.accumulo.core.client.admin.NewTableConfiguration;
import org.apache.accumulo.core.client.impl.AccumuloServerException;
import org.apache.accumulo.core.client.security.SecurityErrorCode;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.client.summary.CounterSummary;
import org.apache.accumulo.core.client.summary.Summarizer;
import org.apache.accumulo.core.client.summary.SummarizerConfiguration;
import org.apache.accumulo.core.client.summary.Summary;
import org.apache.accumulo.core.client.summary.Summary.FileStatistics;
import org.apache.accumulo.core.client.summary.summarizers.FamilySummarizer;
import org.apache.accumulo.core.client.summary.summarizers.VisibilitySummarizer;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.Filter;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.TablePermission;
import org.apache.accumulo.fate.util.UtilWaitThread;
import org.apache.accumulo.harness.AccumuloClusterHarness;
import org.apache.accumulo.tserver.compaction.CompactionPlan;
import org.apache.accumulo.tserver.compaction.CompactionStrategy;
import org.apache.accumulo.tserver.compaction.MajorCompactionRequest;
import org.apache.hadoop.io.Text;
import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMap.Builder;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

public class SummaryIT extends AccumuloClusterHarness {

  private LongSummaryStatistics getTimestampStats(final String table, Connector c) throws TableNotFoundException {
    try (Scanner scanner = c.createScanner(table, Authorizations.EMPTY)) {
      Stream<Entry<Key,Value>> stream = StreamSupport.stream(scanner.spliterator(), false);
      LongSummaryStatistics stats = stream.mapToLong(e -> e.getKey().getTimestamp()).summaryStatistics();
      return stats;
    }
  }

  private LongSummaryStatistics getTimestampStats(final String table, Connector c, String startRow, String endRow) throws TableNotFoundException {
    try (Scanner scanner = c.createScanner(table, Authorizations.EMPTY)) {
      scanner.setRange(new Range(startRow, false, endRow, true));
      Stream<Entry<Key,Value>> stream = StreamSupport.stream(scanner.spliterator(), false);
      LongSummaryStatistics stats = stream.mapToLong(e -> e.getKey().getTimestamp()).summaryStatistics();
      return stats;
    }
  }

  private void checkSummaries(Collection<Summary> summaries, SummarizerConfiguration sc, int total, int missing, int extra, Object... kvs) {
    Summary summary = Iterables.getOnlyElement(summaries);
    Assert.assertEquals("total wrong", total, summary.getFileStatistics().getTotal());
    Assert.assertEquals("missing wrong", missing, summary.getFileStatistics().getMissing());
    Assert.assertEquals("extra wrong", extra, summary.getFileStatistics().getExtra());
    Assert.assertEquals("deleted wrong", 0, summary.getFileStatistics().getDeleted());
    Assert.assertEquals(sc, summary.getSummarizerConfiguration());
    Map<String,Long> expected = new HashMap<>();
    for (int i = 0; i < kvs.length; i += 2) {
      expected.put((String) kvs[i], (Long) kvs[i + 1]);
    }
    Assert.assertEquals(expected, summary.getStatistics());
  }

  private void addSplits(final String table, Connector c, String... splits) throws TableNotFoundException, AccumuloException, AccumuloSecurityException {
    c.tableOperations().addSplits(table, new TreeSet<Text>(Lists.transform(Arrays.asList(splits), Text::new)));
  }

  @Test
  public void basicSummaryTest() throws Exception {
    final String table = getUniqueNames(1)[0];
    Connector c = getConnector();
    NewTableConfiguration ntc = new NewTableConfiguration();
    SummarizerConfiguration sc1 = SummarizerConfiguration.builder(BasicSummarizer.class.getName()).build();
    ntc.enableSummarization(sc1);
    c.tableOperations().create(table, ntc);

    BatchWriter bw = writeData(table, c);

    Collection<Summary> summaries = c.tableOperations().summaries(table).flush(false).retrieve();
    Assert.assertEquals(0, summaries.size());

    LongSummaryStatistics stats = getTimestampStats(table, c);

    summaries = c.tableOperations().summaries(table).flush(true).retrieve();
    checkSummaries(summaries, sc1, 1, 0, 0, TOTAL_STAT, 100_000l, MIN_TIMESTAMP_STAT, stats.getMin(), MAX_TIMESTAMP_STAT, stats.getMax(), DELETES_STAT, 0l);

    Mutation m = new Mutation(String.format("r%09x", 999));
    m.put("f1", "q1", "999-0");
    m.putDelete("f1", "q2");
    bw.addMutation(m);
    bw.flush();

    c.tableOperations().flush(table, null, null, true);

    stats = getTimestampStats(table, c);

    summaries = c.tableOperations().summaries(table).retrieve();

    checkSummaries(summaries, sc1, 2, 0, 0, TOTAL_STAT, 100_002l, MIN_TIMESTAMP_STAT, stats.getMin(), MAX_TIMESTAMP_STAT, stats.getMax(), DELETES_STAT, 1l);

    bw.close();

    c.tableOperations().compact(table, new CompactionConfig().setWait(true));

    summaries = c.tableOperations().summaries(table).retrieve();
    checkSummaries(summaries, sc1, 1, 0, 0, TOTAL_STAT, 100_000l, MIN_TIMESTAMP_STAT, stats.getMin(), MAX_TIMESTAMP_STAT, stats.getMax(), DELETES_STAT, 0l);

    // split tablet into two
    String sp1 = String.format("r%09x", 50_000);
    addSplits(table, c, sp1);

    summaries = c.tableOperations().summaries(table).retrieve();

    checkSummaries(summaries, sc1, 1, 0, 0, TOTAL_STAT, 100_000l, MIN_TIMESTAMP_STAT, stats.getMin(), MAX_TIMESTAMP_STAT, stats.getMax(), DELETES_STAT, 0l);

    // compact 2nd tablet
    c.tableOperations().compact(table, new CompactionConfig().setStartRow(new Text(sp1)).setWait(true));

    summaries = c.tableOperations().summaries(table).retrieve();
    checkSummaries(summaries, sc1, 2, 0, 1, TOTAL_STAT, 113_999l, MIN_TIMESTAMP_STAT, stats.getMin(), MAX_TIMESTAMP_STAT, stats.getMax(), DELETES_STAT, 0l);

    // get summaries for first tablet
    stats = getTimestampStats(table, c, sp1, null);
    summaries = c.tableOperations().summaries(table).startRow(sp1).retrieve();
    checkSummaries(summaries, sc1, 1, 0, 0, TOTAL_STAT, 49_999l, MIN_TIMESTAMP_STAT, stats.getMin(), MAX_TIMESTAMP_STAT, stats.getMax(), DELETES_STAT, 0l);

    // compact all tablets and regenerate all summaries
    c.tableOperations().compact(table, new CompactionConfig());

    summaries = c.tableOperations().summaries(table).retrieve();
    stats = getTimestampStats(table, c);
    checkSummaries(summaries, sc1, 2, 0, 0, TOTAL_STAT, 100_000l, MIN_TIMESTAMP_STAT, stats.getMin(), MAX_TIMESTAMP_STAT, stats.getMax(), DELETES_STAT, 0l);

    summaries = c.tableOperations().summaries(table).startRow(String.format("r%09x", 75_000)).endRow(String.format("r%09x", 80_000)).retrieve();
    Summary summary = Iterables.getOnlyElement(summaries);
    Assert.assertEquals(1, summary.getFileStatistics().getTotal());
    Assert.assertEquals(1, summary.getFileStatistics().getExtra());
    long total = summary.getStatistics().get(TOTAL_STAT);
    Assert.assertTrue("Total " + total + " out of expected range", total > 0 && total <= 10_000);

    // test adding and removing
    c.tableOperations().removeSummarizers(table, sc -> sc.getClassName().contains("foo"));

    List<SummarizerConfiguration> summarizers = c.tableOperations().listSummarizers(table);
    Assert.assertEquals(1, summarizers.size());
    Assert.assertTrue(summarizers.contains(sc1));

    c.tableOperations().removeSummarizers(table, sc -> sc.getClassName().equals(BasicSummarizer.class.getName()));
    summarizers = c.tableOperations().listSummarizers(table);
    Assert.assertEquals(0, summarizers.size());

    c.tableOperations().compact(table, new CompactionConfig().setWait(true));

    summaries = c.tableOperations().summaries(table).retrieve();
    Assert.assertEquals(0, summaries.size());

    c.tableOperations().addSummarizers(table, sc1);
    c.tableOperations().compact(table, new CompactionConfig().setWait(true));
    summaries = c.tableOperations().summaries(table).retrieve();
    checkSummaries(summaries, sc1, 2, 0, 0, TOTAL_STAT, 100_000l, MIN_TIMESTAMP_STAT, stats.getMin(), MAX_TIMESTAMP_STAT, stats.getMax(), DELETES_STAT, 0l);
  }

  private BatchWriter writeData(final String table, Connector c) throws TableNotFoundException, MutationsRejectedException {
    BatchWriter bw = c.createBatchWriter(table, new BatchWriterConfig());
    for (int i = 0; i < 100_000; i++) {
      Mutation m = new Mutation(String.format("r%09x", i));
      m.put("f1", "q1", "" + i);
      bw.addMutation(m);
    }
    bw.flush();
    return bw;
  }

  public static class KeySizeSummarizer implements Summarizer {

    @Override
    public Collector collector(SummarizerConfiguration sc) {
      return new Collector() {
        private int maxLen = Integer.parseInt(sc.getOptions().getOrDefault("maxLen", "1024"));
        private long[] lengths = new long[maxLen];
        private long overMax = 0;

        @Override
        public void accept(Key k, Value v) {
          int size = k.getSize();
          if (size >= maxLen) {
            overMax++;
          } else {
            lengths[size]++;
          }
        }

        @Override
        public void summarize(StatisticConsumer sc) {
          if (overMax > 0) {
            sc.accept("len>=" + maxLen, overMax);
          }
          for (int i = 0; i < lengths.length; i++) {
            if (lengths[i] > 0) {
              sc.accept("len=" + i, lengths[i]);
            }
          }
        }

      };
    }

    @Override
    public Combiner combiner(SummarizerConfiguration sc) {
      return (m1, m2) -> m2.forEach((k, v) -> m1.merge(k, v, Long::sum));
    }
  }

  private static void checkSummary(Collection<Summary> summaries, SummarizerConfiguration sc, Object... stats) {
    Map<String,Long> expected = new HashMap<>();
    for (int i = 0; i < stats.length; i += 2) {
      expected.put((String) stats[i], (Long) stats[i + 1]);
    }

    for (Summary summary : summaries) {
      if (summary.getSummarizerConfiguration().equals(sc)) {
        Assert.assertEquals(expected, summary.getStatistics());
        return;
      }
    }

    Assert.fail("Did not find summary with config : " + sc);
  }

  @Test
  public void selectionTest() throws Exception {
    final String table = getUniqueNames(1)[0];
    Connector c = getConnector();
    NewTableConfiguration ntc = new NewTableConfiguration();
    SummarizerConfiguration sc1 = SummarizerConfiguration.builder(BasicSummarizer.class).build();
    SummarizerConfiguration sc2 = SummarizerConfiguration.builder(KeySizeSummarizer.class).addOption("maxLen", "512").build();
    ntc.enableSummarization(sc1, sc2);
    c.tableOperations().create(table, ntc);

    BatchWriter bw = writeData(table, c);
    bw.close();

    c.tableOperations().flush(table, null, null, true);

    LongSummaryStatistics stats = getTimestampStats(table, c);

    Collection<Summary> summaries = c.tableOperations().summaries(table).withConfiguration(sc2).retrieve();
    Assert.assertEquals(1, summaries.size());
    checkSummary(summaries, sc2, "len=14", 100_000l);

    summaries = c.tableOperations().summaries(table).withConfiguration(sc1).retrieve();
    Assert.assertEquals(1, summaries.size());
    checkSummary(summaries, sc1, TOTAL_STAT, 100_000l, MIN_TIMESTAMP_STAT, stats.getMin(), MAX_TIMESTAMP_STAT, stats.getMax(), DELETES_STAT, 0l);

    // retrieve a non-existant summary
    SummarizerConfiguration sc3 = SummarizerConfiguration.builder(KeySizeSummarizer.class.getName()).addOption("maxLen", "256").build();
    summaries = c.tableOperations().summaries(table).withConfiguration(sc3).retrieve();
    Assert.assertEquals(0, summaries.size());

    summaries = c.tableOperations().summaries(table).withConfiguration(sc1, sc2).retrieve();
    Assert.assertEquals(2, summaries.size());
    checkSummary(summaries, sc1, TOTAL_STAT, 100_000l, MIN_TIMESTAMP_STAT, stats.getMin(), MAX_TIMESTAMP_STAT, stats.getMax(), DELETES_STAT, 0l);
    checkSummary(summaries, sc2, "len=14", 100_000l);

    summaries = c.tableOperations().summaries(table).retrieve();
    Assert.assertEquals(2, summaries.size());
    checkSummary(summaries, sc1, TOTAL_STAT, 100_000l, MIN_TIMESTAMP_STAT, stats.getMin(), MAX_TIMESTAMP_STAT, stats.getMax(), DELETES_STAT, 0l);
    checkSummary(summaries, sc2, "len=14", 100_000l);

    summaries = c.tableOperations().summaries(table).withMatchingConfiguration(".*BasicSummarizer \\{\\}.*").retrieve();
    Assert.assertEquals(1, summaries.size());
    checkSummary(summaries, sc1, TOTAL_STAT, 100_000l, MIN_TIMESTAMP_STAT, stats.getMin(), MAX_TIMESTAMP_STAT, stats.getMax(), DELETES_STAT, 0l);

    summaries = c.tableOperations().summaries(table).withMatchingConfiguration(".*KeySizeSummarizer \\{maxLen=512\\}.*").retrieve();
    Assert.assertEquals(1, summaries.size());
    checkSummary(summaries, sc2, "len=14", 100_000l);

    summaries = c.tableOperations().summaries(table).withMatchingConfiguration(".*KeySizeSummarizer \\{maxLen=256\\}.*").retrieve();
    Assert.assertEquals(0, summaries.size());

    summaries = c.tableOperations().summaries(table).withMatchingConfiguration(".*BasicSummarizer \\{\\}.*").withConfiguration(sc2).retrieve();
    Assert.assertEquals(2, summaries.size());
    checkSummary(summaries, sc1, TOTAL_STAT, 100_000l, MIN_TIMESTAMP_STAT, stats.getMin(), MAX_TIMESTAMP_STAT, stats.getMax(), DELETES_STAT, 0l);
    checkSummary(summaries, sc2, "len=14", 100_000l);

    // Ensure a bad regex fails fast.
    try {
      summaries = c.tableOperations().summaries(table).withMatchingConfiguration(".*KeySizeSummarizer {maxLen=256}.*").retrieve();
      Assert.fail("Bad regex should have caused exception");
    } catch (PatternSyntaxException e) {}
  }

  /**
   * A summarizer that counts the number of times {@code foo} and {@code bar} occur in the row.
   */
  public static class FooCounter implements Summarizer {

    @Override
    public Collector collector(SummarizerConfiguration sc) {
      return new Collector() {

        long foos = 0;
        long bars = 0;

        @Override
        public void accept(Key k, Value v) {
          String row = k.getRowData().toString();

          if (row.contains("foo")) {
            foos++;
          } else if (row.contains("bar")) {
            bars++;
          }
        }

        @Override
        public void summarize(StatisticConsumer sc) {
          sc.accept("foos", foos);
          sc.accept("bars", bars);
        }

      };
    }

    @Override
    public Combiner combiner(SummarizerConfiguration sc) {
      return (m1, m2) -> m2.forEach((k, v) -> m1.merge(k, v, Long::sum));
    }
  }

  /**
   * An Accumulo iterator that filters out entries where the row contains {@code foo}.
   */
  public static class FooFilter extends Filter {
    @Override
    public boolean accept(Key k, Value v) {
      return !k.getRowData().toString().contains("foo");
    }
  }

  /**
   * A compaction strategy that intitiates a compaction when {@code foo} occurs more than {@code bar} in the data. The {@link FooCounter} summary data is used
   * to make the determination.
   */
  public static class FooCS extends CompactionStrategy {

    private boolean compact = false;

    @Override
    public boolean shouldCompact(MajorCompactionRequest request) throws IOException {
      return true;
    }

    public void gatherInformation(MajorCompactionRequest request) throws IOException {
      List<Summary> summaries = request.getSummaries(request.getFiles().keySet(), conf -> conf.getClassName().contains("FooCounter"));
      if (summaries.size() == 1) {
        Summary summary = summaries.get(0);
        Long foos = summary.getStatistics().getOrDefault("foos", 0l);
        Long bars = summary.getStatistics().getOrDefault("bars", 0l);

        compact = foos > bars;
      }
    }

    @Override
    public CompactionPlan getCompactionPlan(MajorCompactionRequest request) throws IOException {
      if (compact) {
        CompactionPlan cp = new CompactionPlan();
        cp.inputFiles.addAll(request.getFiles().keySet());
        return cp;
      }
      return null;
    }

  }

  @Test
  public void compactionTest() throws Exception {
    final String table = getUniqueNames(1)[0];
    Connector c = getConnector();
    NewTableConfiguration ntc = new NewTableConfiguration();
    SummarizerConfiguration sc1 = SummarizerConfiguration.builder(FooCounter.class.getName()).build();
    ntc.enableSummarization(sc1);
    c.tableOperations().create(table, ntc);

    try (BatchWriter bw = c.createBatchWriter(table, new BatchWriterConfig())) {
      write(bw, "bar1", "f1", "q1", "v1");
      write(bw, "bar2", "f1", "q1", "v2");
      write(bw, "foo1", "f1", "q1", "v3");
    }

    // Create a compaction config that will filter out foos if there are too many. Uses summary data to know if there are too many foos.
    CompactionStrategyConfig csc = new CompactionStrategyConfig(FooCS.class.getName());
    List<IteratorSetting> iterators = Collections.singletonList(new IteratorSetting(100, FooFilter.class));
    CompactionConfig compactConfig = new CompactionConfig().setFlush(true).setCompactionStrategy(csc).setIterators(iterators).setWait(true);

    // this compaction should make no changes because there are less foos than bars
    c.tableOperations().compact(table, compactConfig);

    try (Scanner scanner = c.createScanner(table, Authorizations.EMPTY)) {
      Stream<Entry<Key,Value>> stream = StreamSupport.stream(scanner.spliterator(), false);
      Map<String,Long> counts = stream.map(e -> e.getKey().getRowData().toString()) // convert to row
          .map(r -> r.replaceAll("[0-9]+", "")) // strip numbers off row
          .collect(groupingBy(identity(), counting())); // count different row types
      Assert.assertEquals(1l, (long) counts.getOrDefault("foo", 0l));
      Assert.assertEquals(2l, (long) counts.getOrDefault("bar", 0l));
      Assert.assertEquals(2, counts.size());
    }

    try (BatchWriter bw = c.createBatchWriter(table, new BatchWriterConfig())) {
      write(bw, "foo2", "f1", "q1", "v4");
      write(bw, "foo3", "f1", "q1", "v5");
      write(bw, "foo4", "f1", "q1", "v6");
    }

    // this compaction should remove all foos because there are more foos than bars
    c.tableOperations().compact(table, compactConfig);

    try (Scanner scanner = c.createScanner(table, Authorizations.EMPTY)) {
      Stream<Entry<Key,Value>> stream = StreamSupport.stream(scanner.spliterator(), false);
      Map<String,Long> counts = stream.map(e -> e.getKey().getRowData().toString()) // convert to row
          .map(r -> r.replaceAll("[0-9]+", "")) // strip numbers off row
          .collect(groupingBy(identity(), counting())); // count different row types
      Assert.assertEquals(0l, (long) counts.getOrDefault("foo", 0l));
      Assert.assertEquals(2l, (long) counts.getOrDefault("bar", 0l));
      Assert.assertEquals(1, counts.size());
    }
  }

  public static class BuggySummarizer extends FooCounter {
    @Override
    public Combiner combiner(SummarizerConfiguration sc) {
      return (m1, m2) -> {
        throw new NullPointerException();
      };
    }
  }

  @Test
  public void testBuggySummarizer() throws Exception {
    final String table = getUniqueNames(1)[0];
    Connector c = getConnector();
    NewTableConfiguration ntc = new NewTableConfiguration();
    SummarizerConfiguration sc1 = SummarizerConfiguration.builder(BuggySummarizer.class).build();
    ntc.enableSummarization(sc1);
    c.tableOperations().create(table, ntc);

    // add a single split so that summary stats merge is forced
    c.tableOperations().addSplits(table, new TreeSet<>(Collections.singleton(new Text("g"))));

    try (BatchWriter bw = c.createBatchWriter(table, new BatchWriterConfig())) {
      write(bw, "bar1", "f1", "q1", "v1");
      write(bw, "bar2", "f1", "q1", "v2");
      write(bw, "foo1", "f1", "q1", "v3");
    }

    c.tableOperations().flush(table, null, null, true);
    try {
      c.tableOperations().summaries(table).retrieve();
      Assert.fail("Expected server side failure and did not see it");
    } catch (AccumuloServerException ase) {}

  }

  @Test
  public void testPermissions() throws Exception {
    final String table = getUniqueNames(1)[0];
    Connector c = getConnector();
    NewTableConfiguration ntc = new NewTableConfiguration();
    SummarizerConfiguration sc1 = SummarizerConfiguration.builder(FooCounter.class).build();
    ntc.enableSummarization(sc1);
    c.tableOperations().create(table, ntc);

    try (BatchWriter bw = c.createBatchWriter(table, new BatchWriterConfig())) {
      write(bw, "bar1", "f1", "q1", "v1");
      write(bw, "bar2", "f1", "q1", "v2");
      write(bw, "foo1", "f1", "q1", "v3");
    }

    c.tableOperations().flush(table, null, null, true);

    PasswordToken passTok = new PasswordToken("letmesee");
    c.securityOperations().createLocalUser("user1", passTok);

    String instanceName = c.getInstance().getInstanceName();
    String zookeepers = c.getInstance().getZooKeepers();
    Connector c2 = new ZooKeeperInstance(instanceName, zookeepers).getConnector("user1", passTok);
    try {
      c2.tableOperations().summaries(table).retrieve();
      Assert.fail("Expected operation to fail because user does not have permssion to get summaries");
    } catch (AccumuloSecurityException ase) {
      Assert.assertEquals(SecurityErrorCode.PERMISSION_DENIED, ase.getSecurityErrorCode());
    }

    c.securityOperations().grantTablePermission("user1", table, TablePermission.GET_SUMMARIES);

    int tries = 0;
    while (tries < 10) {
      try {
        Summary summary = c2.tableOperations().summaries(table).retrieve().get(0);
        Assert.assertEquals(2, summary.getStatistics().size());
        Assert.assertEquals(2l, (long) summary.getStatistics().getOrDefault("bars", 0l));
        Assert.assertEquals(1l, (long) summary.getStatistics().getOrDefault("foos", 0l));
        break;
      } catch (AccumuloSecurityException ase) {
        UtilWaitThread.sleep(500);
        tries++;
      }
    }
  }

  public static class BigSummarizer implements Summarizer {
    @Override
    public Collector collector(SummarizerConfiguration sc) {
      return new Collector() {
        private int num = 10;

        @Override
        public void accept(Key k, Value v) {
          if (k.getRowData().toString().contains("large")) {
            num = 100_000;
          }
        }

        @Override
        public void summarize(StatisticConsumer sc) {
          for (int i = 0; i < num; i++) {
            sc.accept(String.format("%09x", i), i * 19);
          }
        }
      };
    }

    @Override
    public Combiner combiner(SummarizerConfiguration sc) {
      return (m1, m2) -> m2.forEach((k, v) -> m1.merge(k, v, Long::sum));
    }
  }

  @Test
  public void tooLargeTest() throws Exception {
    final String table = getUniqueNames(1)[0];
    Connector c = getConnector();
    NewTableConfiguration ntc = new NewTableConfiguration();
    SummarizerConfiguration sc1 = SummarizerConfiguration.builder(BigSummarizer.class).build();
    ntc.enableSummarization(sc1);
    c.tableOperations().create(table, ntc);

    try (BatchWriter bw = c.createBatchWriter(table, new BatchWriterConfig())) {
      write(bw, "a_large", "f1", "q1", "v1");
      write(bw, "v_small", "f1", "q1", "v2");
    }

    c.tableOperations().flush(table, null, null, true);
    Summary summary = c.tableOperations().summaries(table).retrieve().get(0);
    Assert.assertEquals(1, summary.getFileStatistics().getLarge());
    Assert.assertEquals(0, summary.getFileStatistics().getMissing());
    Assert.assertEquals(0, summary.getFileStatistics().getExtra());
    Assert.assertEquals(0, summary.getFileStatistics().getDeleted());
    Assert.assertEquals(1, summary.getFileStatistics().getInaccurate());
    Assert.assertEquals(1, summary.getFileStatistics().getTotal());
    Assert.assertEquals(Collections.emptyMap(), summary.getStatistics());

    // create situation where one tablet has summary data and one does not because the summary data was too large
    c.tableOperations().addSplits(table, new TreeSet<>(Collections.singleton(new Text("m"))));
    c.tableOperations().compact(table, new CompactionConfig().setWait(true));

    summary = c.tableOperations().summaries(table).retrieve().get(0);
    Assert.assertEquals(1, summary.getFileStatistics().getLarge());
    Assert.assertEquals(0, summary.getFileStatistics().getMissing());
    Assert.assertEquals(0, summary.getFileStatistics().getExtra());
    Assert.assertEquals(0, summary.getFileStatistics().getDeleted());
    Assert.assertEquals(1, summary.getFileStatistics().getInaccurate());
    Assert.assertEquals(2, summary.getFileStatistics().getTotal());

    HashMap<String,Long> expected = new HashMap<>();
    for (int i = 0; i < 10; i++) {
      expected.put(String.format("%09x", i), i * 19l);
    }

    Assert.assertEquals(expected, summary.getStatistics());
  }

  private void write(BatchWriter bw, String row, String family, String qualifier, String value) throws MutationsRejectedException {
    Mutation m1 = new Mutation(row);
    m1.put(family, qualifier, value);
    bw.addMutation(m1);
  }

  private void write(BatchWriter bw, Map<Key,Value> expected, String row, String family, String qualifier, long ts, String value)
      throws MutationsRejectedException {
    Mutation m1 = new Mutation(row);
    m1.put(family, qualifier, ts, value);
    bw.addMutation(m1);
    expected.put(Key.builder().row(row).family(family).qualifier(qualifier).timestamp(ts).build(), new Value(value));
  }

  private Map<String,Long> nm(Object... entries) {
    Builder<String,Long> imb = ImmutableMap.builder();
    for (int i = 0; i < entries.length; i += 2) {
      imb.put((String) entries[i], (Long) entries[i + 1]);
    }
    return imb.build();
  }

  @Test
  public void testLocalityGroups() throws Exception {
    final String table = getUniqueNames(1)[0];
    Connector c = getConnector();
    NewTableConfiguration ntc = new NewTableConfiguration();
    SummarizerConfiguration sc1 = SummarizerConfiguration.builder(FamilySummarizer.class).build();
    SummarizerConfiguration sc2 = SummarizerConfiguration.builder(BasicSummarizer.class).build();
    ntc.enableSummarization(sc1, sc2);
    c.tableOperations().create(table, ntc);

    Map<String,Set<Text>> lgroups = new HashMap<>();
    lgroups.put("lg1", ImmutableSet.of(new Text("chocolate"), new Text("coffee")));
    lgroups.put("lg2", ImmutableSet.of(new Text(" broccoli "), new Text("cabbage")));

    c.tableOperations().setLocalityGroups(table, lgroups);

    Map<Key,Value> expected = new HashMap<>();
    try (BatchWriter bw = c.createBatchWriter(table, new BatchWriterConfig())) {
      write(bw, expected, "order:001", "chocolate", "dark", 3l, "99kg");
      write(bw, expected, "order:001", "chocolate", "light", 4l, "94kg");
      write(bw, expected, "order:001", "coffee", "dark", 5l, "33kg");
      write(bw, expected, "order:001", "broccoli", "crowns", 6l, "2kg");
      write(bw, expected, "order:001", "cheddar", "canadian", 7l, "40kg");

      write(bw, expected, "order:653", "chocolate", "dark", 3l, "3kg");
      write(bw, expected, "order:653", "chocolate", "light", 4l, "4kg");
      write(bw, expected, "order:653", "coffee", "dark", 5l, "2kg");
      write(bw, expected, "order:653", "broccoli", "crowns", 6l, "105kg");
      write(bw, expected, "order:653", "cabbage", "heads", 7l, "199kg");
      write(bw, expected, "order:653", "cheddar", "canadian", 8l, "43kg");
    }

    List<Summary> summaries = c.tableOperations().summaries(table).flush(true).retrieve();
    Assert.assertEquals(2, summaries.stream().map(Summary::getSummarizerConfiguration).distinct().count());
    for (Summary summary : summaries) {
      if (summary.getSummarizerConfiguration().equals(sc1)) {
        Map<String,Long> expectedStats = nm("c:chocolate", 4l, "c:coffee", 2l, "c:broccoli", 2l, "c:cheddar", 2l, "c:cabbage", 1l, TOO_LONG_STAT, 0l,
            TOO_MANY_STAT, 0l, SEEN_STAT, 11l, EMITTED_STAT, 11l, DELETES_IGNORED_STAT, 0l);
        Assert.assertEquals(expectedStats, summary.getStatistics());
        Assert.assertEquals(0, summary.getFileStatistics().getInaccurate());
        Assert.assertEquals(1, summary.getFileStatistics().getTotal());
      } else if (summary.getSummarizerConfiguration().equals(sc2)) {
        Map<String,Long> expectedStats = nm(DELETES_STAT, 0l, TOTAL_STAT, 11l, MIN_TIMESTAMP_STAT, 3l, MAX_TIMESTAMP_STAT, 8l);
        Assert.assertEquals(expectedStats, summary.getStatistics());
        Assert.assertEquals(0, summary.getFileStatistics().getInaccurate());
        Assert.assertEquals(1, summary.getFileStatistics().getTotal());
      } else {
        Assert.fail("unexpected summary config " + summary.getSummarizerConfiguration());
      }
    }

    Map<Key,Value> actual = new HashMap<>();
    c.createScanner(table, Authorizations.EMPTY).forEach(e -> actual.put(e.getKey(), e.getValue()));

    Assert.assertEquals(expected, actual);
  }

  @Test
  public void testExceptions() throws Exception {
    Connector c = getConnector();

    try {
      c.tableOperations().summaries("foo").retrieve();
      Assert.fail();
    } catch (TableNotFoundException e) {}

    try {
      c.tableOperations().addSummarizers("foo", SummarizerConfiguration.builder(VisibilitySummarizer.class).build());
      Assert.fail();
    } catch (TableNotFoundException e) {}

    try {
      c.tableOperations().listSummarizers("foo");
      Assert.fail();
    } catch (TableNotFoundException e) {}

    try {
      c.tableOperations().removeSummarizers("foo", sc -> true);
      Assert.fail();
    } catch (TableNotFoundException e) {}

    SummarizerConfiguration sc1 = SummarizerConfiguration.builder(FamilySummarizer.class).setPropertyId("p1").build();
    SummarizerConfiguration sc2 = SummarizerConfiguration.builder(VisibilitySummarizer.class).setPropertyId("p1").build();

    c.tableOperations().create("foo");
    c.tableOperations().addSummarizers("foo", sc1);
    c.tableOperations().addSummarizers("foo", sc1);
    try {
      // adding second summarizer with same id should fail
      c.tableOperations().addSummarizers("foo", sc2);
      Assert.fail();
    } catch (IllegalArgumentException e) {}

    c.tableOperations().removeSummarizers("foo", sc -> true);
    Assert.assertEquals(0, c.tableOperations().listSummarizers("foo").size());

    try {
      // adding two summarizers at the same time with same id should fail
      c.tableOperations().addSummarizers("foo", sc1, sc2);
      Assert.fail();
    } catch (IllegalArgumentException e) {}
    Assert.assertEquals(0, c.tableOperations().listSummarizers("foo").size());

    c.tableOperations().offline("foo", true);
    try {
      c.tableOperations().summaries("foo").retrieve();
      Assert.fail();
    } catch (TableOfflineException e) {}
  }

  @Test
  public void testManyFiles() throws Exception {
    final String table = getUniqueNames(1)[0];
    Connector c = getConnector();
    NewTableConfiguration ntc = new NewTableConfiguration();
    ntc.enableSummarization(SummarizerConfiguration.builder(FamilySummarizer.class).build());
    c.tableOperations().create(table, ntc);

    Random rand = new Random(42);
    int q = 0;

    SortedSet<Text> partitionKeys = new TreeSet<>();
    for (int split = 100_000; split < 1_000_000; split += 100_000) {
      partitionKeys.add(new Text(String.format("%06d", split)));
    }
    c.tableOperations().addSplits(table, partitionKeys);
    Map<String,Long> famCounts = new HashMap<>();

    for (int t = 0; t < 20; t++) {
      // this loop should cause a varying number of files and compactions
      try (BatchWriter bw = c.createBatchWriter(table, new BatchWriterConfig())) {
        for (int i = 0; i < 10000; i++) {
          String row = String.format("%06d", rand.nextInt(1_000_000));
          String fam = String.format("%03d", rand.nextInt(100));
          String qual = String.format("%06d", q++);
          write(bw, row, fam, qual, "val");
          famCounts.merge(fam, 1L, Long::sum);
        }
      }

      List<Summary> summaries = c.tableOperations().summaries(table).flush(true).retrieve();
      Assert.assertEquals(1, summaries.size());
      CounterSummary cs = new CounterSummary(summaries.get(0));
      Assert.assertEquals(famCounts, cs.getCounters());
      FileStatistics fileStats = summaries.get(0).getFileStatistics();
      Assert.assertEquals(0, fileStats.getInaccurate());
      Assert.assertTrue("Saw " + fileStats.getTotal() + " files expected >=10", fileStats.getTotal() >= 10);
    }
  }
}

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
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.LongSummaryStatistics;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.function.IntPredicate;
import java.util.regex.PatternSyntaxException;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.TableOfflineException;
import org.apache.accumulo.core.client.admin.CompactionConfig;
import org.apache.accumulo.core.client.admin.NewTableConfiguration;
import org.apache.accumulo.core.client.admin.PluginConfig;
import org.apache.accumulo.core.client.admin.SummaryRetriever;
import org.apache.accumulo.core.client.admin.compaction.CompactionSelector;
import org.apache.accumulo.core.client.security.SecurityErrorCode;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.client.summary.CounterSummary;
import org.apache.accumulo.core.client.summary.Summarizer;
import org.apache.accumulo.core.client.summary.SummarizerConfiguration;
import org.apache.accumulo.core.client.summary.Summary;
import org.apache.accumulo.core.client.summary.Summary.FileStatistics;
import org.apache.accumulo.core.client.summary.summarizers.FamilySummarizer;
import org.apache.accumulo.core.client.summary.summarizers.VisibilitySummarizer;
import org.apache.accumulo.core.clientImpl.AccumuloServerException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.Filter;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.TablePermission;
import org.apache.accumulo.core.util.UtilWaitThread;
import org.apache.accumulo.harness.SharedMiniClusterBase;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class SummaryIT extends SharedMiniClusterBase {

  @BeforeAll
  public static void setup() throws Exception {
    SharedMiniClusterBase.startMiniCluster();
  }

  @AfterAll
  public static void teardown() {
    SharedMiniClusterBase.stopMiniCluster();
  }

  private LongSummaryStatistics getTimestampStats(final String table, AccumuloClient c)
      throws TableNotFoundException {
    try (Scanner scanner = c.createScanner(table, Authorizations.EMPTY)) {
      return scanner.stream().mapToLong(e -> e.getKey().getTimestamp()).summaryStatistics();
    }
  }

  private LongSummaryStatistics getTimestampStats(final String table, AccumuloClient c,
      String startRow, String endRow) throws TableNotFoundException {
    try (Scanner scanner = c.createScanner(table, Authorizations.EMPTY)) {
      scanner.setRange(new Range(startRow, false, endRow, true));
      return scanner.stream().mapToLong(e -> e.getKey().getTimestamp()).summaryStatistics();
    }
  }

  private void checkSummaries(Collection<Summary> summaries, SummarizerConfiguration sc, int total,
      int missing, int extra, Object... kvs) {
    Summary summary = getOnlyElement(summaries);
    assertEquals(total, summary.getFileStatistics().getTotal(), "total wrong");
    assertEquals(missing, summary.getFileStatistics().getMissing(), "missing wrong");
    assertEquals(extra, summary.getFileStatistics().getExtra(), "extra wrong");
    assertEquals(0, summary.getFileStatistics().getDeleted(), "deleted wrong");
    assertEquals(sc, summary.getSummarizerConfiguration());
    Map<String,Long> expected = new HashMap<>();
    for (int i = 0; i < kvs.length; i += 2) {
      expected.put((String) kvs[i], (Long) kvs[i + 1]);
    }
    assertEquals(expected, summary.getStatistics());
  }

  private void addSplits(final String table, AccumuloClient c, String... splits)
      throws TableNotFoundException, AccumuloException, AccumuloSecurityException {
    c.tableOperations().addSplits(table,
        Stream.of(splits).map(Text::new).collect(Collectors.toCollection(TreeSet::new)));
  }

  @Test
  public void basicSummaryTest() throws Exception {
    final String table = getUniqueNames(1)[0];
    try (AccumuloClient c = Accumulo.newClient().from(getClientProps()).build()) {
      NewTableConfiguration ntc = new NewTableConfiguration();
      SummarizerConfiguration sc1 =
          SummarizerConfiguration.builder(BasicSummarizer.class.getName()).build();
      ntc.enableSummarization(sc1);
      c.tableOperations().create(table, ntc);

      BatchWriter bw = writeData(table, c);

      Collection<Summary> summaries = c.tableOperations().summaries(table).flush(false).retrieve();
      assertEquals(0, summaries.size());

      LongSummaryStatistics stats = getTimestampStats(table, c);

      summaries = c.tableOperations().summaries(table).flush(true).retrieve();
      checkSummaries(summaries, sc1, 1, 0, 0, TOTAL_STAT, 100_000L, MIN_TIMESTAMP_STAT,
          stats.getMin(), MAX_TIMESTAMP_STAT, stats.getMax(), DELETES_STAT, 0L);

      Mutation m = new Mutation(String.format("r%09x", 999));
      m.put("f1", "q1", "999-0");
      m.putDelete("f1", "q2");
      bw.addMutation(m);
      bw.flush();

      c.tableOperations().flush(table, null, null, true);

      stats = getTimestampStats(table, c);

      summaries = c.tableOperations().summaries(table).retrieve();

      checkSummaries(summaries, sc1, 2, 0, 0, TOTAL_STAT, 100_002L, MIN_TIMESTAMP_STAT,
          stats.getMin(), MAX_TIMESTAMP_STAT, stats.getMax(), DELETES_STAT, 1L);

      bw.close();

      c.tableOperations().compact(table, new CompactionConfig().setWait(true));

      summaries = c.tableOperations().summaries(table).retrieve();
      checkSummaries(summaries, sc1, 1, 0, 0, TOTAL_STAT, 100_000L, MIN_TIMESTAMP_STAT,
          stats.getMin(), MAX_TIMESTAMP_STAT, stats.getMax(), DELETES_STAT, 0L);

      // split tablet into two
      String sp1 = String.format("r%09x", 50_000);
      addSplits(table, c, sp1);

      summaries = c.tableOperations().summaries(table).retrieve();

      checkSummaries(summaries, sc1, 1, 0, 0, TOTAL_STAT, 100_000L, MIN_TIMESTAMP_STAT,
          stats.getMin(), MAX_TIMESTAMP_STAT, stats.getMax(), DELETES_STAT, 0L);

      // compact 2nd tablet
      c.tableOperations().compact(table,
          new CompactionConfig().setStartRow(new Text(sp1)).setWait(true));

      summaries = c.tableOperations().summaries(table).retrieve();
      checkSummaries(summaries, sc1, 2, 0, 1, TOTAL_STAT, 113_999L, MIN_TIMESTAMP_STAT,
          stats.getMin(), MAX_TIMESTAMP_STAT, stats.getMax(), DELETES_STAT, 0L);

      // get summaries for first tablet
      stats = getTimestampStats(table, c, sp1, null);
      summaries = c.tableOperations().summaries(table).startRow(sp1).retrieve();
      checkSummaries(summaries, sc1, 1, 0, 0, TOTAL_STAT, 49_999L, MIN_TIMESTAMP_STAT,
          stats.getMin(), MAX_TIMESTAMP_STAT, stats.getMax(), DELETES_STAT, 0L);

      // compact all tablets and regenerate all summaries
      c.tableOperations().compact(table, new CompactionConfig());

      summaries = c.tableOperations().summaries(table).retrieve();
      stats = getTimestampStats(table, c);
      checkSummaries(summaries, sc1, 2, 0, 0, TOTAL_STAT, 100_000L, MIN_TIMESTAMP_STAT,
          stats.getMin(), MAX_TIMESTAMP_STAT, stats.getMax(), DELETES_STAT, 0L);

      summaries = c.tableOperations().summaries(table).startRow(String.format("r%09x", 75_000))
          .endRow(String.format("r%09x", 80_000)).retrieve();
      Summary summary = getOnlyElement(summaries);
      assertEquals(1, summary.getFileStatistics().getTotal());
      assertEquals(1, summary.getFileStatistics().getExtra());
      long total = summary.getStatistics().get(TOTAL_STAT);
      assertTrue(total > 0 && total <= 10_000, "Total " + total + " out of expected range");

      // test adding and removing
      c.tableOperations().removeSummarizers(table, sc -> sc.getClassName().contains("foo"));

      List<SummarizerConfiguration> summarizers = c.tableOperations().listSummarizers(table);
      assertEquals(1, summarizers.size());
      assertTrue(summarizers.contains(sc1));

      c.tableOperations().removeSummarizers(table,
          sc -> sc.getClassName().equals(BasicSummarizer.class.getName()));
      summarizers = c.tableOperations().listSummarizers(table);
      assertEquals(0, summarizers.size());

      c.tableOperations().compact(table, new CompactionConfig().setWait(true));

      summaries = c.tableOperations().summaries(table).retrieve();
      assertEquals(0, summaries.size());

      c.tableOperations().addSummarizers(table, sc1);
      c.tableOperations().compact(table, new CompactionConfig().setWait(true));
      summaries = c.tableOperations().summaries(table).retrieve();
      checkSummaries(summaries, sc1, 2, 0, 0, TOTAL_STAT, 100_000L, MIN_TIMESTAMP_STAT,
          stats.getMin(), MAX_TIMESTAMP_STAT, stats.getMax(), DELETES_STAT, 0L);
    }
  }

  private BatchWriter writeData(final String table, AccumuloClient c)
      throws TableNotFoundException, MutationsRejectedException {
    BatchWriter bw = c.createBatchWriter(table);
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

  private static void checkSummary(Collection<Summary> summaries, SummarizerConfiguration sc,
      Object... stats) {
    Map<String,Long> expected = new HashMap<>();
    for (int i = 0; i < stats.length; i += 2) {
      expected.put((String) stats[i], (Long) stats[i + 1]);
    }

    for (Summary summary : summaries) {
      if (summary.getSummarizerConfiguration().equals(sc)) {
        assertEquals(expected, summary.getStatistics());
        return;
      }
    }

    fail("Did not find summary with config : " + sc);
  }

  @Test
  public void selectionTest() throws Exception {
    final String table = getUniqueNames(1)[0];
    try (AccumuloClient c = Accumulo.newClient().from(getClientProps()).build()) {
      NewTableConfiguration ntc = new NewTableConfiguration();
      SummarizerConfiguration sc1 = SummarizerConfiguration.builder(BasicSummarizer.class).build();
      SummarizerConfiguration sc2 = SummarizerConfiguration.builder(KeySizeSummarizer.class)
          .addOption("maxLen", "512").build();
      ntc.enableSummarization(sc1, sc2);
      c.tableOperations().create(table, ntc);

      BatchWriter bw = writeData(table, c);
      bw.close();

      c.tableOperations().flush(table, null, null, true);

      LongSummaryStatistics stats = getTimestampStats(table, c);

      Collection<Summary> summaries =
          c.tableOperations().summaries(table).withConfiguration(sc2).retrieve();
      assertEquals(1, summaries.size());
      checkSummary(summaries, sc2, "len=14", 100_000L);

      summaries = c.tableOperations().summaries(table).withConfiguration(sc1).retrieve();
      assertEquals(1, summaries.size());
      checkSummary(summaries, sc1, TOTAL_STAT, 100_000L, MIN_TIMESTAMP_STAT, stats.getMin(),
          MAX_TIMESTAMP_STAT, stats.getMax(), DELETES_STAT, 0L);

      // retrieve a nonexistent summary
      SummarizerConfiguration sc3 = SummarizerConfiguration
          .builder(KeySizeSummarizer.class.getName()).addOption("maxLen", "256").build();
      summaries = c.tableOperations().summaries(table).withConfiguration(sc3).retrieve();
      assertEquals(0, summaries.size());

      summaries = c.tableOperations().summaries(table).withConfiguration(sc1, sc2).retrieve();
      assertEquals(2, summaries.size());
      checkSummary(summaries, sc1, TOTAL_STAT, 100_000L, MIN_TIMESTAMP_STAT, stats.getMin(),
          MAX_TIMESTAMP_STAT, stats.getMax(), DELETES_STAT, 0L);
      checkSummary(summaries, sc2, "len=14", 100_000L);

      summaries = c.tableOperations().summaries(table).retrieve();
      assertEquals(2, summaries.size());
      checkSummary(summaries, sc1, TOTAL_STAT, 100_000L, MIN_TIMESTAMP_STAT, stats.getMin(),
          MAX_TIMESTAMP_STAT, stats.getMax(), DELETES_STAT, 0L);
      checkSummary(summaries, sc2, "len=14", 100_000L);

      summaries = c.tableOperations().summaries(table)
          .withMatchingConfiguration(".*BasicSummarizer \\{\\}.*").retrieve();
      assertEquals(1, summaries.size());
      checkSummary(summaries, sc1, TOTAL_STAT, 100_000L, MIN_TIMESTAMP_STAT, stats.getMin(),
          MAX_TIMESTAMP_STAT, stats.getMax(), DELETES_STAT, 0L);

      summaries = c.tableOperations().summaries(table)
          .withMatchingConfiguration(".*KeySizeSummarizer \\{maxLen=512\\}.*").retrieve();
      assertEquals(1, summaries.size());
      checkSummary(summaries, sc2, "len=14", 100_000L);

      summaries = c.tableOperations().summaries(table)
          .withMatchingConfiguration(".*KeySizeSummarizer \\{maxLen=256\\}.*").retrieve();
      assertEquals(0, summaries.size());

      summaries = c.tableOperations().summaries(table)
          .withMatchingConfiguration(".*BasicSummarizer \\{\\}.*").withConfiguration(sc2)
          .retrieve();
      assertEquals(2, summaries.size());
      checkSummary(summaries, sc1, TOTAL_STAT, 100_000L, MIN_TIMESTAMP_STAT, stats.getMin(),
          MAX_TIMESTAMP_STAT, stats.getMax(), DELETES_STAT, 0L);
      checkSummary(summaries, sc2, "len=14", 100_000L);

      // Ensure a bad regex fails fast.
      assertThrows(PatternSyntaxException.class,
          () -> c.tableOperations().summaries(table)
              .withMatchingConfiguration(".*KeySizeSummarizer {maxLen=256}.*").retrieve(),
          "Bad regex should have caused exception");
    }
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
   * A compaction selector that initiates a compaction when {@code foo} occurs more than {@code bar}
   * in the data. The {@link FooCounter} summary data is used to make the determination.
   */
  public static class FooSelector implements CompactionSelector {

    @Override
    public void init(InitParameters iparams) {}

    @Override
    public Selection select(SelectionParameters sparams) {
      Collection<Summary> summaries = sparams.getSummaries(sparams.getAvailableFiles(),
          conf -> conf.getClassName().contains("FooCounter"));
      if (summaries.size() == 1) {
        Summary summary = summaries.iterator().next();
        Long foos = summary.getStatistics().getOrDefault("foos", 0L);
        Long bars = summary.getStatistics().getOrDefault("bars", 0L);

        if (foos > bars) {
          return new Selection(sparams.getAvailableFiles());
        }
      }

      return new Selection(Set.of());
    }

  }

  /**
   * A compaction strategy that initiates a compaction when {@code foo} occurs more than {@code bar}
   * in the data. The {@link FooCounter} summary data is used to make the determination.
   */
  @SuppressWarnings("removal")
  public static class FooCS extends org.apache.accumulo.tserver.compaction.CompactionStrategy {

    private boolean compact = false;

    @Override
    public boolean
        shouldCompact(org.apache.accumulo.tserver.compaction.MajorCompactionRequest request) {
      return true;
    }

    @Override
    public void
        gatherInformation(org.apache.accumulo.tserver.compaction.MajorCompactionRequest request) {
      List<Summary> summaries = request.getSummaries(request.getFiles().keySet(),
          conf -> conf.getClassName().contains("FooCounter"));
      if (summaries.size() == 1) {
        Summary summary = summaries.get(0);
        Long foos = summary.getStatistics().getOrDefault("foos", 0L);
        Long bars = summary.getStatistics().getOrDefault("bars", 0L);

        compact = foos > bars;
      }
    }

    @Override
    public org.apache.accumulo.tserver.compaction.CompactionPlan
        getCompactionPlan(org.apache.accumulo.tserver.compaction.MajorCompactionRequest request) {
      if (compact) {
        var cp = new org.apache.accumulo.tserver.compaction.CompactionPlan();
        cp.inputFiles.addAll(request.getFiles().keySet());
        return cp;
      }
      return null;
    }

  }

  @Test
  public void compactionSelectorTest() throws Exception {
    // Create a compaction config that will filter out foos if there are too many. Uses summary
    // data to know if there are too many foos.
    PluginConfig csc = new PluginConfig(FooSelector.class.getName());
    CompactionConfig compactConfig = new CompactionConfig().setSelector(csc);
    compactionTest(compactConfig);
  }

  @SuppressWarnings("removal")
  @Test
  public void compactionStrategyTest() throws Exception {
    var csc =
        new org.apache.accumulo.core.client.admin.CompactionStrategyConfig(FooCS.class.getName());
    CompactionConfig compactConfig = new CompactionConfig().setCompactionStrategy(csc);
    compactionTest(compactConfig);
  }

  private void compactionTest(CompactionConfig compactConfig) throws Exception {
    final String table = getUniqueNames(1)[0];
    try (AccumuloClient c = Accumulo.newClient().from(getClientProps()).build()) {
      NewTableConfiguration ntc = new NewTableConfiguration();
      SummarizerConfiguration sc1 =
          SummarizerConfiguration.builder(FooCounter.class.getName()).build();
      ntc.enableSummarization(sc1);
      c.tableOperations().create(table, ntc);

      try (BatchWriter bw = c.createBatchWriter(table)) {
        write(bw, "bar1", "f1", "q1", "v1");
        write(bw, "bar2", "f1", "q1", "v2");
        write(bw, "foo1", "f1", "q1", "v3");
      }

      List<IteratorSetting> iterators =
          Collections.singletonList(new IteratorSetting(100, FooFilter.class));
      compactConfig = compactConfig.setFlush(true).setIterators(iterators).setWait(true);

      // this compaction should make no changes because there are less foos than bars
      c.tableOperations().compact(table, compactConfig);

      try (Scanner scanner = c.createScanner(table, Authorizations.EMPTY)) {
        // convert to row
        Map<String,
            Long> counts = scanner.stream().map(e -> e.getKey().getRowData().toString())
                .map(r -> r.replaceAll("[0-9]+", "")) // strip numbers off row
                .collect(groupingBy(identity(), counting())); // count different row types
        assertEquals(1L, (long) counts.getOrDefault("foo", 0L));
        assertEquals(2L, (long) counts.getOrDefault("bar", 0L));
        assertEquals(2, counts.size());
      }

      try (BatchWriter bw = c.createBatchWriter(table)) {
        write(bw, "foo2", "f1", "q1", "v4");
        write(bw, "foo3", "f1", "q1", "v5");
        write(bw, "foo4", "f1", "q1", "v6");
      }

      // this compaction should remove all foos because there are more foos than bars
      c.tableOperations().compact(table, compactConfig);

      try (Scanner scanner = c.createScanner(table, Authorizations.EMPTY)) {
        // convert to row
        Map<String,
            Long> counts = scanner.stream().map(e -> e.getKey().getRowData().toString())
                .map(r -> r.replaceAll("[0-9]+", "")) // strip numbers off row
                .collect(groupingBy(identity(), counting())); // count different row types
        assertEquals(0L, (long) counts.getOrDefault("foo", 0L));
        assertEquals(2L, (long) counts.getOrDefault("bar", 0L));
        assertEquals(1, counts.size());
      }
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
    try (AccumuloClient c = Accumulo.newClient().from(getClientProps()).build()) {
      SummarizerConfiguration sc1 = SummarizerConfiguration.builder(BuggySummarizer.class).build();
      // create table with a single split so that summary stats merge is forced
      SortedSet<Text> split = new TreeSet<>(Collections.singleton(new Text("g")));
      NewTableConfiguration ntc =
          new NewTableConfiguration().enableSummarization(sc1).withSplits(split);
      c.tableOperations().create(table, ntc);

      try (BatchWriter bw = c.createBatchWriter(table)) {
        write(bw, "bar1", "f1", "q1", "v1");
        write(bw, "bar2", "f1", "q1", "v2");
        write(bw, "foo1", "f1", "q1", "v3");
      }

      c.tableOperations().flush(table, null, null, true);
      assertThrows(AccumuloServerException.class,
          () -> c.tableOperations().summaries(table).retrieve(),
          "Expected server side failure and did not see it");
    }
  }

  @Test
  public void testPermissions() throws Exception {
    final String table = getUniqueNames(1)[0];
    try (AccumuloClient c = Accumulo.newClient().from(getClientProps()).build()) {
      NewTableConfiguration ntc = new NewTableConfiguration();
      SummarizerConfiguration sc1 = SummarizerConfiguration.builder(FooCounter.class).build();
      ntc.enableSummarization(sc1);
      c.tableOperations().create(table, ntc);

      try (BatchWriter bw = c.createBatchWriter(table)) {
        write(bw, "bar1", "f1", "q1", "v1");
        write(bw, "bar2", "f1", "q1", "v2");
        write(bw, "foo1", "f1", "q1", "v3");
      }

      c.tableOperations().flush(table, null, null, true);

      PasswordToken passTok = new PasswordToken("letmesee");
      c.securityOperations().createLocalUser("user1", passTok);

      try (AccumuloClient c2 =
          Accumulo.newClient().from(c.properties()).as("user1", passTok).build()) {
        var e = assertThrows(AccumuloSecurityException.class,
            () -> c2.tableOperations().summaries(table).retrieve(),
            "Expected operation to fail because user does not have permission to get summaries");
        assertEquals(SecurityErrorCode.PERMISSION_DENIED, e.getSecurityErrorCode());

        c.securityOperations().grantTablePermission("user1", table, TablePermission.GET_SUMMARIES);

        int tries = 0;
        while (tries < 10) {
          try {
            Summary summary = c2.tableOperations().summaries(table).retrieve().get(0);
            assertEquals(2, summary.getStatistics().size());
            assertEquals(2L, (long) summary.getStatistics().getOrDefault("bars", 0L));
            assertEquals(1L, (long) summary.getStatistics().getOrDefault("foos", 0L));
            break;
          } catch (AccumuloSecurityException ase) {
            UtilWaitThread.sleep(500);
            tries++;
          }
        }
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
    try (AccumuloClient c = Accumulo.newClient().from(getClientProps()).build()) {
      NewTableConfiguration ntc = new NewTableConfiguration();
      SummarizerConfiguration sc1 = SummarizerConfiguration.builder(BigSummarizer.class).build();
      ntc.enableSummarization(sc1);
      c.tableOperations().create(table, ntc);

      try (BatchWriter bw = c.createBatchWriter(table)) {
        write(bw, "a_large", "f1", "q1", "v1");
        write(bw, "v_small", "f1", "q1", "v2");
      }

      c.tableOperations().flush(table, null, null, true);
      Summary summary = c.tableOperations().summaries(table).retrieve().get(0);
      assertEquals(1, summary.getFileStatistics().getLarge());
      assertEquals(0, summary.getFileStatistics().getMissing());
      assertEquals(0, summary.getFileStatistics().getExtra());
      assertEquals(0, summary.getFileStatistics().getDeleted());
      assertEquals(1, summary.getFileStatistics().getInaccurate());
      assertEquals(1, summary.getFileStatistics().getTotal());
      assertEquals(Collections.emptyMap(), summary.getStatistics());

      // create situation where one tablet has summary data and one does not because the summary
      // data
      // was too large
      c.tableOperations().addSplits(table, new TreeSet<>(Collections.singleton(new Text("m"))));
      c.tableOperations().compact(table, new CompactionConfig().setWait(true));

      summary = c.tableOperations().summaries(table).retrieve().get(0);
      assertEquals(1, summary.getFileStatistics().getLarge());
      assertEquals(0, summary.getFileStatistics().getMissing());
      assertEquals(0, summary.getFileStatistics().getExtra());
      assertEquals(0, summary.getFileStatistics().getDeleted());
      assertEquals(1, summary.getFileStatistics().getInaccurate());
      assertEquals(2, summary.getFileStatistics().getTotal());

      HashMap<String,Long> expected = new HashMap<>();
      for (int i = 0; i < 10; i++) {
        expected.put(String.format("%09x", i), i * 19L);
      }

      assertEquals(expected, summary.getStatistics());
    }
  }

  private void write(BatchWriter bw, String row, String family, String qualifier, String value)
      throws MutationsRejectedException {
    Mutation m1 = new Mutation(row);
    m1.put(family, qualifier, value);
    bw.addMutation(m1);
  }

  private void write(BatchWriter bw, Map<Key,Value> expected, String row, String family,
      String qualifier, long ts, String value) throws MutationsRejectedException {
    Mutation m1 = new Mutation(row);
    m1.put(family, qualifier, ts, value);
    bw.addMutation(m1);
    expected.put(Key.builder().row(row).family(family).qualifier(qualifier).timestamp(ts).build(),
        new Value(value));
  }

  private Map<String,Long> nm(Object... entries) {
    IntPredicate evenIndex = i -> i % 2 == 0;
    return IntStream.range(0, entries.length).filter(evenIndex).boxed().collect(
        Collectors.toUnmodifiableMap(i -> (String) entries[i], i -> (Long) entries[i + 1]));
  }

  @Test
  public void testLocalityGroups() throws Exception {
    final String table = getUniqueNames(1)[0];
    try (AccumuloClient c = Accumulo.newClient().from(getClientProps()).build()) {
      NewTableConfiguration ntc = new NewTableConfiguration();
      SummarizerConfiguration sc1 = SummarizerConfiguration.builder(FamilySummarizer.class).build();
      SummarizerConfiguration sc2 = SummarizerConfiguration.builder(BasicSummarizer.class).build();
      ntc.enableSummarization(sc1, sc2);

      Map<String,Set<Text>> lgroups = new HashMap<>();
      lgroups.put("lg1", Set.of(new Text("chocolate"), new Text("coffee")));
      lgroups.put("lg2", Set.of(new Text(" broccoli "), new Text("cabbage")));
      // create a locality group that will not have data in it
      lgroups.put("lg3", Set.of(new Text(" apple "), new Text("orange")));

      ntc.setLocalityGroups(lgroups);
      c.tableOperations().create(table, ntc);

      Map<Key,Value> expected = new HashMap<>();
      try (BatchWriter bw = c.createBatchWriter(table)) {
        write(bw, expected, "order:001", "chocolate", "dark", 3L, "99kg");
        write(bw, expected, "order:001", "chocolate", "light", 4L, "94kg");
        write(bw, expected, "order:001", "coffee", "dark", 5L, "33kg");
        write(bw, expected, "order:001", "broccoli", "crowns", 6L, "2kg");
        write(bw, expected, "order:001", "cheddar", "canadian", 7L, "40kg");

        write(bw, expected, "order:653", "chocolate", "dark", 3L, "3kg");
        write(bw, expected, "order:653", "chocolate", "light", 4L, "4kg");
        write(bw, expected, "order:653", "coffee", "dark", 5L, "2kg");
        write(bw, expected, "order:653", "broccoli", "crowns", 6L, "105kg");
        write(bw, expected, "order:653", "cabbage", "heads", 7L, "199kg");
        write(bw, expected, "order:653", "cheddar", "canadian", 8L, "43kg");
      }

      List<Summary> summaries = c.tableOperations().summaries(table).flush(true).retrieve();
      assertEquals(2,
          summaries.stream().map(Summary::getSummarizerConfiguration).distinct().count());
      for (Summary summary : summaries) {
        if (summary.getSummarizerConfiguration().equals(sc1)) {
          Map<String,
              Long> expectedStats = nm("c:chocolate", 4L, "c:coffee", 2L, "c:broccoli", 2L,
                  "c:cheddar", 2L, "c:cabbage", 1L, TOO_LONG_STAT, 0L, TOO_MANY_STAT, 0L, SEEN_STAT,
                  11L, EMITTED_STAT, 11L, DELETES_IGNORED_STAT, 0L);
          assertEquals(expectedStats, summary.getStatistics());
          assertEquals(0, summary.getFileStatistics().getInaccurate());
          assertEquals(1, summary.getFileStatistics().getTotal());
        } else if (summary.getSummarizerConfiguration().equals(sc2)) {
          Map<String,Long> expectedStats =
              nm(DELETES_STAT, 0L, TOTAL_STAT, 11L, MIN_TIMESTAMP_STAT, 3L, MAX_TIMESTAMP_STAT, 8L);
          assertEquals(expectedStats, summary.getStatistics());
          assertEquals(0, summary.getFileStatistics().getInaccurate());
          assertEquals(1, summary.getFileStatistics().getTotal());
        } else {
          fail("unexpected summary config " + summary.getSummarizerConfiguration());
        }
      }

      Map<Key,Value> actual = new HashMap<>();
      c.createScanner(table, Authorizations.EMPTY)
          .forEach(e -> actual.put(e.getKey(), e.getValue()));

      assertEquals(expected, actual);
    }
  }

  @Test
  public void testExceptions() throws Exception {
    String testTableName = getUniqueNames(1)[0];
    try (AccumuloClient c = Accumulo.newClient().from(getClientProps()).build()) {

      SummaryRetriever summaryRetriever = c.tableOperations().summaries(testTableName);
      assertThrows(TableNotFoundException.class, summaryRetriever::retrieve);
      var summarizerConf = SummarizerConfiguration.builder(VisibilitySummarizer.class).build();
      assertThrows(TableNotFoundException.class,
          () -> c.tableOperations().addSummarizers(testTableName, summarizerConf));
      assertThrows(TableNotFoundException.class,
          () -> c.tableOperations().listSummarizers(testTableName));
      assertThrows(TableNotFoundException.class,
          () -> c.tableOperations().removeSummarizers(testTableName, sc -> true));

      SummarizerConfiguration sc1 =
          SummarizerConfiguration.builder(FamilySummarizer.class).setPropertyId("p1").build();
      SummarizerConfiguration sc2 =
          SummarizerConfiguration.builder(VisibilitySummarizer.class).setPropertyId("p1").build();

      c.tableOperations().create(testTableName);
      c.tableOperations().addSummarizers(testTableName, sc1);
      c.tableOperations().addSummarizers(testTableName, sc1);
      assertThrows(IllegalArgumentException.class,
          () -> c.tableOperations().addSummarizers(testTableName, sc2),
          "adding second summarizer with same id should fail");

      c.tableOperations().removeSummarizers(testTableName, sc -> true);
      assertEquals(0, c.tableOperations().listSummarizers(testTableName).size());

      assertThrows(IllegalArgumentException.class,
          () -> c.tableOperations().addSummarizers(testTableName, sc1, sc2),
          "adding two summarizers at the same time with same id should fail");
      assertEquals(0, c.tableOperations().listSummarizers(testTableName).size());

      c.tableOperations().offline(testTableName, true);
      assertThrows(TableOfflineException.class,
          () -> c.tableOperations().summaries(testTableName).retrieve());
    }
  }

  @Test
  public void testManyFiles() throws Exception {
    final String table = getUniqueNames(1)[0];
    try (AccumuloClient c = Accumulo.newClient().from(getClientProps()).build()) {

      int q = 0;

      SortedSet<Text> partitionKeys = new TreeSet<>();
      for (int split = 100_000; split < 1_000_000; split += 100_000) {
        partitionKeys.add(new Text(String.format("%06d", split)));
      }
      NewTableConfiguration ntc = new NewTableConfiguration()
          .enableSummarization(SummarizerConfiguration.builder(FamilySummarizer.class).build())
          .withSplits(partitionKeys);
      c.tableOperations().create(table, ntc);
      Map<String,Long> famCounts = new HashMap<>();

      for (int t = 0; t < 20; t++) {
        // this loop should cause a varying number of files and compactions
        try (BatchWriter bw = c.createBatchWriter(table)) {
          for (int i = 0; i < 10000; i++) {
            String row = String.format("%06d", random.nextInt(1_000_000));
            String fam = String.format("%03d", random.nextInt(100));
            String qual = String.format("%06d", q++);
            write(bw, row, fam, qual, "val");
            famCounts.merge(fam, 1L, Long::sum);
          }
        }

        List<Summary> summaries = c.tableOperations().summaries(table).flush(true).retrieve();
        assertEquals(1, summaries.size());
        CounterSummary cs = new CounterSummary(summaries.get(0));
        assertEquals(famCounts, cs.getCounters());
        FileStatistics fileStats = summaries.get(0).getFileStatistics();
        assertEquals(0, fileStats.getInaccurate());
        assertTrue(fileStats.getTotal() >= 10,
            "Saw " + fileStats.getTotal() + " files expected >=10");
      }
    }
  }
}

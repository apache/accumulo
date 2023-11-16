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

import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.time.Duration;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Predicate;
import java.util.stream.IntStream;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.CompactionConfig;
import org.apache.accumulo.core.client.admin.NewTableConfiguration;
import org.apache.accumulo.core.client.admin.PluginConfig;
import org.apache.accumulo.core.client.admin.compaction.CompactionSelector;
import org.apache.accumulo.core.clientImpl.ClientContext;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.iterators.Filter;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.IteratorUtil.IteratorScope;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.user.GrepIterator;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.metadata.RootTable;
import org.apache.accumulo.core.metadata.StoredTabletFile;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.DataFileColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.TabletColumnFamily;
import org.apache.accumulo.core.metadata.schema.TabletMetadata;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.harness.AccumuloClusterHarness;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloConfigImpl;
import org.apache.accumulo.test.VerifyIngest;
import org.apache.accumulo.test.VerifyIngest.VerifyParams;
import org.apache.accumulo.test.compaction.ExternalCompactionTestUtils;
import org.apache.accumulo.test.util.Wait;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.collect.Iterators;

public class CompactionIT extends AccumuloClusterHarness {

  public static class TestFilter extends Filter {

    int modulus = 1;

    @Override
    public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options,
        IteratorEnvironment env) throws IOException {
      super.init(source, options, env);

      // if the init function is never called at all, then not setting the modulus option should
      // cause the test to fail
      if (options.containsKey("modulus")) {
        Preconditions.checkArgument(!options.containsKey("pmodulus"));
        modulus = Integer.parseInt(options.get("modulus"));
      }

      // use when partial compaction is expected
      if (options.containsKey("pmodulus")) {
        Preconditions.checkArgument(!options.containsKey("modulus"));
        modulus = Integer.parseInt(options.get("pmodulus"));
      }
    }

    @Override
    public boolean accept(Key k, Value v) {
      return Integer.parseInt(v.toString()) % modulus == 0;
    }

  }

  public static class ErrorThrowingSelector implements CompactionSelector {

    @Override
    public void init(InitParameters iparams) {

    }

    @Override
    public Selection select(SelectionParameters sparams) {
      throw new RuntimeException("Exception for test");
    }

  }

  /**
   * CompactionSelector that selects nothing for testing
   */
  public static class EmptyCompactionSelector implements CompactionSelector {

    @Override
    public void init(InitParameters iparams) {

    }

    @Override
    public Selection select(SelectionParameters sparams) {
      return new Selection(Set.of());
    }
  }

  private static final Logger log = LoggerFactory.getLogger(CompactionIT.class);

  private static final int MAX_DATA = 1000;

  @Override
  protected Duration defaultTimeout() {
    return Duration.ofMinutes(4);
  }

  @Override
  public void configureMiniCluster(MiniAccumuloConfigImpl cfg, Configuration hadoopCoreSite) {
    cfg.setProperty(Property.INSTANCE_ZK_TIMEOUT, "15s");
    cfg.setProperty(Property.MANAGER_TABLET_GROUP_WATCHER_INTERVAL, "1s");
    cfg.setProperty(Property.COMPACTOR_MIN_JOB_WAIT_TIME, "100ms");
    cfg.setProperty(Property.COMPACTOR_MAX_JOB_WAIT_TIME, "1s");
    // use raw local file system so walogs sync and flush will work
    hadoopCoreSite.set("fs.file.impl", RawLocalFileSystem.class.getName());
  }

  private long countTablets(String tableName, Predicate<TabletMetadata> tabletTest) {
    var tableId = TableId.of(getServerContext().tableOperations().tableIdMap().get(tableName));
    try (var tabletsMetadata =
        getServerContext().getAmple().readTablets().forTable(tableId).build()) {
      return tabletsMetadata.stream().filter(tabletTest).count();
    }
  }

  @Test
  public void testSuccessfulCompaction() throws Exception {
    try (AccumuloClient c = Accumulo.newClient().from(getClientProps()).build()) {
      final String tableName = getUniqueNames(1)[0];
      c.tableOperations().create(tableName);
      c.tableOperations().setProperty(tableName, Property.TABLE_MAJC_RATIO.getKey(), "1.0");
      FileSystem fs = getFileSystem();
      Path root = new Path(cluster.getTemporaryPath(), getClass().getName());
      fs.deleteOnExit(root);
      Path testrf = new Path(root, "testrf");
      fs.deleteOnExit(testrf);
      FunctionalTestUtils.createRFiles(c, fs, testrf.toString(), 500000, 59, 4);

      c.tableOperations().importDirectory(testrf.toString()).to(tableName).load();
      int beforeCount = countFiles(c);

      final AtomicBoolean fail = new AtomicBoolean(false);
      final int THREADS = 5;
      for (int count = 0; count < THREADS; count++) {
        ExecutorService executor = Executors.newFixedThreadPool(THREADS);
        final int span = 500000 / 59;
        for (int i = 0; i < 500000; i += 500000 / 59) {
          final int finalI = i;
          Runnable r = () -> {
            try {
              VerifyParams params = new VerifyParams(getClientProps(), tableName, span);
              params.startRow = finalI;
              params.random = 56;
              params.dataSize = 50;
              params.cols = 1;
              VerifyIngest.verifyIngest(c, params);
            } catch (Exception ex) {
              log.warn("Got exception verifying data", ex);
              fail.set(true);
            }
          };
          executor.execute(r);
        }
        executor.shutdown();
        executor.awaitTermination(defaultTimeout().toSeconds(), SECONDS);
        assertFalse(fail.get(),
            "Failed to successfully run all threads, Check the test output for error");
      }

      int finalCount = countFiles(c);
      assertTrue(finalCount < beforeCount);
      try {
        getClusterControl().adminStopAll();
      } finally {
        // Make sure the internal state in the cluster is reset (e.g. processes in MAC)
        getCluster().stop();
        if (getClusterType() == ClusterType.STANDALONE) {
          // Then restart things for the next test if it's a standalone
          getCluster().start();
        }
      }
    }
  }

  @Test
  public void testMultiStepCompactionThatDeletesAll() throws Exception {

    // There was a bug where user compactions would never complete when : the tablet had to be
    // compacted in multiple passes AND the intermediate passes produced no output.

    try (AccumuloClient c = Accumulo.newClient().from(getClientProps()).build()) {
      final String tableName = getUniqueNames(1)[0];
      c.tableOperations().create(tableName);
      c.tableOperations().setProperty(tableName, Property.TABLE_MAJC_RATIO.getKey(), "100.0");

      var beforeCount = countFiles(c);

      final int NUM_ENTRIES_AND_FILES = 60;

      try (var writer = c.createBatchWriter(tableName)) {
        for (int i = 0; i < NUM_ENTRIES_AND_FILES; i++) {
          Mutation m = new Mutation("r" + i);
          m.put("f1", "q1", "v" + i);
          writer.addMutation(m);
          writer.flush();
          // flush often to create multiple files to compact
          c.tableOperations().flush(tableName, null, null, true);
        }
      }

      try (var scanner = c.createScanner(tableName)) {
        assertEquals(NUM_ENTRIES_AND_FILES, scanner.stream().count());
      }

      var afterCount = countFiles(c);

      assertTrue(afterCount >= beforeCount + NUM_ENTRIES_AND_FILES);

      CompactionConfig comactionConfig = new CompactionConfig();
      // configure an iterator that drops all data
      IteratorSetting iter = new IteratorSetting(100, GrepIterator.class);
      GrepIterator.setTerm(iter, "keep");
      comactionConfig.setIterators(List.of(iter));
      comactionConfig.setWait(true);
      c.tableOperations().compact(tableName, comactionConfig);

      try (var scanner = c.createScanner(tableName)) {
        assertEquals(0, scanner.stream().count());
      }

      var finalCount = countFiles(c);
      assertTrue(finalCount <= beforeCount);

      ExternalCompactionTestUtils.assertNoCompactionMetadata(getServerContext(), tableName);
    }
  }

  @Test
  public void testSelectNoFiles() throws Exception {

    // Test a compaction selector that selects no files. In this case there is no work to,
    // so we want to ensure it does not hang

    try (AccumuloClient c = Accumulo.newClient().from(getClientProps()).build()) {
      String tableName = getUniqueNames(1)[0];
      c.tableOperations().create(tableName);

      writeFlush(c, tableName, "a");
      writeFlush(c, tableName, "b");

      CompactionConfig config = new CompactionConfig()
          .setSelector(new PluginConfig(EmptyCompactionSelector.class.getName(), Map.of()))
          .setWait(true);
      c.tableOperations().compact(tableName, config);

      assertEquals(Set.of("a", "b"), getRows(c, tableName));

      ExternalCompactionTestUtils.assertNoCompactionMetadata(getServerContext(), tableName);
    }

  }

  @Test
  public void testConcurrentWithIterators() throws Exception {
    try (AccumuloClient c = Accumulo.newClient().from(getClientProps()).build()) {

      String tableName = getUniqueNames(1)[0];
      c.tableOperations().create(tableName);
      try (BatchWriter bw = c.createBatchWriter(tableName)) {
        for (int i = 0; i < MAX_DATA; i++) {
          Mutation m = new Mutation(String.format("r:%04d", i));
          m.put("", "", "" + i);
          bw.addMutation(m);
        }
      }

      TreeSet<Text> splits = new TreeSet<>();
      for (int i = 10; i < MAX_DATA; i += 10) {
        splits.add(new Text(String.format("r:%04d", i)));
      }

      c.tableOperations().addSplits(tableName, splits);

      // Start three concurrent compactions with different iterators that filter different data.
      // Expect all to run on each tablet in some order.
      for (int modulus : List.of(2, 3, 5)) {
        IteratorSetting iterSetting = new IteratorSetting(100, TestFilter.class);
        // make sure iterator options make it to compactor process
        iterSetting.addOption("modulus", modulus + "");
        CompactionConfig config =
            new CompactionConfig().setIterators(List.of(iterSetting)).setWait(false);
        c.tableOperations().compact(tableName, config);
      }

      // only expected to see numbers that are divisible by 2, 3, and 5 in the data after all
      // compactions run
      var expected = IntStream.range(0, MAX_DATA).filter(i -> i % (2 * 3 * 5) == 0)
          .mapToObj(i -> String.format("r:%04d", i)).collect(toSet());

      Supplier<Set<String>> actualSupplier = () -> {
        try (var scanner = c.createScanner(tableName)) {
          return scanner.stream().map(e -> e.getKey().getRowData().toString()).collect(toSet());
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      };

      // wait until the filtering done by all three compactions is seen
      while (!expected.equals(actualSupplier.get())) {
        Thread.sleep(250);
      }

      // eventually the compactions should clean up all of their metadata, wait for this to happen
      while (countTablets(tableName,
          tabletMetadata -> !tabletMetadata.getCompacted().isEmpty()
              || tabletMetadata.getSelectedFiles() != null
              || !tabletMetadata.getExternalCompactions().isEmpty())
          > 0) {
        Thread.sleep(250);
      }
    }
  }

  @Test
  public void testConcurrentSplit() throws Exception {
    // test compaction and split running concurrently
    try (AccumuloClient c = Accumulo.newClient().from(getClientProps()).build()) {

      String tableName = getUniqueNames(1)[0];
      c.tableOperations().create(tableName);
      try (BatchWriter bw = c.createBatchWriter(tableName)) {
        for (int i = 0; i < MAX_DATA; i++) {
          Mutation m = new Mutation(String.format("r:%04d", i));
          m.put("", "", "" + i);
          bw.addMutation(m);
        }
      }

      TreeSet<Text> splits = new TreeSet<>();
      for (int i = 100; i < MAX_DATA; i += 100) {
        splits.add(new Text(String.format("r:%04d", i)));
      }

      // add 10 splits to the table
      c.tableOperations().addSplits(tableName, splits);

      for (int modulus : List.of(2, 3)) {
        IteratorSetting iterSetting = new IteratorSetting(100, TestFilter.class);
        // make sure iterator options make it to compactor process
        iterSetting.addOption("modulus", modulus + "");
        CompactionConfig config =
            new CompactionConfig().setIterators(List.of(iterSetting)).setWait(false);
        c.tableOperations().compact(tableName, config);
      }

      splits = new TreeSet<>();
      for (int i = 50; i < MAX_DATA; i += 50) {
        splits.add(new Text(String.format("r:%04d", i)));
      }

      // wait a bit for some tablets to have files selected, it possible the compaction have
      // completed before this so do not wait long
      Wait.waitFor(
          () -> countTablets(tableName, tabletMetadata -> tabletMetadata.getSelectedFiles() != null)
              > 0,
          3000, 10);

      // add 10 more splits to the table
      c.tableOperations().addSplits(tableName, splits);

      splits = new TreeSet<>();
      for (int i = 10; i < MAX_DATA; i += 10) {
        splits.add(new Text(String.format("r:%04d", i)));
      }

      // wait a bit for some tablets to be compacted, it possible the compaction have completed
      // before this so do not wait long
      Wait.waitFor(
          () -> countTablets(tableName, tabletMetadata -> !tabletMetadata.getCompacted().isEmpty())
              > 0,
          3000, 10);

      // add 80 more splits to the table
      c.tableOperations().addSplits(tableName, splits);

      assertEquals(99, c.tableOperations().listSplits(tableName).size());

      // only expect to see numbers that are divisible by 2 and 3 in the data after all
      // compactions run
      var expected = IntStream.range(0, MAX_DATA).filter(i -> i % (2 * 3) == 0)
          .mapToObj(i -> String.format("r:%04d", i)).collect(toSet());

      Supplier<Set<String>> actualSupplier = () -> {
        try (var scanner = c.createScanner(tableName)) {
          return scanner.stream().map(e -> e.getKey().getRowData().toString()).collect(toSet());
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      };

      // wait until the filtering done by all three compactions is seen
      while (!expected.equals(actualSupplier.get())) {
        Thread.sleep(250);
      }

      // eventually the compactions should clean up all of their metadata, wait for this to happen
      while (countTablets(tableName,
          tabletMetadata -> !tabletMetadata.getCompacted().isEmpty()
              || tabletMetadata.getSelectedFiles() != null
              || !tabletMetadata.getExternalCompactions().isEmpty())
          > 0) {
        Thread.sleep(250);
      }
    }
  }

  @Test
  public void testMetadataCompactions() throws Exception {
    // The metadata and root table have default config that causes them to compact down to one
    // tablet. This test verifies that both tables compact to one file after a flush.
    try (AccumuloClient c = Accumulo.newClient().from(getClientProps()).build()) {
      String[] tableNames = getUniqueNames(2);

      // creating a user table should cause a write to the metadata table
      c.tableOperations().create(tableNames[0]);

      var mfiles1 = getServerContext().getAmple().readTablets().forTable(MetadataTable.ID).build()
          .iterator().next().getFiles();
      var rootFiles1 = getServerContext().getAmple().readTablet(RootTable.EXTENT).getFiles();

      log.debug("mfiles1 {}",
          mfiles1.stream().map(StoredTabletFile::getFileName).collect(toList()));
      log.debug("rootFiles1 {}",
          rootFiles1.stream().map(StoredTabletFile::getFileName).collect(toList()));

      c.tableOperations().flush(MetadataTable.NAME, null, null, true);
      c.tableOperations().flush(RootTable.NAME, null, null, true);

      // create another table to cause more metadata writes
      c.tableOperations().create(tableNames[1]);
      try (var writer = c.createBatchWriter(tableNames[1])) {
        var m = new Mutation("r1");
        m.put("f1", "q1", "v1");
        writer.addMutation(m);
      }
      c.tableOperations().flush(tableNames[1], null, null, true);

      // create another metadata file
      c.tableOperations().flush(MetadataTable.NAME, null, null, true);
      c.tableOperations().flush(RootTable.NAME, null, null, true);

      // The multiple flushes should create multiple files. We expect the file sets to changes and
      // eventually equal one.

      Wait.waitFor(() -> {
        var mfiles2 = getServerContext().getAmple().readTablets().forTable(MetadataTable.ID).build()
            .iterator().next().getFiles();
        log.debug("mfiles2 {}",
            mfiles2.stream().map(StoredTabletFile::getFileName).collect(toList()));
        return mfiles2.size() == 1 && !mfiles2.equals(mfiles1);
      });

      Wait.waitFor(() -> {
        var rootFiles2 = getServerContext().getAmple().readTablet(RootTable.EXTENT).getFiles();
        log.debug("rootFiles2 {}",
            rootFiles2.stream().map(StoredTabletFile::getFileName).collect(toList()));
        return rootFiles2.size() == 1 && !rootFiles2.equals(rootFiles1);
      });

      var entries = c.createScanner(tableNames[1]).stream()
          .map(e -> e.getKey().getRow() + ":" + e.getKey().getColumnFamily() + ":"
              + e.getKey().getColumnQualifier() + ":" + e.getValue())
          .collect(toSet());

      assertEquals(Set.of("r1:f1:q1:v1"), entries);
    }
  }

  @Test
  public void testSystemCompactionsRefresh() throws Exception {
    // This test ensures that after a system compaction occurs that a tablet will refresh its files.

    String tableName = getUniqueNames(1)[0];
    try (final AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {

      // configure tablet compaction iterator that filters out data not divisible by 7 and cause
      // table to compact to one file

      var ntc = new NewTableConfiguration();
      IteratorSetting iterSetting = new IteratorSetting(100, TestFilter.class);
      iterSetting.addOption("modulus", 7 + "");
      ntc.attachIterator(iterSetting, EnumSet.of(IteratorScope.majc));
      ntc.setProperties(Map.of(Property.TABLE_MAJC_RATIO.getKey(), "20"));

      client.tableOperations().create(tableName, ntc);

      Set<Integer> expectedData = new HashSet<>();

      // Insert MAX_DATA rows
      try (BatchWriter bw = client.createBatchWriter(tableName)) {
        for (int i = 0; i < MAX_DATA; i++) {
          Mutation m = new Mutation(String.format("r:%04d", i));
          m.put("", "", "" + i);
          bw.addMutation(m);

          if (i % 75 == 0) {
            // create many files as this will cause a system compaction
            bw.flush();
            client.tableOperations().flush(tableName, null, null, true);
          }

          if (i % 7 == 0) {
            expectedData.add(i);
          }
        }
      }

      client.tableOperations().flush(tableName, null, null, true);

      // there should be no system compactions yet and no data should be filtered, so should see all
      // data that was written
      try (Scanner scanner = client.createScanner(tableName)) {
        assertEquals(MAX_DATA, scanner.stream().count());
      }

      // set the compaction ratio 1 which should cause system compactions to filter data and refresh
      // the tablets files
      client.tableOperations().setProperty(tableName, Property.TABLE_MAJC_RATIO.getKey(), "1");

      var tableId = TableId.of(client.tableOperations().tableIdMap().get(tableName));
      var extent = new KeyExtent(tableId, null, null);

      // wait for the compactions to filter data and refresh that tablets files
      Wait.waitFor(() -> {
        var tabletMeta = ((ClientContext) client).getAmple().readTablet(extent);
        var files = tabletMeta.getFiles();
        log.debug("Current files {}",
            files.stream().map(StoredTabletFile::getFileName).collect(toList()));

        if (files.size() == 1) {
          // Once only one file exists the tablet may still have not gotten the refresh message
          // because its sent after the metadata update. After the tablet is down to one file should
          // eventually see the tablet refresh its files.
          try (Scanner scanner = client.createScanner(tableName)) {
            var acutalData = scanner.stream().map(e -> Integer.parseInt(e.getValue().toString()))
                .collect(toSet());
            return acutalData.equals(expectedData);
          }
        }
        return false;
      });
    }
  }

  private int countFiles(AccumuloClient c) throws Exception {
    try (Scanner s = c.createScanner(MetadataTable.NAME, Authorizations.EMPTY)) {
      s.fetchColumnFamily(new Text(TabletColumnFamily.NAME));
      s.fetchColumnFamily(new Text(DataFileColumnFamily.NAME));
      return Iterators.size(s.iterator());
    }
  }

  private Set<String> getRows(AccumuloClient c, String tableName) throws TableNotFoundException {
    Set<String> rows = new HashSet<>();
    try (Scanner scanner = c.createScanner(tableName, Authorizations.EMPTY)) {
      for (Entry<Key,Value> entry : scanner) {
        rows.add(entry.getKey().getRowData().toString());
      }
    }
    return rows;
  }

  private void writeFlush(AccumuloClient client, String tablename, String row) throws Exception {
    try (BatchWriter bw = client.createBatchWriter(tablename)) {
      Mutation m = new Mutation(row);
      m.put("", "", "");
      bw.addMutation(m);
    }
    client.tableOperations().flush(tablename, null, null, true);
  }
}

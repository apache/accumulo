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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;
import java.util.stream.IntStream;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.ActiveCompaction;
import org.apache.accumulo.core.client.admin.ActiveCompaction.CompactionHost;
import org.apache.accumulo.core.client.admin.CompactionConfig;
import org.apache.accumulo.core.client.admin.NewTableConfiguration;
import org.apache.accumulo.core.client.admin.PluginConfig;
import org.apache.accumulo.core.client.admin.compaction.CompactionConfigurer;
import org.apache.accumulo.core.client.admin.compaction.CompactionSelector;
import org.apache.accumulo.core.clientImpl.ClientContext;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.file.rfile.bcfile.PrintBCInfo;
import org.apache.accumulo.core.iterators.Filter;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.IteratorUtil.IteratorScope;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.user.GrepIterator;
import org.apache.accumulo.core.metadata.AccumuloTable;
import org.apache.accumulo.core.metadata.RootTable;
import org.apache.accumulo.core.metadata.StoredTabletFile;
import org.apache.accumulo.core.metadata.schema.MetadataSchema;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.DataFileColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.TabletColumnFamily;
import org.apache.accumulo.core.metadata.schema.TabletMetadata;
import org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType;
import org.apache.accumulo.core.metadata.schema.TabletsMetadata;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.spi.compaction.CompactionKind;
import org.apache.accumulo.core.spi.compaction.CompactionPlan;
import org.apache.accumulo.core.spi.compaction.CompactionPlanner;
import org.apache.accumulo.core.spi.compaction.RatioBasedCompactionPlanner;
import org.apache.accumulo.core.spi.compaction.SimpleCompactionDispatcher;
import org.apache.accumulo.core.util.compaction.CompactionJobImpl;
import org.apache.accumulo.core.util.compaction.ExternalCompactionUtil;
import org.apache.accumulo.minicluster.ServerType;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.test.VerifyIngest;
import org.apache.accumulo.test.VerifyIngest.VerifyParams;
import org.apache.accumulo.test.compaction.ExternalCompactionTestUtils;
import org.apache.accumulo.test.util.Wait;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.thrift.transport.TTransportException;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.collect.Iterators;
import com.google.common.net.HostAndPort;

public class CompactionIT extends CompactionBaseIT {

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

  /**
   * A CompactionConfigurer that can be used to configure the compression type used for intermediate
   * and final compactions. An intermediate compaction is a compaction whose result is short-lived.
   * For instance, if 10 files are selected for compaction, 5 of these files are compacted to a file
   * 'f0', then f0 is compacted with the 5 remaining files creating file 'f1', then f0 would be an
   * intermediate file and f1 would be the final file.
   */
  public static class CompressionTypeConfigurer implements CompactionConfigurer {
    public static final String COMPRESS_TYPE_KEY = Property.TABLE_FILE_COMPRESSION_TYPE.getKey();
    public static final String FINAL_COMPRESS_TYPE_KEY = "final.compress.type";
    public static final String INTERMEDIATE_COMPRESS_TYPE_KEY = "intermediate.compress.type";
    private String finalCompressTypeVal;
    private String interCompressTypeVal;

    @Override
    public void init(InitParameters iparams) {
      var options = iparams.getOptions();
      String finalCompressTypeVal = options.get(FINAL_COMPRESS_TYPE_KEY);
      String interCompressTypeVal = options.get(INTERMEDIATE_COMPRESS_TYPE_KEY);
      if (finalCompressTypeVal != null && interCompressTypeVal != null) {
        this.finalCompressTypeVal = finalCompressTypeVal;
        this.interCompressTypeVal = interCompressTypeVal;
      } else {
        throw new IllegalArgumentException(
            "Must set " + FINAL_COMPRESS_TYPE_KEY + " and " + INTERMEDIATE_COMPRESS_TYPE_KEY);
      }
    }

    @Override
    public Overrides override(InputParameters params) {
      var inputFiles = params.getInputFiles();
      var selectedFiles = params.getSelectedFiles();
      // If this is the final compaction, set the compression type to the value set for
      // finalCompressTypeVal
      // If this is an intermediate compaction, set the compression type to the value set for
      // interCompressTypeVal
      if (selectedFiles.equals(inputFiles instanceof Set ? inputFiles : Set.copyOf(inputFiles))) {
        return new Overrides(Map.of(COMPRESS_TYPE_KEY, finalCompressTypeVal));
      } else {
        return new Overrides(Map.of(COMPRESS_TYPE_KEY, interCompressTypeVal));
      }
    }

  }

  public static class TestPlanner implements CompactionPlanner {
    private static final Logger LOG = LoggerFactory.getLogger(TestPlanner.class);

    RatioBasedCompactionPlanner p = new RatioBasedCompactionPlanner();

    @Override
    public void init(InitParameters params) {
      p.init(params);
    }

    @Override
    public CompactionPlan makePlan(PlanningParameters params) {
      return () -> p.makePlan(params).getJobs().stream().map(job -> {
        LOG.debug("Plan job priority is {}:{}", job.getKind(), job.getPriority());
        return new CompactionJobImpl(
            job.getKind() == CompactionKind.SYSTEM ? Short.MAX_VALUE : job.getPriority(),
            job.getGroup(), job.getFiles(), job.getKind(), Optional.empty());
      }).collect(toList());
    }
  }

  private static final Logger log = LoggerFactory.getLogger(CompactionIT.class);

  @Override
  protected Duration defaultTimeout() {
    return Duration.ofMinutes(4);
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
      int beforeCount = countFiles(c, tableName);

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

      int finalCount = countFiles(c, tableName);
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
      c.tableOperations().setProperty(tableName, Property.TABLE_FILE_MAX.getKey(), "1001");
      c.tableOperations().setProperty(tableName, Property.TABLE_MAJC_RATIO.getKey(), "100.0");

      var beforeCount = countFiles(c, tableName);

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

      var afterCount = countFiles(c, tableName);

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

      var finalCount = countFiles(c, tableName);
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

      // Wait a bit for some tablets to have files selected, it possible the compaction have
      // completed before this so do not wait long. Once files are selected compactions can start.
      // This speed bump is an attempt to increase the chance that splits and compactions run
      // concurrently. Wait.waitFor() is not used here because it will throw an exception if the
      // time limit is exceeded.
      long startTime = System.nanoTime();
      while (System.nanoTime() - startTime < SECONDS.toNanos(3)
          && countTablets(tableName, tabletMetadata -> tabletMetadata.getSelectedFiles() != null)
              == 0) {
        Thread.sleep(10);
      }

      // add 10 more splits to the table
      c.tableOperations().addSplits(tableName, splits);

      splits = new TreeSet<>();
      for (int i = 10; i < MAX_DATA; i += 10) {
        splits.add(new Text(String.format("r:%04d", i)));
      }

      // Wait a bit for some tablets to be compacted, it possible the compaction have completed
      // before this so do not wait long. Wait.waitFor() is not used here because it will throw an
      // exception if the time limit is exceeded. This is just a speed bump, its ok if the condition
      // is not met within the time limit.
      startTime = System.nanoTime();
      while (System.nanoTime() - startTime < SECONDS.toNanos(3)
          && countTablets(tableName, tabletMetadata -> !tabletMetadata.getCompacted().isEmpty())
              == 0) {
        Thread.sleep(10);
      }

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

      Set<StoredTabletFile> mfiles1;
      try (TabletsMetadata tabletsMetadata = getServerContext().getAmple().readTablets()
          .forTable(AccumuloTable.METADATA.tableId()).build()) {
        mfiles1 = tabletsMetadata.iterator().next().getFiles();
      }
      var rootFiles1 = getServerContext().getAmple().readTablet(RootTable.EXTENT).getFiles();

      log.debug("mfiles1 {}",
          mfiles1.stream().map(StoredTabletFile::getFileName).collect(toList()));
      log.debug("rootFiles1 {}",
          rootFiles1.stream().map(StoredTabletFile::getFileName).collect(toList()));

      c.tableOperations().flush(AccumuloTable.METADATA.tableName(), null, null, true);
      c.tableOperations().flush(AccumuloTable.ROOT.tableName(), null, null, true);

      // create another table to cause more metadata writes
      c.tableOperations().create(tableNames[1]);
      try (var writer = c.createBatchWriter(tableNames[1])) {
        var m = new Mutation("r1");
        m.put("f1", "q1", "v1");
        writer.addMutation(m);
      }
      c.tableOperations().flush(tableNames[1], null, null, true);

      // create another metadata file
      c.tableOperations().flush(AccumuloTable.METADATA.tableName(), null, null, true);
      c.tableOperations().flush(AccumuloTable.ROOT.tableName(), null, null, true);

      // The multiple flushes should create multiple files. We expect the file sets to changes and
      // eventually equal one.

      Wait.waitFor(() -> {
        Set<StoredTabletFile> mfiles2;
        try (TabletsMetadata tabletsMetadata = getServerContext().getAmple().readTablets()
            .forTable(AccumuloTable.METADATA.tableId()).build()) {
          mfiles2 = tabletsMetadata.iterator().next().getFiles();
        }
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

  @Test
  public void testGetSelectedFilesForCompaction() throws Exception {

    // Tests CompactionConfigurer.InputParameters.getSelectedFiles()

    String tableName = this.getUniqueNames(1)[0];
    // Disable GC so intermediate compaction files are not deleted
    getCluster().getClusterControl().stopAllServers(ServerType.GARBAGE_COLLECTOR);

    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
      Map<String,String> props = new HashMap<>();
      props.put(Property.TABLE_FILE_COMPRESSION_TYPE.getKey(), "none");
      // This is done to avoid system compactions - we want to do all the compactions ourselves
      props.put("table.compaction.dispatcher.opts.service.system", "nonexitant");
      NewTableConfiguration ntc = new NewTableConfiguration().setProperties(props);
      client.tableOperations().create(tableName, ntc);

      // The following will create 4 small and 4 large RFiles
      // The 4 small files will be compacted into one file (an "intermediate compaction" file)
      // Then, this file will be compacted with the 4 large files, creating the final compaction
      // file
      byte[] largeData = new byte[1_000_000];
      byte[] smallData = new byte[100_000];
      final int numFiles = 8;
      Arrays.fill(largeData, (byte) 65);
      Arrays.fill(smallData, (byte) 65);
      try (var writer = client.createBatchWriter(tableName)) {
        for (int i = 0; i < numFiles; i++) {
          Mutation mut = new Mutation("r" + i);
          if (i < numFiles / 2) {
            mut.at().family("f").qualifier("q").put(largeData);
          } else {
            mut.at().family("f").qualifier("q").put(smallData);
          }
          writer.addMutation(mut);
          writer.flush();
          client.tableOperations().flush(tableName, null, null, true);
        }
      }

      client.tableOperations().compact(tableName,
          new CompactionConfig().setWait(true)
              .setConfigurer(new PluginConfig(CompressionTypeConfigurer.class.getName(),
                  Map.of(CompressionTypeConfigurer.FINAL_COMPRESS_TYPE_KEY, "snappy",
                      CompressionTypeConfigurer.INTERMEDIATE_COMPRESS_TYPE_KEY, "gz"))));

      var tableId = TableId.of(client.tableOperations().tableIdMap().get(tableName));
      // The directory of the RFiles
      java.nio.file.Path rootPath = null;
      // The path to the final compaction RFile (located within rootPath)
      java.nio.file.Path finalCompactionFilePath = null;
      int count = 0;
      try (var tabletsMeta =
          TabletsMetadata.builder(client).forTable(tableId).fetch(ColumnType.FILES).build()) {
        for (TabletMetadata tm : tabletsMeta) {
          for (StoredTabletFile stf : tm.getFiles()) {
            // Since the 8 files should be compacted down to 1 file, these should only be set once
            finalCompactionFilePath = Paths.get(stf.getPath().toUri().getRawPath());
            rootPath = Paths.get(stf.getPath().getParent().toUri().getRawPath());
            count++;
          }
        }
      }
      assertEquals(1, count);
      assertNotNull(finalCompactionFilePath);
      assertNotNull(rootPath);
      String finalCompactionFile = finalCompactionFilePath.toString();
      // The following will find the intermediate compaction file in the root path.
      // Intermediate compaction files begin with 'C' and end with '.rf'
      final String[] interCompactionFile = {null};
      Files.walkFileTree(rootPath, new SimpleFileVisitor<java.nio.file.Path>() {
        @Override
        public FileVisitResult visitFile(java.nio.file.Path file, BasicFileAttributes attrs)
            throws IOException {
          String regex = "^C.*\\.rf$";
          java.nio.file.Path fileName = (file != null) ? file.getFileName() : null;
          if (fileName != null && fileName.toString().matches(regex)) {
            interCompactionFile[0] = file.toString();
            return FileVisitResult.TERMINATE;
          }
          return FileVisitResult.CONTINUE;
        }
      });
      assertNotNull(interCompactionFile[0]);
      String[] args = new String[3];
      args[0] = "--props";
      args[1] = getCluster().getAccumuloPropertiesPath();
      args[2] = finalCompactionFile;
      PrintBCInfo bcInfo = new PrintBCInfo(args);
      String finalCompressionType = bcInfo.getCompressionType();
      // The compression type used on the final compaction file should be 'snappy'
      assertEquals("snappy", finalCompressionType);
      args[2] = interCompactionFile[0];
      bcInfo = new PrintBCInfo(args);
      String interCompressionType = bcInfo.getCompressionType();
      // The compression type used on the intermediate compaction file should be 'gz'
      assertEquals("gz", interCompressionType);
    } finally {
      // Re-enable GC
      getCluster().getClusterControl().startAllServers(ServerType.GARBAGE_COLLECTOR);
    }
  }

  @Test
  public void testDeleteCompactionService() throws Exception {
    try (AccumuloClient c = Accumulo.newClient().from(getClientProps()).build()) {
      var uniqueNames = getUniqueNames(2);
      String table1 = uniqueNames[0];
      String table2 = uniqueNames[1];

      // create a compaction service named deleteme
      c.instanceOperations().setProperty(
          Property.COMPACTION_SERVICE_PREFIX.getKey() + "deleteme.planner",
          RatioBasedCompactionPlanner.class.getName());
      c.instanceOperations().setProperty(
          Property.COMPACTION_SERVICE_PREFIX.getKey() + "deleteme.planner.opts.groups",
          ("[{'group':'" + COMPACTOR_GROUP_1 + "'}]").replaceAll("'", "\""));

      // create a compaction service named keepme
      c.instanceOperations().setProperty(
          Property.COMPACTION_SERVICE_PREFIX.getKey() + "keepme.planner",
          RatioBasedCompactionPlanner.class.getName());
      c.instanceOperations().setProperty(
          Property.COMPACTION_SERVICE_PREFIX.getKey() + "keepme.planner.opts.groups",
          ("[{'group':'" + COMPACTOR_GROUP_2 + "'}]").replaceAll("'", "\""));

      // create a table that uses the compaction service deleteme
      Map<String,String> props = new HashMap<>();
      props.put(Property.TABLE_COMPACTION_DISPATCHER.getKey(),
          SimpleCompactionDispatcher.class.getName());
      props.put(Property.TABLE_COMPACTION_DISPATCHER_OPTS.getKey() + "service", "deleteme");
      c.tableOperations().create(table1, new NewTableConfiguration().setProperties(props));

      // create a table that uses the compaction service keepme
      props.clear();
      props.put(Property.TABLE_COMPACTION_DISPATCHER.getKey(),
          SimpleCompactionDispatcher.class.getName());
      props.put(Property.TABLE_COMPACTION_DISPATCHER_OPTS.getKey() + "service", "keepme");
      c.tableOperations().create(table2, new NewTableConfiguration().setProperties(props));

      try (var writer1 = c.createBatchWriter(table1); var writer2 = c.createBatchWriter(table2)) {
        for (int i = 0; i < 10; i++) {
          Mutation m = new Mutation("" + i);
          m.put("f", "q", "" + i);
          writer1.addMutation(m);
          writer2.addMutation(m);
        }
      }

      c.tableOperations().compact(table1, new CompactionConfig().setWait(true));
      c.tableOperations().compact(table2, new CompactionConfig().setWait(true));

      // delete the compaction service deleteme
      c.instanceOperations()
          .removeProperty(Property.COMPACTION_SERVICE_PREFIX.getKey() + "deleteme.planner");
      c.instanceOperations().removeProperty(
          Property.COMPACTION_SERVICE_PREFIX.getKey() + "deleteme.planner.opts.groups");

      // add a new compaction service named newcs
      c.instanceOperations().setProperty(
          Property.COMPACTION_SERVICE_PREFIX.getKey() + "newcs.planner",
          RatioBasedCompactionPlanner.class.getName());
      c.instanceOperations().setProperty(
          Property.COMPACTION_SERVICE_PREFIX.getKey() + "newcs.planner.opts.groups",
          ("[{'group':'" + COMPACTOR_GROUP_1 + "'}]").replaceAll("'", "\""));

      // set table 1 to a compaction service newcs
      c.tableOperations().setProperty(table1,
          Property.TABLE_COMPACTION_DISPATCHER_OPTS.getKey() + "service", "newcs");

      // ensure tables can still compact and are not impacted by the deleted compaction service
      for (int i = 0; i < 10; i++) {
        c.tableOperations().compact(table1, new CompactionConfig().setWait(true));
        c.tableOperations().compact(table2, new CompactionConfig().setWait(true));

        try (var scanner = c.createScanner(table1)) {
          assertEquals(9 * 10 / 2, scanner.stream().map(Entry::getValue)
              .mapToInt(v -> Integer.parseInt(v.toString())).sum());
        }
        try (var scanner = c.createScanner(table2)) {
          assertEquals(9 * 10 / 2, scanner.stream().map(Entry::getValue)
              .mapToInt(v -> Integer.parseInt(v.toString())).sum());
        }

        Thread.sleep(100);
      }
    }
  }

  @Test
  public void testCancelUserCompactionTimeoutExceeded() throws Exception {
    testCancelUserCompactionTimeout(true);
  }

  @Test
  public void testCancelUserCompactionTimeoutNotExceeded() throws Exception {
    testCancelUserCompactionTimeout(false);
  }

  private void testCancelUserCompactionTimeout(boolean timeout) throws Exception {

    var uniqueNames = getUniqueNames(2);
    String table1 = uniqueNames[0];
    String table2 = uniqueNames[1];

    try (final AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {

      // create a compaction service that uses a Planner that will schedule system jobs
      // at a higher priority than user jobs
      client.instanceOperations().setProperty(
          Property.COMPACTION_SERVICE_PREFIX.getKey() + "testcancel.planner",
          TestPlanner.class.getName());
      client.instanceOperations().setProperty(
          Property.COMPACTION_SERVICE_PREFIX.getKey() + "testcancel.planner.opts.groups",
          ("[{'group':'" + COMPACTOR_GROUP_1 + "'}]").replaceAll("'", "\""));

      // create two tables that uses the compaction service
      Map<String,String> props = new HashMap<>();
      props.put(Property.TABLE_COMPACTION_DISPATCHER.getKey(),
          SimpleCompactionDispatcher.class.getName());
      props.put(Property.TABLE_COMPACTION_DISPATCHER_OPTS.getKey() + "service", "testcancel");
      // Disable system compactions to start for these tables
      props.put(Property.TABLE_MAJC_RATIO.getKey(), "20");

      // configure tablet compaction iterator that slows compaction down
      var ntc = new NewTableConfiguration();
      IteratorSetting iterSetting = new IteratorSetting(50, SlowIterator.class);
      SlowIterator.setSleepTime(iterSetting, 5);
      ntc.attachIterator(iterSetting, EnumSet.of(IteratorScope.majc));
      ntc.setProperties(props);

      // Create two tables and write some data
      client.tableOperations().create(table1, ntc);
      client.tableOperations().create(table2, ntc);
      writeRows((ClientContext) client, table1, MAX_DATA, true);
      writeRows((ClientContext) client, table2, MAX_DATA, true);

      var ctx = getCluster().getServerContext();
      Optional<HostAndPort> coordinatorHost = ExternalCompactionUtil.findCompactionCoordinator(ctx);
      if (coordinatorHost.isEmpty()) {
        throw new TTransportException("Unable to get CompactionCoordinator address from ZooKeeper");
      }

      // Start a compaction for table2, this is done so that the compactor will be busy
      // and new jobs will queue up and wait
      client.tableOperations().compact(table2, new CompactionConfig().setWait(false));

      var tableId = TableId.of(client.tableOperations().tableIdMap().get(table1));
      var extent = new KeyExtent(tableId, null, null);

      // If timeout is true then set a short timeout so the system job can cancel the user job
      // Otherwise the long timeout should prevent the system from clearing the selected files
      var expiration = timeout ? "100ms" : "100s";
      client.tableOperations().setProperty(table1,
          Property.TABLE_COMPACTION_SELECTION_EXPIRATION.getKey(), expiration);

      // Submit a user job for table1 that will be put on the queue and waiting
      // for the current job to finish
      client.tableOperations().compact(table1, new CompactionConfig().setWait(false));
      // Wait for the fate operation to write selectedFiles
      Wait.waitFor(() -> {
        var tabletMeta = ((ClientContext) client).getAmple().readTablet(extent);
        var selectedFiles = tabletMeta.getSelectedFiles();
        if (selectedFiles != null) {
          return !selectedFiles.getFiles().isEmpty();
        }
        return false;
      }, Wait.MAX_WAIT_MILLIS, 10);

      // Change the ratio so a system compaction will attempt to be scheduled for table 1
      client.tableOperations().setProperty(table1, Property.TABLE_MAJC_RATIO.getKey(), "1");

      if (timeout) {
        // Because of the custom planner, the system compaction should now take priority
        // System compactions were previously not eligible to run if selectedFiles existed
        // for a user compaction already (and they overlapped). But now system compaction jobs
        // are eligible to run if the user compaction has not started or completed any jobs
        // and the expiration period has been exceeded.
        // When this happens the system compaction will delete the selectedFiles column
        Wait.waitFor(() -> {
          var tabletMeta = ((ClientContext) client).getAmple().readTablet(extent);
          return tabletMeta.getSelectedFiles() == null;
        }, Wait.MAX_WAIT_MILLIS, 100);

        // Wait for the system compaction to be running
        Wait.waitFor(() -> {
          var tabletMeta = ((ClientContext) client).getAmple().readTablet(extent);
          var externalCompactions = tabletMeta.getExternalCompactions();
          assertTrue(externalCompactions.values().stream()
              .allMatch(ec -> ec.getKind() == CompactionKind.SYSTEM));
          return externalCompactions.size() == 1;
        }, Wait.MAX_WAIT_MILLIS, 10);

        // Wait for the user compaction to now run after the system finishes
        Wait.waitFor(() -> {
          var tabletMeta = ((ClientContext) client).getAmple().readTablet(extent);
          var externalCompactions = tabletMeta.getExternalCompactions();
          var running = externalCompactions.values().stream()
              .filter(ec -> ec.getKind() == CompactionKind.USER).count();
          return running == 1;
        }, Wait.MAX_WAIT_MILLIS, 100);
      } else {
        // Wait for the user compaction to run, there should no system compactions scheduled
        // even though system has the higher priority in the test because the timeout was
        // not exceeded
        Wait.waitFor(() -> {
          var tabletMeta = ((ClientContext) client).getAmple().readTablet(extent);
          var externalCompactions = tabletMeta.getExternalCompactions();
          assertTrue(externalCompactions.values().stream()
              .allMatch(ec -> ec.getKind() == CompactionKind.USER));
          return externalCompactions.size() == 1;
        }, Wait.MAX_WAIT_MILLIS, 10);
      }

      // Wait and verify all compactions finish
      Wait.waitFor(() -> {
        var tabletMeta = ((ClientContext) client).getAmple().readTablet(extent);
        var externalCompactions = tabletMeta.getExternalCompactions().size();
        log.debug("Waiting for compactions to finish, count {}", externalCompactions);
        return externalCompactions == 0 && tabletMeta.getCompacted().isEmpty()
            && tabletMeta.getSelectedFiles() == null;
      }, Wait.MAX_WAIT_MILLIS, 100);
    }

    ExternalCompactionTestUtils.assertNoCompactionMetadata(getServerContext(), table1);
  }

  @Test
  public void testOfflineAndCompactions() throws Exception {
    var uniqueNames = getUniqueNames(1);
    String table = uniqueNames[0];

    // This test exercises concurrent compactions and table offline.

    try (final AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {

      SortedSet<Text> splits = new TreeSet<>();
      for (int i = 1; i < 32; i++) {
        splits.add(new Text(String.format("r:%04d", i)));
      }

      client.tableOperations().create(table, new NewTableConfiguration().withSplits(splits));
      writeRows(client, table, 33, true);
      // create two files per tablet
      writeRows(client, table, 33, true);

      var ctx = getCluster().getServerContext();
      var tableId = ctx.getTableId(table);

      // verify assumptions of test, expect all tablets to have files
      var files0 = getFiles(ctx, tableId);
      assertEquals(32, files0.size());
      assertFalse(files0.values().stream().anyMatch(Set::isEmpty));

      // lower the tables compaction ratio to cause system compactions
      client.tableOperations().setProperty(table, Property.TABLE_MAJC_RATIO.getKey(), "1");

      // start a bunch of compactions in the background
      var executor = Executors.newCachedThreadPool();
      List<Future<?>> futures = new ArrayList<>();
      // start user compactions on a subset of the tables tablets, system compactions should attempt
      // to run on all tablets. With concurrency should get a mix.
      for (int i = 1; i < 20; i++) {
        var startRow = new Text(String.format("r:%04d", i - 1));
        var endRow = new Text(String.format("r:%04d", i));
        futures.add(executor.submit(() -> {
          CompactionConfig config = new CompactionConfig();
          config.setWait(true);
          config.setStartRow(startRow);
          config.setEndRow(endRow);
          client.tableOperations().compact(table, config);
          return null;
        }));
      }

      log.debug("Waiting for offline");
      // take tablet offline while there are concurrent compactions
      client.tableOperations().offline(table, true);

      // grab a snapshot of all the tablets files after waiting for offline, do not expect any
      // tablets files to change at this point
      var files1 = getFiles(ctx, tableId);

      // wait for the background compactions
      log.debug("Waiting for futures");
      for (var future : futures) {
        try {
          future.get();
        } catch (ExecutionException ee) {
          // its ok if some of the compactions fail because the table was concurrently taken offline
          assertTrue(ee.getMessage().contains("is offline"));
        }
      }

      // grab a second snapshot of the tablets files after all the background operations completed
      var files2 = getFiles(ctx, tableId);

      // do not expect the files to have changed after the offline operation returned.
      assertEquals(files1, files2);

      executor.shutdown();
    }
  }

  private Map<KeyExtent,Set<StoredTabletFile>> getFiles(ServerContext ctx, TableId tableId) {
    Map<KeyExtent,Set<StoredTabletFile>> files = new HashMap<>();
    try (var tablets = ctx.getAmple().readTablets().forTable(tableId).build()) {
      for (var tablet : tablets) {
        files.put(tablet.getExtent(), tablet.getFiles());
      }
    }
    return files;
  }

  @Test
  public void testGetActiveCompactions() throws Exception {
    final String table1 = this.getUniqueNames(1)[0];
    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
      client.tableOperations().create(table1);
      try (BatchWriter bw = client.createBatchWriter(table1)) {
        for (int i = 1; i <= MAX_DATA; i++) {
          Mutation m = new Mutation(Integer.toString(i));
          m.put("cf", "cq", new Value());
          bw.addMutation(m);
          bw.flush();
          // flush often to create multiple files to compact
          client.tableOperations().flush(table1, null, null, true);
        }
      }

      final AtomicReference<Exception> error = new AtomicReference<>();
      final CountDownLatch started = new CountDownLatch(1);
      Thread t = new Thread(() -> {
        try {
          IteratorSetting setting = new IteratorSetting(50, "sleepy", SlowIterator.class);
          setting.addOption("sleepTime", "3000");
          setting.addOption("seekSleepTime", "3000");
          client.tableOperations().attachIterator(table1, setting, EnumSet.of(IteratorScope.majc));
          started.countDown();
          client.tableOperations().compact(table1, new CompactionConfig().setWait(true));
        } catch (AccumuloSecurityException | TableNotFoundException | AccumuloException e) {
          error.set(e);
        }
      });
      t.start();

      started.await();

      List<ActiveCompaction> compactions = new ArrayList<>();
      do {
        client.instanceOperations().getActiveCompactions().forEach((ac) -> {
          try {
            if (ac.getTable().equals(table1)) {
              compactions.add(ac);
            }
          } catch (TableNotFoundException e1) {
            fail("Table was deleted during test, should not happen");
          }
        });
        Thread.sleep(1000);
      } while (compactions.isEmpty());

      ActiveCompaction running1 = compactions.get(0);
      CompactionHost host = running1.getHost();
      assertTrue(host.getType() == CompactionHost.Type.COMPACTOR);

      compactions.clear();
      do {
        HostAndPort hp = HostAndPort.fromParts(host.getAddress(), host.getPort());
        client.instanceOperations().getActiveCompactions(hp.toString()).forEach((ac) -> {
          try {
            if (ac.getTable().equals(table1)) {
              compactions.add(ac);
            }
          } catch (TableNotFoundException e1) {
            fail("Table was deleted during test, should not happen");
          }
        });
        Thread.sleep(1000);
      } while (compactions.isEmpty());

      ActiveCompaction running2 = compactions.get(0);
      assertEquals(running1.getInputFiles(), running2.getInputFiles());
      assertEquals(running1.getOutputFile(), running2.getOutputFile());
      assertEquals(running1.getTablet(), running2.getTablet());

      client.tableOperations().cancelCompaction(table1);
      t.join();
    }
  }

  /**
   * Counts the number of tablets and files in a table.
   */
  private int countFiles(AccumuloClient c, String tableName) throws Exception {
    var tableId = getCluster().getServerContext().getTableId(tableName);
    try (Scanner s = c.createScanner(AccumuloTable.METADATA.tableName(), Authorizations.EMPTY)) {
      s.setRange(MetadataSchema.TabletsSection.getRange(tableId));
      TabletColumnFamily.PREV_ROW_COLUMN.fetch(s);
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

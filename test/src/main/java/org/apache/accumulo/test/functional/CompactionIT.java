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
import static org.apache.accumulo.core.util.LazySingletons.RANDOM;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
import java.io.UncheckedIOException;
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
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.CompactionConfig;
import org.apache.accumulo.core.client.admin.NewTableConfiguration;
import org.apache.accumulo.core.client.admin.PluginConfig;
import org.apache.accumulo.core.client.admin.compaction.CompactableFile;
import org.apache.accumulo.core.client.admin.compaction.CompactionConfigurer;
import org.apache.accumulo.core.client.admin.compaction.CompactionSelector;
import org.apache.accumulo.core.client.admin.compaction.CompressionConfigurer;
import org.apache.accumulo.core.clientImpl.ClientContext;
import org.apache.accumulo.core.clientImpl.TableOperationsImpl;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.file.rfile.bcfile.PrintBCInfo;
import org.apache.accumulo.core.iterators.DevNull;
import org.apache.accumulo.core.iterators.Filter;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.IteratorUtil.IteratorScope;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.user.AgeOffFilter;
import org.apache.accumulo.core.iterators.user.GrepIterator;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.metadata.StoredTabletFile;
import org.apache.accumulo.core.metadata.schema.Ample;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.DataFileColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.TabletColumnFamily;
import org.apache.accumulo.core.metadata.schema.TabletMetadata;
import org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType;
import org.apache.accumulo.core.metadata.schema.TabletsMetadata;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.harness.AccumuloClusterHarness;
import org.apache.accumulo.minicluster.ServerType;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloConfigImpl;
import org.apache.accumulo.test.VerifyIngest;
import org.apache.accumulo.test.VerifyIngest.VerifyParams;
import org.apache.accumulo.test.compaction.CompactionExecutorIT;
import org.apache.accumulo.test.compaction.ExternalCompaction_1_IT.FSelector;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;

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

  public static class RandomErrorThrowingSelector implements CompactionSelector {

    public static final String FILE_LIST_PARAM = "filesToCompact";
    private static Boolean ERROR_THROWN = Boolean.FALSE;

    private List<String> filesToCompact;

    @Override
    public void init(InitParameters iparams) {
      String files = iparams.getOptions().get(FILE_LIST_PARAM);
      Objects.requireNonNull(files);
      String[] f = files.split(",");
      filesToCompact = Lists.newArrayList(f);
    }

    @Override
    public Selection select(SelectionParameters sparams) {
      if (!ERROR_THROWN) {
        ERROR_THROWN = Boolean.TRUE;
        throw new RuntimeException("Exception for test");
      }
      List<CompactableFile> matches = new ArrayList<>();
      sparams.getAvailableFiles().forEach(cf -> {
        if (filesToCompact.contains(cf.getFileName())) {
          matches.add(cf);
        }
      });
      return new Selection(matches);
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

  private static final Logger log = LoggerFactory.getLogger(CompactionIT.class);

  private static final int MAX_DATA = 1000;

  @Override
  protected Duration defaultTimeout() {
    return Duration.ofMinutes(4);
  }

  @Override
  public void configureMiniCluster(MiniAccumuloConfigImpl cfg, Configuration hadoopCoreSite) {
    cfg.setProperty(Property.INSTANCE_ZK_TIMEOUT, "15s");
    cfg.setProperty(Property.TSERV_MAJC_DELAY, "1");
    // use raw local file system so walogs sync and flush will work
    hadoopCoreSite.set("fs.file.impl", RawLocalFileSystem.class.getName());
  }

  @Test
  public void testBadSelector() throws Exception {
    try (AccumuloClient c = Accumulo.newClient().from(getClientProps()).build()) {
      final String tableName = getUniqueNames(1)[0];
      NewTableConfiguration tc = new NewTableConfiguration();
      // Ensure compactions don't kick off
      tc.setProperties(Map.of(Property.TABLE_MAJC_RATIO.getKey(), "10.0"));
      c.tableOperations().create(tableName, tc);
      // Create multiple RFiles
      try (BatchWriter bw = c.createBatchWriter(tableName)) {
        for (int i = 1; i <= 4; i++) {
          Mutation m = new Mutation(Integer.toString(i));
          m.put("cf", "cq", new Value());
          bw.addMutation(m);
          bw.flush();
          // flush often to create multiple files to compact
          c.tableOperations().flush(tableName, null, null, true);
        }
      }

      List<String> files = FunctionalTestUtils.getRFilePaths(c, tableName);
      assertEquals(4, files.size());

      String subset = files.get(0).substring(files.get(0).lastIndexOf('/') + 1) + ","
          + files.get(3).substring(files.get(3).lastIndexOf('/') + 1);

      CompactionConfig config = new CompactionConfig()
          .setSelector(new PluginConfig(RandomErrorThrowingSelector.class.getName(),
              Map.of(RandomErrorThrowingSelector.FILE_LIST_PARAM, subset)))
          .setWait(true);
      c.tableOperations().compact(tableName, config);

      // check that the subset of files selected are compacted, but the others remain untouched
      List<String> filesAfterCompact = FunctionalTestUtils.getRFilePaths(c, tableName);
      assertFalse(filesAfterCompact.contains(files.get(0)));
      assertTrue(filesAfterCompact.contains(files.get(1)));
      assertTrue(filesAfterCompact.contains(files.get(2)));
      assertFalse(filesAfterCompact.contains(files.get(3)));

      List<String> rows = new ArrayList<>();
      c.createScanner(tableName).forEach((k, v) -> rows.add(k.getRow().toString()));
      assertEquals(List.of("1", "2", "3", "4"), rows);
    }
  }

  @Test
  public void testCompactionWithTableIterator() throws Exception {
    String table1 = this.getUniqueNames(1)[0];
    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
      client.tableOperations().create(table1);
      try (BatchWriter bw = client.createBatchWriter(table1)) {
        for (int i = 1; i <= 4; i++) {
          Mutation m = new Mutation(Integer.toString(i));
          m.put("cf", "cq", new Value());
          bw.addMutation(m);
          bw.flush();
          // flush often to create multiple files to compact
          client.tableOperations().flush(table1, null, null, true);
        }
      }

      IteratorSetting setting = new IteratorSetting(50, "delete", DevNull.class);
      client.tableOperations().attachIterator(table1, setting, EnumSet.of(IteratorScope.majc));
      client.tableOperations().compact(table1, new CompactionConfig().setWait(true));

      try (Scanner s = client.createScanner(table1)) {
        assertFalse(s.iterator().hasNext());
      }
    }
  }

  @Test
  public void testUserCompactionCancellation() throws Exception {
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
      final AtomicBoolean started = new AtomicBoolean(false);
      Thread t = new Thread(() -> {
        try {
          started.set(true);
          IteratorSetting setting = new IteratorSetting(50, "sleepy", SlowIterator.class);
          setting.addOption("sleepTime", "3000");
          setting.addOption("seekSleepTime", "3000");
          client.tableOperations().attachIterator(table1, setting, EnumSet.of(IteratorScope.majc));
          client.tableOperations().compact(table1, new CompactionConfig().setWait(true));
        } catch (AccumuloSecurityException | TableNotFoundException | AccumuloException e) {
          error.set(e);
        }
      });
      t.start();
      while (!started.get()) {
        Thread.sleep(1000);
      }
      client.tableOperations().cancelCompaction(table1);
      t.join();
      Exception e = error.get();
      assertNotNull(e);
      assertEquals(TableOperationsImpl.COMPACTION_CANCELED_MSG, e.getMessage());
    }
  }

  @Test
  public void testErrorDuringUserCompaction() throws Exception {
    final String table1 = this.getUniqueNames(1)[0];
    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
      client.tableOperations().create(table1);
      client.tableOperations().setProperty(table1, Property.TABLE_FILE_MAX.getKey(), "1001");
      client.tableOperations().setProperty(table1, Property.TABLE_MAJC_RATIO.getKey(), "1001");
      TableId tid = TableId.of(client.tableOperations().tableIdMap().get(table1));

      ReadWriteIT.ingest(client, MAX_DATA, 1, 1, 0, "colf", table1, 1);

      Ample ample = ((ClientContext) client).getAmple();
      try (TabletsMetadata tms =
          ample.readTablets().forTable(tid).fetch(ColumnType.FILES).build();) {
        TabletMetadata tm = tms.iterator().next();
        assertEquals(1000, tm.getFiles().size());
      }

      IteratorSetting setting = new IteratorSetting(50, "error", ErrorThrowingIterator.class);
      setting.addOption(ErrorThrowingIterator.TIMES, "3");
      client.tableOperations().attachIterator(table1, setting, EnumSet.of(IteratorScope.majc));
      client.tableOperations().compact(table1, new CompactionConfig().setWait(true));

      try (
          TabletsMetadata tms = ample.readTablets().forTable(tid).fetch(ColumnType.FILES).build()) {
        TabletMetadata tm = tms.iterator().next();
        assertEquals(1, tm.getFiles().size());
      }

      ReadWriteIT.verify(client, MAX_DATA, 1, 1, 0, table1);

    }
  }

  @Test
  public void testErrorDuringCompactionNoOutput() throws Exception {
    final String table1 = this.getUniqueNames(1)[0];
    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
      client.tableOperations().create(table1);
      client.tableOperations().setProperty(table1, Property.TABLE_MAJC_RATIO.getKey(), "51");
      TableId tid = TableId.of(client.tableOperations().tableIdMap().get(table1));

      ReadWriteIT.ingest(client, 50, 1, 1, 0, "colf", table1, 1);
      ReadWriteIT.verify(client, 50, 1, 1, 0, table1);

      Ample ample = ((ClientContext) client).getAmple();
      try (
          TabletsMetadata tms = ample.readTablets().forTable(tid).fetch(ColumnType.FILES).build()) {
        TabletMetadata tm = tms.iterator().next();
        assertEquals(50, tm.getFiles().size());
      }

      IteratorSetting setting = new IteratorSetting(50, "ageoff", AgeOffFilter.class);
      setting.addOption("ttl", "0");
      setting.addOption("currentTime", Long.toString(System.currentTimeMillis() + 86400));
      client.tableOperations().attachIterator(table1, setting, EnumSet.of(IteratorScope.majc));

      // Since this iterator is on the top, it will throw an error 3 times, then allow the
      // ageoff iterator to do its work.
      IteratorSetting setting2 = new IteratorSetting(51, "error", ErrorThrowingIterator.class);
      setting2.addOption(ErrorThrowingIterator.TIMES, "3");
      client.tableOperations().attachIterator(table1, setting2, EnumSet.of(IteratorScope.majc));
      client.tableOperations().compact(table1, new CompactionConfig().setWait(true));

      try (TabletsMetadata tm = ample.readTablets().forTable(tid).fetch(ColumnType.FILES).build()) {
        assertThrows(NoSuchElementException.class, () -> tm.iterator().next());
      }
      assertEquals(0, client.createScanner(table1).stream().count());
    }
  }

  @Test
  public void testTableDeletedDuringUserCompaction() throws Exception {
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
      final AtomicBoolean started = new AtomicBoolean(false);
      Thread t = new Thread(() -> {
        try {
          started.set(true);
          IteratorSetting setting = new IteratorSetting(50, "sleepy", SlowIterator.class);
          setting.addOption("sleepTime", "3000");
          setting.addOption("seekSleepTime", "3000");
          client.tableOperations().attachIterator(table1, setting, EnumSet.of(IteratorScope.majc));
          client.tableOperations().compact(table1, new CompactionConfig().setWait(true));
        } catch (AccumuloSecurityException | TableNotFoundException | AccumuloException e) {
          error.set(e);
        }
      });
      t.start();
      while (!started.get()) {
        Thread.sleep(1000);
      }
      client.tableOperations().delete(table1);
      t.join();
      Exception e = error.get();
      assertNotNull(e);
      assertEquals(TableOperationsImpl.COMPACTION_CANCELED_MSG, e.getMessage());
    }
  }

  @Test
  public void testPartialCompaction() throws Exception {
    String tableName = getUniqueNames(1)[0];
    try (final AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {

      client.tableOperations().create(tableName);

      // Insert MAX_DATA rows
      try (BatchWriter bw = client.createBatchWriter(tableName)) {
        for (int i = 0; i < MAX_DATA; i++) {
          Mutation m = new Mutation(String.format("r:%04d", i));
          m.put("", "", "" + i);
          bw.addMutation(m);
        }
      }
      client.tableOperations().flush(tableName);
      IteratorSetting iterSetting = new IteratorSetting(100, TestFilter.class);
      // make sure iterator options make it to compactor process
      iterSetting.addOption("modulus", 17 + "");
      CompactionConfig config =
          new CompactionConfig().setIterators(List.of(iterSetting)).setWait(true);
      client.tableOperations().compact(tableName, config);

      // Insert 2 * MAX_DATA rows
      try (BatchWriter bw = client.createBatchWriter(tableName)) {
        for (int i = MAX_DATA; i < MAX_DATA * 2; i++) {
          Mutation m = new Mutation(String.format("r:%04d", i));
          m.put("", "", "" + i);
          bw.addMutation(m);
        }
      }
      // this should create an F file
      client.tableOperations().flush(tableName);

      // run a compaction that only compacts F files
      iterSetting = new IteratorSetting(100, TestFilter.class);
      // compact F file w/ different modulus and user pmodulus option for partial compaction
      iterSetting.addOption("pmodulus", 19 + "");
      config = new CompactionConfig().setIterators(List.of(iterSetting)).setWait(true)
          .setSelector(new PluginConfig(FSelector.class.getName()));
      client.tableOperations().compact(tableName, config);

      try (Scanner scanner = client.createScanner(tableName)) {
        int count = 0;
        for (Entry<Key,Value> entry : scanner) {

          int v = Integer.parseInt(entry.getValue().toString());
          int modulus = v < MAX_DATA ? 17 : 19;

          assertEquals(0, Integer.parseInt(entry.getValue().toString()) % modulus,
              String.format("%s %s %d != 0", entry.getValue(), "%", modulus));
          count++;
        }

        // Verify
        int expectedCount = 0;
        for (int i = 0; i < MAX_DATA * 2; i++) {
          int modulus = i < MAX_DATA ? 17 : 19;
          if (i % modulus == 0) {
            expectedCount++;
          }
        }
        assertEquals(expectedCount, count);
      }

    }
  }

  @Test
  public void testConfigurer() throws Exception {
    String tableName = this.getUniqueNames(1)[0];

    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {

      Map<String,String> props = Map.of(Property.TABLE_FILE_COMPRESSION_TYPE.getKey(), "none");
      NewTableConfiguration ntc = new NewTableConfiguration().setProperties(props);
      client.tableOperations().create(tableName, ntc);

      byte[] data = new byte[100000];
      Arrays.fill(data, (byte) 65);
      try (var writer = client.createBatchWriter(tableName)) {
        for (int row = 0; row < 10; row++) {
          Mutation m = new Mutation(row + "");
          m.at().family("big").qualifier("stuff").put(data);
          writer.addMutation(m);
        }
      }
      client.tableOperations().flush(tableName, null, null, true);

      // without compression, expect file to be large
      long sizes = CompactionExecutorIT.getFileSizes(client, tableName);
      assertTrue(sizes > data.length * 10 && sizes < data.length * 11,
          "Unexpected files sizes : " + sizes);

      client.tableOperations().compact(tableName,
          new CompactionConfig().setWait(true)
              .setConfigurer(new PluginConfig(CompressionConfigurer.class.getName(),
                  Map.of(CompressionConfigurer.LARGE_FILE_COMPRESSION_TYPE, "gz",
                      CompressionConfigurer.LARGE_FILE_COMPRESSION_THRESHOLD, data.length + ""))));

      // after compacting with compression, expect small file
      sizes = CompactionExecutorIT.getFileSizes(client, tableName);
      assertTrue(sizes < data.length,
          "Unexpected files sizes: data: " + data.length + ", file:" + sizes);

      client.tableOperations().compact(tableName, new CompactionConfig().setWait(true));

      // after compacting without compression, expect big files again
      sizes = CompactionExecutorIT.getFileSizes(client, tableName);
      assertTrue(sizes > data.length * 10 && sizes < data.length * 11,
          "Unexpected files sizes : " + sizes);

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
    }

  }

  @Test
  public void testConcurrent() throws Exception {
    // two compactions without iterators or strategy should be able to run concurrently

    try (AccumuloClient c = Accumulo.newClient().from(getClientProps()).build()) {

      String tableName = getUniqueNames(1)[0];
      c.tableOperations().create(tableName);

      // write random data because its very unlikely it will compress
      writeRandomValue(c, tableName, 1 << 16);
      writeRandomValue(c, tableName, 1 << 16);

      c.tableOperations().compact(tableName, new CompactionConfig().setWait(false));
      c.tableOperations().compact(tableName, new CompactionConfig().setWait(true));

      assertEquals(1, FunctionalTestUtils.countRFiles(c, tableName));

      writeRandomValue(c, tableName, 1 << 16);

      IteratorSetting iterConfig = new IteratorSetting(30, SlowIterator.class);
      SlowIterator.setSleepTime(iterConfig, 1000);

      long t1 = System.currentTimeMillis();
      c.tableOperations().compact(tableName,
          new CompactionConfig().setWait(false).setIterators(java.util.Arrays.asList(iterConfig)));
      try {
        // this compaction should fail because previous one set iterators
        c.tableOperations().compact(tableName, new CompactionConfig().setWait(true));
        if (System.currentTimeMillis() - t1 < 2000) {
          fail("Expected compaction to fail because another concurrent compaction set iterators");
        }
      } catch (AccumuloException e) {}
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

  /**
   * Was used in debugging {@link #testGetSelectedFilesForCompaction}. May be useful later.
   *
   * @param client An accumulo client
   * @param tableName The name of the table
   * @return a map of the RFiles to their size in bytes
   */
  private Map<String,Long> getFileSizeMap(AccumuloClient client, String tableName) {
    var tableId = TableId.of(client.tableOperations().tableIdMap().get(tableName));
    Map<String,Long> map = new HashMap<>();

    try (var tabletsMeta =
        TabletsMetadata.builder(client).forTable(tableId).fetch(ColumnType.FILES).build()) {
      for (TabletMetadata tm : tabletsMeta) {
        for (StoredTabletFile stf : tm.getFiles()) {
          try {
            String filePath = stf.getPath().toString();
            Long fileSize =
                FileSystem.getLocal(new Configuration()).getFileStatus(stf.getPath()).getLen();
            map.put(filePath, fileSize);
          } catch (IOException e) {
            throw new UncheckedIOException(e);
          }
        }
      }

      return map;
    }
  }

  private int countFiles(AccumuloClient c) throws Exception {
    try (Scanner s = c.createScanner(MetadataTable.NAME, Authorizations.EMPTY)) {
      s.fetchColumnFamily(new Text(TabletColumnFamily.NAME));
      s.fetchColumnFamily(new Text(DataFileColumnFamily.NAME));
      return Iterators.size(s.iterator());
    }
  }

  private void writeRandomValue(AccumuloClient c, String tableName, int size) throws Exception {
    byte[] data1 = new byte[size];
    RANDOM.get().nextBytes(data1);

    try (BatchWriter bw = c.createBatchWriter(tableName)) {
      Mutation m1 = new Mutation("r" + RANDOM.get().nextInt(909090));
      m1.put("data", "bl0b", new Value(data1));
      bw.addMutation(m1);
    }
    c.tableOperations().flush(tableName, null, null, true);
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

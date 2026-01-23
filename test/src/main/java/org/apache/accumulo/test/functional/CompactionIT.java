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
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
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
import org.apache.accumulo.core.file.FilePrefix;
import org.apache.accumulo.core.file.rfile.RFile;
import org.apache.accumulo.core.iterators.DevNull;
import org.apache.accumulo.core.iterators.Filter;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.IteratorUtil.IteratorScope;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.user.AgeOffFilter;
import org.apache.accumulo.core.iterators.user.GrepIterator;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.metadata.TabletFile;
import org.apache.accumulo.core.metadata.schema.Ample;
import org.apache.accumulo.core.metadata.schema.DataFileValue;
import org.apache.accumulo.core.metadata.schema.MetadataSchema;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.DataFileColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.TabletColumnFamily;
import org.apache.accumulo.core.metadata.schema.TabletMetadata;
import org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType;
import org.apache.accumulo.core.metadata.schema.TabletsMetadata;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.spi.compaction.DefaultCompactionPlanner;
import org.apache.accumulo.core.spi.compaction.SimpleCompactionDispatcher;
import org.apache.accumulo.core.util.HostAndPort;
import org.apache.accumulo.harness.AccumuloClusterHarness;
import org.apache.accumulo.minicluster.ServerType;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloClusterImpl;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloConfigImpl;
import org.apache.accumulo.test.VerifyIngest;
import org.apache.accumulo.test.VerifyIngest.VerifyParams;
import org.apache.accumulo.test.compaction.CompactionExecutorIT;
import org.apache.accumulo.test.compaction.ExternalCompaction_1_IT.FSelector;
import org.apache.accumulo.test.functional.ScanIteratorIT.AppendingIterator;
import org.apache.accumulo.test.util.Wait;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;

@SuppressWarnings("removal")
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

  private static final Logger log = LoggerFactory.getLogger(CompactionIT.class);

  private static final int MAX_DATA = 1000;

  @Override
  protected Duration defaultTimeout() {
    return Duration.ofMinutes(4);
  }

  @Override
  public void configureMiniCluster(MiniAccumuloConfigImpl cfg, Configuration hadoopCoreSite) {
    cfg.setProperty(Property.INSTANCE_ZK_TIMEOUT, "15s");
    cfg.setProperty(Property.TSERV_MAJC_THREAD_MAXOPEN, "4");
    cfg.setProperty(Property.TSERV_MAJC_DELAY, "1");
    cfg.setProperty(Property.TSERV_MAJC_MAXCONCURRENT, "1");
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
      TabletsMetadata tms = ample.readTablets().forTable(tid).fetch(ColumnType.FILES).build();
      TabletMetadata tm = tms.iterator().next();
      assertEquals(1000, tm.getFiles().size());

      IteratorSetting setting = new IteratorSetting(50, "error", ErrorThrowingIterator.class);
      setting.addOption(ErrorThrowingIterator.TIMES, "3");
      client.tableOperations().attachIterator(table1, setting, EnumSet.of(IteratorScope.majc));
      client.tableOperations().compact(table1, new CompactionConfig().setWait(true));

      tms = ample.readTablets().forTable(tid).fetch(ColumnType.FILES).build();
      tm = tms.iterator().next();
      assertEquals(1, tm.getFiles().size());

      ReadWriteIT.verify(client, MAX_DATA, 1, 1, 0, table1);

    }
  }

  @Test
  public void testErrorDuringCompactionNoOutput() throws Exception {
    final String table1 = this.getUniqueNames(1)[0];
    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
      client.tableOperations().create(table1);
      client.tableOperations().setProperty(table1, Property.TABLE_FILE_MAX.getKey(), "1001");
      client.tableOperations().setProperty(table1, Property.TABLE_MAJC_RATIO.getKey(), "51");
      TableId tid = TableId.of(client.tableOperations().tableIdMap().get(table1));

      ReadWriteIT.ingest(client, 50, 1, 1, 0, "colf", table1, 1);
      ReadWriteIT.verify(client, 50, 1, 1, 0, table1);

      Ample ample = ((ClientContext) client).getAmple();
      TabletsMetadata tms = ample.readTablets().forTable(tid).fetch(ColumnType.FILES).build();
      TabletMetadata tm = tms.iterator().next();
      assertEquals(50, tm.getFiles().size());

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

      assertThrows(NoSuchElementException.class, () -> ample.readTablets().forTable(tid)
          .fetch(ColumnType.FILES).build().iterator().next());
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

  public static class FourConfigurer implements CompactionConfigurer {

    @Override
    public void init(InitParameters iparams) {}

    @Override
    public Overrides override(InputParameters params) {
      if (new Text("4").equals(params.getTabletId().getEndRow())) {
        return new Overrides(Map.of(Property.TABLE_FILE_COMPRESSION_TYPE.getKey(), "gz"));
      } else {
        return new Overrides(Map.of(Property.TABLE_FILE_COMPRESSION_TYPE.getKey(), "none"));
      }
    }
  }

  public static class UriConfigurer implements CompactionConfigurer {

    @Override
    public void init(InitParameters iparams) {}

    @Override
    public Overrides override(InputParameters params) {
      // This will validate the paths looks like a tablet file path and throw an exception if it
      // does not
      var parts = TabletFile.parsePath(new Path(params.getOutputFile()));
      // For this test should be producing A files
      Preconditions.checkArgument(
          parts.getFileName().startsWith(FilePrefix.FULL_COMPACTION.getPrefix() + ""));
      Preconditions.checkArgument(parts.getFileName().endsWith(RFile.EXTENSION));

      if (parts.getTabletDir()
          .equals(MetadataSchema.TabletsSection.ServerColumnFamily.DEFAULT_TABLET_DIR_NAME)) {
        return new Overrides(Map.of(Property.TABLE_FILE_COMPRESSION_TYPE.getKey(), "gz"));
      } else {
        return new Overrides(Map.of(Property.TABLE_FILE_COMPRESSION_TYPE.getKey(), "none"));
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

      // compact using a custom configurer that considers tablet end row
      client.tableOperations().addSplits(tableName, new TreeSet<>(List.of(new Text("4"))));
      client.tableOperations().compact(tableName, new CompactionConfig().setWait(true)
          .setConfigurer(new PluginConfig(FourConfigurer.class.getName())));
      var tabletSizes = getFilesSizesPerTablet(client, tableName);
      assertTrue(tabletSizes.get("4") > 0 && tabletSizes.get("4") < 1000, tabletSizes.toString());
      assertTrue(tabletSizes.get("null") > 500_000 && tabletSizes.get("4") < 510_000,
          tabletSizes.toString());

      // compact using a custom configurer that considers the output path, should invert which file
      // is compressed
      client.tableOperations().addSplits(tableName, new TreeSet<>(List.of(new Text("4"))));
      client.tableOperations().compact(tableName, new CompactionConfig().setWait(true)
          .setConfigurer(new PluginConfig(UriConfigurer.class.getName())));
      tabletSizes = getFilesSizesPerTablet(client, tableName);
      assertTrue(tabletSizes.get("4") > 500_000 && tabletSizes.get("4") < 510_000,
          tabletSizes.toString());
      assertTrue(tabletSizes.get("null") > 0 && tabletSizes.get("null") < 1000,
          tabletSizes.toString());

    }
  }

  private Map<String,Long> getFilesSizesPerTablet(AccumuloClient client, String table)
      throws Exception {
    var ctx = (ClientContext) client;
    var ample = ctx.getAmple();
    var id = ctx.getTableId(table);

    try (var tablets = ample.readTablets().forTable(id).build()) {
      Map<String,Long> sizes = new TreeMap<>();
      for (var tablet : tablets) {
        var tsize = tablet.getFilesMap().values().stream().mapToLong(DataFileValue::getSize).sum();
        var endRow = tablet.getEndRow();
        sizes.put(endRow == null ? "null" : endRow.toString(), tsize);
      }
      return sizes;
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
          Property.TSERV_COMPACTION_SERVICE_PREFIX.getKey() + "deleteme.planner",
          DefaultCompactionPlanner.class.getName());
      c.instanceOperations().setProperty(
          Property.TSERV_COMPACTION_SERVICE_PREFIX.getKey() + "deleteme.planner.opts.executors",
          "[{'name':'all','type':'internal','numThreads':1}]".replaceAll("'", "\""));

      // create a compaction service named keepme
      c.instanceOperations().setProperty(
          Property.TSERV_COMPACTION_SERVICE_PREFIX.getKey() + "keepme.planner",
          DefaultCompactionPlanner.class.getName());
      c.instanceOperations().setProperty(
          Property.TSERV_COMPACTION_SERVICE_PREFIX.getKey() + "keepme.planner.opts.executors",
          "[{'name':'all','type':'internal','numThreads':1}]".replaceAll("'", "\""));

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
          .removeProperty(Property.TSERV_COMPACTION_SERVICE_PREFIX.getKey() + "deleteme.planner");
      c.instanceOperations().removeProperty(
          Property.TSERV_COMPACTION_SERVICE_PREFIX.getKey() + "deleteme.planner.opts.executors");

      // add a new compaction service named newcs
      c.instanceOperations().setProperty(
          Property.TSERV_COMPACTION_SERVICE_PREFIX.getKey() + "newcs.planner",
          DefaultCompactionPlanner.class.getName());
      c.instanceOperations().setProperty(
          Property.TSERV_COMPACTION_SERVICE_PREFIX.getKey() + "newcs.planner.opts.executors",
          "[{'name':'all','type':'internal','numThreads':1}]".replaceAll("'", "\""));

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
      assertTrue(host.getType() == CompactionHost.Type.TSERVER);

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

  @Test
  public void testMigrationCancelCompaction() throws Exception {

    // This test creates 40 tablets w/ slow iterator, causes 40 compactions to start, and then
    // starts a new tablet server. Some of the tablets should migrate to the new tserver and cancel
    // their compaction. Because the test uses a slow iterator, if close blocks on compaction then
    // the test should timeout. Two tables are used to have different iterator settings inorder to
    // test the two different way compactions can be canceled. Compactions can be canceled by thread
    // interrupt or by a check that is done after a compaction iterator returns a key value.

    final String[] tables = this.getUniqueNames(2);
    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
      client.instanceOperations().setProperty(
          Property.TSERV_COMPACTION_SERVICE_DEFAULT_EXECUTORS.getKey(),
          "[{'name':'any','numThreads':20}]".replaceAll("'", "\""));

      SortedSet<Text> splits = IntStream.range(1, 20).mapToObj(i -> String.format("%06d", i * 1000))
          .map(Text::new).collect(Collectors.toCollection(TreeSet::new));

      // This iterator is intended to cover the case of a compaction being canceled by thread
      // interrupt.
      IteratorSetting setting1 = new IteratorSetting(50, "sleepy", SlowIterator.class);
      setting1.addOption("sleepTime", "300000");
      setting1.addOption("seekSleepTime", "3000");
      SlowIterator.sleepUninterruptibly(setting1, false);

      client.tableOperations().create(tables[0], new NewTableConfiguration().withSplits(splits)
          .attachIterator(setting1, EnumSet.of(IteratorScope.majc)));

      // This iterator is intended to cover the case of compaction being canceled by the check after
      // a key value is returned. The iterator is configured to ignore interrupts.
      IteratorSetting setting2 = new IteratorSetting(50, "sleepy", SlowIterator.class);
      setting2.addOption("sleepTime", "2000");
      setting2.addOption("seekSleepTime", "2000");
      SlowIterator.sleepUninterruptibly(setting2, true);

      client.tableOperations().create(tables[1], new NewTableConfiguration().withSplits(splits)
          .attachIterator(setting2, EnumSet.of(IteratorScope.majc)));

      // write files to each tablet, should cause compactions to start
      for (var table : tables) {
        for (int round = 0; round < 5; round++) {
          try (var writer = client.createBatchWriter(table)) {
            for (int i = 0; i < 20_000; i++) {
              Mutation m = new Mutation(String.format("%06d", i));
              m.put("f", "q", "v");
              writer.addMutation(m);
            }
          }
          client.tableOperations().flush(table, null, null, true);
        }
      }

      assertEquals(2, client.instanceOperations().getTabletServers().size());

      var tableId1 = ((ClientContext) client).getTableId(tables[0]);
      var tableId2 = ((ClientContext) client).getTableId(tables[1]);

      Wait.waitFor(() -> {
        var runningCompactions = client.instanceOperations().getActiveCompactions().stream()
            .map(ac -> ac.getTablet().getTable())
            .filter(tid -> tid.equals(tableId1) || tid.equals(tableId2)).count();
        log.debug("Running compactions {}", runningCompactions);
        return runningCompactions == 40;
      });

      ((MiniAccumuloClusterImpl) getCluster()).getConfig().setNumTservers(3);
      getCluster().getClusterControl().start(ServerType.TABLET_SERVER, "localhost");

      Wait.waitFor(() -> {
        var servers = client.instanceOperations().getTabletServers().size();
        log.debug("Server count {}", servers);
        return 3 == servers;
      });

      Wait.waitFor(() -> {
        try (var tablets =
            ((ClientContext) client).getAmple().readTablets().forLevel(Ample.DataLevel.USER)
                .fetch(ColumnType.LOCATION, ColumnType.PREV_ROW).build()) {
          Map<String,Long> counts = new HashMap<>();
          for (var tablet : tablets) {
            if (!tablet.getTableId().equals(tableId1) && !tablet.getTableId().equals(tableId2)) {
              continue;
            }

            if (tablet.getLocation() != null
                && tablet.getLocation().getType() == TabletMetadata.LocationType.CURRENT) {
              counts.merge(tablet.getLocation().getHostPort(), 1L, Long::sum);
            }
          }

          var total = counts.values().stream().mapToLong(l -> l).sum();
          var min = counts.values().stream().mapToLong(l -> l).min().orElse(0);
          var max = counts.values().stream().mapToLong(l -> l).max().orElse(100);
          var serversSeen = counts.keySet();
          log.debug("total:{} min:{} max:{} serversSeen:{}", total, min, max, serversSeen);
          return total == 40 && min == 12 && max == 14 && serversSeen.size() == 3;
        }
      });
    }
  }

  @Test
  public void testIteratorOrder() throws Exception {
    String[] names = getUniqueNames(2);
    try (AccumuloClient c = Accumulo.newClient().from(getClientProps()).build()) {

      // create a table with minor compaction iterators configured to ensure those iterators are
      // applied in the correct order
      NewTableConfiguration ntc = new NewTableConfiguration()
          .attachIterator(AppendingIterator.configure(50, "x"), EnumSet.of(IteratorScope.minc))
          .attachIterator(AppendingIterator.configure(100, "a"), EnumSet.of(IteratorScope.minc));
      c.tableOperations().create(names[0], ntc);

      // create a table with major compaction iterators configured to ensure those iterators are
      // applied in the correct order
      NewTableConfiguration ntc2 = new NewTableConfiguration()
          .attachIterator(AppendingIterator.configure(50, "x"), EnumSet.of(IteratorScope.majc))
          .attachIterator(AppendingIterator.configure(100, "a"), EnumSet.of(IteratorScope.majc));
      c.tableOperations().create(names[1], ntc2);

      try (var writer = c.createBatchWriter(names[0]);
          var writer2 = c.createBatchWriter(names[1])) {
        Mutation m = new Mutation("r1");
        m.put("", "", "base:");
        writer.addMutation(m);
        writer2.addMutation(m);
      }

      try (var mincScanner = c.createScanner(names[0]);
          var majcScanner = c.createScanner(names[1])) {
        // iterators should not be applied yet
        Assertions.assertEquals("base:", mincScanner.iterator().next().getValue().toString());
        Assertions.assertEquals("base:", majcScanner.iterator().next().getValue().toString());

        c.tableOperations().flush(names[0], null, null, true);
        Assertions.assertEquals("base:xa", mincScanner.iterator().next().getValue().toString());
        Assertions.assertEquals("base:", majcScanner.iterator().next().getValue().toString());

        List<IteratorSetting> iters = List.of(AppendingIterator.configure(70, "m"),
            AppendingIterator.configure(50, "b"), AppendingIterator.configure(100, "c"));
        c.tableOperations().compact(names[1],
            new CompactionConfig().setWait(true).setFlush(true).setIterators(iters));
        Assertions.assertEquals("base:xa", mincScanner.iterator().next().getValue().toString());
        Assertions.assertEquals("base:bxmac", majcScanner.iterator().next().getValue().toString());

      }
    }
  }

  /**
   * Counts the number of tablets and files in a table.
   */
  private int countFiles(AccumuloClient c, String tableName) throws Exception {
    var tableId = getCluster().getServerContext().getTableId(tableName);
    try (Scanner s = c.createScanner(MetadataTable.NAME, Authorizations.EMPTY)) {
      s.setRange(MetadataSchema.TabletsSection.getRange(tableId));
      TabletColumnFamily.PREV_ROW_COLUMN.fetch(s);
      s.fetchColumnFamily(new Text(DataFileColumnFamily.NAME));
      return Iterators.size(s.iterator());
    }
  }

}

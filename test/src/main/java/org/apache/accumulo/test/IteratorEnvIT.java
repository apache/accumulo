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
package org.apache.accumulo.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.EnumSet;
import java.util.Map;
import java.util.TreeMap;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.ClientSideIteratorScanner;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.PluginEnvironment;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.admin.CompactionConfig;
import org.apache.accumulo.core.client.admin.NewTableConfiguration;
import org.apache.accumulo.core.client.rfile.RFile;
import org.apache.accumulo.core.client.rfile.RFileWriter;
import org.apache.accumulo.core.clientImpl.OfflineScanner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.Filter;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.IteratorUtil.IteratorScope;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.spi.common.ServiceEnvironment;
import org.apache.accumulo.harness.AccumuloClusterHarness;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloConfigImpl;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Test that objects in IteratorEnvironment returned from the server are as expected.
 */
public class IteratorEnvIT extends AccumuloClusterHarness {

  @Override
  protected Duration defaultTimeout() {
    return Duration.ofMinutes(1);
  }

  @Override
  public void configureMiniCluster(MiniAccumuloConfigImpl cfg, Configuration hadoopCoreSite) {
    cfg.setNumTservers(1);
  }

  private AccumuloClient client;

  /**
   * Basic scan iterator to test IteratorEnvironment returns what is expected.
   */
  public static class ScanIter extends Filter {
    IteratorScope scope = IteratorScope.scan;
    String badColFam;

    @Override
    public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options,
        IteratorEnvironment env) throws IOException {
      super.init(source, options, env);
      this.badColFam = options.get("bad.col.fam");
      testEnv(scope, options, env);

      // Checking for compaction on a scan should throw an error.
      try {
        env.isUserCompaction();
        throw new RuntimeException(
            "Test failed - Expected to throw IllegalStateException when checking compaction on a scan.");
      } catch (IllegalStateException e) {}
      try {
        env.isFullMajorCompaction();
        throw new RuntimeException(
            "Test failed - Expected to throw IllegalStateException when checking compaction on a scan.");
      } catch (IllegalStateException e) {}
    }

    @Override
    public boolean accept(Key k, Value v) {
      // The only reason for filtering out some data is as a way to verify init() and testEnv()
      // have been called
      return !k.getColumnFamily().toString().equals(badColFam);
    }
  }

  /**
   * Basic compaction iterator to test IteratorEnvironment returns what is expected.
   */
  public static class MajcIter extends Filter {
    IteratorScope scope = IteratorScope.majc;
    String badColFam;

    @Override
    public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options,
        IteratorEnvironment env) throws IOException {
      super.init(source, options, env);
      this.badColFam = options.get("bad.col.fam");
      testEnv(scope, options, env);
      try {
        env.isUserCompaction();
      } catch (IllegalStateException e) {
        throw new RuntimeException("Test failed");
      }
      try {
        env.isFullMajorCompaction();
      } catch (IllegalStateException e) {
        throw new RuntimeException("Test failed");
      }
    }

    @Override
    public boolean accept(Key k, Value v) {
      // The only reason for filtering out some data is as a way to verify init() and testEnv()
      // have been called
      return !k.getColumnFamily().toString().equals(badColFam);
    }
  }

  /**
   *
   */
  public static class MincIter extends Filter {
    IteratorScope scope = IteratorScope.minc;
    String badColFam;

    @Override
    public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options,
        IteratorEnvironment env) throws IOException {
      super.init(source, options, env);
      this.badColFam = options.get("bad.col.fam");
      testEnv(scope, options, env);
      try {
        env.isUserCompaction();
        throw new RuntimeException(
            "Test failed - Expected to throw IllegalStateException when checking compaction on a scan.");
      } catch (IllegalStateException e) {}
      try {
        env.isFullMajorCompaction();
        throw new RuntimeException(
            "Test failed - Expected to throw IllegalStateException when checking compaction on a scan.");
      } catch (IllegalStateException e) {}
    }

    @Override
    public boolean accept(Key k, Value v) {
      // The only reason for filtering out some data is as a way to verify init() and testEnv()
      // have been called
      return !k.getColumnFamily().toString().equals(badColFam);
    }
  }

  /**
   * Test the environment methods return what is expected.
   */
  private static void testEnv(IteratorScope scope, Map<String,String> opts,
      IteratorEnvironment env) {
    String expTableIdStr = opts.get("expected.table.id");
    TableId expTableId = expTableIdStr == null ? null : TableId.of(expTableIdStr);

    // verify getServiceEnv() and getPluginEnv() are the same objects,
    // so further checks only need to use getPluginEnv()
    @SuppressWarnings("deprecation")
    ServiceEnvironment serviceEnv = env.getServiceEnv();
    PluginEnvironment pluginEnv = env.getPluginEnv();
    if (serviceEnv != pluginEnv) {
      throw new RuntimeException("Test failed - assertSame(getServiceEnv(),getPluginEnv())");
    }

    // verify property exists on the table config (deprecated and new),
    // with and without custom prefix, but not in the system config
    @SuppressWarnings("deprecation")
    String accTableConf = env.getConfig().get("table.custom.iterator.env.test");
    if (!"value1".equals(accTableConf)) {
      throw new RuntimeException("Test failed - Expected table property not found in getConfig().");
    }
    var tableConf = pluginEnv.getConfiguration(env.getTableId());
    if (!"value1".equals(tableConf.get("table.custom.iterator.env.test"))) {
      throw new RuntimeException("Test failed - Expected table property not found in table conf.");
    }
    if (!"value1".equals(tableConf.getTableCustom("iterator.env.test"))) {
      throw new RuntimeException("Test failed - Expected table property not found in table conf.");
    }
    if (!env.getClass().getName().contains("RFileScanner$IterEnv")) {
      var systemConf = pluginEnv.getConfiguration();
      if (systemConf.get("table.custom.iterator.env.test") != null) {
        throw new RuntimeException("Test failed - Unexpected table property found in system conf.");
      }
    } else {
      // We expect RFileScanner's IterEnv to throw an UOE
      assertThrows(UnsupportedOperationException.class, pluginEnv::getConfiguration);
    }

    // check other environment settings
    if (!scope.equals(env.getIteratorScope())) {
      throw new RuntimeException("Test failed - Error getting iterator scope");
    }
    if (env.isSamplingEnabled()) {
      throw new RuntimeException("Test failed - isSamplingEnabled returned true, expected false");
    }
    if (expTableId != null && !expTableId.equals(env.getTableId())) {
      throw new RuntimeException("Test failed - Error getting Table ID");
    }
  }

  @BeforeEach
  public void setup() {
    client = Accumulo.newClient().from(getClientProps()).build();
  }

  @AfterEach
  public void finish() {
    if (client != null) {
      client.close();
    }
  }

  @Test
  public void test() throws Exception {
    String[] tables = getUniqueNames(5);
    testScan(tables[0], ScanIter.class);
    // No table id when scanning at file level
    testRFileScan(ScanIter.class);
    testOfflineScan(tables[1], ScanIter.class);
    testClientSideScan(tables[2], ScanIter.class);
    testCompact(tables[3], MajcIter.class);
    testMinCompact(tables[4], MincIter.class);
  }

  private void testScan(String tableName,
      Class<? extends SortedKeyValueIterator<Key,Value>> iteratorClass) throws Exception {
    writeData(tableName);

    IteratorSetting cfg = new IteratorSetting(1, iteratorClass);
    cfg.addOption("expected.table.id", client.tableOperations().tableIdMap().get(tableName));
    cfg.addOption("bad.col.fam", "badcf");

    try (Scanner scan = client.createScanner(tableName)) {
      scan.addScanIterator(cfg);
      validateScanner(scan);
    }
  }

  private void testRFileScan(Class<? extends SortedKeyValueIterator<Key,Value>> iteratorClass)
      throws Exception {
    TreeMap<Key,Value> data = createTestData();
    LocalFileSystem fs = FileSystem.getLocal(new Configuration());
    String rFilePath = createRFile(fs, data);

    IteratorSetting cfg = new IteratorSetting(1, iteratorClass);
    cfg.addOption("bad.col.fam", "badcf");

    try (Scanner scan = RFile.newScanner().from(rFilePath).withFileSystem(fs)
        .withTableProperties(getTableConfig().getProperties()).build()) {
      scan.addScanIterator(cfg);
      validateScanner(scan);
    }
  }

  public void testOfflineScan(String tableName,
      Class<? extends SortedKeyValueIterator<Key,Value>> iteratorClass) throws Exception {
    writeData(tableName);

    TableId tableId = getServerContext().getTableId(tableName);
    getServerContext().tableOperations().offline(tableName, true);

    IteratorSetting cfg = new IteratorSetting(1, iteratorClass);
    cfg.addOption("expected.table.id",
        getServerContext().tableOperations().tableIdMap().get(tableName));
    cfg.addOption("bad.col.fam", "badcf");

    try (OfflineScanner scan =
        new OfflineScanner(getServerContext(), tableId, new Authorizations())) {
      scan.addScanIterator(cfg);
      validateScanner(scan);
    }
  }

  public void testClientSideScan(String tableName,
      Class<? extends SortedKeyValueIterator<Key,Value>> iteratorClass) throws Exception {
    writeData(tableName);

    IteratorSetting cfg = new IteratorSetting(1, iteratorClass);
    cfg.addOption("expected.table.id",
        getServerContext().tableOperations().tableIdMap().get(tableName));
    cfg.addOption("bad.col.fam", "badcf");

    try (Scanner scan = client.createScanner(tableName);
        var clientIterScan = new ClientSideIteratorScanner(scan)) {
      clientIterScan.addScanIterator(cfg);
      validateScanner(clientIterScan);
    }
  }

  public void testCompact(String tableName,
      Class<? extends SortedKeyValueIterator<Key,Value>> iteratorClass) throws Exception {
    writeData(tableName);

    IteratorSetting cfg = new IteratorSetting(1, iteratorClass);
    cfg.addOption("expected.table.id", client.tableOperations().tableIdMap().get(tableName));
    cfg.addOption("bad.col.fam", "badcf");
    CompactionConfig config = new CompactionConfig();
    config.setIterators(Collections.singletonList(cfg));
    client.tableOperations().compact(tableName, config);

    try (Scanner scan = client.createScanner(tableName)) {
      validateScanner(scan);
    }
  }

  public void testMinCompact(String tableName,
      Class<? extends SortedKeyValueIterator<Key,Value>> iteratorClass) throws Exception {
    writeData(tableName);

    IteratorSetting cfg = new IteratorSetting(1, iteratorClass);
    cfg.addOption("expected.table.id", client.tableOperations().tableIdMap().get(tableName));
    cfg.addOption("bad.col.fam", "badcf");

    client.tableOperations().attachIterator(tableName, cfg, EnumSet.of(IteratorScope.minc));

    client.tableOperations().flush(tableName, null, null, true);

    try (Scanner scan = client.createScanner(tableName)) {
      validateScanner(scan);
    }
  }

  private NewTableConfiguration getTableConfig() {
    NewTableConfiguration ntc = new NewTableConfiguration();
    ntc.setProperties(Collections.singletonMap("table.custom.iterator.env.test", "value1"));
    return ntc;
  }

  private void writeData(String tableName) throws Exception {
    client.tableOperations().create(tableName, getTableConfig());

    try (BatchWriter bw = client.createBatchWriter(tableName)) {
      for (Map.Entry<Key,Value> data : createTestData().entrySet()) {
        Mutation m = new Mutation(data.getKey().getRow());
        m.at().family(data.getKey().getColumnFamily()).qualifier(data.getKey().getColumnQualifier())
            .put(data.getValue());
        bw.addMutation(m);
      }
    }
  }

  private TreeMap<Key,Value> createTestData() {
    TreeMap<Key,Value> testData = new TreeMap<>();

    // Write data that we do not expect to be filtered out
    testData.put(new Key("row1", "cf1", "cq1"), new Value("val1"));
    testData.put(new Key("row2", "cf1", "cq1"), new Value("val2"));
    testData.put(new Key("row3", "cf1", "cq1"), new Value("val3"));
    // Write data that we expect to be filtered out
    testData.put(new Key("row4", "badcf", "badcq"), new Value("val1"));
    testData.put(new Key("row5", "badcf", "badcq"), new Value("val2"));
    testData.put(new Key("row6", "badcf", "badcq"), new Value("val3"));

    return testData;
  }

  private String createRFile(FileSystem fs, TreeMap<Key,Value> data) throws Exception {
    File dir = new File(System.getProperty("user.dir") + "/target/rfilescan-iterenv-test");
    assertTrue(dir.mkdirs());
    String filePath = dir.getAbsolutePath() + "/test.rf";

    try (RFileWriter writer = RFile.newWriter().to(filePath).withFileSystem(fs).build()) {
      writer.append(data.entrySet());
    }

    fs.deleteOnExit(new Path(dir.getAbsolutePath()));

    return filePath;
  }

  private void validateScanner(Scanner scan) {
    // Ensure the badcf was filtered out to ensure init() and testEnv() were called
    int numElts = 0;
    for (var e : scan) {
      numElts++;
      assertEquals("cf1", e.getKey().getColumnFamily().toString());
    }
    assertEquals(3, numElts);
  }
}

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

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;
import java.nio.file.Path;
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
import org.apache.accumulo.core.clientImpl.ClientContext;
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
import org.apache.accumulo.harness.AccumuloClusterHarness;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloConfigImpl;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Test that objects in IteratorEnvironment returned from the server are as expected.
 */
public class IteratorEnvIT extends AccumuloClusterHarness {
  private static final String BAD_COL_FAM = "badcf";
  private static final String GOOD_COL_FAM = "cf1";
  private static final String CUSTOM_PROP_KEY_SUFFIX = "iterator.env.test";
  private static final String CUSTOM_PROP_KEY = "table.custom." + CUSTOM_PROP_KEY_SUFFIX;
  private static final String CUSTOM_PROP_VAL = "value1";
  private static final String EXPECTED_TABLE_ID_OPT = "expected.table.id";
  @TempDir
  private static Path tempDir;

  @Override
  protected Duration defaultTimeout() {
    return Duration.ofMinutes(1);
  }

  @Override
  public void configureMiniCluster(MiniAccumuloConfigImpl cfg, Configuration hadoopCoreSite) {
    cfg.getClusterServerConfiguration().setNumDefaultTabletServers(1);
  }

  private AccumuloClient client;

  /**
   * Basic scan iterator to test IteratorEnvironment returns what is expected.
   */
  public static class ScanIter extends Filter {
    IteratorScope scope = IteratorScope.scan;

    @Override
    public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options,
        IteratorEnvironment env) throws IOException {
      super.init(source, options, env);
      testEnv(scope, options, env);
    }

    @Override
    public boolean accept(Key k, Value v) {
      // The only reason for filtering out some data is as a way to verify init() and testEnv()
      // have been called
      return !k.getColumnFamily().toString().equals(BAD_COL_FAM);
    }
  }

  /**
   * Basic compaction iterator to test IteratorEnvironment returns what is expected.
   */
  public static class MajcIter extends Filter {
    IteratorScope scope = IteratorScope.majc;

    @Override
    public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options,
        IteratorEnvironment env) throws IOException {
      super.init(source, options, env);
      testEnv(scope, options, env);
    }

    @Override
    public boolean accept(Key k, Value v) {
      // The only reason for filtering out some data is as a way to verify init() and testEnv()
      // have been called
      return !k.getColumnFamily().toString().equals(BAD_COL_FAM);
    }
  }

  /**
   *
   */
  public static class MincIter extends Filter {
    IteratorScope scope = IteratorScope.minc;

    @Override
    public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options,
        IteratorEnvironment env) throws IOException {
      super.init(source, options, env);
      testEnv(scope, options, env);
    }

    @Override
    public boolean accept(Key k, Value v) {
      // The only reason for filtering out some data is as a way to verify init() and testEnv()
      // have been called
      return !k.getColumnFamily().toString().equals(BAD_COL_FAM);
    }
  }

  /**
   * Test the environment methods return what is expected.
   */
  private static void testEnv(IteratorScope scope, Map<String,String> opts,
      IteratorEnvironment env) {
    String expTableIdString = opts.get(EXPECTED_TABLE_ID_OPT);
    TableId expectedTableId = expTableIdString == null ? null : TableId.of(expTableIdString);

    if (scope != IteratorScope.majc) {
      assertThrows(IllegalStateException.class, env::isUserCompaction,
          "Test failed - Expected to throw IllegalStateException when checking compaction");
      assertThrows(IllegalStateException.class, env::isFullMajorCompaction,
          "Test failed - Expected to throw IllegalStateException when checking compaction");
    } else {
      assertDoesNotThrow(env::isUserCompaction,
          "Test failed - Expected not to throw exception when checking compaction");
      assertDoesNotThrow(env::isFullMajorCompaction,
          "Test failed - Expected not to throw exception when checking compaction");
    }

    PluginEnvironment pluginEnv = env.getPluginEnv();

    // verify property exists on the table config,
    // with and without custom prefix, but not in the system config
    var tableConf = pluginEnv.getConfiguration(env.getTableId());
    assertEquals(CUSTOM_PROP_VAL, tableConf.get(CUSTOM_PROP_KEY),
        "Test failed - Expected table property not found in table conf.");
    assertEquals(CUSTOM_PROP_VAL, tableConf.getTableCustom(CUSTOM_PROP_KEY_SUFFIX),
        "Test failed - Expected table property not found in table conf.");
    var systemConf = pluginEnv.getConfiguration();
    assertNull(systemConf.get(CUSTOM_PROP_KEY),
        "Test failed - Unexpected table property found in system conf.");

    // check other environment settings
    assertEquals(scope, env.getIteratorScope(), "Test failed - Error getting iterator scope");
    assertFalse(env.isSamplingEnabled(),
        "Test failed - isSamplingEnabled returned true, expected false");
    assertEquals(expectedTableId, env.getTableId(), "Test failed - Error getting Table ID");
    assertNull(env.getSamplerConfiguration());
    assertThrows(Exception.class, env::cloneWithSamplingEnabled);
    // per javadoc, getAuthorizations will throw UOE if scope != scan
    if (env.getIteratorScope() == IteratorScope.scan) {
      assertNotNull(env.getAuthorizations());
    } else {
      assertThrows(UnsupportedOperationException.class, env::getAuthorizations);
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
    testCompact(tables[1], MajcIter.class);
    testMinCompact(tables[2], MincIter.class);
    // No table when scanning at file level
    testRFileScan(ScanIter.class);
    testOfflineScan(tables[3], ScanIter.class);
    testClientSideScan(tables[4], ScanIter.class);
  }

  private void testScan(String tableName,
      Class<? extends SortedKeyValueIterator<Key,Value>> iteratorClass) throws Exception {
    writeData(tableName);

    IteratorSetting cfg = new IteratorSetting(1, iteratorClass);
    cfg.addOption(EXPECTED_TABLE_ID_OPT, client.tableOperations().tableIdMap().get(tableName));
    try (Scanner scan = client.createScanner(tableName)) {
      scan.addScanIterator(cfg);
      validateScanner(scan);
    }
  }

  public void testCompact(String tableName,
      Class<? extends SortedKeyValueIterator<Key,Value>> iteratorClass) throws Exception {
    writeData(tableName);

    IteratorSetting cfg = new IteratorSetting(1, iteratorClass);
    cfg.addOption(EXPECTED_TABLE_ID_OPT, client.tableOperations().tableIdMap().get(tableName));
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
    cfg.addOption(EXPECTED_TABLE_ID_OPT, client.tableOperations().tableIdMap().get(tableName));

    client.tableOperations().attachIterator(tableName, cfg, EnumSet.of(IteratorScope.minc));

    client.tableOperations().flush(tableName, null, null, true);

    try (Scanner scan = client.createScanner(tableName)) {
      validateScanner(scan);
    }
  }

  private void testRFileScan(Class<? extends SortedKeyValueIterator<Key,Value>> iteratorClass)
      throws Exception {
    TreeMap<Key,Value> data = createTestData();
    var fs = FileSystem.getLocal(new Configuration());
    String rFilePath = createRFile(fs, data);

    IteratorSetting cfg = new IteratorSetting(1, iteratorClass);

    try (Scanner scan = RFile.newScanner().from(rFilePath).withFileSystem(fs)
        .withTableProperties(getTableConfig().getProperties()).build()) {
      scan.addScanIterator(cfg);
      validateScanner(scan);
    }
  }

  public void testOfflineScan(String tableName,
      Class<? extends SortedKeyValueIterator<Key,Value>> iteratorClass) throws Exception {
    writeData(tableName);

    TableId tableId = TableId.of(client.tableOperations().tableIdMap().get(tableName));
    client.tableOperations().offline(tableName, true);

    IteratorSetting cfg = new IteratorSetting(1, iteratorClass);
    cfg.addOption(EXPECTED_TABLE_ID_OPT, tableId.canonical());

    try (OfflineScanner scan =
        new OfflineScanner((ClientContext) client, tableId, Authorizations.EMPTY)) {
      scan.addScanIterator(cfg);
      validateScanner(scan);
    }
  }

  public void testClientSideScan(String tableName,
      Class<? extends SortedKeyValueIterator<Key,Value>> iteratorClass) throws Exception {
    writeData(tableName);

    IteratorSetting cfg = new IteratorSetting(1, iteratorClass);
    cfg.addOption(EXPECTED_TABLE_ID_OPT, client.tableOperations().tableIdMap().get(tableName));

    try (Scanner scan = client.createScanner(tableName);
        var clientIterScan = new ClientSideIteratorScanner(scan)) {
      clientIterScan.addScanIterator(cfg);
      validateScanner(clientIterScan);
    }
  }

  private TreeMap<Key,Value> createTestData() {
    TreeMap<Key,Value> testData = new TreeMap<>();

    // Write data that we do not expect to be filtered out
    testData.put(new Key("row1", GOOD_COL_FAM, "cq1"), new Value("val1"));
    testData.put(new Key("row2", GOOD_COL_FAM, "cq1"), new Value("val2"));
    testData.put(new Key("row3", GOOD_COL_FAM, "cq1"), new Value("val3"));
    // Write data that we expect to be filtered out
    testData.put(new Key("row4", BAD_COL_FAM, "badcq"), new Value("val1"));
    testData.put(new Key("row5", BAD_COL_FAM, "badcq"), new Value("val2"));
    testData.put(new Key("row6", BAD_COL_FAM, "badcq"), new Value("val3"));

    return testData;
  }

  private void validateScanner(Scanner scan) {
    // Ensure the bad cf was filtered out to ensure init() and testEnv() were called
    int numElts = 0;
    for (var e : scan) {
      numElts++;
      assertEquals(GOOD_COL_FAM, e.getKey().getColumnFamily().toString());
    }
    assertEquals(3, numElts);
  }

  private String createRFile(FileSystem fs, TreeMap<Key,Value> data) throws Exception {
    String filePath = tempDir.resolve("test.rf").toAbsolutePath().toString();

    try (RFileWriter writer = RFile.newWriter().to(filePath).withFileSystem(fs).build()) {
      writer.append(data.entrySet());
    }

    return filePath;
  }

  private NewTableConfiguration getTableConfig() {
    NewTableConfiguration ntc = new NewTableConfiguration();
    ntc.setProperties(Collections.singletonMap(CUSTOM_PROP_KEY, CUSTOM_PROP_VAL));
    return ntc;
  }

  private void writeData(String tableName) throws Exception {
    client.tableOperations().create(tableName, getTableConfig());

    try (BatchWriter bw = client.createBatchWriter(tableName)) {
      for (Map.Entry<Key,Value> data : createTestData().entrySet()) {
        var key = data.getKey();
        Mutation m = new Mutation(key.getRow());
        m.at().family(key.getColumnFamily()).qualifier(key.getColumnQualifier())
            .put(data.getValue());
        bw.addMutation(m);
      }
    }
  }
}

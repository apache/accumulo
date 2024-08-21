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

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;

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
import org.apache.accumulo.core.clientImpl.OfflineScanner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.IteratorUtil.IteratorScope;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.WrappingIterator;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.spi.common.ServiceEnvironment;
import org.apache.accumulo.harness.AccumuloClusterHarness;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloConfigImpl;
import org.apache.accumulo.test.functional.FunctionalTestUtils;
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
  public static class ScanIter extends WrappingIterator {
    IteratorScope scope = IteratorScope.scan;

    @Override
    public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options,
        IteratorEnvironment env) throws IOException {
      super.init(source, options, env);
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
  }

  /**
   * Basic compaction iterator to test IteratorEnvironment returns what is expected.
   */
  public static class MajcIter extends WrappingIterator {
    IteratorScope scope = IteratorScope.majc;

    @Override
    public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options,
        IteratorEnvironment env) throws IOException {
      super.init(source, options, env);
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
  }

  /**
   *
   */
  public static class MincIter extends WrappingIterator {
    IteratorScope scope = IteratorScope.minc;

    @Override
    public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options,
        IteratorEnvironment env) throws IOException {
      super.init(source, options, env);
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
  }

  /**
   * Test the environment methods return what is expected.
   */
  private static void testEnv(IteratorScope scope, Map<String,String> opts,
      IteratorEnvironment env) {
    // In some cases, a table id won't be provided (e.g., testing the env of RFileScanner)
    String tableIdStr = opts.get("expected.table.id");
    TableId expectedTableId = tableIdStr != null ? TableId.of(tableIdStr) : null;

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
    if (!Objects.equals(expectedTableId, env.getTableId())) {
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
    try (Scanner scan = client.createScanner(tableName)) {
      scan.addScanIterator(cfg);
      Iterator<Map.Entry<Key,Value>> iter = scan.iterator();
      iter.forEachRemaining(e -> assertEquals("cf1", e.getKey().getColumnFamily().toString()));
    }
  }

  private void testRFileScan(Class<? extends SortedKeyValueIterator<Key,Value>> iteratorClass)
      throws Exception {
    LocalFileSystem fs = FileSystem.getLocal(new Configuration());
    String rFilePath = createRFile(fs);
    IteratorSetting is = new IteratorSetting(1, iteratorClass);

    try (Scanner scanner = RFile.newScanner().from(rFilePath).withFileSystem(fs)
        .withTableProperties(getTableConfig().getProperties()).build()) {
      scanner.addScanIterator(is);
      var unused = scanner.iterator();
    }
  }

  public void testOfflineScan(String tableName,
      Class<? extends SortedKeyValueIterator<Key,Value>> iteratorClass) throws Exception {
    writeData(tableName);
    TableId tableId = getServerContext().getTableId(tableName);
    getServerContext().tableOperations().offline(tableName, true);
    IteratorSetting is = new IteratorSetting(1, iteratorClass);
    is.addOption("expected.table.id",
        getServerContext().tableOperations().tableIdMap().get(tableName));

    try (OfflineScanner scanner =
        new OfflineScanner(getServerContext(), tableId, new Authorizations())) {
      scanner.addScanIterator(is);
      var unused = scanner.iterator();
    }
  }

  public void testClientSideScan(String tableName,
      Class<? extends SortedKeyValueIterator<Key,Value>> iteratorClass) throws Exception {
    writeData(tableName);
    IteratorSetting is = new IteratorSetting(1, iteratorClass);
    is.addOption("expected.table.id",
        getServerContext().tableOperations().tableIdMap().get(tableName));

    try (Scanner scanner = client.createScanner(tableName);
        var clientIterScanner = new ClientSideIteratorScanner(scanner)) {
      clientIterScanner.addScanIterator(is);
      var unused = clientIterScanner.iterator();
    }
  }

  public void testCompact(String tableName,
      Class<? extends SortedKeyValueIterator<Key,Value>> iteratorClass) throws Exception {
    writeData(tableName);

    IteratorSetting cfg = new IteratorSetting(1, iteratorClass);
    cfg.addOption("expected.table.id", client.tableOperations().tableIdMap().get(tableName));
    CompactionConfig config = new CompactionConfig();
    config.setIterators(Collections.singletonList(cfg));
    client.tableOperations().compact(tableName, config);
  }

  public void testMinCompact(String tableName,
      Class<? extends SortedKeyValueIterator<Key,Value>> iteratorClass) throws Exception {
    writeData(tableName);

    IteratorSetting cfg = new IteratorSetting(1, iteratorClass);
    cfg.addOption("expected.table.id", client.tableOperations().tableIdMap().get(tableName));

    client.tableOperations().attachIterator(tableName, cfg, EnumSet.of(IteratorScope.minc));

    client.tableOperations().flush(tableName);
  }

  private NewTableConfiguration getTableConfig() {
    NewTableConfiguration ntc = new NewTableConfiguration();
    ntc.setProperties(Collections.singletonMap("table.custom.iterator.env.test", "value1"));
    return ntc;
  }

  private void writeData(String tableName) throws Exception {
    client.tableOperations().create(tableName, getTableConfig());

    try (BatchWriter bw = client.createBatchWriter(tableName)) {
      Mutation m = new Mutation("row1");
      m.at().family("cf1").qualifier("cq1").put("val1");
      bw.addMutation(m);
      m = new Mutation("row2");
      m.at().family("cf1").qualifier("cq1").put("val2");
      bw.addMutation(m);
      m = new Mutation("row3");
      m.at().family("cf1").qualifier("cq1").put("val3");
      bw.addMutation(m);
    }
  }

  private String createRFile(FileSystem fs) throws Exception {
    Path dir = new Path(System.getProperty("user.dir") + "/target/rfilescan-iterenv-test/testrf");

    FunctionalTestUtils.createRFiles(client, fs, dir.toString(), 1, 1, 1);
    fs.deleteOnExit(dir);

    var listStatus = fs.listStatus(dir);
    assertEquals(1, listStatus.length);
    return Arrays.stream(fs.listStatus(dir)).findFirst().orElseThrow().getPath().toString();
  }
}

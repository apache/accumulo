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

import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.Map;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.PluginEnvironment;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.admin.CompactionConfig;
import org.apache.accumulo.core.client.admin.NewTableConfiguration;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.IteratorUtil.IteratorScope;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.WrappingIterator;
import org.apache.accumulo.core.spi.common.ServiceEnvironment;
import org.apache.accumulo.harness.AccumuloClusterHarness;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloConfigImpl;
import org.apache.hadoop.conf.Configuration;
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
    TableId expectedTableId = TableId.of(opts.get("expected.table.id"));

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
    var systemConf = pluginEnv.getConfiguration();
    if (systemConf.get("table.custom.iterator.env.test") != null) {
      throw new RuntimeException("Test failed - Unexpected table property found in system conf.");
    }

    // check other environment settings
    if (!scope.equals(env.getIteratorScope())) {
      throw new RuntimeException("Test failed - Error getting iterator scope");
    }
    if (env.isSamplingEnabled()) {
      throw new RuntimeException("Test failed - isSamplingEnabled returned true, expected false");
    }
    if (!expectedTableId.equals(env.getTableId())) {
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
    String[] tables = getUniqueNames(3);
    testScan(tables[0], ScanIter.class);
    testCompact(tables[1], MajcIter.class);
    testMinCompact(tables[2], MincIter.class);
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
}

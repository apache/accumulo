/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.accumulo.test;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.Collections;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.Map;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.IteratorSetting;
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
import org.apache.accumulo.harness.AccumuloClusterHarness;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloConfigImpl;
import org.apache.hadoop.conf.Configuration;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Test that objects in IteratorEnvironment returned from the server are as expected.
 */
public class IteratorEnvIT extends AccumuloClusterHarness {

  @Override
  public void configureMiniCluster(MiniAccumuloConfigImpl cfg, Configuration hadoopCoreSite) {
    cfg.setNumTservers(1);
  }

  @Override
  protected int defaultTimeoutSeconds() {
    return 60;
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
  @SuppressWarnings("deprecation")
  private static void testEnv(IteratorScope scope, Map<String,String> opts,
      IteratorEnvironment env) {
    TableId expectedTableId = TableId.of(opts.get("expected.table.id"));
    if (!"value1".equals(env.getConfig().get("table.custom.iterator.env.test")) && !"value1".equals(
        env.getServiceEnv().getConfiguration(env.getTableId()).getTableCustom("iterator.env.test")))
      throw new RuntimeException("Test failed - Expected table property not found.");
    if (!"value1".equals(env.getConfig().get("table.custom.iterator.env.test")) && !"value1".equals(
        env.getPluginEnv().getConfiguration(env.getTableId()).getTableCustom("iterator.env.test")))
      throw new RuntimeException("Test failed - Expected table property not found.");
    if (!scope.equals(env.getIteratorScope()))
      throw new RuntimeException("Test failed - Error getting iterator scope");
    if (env.isSamplingEnabled())
      throw new RuntimeException("Test failed - isSamplingEnabled returned true, expected false");
    if (!expectedTableId.equals(env.getTableId()))
      throw new RuntimeException("Test failed - Error getting Table ID");

  }

  @Before
  public void setup() {
    client = Accumulo.newClient().from(getClientProps()).build();
  }

  @After
  public void finish() {
    client.close();
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

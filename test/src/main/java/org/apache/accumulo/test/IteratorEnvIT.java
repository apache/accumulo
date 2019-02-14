/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.accumulo.test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.admin.CompactionConfig;
import org.apache.accumulo.core.client.admin.NewTableConfiguration;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.IteratorUtil.IteratorScope;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.WrappingIterator;
import org.apache.accumulo.harness.AccumuloClusterHarness;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

/**
 * Test that objects in IteratorEnvironment returned from the server are as expected.
 */
public class IteratorEnvIT extends AccumuloClusterHarness {
  @ClassRule
  public static ExpectedException exception = ExpectedException.none();

  private AccumuloClient client;

  public static class ScanIter extends WrappingIterator {
    IteratorScope scope = IteratorScope.scan;

    @Override
    public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options,
        IteratorEnvironment env) throws IOException {
      super.init(source, options, env);
      testEnv(env, scope);
    }
  }

  public static class MajcIter extends WrappingIterator {
    IteratorScope scope = IteratorScope.majc;

    @Override
    public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options,
        IteratorEnvironment env) throws IOException {
      super.init(source, options, env);
      testEnv(env, scope);
    }
  }

  /**
   * Checking for compaction on a scan should throw an error.
   */
  public static class BadStateIter extends WrappingIterator {
    @Override
    public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options,
        IteratorEnvironment env) throws IOException {
      super.init(source, options, env);
      try {
        assertFalse(env.isUserCompaction());
        fail("Expected to throw IllegalStateException when checking compaction on a scan.");
      } catch (IllegalStateException e) {}
      ;
    }
  }

  /**
   * Test the environment methods return what is expected.
   */
  public static void testEnv(IteratorEnvironment env, IteratorScope scope) {
    assertEquals("value1", env.getConfig().get("table.custom.iterator.env.test"));
    System.out.println("MIKE got conf in iter:");
    env.getServiceEnv().getConfiguration()
        .forEach(e -> System.out.println(e.getKey() + "=" + e.getValue()));
    assertEquals("value1",
        env.getServiceEnv().getConfiguration().getTableCustom("iterator.env.test"));
    assertEquals(scope, env.getIteratorScope());
    assertFalse(env.isSamplingEnabled());
    if (scope != IteratorScope.scan) {
      assertFalse(env.isUserCompaction());
      assertFalse(env.isFullMajorCompaction());
    }
  }

  @Before
  public void setup() {
    client = createAccumuloClient();
  }

  @After
  public void finish() {
    client.close();
  }

  /**
   * Test the scan environment methods return what is expected.
   */
  @Test
  public void testScanEnv() throws Exception {
    String[] tables = getUniqueNames(2);
    testScan(tables[0], ScanIter.class);
    testScan(tables[1], BadStateIter.class);
  }

  public void testCompactEnv() throws Exception {
    String tableName = getUniqueNames(1)[0];
    IteratorSetting cfg = new IteratorSetting(1, MajcIter.class);
    client.tableOperations().create(tableName);

    writeData(tableName);

    CompactionConfig config = new CompactionConfig();
    config.setIterators(Collections.singletonList(cfg));
    client.tableOperations().compact(tableName, config);
  }

  private void testScan(String tableName,
      Class<? extends SortedKeyValueIterator<Key,Value>> iteratorClass) throws Exception {
    NewTableConfiguration ntc = new NewTableConfiguration();
    ntc.setProperties(Collections.singletonMap("table.custom.iterator.env.test", "value1"));
    client.tableOperations().create(tableName, ntc);

    writeData(tableName);

    IteratorSetting cfg = new IteratorSetting(1, iteratorClass);
    try (Scanner scan = client.createScanner(tableName)) {
      scan.addScanIterator(cfg);
      Iterator<Map.Entry<Key,Value>> iter = scan.iterator();
      iter.forEachRemaining(e -> assertEquals("cf1", e.getKey().getColumnFamily().toString()));
    }
  }

  private void writeData(String tableName) throws Exception {
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

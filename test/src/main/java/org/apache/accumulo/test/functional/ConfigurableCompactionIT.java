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

import static java.util.Collections.singletonMap;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeSet;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.admin.NewTableConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.DataFileColumnFamily;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.util.UtilWaitThread;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloConfigImpl;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.Test;

import com.google.common.collect.Iterators;

public class ConfigurableCompactionIT extends ConfigurableMacBase {

  @Override
  protected Duration defaultTimeout() {
    return Duration.ofMinutes(2);
  }

  @Override
  public void configure(MiniAccumuloConfigImpl cfg, Configuration hadoopCoreSite) {
    cfg.setSiteConfig(singletonMap(Property.TSERV_MAJC_DELAY.getKey(), "1s"));
  }

  @SuppressWarnings("removal")
  public static class SimpleCompactionStrategy
      extends org.apache.accumulo.tserver.compaction.CompactionStrategy {

    @Override
    public void init(Map<String,String> options) {
      String countString = options.get("count");
      if (countString != null) {
        count = Integer.parseInt(countString);
      }
    }

    int count = 3;

    @Override
    public boolean
        shouldCompact(org.apache.accumulo.tserver.compaction.MajorCompactionRequest request) {
      return request.getFiles().size() == count;

    }

    @Override
    public org.apache.accumulo.tserver.compaction.CompactionPlan
        getCompactionPlan(org.apache.accumulo.tserver.compaction.MajorCompactionRequest request) {
      var result = new org.apache.accumulo.tserver.compaction.CompactionPlan();
      result.inputFiles.addAll(request.getFiles().keySet());
      return result;
    }

  }

  @SuppressWarnings("removal")
  @Test
  public void test() throws Exception {
    try (AccumuloClient c = Accumulo.newClient().from(getClientProperties()).build()) {
      final String tableName = getUniqueNames(1)[0];

      c.tableOperations().create(tableName, new NewTableConfiguration().setProperties(singletonMap(
          Property.TABLE_COMPACTION_STRATEGY.getKey(), SimpleCompactionStrategy.class.getName())));
      runTest(c, tableName, 3);

      c.tableOperations().setProperty(tableName,
          Property.TABLE_COMPACTION_STRATEGY_PREFIX.getKey() + "count", "" + 5);
      runTest(c, tableName, 5);
    }
  }

  @SuppressWarnings("removal")
  @Test
  public void testPerTableClasspath() throws Exception {
    try (AccumuloClient c = Accumulo.newClient().from(getClientProperties()).build()) {
      final String tableName = getUniqueNames(1)[0];
      var destFile = initJar("/org/apache/accumulo/test/TestCompactionStrat.jar",
          "TestCompactionStrat", getCluster().getConfig().getAccumuloDir().getAbsolutePath());
      c.instanceOperations().setProperty(
          Property.VFS_CONTEXT_CLASSPATH_PROPERTY.getKey() + "context1", destFile.toString());
      Map<String,String> props = new HashMap<>();
      props.put(Property.TABLE_MAJC_RATIO.getKey(), "10");
      props.put(Property.TABLE_CLASSLOADER_CONTEXT.getKey(), "context1");
      // EfgCompactionStrat will only compact a tablet w/ end row of 'efg'. No other tablets are
      // compacted.
      props.put(Property.TABLE_COMPACTION_STRATEGY.getKey(),
          "org.apache.accumulo.test.EfgCompactionStrat");
      c.tableOperations().create(tableName, new NewTableConfiguration().setProperties(props)
          .withSplits(new TreeSet<>(Arrays.asList(new Text("efg")))));

      for (char ch = 'a'; ch < 'l'; ch++) {
        writeFlush(c, tableName, ch + "");
      }

      while (countFiles(c) != 7) {
        UtilWaitThread.sleep(200);
      }
    }
  }

  private void writeFlush(AccumuloClient client, String tablename, String row) throws Exception {
    try (BatchWriter bw = client.createBatchWriter(tablename)) {
      Mutation m = new Mutation(row);
      m.put("", "", "");
      bw.addMutation(m);
    }
    client.tableOperations().flush(tablename, null, null, true);
  }

  private void makeFile(AccumuloClient client, String tablename) throws Exception {
    try (BatchWriter bw = client.createBatchWriter(tablename)) {
      byte[] empty = {};
      byte[] row = new byte[10];
      random.nextBytes(row);
      Mutation m = new Mutation(row, 0, 10);
      m.put(empty, empty, empty);
      bw.addMutation(m);
      bw.flush();
    }
    client.tableOperations().flush(tablename, null, null, true);
  }

  private void runTest(final AccumuloClient c, final String tableName, final int n)
      throws Exception {
    for (int i = countFiles(c); i < n - 1; i++) {
      makeFile(c, tableName);
    }
    assertEquals(n - 1, countFiles(c));
    makeFile(c, tableName);
    for (int i = 0; i < 10; i++) {
      int count = countFiles(c);
      assertTrue(count == 1 || count == n);
      if (count == 1) {
        break;
      }
      UtilWaitThread.sleep(1000);
    }
  }

  private int countFiles(AccumuloClient c) throws Exception {
    try (Scanner s = c.createScanner(MetadataTable.NAME, Authorizations.EMPTY)) {
      s.fetchColumnFamily(DataFileColumnFamily.NAME);
      return Iterators.size(s.iterator());
    }
  }

}

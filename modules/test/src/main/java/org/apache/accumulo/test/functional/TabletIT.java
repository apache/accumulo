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
package org.apache.accumulo.test.functional;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertEquals;

import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeSet;

import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.harness.AccumuloClusterHarness;
import org.apache.accumulo.minicluster.MemoryUnit;
import org.apache.accumulo.minicluster.impl.MiniAccumuloConfigImpl;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.junit.Test;

public class TabletIT extends AccumuloClusterHarness {

  private static final int N = 1000;

  @Override
  public void configureMiniCluster(MiniAccumuloConfigImpl cfg, Configuration hadoopCoreSite) {
    Map<String,String> siteConfig = cfg.getSiteConfig();
    siteConfig.put(Property.TSERV_MAXMEM.getKey(), "128M");
    cfg.setDefaultMemory(256, MemoryUnit.MEGABYTE);
    cfg.setSiteConfig(siteConfig);
  }

  @Override
  protected int defaultTimeoutSeconds() {
    return 2 * 60;
  }

  @Test
  public void createTableTest() throws Exception {
    String tableName = getUniqueNames(1)[0];
    createTableTest(tableName, false);
    createTableTest(tableName, true);
  }

  public void createTableTest(String tableName, boolean readOnly) throws Exception {
    // create the test table within accumulo
    Connector connector = getConnector();

    if (!readOnly) {
      TreeSet<Text> keys = new TreeSet<>();
      for (int i = N / 100; i < N; i += N / 100) {
        keys.add(new Text(String.format("%05d", i)));
      }

      // presplit
      connector.tableOperations().create(tableName);
      connector.tableOperations().setProperty(tableName, Property.TABLE_SPLIT_THRESHOLD.getKey(), "200");
      connector.tableOperations().addSplits(tableName, keys);
      BatchWriter b = connector.createBatchWriter(tableName, new BatchWriterConfig());

      // populate
      for (int i = 0; i < N; i++) {
        Mutation m = new Mutation(new Text(String.format("%05d", i)));
        m.put(new Text("col" + Integer.toString((i % 3) + 1)), new Text("qual"), new Value("junk".getBytes(UTF_8)));
        b.addMutation(m);
      }
      b.close();
    }

    Scanner scanner = getConnector().createScanner(tableName, Authorizations.EMPTY);
    int count = 0;
    for (Entry<Key,Value> elt : scanner) {
      String expected = String.format("%05d", count);
      assert (elt.getKey().getRow().toString().equals(expected));
      count++;
    }
    assertEquals(N, count);
  }

}

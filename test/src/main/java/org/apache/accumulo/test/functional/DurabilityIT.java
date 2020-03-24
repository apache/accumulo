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
package org.apache.accumulo.test.functional;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.minicluster.ServerType;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloConfigImpl;
import org.apache.accumulo.miniclusterImpl.ProcessReference;
import org.apache.accumulo.test.categories.MiniClusterOnlyTests;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.google.common.collect.Iterators;

@Category({MiniClusterOnlyTests.class})
public class DurabilityIT extends ConfigurableMacBase {

  @Override
  public void configure(MiniAccumuloConfigImpl cfg, Configuration hadoopCoreSite) {
    hadoopCoreSite.set("fs.file.impl", RawLocalFileSystem.class.getName());
    cfg.setProperty(Property.INSTANCE_ZK_TIMEOUT, "15s");
    cfg.setNumTservers(1);
  }

  @Override
  protected int defaultTimeoutSeconds() {
    return 4 * 60;
  }

  static final long N = 100000;

  private String[] init(AccumuloClient c) throws Exception {
    String[] tableNames = getUniqueNames(4);
    TableOperations tableOps = c.tableOperations();
    createTable(c, tableNames[0]);
    createTable(c, tableNames[1]);
    createTable(c, tableNames[2]);
    createTable(c, tableNames[3]);
    // default is sync
    tableOps.setProperty(tableNames[1], Property.TABLE_DURABILITY.getKey(), "flush");
    tableOps.setProperty(tableNames[2], Property.TABLE_DURABILITY.getKey(), "log");
    tableOps.setProperty(tableNames[3], Property.TABLE_DURABILITY.getKey(), "none");
    return tableNames;
  }

  private void cleanup(AccumuloClient c, String[] tableNames) throws Exception {
    for (String tableName : tableNames) {
      c.tableOperations().delete(tableName);
    }
  }

  private void createTable(AccumuloClient c, String tableName) throws Exception {
    c.tableOperations().create(tableName);
  }

  @Test
  public void testSync() throws Exception {
    try (AccumuloClient client = Accumulo.newClient().from(getClientProperties()).build()) {
      String[] tableNames = init(client);
      // sync table should lose nothing
      writeSome(client, tableNames[0], N);
      restartTServer();
      assertEquals(N, readSome(client, tableNames[0]));
      cleanup(client, tableNames);
    }
  }

  @Test
  public void testFlush() throws Exception {
    try (AccumuloClient client = Accumulo.newClient().from(getClientProperties()).build()) {
      String[] tableNames = init(client);
      // flush table won't lose anything since we're not losing power/dfs
      writeSome(client, tableNames[1], N);
      restartTServer();
      assertEquals(N, readSome(client, tableNames[1]));
      cleanup(client, tableNames);
    }
  }

  @Test
  public void testLog() throws Exception {
    try (AccumuloClient client = Accumulo.newClient().from(getClientProperties()).build()) {
      String[] tableNames = init(client);
      // we're probably going to lose something the the log setting
      writeSome(client, tableNames[2], N);
      restartTServer();
      long numResults = readSome(client, tableNames[2]);
      assertTrue("Expected " + N + " >= " + numResults, numResults <= N);
      cleanup(client, tableNames);
    }
  }

  @Test
  public void testNone() throws Exception {
    try (AccumuloClient client = Accumulo.newClient().from(getClientProperties()).build()) {
      String[] tableNames = init(client);
      // probably won't get any data back without logging
      writeSome(client, tableNames[3], N);
      restartTServer();
      long numResults = readSome(client, tableNames[3]);
      assertTrue("Expected " + N + " >= " + numResults, numResults <= N);
      cleanup(client, tableNames);
    }
  }

  @Test
  public void testIncreaseDurability() throws Exception {
    try (AccumuloClient c = Accumulo.newClient().from(getClientProperties()).build()) {
      String tableName = getUniqueNames(1)[0];
      c.tableOperations().create(tableName);
      c.tableOperations().setProperty(tableName, Property.TABLE_DURABILITY.getKey(), "none");
      writeSome(c, tableName, N);
      restartTServer();
      long numResults = readSome(c, tableName);
      assertTrue("Expected " + N + " >= " + numResults, numResults <= N);
      c.tableOperations().setProperty(tableName, Property.TABLE_DURABILITY.getKey(), "sync");
      writeSome(c, tableName, N);
      restartTServer();
      assertEquals(N, readSome(c, tableName));
    }
  }

  private static Map<String,String> map(Iterable<Entry<String,String>> entries) {
    Map<String,String> result = new HashMap<>();
    for (Entry<String,String> entry : entries) {
      result.put(entry.getKey(), entry.getValue());
    }
    return result;
  }

  @Test
  public void testMetaDurability() throws Exception {
    try (AccumuloClient c = Accumulo.newClient().from(getClientProperties()).build()) {
      String tableName = getUniqueNames(1)[0];
      c.instanceOperations().setProperty(Property.TABLE_DURABILITY.getKey(), "none");
      Map<String,String> props = map(c.tableOperations().getProperties(MetadataTable.NAME));
      assertEquals("sync", props.get(Property.TABLE_DURABILITY.getKey()));
      c.tableOperations().create(tableName);
      props = map(c.tableOperations().getProperties(tableName));
      assertEquals("none", props.get(Property.TABLE_DURABILITY.getKey()));
      restartTServer();
      assertTrue(c.tableOperations().exists(tableName));
    }
  }

  private long readSome(AccumuloClient client, String table) throws Exception {
    return Iterators.size(client.createScanner(table, Authorizations.EMPTY).iterator());
  }

  private void restartTServer() throws Exception {
    for (ProcessReference proc : cluster.getProcesses().get(ServerType.TABLET_SERVER)) {
      cluster.killProcess(ServerType.TABLET_SERVER, proc);
    }
    cluster.start();
  }

  private void writeSome(AccumuloClient c, String table, long count) throws Exception {
    try (BatchWriter bw = c.createBatchWriter(table)) {
      for (int i = 1; i < count + 1; i++) {
        Mutation m = new Mutation("" + i);
        m.put("", "", "");
        bw.addMutation(m);
        if (i % (Math.max(1, count / 100)) == 0) {
          bw.flush();
        }
      }
    }
  }

}

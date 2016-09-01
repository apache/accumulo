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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeFalse;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.minicluster.ServerType;
import org.apache.accumulo.minicluster.impl.MiniAccumuloConfigImpl;
import org.apache.accumulo.minicluster.impl.ProcessReference;
import org.apache.accumulo.test.categories.MiniClusterOnlyTests;
import org.apache.accumulo.test.categories.PerformanceTests;
import org.apache.accumulo.test.mrit.IntegrationTestMapReduce;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Iterators;

@Category({MiniClusterOnlyTests.class, PerformanceTests.class})
public class DurabilityIT extends ConfigurableMacBase {
  private static final Logger log = LoggerFactory.getLogger(DurabilityIT.class);

  @Override
  public void configure(MiniAccumuloConfigImpl cfg, Configuration hadoopCoreSite) {
    hadoopCoreSite.set("fs.file.impl", RawLocalFileSystem.class.getName());
    cfg.setProperty(Property.INSTANCE_ZK_TIMEOUT, "15s");
    cfg.setNumTservers(1);
  }

  @BeforeClass
  static public void checkMR() {
    assumeFalse(IntegrationTestMapReduce.isMapReduce());
  }

  static final long N = 100000;

  private String[] init() throws Exception {
    String[] tableNames = getUniqueNames(4);
    Connector c = getConnector();
    TableOperations tableOps = c.tableOperations();
    createTable(tableNames[0]);
    createTable(tableNames[1]);
    createTable(tableNames[2]);
    createTable(tableNames[3]);
    // default is sync
    tableOps.setProperty(tableNames[1], Property.TABLE_DURABILITY.getKey(), "flush");
    tableOps.setProperty(tableNames[2], Property.TABLE_DURABILITY.getKey(), "log");
    tableOps.setProperty(tableNames[3], Property.TABLE_DURABILITY.getKey(), "none");
    return tableNames;
  }

  private void cleanup(String[] tableNames) throws Exception {
    Connector c = getConnector();
    for (String tableName : tableNames) {
      c.tableOperations().delete(tableName);
    }
  }

  private void createTable(String tableName) throws Exception {
    TableOperations tableOps = getConnector().tableOperations();
    tableOps.create(tableName);
  }

  @Test(timeout = 2 * 60 * 1000)
  public void testWriteSpeed() throws Exception {
    TableOperations tableOps = getConnector().tableOperations();
    String tableNames[] = init();
    // write some gunk, delete the table to keep that table from messing with the performance numbers of successive calls
    // sync
    long t0 = writeSome(tableNames[0], N);
    tableOps.delete(tableNames[0]);
    // flush
    long t1 = writeSome(tableNames[1], N);
    tableOps.delete(tableNames[1]);
    // log
    long t2 = writeSome(tableNames[2], N);
    tableOps.delete(tableNames[2]);
    // none
    long t3 = writeSome(tableNames[3], N);
    tableOps.delete(tableNames[3]);
    System.out.println(String.format("sync %d flush %d log %d none %d", t0, t1, t2, t3));
    assertTrue("flush should be faster than sync", t0 > t1);
    assertTrue("log should be faster than flush", t1 > t2);
    assertTrue("no durability should be faster than log", t2 > t3);
  }

  @Test(timeout = 4 * 60 * 1000)
  public void testSync() throws Exception {
    String tableNames[] = init();
    // sync table should lose nothing
    writeSome(tableNames[0], N);
    restartTServer();
    assertEquals(N, readSome(tableNames[0]));
    cleanup(tableNames);
  }

  @Test(timeout = 4 * 60 * 1000)
  public void testFlush() throws Exception {
    String tableNames[] = init();
    // flush table won't lose anything since we're not losing power/dfs
    writeSome(tableNames[1], N);
    restartTServer();
    assertEquals(N, readSome(tableNames[1]));
    cleanup(tableNames);
  }

  @Test(timeout = 4 * 60 * 1000)
  public void testLog() throws Exception {
    String tableNames[] = init();
    // we're probably going to lose something the the log setting
    writeSome(tableNames[2], N);
    restartTServer();
    long numResults = readSome(tableNames[2]);
    assertTrue("Expected " + N + " >= " + numResults, N >= numResults);
    cleanup(tableNames);
  }

  @Test(timeout = 4 * 60 * 1000)
  public void testNone() throws Exception {
    String tableNames[] = init();
    // probably won't get any data back without logging
    writeSome(tableNames[3], N);
    restartTServer();
    long numResults = readSome(tableNames[3]);
    assertTrue("Expected " + N + " >= " + numResults, N >= numResults);
    cleanup(tableNames);
  }

  @Test(timeout = 4 * 60 * 1000)
  public void testIncreaseDurability() throws Exception {
    Connector c = getConnector();
    String tableName = getUniqueNames(1)[0];
    c.tableOperations().create(tableName);
    c.tableOperations().setProperty(tableName, Property.TABLE_DURABILITY.getKey(), "none");
    writeSome(tableName, N);
    restartTServer();
    long numResults = readSome(tableName);
    assertTrue("Expected " + N + " >= " + numResults, N >= numResults);
    c.tableOperations().setProperty(tableName, Property.TABLE_DURABILITY.getKey(), "sync");
    writeSome(tableName, N);
    restartTServer();
    assertTrue(N == readSome(tableName));
  }

  private static Map<String,String> map(Iterable<Entry<String,String>> entries) {
    Map<String,String> result = new HashMap<>();
    for (Entry<String,String> entry : entries) {
      result.put(entry.getKey(), entry.getValue());
    }
    return result;
  }

  @Test(timeout = 4 * 60 * 1000)
  public void testMetaDurability() throws Exception {
    Connector c = getConnector();
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

  private long readSome(String table) throws Exception {
    return Iterators.size(getConnector().createScanner(table, Authorizations.EMPTY).iterator());
  }

  private void restartTServer() throws Exception {
    for (ProcessReference proc : cluster.getProcesses().get(ServerType.TABLET_SERVER)) {
      cluster.killProcess(ServerType.TABLET_SERVER, proc);
    }
    cluster.start();
  }

  private long writeSome(String table, long count) throws Exception {
    int iterations = 5;
    long[] attempts = new long[iterations];
    for (int attempt = 0; attempt < iterations; attempt++) {
      long now = System.currentTimeMillis();
      Connector c = getConnector();
      BatchWriter bw = c.createBatchWriter(table, null);
      for (int i = 1; i < count + 1; i++) {
        Mutation m = new Mutation("" + i);
        m.put("", "", "");
        bw.addMutation(m);
        if (i % (Math.max(1, count / 100)) == 0) {
          bw.flush();
        }
      }
      bw.close();
      attempts[attempt] = System.currentTimeMillis() - now;
    }
    Arrays.sort(attempts);
    log.info("Attempt durations: {}", Arrays.toString(attempts));
    // Return the median duration
    return attempts[2];
  }

}

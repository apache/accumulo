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

import java.util.Map.Entry;

import org.apache.accumulo.cluster.ClusterServerType;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.metadata.RootTable;
import org.apache.accumulo.core.metadata.schema.MetadataSchema;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.util.UtilWaitThread;
import org.apache.accumulo.harness.AccumuloClusterIT;
import org.apache.accumulo.minicluster.impl.MiniAccumuloConfigImpl;
import org.apache.accumulo.test.functional.FunctionalTestUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class CleanWalIT extends AccumuloClusterIT {
  private static final Logger log = Logger.getLogger(CleanWalIT.class);

  @Override
  public int defaultTimeoutSeconds() {
    return 4 * 60;
  }

  @Override
  public void configureMiniCluster(MiniAccumuloConfigImpl cfg, Configuration hadoopCoreSite) {
    cfg.setProperty(Property.INSTANCE_ZK_TIMEOUT, "3s");
    cfg.setNumTservers(1);
    cfg.useMiniDFS(true);
  }

  @Before
  public void offlineTraceTable() throws Exception {
    Connector conn = getConnector();
    String traceTable = conn.instanceOperations().getSystemConfiguration().get(Property.TRACE_TABLE.getKey());
    if (conn.tableOperations().exists(traceTable)) {
      conn.tableOperations().offline(traceTable, true);
    }
  }

  @After
  public void onlineTraceTable() throws Exception {
    Connector conn = getConnector();
    String traceTable = conn.instanceOperations().getSystemConfiguration().get(Property.TRACE_TABLE.getKey());
    if (conn.tableOperations().exists(traceTable)) {
      conn.tableOperations().online(traceTable, true);
    }
  }

  // test for ACCUMULO-1830
  @Test
  public void test() throws Exception {
    Connector conn = getConnector();
    String tableName = getUniqueNames(1)[0];
    conn.tableOperations().create(tableName);
    BatchWriter bw = conn.createBatchWriter(tableName, new BatchWriterConfig());
    Mutation m = new Mutation("row");
    m.put("cf", "cq", "value");
    bw.addMutation(m);
    bw.close();
    getCluster().getClusterControl().stopAllServers(ClusterServerType.TABLET_SERVER);
    // all 3 tables should do recovery, but the bug doesn't really remove the log file references

    getCluster().getClusterControl().startAllServers(ClusterServerType.TABLET_SERVER);

    for (String table : new String[] {MetadataTable.NAME, RootTable.NAME})
      conn.tableOperations().flush(table, null, null, true);
    log.debug("Checking entries for " + tableName);
    assertEquals(1, count(tableName, conn));
    for (String table : new String[] {MetadataTable.NAME, RootTable.NAME}) {
      log.debug("Checking logs for " + table);
      assertEquals("Found logs for " + table, 0, countLogs(table, conn));
    }

    bw = conn.createBatchWriter(tableName, new BatchWriterConfig());
    m = new Mutation("row");
    m.putDelete("cf", "cq");
    bw.addMutation(m);
    bw.close();
    assertEquals(0, count(tableName, conn));
    conn.tableOperations().flush(tableName, null, null, true);
    conn.tableOperations().flush(MetadataTable.NAME, null, null, true);
    conn.tableOperations().flush(RootTable.NAME, null, null, true);
    try {
      getCluster().getClusterControl().stopAllServers(ClusterServerType.TABLET_SERVER);
      UtilWaitThread.sleep(3 * 1000);
    } finally {
      getCluster().getClusterControl().startAllServers(ClusterServerType.TABLET_SERVER);
    }
    assertEquals(0, count(tableName, conn));
  }

  private int countLogs(String tableName, Connector conn) throws TableNotFoundException {
    Scanner scanner = conn.createScanner(MetadataTable.NAME, Authorizations.EMPTY);
    scanner.fetchColumnFamily(MetadataSchema.TabletsSection.LogColumnFamily.NAME);
    int count = 0;
    for (Entry<Key,Value> entry : scanner) {
      log.debug("Saw " + entry.getKey() + "=" + entry.getValue());
      count++;
    }
    return count;
  }

  int count(String tableName, Connector conn) throws Exception {
    Scanner s = conn.createScanner(tableName, Authorizations.EMPTY);
    return FunctionalTestUtils.count(s);
  }

}

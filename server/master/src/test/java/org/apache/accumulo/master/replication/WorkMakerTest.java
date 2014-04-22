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
package org.apache.accumulo.master.replication;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.conf.ConfigurationCopy;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.KeyExtent;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.replication.ReplicationSchema.WorkSection;
import org.apache.accumulo.core.replication.ReplicationTable;
import org.apache.accumulo.core.replication.StatusUtil;
import org.apache.accumulo.server.conf.TableConfiguration;
import org.apache.accumulo.server.util.ReplicationTableUtil;
import org.apache.hadoop.io.Text;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import com.google.common.collect.ImmutableMap;

/**
 * 
 */
public class WorkMakerTest {

  private final Map<String,TableConfiguration> tableConfs = new HashMap<>();

  private MockInstance instance;
  private Connector conn;

  @Rule
  public TestName name = new TestName();

  @Before
  public void createMockAccumulo() throws Exception {
    instance = new MockInstance();
    conn = instance.getConnector("root", new PasswordToken(""));

    if (conn.tableOperations().exists(ReplicationTable.NAME)) {
      conn.tableOperations().delete(ReplicationTable.NAME);
    }

    conn.tableOperations().create(ReplicationTable.NAME);

    tableConfs.clear();
  }

  @Test
  public void singleUnitSingleTarget() throws Exception {
    String table = name.getMethodName();
    conn.tableOperations().create(name.getMethodName());
    String tableId = conn.tableOperations().tableIdMap().get(table);
    String file = "hdfs://localhost:8020/accumulo/wal/123456-1234-1234-12345678";

    KeyExtent extent = new KeyExtent(new Text(tableId), null, null);
    Mutation m = ReplicationTableUtil.createUpdateMutation(file, StatusUtil.fileClosedValue(), extent);
    BatchWriter bw = ReplicationTable.getBatchWriter(conn);
    bw.addMutation(m);
    bw.close();

    ConfigurationCopy conf = new ConfigurationCopy(new HashMap<String,String>());

    WorkMaker workMaker = new WorkMaker(conn, conf);

    workMaker.addWorkRecord(new Text(file), StatusUtil.fileClosedValue(), ImmutableMap.of("remote_cluster_1", "4"));

    Scanner s = ReplicationTable.getScanner(conn);
    s.setRange(WorkSection.getRange());
    Iterator<Entry<Key,Value>> iter = s.iterator();
    Assert.assertTrue(iter.hasNext());

    Entry<Key,Value> workEntry = iter.next();
    Key workKey = workEntry.getKey();

    Assert.assertEquals(WorkSection.getRowPrefix() + file, workKey.getRow().toString());
    Assert.assertEquals("remote_cluster_1", workKey.getColumnFamily().toString());
    Assert.assertEquals("4", workKey.getColumnQualifier().toString());
    Assert.assertEquals(workEntry.getValue(), StatusUtil.fileClosedValue());

    Assert.assertFalse(iter.hasNext());
  }

  @Test
  public void singleUnitMultipleTargets() throws Exception {
    String table = name.getMethodName();
    conn.tableOperations().create(name.getMethodName());
    String tableId = conn.tableOperations().tableIdMap().get(table);
    String file = "hdfs://localhost:8020/accumulo/wal/123456-1234-1234-12345678";

    KeyExtent extent = new KeyExtent(new Text(tableId), null, null);
    Mutation m = ReplicationTableUtil.createUpdateMutation(file, StatusUtil.fileClosedValue(), extent);
    BatchWriter bw = ReplicationTable.getBatchWriter(conn);
    bw.addMutation(m);
    bw.close();

    ConfigurationCopy conf = new ConfigurationCopy(new HashMap<String,String>());

    WorkMaker workMaker = new WorkMaker(conn, conf);

    Map<String,String> targetClusters = ImmutableMap.of("remote_cluster_1", "4", "remote_cluster_2", "6", "remote_cluster_3", "8");
    workMaker.addWorkRecord(new Text(file), StatusUtil.fileClosedValue(), targetClusters);

    Scanner s = ReplicationTable.getScanner(conn);
    s.setRange(WorkSection.getRange());

    Map<String,String> actualClusters = new HashMap<>();
    for (Entry<Key,Value> entry : s) {
      actualClusters.put(entry.getKey().getColumnFamily().toString(), entry.getKey().getColumnQualifier().toString());
    }

    for (Entry<String,String> expected : targetClusters.entrySet()) {
      Assert.assertTrue("Did not find expected key: " + expected.getKey(), actualClusters.containsKey(expected.getKey()));
      Assert.assertEquals("Values differed for " + expected.getKey(), expected.getValue(), actualClusters.get(expected.getKey()));
      actualClusters.remove(expected.getKey());
    }

    Assert.assertTrue("Found extra replication work entries: " + actualClusters, actualClusters.isEmpty());
  }

}

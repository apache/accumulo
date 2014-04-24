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
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.KeyExtent;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.replication.ReplicationSchema.WorkSection;
import org.apache.accumulo.core.replication.ReplicationTarget;
import org.apache.accumulo.core.replication.StatusUtil;
import org.apache.accumulo.server.conf.TableConfiguration;
import org.apache.accumulo.server.replication.ReplicationTable;
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
    bw.flush();

    WorkMaker workMaker = new WorkMaker(conn);

    ReplicationTarget expected = new ReplicationTarget("remote_cluster_1", "4");
    workMaker.setBatchWriter(bw);
    workMaker.addWorkRecord(new Text(file), StatusUtil.fileClosedValue(), ImmutableMap.of("remote_cluster_1", "4"));

    Scanner s = ReplicationTable.getScanner(conn);
    WorkSection.limit(s);

    Iterator<Entry<Key,Value>> iter = s.iterator();
    Assert.assertTrue(iter.hasNext());

    Entry<Key,Value> workEntry = iter.next();
    Key workKey = workEntry.getKey();
    ReplicationTarget actual = ReplicationTarget.from(workKey.getColumnQualifier());

    Assert.assertEquals(file, workKey.getRow().toString());
    Assert.assertEquals(WorkSection.NAME, workKey.getColumnFamily());
    Assert.assertEquals(expected, actual);
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
    bw.flush();

    WorkMaker workMaker = new WorkMaker(conn);

    Map<String,String> targetClusters = ImmutableMap.of("remote_cluster_1", "4", "remote_cluster_2", "6", "remote_cluster_3", "8");
    Set<ReplicationTarget> expectedTargets = new HashSet<>();
    for (Entry<String,String> cluster : targetClusters.entrySet()) {
      expectedTargets.add(new ReplicationTarget(cluster.getKey(), cluster.getValue()));
    }
    workMaker.setBatchWriter(bw);
    workMaker.addWorkRecord(new Text(file), StatusUtil.fileClosedValue(), targetClusters);

    Scanner s = ReplicationTable.getScanner(conn);
    WorkSection.limit(s);

    Set<ReplicationTarget> actualTargets = new HashSet<>();
    for (Entry<Key,Value> entry : s) {
      Assert.assertEquals(file, entry.getKey().getRow().toString());
      Assert.assertEquals(WorkSection.NAME, entry.getKey().getColumnFamily());

      ReplicationTarget target = ReplicationTarget.from(entry.getKey().getColumnQualifier());
      actualTargets.add(target);
    }

    for (ReplicationTarget expected : expectedTargets) {
      Assert.assertTrue("Did not find expected target: " + expected, actualTargets.contains(expected));
      actualTargets.remove(expected);
    }

    Assert.assertTrue("Found extra replication work entries: " + actualTargets, actualTargets.isEmpty());
  }
}

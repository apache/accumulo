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
package org.apache.accumulo.test.replication;

import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.accumulo.cluster.ClusterUser;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.client.impl.Table;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorUtil.IteratorScope;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.metadata.schema.MetadataSchema;
import org.apache.accumulo.core.protobuf.ProtobufUtil;
import org.apache.accumulo.core.replication.ReplicationSchema.StatusSection;
import org.apache.accumulo.core.replication.ReplicationTable;
import org.apache.accumulo.core.security.TablePermission;
import org.apache.accumulo.harness.SharedMiniClusterBase;
import org.apache.accumulo.server.replication.StatusUtil;
import org.apache.accumulo.server.replication.proto.Replication.Status;
import org.apache.accumulo.server.util.ReplicationTableUtil;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.Iterables;

public class StatusCombinerMacIT extends SharedMiniClusterBase {

  @Override
  public int defaultTimeoutSeconds() {
    return 60;
  }

  @BeforeClass
  public static void setup() throws Exception {
    SharedMiniClusterBase.startMiniCluster();
  }

  @AfterClass
  public static void teardown() throws Exception {
    SharedMiniClusterBase.stopMiniCluster();
  }

  @Test
  public void testCombinerSetOnMetadata() throws Exception {
    TableOperations tops = getConnector().tableOperations();
    Map<String,EnumSet<IteratorScope>> iterators = tops.listIterators(MetadataTable.NAME);

    Assert.assertTrue(iterators.containsKey(ReplicationTableUtil.COMBINER_NAME));
    EnumSet<IteratorScope> scopes = iterators.get(ReplicationTableUtil.COMBINER_NAME);
    Assert.assertEquals(3, scopes.size());
    Assert.assertTrue(scopes.contains(IteratorScope.scan));
    Assert.assertTrue(scopes.contains(IteratorScope.minc));
    Assert.assertTrue(scopes.contains(IteratorScope.majc));

    Iterable<Entry<String,String>> propIter = tops.getProperties(MetadataTable.NAME);
    HashMap<String,String> properties = new HashMap<>();
    for (Entry<String,String> entry : propIter) {
      properties.put(entry.getKey(), entry.getValue());
    }

    for (IteratorScope scope : scopes) {
      String key = Property.TABLE_ITERATOR_PREFIX.getKey() + scope.name() + "." + ReplicationTableUtil.COMBINER_NAME + ".opt.columns";
      Assert.assertTrue("Properties did not contain key : " + key, properties.containsKey(key));
      Assert.assertEquals(MetadataSchema.ReplicationSection.COLF.toString(), properties.get(key));
    }
  }

  @Test
  public void test() throws Exception {
    Connector conn = getConnector();
    ClusterUser user = getAdminUser();

    ReplicationTable.setOnline(conn);
    conn.securityOperations().grantTablePermission(user.getPrincipal(), ReplicationTable.NAME, TablePermission.WRITE);
    BatchWriter bw = ReplicationTable.getBatchWriter(conn);
    long createTime = System.currentTimeMillis();
    try {
      Mutation m = new Mutation("file:/accumulo/wal/HW10447.local+56808/93cdc17e-7521-44fa-87b5-37f45bcb92d3");
      StatusSection.add(m, Table.ID.of("1"), StatusUtil.fileCreatedValue(createTime));
      bw.addMutation(m);
    } finally {
      bw.close();
    }

    Scanner s = ReplicationTable.getScanner(conn);
    Entry<Key,Value> entry = Iterables.getOnlyElement(s);
    Assert.assertEquals(StatusUtil.fileCreatedValue(createTime), entry.getValue());

    bw = ReplicationTable.getBatchWriter(conn);
    try {
      Mutation m = new Mutation("file:/accumulo/wal/HW10447.local+56808/93cdc17e-7521-44fa-87b5-37f45bcb92d3");
      StatusSection.add(m, Table.ID.of("1"), ProtobufUtil.toValue(StatusUtil.replicated(Long.MAX_VALUE)));
      bw.addMutation(m);
    } finally {
      bw.close();
    }

    s = ReplicationTable.getScanner(conn);
    entry = Iterables.getOnlyElement(s);
    Status stat = Status.parseFrom(entry.getValue().get());
    Assert.assertEquals(Long.MAX_VALUE, stat.getBegin());
  }

}

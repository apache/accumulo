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
package org.apache.accumulo.test.replication;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.EnumSet;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.accumulo.cluster.ClusterUser;
import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorUtil.IteratorScope;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.ReplicationSection;
import org.apache.accumulo.core.protobuf.ProtobufUtil;
import org.apache.accumulo.core.replication.ReplicationSchema.StatusSection;
import org.apache.accumulo.core.replication.ReplicationTable;
import org.apache.accumulo.core.security.TablePermission;
import org.apache.accumulo.harness.SharedMiniClusterBase;
import org.apache.accumulo.server.replication.StatusUtil;
import org.apache.accumulo.server.replication.proto.Replication.Status;
import org.apache.accumulo.server.util.ReplicationTableUtil;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

@Disabled("Replication ITs are not stable and not currently maintained")
@Deprecated
public class StatusCombinerMacIT extends SharedMiniClusterBase {

  @Override
  protected Duration defaultTimeout() {
    return Duration.ofMinutes(1);
  }

  @BeforeAll
  public static void setup() throws Exception {
    SharedMiniClusterBase.startMiniCluster();
  }

  @AfterAll
  public static void teardown() {
    SharedMiniClusterBase.stopMiniCluster();
  }

  @Test
  public void testCombinerSetOnMetadata() throws Exception {
    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
      TableOperations tops = client.tableOperations();
      Map<String,EnumSet<IteratorScope>> iterators = tops.listIterators(MetadataTable.NAME);

      assertTrue(iterators.containsKey(ReplicationTableUtil.COMBINER_NAME));
      EnumSet<IteratorScope> scopes = iterators.get(ReplicationTableUtil.COMBINER_NAME);
      assertEquals(3, scopes.size());
      assertTrue(scopes.contains(IteratorScope.scan));
      assertTrue(scopes.contains(IteratorScope.minc));
      assertTrue(scopes.contains(IteratorScope.majc));

      Map<String,String> config = tops.getConfiguration(MetadataTable.NAME);
      Map<String,String> properties = Map.copyOf(config);

      for (IteratorScope scope : scopes) {
        String key = Property.TABLE_ITERATOR_PREFIX.getKey() + scope.name() + "."
            + ReplicationTableUtil.COMBINER_NAME + ".opt.columns";
        assertTrue(properties.containsKey(key), "Properties did not contain key : " + key);
        assertEquals(ReplicationSection.COLF.toString(), properties.get(key));
      }
    }
  }

  @Test
  public void test() throws Exception {
    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
      ClusterUser user = getAdminUser();

      ReplicationTable.setOnline(client);
      client.securityOperations().grantTablePermission(user.getPrincipal(), ReplicationTable.NAME,
          TablePermission.WRITE);
      BatchWriter bw = ReplicationTable.getBatchWriter(client);
      long createTime = System.currentTimeMillis();
      try {
        Mutation m = new Mutation(
            "file:/accumulo/wal/HW10447.local+56808/93cdc17e-7521-44fa-87b5-37f45bcb92d3");
        StatusSection.add(m, TableId.of("1"), StatusUtil.fileCreatedValue(createTime));
        bw.addMutation(m);
      } finally {
        bw.close();
      }

      Entry<Key,Value> entry;
      try (Scanner s = ReplicationTable.getScanner(client)) {
        entry = getOnlyElement(s);
        assertEquals(StatusUtil.fileCreatedValue(createTime), entry.getValue());

        bw = ReplicationTable.getBatchWriter(client);
        try {
          Mutation m = new Mutation(
              "file:/accumulo/wal/HW10447.local+56808/93cdc17e-7521-44fa-87b5-37f45bcb92d3");
          StatusSection.add(m, TableId.of("1"),
              ProtobufUtil.toValue(StatusUtil.replicated(Long.MAX_VALUE)));
          bw.addMutation(m);
        } finally {
          bw.close();
        }
      }

      try (Scanner s = ReplicationTable.getScanner(client)) {
        entry = getOnlyElement(s);
        Status stat = Status.parseFrom(entry.getValue().get());
        assertEquals(Long.MAX_VALUE, stat.getBegin());
      }
    }
  }

}

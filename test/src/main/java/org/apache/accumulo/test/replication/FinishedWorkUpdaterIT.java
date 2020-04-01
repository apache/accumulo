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
package org.apache.accumulo.test.replication;

import static org.junit.Assert.assertEquals;

import java.util.Map.Entry;
import java.util.UUID;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.protobuf.ProtobufUtil;
import org.apache.accumulo.core.replication.ReplicationSchema.StatusSection;
import org.apache.accumulo.core.replication.ReplicationSchema.WorkSection;
import org.apache.accumulo.core.replication.ReplicationTable;
import org.apache.accumulo.core.replication.ReplicationTarget;
import org.apache.accumulo.core.security.TablePermission;
import org.apache.accumulo.master.replication.FinishedWorkUpdater;
import org.apache.accumulo.server.replication.proto.Replication.Status;
import org.apache.accumulo.test.functional.ConfigurableMacBase;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import com.google.common.collect.Iterables;

@Ignore("Replication ITs are not stable and not currently maintained")
public class FinishedWorkUpdaterIT extends ConfigurableMacBase {

  private AccumuloClient client;
  private FinishedWorkUpdater updater;

  @Before
  public void configureUpdater() {
    client = Accumulo.newClient().from(getClientProperties()).build();
    updater = new FinishedWorkUpdater(client);
  }

  @Test
  public void offlineReplicationTableFailsGracefully() {
    updater.run();
  }

  @Test
  public void recordsWithProgressUpdateBothTables() throws Exception {
    client.securityOperations().grantTablePermission(client.whoami(), ReplicationTable.NAME,
        TablePermission.READ);
    client.securityOperations().grantTablePermission(client.whoami(), ReplicationTable.NAME,
        TablePermission.WRITE);
    ReplicationTable.setOnline(client);

    String file = "/accumulo/wals/tserver+port/" + UUID.randomUUID();
    Status stat =
        Status.newBuilder().setBegin(100).setEnd(200).setClosed(true).setInfiniteEnd(false).build();
    ReplicationTarget target = new ReplicationTarget("peer", "table1", TableId.of("1"));

    // Create a single work record for a file to some peer
    BatchWriter bw = ReplicationTable.getBatchWriter(client);
    Mutation m = new Mutation(file);
    WorkSection.add(m, target.toText(), ProtobufUtil.toValue(stat));
    bw.addMutation(m);
    bw.close();

    updater.run();

    try (Scanner s = ReplicationTable.getScanner(client)) {
      s.setRange(Range.exact(file));
      StatusSection.limit(s);
      Entry<Key,Value> entry = Iterables.getOnlyElement(s);

      assertEquals(entry.getKey().getColumnFamily(), StatusSection.NAME);
      assertEquals(entry.getKey().getColumnQualifier().toString(),
          target.getSourceTableId().canonical());

      // We should only rely on the correct begin attribute being returned
      Status actual = Status.parseFrom(entry.getValue().get());
      assertEquals(stat.getBegin(), actual.getBegin());
    }
  }

  @Test
  public void chooseMinimumBeginOffset() throws Exception {
    client.securityOperations().grantTablePermission(client.whoami(), ReplicationTable.NAME,
        TablePermission.READ);
    client.securityOperations().grantTablePermission(client.whoami(), ReplicationTable.NAME,
        TablePermission.WRITE);
    ReplicationTable.setOnline(client);

    String file = "/accumulo/wals/tserver+port/" + UUID.randomUUID();
    Status stat1 = Status.newBuilder().setBegin(100).setEnd(1000).setClosed(true)
        .setInfiniteEnd(false).build();
    Status stat2 = Status.newBuilder().setBegin(500).setEnd(1000).setClosed(true)
        .setInfiniteEnd(false).build();
    Status stat3 =
        Status.newBuilder().setBegin(1).setEnd(1000).setClosed(true).setInfiniteEnd(false).build();
    ReplicationTarget target1 = new ReplicationTarget("peer1", "table1", TableId.of("1"));
    ReplicationTarget target2 = new ReplicationTarget("peer2", "table2", TableId.of("1"));
    ReplicationTarget target3 = new ReplicationTarget("peer3", "table3", TableId.of("1"));

    // Create a single work record for a file to some peer
    BatchWriter bw = ReplicationTable.getBatchWriter(client);
    Mutation m = new Mutation(file);
    WorkSection.add(m, target1.toText(), ProtobufUtil.toValue(stat1));
    WorkSection.add(m, target2.toText(), ProtobufUtil.toValue(stat2));
    WorkSection.add(m, target3.toText(), ProtobufUtil.toValue(stat3));
    bw.addMutation(m);
    bw.close();

    updater.run();

    try (Scanner s = ReplicationTable.getScanner(client)) {
      s.setRange(Range.exact(file));
      StatusSection.limit(s);
      Entry<Key,Value> entry = Iterables.getOnlyElement(s);

      assertEquals(entry.getKey().getColumnFamily(), StatusSection.NAME);
      assertEquals(entry.getKey().getColumnQualifier().toString(),
          target1.getSourceTableId().canonical());

      // We should only rely on the correct begin attribute being returned
      Status actual = Status.parseFrom(entry.getValue().get());
      assertEquals(1, actual.getBegin());
    }
  }

  @Test
  public void chooseMinimumBeginOffsetInfiniteEnd() throws Exception {
    client.securityOperations().grantTablePermission(client.whoami(), ReplicationTable.NAME,
        TablePermission.READ);
    client.securityOperations().grantTablePermission(client.whoami(), ReplicationTable.NAME,
        TablePermission.WRITE);
    ReplicationTable.setOnline(client);

    String file = "/accumulo/wals/tserver+port/" + UUID.randomUUID();
    Status stat1 =
        Status.newBuilder().setBegin(100).setEnd(1000).setClosed(true).setInfiniteEnd(true).build();
    Status stat2 =
        Status.newBuilder().setBegin(1).setEnd(1000).setClosed(true).setInfiniteEnd(true).build();
    Status stat3 =
        Status.newBuilder().setBegin(500).setEnd(1000).setClosed(true).setInfiniteEnd(true).build();
    ReplicationTarget target1 = new ReplicationTarget("peer1", "table1", TableId.of("1"));
    ReplicationTarget target2 = new ReplicationTarget("peer2", "table2", TableId.of("1"));
    ReplicationTarget target3 = new ReplicationTarget("peer3", "table3", TableId.of("1"));

    // Create a single work record for a file to some peer
    BatchWriter bw = ReplicationTable.getBatchWriter(client);
    Mutation m = new Mutation(file);
    WorkSection.add(m, target1.toText(), ProtobufUtil.toValue(stat1));
    WorkSection.add(m, target2.toText(), ProtobufUtil.toValue(stat2));
    WorkSection.add(m, target3.toText(), ProtobufUtil.toValue(stat3));
    bw.addMutation(m);
    bw.close();

    updater.run();

    try (Scanner s = ReplicationTable.getScanner(client)) {
      s.setRange(Range.exact(file));
      StatusSection.limit(s);
      Entry<Key,Value> entry = Iterables.getOnlyElement(s);

      assertEquals(entry.getKey().getColumnFamily(), StatusSection.NAME);
      assertEquals(entry.getKey().getColumnQualifier().toString(),
          target1.getSourceTableId().canonical());

      // We should only rely on the correct begin attribute being returned
      Status actual = Status.parseFrom(entry.getValue().get());
      assertEquals(1, actual.getBegin());
    }
  }

}

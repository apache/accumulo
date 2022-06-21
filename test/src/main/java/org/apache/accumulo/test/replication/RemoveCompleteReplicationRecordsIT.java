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
import static org.junit.jupiter.api.Assertions.assertFalse;

import java.util.Collections;
import java.util.HashSet;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.user.WholeRowIterator;
import org.apache.accumulo.core.protobuf.ProtobufUtil;
import org.apache.accumulo.core.replication.ReplicationSchema.OrderSection;
import org.apache.accumulo.core.replication.ReplicationSchema.StatusSection;
import org.apache.accumulo.core.replication.ReplicationSchema.WorkSection;
import org.apache.accumulo.core.replication.ReplicationTable;
import org.apache.accumulo.core.replication.ReplicationTarget;
import org.apache.accumulo.core.security.TablePermission;
import org.apache.accumulo.manager.replication.RemoveCompleteReplicationRecords;
import org.apache.accumulo.server.replication.StatusUtil;
import org.apache.accumulo.server.replication.proto.Replication.Status;
import org.apache.accumulo.test.functional.ConfigurableMacBase;
import org.easymock.EasyMock;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import com.google.common.collect.Iterables;

@Disabled("Replication ITs are not stable and not currently maintained")
@Deprecated
public class RemoveCompleteReplicationRecordsIT extends ConfigurableMacBase {

  private MockRemoveCompleteReplicationRecords rcrr;
  private AccumuloClient client;

  private static class MockRemoveCompleteReplicationRecords
      extends RemoveCompleteReplicationRecords {

    public MockRemoveCompleteReplicationRecords(AccumuloClient client) {
      super(client);
    }

    @Override
    public long removeCompleteRecords(AccumuloClient client, BatchScanner bs, BatchWriter bw) {
      return super.removeCompleteRecords(client, bs, bw);
    }

  }

  @BeforeEach
  public void initialize() throws Exception {
    client = Accumulo.newClient().from(getClientProperties()).build();
    rcrr = new MockRemoveCompleteReplicationRecords(client);
    client.securityOperations().grantTablePermission(client.whoami(), ReplicationTable.NAME,
        TablePermission.READ);
    client.securityOperations().grantTablePermission(client.whoami(), ReplicationTable.NAME,
        TablePermission.WRITE);
    ReplicationTable.setOnline(client);
  }

  private TableId createTableId(int i) {
    return TableId.of(Integer.toString(i));
  }

  @Test
  public void notYetReplicationRecordsIgnored() throws Exception {
    BatchWriter bw = ReplicationTable.getBatchWriter(client);
    int numRecords = 3;
    for (int i = 0; i < numRecords; i++) {
      String file = "/accumulo/wal/tserver+port/" + UUID.randomUUID();
      Mutation m = new Mutation(file);
      StatusSection.add(m, createTableId(i), StatusUtil.openWithUnknownLengthValue());
      bw.addMutation(m);
    }

    bw.close();

    assertEquals(numRecords, Iterables.size(ReplicationTable.getScanner(client)));

    try (BatchScanner bs = ReplicationTable.getBatchScanner(client, 1)) {
      bs.setRanges(Collections.singleton(new Range()));
      IteratorSetting cfg = new IteratorSetting(50, WholeRowIterator.class);
      bs.addScanIterator(cfg);
      bw = EasyMock.createMock(BatchWriter.class);

      EasyMock.replay(bw);

      rcrr.removeCompleteRecords(client, bs, bw);
      assertEquals(numRecords, Iterables.size(ReplicationTable.getScanner(client)));
    }
  }

  @Test
  public void partiallyReplicatedRecordsIgnored() throws Exception {
    BatchWriter bw = ReplicationTable.getBatchWriter(client);
    int numRecords = 3;
    Status.Builder builder = Status.newBuilder();
    builder.setClosed(false);
    builder.setEnd(10000);
    builder.setInfiniteEnd(false);
    for (int i = 0; i < numRecords; i++) {
      String file = "/accumulo/wal/tserver+port/" + UUID.randomUUID();
      Mutation m = new Mutation(file);
      StatusSection.add(m, createTableId(i),
          ProtobufUtil.toValue(builder.setBegin(1000 * (i + 1)).build()));
      bw.addMutation(m);
    }

    bw.close();

    assertEquals(numRecords, Iterables.size(ReplicationTable.getScanner(client)));

    try (BatchScanner bs = ReplicationTable.getBatchScanner(client, 1)) {
      bs.setRanges(Collections.singleton(new Range()));
      IteratorSetting cfg = new IteratorSetting(50, WholeRowIterator.class);
      bs.addScanIterator(cfg);
      bw = EasyMock.createMock(BatchWriter.class);

      EasyMock.replay(bw);

      // We don't remove any records, so we can just pass in a fake BW for both
      rcrr.removeCompleteRecords(client, bs, bw);
      assertEquals(numRecords, Iterables.size(ReplicationTable.getScanner(client)));
    }
  }

  @Test
  public void replicatedClosedWorkRecordsAreNotRemovedWithoutClosedStatusRecords()
      throws Exception {
    BatchWriter replBw = ReplicationTable.getBatchWriter(client);
    int numRecords = 3;

    Status.Builder builder = Status.newBuilder();
    builder.setClosed(false);
    builder.setEnd(10000);
    builder.setInfiniteEnd(false);

    // Write out numRecords entries to both replication and metadata tables, none of which are fully
    // replicated
    for (int i = 0; i < numRecords; i++) {
      String file = "/accumulo/wal/tserver+port/" + UUID.randomUUID();
      Mutation m = new Mutation(file);
      StatusSection.add(m, createTableId(i),
          ProtobufUtil.toValue(builder.setBegin(1000 * (i + 1)).build()));
      replBw.addMutation(m);
    }

    // Add two records that we can delete
    String fileToRemove = "/accumulo/wal/tserver+port/" + UUID.randomUUID();
    Mutation m = new Mutation(fileToRemove);
    StatusSection.add(m, TableId.of("5"),
        ProtobufUtil.toValue(builder.setBegin(10000).setEnd(10000).setClosed(false).build()));
    replBw.addMutation(m);

    numRecords++;

    fileToRemove = "/accumulo/wal/tserver+port/" + UUID.randomUUID();
    m = new Mutation(fileToRemove);
    StatusSection.add(m, TableId.of("6"),
        ProtobufUtil.toValue(builder.setBegin(10000).setEnd(10000).setClosed(false).build()));
    replBw.addMutation(m);

    numRecords++;

    replBw.flush();

    // Make sure that we have the expected number of records in both tables
    assertEquals(numRecords, Iterables.size(ReplicationTable.getScanner(client)));

    // We should not remove any records because they're missing closed status
    try (BatchScanner bs = ReplicationTable.getBatchScanner(client, 1)) {
      bs.setRanges(Collections.singleton(new Range()));
      IteratorSetting cfg = new IteratorSetting(50, WholeRowIterator.class);
      bs.addScanIterator(cfg);

      try {
        assertEquals(0L, rcrr.removeCompleteRecords(client, bs, replBw));
      } finally {
        replBw.close();
      }
    }
  }

  @Test
  public void replicatedClosedRowsAreRemoved() throws Exception {
    BatchWriter replBw = ReplicationTable.getBatchWriter(client);
    int numRecords = 3;

    Status.Builder builder = Status.newBuilder();
    builder.setClosed(false);
    builder.setEnd(10000);
    builder.setInfiniteEnd(false);

    long time = System.currentTimeMillis();
    // Write out numRecords entries to both replication and metadata tables, none of which are fully
    // replicated
    for (int i = 0; i < numRecords; i++) {
      builder.setCreatedTime(time++);
      String file = "/accumulo/wal/tserver+port/" + UUID.randomUUID();
      Mutation m = new Mutation(file);
      Value v = ProtobufUtil.toValue(builder.setBegin(1000 * (i + 1)).build());
      StatusSection.add(m, createTableId(i), v);
      replBw.addMutation(m);
      m = OrderSection.createMutation(file, time);
      OrderSection.add(m, createTableId(i), v);
      replBw.addMutation(m);
    }

    Set<String> filesToRemove = new HashSet<>();
    // We created two mutations for each file
    numRecords *= 2;
    int finalNumRecords = numRecords;

    // Add two records that we can delete
    String fileToRemove = "/accumulo/wal/tserver+port/" + UUID.randomUUID();
    filesToRemove.add(fileToRemove);
    Mutation m = new Mutation(fileToRemove);
    ReplicationTarget target = new ReplicationTarget("peer1", "5", TableId.of("5"));
    Value value = ProtobufUtil.toValue(
        builder.setBegin(10000).setEnd(10000).setClosed(true).setCreatedTime(time).build());
    StatusSection.add(m, TableId.of("5"), value);
    WorkSection.add(m, target.toText(), value);
    replBw.addMutation(m);

    m = OrderSection.createMutation(fileToRemove, time);
    OrderSection.add(m, TableId.of("5"), value);
    replBw.addMutation(m);
    time++;

    numRecords += 3;

    fileToRemove = "/accumulo/wal/tserver+port/" + UUID.randomUUID();
    filesToRemove.add(fileToRemove);
    m = new Mutation(fileToRemove);
    value = ProtobufUtil.toValue(
        builder.setBegin(10000).setEnd(10000).setClosed(true).setCreatedTime(time).build());
    target = new ReplicationTarget("peer1", "6", TableId.of("6"));
    StatusSection.add(m, TableId.of("6"), value);
    WorkSection.add(m, target.toText(), value);
    replBw.addMutation(m);

    m = OrderSection.createMutation(fileToRemove, time);
    OrderSection.add(m, TableId.of("6"), value);
    replBw.addMutation(m);
    time++;

    numRecords += 3;

    replBw.flush();

    // Make sure that we have the expected number of records in both tables
    assertEquals(numRecords, Iterables.size(ReplicationTable.getScanner(client)));

    // We should remove the two fully completed records we inserted
    try (BatchScanner bs = ReplicationTable.getBatchScanner(client, 1)) {
      bs.setRanges(Collections.singleton(new Range()));
      StatusSection.limit(bs);
      WorkSection.limit(bs);
      IteratorSetting cfg = new IteratorSetting(50, WholeRowIterator.class);
      bs.addScanIterator(cfg);

      try {
        assertEquals(4L, rcrr.removeCompleteRecords(client, bs, replBw));
      } finally {
        replBw.close();
      }

      int actualRecords = 0;
      try (var scanner = ReplicationTable.getScanner(client)) {
        for (Entry<Key,Value> entry : scanner) {
          assertFalse(filesToRemove.contains(entry.getKey().getRow().toString()));
          actualRecords++;
        }
      }

      assertEquals(finalNumRecords, actualRecords);
    }
  }

  @Test
  public void partiallyReplicatedEntriesPrecludeRowDeletion() throws Exception {
    BatchWriter replBw = ReplicationTable.getBatchWriter(client);
    int numRecords = 3;

    Status.Builder builder = Status.newBuilder();
    builder.setClosed(false);
    builder.setEnd(10000);
    builder.setInfiniteEnd(false);

    // Write out numRecords entries to both replication and metadata tables, none of which are fully
    // replicated
    for (int i = 0; i < numRecords; i++) {
      String file = "/accumulo/wal/tserver+port/" + UUID.randomUUID();
      Mutation m = new Mutation(file);
      StatusSection.add(m, createTableId(i),
          ProtobufUtil.toValue(builder.setBegin(1000 * (i + 1)).build()));
      replBw.addMutation(m);
    }

    // Add two records that we can delete
    String fileToRemove = "/accumulo/wal/tserver+port/" + UUID.randomUUID();
    Mutation m = new Mutation(fileToRemove);
    ReplicationTarget target = new ReplicationTarget("peer1", "5", TableId.of("5"));
    Value value =
        ProtobufUtil.toValue(builder.setBegin(10000).setEnd(10000).setClosed(true).build());
    StatusSection.add(m, TableId.of("5"), value);
    WorkSection.add(m, target.toText(), value);
    target = new ReplicationTarget("peer2", "5", TableId.of("5"));
    WorkSection.add(m, target.toText(), value);
    target = new ReplicationTarget("peer3", "5", TableId.of("5"));
    WorkSection.add(m, target.toText(), ProtobufUtil.toValue(builder.setClosed(false).build()));
    replBw.addMutation(m);

    numRecords += 4;

    replBw.flush();

    // Make sure that we have the expected number of records in both tables
    assertEquals(numRecords, Iterables.size(ReplicationTable.getScanner(client)));

    // We should remove the two fully completed records we inserted
    try (BatchScanner bs = ReplicationTable.getBatchScanner(client, 1)) {
      bs.setRanges(Collections.singleton(new Range()));
      IteratorSetting cfg = new IteratorSetting(50, WholeRowIterator.class);
      bs.addScanIterator(cfg);

      try {
        assertEquals(0L, rcrr.removeCompleteRecords(client, bs, replBw));
      } finally {
        replBw.close();
      }
    }
  }
}

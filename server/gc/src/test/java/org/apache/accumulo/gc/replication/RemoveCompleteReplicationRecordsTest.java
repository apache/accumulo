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
package org.apache.accumulo.gc.replication;

import java.util.HashSet;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;

import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.protobuf.ProtobufUtil;
import org.apache.accumulo.core.replication.ReplicationSchema.StatusSection;
import org.apache.accumulo.core.replication.StatusUtil;
import org.apache.accumulo.core.replication.proto.Replication.Status;
import org.apache.accumulo.server.replication.ReplicationTable;
import org.apache.hadoop.io.Text;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import com.google.common.collect.Iterables;

/**
 * 
 */
public class RemoveCompleteReplicationRecordsTest {

  private RemoveCompleteReplicationRecords rcrr;
  private MockInstance inst;
  private Connector conn;

  @Rule
  public TestName test = new TestName();
  
  @Before
  public void initialize() throws Exception {
    inst = new MockInstance(test.getMethodName());
    conn = inst.getConnector("root", new PasswordToken(""));
    rcrr = new RemoveCompleteReplicationRecords(inst);
  }

  @Test
  public void notYetReplicationRecordsIgnored() throws Exception {
    ReplicationTable.create(conn);
    BatchWriter bw = ReplicationTable.getBatchWriter(conn);
    int numRecords = 3;
    for (int i = 0; i < numRecords; i++) {
      String file = "/accumulo/wal/tserver+port/" + UUID.randomUUID();
      Mutation m = new Mutation(file);
      StatusSection.add(m, new Text(Integer.toString(i)), StatusUtil.openWithUnknownLengthValue());
      bw.addMutation(m);
    }

    bw.close();

    Assert.assertEquals(numRecords, Iterables.size(ReplicationTable.getScanner(conn)));

    BatchScanner bs = ReplicationTable.getBatchScanner(conn, 1);
    rcrr.removeCompleteRecords(conn, bs, bw);
    bs.close();

    Assert.assertEquals(numRecords, Iterables.size(ReplicationTable.getScanner(conn)));
  }

  @Test
  public void partiallyReplicatedRecordsIgnored() throws Exception {
    ReplicationTable.create(conn);
    BatchWriter bw = ReplicationTable.getBatchWriter(conn);
    int numRecords = 3;
    Status.Builder builder = Status.newBuilder();
    builder.setClosed(false);
    builder.setEnd(10000);
    builder.setInfiniteEnd(false);
    for (int i = 0; i < numRecords; i++) {
      String file = "/accumulo/wal/tserver+port/" + UUID.randomUUID();
      Mutation m = new Mutation(file);
      StatusSection.add(m, new Text(Integer.toString(i)), ProtobufUtil.toValue(builder.setBegin(1000*(i+1)).build()));
      bw.addMutation(m);
    }

    bw.close();

    Assert.assertEquals(numRecords, Iterables.size(ReplicationTable.getScanner(conn)));

    BatchScanner bs = ReplicationTable.getBatchScanner(conn, 1);
    rcrr.removeCompleteRecords(conn, bs, bw);
    bs.close();

    Assert.assertEquals(numRecords, Iterables.size(ReplicationTable.getScanner(conn)));
  }

  @Test
  public void replicatedClosedRecordsRemoved() throws Exception {
    ReplicationTable.create(conn);
    BatchWriter bw = ReplicationTable.getBatchWriter(conn);
    int numRecords = 3;
    Status.Builder builder = Status.newBuilder();
    builder.setClosed(false);
    builder.setEnd(10000);
    builder.setInfiniteEnd(false);
    for (int i = 0; i < numRecords; i++) {
      String file = "/accumulo/wal/tserver+port/" + UUID.randomUUID();
      Mutation m = new Mutation(file);
      StatusSection.add(m, new Text(Integer.toString(i)), ProtobufUtil.toValue(builder.setBegin(1000*(i+1)).build()));
      bw.addMutation(m);
    }

    Set<String> filesToRemove = new HashSet<>();
    int finalNumRecords = numRecords;

    // Add some records that we can delete
    String fileToRemove = "/accumulo/wal/tserver+port/" + UUID.randomUUID();
    filesToRemove.add(fileToRemove);
    Mutation m = new Mutation(fileToRemove);
    StatusSection.add(m, new Text("5"), ProtobufUtil.toValue(builder.setBegin(10000).setEnd(10000).setClosed(true).build()));
    bw.addMutation(m);

    numRecords++;

    fileToRemove = "/accumulo/wal/tserver+port/" + UUID.randomUUID();
    filesToRemove.add(fileToRemove);
    m = new Mutation(fileToRemove);
    StatusSection.add(m, new Text("6"), ProtobufUtil.toValue(builder.setBegin(10000).setEnd(10000).setClosed(true).build()));
    bw.addMutation(m);

    numRecords++;

    bw.close();

    Assert.assertEquals(numRecords, Iterables.size(ReplicationTable.getScanner(conn)));

    BatchScanner bs = ReplicationTable.getBatchScanner(conn, 1);
    rcrr.removeCompleteRecords(conn, bs, bw);
    bs.close();

    int actualRecords = 0;
    for (Entry<Key,Value> entry : ReplicationTable.getScanner(conn)) {
      Assert.assertFalse(filesToRemove.contains(entry.getKey().getRow().toString()));
      actualRecords++;
    }

    Assert.assertEquals(finalNumRecords, actualRecords);
  }
}

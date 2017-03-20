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
package org.apache.accumulo.tserver.replication;

import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.nio.ByteBuffer;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.impl.ClientContext;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.thrift.TMutation;
import org.apache.accumulo.core.replication.thrift.WalEdits;
import org.apache.accumulo.server.data.ServerMutation;
import org.apache.accumulo.tserver.logger.LogEvents;
import org.apache.accumulo.tserver.logger.LogFileKey;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Lists;

/**
 *
 */
public class BatchWriterReplicationReplayerTest {

  private ClientContext context;
  private Connector conn;
  private AccumuloConfiguration conf;
  private BatchWriter bw;

  @Before
  public void setUpContext() throws AccumuloException, AccumuloSecurityException {
    conn = createMock(Connector.class);
    conf = createMock(AccumuloConfiguration.class);
    bw = createMock(BatchWriter.class);
    context = createMock(ClientContext.class);
    expect(context.getConfiguration()).andReturn(conf).anyTimes();
    expect(context.getConnector()).andReturn(conn).anyTimes();
    replay(context);
  }

  @After
  public void verifyMock() {
    verify(context, conn, conf, bw);
  }

  @Test
  public void systemTimestampsAreSetOnUpdates() throws Exception {
    final BatchWriterReplicationReplayer replayer = new BatchWriterReplicationReplayer();
    final String tableName = "foo";
    final long systemTimestamp = 1000;
    final BatchWriterConfig bwCfg = new BatchWriterConfig();
    bwCfg.setMaxMemory(1l);

    LogFileKey key = new LogFileKey();
    key.event = LogEvents.MANY_MUTATIONS;
    key.seq = 1;
    key.tid = 1;

    WalEdits edits = new WalEdits();

    // Make a mutation without timestamps
    Mutation m = new Mutation("row");
    m.put("cf", "cq1", "value");
    m.put("cf", "cq2", "value");
    m.put("cf", "cq3", "value");
    m.put("cf", "cq4", "value");
    m.put("cf", "cq5", "value");

    // Make it a TMutation
    TMutation tMutation = m.toThrift();

    // And then make a ServerMutation from the TMutation, adding in our systemTimestamp
    ServerMutation sMutation = new ServerMutation(tMutation);
    sMutation.setSystemTimestamp(systemTimestamp);

    // Serialize the ServerMutation (what AccumuloReplicaSystem will be doing)
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream out = new DataOutputStream(baos);

    key.write(out);
    out.writeInt(1);
    sMutation.write(out);

    out.close();

    // Add it to our "input" to BatchWriterReplicationReplayer
    edits.addToEdits(ByteBuffer.wrap(baos.toByteArray()));

    Mutation expectedMutation = new Mutation("row");
    expectedMutation.put("cf", "cq1", sMutation.getSystemTimestamp(), "value");
    expectedMutation.put("cf", "cq2", sMutation.getSystemTimestamp(), "value");
    expectedMutation.put("cf", "cq3", sMutation.getSystemTimestamp(), "value");
    expectedMutation.put("cf", "cq4", sMutation.getSystemTimestamp(), "value");
    expectedMutation.put("cf", "cq5", sMutation.getSystemTimestamp(), "value");

    expect(conf.getAsBytes(Property.TSERV_REPLICATION_BW_REPLAYER_MEMORY)).andReturn(bwCfg.getMaxMemory());
    expect(conn.createBatchWriter(tableName, bwCfg)).andReturn(bw);

    bw.addMutations(Lists.newArrayList(expectedMutation));
    expectLastCall().once();

    bw.close();
    expectLastCall().once();

    replay(conn, conf, bw);

    replayer.replicateLog(context, tableName, edits);
  }

  @Test
  public void replicationSourcesArePreserved() throws Exception {
    final BatchWriterReplicationReplayer replayer = new BatchWriterReplicationReplayer();
    final String tableName = "foo";
    final long systemTimestamp = 1000;
    final String peerName = "peer";
    final BatchWriterConfig bwCfg = new BatchWriterConfig();
    bwCfg.setMaxMemory(1l);

    LogFileKey key = new LogFileKey();
    key.event = LogEvents.MANY_MUTATIONS;
    key.seq = 1;
    key.tid = 1;

    WalEdits edits = new WalEdits();

    // Make a mutation without timestamps
    Mutation m = new Mutation("row");
    m.put("cf", "cq1", "value");
    m.put("cf", "cq2", "value");
    m.put("cf", "cq3", "value");
    m.put("cf", "cq4", "value");
    m.put("cf", "cq5", "value");

    // This Mutation "came" from a system called "peer"
    m.addReplicationSource(peerName);

    // Make it a TMutation
    TMutation tMutation = m.toThrift();

    // And then make a ServerMutation from the TMutation, adding in our systemTimestamp
    ServerMutation sMutation = new ServerMutation(tMutation);
    sMutation.setSystemTimestamp(systemTimestamp);

    // Serialize the ServerMutation (what AccumuloReplicaSystem will be doing)
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream out = new DataOutputStream(baos);

    key.write(out);
    out.writeInt(1);
    sMutation.write(out);

    out.close();

    // Add it to our "input" to BatchWriterReplicationReplayer
    edits.addToEdits(ByteBuffer.wrap(baos.toByteArray()));

    Mutation expectedMutation = new Mutation("row");
    expectedMutation.put("cf", "cq1", sMutation.getSystemTimestamp(), "value");
    expectedMutation.put("cf", "cq2", sMutation.getSystemTimestamp(), "value");
    expectedMutation.put("cf", "cq3", sMutation.getSystemTimestamp(), "value");
    expectedMutation.put("cf", "cq4", sMutation.getSystemTimestamp(), "value");
    expectedMutation.put("cf", "cq5", sMutation.getSystemTimestamp(), "value");

    // We expect our peer name to be preserved in the mutation that gets written
    expectedMutation.addReplicationSource(peerName);

    expect(conf.getAsBytes(Property.TSERV_REPLICATION_BW_REPLAYER_MEMORY)).andReturn(bwCfg.getMaxMemory());
    expect(conn.createBatchWriter(tableName, bwCfg)).andReturn(bw);

    bw.addMutations(Lists.newArrayList(expectedMutation));
    expectLastCall().once();

    bw.close();
    expectLastCall().once();

    replay(conn, conf, bw);

    replayer.replicateLog(context, tableName, edits);
  }
}

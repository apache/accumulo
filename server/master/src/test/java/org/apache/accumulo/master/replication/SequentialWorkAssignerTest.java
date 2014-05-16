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

import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.junit.Assert.fail;

import java.util.HashSet;

import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.protobuf.ProtobufUtil;
import org.apache.accumulo.core.replication.ReplicationSchema.OrderSection;
import org.apache.accumulo.core.replication.ReplicationSchema.WorkSection;
import org.apache.accumulo.core.replication.ReplicationTarget;
import org.apache.accumulo.core.replication.proto.Replication.Status;
import org.apache.accumulo.core.security.Credentials;
import org.apache.accumulo.core.security.TablePermission;
import org.apache.accumulo.server.replication.AbstractWorkAssigner;
import org.apache.accumulo.server.replication.ReplicationTable;
import org.apache.accumulo.server.zookeeper.DistributedWorkQueue;
import org.apache.hadoop.io.Text;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

/**
 * 
 */
public class SequentialWorkAssignerTest {

  @Rule
  public TestName test = new TestName();

  private AccumuloConfiguration conf;
  private Connector conn;
  private SequentialWorkAssigner assigner;

  @Before
  public void init() {
    conf = createMock(AccumuloConfiguration.class);
    conn = createMock(Connector.class);
    assigner = new SequentialWorkAssigner(conf, conn);
  }

  @Test
  public void test() {
    fail("Not yet implemented");
  }

//  @Test
  public void createWorkForFilesInCorrectOrder() throws Exception {
    ReplicationTarget target = new ReplicationTarget("cluster1", "table1", "1");
    Text serializedTarget = target.toText();
    String keyTarget = target.getPeerName() + AbstractWorkAssigner.KEY_SEPARATOR + target.getRemoteIdentifier()
        + AbstractWorkAssigner.KEY_SEPARATOR + target.getSourceTableId();

    MockInstance inst = new MockInstance(test.getMethodName());
    Credentials creds = new Credentials("root", new PasswordToken(""));
    Connector conn = inst.getConnector(creds.getPrincipal(), creds.getToken());

    // Set the connector
    assigner.setConnector(conn);

    // Create and grant ourselves write to the replication table
    ReplicationTable.create(conn);
    conn.securityOperations().grantTablePermission("root", ReplicationTable.NAME, TablePermission.WRITE);

    // Create two mutations, both of which need replication work done
    BatchWriter bw = ReplicationTable.getBatchWriter(conn);
    // We want the name of file2 to sort before file1
    String filename1 = "z_file1", filename2 = "a_file1";
    String file1 = "/accumulo/wal/tserver+port/" + filename1, file2 = "/accumulo/wal/tserver+port/" + filename2;

    // File1 was closed before file2, however
    Status stat1 = Status.newBuilder().setBegin(0).setEnd(100).setClosed(true).setInfiniteEnd(false).setClosedTime(250).build();
    Status stat2 = Status.newBuilder().setBegin(0).setEnd(100).setClosed(true).setInfiniteEnd(false).setClosedTime(500).build();

    Mutation m = new Mutation(file1);
    WorkSection.add(m, serializedTarget, ProtobufUtil.toValue(stat1));
    bw.addMutation(m);

    m = new Mutation(file2);
    WorkSection.add(m, serializedTarget, ProtobufUtil.toValue(stat2));
    bw.addMutation(m);

    m = OrderSection.createMutation(file1, stat1.getClosedTime());
    OrderSection.add(m, new Text(target.getSourceTableId()), ProtobufUtil.toValue(stat1));
    bw.addMutation(m);

    m = OrderSection.createMutation(file2, stat2.getClosedTime());
    OrderSection.add(m, new Text(target.getSourceTableId()), ProtobufUtil.toValue(stat2));
    bw.addMutation(m);

    bw.close();

    DistributedWorkQueue workQueue = createMock(DistributedWorkQueue.class);
    @SuppressWarnings("unchecked")
    HashSet<String> queuedWork = createMock(HashSet.class);
    assigner.setQueuedWork(queuedWork);
    assigner.setWorkQueue(workQueue);
    assigner.setMaxQueueSize(Integer.MAX_VALUE);

    expect(queuedWork.size()).andReturn(0).anyTimes();

    // Make sure we expect the invocations in the correct order (accumulo is sorted)
    expect(queuedWork.contains(filename1 + "|" + keyTarget)).andReturn(false);
    workQueue.addWork(filename1 + "|" + keyTarget, file1);
    expectLastCall().once();

    // file2 is *not* queued because file1 must be replicated first

    replay(queuedWork, workQueue);

    assigner.createWork();

    verify(queuedWork, workQueue);
  }
}

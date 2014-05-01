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

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.replication.ReplicationSchema.WorkSection;
import org.apache.accumulo.core.replication.ReplicationTarget;
import org.apache.accumulo.core.replication.StatusUtil;
import org.apache.accumulo.core.security.Credentials;
import org.apache.accumulo.core.security.TablePermission;
import org.apache.accumulo.master.Master;
import org.apache.accumulo.server.replication.ReplicationTable;
import org.apache.accumulo.server.zookeeper.DistributedWorkQueue;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

/**
 * 
 */
public class ReplicationWorkAssignerTest {

  @Rule
  public TestName test = new TestName();

  private Master master;
  private Connector conn;
  private ReplicationWorkAssigner assigner;

  @Before
  public void init() {
    master = createMock(Master.class);
    conn = createMock(Connector.class);
    assigner = new ReplicationWorkAssigner(master, conn);
  }

  @Test
  public void workQueuedUsingFileName() throws Exception {
    ReplicationTarget target = new ReplicationTarget("cluster1", "table1"); 
    Text serializedTarget = ReplicationTarget.toText(target);

    DistributedWorkQueue workQueue = createMock(DistributedWorkQueue.class);
    Set<String> queuedWork = new HashSet<>();
    assigner.setQueuedWork(queuedWork);
    assigner.setWorkQueue(workQueue);

    Path p = new Path("/accumulo/wal/tserver+port/" + UUID.randomUUID());
    assigner.queueWork(p.toString(), p.getName(), serializedTarget);
    
    workQueue.addWork(p.getName(), p.toString().getBytes(StandardCharsets.UTF_8));
    expectLastCall().once();

    replay(workQueue);

    Assert.assertEquals(1, queuedWork.size());
    Assert.assertEquals(p.getName() + "|" + serializedTarget, queuedWork.iterator().next());
  }

  @Test
  public void existingWorkIsReQueued() throws Exception {
    DistributedWorkQueue workQueue = createMock(DistributedWorkQueue.class);

    List<String> existingWork = Arrays.asList("/accumulo/wal/tserver+port/wal1", "/accumulo/wal/tserver+port/wal2");
    expect(workQueue.getWorkQueued()).andReturn(existingWork);

    replay(workQueue);

    assigner.setWorkQueue(workQueue);
    assigner.initializeQueuedWork();

    Set<String> queuedWork = assigner.getQueuedWork();
    Assert.assertEquals("Expected existing work and queued work to be the same size", existingWork.size(), queuedWork.size());
    Assert.assertTrue("Expected all existing work to be queued", queuedWork.containsAll(existingWork));
  }

  @Test
  public void createWorkForFilesNeedingIt() throws Exception {
    ReplicationTarget target1 = new ReplicationTarget("cluster1", "table1"), target2 = new ReplicationTarget("cluster1", "table2"); 
    Text serializedTarget1 = ReplicationTarget.toText(target1), serializedTarget2 = ReplicationTarget.toText(target2);

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
    String filename1 = UUID.randomUUID().toString(), filename2 = UUID.randomUUID().toString();
    String file1 = "/accumulo/wal/tserver+port/" + filename1, file2 = "/accumulo/wal/tserver+port/" + filename2;
    Mutation m = new Mutation(file1);
    WorkSection.add(m, serializedTarget1, StatusUtil.openWithUnknownLengthValue());
    bw.addMutation(m);

    m = new Mutation(file2);
    WorkSection.add(m, serializedTarget2, StatusUtil.openWithUnknownLengthValue());
    bw.addMutation(m);

    bw.close();

    DistributedWorkQueue workQueue = createMock(DistributedWorkQueue.class);
    @SuppressWarnings("unchecked")
    HashSet<String> queuedWork = createMock(HashSet.class);
    assigner.setQueuedWork(queuedWork);

    expect(queuedWork.size()).andReturn(0).anyTimes();

    // Make sure we expect the invocations in the correct order (accumulo is sorted)
    if (file1.compareTo(file2) <= 0) {
      String key = filename1 + "|" + serializedTarget1;
      workQueue.addWork(key, file1.getBytes(StandardCharsets.UTF_8));
      expectLastCall().once();
      expect(queuedWork.add(key)).andReturn(true).once();
      
      key = filename2 + "|" + serializedTarget2;
      workQueue.addWork(key, file2.getBytes(StandardCharsets.UTF_8));
      expectLastCall().once();
      expect(queuedWork.add(key)).andReturn(true).once();
    } else {
      String key = filename2 + "|" + serializedTarget2;
      workQueue.addWork(key, file2.getBytes(StandardCharsets.UTF_8));
      expectLastCall().once();
      expect(queuedWork.add(key)).andReturn(true).once();

      key = filename1 + "|" + serializedTarget1;
      workQueue.addWork(key, file1.getBytes(StandardCharsets.UTF_8));
      expectLastCall().once();
      expect(queuedWork.add(key)).andReturn(true).once();
    }

    replay(queuedWork, workQueue);

    assigner.createWork();
  }

  @Test
  public void doNotCreateWorkForFilesNotNeedingIt() throws Exception {
    ReplicationTarget target1 = new ReplicationTarget("cluster1", "table1"), target2 = new ReplicationTarget("cluster1", "table2"); 
    Text serializedTarget1 = ReplicationTarget.toText(target1), serializedTarget2 = ReplicationTarget.toText(target2);

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
    String filename1 = UUID.randomUUID().toString(), filename2 = UUID.randomUUID().toString();
    String file1 = "/accumulo/wal/tserver+port/" + filename1, file2 = "/accumulo/wal/tserver+port/" + filename2;
    Mutation m = new Mutation(file1);
    WorkSection.add(m, serializedTarget1, StatusUtil.newFileValue());
    bw.addMutation(m);

    m = new Mutation(file2);
    WorkSection.add(m, serializedTarget2, StatusUtil.newFileValue());
    bw.addMutation(m);

    bw.close();

    DistributedWorkQueue workQueue = createMock(DistributedWorkQueue.class);
    @SuppressWarnings("unchecked")
    HashSet<String> queuedWork = createMock(HashSet.class);
    assigner.setQueuedWork(queuedWork);

    expect(queuedWork.size()).andReturn(0).anyTimes();

    replay(queuedWork, workQueue);

    assigner.createWork();
  }

}

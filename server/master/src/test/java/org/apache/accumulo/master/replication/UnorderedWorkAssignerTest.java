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

import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.protobuf.ProtobufUtil;
import org.apache.accumulo.core.replication.ReplicationConstants;
import org.apache.accumulo.core.replication.ReplicationSchema.OrderSection;
import org.apache.accumulo.core.replication.ReplicationSchema.WorkSection;
import org.apache.accumulo.core.replication.ReplicationTable;
import org.apache.accumulo.core.replication.ReplicationTarget;
import org.apache.accumulo.core.security.TablePermission;
import org.apache.accumulo.server.replication.DistributedWorkQueueWorkAssignerHelper;
import org.apache.accumulo.server.replication.StatusUtil;
import org.apache.accumulo.server.replication.proto.Replication.Status;
import org.apache.accumulo.server.zookeeper.DistributedWorkQueue;
import org.apache.accumulo.server.zookeeper.ZooCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

public class UnorderedWorkAssignerTest {

  @Rule
  public TestName test = new TestName();

  private AccumuloConfiguration conf;
  private Connector conn;
  private Connector mockConn;
  private UnorderedWorkAssigner assigner;

  @Before
  public void init() throws Exception {
    conf = createMock(AccumuloConfiguration.class);
    conn = createMock(Connector.class);
    assigner = new UnorderedWorkAssigner(conf, conn);

    Instance inst = new org.apache.accumulo.core.client.mock.MockInstance(test.getMethodName());
    mockConn = inst.getConnector("root", new PasswordToken(""));
  }

  @Test
  public void workQueuedUsingFileName() throws Exception {
    ReplicationTarget target = new ReplicationTarget("cluster1", "table1", "1");

    DistributedWorkQueue workQueue = createMock(DistributedWorkQueue.class);
    Set<String> queuedWork = new HashSet<>();
    assigner.setQueuedWork(queuedWork);
    assigner.setWorkQueue(workQueue);

    Path p = new Path("/accumulo/wal/tserver+port/" + UUID.randomUUID());

    String expectedQueueKey = p.getName() + DistributedWorkQueueWorkAssignerHelper.KEY_SEPARATOR + target.getPeerName()
        + DistributedWorkQueueWorkAssignerHelper.KEY_SEPARATOR + target.getRemoteIdentifier() + DistributedWorkQueueWorkAssignerHelper.KEY_SEPARATOR
        + target.getSourceTableId();

    workQueue.addWork(expectedQueueKey, p.toString());
    expectLastCall().once();

    replay(workQueue);

    assigner.queueWork(p, target);

    Assert.assertEquals(1, queuedWork.size());
    Assert.assertEquals(expectedQueueKey, queuedWork.iterator().next());
  }

  @Test
  public void existingWorkIsReQueued() throws Exception {
    DistributedWorkQueue workQueue = createMock(DistributedWorkQueue.class);

    List<String> existingWork = Arrays.asList("/accumulo/wal/tserver+port/wal1", "/accumulo/wal/tserver+port/wal2");
    expect(workQueue.getWorkQueued()).andReturn(existingWork);

    replay(workQueue);

    assigner.setWorkQueue(workQueue);
    assigner.initializeQueuedWork();

    verify(workQueue);

    Set<String> queuedWork = assigner.getQueuedWork();
    Assert.assertEquals("Expected existing work and queued work to be the same size", existingWork.size(), queuedWork.size());
    Assert.assertTrue("Expected all existing work to be queued", queuedWork.containsAll(existingWork));
  }

  @Test
  public void createWorkForFilesNeedingIt() throws Exception {
    ReplicationTarget target1 = new ReplicationTarget("cluster1", "table1", "1"), target2 = new ReplicationTarget("cluster1", "table2", "2");
    Text serializedTarget1 = target1.toText(), serializedTarget2 = target2.toText();
    String keyTarget1 = target1.getPeerName() + DistributedWorkQueueWorkAssignerHelper.KEY_SEPARATOR + target1.getRemoteIdentifier()
        + DistributedWorkQueueWorkAssignerHelper.KEY_SEPARATOR + target1.getSourceTableId(), keyTarget2 = target2.getPeerName()
        + DistributedWorkQueueWorkAssignerHelper.KEY_SEPARATOR + target2.getRemoteIdentifier() + DistributedWorkQueueWorkAssignerHelper.KEY_SEPARATOR
        + target2.getSourceTableId();

    // Set the connector
    assigner.setConnector(mockConn);

    // grant ourselves write to the replication table
    mockConn.securityOperations().grantTablePermission("root", ReplicationTable.NAME, TablePermission.WRITE);

    Status.Builder builder = Status.newBuilder().setBegin(0).setEnd(0).setInfiniteEnd(true).setClosed(false).setCreatedTime(5l);
    Status status1 = builder.build();
    builder.setCreatedTime(10l);
    Status status2 = builder.build();

    // Create two mutations, both of which need replication work done
    BatchWriter bw = ReplicationTable.getBatchWriter(mockConn);
    String filename1 = UUID.randomUUID().toString(), filename2 = UUID.randomUUID().toString();
    String file1 = "/accumulo/wal/tserver+port/" + filename1, file2 = "/accumulo/wal/tserver+port/" + filename2;
    Mutation m = new Mutation(file1);
    WorkSection.add(m, serializedTarget1, ProtobufUtil.toValue(status1));
    bw.addMutation(m);
    m = OrderSection.createMutation(file1, status1.getCreatedTime());
    OrderSection.add(m, new Text(target1.getSourceTableId()), ProtobufUtil.toValue(status1));
    bw.addMutation(m);

    m = new Mutation(file2);
    WorkSection.add(m, serializedTarget2, ProtobufUtil.toValue(status2));
    bw.addMutation(m);
    m = OrderSection.createMutation(file2, status2.getCreatedTime());
    OrderSection.add(m, new Text(target2.getSourceTableId()), ProtobufUtil.toValue(status2));
    bw.addMutation(m);

    bw.close();

    DistributedWorkQueue workQueue = createMock(DistributedWorkQueue.class);
    HashSet<String> queuedWork = new HashSet<>();
    assigner.setQueuedWork(queuedWork);
    assigner.setWorkQueue(workQueue);
    assigner.setMaxQueueSize(Integer.MAX_VALUE);

    // Make sure we expect the invocations in the order they were created
    String key = filename1 + "|" + keyTarget1;
    workQueue.addWork(key, file1);
    expectLastCall().once();

    key = filename2 + "|" + keyTarget2;
    workQueue.addWork(key, file2);
    expectLastCall().once();

    replay(workQueue);

    assigner.createWork();

    verify(workQueue);
  }

  @Test
  public void doNotCreateWorkForFilesNotNeedingIt() throws Exception {
    ReplicationTarget target1 = new ReplicationTarget("cluster1", "table1", "1"), target2 = new ReplicationTarget("cluster1", "table2", "2");
    Text serializedTarget1 = target1.toText(), serializedTarget2 = target2.toText();

    // Set the connector
    assigner.setConnector(mockConn);

    // grant ourselves write to the replication table
    mockConn.securityOperations().grantTablePermission("root", ReplicationTable.NAME, TablePermission.WRITE);

    // Create two mutations, both of which need replication work done
    BatchWriter bw = ReplicationTable.getBatchWriter(mockConn);
    String filename1 = UUID.randomUUID().toString(), filename2 = UUID.randomUUID().toString();
    String file1 = "/accumulo/wal/tserver+port/" + filename1, file2 = "/accumulo/wal/tserver+port/" + filename2;

    Mutation m = new Mutation(file1);
    WorkSection.add(m, serializedTarget1, StatusUtil.fileCreatedValue(5));
    bw.addMutation(m);

    m = new Mutation(file2);
    WorkSection.add(m, serializedTarget2, StatusUtil.fileCreatedValue(10));
    bw.addMutation(m);

    bw.close();

    DistributedWorkQueue workQueue = createMock(DistributedWorkQueue.class);
    HashSet<String> queuedWork = new HashSet<>();
    assigner.setQueuedWork(queuedWork);
    assigner.setMaxQueueSize(Integer.MAX_VALUE);

    replay(workQueue);

    assigner.createWork();

    verify(workQueue);
  }

  @Test
  public void workNotInZooKeeperIsCleanedUp() {
    Set<String> queuedWork = new LinkedHashSet<>(Arrays.asList("wal1", "wal2"));
    assigner.setQueuedWork(queuedWork);

    Instance inst = createMock(Instance.class);
    ZooCache cache = createMock(ZooCache.class);
    assigner.setZooCache(cache);

    expect(conn.getInstance()).andReturn(inst);
    expect(inst.getInstanceID()).andReturn("id");
    expect(cache.get(Constants.ZROOT + "/id" + ReplicationConstants.ZOO_WORK_QUEUE + "/wal1")).andReturn(null);
    expect(cache.get(Constants.ZROOT + "/id" + ReplicationConstants.ZOO_WORK_QUEUE + "/wal2")).andReturn(null);

    replay(cache, inst, conn);

    assigner.cleanupFinishedWork();

    verify(cache, inst, conn);
    Assert.assertTrue("Queued work was not emptied", queuedWork.isEmpty());
  }

  @Test
  public void workNotReAdded() throws Exception {
    Set<String> queuedWork = new HashSet<>();

    assigner.setQueuedWork(queuedWork);

    ReplicationTarget target = new ReplicationTarget("cluster1", "table1", "1");
    String serializedTarget = target.getPeerName() + DistributedWorkQueueWorkAssignerHelper.KEY_SEPARATOR + target.getRemoteIdentifier()
        + DistributedWorkQueueWorkAssignerHelper.KEY_SEPARATOR + target.getSourceTableId();

    queuedWork.add("wal1|" + serializedTarget.toString());

    // Set the connector
    assigner.setConnector(mockConn);

    // grant ourselves write to the replication table
    mockConn.securityOperations().grantTablePermission("root", ReplicationTable.NAME, TablePermission.WRITE);

    // Create two mutations, both of which need replication work done
    BatchWriter bw = ReplicationTable.getBatchWriter(mockConn);
    String file1 = "/accumulo/wal/tserver+port/wal1";
    Mutation m = new Mutation(file1);
    WorkSection.add(m, target.toText(), StatusUtil.openWithUnknownLengthValue());
    bw.addMutation(m);

    bw.close();

    DistributedWorkQueue workQueue = createMock(DistributedWorkQueue.class);
    assigner.setWorkQueue(workQueue);
    assigner.setMaxQueueSize(Integer.MAX_VALUE);

    replay(workQueue);

    assigner.createWork();

    verify(workQueue);
  }
}

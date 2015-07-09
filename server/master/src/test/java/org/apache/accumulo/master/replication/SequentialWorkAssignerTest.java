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

import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

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
import org.apache.accumulo.core.zookeeper.ZooUtil;
import org.apache.accumulo.server.replication.DistributedWorkQueueWorkAssignerHelper;
import org.apache.accumulo.server.replication.proto.Replication.Status;
import org.apache.accumulo.server.zookeeper.DistributedWorkQueue;
import org.apache.accumulo.server.zookeeper.ZooCache;
import org.apache.hadoop.io.Text;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

public class SequentialWorkAssignerTest {

  @Rule
  public TestName test = new TestName();

  private AccumuloConfiguration conf;
  private Connector conn;
  private Connector mockConn;
  private SequentialWorkAssigner assigner;

  @Before
  public void init() throws Exception {
    conf = createMock(AccumuloConfiguration.class);
    conn = createMock(Connector.class);
    assigner = new SequentialWorkAssigner(conf, conn);

    Instance inst = new org.apache.accumulo.core.client.mock.MockInstance(test.getMethodName());
    mockConn = inst.getConnector("root", new PasswordToken(""));
    // Set the connector
    assigner.setConnector(mockConn);
    // grant ourselves write to the replication table
    mockConn.securityOperations().grantTablePermission("root", ReplicationTable.NAME, TablePermission.WRITE);
  }

  @Test
  public void createWorkForFilesInCorrectOrder() throws Exception {
    ReplicationTarget target = new ReplicationTarget("cluster1", "table1", "1");
    Text serializedTarget = target.toText();

    // Create two mutations, both of which need replication work done
    BatchWriter bw = ReplicationTable.getBatchWriter(mockConn);
    // We want the name of file2 to sort before file1
    String filename1 = "z_file1", filename2 = "a_file1";
    String file1 = "/accumulo/wal/tserver+port/" + filename1, file2 = "/accumulo/wal/tserver+port/" + filename2;

    // File1 was closed before file2, however
    Status stat1 = Status.newBuilder().setBegin(0).setEnd(100).setClosed(true).setInfiniteEnd(false).setCreatedTime(250).build();
    Status stat2 = Status.newBuilder().setBegin(0).setEnd(100).setClosed(true).setInfiniteEnd(false).setCreatedTime(500).build();

    Mutation m = new Mutation(file1);
    WorkSection.add(m, serializedTarget, ProtobufUtil.toValue(stat1));
    bw.addMutation(m);

    m = new Mutation(file2);
    WorkSection.add(m, serializedTarget, ProtobufUtil.toValue(stat2));
    bw.addMutation(m);

    m = OrderSection.createMutation(file1, stat1.getCreatedTime());
    OrderSection.add(m, new Text(target.getSourceTableId()), ProtobufUtil.toValue(stat1));
    bw.addMutation(m);

    m = OrderSection.createMutation(file2, stat2.getCreatedTime());
    OrderSection.add(m, new Text(target.getSourceTableId()), ProtobufUtil.toValue(stat2));
    bw.addMutation(m);

    bw.close();

    DistributedWorkQueue workQueue = createMock(DistributedWorkQueue.class);
    Map<String,Map<String,String>> queuedWork = new HashMap<>();
    assigner.setQueuedWork(queuedWork);
    assigner.setWorkQueue(workQueue);
    assigner.setMaxQueueSize(Integer.MAX_VALUE);

    // Make sure we expect the invocations in the correct order (accumulo is sorted)
    workQueue.addWork(DistributedWorkQueueWorkAssignerHelper.getQueueKey(filename1, target), file1);
    expectLastCall().once();

    // file2 is *not* queued because file1 must be replicated first

    replay(workQueue);

    assigner.createWork();

    verify(workQueue);

    Assert.assertEquals(1, queuedWork.size());
    Assert.assertTrue(queuedWork.containsKey("cluster1"));
    Map<String,String> cluster1Work = queuedWork.get("cluster1");
    Assert.assertEquals(1, cluster1Work.size());
    Assert.assertTrue(cluster1Work.containsKey(target.getSourceTableId()));
    Assert.assertEquals(DistributedWorkQueueWorkAssignerHelper.getQueueKey(filename1, target), cluster1Work.get(target.getSourceTableId()));
  }

  @Test
  public void workAcrossTablesHappensConcurrently() throws Exception {
    ReplicationTarget target1 = new ReplicationTarget("cluster1", "table1", "1");
    Text serializedTarget1 = target1.toText();

    ReplicationTarget target2 = new ReplicationTarget("cluster1", "table2", "2");
    Text serializedTarget2 = target2.toText();

    // Create two mutations, both of which need replication work done
    BatchWriter bw = ReplicationTable.getBatchWriter(mockConn);
    // We want the name of file2 to sort before file1
    String filename1 = "z_file1", filename2 = "a_file1";
    String file1 = "/accumulo/wal/tserver+port/" + filename1, file2 = "/accumulo/wal/tserver+port/" + filename2;

    // File1 was closed before file2, however
    Status stat1 = Status.newBuilder().setBegin(0).setEnd(100).setClosed(true).setInfiniteEnd(false).setCreatedTime(250).build();
    Status stat2 = Status.newBuilder().setBegin(0).setEnd(100).setClosed(true).setInfiniteEnd(false).setCreatedTime(500).build();

    Mutation m = new Mutation(file1);
    WorkSection.add(m, serializedTarget1, ProtobufUtil.toValue(stat1));
    bw.addMutation(m);

    m = new Mutation(file2);
    WorkSection.add(m, serializedTarget2, ProtobufUtil.toValue(stat2));
    bw.addMutation(m);

    m = OrderSection.createMutation(file1, stat1.getCreatedTime());
    OrderSection.add(m, new Text(target1.getSourceTableId()), ProtobufUtil.toValue(stat1));
    bw.addMutation(m);

    m = OrderSection.createMutation(file2, stat2.getCreatedTime());
    OrderSection.add(m, new Text(target2.getSourceTableId()), ProtobufUtil.toValue(stat2));
    bw.addMutation(m);

    bw.close();

    DistributedWorkQueue workQueue = createMock(DistributedWorkQueue.class);
    Map<String,Map<String,String>> queuedWork = new HashMap<>();
    assigner.setQueuedWork(queuedWork);
    assigner.setWorkQueue(workQueue);
    assigner.setMaxQueueSize(Integer.MAX_VALUE);

    // Make sure we expect the invocations in the correct order (accumulo is sorted)
    workQueue.addWork(DistributedWorkQueueWorkAssignerHelper.getQueueKey(filename1, target1), file1);
    expectLastCall().once();

    workQueue.addWork(DistributedWorkQueueWorkAssignerHelper.getQueueKey(filename2, target2), file2);
    expectLastCall().once();

    // file2 is *not* queued because file1 must be replicated first

    replay(workQueue);

    assigner.createWork();

    verify(workQueue);

    Assert.assertEquals(1, queuedWork.size());
    Assert.assertTrue(queuedWork.containsKey("cluster1"));

    Map<String,String> cluster1Work = queuedWork.get("cluster1");
    Assert.assertEquals(2, cluster1Work.size());
    Assert.assertTrue(cluster1Work.containsKey(target1.getSourceTableId()));
    Assert.assertEquals(DistributedWorkQueueWorkAssignerHelper.getQueueKey(filename1, target1), cluster1Work.get(target1.getSourceTableId()));

    Assert.assertTrue(cluster1Work.containsKey(target2.getSourceTableId()));
    Assert.assertEquals(DistributedWorkQueueWorkAssignerHelper.getQueueKey(filename2, target2), cluster1Work.get(target2.getSourceTableId()));
  }

  @Test
  public void workAcrossPeersHappensConcurrently() throws Exception {
    ReplicationTarget target1 = new ReplicationTarget("cluster1", "table1", "1");
    Text serializedTarget1 = target1.toText();

    ReplicationTarget target2 = new ReplicationTarget("cluster2", "table1", "1");
    Text serializedTarget2 = target2.toText();

    // Create two mutations, both of which need replication work done
    BatchWriter bw = ReplicationTable.getBatchWriter(mockConn);
    // We want the name of file2 to sort before file1
    String filename1 = "z_file1", filename2 = "a_file1";
    String file1 = "/accumulo/wal/tserver+port/" + filename1, file2 = "/accumulo/wal/tserver+port/" + filename2;

    // File1 was closed before file2, however
    Status stat1 = Status.newBuilder().setBegin(0).setEnd(100).setClosed(true).setInfiniteEnd(false).setCreatedTime(250).build();
    Status stat2 = Status.newBuilder().setBegin(0).setEnd(100).setClosed(true).setInfiniteEnd(false).setCreatedTime(500).build();

    Mutation m = new Mutation(file1);
    WorkSection.add(m, serializedTarget1, ProtobufUtil.toValue(stat1));
    bw.addMutation(m);

    m = new Mutation(file2);
    WorkSection.add(m, serializedTarget2, ProtobufUtil.toValue(stat2));
    bw.addMutation(m);

    m = OrderSection.createMutation(file1, stat1.getCreatedTime());
    OrderSection.add(m, new Text(target1.getSourceTableId()), ProtobufUtil.toValue(stat1));
    bw.addMutation(m);

    m = OrderSection.createMutation(file2, stat2.getCreatedTime());
    OrderSection.add(m, new Text(target2.getSourceTableId()), ProtobufUtil.toValue(stat2));
    bw.addMutation(m);

    bw.close();

    DistributedWorkQueue workQueue = createMock(DistributedWorkQueue.class);
    Map<String,Map<String,String>> queuedWork = new HashMap<>();
    assigner.setQueuedWork(queuedWork);
    assigner.setWorkQueue(workQueue);
    assigner.setMaxQueueSize(Integer.MAX_VALUE);

    // Make sure we expect the invocations in the correct order (accumulo is sorted)
    workQueue.addWork(DistributedWorkQueueWorkAssignerHelper.getQueueKey(filename1, target1), file1);
    expectLastCall().once();

    workQueue.addWork(DistributedWorkQueueWorkAssignerHelper.getQueueKey(filename2, target2), file2);
    expectLastCall().once();

    // file2 is *not* queued because file1 must be replicated first

    replay(workQueue);

    assigner.createWork();

    verify(workQueue);

    Assert.assertEquals(2, queuedWork.size());
    Assert.assertTrue(queuedWork.containsKey("cluster1"));

    Map<String,String> cluster1Work = queuedWork.get("cluster1");
    Assert.assertEquals(1, cluster1Work.size());
    Assert.assertTrue(cluster1Work.containsKey(target1.getSourceTableId()));
    Assert.assertEquals(DistributedWorkQueueWorkAssignerHelper.getQueueKey(filename1, target1), cluster1Work.get(target1.getSourceTableId()));

    Map<String,String> cluster2Work = queuedWork.get("cluster2");
    Assert.assertEquals(1, cluster2Work.size());
    Assert.assertTrue(cluster2Work.containsKey(target2.getSourceTableId()));
    Assert.assertEquals(DistributedWorkQueueWorkAssignerHelper.getQueueKey(filename2, target2), cluster2Work.get(target2.getSourceTableId()));
  }

  @Test
  public void basicZooKeeperCleanup() throws Exception {
    DistributedWorkQueue workQueue = createMock(DistributedWorkQueue.class);
    ZooCache zooCache = createMock(ZooCache.class);
    Instance inst = createMock(Instance.class);

    Map<String,Map<String,String>> queuedWork = new TreeMap<>();
    Map<String,String> cluster1Work = new TreeMap<>();

    // Two files for cluster1, one for table '1' and another for table '2' we havce assigned work for
    cluster1Work.put("1", DistributedWorkQueueWorkAssignerHelper.getQueueKey("file1", new ReplicationTarget("cluster1", "1", "1")));
    cluster1Work.put("2", DistributedWorkQueueWorkAssignerHelper.getQueueKey("file2", new ReplicationTarget("cluster1", "2", "2")));

    queuedWork.put("cluster1", cluster1Work);

    assigner.setConnector(conn);
    assigner.setZooCache(zooCache);
    assigner.setWorkQueue(workQueue);
    assigner.setQueuedWork(queuedWork);

    expect(conn.getInstance()).andReturn(inst);
    expect(inst.getInstanceID()).andReturn("instance");

    // file1 replicated
    expect(
        zooCache.get(ZooUtil.getRoot("instance") + ReplicationConstants.ZOO_WORK_QUEUE + "/"
            + DistributedWorkQueueWorkAssignerHelper.getQueueKey("file1", new ReplicationTarget("cluster1", "1", "1")))).andReturn(null);
    // file2 still needs to replicate
    expect(
        zooCache.get(ZooUtil.getRoot("instance") + ReplicationConstants.ZOO_WORK_QUEUE + "/"
            + DistributedWorkQueueWorkAssignerHelper.getQueueKey("file2", new ReplicationTarget("cluster1", "2", "2")))).andReturn(new byte[0]);

    replay(workQueue, zooCache, conn, inst);

    assigner.cleanupFinishedWork();

    verify(workQueue, zooCache, conn, inst);

    Assert.assertEquals(1, cluster1Work.size());
    Assert.assertEquals(DistributedWorkQueueWorkAssignerHelper.getQueueKey("file2", new ReplicationTarget("cluster1", "2", "2")), cluster1Work.get("2"));
  }

  @Test
  public void reprocessingOfCompletedWorkRemovesWork() throws Exception {
    ReplicationTarget target = new ReplicationTarget("cluster1", "table1", "1");
    Text serializedTarget = target.toText();

    // Create two mutations, both of which need replication work done
    BatchWriter bw = ReplicationTable.getBatchWriter(mockConn);
    // We want the name of file2 to sort before file1
    String filename1 = "z_file1", filename2 = "a_file1";
    String file1 = "/accumulo/wal/tserver+port/" + filename1, file2 = "/accumulo/wal/tserver+port/" + filename2;

    // File1 was closed before file2, however
    Status stat1 = Status.newBuilder().setBegin(100).setEnd(100).setClosed(true).setInfiniteEnd(false).setCreatedTime(250).build();
    Status stat2 = Status.newBuilder().setBegin(0).setEnd(100).setClosed(true).setInfiniteEnd(false).setCreatedTime(500).build();

    Mutation m = new Mutation(file1);
    WorkSection.add(m, serializedTarget, ProtobufUtil.toValue(stat1));
    bw.addMutation(m);

    m = new Mutation(file2);
    WorkSection.add(m, serializedTarget, ProtobufUtil.toValue(stat2));
    bw.addMutation(m);

    m = OrderSection.createMutation(file1, stat1.getCreatedTime());
    OrderSection.add(m, new Text(target.getSourceTableId()), ProtobufUtil.toValue(stat1));
    bw.addMutation(m);

    m = OrderSection.createMutation(file2, stat2.getCreatedTime());
    OrderSection.add(m, new Text(target.getSourceTableId()), ProtobufUtil.toValue(stat2));
    bw.addMutation(m);

    bw.close();

    DistributedWorkQueue workQueue = createMock(DistributedWorkQueue.class);

    // Treat filename1 as we have already submitted it for replication
    Map<String,Map<String,String>> queuedWork = new HashMap<>();
    Map<String,String> queuedWorkForCluster = new HashMap<>();
    queuedWorkForCluster.put(target.getSourceTableId(), DistributedWorkQueueWorkAssignerHelper.getQueueKey(filename1, target));
    queuedWork.put("cluster1", queuedWorkForCluster);

    assigner.setQueuedWork(queuedWork);
    assigner.setWorkQueue(workQueue);
    assigner.setMaxQueueSize(Integer.MAX_VALUE);

    // Make sure we expect the invocations in the correct order (accumulo is sorted)
    workQueue.addWork(DistributedWorkQueueWorkAssignerHelper.getQueueKey(filename2, target), file2);
    expectLastCall().once();

    // file2 is queued because we remove file1 because it's fully replicated

    replay(workQueue);

    assigner.createWork();

    verify(workQueue);

    Assert.assertEquals(1, queuedWork.size());
    Assert.assertTrue(queuedWork.containsKey("cluster1"));
    Map<String,String> cluster1Work = queuedWork.get("cluster1");
    Assert.assertEquals(1, cluster1Work.size());
    Assert.assertTrue(cluster1Work.containsKey(target.getSourceTableId()));
    Assert.assertEquals(DistributedWorkQueueWorkAssignerHelper.getQueueKey(filename2, target), cluster1Work.get(target.getSourceTableId()));
  }
}

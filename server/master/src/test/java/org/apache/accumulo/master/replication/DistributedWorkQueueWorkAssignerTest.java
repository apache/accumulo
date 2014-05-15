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
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.replication.ReplicationSchema.WorkSection;
import org.apache.accumulo.core.replication.ReplicationTarget;
import org.apache.accumulo.core.replication.StatusUtil;
import org.apache.accumulo.core.security.Credentials;
import org.apache.accumulo.core.security.TablePermission;
import org.apache.accumulo.server.replication.ReplicationTable;
import org.apache.accumulo.server.replication.ReplicationWorkAssignerHelper;
import org.apache.accumulo.server.zookeeper.DistributedWorkQueue;
import org.apache.accumulo.server.zookeeper.ZooCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

public class DistributedWorkQueueWorkAssignerTest {

  @Rule
  public TestName test = new TestName();

  private AccumuloConfiguration conf;
  private Connector conn;
  private DistributedWorkQueueWorkAssigner assigner;

  @Before
  public void init() {
    conf = createMock(AccumuloConfiguration.class);
    conn = createMock(Connector.class);
    assigner = new DistributedWorkQueueWorkAssigner(conf, conn);
  }

  @Test
  public void workQueuedUsingFileName() throws Exception {
    ReplicationTarget target = new ReplicationTarget("cluster1", "table1", "1");
    Text serializedTarget = target.toText();

    DistributedWorkQueue workQueue = createMock(DistributedWorkQueue.class);
    Set<String> queuedWork = new HashSet<>();
    assigner.setQueuedWork(queuedWork);
    assigner.setWorkQueue(workQueue);

    Path p = new Path("/accumulo/wal/tserver+port/" + UUID.randomUUID());

    workQueue.addWork(p.getName() + "|" + serializedTarget.toString(), p.toString());
    expectLastCall().once();

    replay(workQueue);

    assigner.queueWork(p.getName() + "|" + serializedTarget, p.toString());

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

    verify(workQueue);

    Set<String> queuedWork = assigner.getQueuedWork();
    Assert.assertEquals("Expected existing work and queued work to be the same size", existingWork.size(), queuedWork.size());
    Assert.assertTrue("Expected all existing work to be queued", queuedWork.containsAll(existingWork));
  }

  @Test
  public void createWorkForFilesNeedingIt() throws Exception {
    ReplicationTarget target1 = new ReplicationTarget("cluster1", "table1", "1"), target2 = new ReplicationTarget("cluster1", "table2", "2");
    Text serializedTarget1 = target1.toText(), serializedTarget2 = target2.toText();
    String keyTarget1 = target1.getPeerName() + ReplicationWorkAssignerHelper.KEY_SEPARATOR + target1.getRemoteIdentifier()
        + ReplicationWorkAssignerHelper.KEY_SEPARATOR + target1.getSourceTableId(), keyTarget2 = target2.getPeerName()
        + ReplicationWorkAssignerHelper.KEY_SEPARATOR + target2.getRemoteIdentifier() + ReplicationWorkAssignerHelper.KEY_SEPARATOR
        + target2.getSourceTableId();

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
    assigner.setWorkQueue(workQueue);
    assigner.setMaxQueueSize(Integer.MAX_VALUE);

    expect(queuedWork.size()).andReturn(0).anyTimes();

    // Make sure we expect the invocations in the correct order (accumulo is sorted)
    if (file1.compareTo(file2) <= 0) {
      String key = filename1 + "|" + keyTarget1;
      expect(queuedWork.contains(key)).andReturn(false);
      workQueue.addWork(key, file1);
      expectLastCall().once();
      expect(queuedWork.add(key)).andReturn(true).once();

      key = filename2 + "|" + keyTarget2;
      expect(queuedWork.contains(key)).andReturn(false);
      workQueue.addWork(key, file2);
      expectLastCall().once();
      expect(queuedWork.add(key)).andReturn(true).once();
    } else {
      String key = filename2 + "|" + keyTarget2;
      expect(queuedWork.contains(key)).andReturn(false);
      workQueue.addWork(key, file2);
      expectLastCall().once();
      expect(queuedWork.add(key)).andReturn(true).once();

      key = filename1 + "|" + keyTarget1;
      expect(queuedWork.contains(key)).andReturn(false);
      workQueue.addWork(key, file1);
      expectLastCall().once();
      expect(queuedWork.add(key)).andReturn(true).once();
    }

    replay(queuedWork, workQueue);

    assigner.createWork();

    verify(queuedWork, workQueue);
  }

  @Test
  public void doNotCreateWorkForFilesNotNeedingIt() throws Exception {
    ReplicationTarget target1 = new ReplicationTarget("cluster1", "table1", "1"), target2 = new ReplicationTarget("cluster1", "table2", "2");
    Text serializedTarget1 = target1.toText(), serializedTarget2 = target2.toText();

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
    assigner.setMaxQueueSize(Integer.MAX_VALUE);

    expect(queuedWork.size()).andReturn(0).times(2);

    replay(queuedWork, workQueue);

    assigner.createWork();

    verify(queuedWork, workQueue);
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
    expect(cache.get(Constants.ZROOT + "/id" + Constants.ZREPLICATION_WORK_QUEUE + "/wal1")).andReturn(null);
    expect(cache.get(Constants.ZROOT + "/id" + Constants.ZREPLICATION_WORK_QUEUE + "/wal2")).andReturn(null);

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
    String serializedTarget = target.getPeerName() + ReplicationWorkAssignerHelper.KEY_SEPARATOR + target.getRemoteIdentifier()
        + ReplicationWorkAssignerHelper.KEY_SEPARATOR + target.getSourceTableId();

    queuedWork.add("wal1|" + serializedTarget.toString());

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

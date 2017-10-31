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
package org.apache.accumulo.test.replication;

import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;

import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.impl.Table;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.protobuf.ProtobufUtil;
import org.apache.accumulo.core.replication.ReplicationSchema.OrderSection;
import org.apache.accumulo.core.replication.ReplicationSchema.WorkSection;
import org.apache.accumulo.core.replication.ReplicationTable;
import org.apache.accumulo.core.replication.ReplicationTarget;
import org.apache.accumulo.core.security.TablePermission;
import org.apache.accumulo.master.replication.UnorderedWorkAssigner;
import org.apache.accumulo.server.replication.DistributedWorkQueueWorkAssignerHelper;
import org.apache.accumulo.server.replication.StatusUtil;
import org.apache.accumulo.server.replication.proto.Replication.Status;
import org.apache.accumulo.server.zookeeper.DistributedWorkQueue;
import org.apache.accumulo.server.zookeeper.ZooCache;
import org.apache.accumulo.test.functional.ConfigurableMacBase;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.junit.Before;
import org.junit.Test;

public class UnorderedWorkAssignerIT extends ConfigurableMacBase {

  private Connector conn;
  private MockUnorderedWorkAssigner assigner;

  private static class MockUnorderedWorkAssigner extends UnorderedWorkAssigner {
    public MockUnorderedWorkAssigner(Connector conn) {
      super(null, conn);
    }

    @Override
    protected void setQueuedWork(Set<String> queuedWork) {
      super.setQueuedWork(queuedWork);
    }

    @Override
    protected void setWorkQueue(DistributedWorkQueue workQueue) {
      super.setWorkQueue(workQueue);
    }

    @Override
    protected boolean queueWork(Path path, ReplicationTarget target) {
      return super.queueWork(path, target);
    }

    @Override
    protected void initializeQueuedWork() {
      super.initializeQueuedWork();
    }

    @Override
    protected Set<String> getQueuedWork() {
      return super.getQueuedWork();
    }

    @Override
    protected void setConnector(Connector conn) {
      super.setConnector(conn);
    }

    @Override
    protected void setMaxQueueSize(int maxQueueSize) {
      super.setMaxQueueSize(maxQueueSize);
    }

    @Override
    protected void createWork() {
      super.createWork();
    }

    @Override
    protected void setZooCache(ZooCache zooCache) {
      super.setZooCache(zooCache);
    }

    @Override
    protected void cleanupFinishedWork() {
      super.cleanupFinishedWork();
    }
  }

  @Before
  public void init() throws Exception {
    conn = getConnector();
    assigner = new MockUnorderedWorkAssigner(conn);
    ReplicationTable.setOnline(conn);
    conn.securityOperations().grantTablePermission(conn.whoami(), ReplicationTable.NAME, TablePermission.WRITE);
    conn.securityOperations().grantTablePermission(conn.whoami(), ReplicationTable.NAME, TablePermission.READ);
  }

  @Test
  public void createWorkForFilesNeedingIt() throws Exception {
    ReplicationTarget target1 = new ReplicationTarget("cluster1", "table1", Table.ID.of("1")), target2 = new ReplicationTarget("cluster1", "table2",
        Table.ID.of("2"));
    Text serializedTarget1 = target1.toText(), serializedTarget2 = target2.toText();
    String keyTarget1 = target1.getPeerName() + DistributedWorkQueueWorkAssignerHelper.KEY_SEPARATOR + target1.getRemoteIdentifier()
        + DistributedWorkQueueWorkAssignerHelper.KEY_SEPARATOR + target1.getSourceTableId(), keyTarget2 = target2.getPeerName()
        + DistributedWorkQueueWorkAssignerHelper.KEY_SEPARATOR + target2.getRemoteIdentifier() + DistributedWorkQueueWorkAssignerHelper.KEY_SEPARATOR
        + target2.getSourceTableId();

    Status.Builder builder = Status.newBuilder().setBegin(0).setEnd(0).setInfiniteEnd(true).setClosed(false).setCreatedTime(5l);
    Status status1 = builder.build();
    builder.setCreatedTime(10l);
    Status status2 = builder.build();

    // Create two mutations, both of which need replication work done
    BatchWriter bw = ReplicationTable.getBatchWriter(conn);
    String filename1 = UUID.randomUUID().toString(), filename2 = UUID.randomUUID().toString();
    String file1 = "/accumulo/wal/tserver+port/" + filename1, file2 = "/accumulo/wal/tserver+port/" + filename2;
    Mutation m = new Mutation(file1);
    WorkSection.add(m, serializedTarget1, ProtobufUtil.toValue(status1));
    bw.addMutation(m);
    m = OrderSection.createMutation(file1, status1.getCreatedTime());
    OrderSection.add(m, target1.getSourceTableId(), ProtobufUtil.toValue(status1));
    bw.addMutation(m);

    m = new Mutation(file2);
    WorkSection.add(m, serializedTarget2, ProtobufUtil.toValue(status2));
    bw.addMutation(m);
    m = OrderSection.createMutation(file2, status2.getCreatedTime());
    OrderSection.add(m, target2.getSourceTableId(), ProtobufUtil.toValue(status2));
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
    ReplicationTarget target1 = new ReplicationTarget("cluster1", "table1", Table.ID.of("1")), target2 = new ReplicationTarget("cluster1", "table2",
        Table.ID.of("2"));
    Text serializedTarget1 = target1.toText(), serializedTarget2 = target2.toText();

    // Create two mutations, both of which need replication work done
    BatchWriter bw = ReplicationTable.getBatchWriter(conn);
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
  public void workNotReAdded() throws Exception {
    Set<String> queuedWork = new HashSet<>();

    assigner.setQueuedWork(queuedWork);

    ReplicationTarget target = new ReplicationTarget("cluster1", "table1", Table.ID.of("1"));
    String serializedTarget = target.getPeerName() + DistributedWorkQueueWorkAssignerHelper.KEY_SEPARATOR + target.getRemoteIdentifier()
        + DistributedWorkQueueWorkAssignerHelper.KEY_SEPARATOR + target.getSourceTableId();

    queuedWork.add("wal1|" + serializedTarget.toString());

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

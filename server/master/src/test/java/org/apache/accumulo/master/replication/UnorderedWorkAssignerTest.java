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
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.impl.Table;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.replication.ReplicationConstants;
import org.apache.accumulo.core.replication.ReplicationTarget;
import org.apache.accumulo.server.replication.DistributedWorkQueueWorkAssignerHelper;
import org.apache.accumulo.server.zookeeper.DistributedWorkQueue;
import org.apache.accumulo.server.zookeeper.ZooCache;
import org.apache.hadoop.fs.Path;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class UnorderedWorkAssignerTest {

  private Connector conn;
  private UnorderedWorkAssigner assigner;

  @Before
  public void init() throws Exception {
    AccumuloConfiguration conf = createMock(AccumuloConfiguration.class);
    conn = createMock(Connector.class);
    assigner = new UnorderedWorkAssigner(conf, conn);
  }

  @Test
  public void workQueuedUsingFileName() throws Exception {
    ReplicationTarget target = new ReplicationTarget("cluster1", "table1", Table.ID.of("1"));

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

}

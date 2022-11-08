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
package org.apache.accumulo.manager.replication;

import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.admin.InstanceOperations;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.fate.zookeeper.ZooCache;
import org.apache.accumulo.core.replication.ReplicationConstants;
import org.apache.accumulo.core.replication.ReplicationTarget;
import org.apache.accumulo.server.replication.DistributedWorkQueueWorkAssignerHelper;
import org.apache.accumulo.server.zookeeper.DistributedWorkQueue;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

@Deprecated
@Disabled("Replication Tests are not stable and not currently maintained")
public class UnorderedWorkAssignerTest {

  private AccumuloClient client;
  private UnorderedWorkAssigner assigner;

  @BeforeEach
  public void init() {
    AccumuloConfiguration conf = createMock(AccumuloConfiguration.class);
    client = createMock(AccumuloClient.class);
    assigner = new UnorderedWorkAssigner(conf, client);
  }

  @Test
  public void workQueuedUsingFileName() throws Exception {
    ReplicationTarget target = new ReplicationTarget("cluster1", "table1", TableId.of("1"));

    DistributedWorkQueue workQueue = createMock(DistributedWorkQueue.class);
    Set<String> queuedWork = new HashSet<>();
    assigner.setQueuedWork(queuedWork);
    assigner.setWorkQueue(workQueue);

    Path p = new Path("/accumulo/wal/tserver+port/" + UUID.randomUUID());

    String expectedQueueKey =
        p.getName() + DistributedWorkQueueWorkAssignerHelper.KEY_SEPARATOR + target.getPeerName()
            + DistributedWorkQueueWorkAssignerHelper.KEY_SEPARATOR + target.getRemoteIdentifier()
            + DistributedWorkQueueWorkAssignerHelper.KEY_SEPARATOR + target.getSourceTableId();

    workQueue.addWork(expectedQueueKey, p.toString());
    expectLastCall().once();

    replay(workQueue);

    assigner.queueWork(p, target);

    assertEquals(1, queuedWork.size());
    assertEquals(expectedQueueKey, queuedWork.iterator().next());
  }

  @Test
  public void existingWorkIsReQueued() throws Exception {
    DistributedWorkQueue workQueue = createMock(DistributedWorkQueue.class);

    List<String> existingWork =
        Arrays.asList("/accumulo/wal/tserver+port/wal1", "/accumulo/wal/tserver+port/wal2");
    expect(workQueue.getWorkQueued()).andReturn(existingWork);

    replay(workQueue);

    assigner.setWorkQueue(workQueue);
    assigner.initializeQueuedWork();

    verify(workQueue);

    Set<String> queuedWork = assigner.getQueuedWork();
    assertEquals(existingWork.size(), queuedWork.size(),
        "Expected existing work and queued work to be the same size");
    assertTrue(queuedWork.containsAll(existingWork), "Expected all existing work to be queued");
  }

  @Test
  public void workNotInZooKeeperIsCleanedUp() {
    Set<String> queuedWork = new LinkedHashSet<>(Arrays.asList("wal1", "wal2"));
    assigner.setQueuedWork(queuedWork);

    ZooCache cache = createMock(ZooCache.class);
    assigner.setZooCache(cache);

    InstanceOperations opts = createMock(InstanceOperations.class);
    expect(opts.getInstanceID()).andReturn("id");
    expect(client.instanceOperations()).andReturn(opts);

    expect(cache.get(Constants.ZROOT + "/id" + ReplicationConstants.ZOO_WORK_QUEUE + "/wal1"))
        .andReturn(null);
    expect(cache.get(Constants.ZROOT + "/id" + ReplicationConstants.ZOO_WORK_QUEUE + "/wal2"))
        .andReturn(null);

    replay(cache, opts, client);

    assigner.cleanupFinishedWork();

    verify(cache, client);
    assertTrue(queuedWork.isEmpty(), "Queued work was not emptied");
  }

}

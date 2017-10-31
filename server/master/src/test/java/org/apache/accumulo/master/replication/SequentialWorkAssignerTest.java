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
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;

import java.util.Map;
import java.util.TreeMap;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.impl.Table;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.replication.ReplicationConstants;
import org.apache.accumulo.core.replication.ReplicationTarget;
import org.apache.accumulo.core.zookeeper.ZooUtil;
import org.apache.accumulo.server.replication.DistributedWorkQueueWorkAssignerHelper;
import org.apache.accumulo.server.zookeeper.DistributedWorkQueue;
import org.apache.accumulo.server.zookeeper.ZooCache;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class SequentialWorkAssignerTest {

  private Connector conn;
  private SequentialWorkAssigner assigner;

  @Before
  public void init() throws Exception {
    AccumuloConfiguration conf = createMock(AccumuloConfiguration.class);
    conn = createMock(Connector.class);
    assigner = new SequentialWorkAssigner(conf, conn);
  }

  @Test
  public void basicZooKeeperCleanup() throws Exception {
    DistributedWorkQueue workQueue = createMock(DistributedWorkQueue.class);
    ZooCache zooCache = createMock(ZooCache.class);
    Instance inst = createMock(Instance.class);

    Map<String,Map<Table.ID,String>> queuedWork = new TreeMap<>();
    Map<Table.ID,String> cluster1Work = new TreeMap<>();

    // Two files for cluster1, one for table '1' and another for table '2' we havce assigned work for
    cluster1Work.put(Table.ID.of("1"), DistributedWorkQueueWorkAssignerHelper.getQueueKey("file1", new ReplicationTarget("cluster1", "1", Table.ID.of("1"))));
    cluster1Work.put(Table.ID.of("2"), DistributedWorkQueueWorkAssignerHelper.getQueueKey("file2", new ReplicationTarget("cluster1", "2", Table.ID.of("2"))));

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
            + DistributedWorkQueueWorkAssignerHelper.getQueueKey("file1", new ReplicationTarget("cluster1", "1", Table.ID.of("1"))))).andReturn(null);
    // file2 still needs to replicate
    expect(
        zooCache.get(ZooUtil.getRoot("instance") + ReplicationConstants.ZOO_WORK_QUEUE + "/"
            + DistributedWorkQueueWorkAssignerHelper.getQueueKey("file2", new ReplicationTarget("cluster1", "2", Table.ID.of("2"))))).andReturn(new byte[0]);

    replay(workQueue, zooCache, conn, inst);

    assigner.cleanupFinishedWork();

    verify(workQueue, zooCache, conn, inst);

    Assert.assertEquals(1, cluster1Work.size());
    Assert.assertEquals(DistributedWorkQueueWorkAssignerHelper.getQueueKey("file2", new ReplicationTarget("cluster1", "2", Table.ID.of("2"))),
        cluster1Work.get(Table.ID.of("2")));
  }
}

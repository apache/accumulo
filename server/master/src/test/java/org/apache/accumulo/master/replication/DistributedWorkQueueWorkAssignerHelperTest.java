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

import java.util.Map.Entry;
import java.util.UUID;

import org.apache.accumulo.core.client.impl.Table;
import org.apache.accumulo.core.replication.ReplicationTarget;
import org.apache.accumulo.server.replication.DistributedWorkQueueWorkAssignerHelper;
import org.apache.hadoop.fs.Path;
import org.apache.zookeeper.common.PathUtils;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 */
public class DistributedWorkQueueWorkAssignerHelperTest {

  @Test
  public void createsValidZKNodeName() {
    Path p = new Path("/accumulo/wals/tserver+port/" + UUID.randomUUID().toString());
    ReplicationTarget target = new ReplicationTarget("cluster1", "table1", Table.ID.of("1"));

    String key = DistributedWorkQueueWorkAssignerHelper.getQueueKey(p.toString(), target);

    PathUtils.validatePath(key);
  }

  @Test
  public void queueKeySerialization() {
    Path p = new Path("/accumulo/wals/tserver+port/" + UUID.randomUUID().toString());
    ReplicationTarget target = new ReplicationTarget("cluster1", "table1", Table.ID.of("1"));

    String key = DistributedWorkQueueWorkAssignerHelper.getQueueKey(p.toString(), target);

    Entry<String,ReplicationTarget> result = DistributedWorkQueueWorkAssignerHelper.fromQueueKey(key);
    Assert.assertEquals(p.toString(), result.getKey());
    Assert.assertEquals(target, result.getValue());
  }

}

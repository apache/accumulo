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
package org.apache.accumulo.tserver.replication;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.createMockBuilder;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Map;

import org.apache.accumulo.core.conf.ConfigurationCopy;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.replication.ReplicationTarget;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.replication.DistributedWorkQueueWorkAssignerHelper;
import org.apache.accumulo.server.replication.ReplicaSystem;
import org.apache.accumulo.server.replication.ReplicaSystemHelper;
import org.apache.accumulo.server.replication.proto.Replication.Status;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.Test;

@Deprecated
public class ReplicationProcessorTest {

  @Test
  public void peerTypeExtractionFromConfiguration() {
    ServerContext context = createMock(ServerContext.class);

    String peerName = "peer";
    String configuration = "java.lang.String,foo";
    var conf = new ConfigurationCopy(Map.of(Property.REPLICATION_PEERS + peerName, configuration));
    expect(context.getConfiguration()).andReturn(conf);
    replay(context);

    ReplicationProcessor proc = new ReplicationProcessor(context);
    assertEquals(configuration, proc.getPeerType(peerName));
    verify(context);
  }

  @Test
  public void noPeerConfigurationThrowsAnException() {
    ServerContext context = createMock(ServerContext.class);

    var conf = new ConfigurationCopy(Map.of());
    expect(context.getConfiguration()).andReturn(conf);
    replay(context);

    ReplicationProcessor proc = new ReplicationProcessor(context);
    assertThrows(IllegalArgumentException.class, () -> proc.getPeerType("foo"));
    verify(context);
  }

  @Test
  public void filesWhichMakeNoProgressArentReplicatedAgain() throws Exception {
    ReplicaSystem replica = createMock(ReplicaSystem.class);
    ReplicaSystemHelper helper = createMock(ReplicaSystemHelper.class);
    ReplicationProcessor proc = createMockBuilder(ReplicationProcessor.class)
        .addMockedMethods("getReplicaSystem", "doesFileExist", "getStatus", "getHelper")
        .createMock();

    ReplicationTarget target = new ReplicationTarget("peer", "1", TableId.of("1"));
    Status status =
        Status.newBuilder().setBegin(0).setEnd(0).setInfiniteEnd(true).setClosed(true).build();
    Path path = new Path("/accumulo");

    String queueKey = DistributedWorkQueueWorkAssignerHelper.getQueueKey(path.toString(), target);

    expect(proc.getReplicaSystem(target)).andReturn(replica);
    expect(proc.getStatus(path.toString(), target)).andReturn(status);
    expect(proc.doesFileExist(path, target)).andReturn(true);
    expect(proc.getHelper()).andReturn(helper);
    expect(replica.replicate(path, status, target, helper)).andReturn(status);

    replay(replica, proc);

    proc.process(queueKey, path.toString().getBytes(UTF_8));

    verify(replica, proc);
  }
}

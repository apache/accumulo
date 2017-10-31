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
package org.apache.accumulo.tserver.replication;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.util.HashMap;
import java.util.Map;

import org.apache.accumulo.core.client.ClientConfiguration;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.impl.ClientContext;
import org.apache.accumulo.core.client.impl.Credentials;
import org.apache.accumulo.core.client.impl.Table;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.conf.ConfigurationCopy;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.replication.ReplicationTarget;
import org.apache.accumulo.server.fs.VolumeManager;
import org.apache.accumulo.server.replication.DistributedWorkQueueWorkAssignerHelper;
import org.apache.accumulo.server.replication.ReplicaSystem;
import org.apache.accumulo.server.replication.ReplicaSystemHelper;
import org.apache.accumulo.server.replication.proto.Replication.Status;
import org.apache.hadoop.fs.Path;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Test;

public class ReplicationProcessorTest {

  @Test
  public void peerTypeExtractionFromConfiguration() {
    Instance inst = EasyMock.createMock(Instance.class);
    VolumeManager fs = EasyMock.createMock(VolumeManager.class);
    Credentials creds = new Credentials("foo", new PasswordToken("bar"));
    ClientContext context = new ClientContext(inst, creds, new ClientConfiguration());

    Map<String,String> data = new HashMap<>();

    String peerName = "peer";
    String configuration = "java.lang.String,foo";
    data.put(Property.REPLICATION_PEERS + peerName, configuration);
    ConfigurationCopy conf = new ConfigurationCopy(data);

    ReplicationProcessor proc = new ReplicationProcessor(context, conf, fs);

    Assert.assertEquals(configuration, proc.getPeerType(peerName));
  }

  @Test(expected = IllegalArgumentException.class)
  public void noPeerConfigurationThrowsAnException() {
    Instance inst = EasyMock.createMock(Instance.class);
    VolumeManager fs = EasyMock.createMock(VolumeManager.class);
    Credentials creds = new Credentials("foo", new PasswordToken("bar"));
    ClientContext context = new ClientContext(inst, creds, new ClientConfiguration());

    Map<String,String> data = new HashMap<>();
    ConfigurationCopy conf = new ConfigurationCopy(data);

    ReplicationProcessor proc = new ReplicationProcessor(context, conf, fs);

    proc.getPeerType("foo");
  }

  @Test
  public void filesWhichMakeNoProgressArentReplicatedAgain() throws Exception {
    ReplicaSystem replica = EasyMock.createMock(ReplicaSystem.class);
    ReplicaSystemHelper helper = EasyMock.createMock(ReplicaSystemHelper.class);
    ReplicationProcessor proc = EasyMock.createMockBuilder(ReplicationProcessor.class)
        .addMockedMethods("getReplicaSystem", "doesFileExist", "getStatus", "getHelper").createMock();

    ReplicationTarget target = new ReplicationTarget("peer", "1", Table.ID.of("1"));
    Status status = Status.newBuilder().setBegin(0).setEnd(0).setInfiniteEnd(true).setClosed(true).build();
    Path path = new Path("/accumulo");

    String queueKey = DistributedWorkQueueWorkAssignerHelper.getQueueKey(path.toString(), target);

    EasyMock.expect(proc.getReplicaSystem(target)).andReturn(replica);
    EasyMock.expect(proc.getStatus(path.toString(), target)).andReturn(status);
    EasyMock.expect(proc.doesFileExist(path, target)).andReturn(true);
    EasyMock.expect(proc.getHelper()).andReturn(helper);
    EasyMock.expect(replica.replicate(path, status, target, helper)).andReturn(status);

    EasyMock.replay(replica, proc);

    proc.process(queueKey, path.toString().getBytes(UTF_8));

    EasyMock.verify(replica, proc);
  }
}

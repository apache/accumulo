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

import java.util.HashMap;
import java.util.Map;

import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.replication.ReplicaSystem;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.conf.ConfigurationCopy;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.replication.ReplicationTarget;
import org.apache.accumulo.core.replication.proto.Replication.Status;
import org.apache.accumulo.core.security.Credentials;
import org.apache.accumulo.server.fs.VolumeManager;
import org.apache.hadoop.fs.Path;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Test;

/**
 * 
 */
public class ReplicationProcessorTest {

  @Test
  public void peerTypeExtractionFromConfiguration() {
    Instance inst = EasyMock.createMock(Instance.class);
    VolumeManager fs = EasyMock.createMock(VolumeManager.class);
    Credentials creds = new Credentials("foo", new PasswordToken("bar"));

    Map<String,String> data = new HashMap<>();

    String peerName = "peer";
    String configuration = "java.lang.String,foo";
    data.put(Property.REPLICATION_PEERS + peerName, configuration);
    ConfigurationCopy conf = new ConfigurationCopy(data);

    ReplicationProcessor proc = new ReplicationProcessor(inst, conf, fs, creds);

    Assert.assertEquals(configuration, proc.getPeerType(peerName));
  }

  @Test(expected = IllegalArgumentException.class)
  public void noPeerConfigurationThrowsAnException() {
    Instance inst = EasyMock.createMock(Instance.class);
    VolumeManager fs = EasyMock.createMock(VolumeManager.class);
    Credentials creds = new Credentials("foo", new PasswordToken("bar"));

    Map<String,String> data = new HashMap<>();
    ConfigurationCopy conf = new ConfigurationCopy(data);

    ReplicationProcessor proc = new ReplicationProcessor(inst, conf, fs, creds);

    proc.getPeerType("foo");
  }

  @Test
  public void filesContinueReplicationWhenMoreDataIsPresent() throws Exception {
    ReplicaSystem replica = EasyMock.createMock(ReplicaSystem.class);
    ReplicationProcessor proc = EasyMock.createMockBuilder(ReplicationProcessor.class).addMockedMethod("recordNewStatus").createMock();

    ReplicationTarget target = new ReplicationTarget("peer", "1", "1");
    Status status = Status.newBuilder().setBegin(0).setEnd(0).setInfiniteEnd(true).setClosed(true).build();
    Path path = new Path("/accumulo");

    Status firstStatus = Status.newBuilder().setBegin(100).setEnd(0).setInfiniteEnd(true).setClosed(true).build();
    Status secondStatus = Status.newBuilder().setBegin(Long.MAX_VALUE).setEnd(0).setInfiniteEnd(true).setClosed(true).build();
    
    EasyMock.expect(replica.replicate(path, status, target)).andReturn(firstStatus);
    proc.recordNewStatus(path, firstStatus, target);
    EasyMock.expectLastCall().once();

    EasyMock.expect(replica.replicate(path, firstStatus, target)).andReturn(secondStatus);
    proc.recordNewStatus(path, secondStatus, target);
    EasyMock.expectLastCall().once();

    EasyMock.replay(replica, proc);
    
    proc.replicate(replica, path, status, target);

    EasyMock.verify(replica, proc);
  }

  @Test
  public void filesWhichMakeNoProgressArentReplicatedAgain() throws Exception {
    ReplicaSystem replica = EasyMock.createMock(ReplicaSystem.class);
    ReplicationProcessor proc = EasyMock.createMockBuilder(ReplicationProcessor.class).addMockedMethod("recordNewStatus").createMock();

    ReplicationTarget target = new ReplicationTarget("peer", "1", "1");
    Status status = Status.newBuilder().setBegin(0).setEnd(0).setInfiniteEnd(true).setClosed(true).build();
    Path path = new Path("/accumulo");

    EasyMock.expect(replica.replicate(path, status, target)).andReturn(status);

    EasyMock.replay(replica, proc);
    
    proc.replicate(replica, path, status, target);

    EasyMock.verify(replica, proc);
  }
}

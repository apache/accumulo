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
package org.apache.accumulo.server.replication;

import static org.junit.Assert.assertEquals;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.server.AccumuloServerContext;
import org.apache.accumulo.server.zookeeper.ZooCache;
import org.easymock.EasyMock;
import org.junit.Before;
import org.junit.Test;

public class ReplicationUtilTest {

  AccumuloServerContext context;
  ZooCache zc;
  AccumuloConfiguration conf;
  Map<String,String> confEntries;
  ReplicaSystemFactory factory;
  ReplicationUtil util;

  @Before
  public void setup() {
    context = EasyMock.createMock(AccumuloServerContext.class);
    zc = EasyMock.createMock(ZooCache.class);
    conf = EasyMock.createMock(AccumuloConfiguration.class);
    EasyMock.expect(context.getConfiguration()).andReturn(conf).anyTimes();
    factory = new ReplicaSystemFactory();

    util = new ReplicationUtil(context, zc, factory);
    confEntries = new HashMap<>();
  }

  @Test
  public void testUserNamePassword() {
    final String peerName = "peer";
    final String systemImpl = "my.replica.system.impl";
    final String config = "accumulo_peer,remote_host:2181";
    final String peerDefinition = systemImpl + "," + config;
    confEntries.put(Property.REPLICATION_PEER_USER.getKey() + peerName, "user");
    confEntries.put(Property.REPLICATION_PEER_PASSWORD.getKey() + peerName, "password");
    confEntries.put(Property.REPLICATION_PEERS.getKey() + peerName, peerDefinition);
    ReplicaSystem system = EasyMock.createMock(ReplicaSystem.class);

    // Return out our map of data
    EasyMock.expect(conf.getAllPropertiesWithPrefix(Property.REPLICATION_PEERS)).andReturn(confEntries);

    // Switch to replay
    EasyMock.replay(context, conf, system);

    // Get the peers from our map
    Map<String,String> peers = util.getPeers();

    // Verify the mocked calls
    EasyMock.verify(context, conf, system);

    // Assert one peer with expected class name and configuration
    assertEquals(1, peers.size());
    Entry<String,String> peer = peers.entrySet().iterator().next();
    assertEquals(peerName, peer.getKey());
    assertEquals(systemImpl, peer.getValue());
  }

  @Test
  public void testUserNameKeytab() {
    final String peerName = "peer";
    final String systemImpl = "my.replica.system.impl";
    final String config = "accumulo_peer,remote_host:2181";
    final String peerDefinition = systemImpl + "," + config;
    confEntries.put(Property.REPLICATION_PEER_USER.getKey() + peerName, "user");
    confEntries.put(Property.REPLICATION_PEER_KEYTAB.getKey() + peerName, "/path/to/keytab");
    confEntries.put(Property.REPLICATION_PEERS.getKey() + peerName, peerDefinition);
    ReplicaSystem system = EasyMock.createMock(ReplicaSystem.class);

    // Return out our map of data
    EasyMock.expect(conf.getAllPropertiesWithPrefix(Property.REPLICATION_PEERS)).andReturn(confEntries);

    // Switch to replay
    EasyMock.replay(context, conf, system);

    // Get the peers from our map
    Map<String,String> peers = util.getPeers();

    // Verify the mocked calls
    EasyMock.verify(context, conf, system);

    // Assert one peer with expected class name and configuration
    assertEquals(1, peers.size());
    Entry<String,String> peer = peers.entrySet().iterator().next();
    assertEquals(peerName, peer.getKey());
    assertEquals(systemImpl, peer.getValue());
  }
}

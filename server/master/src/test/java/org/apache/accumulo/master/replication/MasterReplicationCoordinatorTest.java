/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.accumulo.master.replication;

import static org.junit.Assert.assertEquals;

import java.util.Collections;
import java.util.TreeSet;

import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.DefaultConfiguration;
import org.apache.accumulo.core.util.HostAndPort;
import org.apache.accumulo.fate.zookeeper.ZooReader;
import org.apache.accumulo.master.Master;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.master.state.TServerInstance;
import org.easymock.EasyMock;
import org.junit.Test;

public class MasterReplicationCoordinatorTest {

  static AccumuloConfiguration config = DefaultConfiguration.getInstance();

  @Test
  public void randomServer() {
    Master master = EasyMock.createMock(Master.class);
    ZooReader reader = EasyMock.createMock(ZooReader.class);
    ServerContext context = EasyMock.createMock(ServerContext.class);
    EasyMock.expect(context.getConfiguration()).andReturn(config).anyTimes();
    EasyMock.expect(master.getContext()).andReturn(context);
    EasyMock.expect(master.getInstanceID()).andReturn("1234");
    EasyMock.replay(master, reader);

    MasterReplicationCoordinator coordinator = new MasterReplicationCoordinator(master, reader);
    TServerInstance inst1 = new TServerInstance(HostAndPort.fromParts("host1", 1234), "session");

    assertEquals(inst1, coordinator.getRandomTServer(Collections.singleton(inst1), 0));
  }

  @Test(expected = IllegalArgumentException.class)
  public void invalidOffset() {
    Master master = EasyMock.createMock(Master.class);
    ServerContext context = EasyMock.createMock(ServerContext.class);
    EasyMock.expect(context.getConfiguration()).andReturn(config).anyTimes();
    ZooReader reader = EasyMock.createMock(ZooReader.class);
    EasyMock.expect(master.getContext()).andReturn(context);
    EasyMock.expect(master.getInstanceID()).andReturn("1234");
    EasyMock.replay(master, reader);

    MasterReplicationCoordinator coordinator = new MasterReplicationCoordinator(master, reader);
    TServerInstance inst1 = new TServerInstance(HostAndPort.fromParts("host1", 1234), "session");

    assertEquals(inst1, coordinator.getRandomTServer(Collections.singleton(inst1), 1));
  }

  @Test
  public void randomServerFromMany() {
    Master master = EasyMock.createMock(Master.class);
    ZooReader reader = EasyMock.createMock(ZooReader.class);
    ServerContext context = EasyMock.createMock(ServerContext.class);
    EasyMock.expect(context.getConfiguration()).andReturn(config).anyTimes();
    EasyMock.expect(context.getInstanceID()).andReturn("1234").anyTimes();
    EasyMock.expect(context.getZooReaderWriter()).andReturn(null).anyTimes();
    EasyMock.expect(master.getInstanceID()).andReturn("1234").anyTimes();
    EasyMock.expect(master.getContext()).andReturn(context).anyTimes();
    EasyMock.replay(master, context, reader);

    MasterReplicationCoordinator coordinator = new MasterReplicationCoordinator(master, reader);

    EasyMock.verify(master, reader);

    TreeSet<TServerInstance> instances = new TreeSet<>();
    TServerInstance inst1 = new TServerInstance(HostAndPort.fromParts("host1", 1234), "session");
    instances.add(inst1);
    TServerInstance inst2 = new TServerInstance(HostAndPort.fromParts("host2", 1234), "session");
    instances.add(inst2);

    assertEquals(inst1, coordinator.getRandomTServer(instances, 0));
    assertEquals(inst2, coordinator.getRandomTServer(instances, 1));
  }
}

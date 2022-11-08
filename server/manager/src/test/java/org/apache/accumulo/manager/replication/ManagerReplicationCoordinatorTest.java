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
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Collections;
import java.util.TreeSet;

import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.DefaultConfiguration;
import org.apache.accumulo.core.data.InstanceId;
import org.apache.accumulo.core.fate.zookeeper.ZooReader;
import org.apache.accumulo.core.metadata.TServerInstance;
import org.apache.accumulo.core.util.HostAndPort;
import org.apache.accumulo.manager.Manager;
import org.apache.accumulo.server.ServerContext;
import org.junit.jupiter.api.Test;

@Deprecated
public class ManagerReplicationCoordinatorTest {

  static AccumuloConfiguration config = DefaultConfiguration.getInstance();

  @Test
  public void randomServer() {
    Manager manager = createMock(Manager.class);
    ZooReader reader = createMock(ZooReader.class);
    ServerContext context = createMock(ServerContext.class);
    expect(context.getConfiguration()).andReturn(config).anyTimes();
    expect(context.getInstanceID()).andReturn(InstanceId.of("1234")).anyTimes();
    expect(context.getZooReader()).andReturn(null).anyTimes();
    expect(context.getZooReaderWriter()).andReturn(null).anyTimes();
    expect(context.getSecurityOperation()).andReturn(null).anyTimes();
    expect(manager.getContext()).andReturn(context);
    expect(manager.getInstanceID()).andReturn(InstanceId.of("1234"));
    replay(manager, context, reader);

    ManagerReplicationCoordinator coordinator = new ManagerReplicationCoordinator(manager, reader);
    TServerInstance inst1 = new TServerInstance(HostAndPort.fromParts("host1", 1234), "session");

    assertEquals(inst1, coordinator.getRandomTServer(Collections.singleton(inst1), 0));
  }

  @Test
  public void invalidOffset() {
    Manager manager = createMock(Manager.class);
    ServerContext context = createMock(ServerContext.class);
    expect(context.getConfiguration()).andReturn(config).anyTimes();
    expect(context.getInstanceID()).andReturn(InstanceId.of("1234")).anyTimes();
    expect(context.getZooReader()).andReturn(null).anyTimes();
    expect(context.getZooReaderWriter()).andReturn(null).anyTimes();
    expect(context.getSecurityOperation()).andReturn(null).anyTimes();
    ZooReader reader = createMock(ZooReader.class);
    expect(manager.getContext()).andReturn(context);
    expect(manager.getInstanceID()).andReturn(InstanceId.of("1234"));
    replay(manager, context, reader);
    ManagerReplicationCoordinator coordinator = new ManagerReplicationCoordinator(manager, reader);
    TServerInstance inst1 = new TServerInstance(HostAndPort.fromParts("host1", 1234), "session");
    assertThrows(IllegalArgumentException.class,
        () -> coordinator.getRandomTServer(Collections.singleton(inst1), 1));
  }

  @Test
  public void randomServerFromMany() {
    Manager manager = createMock(Manager.class);
    ZooReader reader = createMock(ZooReader.class);
    ServerContext context = createMock(ServerContext.class);
    expect(context.getConfiguration()).andReturn(config).anyTimes();
    expect(context.getInstanceID()).andReturn(InstanceId.of("1234")).anyTimes();
    expect(context.getZooReader()).andReturn(null).anyTimes();
    expect(context.getZooReaderWriter()).andReturn(null).anyTimes();
    expect(context.getSecurityOperation()).andReturn(null).anyTimes();
    expect(manager.getInstanceID()).andReturn(InstanceId.of("1234")).anyTimes();
    expect(manager.getContext()).andReturn(context).anyTimes();
    replay(manager, context, reader);

    ManagerReplicationCoordinator coordinator = new ManagerReplicationCoordinator(manager, reader);

    verify(manager, reader);

    TreeSet<TServerInstance> instances = new TreeSet<>();
    TServerInstance inst1 = new TServerInstance(HostAndPort.fromParts("host1", 1234), "session");
    instances.add(inst1);
    TServerInstance inst2 = new TServerInstance(HostAndPort.fromParts("host2", 1234), "session");
    instances.add(inst2);

    assertEquals(inst1, coordinator.getRandomTServer(instances, 0));
    assertEquals(inst2, coordinator.getRandomTServer(instances, 1));
  }
}

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
package org.apache.accumulo.manager.tableOps;

import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.reset;
import static org.easymock.EasyMock.verify;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashMap;
import java.util.Set;

import org.apache.accumulo.core.master.thrift.TableInfo;
import org.apache.accumulo.core.master.thrift.TabletServerStatus;
import org.apache.accumulo.core.metadata.TServerInstance;
import org.apache.accumulo.core.util.HostAndPort;
import org.apache.accumulo.fate.Repo;
import org.apache.accumulo.manager.Manager;
import org.apache.accumulo.manager.tserverOps.ShutdownTServer;
import org.apache.accumulo.server.manager.LiveTServerSet.TServerConnection;
import org.junit.jupiter.api.Test;

public class ShutdownTServerTest {

  @Test
  public void testSingleShutdown() {
    HostAndPort hap = HostAndPort.fromParts("host1", 1234);
    final TServerInstance tserver1 = new TServerInstance(hap, "fake");
    final ShutdownTServer op = new ShutdownTServer(tserver1, false);
    final Manager manager = createMock(Manager.class);
    final TServerConnection tserverCnxn = createMock(TServerConnection.class);

    expect(manager.onlineTabletServers()).andReturn(Set.of(tserver1)).once();

    replay(tserverCnxn, manager);

    // FATE op should log error
    long wait = op.isReady(1L, manager);
    assertEquals(0, wait, "Expected wait to be greater than 0");

    verify(tserverCnxn, manager);
  }

  @Test
  public void testShutdownWithTwoServers() throws Exception {
    HostAndPort hap1 = HostAndPort.fromParts("host1", 1234);
    HostAndPort hap2 = HostAndPort.fromParts("host2", 1234);
    final TServerInstance tserver1 = new TServerInstance(hap1, "fake");
    final TServerInstance tserver2 = new TServerInstance(hap2, "fake");
    final boolean force = false;

    final ShutdownTServer op = new ShutdownTServer(tserver1, force);

    final Manager manager = createMock(Manager.class);
    final long tid = 1L;

    final TServerConnection tserverCnxn = createMock(TServerConnection.class);
    final TabletServerStatus status = new TabletServerStatus();
    status.tableMap = new HashMap<>();
    // Put in a table info record, don't care what
    status.tableMap.put("a_table", new TableInfo());

    manager.shutdownTServer(tserver1);
    expectLastCall().once();
    expect(manager.onlineTabletServers()).andReturn(Set.of(tserver1, tserver2)).anyTimes();
    expect(manager.getConnection(tserver1)).andReturn(tserverCnxn).anyTimes();
    expect(tserverCnxn.getTableMap(false)).andReturn(status);

    replay(tserverCnxn, manager);

    // FATE op is not ready
    long wait = op.isReady(tid, manager);
    assertTrue(wait > 0, "Expected wait to be greater than 0");

    verify(tserverCnxn, manager);

    // Reset the mocks
    reset(tserverCnxn, manager);

    // reset the table map to the empty set to simulate all tablets unloaded
    status.tableMap = new HashMap<>();
    manager.shutdownTServer(tserver1);
    expectLastCall().once();
    expect(manager.onlineTabletServers()).andReturn(Set.of(tserver1, tserver2)).anyTimes();
    expect(manager.getConnection(tserver1)).andReturn(tserverCnxn).anyTimes();
    expect(tserverCnxn.getTableMap(false)).andReturn(status);
    expect(manager.getManagerLock()).andReturn(null);
    tserverCnxn.halt(null);
    expectLastCall().once();

    replay(tserverCnxn, manager);

    // FATE op is not ready
    wait = op.isReady(tid, manager);
    assertEquals(0, wait, "Expected wait to be 0");

    Repo<Manager> op2 = op.call(tid, manager);
    assertNull(op2, "Expected no follow on step");

    verify(tserverCnxn, manager);
  }

}

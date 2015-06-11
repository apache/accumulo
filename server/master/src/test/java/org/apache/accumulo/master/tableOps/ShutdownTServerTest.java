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
package org.apache.accumulo.master.tableOps;

import static org.junit.Assert.assertTrue;

import java.util.Collections;
import java.util.HashMap;

import org.apache.accumulo.core.master.thrift.TableInfo;
import org.apache.accumulo.core.master.thrift.TabletServerStatus;
import org.apache.accumulo.master.Master;
import org.apache.accumulo.master.tserverOps.ShutdownTServer;
import org.apache.accumulo.server.master.LiveTServerSet.TServerConnection;
import org.apache.accumulo.server.master.state.TServerInstance;
import org.easymock.EasyMock;
import org.junit.Test;

public class ShutdownTServerTest {

  @Test
  public void testSingleShutdown() throws Exception {
    final TServerInstance tserver = EasyMock.createMock(TServerInstance.class);
    final boolean force = false;

    final ShutdownTServer op = new ShutdownTServer(tserver, force);

    final Master master = EasyMock.createMock(Master.class);
    final long tid = 1l;

    final TServerConnection tserverCnxn = EasyMock.createMock(TServerConnection.class);
    final TabletServerStatus status = new TabletServerStatus();
    status.tableMap = new HashMap<>();
    // Put in a table info record, don't care what
    status.tableMap.put("a_table", new TableInfo());

    master.shutdownTServer(tserver);
    EasyMock.expectLastCall().once();
    EasyMock.expect(master.onlineTabletServers()).andReturn(Collections.singleton(tserver));
    EasyMock.expect(master.getConnection(tserver)).andReturn(tserverCnxn);
    EasyMock.expect(tserverCnxn.getTableMap(false)).andReturn(status);

    EasyMock.replay(tserver, tserverCnxn, master);

    // FATE op is not ready
    long wait = op.isReady(tid, master);
    assertTrue("Expected wait to be greater than 0", wait > 0);

    EasyMock.verify(tserver, tserverCnxn, master);

    // Reset the mocks
    EasyMock.reset(tserver, tserverCnxn, master);

    // The same as above, but should not expect call shutdownTServer on master again
    EasyMock.expect(master.onlineTabletServers()).andReturn(Collections.singleton(tserver));
    EasyMock.expect(master.getConnection(tserver)).andReturn(tserverCnxn);
    EasyMock.expect(tserverCnxn.getTableMap(false)).andReturn(status);

    EasyMock.replay(tserver, tserverCnxn, master);

    // FATE op is not ready
    wait = op.isReady(tid, master);
    assertTrue("Expected wait to be greater than 0", wait > 0);

    EasyMock.verify(tserver, tserverCnxn, master);
  }

}

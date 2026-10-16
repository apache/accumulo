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
package org.apache.accumulo.server.log;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.List;

import org.apache.accumulo.core.fate.zookeeper.ZooReaderWriter;
import org.apache.accumulo.core.metadata.TServerInstance;
import org.apache.accumulo.core.tabletserver.log.LogEntry;
import org.apache.accumulo.core.zookeeper.ZooSession;
import org.apache.accumulo.server.ServerContext;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.Test;

class WalStateManagerTest {

  @Test
  void parseDefaultProviderWalState() {
    var parsed = WalStateManager
        .parse(("CLOSED," + DfsWalProvider.PROVIDER_ID + ",hdfs://nn/accumulo/wal/host+1234/id")
            .getBytes(UTF_8));
    assertEquals(WalStateManager.WalState.CLOSED, parsed.state());
    assertEquals(DfsWalProvider.PROVIDER_ID, parsed.providerId());
    assertEquals(new Path("hdfs://nn/accumulo/wal/host+1234/id"), parsed.path());
  }

  @Test
  void rejectLegacyWalState() {
    assertThrows(IllegalArgumentException.class,
        () -> WalStateManager.parse("CLOSED,hdfs://nn/accumulo/wal/host+1234/id".getBytes(UTF_8)));
  }

  @Test
  void parseProviderWalState() {
    var parsed = WalStateManager
        .parse("UNREFERENCED,example,hdfs://nn/accumulo/wal/host+1234/id".getBytes(UTF_8));
    assertEquals(WalStateManager.WalState.UNREFERENCED, parsed.state());
    assertEquals("example", parsed.providerId());
    assertEquals(new Path("hdfs://nn/accumulo/wal/host+1234/id"), parsed.path());
  }

  @Test
  void getWalsInUsePreservesProviderId() throws Exception {
    ServerContext context = createMock(ServerContext.class);
    ZooSession session = createMock(ZooSession.class);
    ZooReaderWriter zoo = createMock(ZooReaderWriter.class);
    TServerInstance server = new TServerInstance("localhost:1234[SESSION]");
    String serverPath = WalStateManager.ZWALS + "/" + server;
    String reference =
        "customfs://nn/accumulo/wal/localhost+1234/11111111-1111-1111-1111-111111111111";

    expect(context.getZooSession()).andReturn(session);
    expect(session.asReaderWriter()).andReturn(zoo);
    expect(zoo.exists(WalStateManager.ZWALS)).andReturn(true);
    zoo.sync(serverPath);
    expectLastCall();
    expect(zoo.getChildren(serverPath)).andReturn(List.of("11111111-1111-1111-1111-111111111111"));
    expect(zoo.getData(serverPath + "/11111111-1111-1111-1111-111111111111"))
        .andReturn(("CLOSED,custom," + reference).getBytes(UTF_8));

    replay(context, session, zoo);
    WalStateManager manager = new WalStateManager(context);
    assertEquals(List.of(LogEntry.fromPath("custom", reference)), manager.getWalsInUse(server));
    verify(context, session, zoo);
  }
}

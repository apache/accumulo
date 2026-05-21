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
package org.apache.accumulo.server.util.adminCommand;

import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.eq;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.getCurrentArguments;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Collections;
import java.util.List;
import java.util.UUID;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.clientImpl.ClientContext;
import org.apache.accumulo.core.data.ResourceGroupId;
import org.apache.accumulo.core.lock.ServiceLockData;
import org.apache.accumulo.core.lock.ServiceLockData.ThriftService;
import org.apache.accumulo.core.lock.ServiceLockPaths;
import org.apache.accumulo.core.zookeeper.ZcStat;
import org.apache.accumulo.core.zookeeper.ZooCache;
import org.apache.zookeeper.KeeperException;
import org.easymock.EasyMock;
import org.junit.jupiter.api.Test;

public class StopServersTest {

  @Test
  public void testQualifySessionId() throws KeeperException, InterruptedException {
    ClientContext ctx = createMock(ClientContext.class);
    ZooCache zc = createMock(ZooCache.class);

    String type = Constants.ZTSERVERS;
    String group = type + "/" + Constants.DEFAULT_RESOURCE_GROUP_NAME;
    String server = "localhost:12345";
    final long session = 123456789L;
    ServiceLockData sld1 = new ServiceLockData(UUID.randomUUID(), server, ThriftService.TABLET_SCAN,
        ResourceGroupId.DEFAULT);

    String serverPath = group + "/" + server;
    String validZLockEphemeralNode = "zlock#" + UUID.randomUUID() + "#0000000000";
    expect(zc.getChildren(type)).andReturn(List.of(Constants.DEFAULT_RESOURCE_GROUP_NAME))
        .anyTimes();
    expect(zc.getChildren(group)).andReturn(List.of(server)).anyTimes();
    expect(zc.getChildren(serverPath)).andReturn(Collections.singletonList(validZLockEphemeralNode))
        .anyTimes();
    expect(zc.get(eq(serverPath + "/" + validZLockEphemeralNode), EasyMock.isA(ZcStat.class)))
        .andReturn(sld1.serialize()).once();
    expect(zc.get(eq(serverPath + "/" + validZLockEphemeralNode), anyObject(ZcStat.class)))
        .andAnswer(() -> {
          ZcStat stat = (ZcStat) getCurrentArguments()[1];
          stat.setEphemeralOwner(session);
          return new byte[0];
        });
    expect(ctx.getServerPaths()).andReturn(new ServiceLockPaths(zc)).anyTimes();
    replay(ctx, zc);

    assertEquals(server + "[" + Long.toHexString(session) + "]",
        StopServers.qualifyWithZooKeeperSessionId(ctx, zc, server));

    verify(ctx, zc);
  }

  @Test
  public void testCannotQualifySessionId() throws KeeperException, InterruptedException {
    ClientContext ctx = createMock(ClientContext.class);
    ZooCache zc = createMock(ZooCache.class);

    String type = Constants.ZTSERVERS;
    String group = type + "/" + Constants.DEFAULT_RESOURCE_GROUP_NAME;
    String server = "localhost:12345";

    String serverPath = group + "/" + server;
    expect(zc.getChildren(type)).andReturn(List.of(Constants.DEFAULT_RESOURCE_GROUP_NAME));
    expect(zc.getChildren(serverPath)).andReturn(Collections.emptyList());
    expect(ctx.getServerPaths()).andReturn(new ServiceLockPaths(zc)).anyTimes();
    replay(ctx, zc);

    // A server that isn't in ZooKeeper. Can't qualify it, should return the original
    assertEquals(server, StopServers.qualifyWithZooKeeperSessionId(ctx, zc, server));

    verify(ctx, zc);
  }

}

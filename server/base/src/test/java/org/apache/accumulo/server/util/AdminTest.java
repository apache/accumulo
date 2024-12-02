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
package org.apache.accumulo.server.util;

import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.eq;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.getCurrentArguments;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Collections;
import java.util.UUID;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.clientImpl.ClientContext;
import org.apache.accumulo.core.data.InstanceId;
import org.apache.accumulo.core.fate.zookeeper.ZooCache;
import org.apache.accumulo.core.fate.zookeeper.ZooCache.ZcStat;
import org.apache.accumulo.core.fate.zookeeper.ZooUtil;
import org.junit.jupiter.api.Test;

public class AdminTest {

  @Test
  public void testZooKeeperTserverPath() {
    ClientContext context = createMock(ClientContext.class);
    InstanceId instanceId = InstanceId.of(UUID.randomUUID());

    expect(context.getZooKeeperRoot()).andReturn(ZooUtil.getRoot(instanceId));

    replay(context);

    assertEquals(ZooUtil.getRoot(instanceId) + Constants.ZTSERVERS,
        Admin.getTServersZkPath(context));

    verify(context);
  }

  @Test
  public void testQualifySessionId() {
    ZooCache zc = createMock(ZooCache.class);
    InstanceId instanceId = InstanceId.of(UUID.randomUUID());

    String root = ZooUtil.getRoot(instanceId) + Constants.ZTSERVERS;
    String server = "localhost:12345";
    final long session = 123456789L;

    String serverPath = root + "/" + server;
    String validZLockEphemeralNode = "zlock#" + UUID.randomUUID() + "#0000000000";
    expect(zc.getChildren(serverPath))
        .andReturn(Collections.singletonList(validZLockEphemeralNode));
    expect(zc.get(eq(serverPath + "/" + validZLockEphemeralNode), anyObject(ZcStat.class)))
        .andAnswer(() -> {
          ZcStat stat = (ZcStat) getCurrentArguments()[1];
          stat.setEphemeralOwner(session);
          return new byte[0];
        });

    replay(zc);

    assertEquals(server + "[" + Long.toHexString(session) + "]",
        Admin.qualifyWithZooKeeperSessionId(root, zc, server));

    verify(zc);
  }

  @Test
  public void testCannotQualifySessionId() {
    ZooCache zc = createMock(ZooCache.class);
    InstanceId instanceId = InstanceId.of(UUID.randomUUID());

    String root = ZooUtil.getRoot(instanceId) + Constants.ZTSERVERS;
    String server = "localhost:12345";

    String serverPath = root + "/" + server;
    expect(zc.getChildren(serverPath)).andReturn(Collections.emptyList());

    replay(zc);

    // A server that isn't in ZooKeeper. Can't qualify it, should return the original
    assertEquals(server, Admin.qualifyWithZooKeeperSessionId(root, zc, server));

    verify(zc);
  }

}

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
package org.apache.accumulo.server.util;

import static org.junit.Assert.assertEquals;

import java.util.Collections;
import java.util.UUID;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.fate.zookeeper.ZooCache;
import org.apache.zookeeper.data.Stat;
import org.easymock.EasyMock;
import org.easymock.IAnswer;
import org.junit.Test;

public class AdminTest {

  @Test
  public void testZooKeeperTserverPath() {
    Instance instance = EasyMock.createMock(Instance.class);
    String instanceId = UUID.randomUUID().toString();

    EasyMock.expect(instance.getInstanceID()).andReturn(instanceId);

    EasyMock.replay(instance);

    assertEquals(Constants.ZROOT + "/" + instanceId + Constants.ZTSERVERS, Admin.getTServersZkPath(instance));

    EasyMock.verify(instance);
  }

  @Test
  public void testQualifySessionId() {
    ZooCache zc = EasyMock.createMock(ZooCache.class);

    String root = "/accumulo/id/tservers";
    String server = "localhost:12345";
    final long session = 123456789l;

    String serverPath = root + "/" + server;
    EasyMock.expect(zc.getChildren(serverPath)).andReturn(Collections.singletonList("child"));
    EasyMock.expect(zc.get(EasyMock.eq(serverPath + "/child"), EasyMock.anyObject(Stat.class))).andAnswer(new IAnswer<byte[]>() {

      @Override
      public byte[] answer() throws Throwable {
        Stat stat = (Stat) EasyMock.getCurrentArguments()[1];
        stat.setEphemeralOwner(session);
        return new byte[0];
      }

    });

    EasyMock.replay(zc);

    assertEquals(server + "[" + Long.toHexString(session) + "]", Admin.qualifyWithZooKeeperSessionId(root, zc, server));

    EasyMock.verify(zc);
  }

  @Test
  public void testCannotQualifySessionId() {
    ZooCache zc = EasyMock.createMock(ZooCache.class);

    String root = "/accumulo/id/tservers";
    String server = "localhost:12345";

    String serverPath = root + "/" + server;
    EasyMock.expect(zc.getChildren(serverPath)).andReturn(Collections.<String> emptyList());

    EasyMock.replay(zc);

    // A server that isn't in ZooKeeper. Can't qualify it, should return the original
    assertEquals(server, Admin.qualifyWithZooKeeperSessionId(root, zc, server));

    EasyMock.verify(zc);
  }

}

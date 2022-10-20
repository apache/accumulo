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
package org.apache.accumulo.core.clientImpl;

import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.fate.zookeeper.ZooCache;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class ZookeeperLockCheckerTest {
  private ClientContext context;
  private ZooCache zc;
  private ZookeeperLockChecker zklc;

  @BeforeEach
  public void setUp() {
    context = createMock(ClientContext.class);
    expect(context.getZooKeeperRoot()).andReturn("/accumulo/iid").anyTimes();
    zc = createMock(ZooCache.class);
    expect(context.getZooCache()).andReturn(zc).anyTimes();
    replay(context);
    zklc = new ZookeeperLockChecker(context);
  }

  @Test
  public void testInvalidateCache() {
    zc.clear(context.getZooKeeperRoot() + Constants.ZTSERVERS + "/server");
    replay(zc);
    zklc.invalidateCache("server");
    verify(zc);
  }
}

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
import static org.easymock.EasyMock.createStrictMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;

import java.util.List;

import org.apache.accumulo.core.clientImpl.ClientTabletCacheImpl.TabletServerLockChecker;
import org.apache.accumulo.core.fate.zookeeper.ZooCache;
import org.apache.accumulo.core.metadata.RootTable;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class RootClientTabletCacheTest {
  private ClientContext context;
  private TabletServerLockChecker lockChecker;
  private ZooCache zc;
  private RootClientTabletCache rtl;

  @BeforeEach
  public void setUp() {
    context = createMock(ClientContext.class);
    expect(context.getZooKeeperRoot()).andReturn("/accumulo/iid").anyTimes();
    zc = createStrictMock(ZooCache.class);
    expect(context.getZooCache()).andReturn(zc).anyTimes();
    replay(context);
    lockChecker = createMock(TabletServerLockChecker.class);
    rtl = new RootClientTabletCache(lockChecker);
  }

  @Test
  public void testInvalidateCache_Noop() {
    replay(zc);
    // its not expected that any of the validate functions will interact w/ zoocache
    rtl.invalidateCache(context, "server");
    rtl.invalidateCache(RootTable.EXTENT);
    rtl.invalidateCache();
    rtl.invalidateCache(List.of(RootTable.EXTENT));
    verify(zc);
  }
}

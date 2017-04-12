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
package org.apache.accumulo.core.client.impl;

import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.impl.TabletLocatorImpl.TabletServerLockChecker;
import org.apache.accumulo.core.zookeeper.ZooUtil;
import org.apache.accumulo.fate.zookeeper.ZooCache;
import org.apache.accumulo.fate.zookeeper.ZooCacheFactory;
import org.junit.Before;
import org.junit.Test;

public class RootTabletLocatorTest {
  private Instance instance;
  private TabletServerLockChecker lockChecker;
  private ZooCacheFactory zcf;
  private ZooCache zc;
  private RootTabletLocator rtl;

  @Before
  public void setUp() {
    instance = createMock(Instance.class);
    expect(instance.getInstanceID()).andReturn("iid").anyTimes();
    expect(instance.getZooKeepers()).andReturn("zk1").anyTimes();
    expect(instance.getZooKeepersSessionTimeOut()).andReturn(30000).anyTimes();
    replay(instance);
    lockChecker = createMock(TabletServerLockChecker.class);
    zcf = createMock(ZooCacheFactory.class);
    zc = createMock(ZooCache.class);
    rtl = new RootTabletLocator(lockChecker, zcf);
  }

  @Test
  public void testInvalidateCache_Server() {
    expect(zcf.getZooCache("zk1", 30000)).andReturn(zc);
    replay(zcf);
    zc.clear(ZooUtil.getRoot(instance) + Constants.ZTSERVERS + "/server");
    replay(zc);
    rtl.invalidateCache(instance, "server");
    verify(zc);
  }
}

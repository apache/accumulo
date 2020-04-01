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
package org.apache.accumulo.server.conf;

import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.eq;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.isA;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;

import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.fate.zookeeper.ZooCache;
import org.apache.accumulo.fate.zookeeper.ZooCacheFactory;
import org.apache.accumulo.server.ServerContext;
import org.apache.zookeeper.Watcher;
import org.junit.Before;
import org.junit.Test;

public class ZooConfigurationFactoryTest {

  private ServerContext context;
  private ZooCacheFactory zcf;
  private ZooCache zc;
  private ZooConfigurationFactory zconff;
  private AccumuloConfiguration parent;

  @Before
  public void setUp() {
    context = createMock(ServerContext.class);
    zcf = createMock(ZooCacheFactory.class);
    zc = createMock(ZooCache.class);
    zconff = new ZooConfigurationFactory();
    parent = createMock(AccumuloConfiguration.class);
  }

  @Test
  public void testGetInstance() {
    expect(context.getZooKeeperRoot()).andReturn("zkroot").anyTimes();
    expect(context.getInstanceID()).andReturn("iid").anyTimes();
    expect(context.getZooKeepers()).andReturn("localhost").anyTimes();
    expect(context.getZooKeepersSessionTimeOut()).andReturn(120000).anyTimes();
    replay(context);
    expect(zcf.getZooCache(eq("localhost"), eq(120000), isA(Watcher.class))).andReturn(zc)
        .anyTimes();
    replay(zcf);

    ZooConfiguration c = zconff.getInstance(context, zcf, parent);
    assertNotNull(c);
    assertSame(c, zconff.getInstance(context, zcf, parent));

    verify(context);
    verify(zcf);
  }
}

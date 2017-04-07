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
package org.apache.accumulo.server.conf;

import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;

import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.fate.zookeeper.ZooCache;
import org.apache.accumulo.fate.zookeeper.ZooCacheFactory;
import org.junit.Before;
import org.junit.Test;

public class ZooConfigurationFactoryTest {
  private Instance instance;
  private ZooCacheFactory zcf;
  private ZooCache zc;
  private ZooConfigurationFactory zconff;
  private AccumuloConfiguration parent;

  @Before
  public void setUp() {
    instance = createMock(Instance.class);
    zcf = createMock(ZooCacheFactory.class);
    zc = createMock(ZooCache.class);
    zconff = new ZooConfigurationFactory();
    parent = createMock(AccumuloConfiguration.class);
  }

  @Test
  public void testGetInstance() {
    expect(instance.getInstanceID()).andReturn("iid");
    expectLastCall().anyTimes();
    expect(instance.getZooKeepers()).andReturn("localhost");
    expect(instance.getZooKeepersSessionTimeOut()).andReturn(120000);
    replay(instance);
    expect(zcf.getZooCache("localhost", 120000)).andReturn(zc);
    replay(zcf);

    ZooConfiguration c = zconff.getInstance(instance, zcf, parent);
    assertNotNull(c);
    assertSame(c, zconff.getInstance(instance, zcf, parent));

    verify(instance);
    verify(zcf);
  }
}

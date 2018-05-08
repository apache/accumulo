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
package org.apache.accumulo.fate.zookeeper;

import static org.easymock.EasyMock.createMock;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;

import org.apache.zookeeper.Watcher;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class ZooCacheFactoryTest {
  private ZooCacheFactory zcf;

  @Before
  public void setUp() {
    zcf = new ZooCacheFactory();
  }

  @After
  public void tearDown() {
    zcf.reset();
  }

  @Test
  public void testGetZooCache() {
    String zks1 = "zk1";
    int timeout1 = 1000;
    ZooCache zc1 = zcf.getZooCache(zks1, timeout1);
    ZooCache zc1a = zcf.getZooCache(zks1, timeout1);
    assertSame(zc1, zc1a);

    String zks2 = "zk2";
    int timeout2 = 1000;
    ZooCache zc2 = zcf.getZooCache(zks2, timeout2);
    assertNotSame(zc1, zc2);

    String zks3 = "zk1";
    int timeout3 = 2000;
    ZooCache zc3 = zcf.getZooCache(zks3, timeout3);
    assertNotSame(zc1, zc3);
  }

  @Test
  public void testGetZooCacheWatcher() {
    String zks1 = "zk1";
    int timeout1 = 1000;
    Watcher watcher = createMock(Watcher.class);
    ZooCache zc1 = zcf.getZooCache(zks1, timeout1, watcher);
    assertNotNull(zc1);
  }

  @Test
  public void testGetZooCacheWatcher_Null() {
    String zks1 = "zk1";
    int timeout1 = 1000;
    ZooCache zc1 = zcf.getZooCache(zks1, timeout1, null);
    assertNotNull(zc1);
    ZooCache zc1a = zcf.getZooCache(zks1, timeout1);
    assertSame(zc1, zc1a);
  }

  @Test
  public void testReset() {
    String zks1 = "zk1";
    int timeout1 = 1000;
    ZooCache zc1 = zcf.getZooCache(zks1, timeout1);
    zcf.reset();
    ZooCache zc1a = zcf.getZooCache(zks1, timeout1);
    assertNotSame(zc1, zc1a);
  }
}

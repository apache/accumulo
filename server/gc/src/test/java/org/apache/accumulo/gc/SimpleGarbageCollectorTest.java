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
package org.apache.accumulo.gc;

import static org.apache.accumulo.gc.SimpleGarbageCollector.CANDIDATE_MEMORY_PERCENTAGE;
import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.partialMockBuilder;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.io.FileNotFoundException;
import java.util.HashMap;
import java.util.Map;

import org.apache.accumulo.core.clientImpl.Credentials;
import org.apache.accumulo.core.conf.ConfigurationCopy;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.conf.SiteConfiguration;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.fs.VolumeManager;
import org.apache.accumulo.server.security.SystemCredentials;
import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.Test;

public class SimpleGarbageCollectorTest {
  private VolumeManager volMgr;
  private ServerContext context;
  private Credentials credentials;
  private SimpleGarbageCollector gc;
  private ConfigurationCopy systemConfig;
  private static SiteConfiguration siteConfig = new SiteConfiguration();

  @Before
  public void setUp() {
    volMgr = createMock(VolumeManager.class);
    context = createMock(ServerContext.class);
    expect(context.getInstanceID()).andReturn("mock").anyTimes();
    expect(context.getZooKeepers()).andReturn("localhost").anyTimes();
    expect(context.getZooKeepersSessionTimeOut()).andReturn(30000).anyTimes();

    systemConfig = createSystemConfig();
    expect(context.getConfiguration()).andReturn(systemConfig).anyTimes();
    expect(context.getVolumeManager()).andReturn(volMgr).anyTimes();

    credentials = SystemCredentials.get("mock", siteConfig);
    expect(context.getPrincipal()).andReturn(credentials.getPrincipal()).anyTimes();
    expect(context.getAuthenticationToken()).andReturn(credentials.getToken()).anyTimes();
    expect(context.getCredentials()).andReturn(credentials).anyTimes();

    replay(context);

    gc = partialMockBuilder(SimpleGarbageCollector.class).addMockedMethod("getContext")
        .createMock();
    expect(gc.getContext()).andReturn(context).anyTimes();
    replay(gc);
  }

  private ConfigurationCopy createSystemConfig() {
    Map<String,String> conf = new HashMap<>();
    conf.put(Property.INSTANCE_RPC_SASL_ENABLED.getKey(), "false");
    conf.put(Property.GC_CYCLE_START.getKey(), "1");
    conf.put(Property.GC_CYCLE_DELAY.getKey(), "20");
    conf.put(Property.GC_DELETE_THREADS.getKey(), "2");
    conf.put(Property.GC_TRASH_IGNORE.getKey(), "false");

    return new ConfigurationCopy(conf);
  }

  @Test
  public void testInit() {
    assertSame(volMgr, gc.getContext().getVolumeManager());
    assertEquals(credentials, gc.getContext().getCredentials());
    assertTrue(gc.isUsingTrash());
    assertEquals(1000L, gc.getStartDelay());
    assertEquals(2, gc.getNumDeleteThreads());
  }

  @Test
  public void testMoveToTrash_UsingTrash() throws Exception {
    Path path = createMock(Path.class);
    expect(volMgr.moveToTrash(path)).andReturn(true);
    replay(volMgr);
    assertTrue(gc.moveToTrash(path));
    verify(volMgr);
  }

  @Test
  public void testMoveToTrash_UsingTrash_VolMgrFailure() throws Exception {
    Path path = createMock(Path.class);
    expect(volMgr.moveToTrash(path)).andThrow(new FileNotFoundException());
    replay(volMgr);
    assertFalse(gc.moveToTrash(path));
    verify(volMgr);
  }

  @Test
  public void testMoveToTrash_NotUsingTrash() throws Exception {
    systemConfig.set(Property.GC_TRASH_IGNORE.getKey(), "true");
    Path path = createMock(Path.class);
    assertFalse(gc.moveToTrash(path));
  }

  @Test
  public void testAlmostOutOfMemory_Pass() {
    testAlmostOutOfMemory(1.0f - (CANDIDATE_MEMORY_PERCENTAGE - 0.05f), false);
  }

  @Test
  public void testAlmostOutOfMemory_Fail() {
    testAlmostOutOfMemory(1.0f - (CANDIDATE_MEMORY_PERCENTAGE + 0.05f), true);
  }

  private void testAlmostOutOfMemory(float freeFactor, boolean expected) {
    Runtime runtime = createMock(Runtime.class);
    expect(runtime.totalMemory()).andReturn(1000L);
    expectLastCall().anyTimes();
    expect(runtime.maxMemory()).andReturn(1000L);
    expectLastCall().anyTimes();
    expect(runtime.freeMemory()).andReturn((long) (freeFactor * 1000.0f));
    expectLastCall().anyTimes();
    replay(runtime);

    assertEquals(expected, SimpleGarbageCollector.almostOutOfMemory(runtime));
  }

  @Test
  public void testIsDir() {
    assertTrue(SimpleGarbageCollector.isDir("/dir1"));
    assertFalse(SimpleGarbageCollector.isDir("file1"));
    assertFalse(SimpleGarbageCollector.isDir("/dir1/file1"));
    assertFalse(SimpleGarbageCollector.isDir(""));
    assertFalse(SimpleGarbageCollector.isDir(null));
  }
}

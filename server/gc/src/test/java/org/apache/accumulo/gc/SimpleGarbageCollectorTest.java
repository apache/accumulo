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
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.io.FileNotFoundException;

import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.security.Credentials;
import org.apache.accumulo.core.security.thrift.TCredentials;
import org.apache.accumulo.gc.SimpleGarbageCollector.Opts;
import org.apache.accumulo.server.fs.VolumeManager;
import org.apache.accumulo.trace.thrift.TInfo;
import org.apache.hadoop.fs.Path;
import org.easymock.EasyMock;
import org.junit.Before;
import org.junit.Test;

public class SimpleGarbageCollectorTest {
  private VolumeManager volMgr;
  private Instance instance;
  private Credentials credentials;
  private Opts opts;
  private SimpleGarbageCollector gc;
  private AccumuloConfiguration systemConfig;

  @Before
  public void setUp() {
    volMgr = createMock(VolumeManager.class);
    instance = createMock(Instance.class);
    credentials = createMock(Credentials.class);

    opts = new Opts();
    gc = new SimpleGarbageCollector(opts);
    systemConfig = mockSystemConfig();
  }

  @Test
  public void testConstruction() {
    assertSame(opts, gc.getOpts());
    assertNotNull(gc.getStatus(createMock(TInfo.class), createMock(TCredentials.class)));
  }

  private AccumuloConfiguration mockSystemConfig() {
    AccumuloConfiguration systemConfig = createMock(AccumuloConfiguration.class);
    expect(systemConfig.getTimeInMillis(Property.GC_CYCLE_START)).andReturn(1000L);
    expect(systemConfig.getTimeInMillis(Property.GC_CYCLE_DELAY)).andReturn(20000L);
    expect(systemConfig.getCount(Property.GC_DELETE_THREADS)).andReturn(2).times(2);
    expect(systemConfig.getBoolean(Property.GC_TRASH_IGNORE)).andReturn(false);
    expect(systemConfig.getBoolean(Property.GC_FILE_ARCHIVE)).andReturn(false);
    replay(systemConfig);
    return systemConfig;
  }

  @Test
  public void testInit() throws Exception {
    EasyMock.reset(systemConfig);
    expect(systemConfig.getTimeInMillis(Property.GC_CYCLE_START)).andReturn(1000L).times(2);
    expect(systemConfig.getTimeInMillis(Property.GC_CYCLE_DELAY)).andReturn(20000L);
    expect(systemConfig.getCount(Property.GC_DELETE_THREADS)).andReturn(2).times(2);
    expect(systemConfig.getBoolean(Property.GC_TRASH_IGNORE)).andReturn(false);
    replay(systemConfig);
    gc.init(volMgr, instance, credentials, systemConfig);
    assertSame(volMgr, gc.getVolumeManager());
    assertSame(instance, gc.getInstance());
    assertSame(credentials, gc.getCredentials());
    assertTrue(gc.isUsingTrash());
    assertEquals(1000L, gc.getStartDelay());
    assertEquals(2, gc.getNumDeleteThreads());
  }

  @Test
  public void testMoveToTrash_UsingTrash() throws Exception {
    gc.init(volMgr, instance, credentials, systemConfig);
    Path path = createMock(Path.class);
    expect(volMgr.moveToTrash(path)).andReturn(true);
    replay(volMgr);
    assertTrue(gc.archiveOrMoveToTrash(path));
    verify(volMgr);
  }

  @Test
  public void testMoveToTrash_UsingTrash_VolMgrFailure() throws Exception {
    gc.init(volMgr, instance, credentials, systemConfig);
    Path path = createMock(Path.class);
    expect(volMgr.moveToTrash(path)).andThrow(new FileNotFoundException());
    replay(volMgr);
    assertFalse(gc.archiveOrMoveToTrash(path));
    verify(volMgr);
  }

  @Test
  public void testMoveToTrash_NotUsingTrash() throws Exception {
    AccumuloConfiguration systemConfig = createMock(AccumuloConfiguration.class);
    expect(systemConfig.getTimeInMillis(Property.GC_CYCLE_START)).andReturn(1000L);
    expect(systemConfig.getTimeInMillis(Property.GC_CYCLE_DELAY)).andReturn(20000L);
    expect(systemConfig.getCount(Property.GC_DELETE_THREADS)).andReturn(2);
    expect(systemConfig.getBoolean(Property.GC_FILE_ARCHIVE)).andReturn(false);
    expect(systemConfig.getBoolean(Property.GC_TRASH_IGNORE)).andReturn(true);
    replay(systemConfig);
    gc.init(volMgr, instance, credentials, systemConfig);
    Path path = createMock(Path.class);
    assertFalse(gc.archiveOrMoveToTrash(path));
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

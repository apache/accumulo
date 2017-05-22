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
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.impl.Credentials;
import org.apache.accumulo.core.conf.ConfigurationCopy;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.conf.SiteConfiguration;
import org.apache.accumulo.core.security.thrift.TCredentials;
import org.apache.accumulo.core.trace.thrift.TInfo;
import org.apache.accumulo.gc.SimpleGarbageCollector.Opts;
import org.apache.accumulo.server.conf.ServerConfigurationFactory;
import org.apache.accumulo.server.fs.VolumeManager;
import org.apache.accumulo.server.security.SystemCredentials;
import org.apache.hadoop.fs.Path;
import org.easymock.EasyMock;
import org.easymock.IAnswer;
import org.junit.Before;
import org.junit.Test;

public class SimpleGarbageCollectorTest {
  private VolumeManager volMgr;
  private Instance instance;
  private Credentials credentials;
  private Opts opts;
  private SimpleGarbageCollector gc;
  private ConfigurationCopy systemConfig;

  @Before
  public void setUp() {
    volMgr = createMock(VolumeManager.class);
    instance = createMock(Instance.class);
    SiteConfiguration siteConfig = EasyMock.createMock(SiteConfiguration.class);
    expect(instance.getInstanceID()).andReturn("mock").anyTimes();
    expect(instance.getZooKeepers()).andReturn("localhost").anyTimes();
    expect(instance.getZooKeepersSessionTimeOut()).andReturn(30000).anyTimes();

    opts = new Opts();
    systemConfig = createSystemConfig();
    ServerConfigurationFactory factory = createMock(ServerConfigurationFactory.class);
    expect(factory.getInstance()).andReturn(instance).anyTimes();
    expect(factory.getSystemConfiguration()).andReturn(systemConfig).anyTimes();
    expect(factory.getSiteConfiguration()).andReturn(siteConfig).anyTimes();

    // Just make the SiteConfiguration delegate to our AccumuloConfiguration
    // Presently, we only need get(Property) and iterator().
    EasyMock.expect(siteConfig.get(EasyMock.anyObject(Property.class))).andAnswer(new IAnswer<String>() {
      @Override
      public String answer() {
        Object[] args = EasyMock.getCurrentArguments();
        return systemConfig.get((Property) args[0]);
      }
    }).anyTimes();
    EasyMock.expect(siteConfig.getBoolean(EasyMock.anyObject(Property.class))).andAnswer(new IAnswer<Boolean>() {
      @Override
      public Boolean answer() {
        Object[] args = EasyMock.getCurrentArguments();
        return systemConfig.getBoolean((Property) args[0]);
      }
    }).anyTimes();

    EasyMock.expect(siteConfig.iterator()).andAnswer(new IAnswer<Iterator<Entry<String,String>>>() {
      @Override
      public Iterator<Entry<String,String>> answer() {
        return systemConfig.iterator();
      }
    }).anyTimes();

    replay(instance, factory, siteConfig);

    credentials = SystemCredentials.get(instance);
    gc = new SimpleGarbageCollector(opts, volMgr, factory);
  }

  @Test
  public void testConstruction() {
    assertSame(opts, gc.getOpts());
    assertNotNull(gc.getStatus(createMock(TInfo.class), createMock(TCredentials.class)));
  }

  private ConfigurationCopy createSystemConfig() {
    Map<String,String> conf = new HashMap<>();
    conf.put(Property.INSTANCE_RPC_SASL_ENABLED.getKey(), "false");
    conf.put(Property.GC_CYCLE_START.getKey(), "1");
    conf.put(Property.GC_CYCLE_DELAY.getKey(), "20");
    conf.put(Property.GC_DELETE_THREADS.getKey(), "2");
    conf.put(Property.GC_TRASH_IGNORE.getKey(), "false");
    conf.put(Property.GC_FILE_ARCHIVE.getKey(), "false");

    return new ConfigurationCopy(conf);
  }

  @Test
  public void testInit() throws Exception {
    assertSame(volMgr, gc.getVolumeManager());
    assertSame(instance, gc.getInstance());
    assertEquals(credentials, gc.getCredentials());
    assertTrue(gc.isUsingTrash());
    assertEquals(1000L, gc.getStartDelay());
    assertEquals(2, gc.getNumDeleteThreads());
  }

  @Test
  public void testMoveToTrash_UsingTrash() throws Exception {
    Path path = createMock(Path.class);
    expect(volMgr.moveToTrash(path)).andReturn(true);
    replay(volMgr);
    assertTrue(gc.archiveOrMoveToTrash(path));
    verify(volMgr);
  }

  @Test
  public void testMoveToTrash_UsingTrash_VolMgrFailure() throws Exception {
    Path path = createMock(Path.class);
    expect(volMgr.moveToTrash(path)).andThrow(new FileNotFoundException());
    replay(volMgr);
    assertFalse(gc.archiveOrMoveToTrash(path));
    verify(volMgr);
  }

  @Test
  public void testMoveToTrash_NotUsingTrash() throws Exception {
    systemConfig.set(Property.GC_TRASH_IGNORE.getKey(), "true");
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

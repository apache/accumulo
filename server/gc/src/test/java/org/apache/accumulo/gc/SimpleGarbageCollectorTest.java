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
package org.apache.accumulo.gc;

import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.partialMockBuilder;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.FileNotFoundException;
import java.util.HashMap;
import java.util.Map;

import org.apache.accumulo.core.clientImpl.Credentials;
import org.apache.accumulo.core.conf.ConfigurationCopy;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.conf.SiteConfiguration;
import org.apache.accumulo.core.data.InstanceId;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.fs.VolumeManager;
import org.apache.accumulo.server.security.SystemCredentials;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SimpleGarbageCollectorTest {
  private static final Logger log = LoggerFactory.getLogger(SimpleGarbageCollectorTest.class);

  private VolumeManager volMgr;
  private ServerContext context;
  private Credentials credentials;
  private SimpleGarbageCollector gc;
  private ConfigurationCopy systemConfig;
  private static SiteConfiguration siteConfig = SiteConfiguration.empty().build();

  @BeforeEach
  public void setUp() {
    volMgr = createMock(VolumeManager.class);
    context = createMock(ServerContext.class);
    expect(context.getInstanceID()).andReturn(InstanceId.of("mock")).anyTimes();
    expect(context.getZooKeepers()).andReturn("localhost").anyTimes();
    expect(context.getZooKeepersSessionTimeOut()).andReturn(30000).anyTimes();

    systemConfig = createSystemConfig();
    expect(context.getConfiguration()).andReturn(systemConfig).anyTimes();
    expect(context.getVolumeManager()).andReturn(volMgr).anyTimes();

    credentials = SystemCredentials.get(InstanceId.of("mock"), siteConfig);
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

    return new ConfigurationCopy(conf);
  }

  @Test
  public void testInit() {
    assertSame(volMgr, gc.getContext().getVolumeManager());
    assertEquals(credentials, gc.getContext().getCredentials());
    assertEquals(1000L, gc.getStartDelay());
    assertEquals(2, gc.getNumDeleteThreads());
    assertFalse(gc.inSafeMode()); // false by default
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
  public void testIsDir() {
    assertTrue(SimpleGarbageCollector.isDir("tid1/dir1"));
    assertTrue(SimpleGarbageCollector.isDir("/dir1"));
    assertFalse(SimpleGarbageCollector.isDir("file1"));
    assertFalse(SimpleGarbageCollector.isDir("/dir1/file1"));
    assertFalse(SimpleGarbageCollector.isDir(""));
    assertFalse(SimpleGarbageCollector.isDir(null));
  }
}

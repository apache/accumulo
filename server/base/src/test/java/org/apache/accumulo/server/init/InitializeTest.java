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
package org.apache.accumulo.server.init;

import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.replay;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;

import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.conf.SiteConfiguration;
import org.apache.accumulo.fate.zookeeper.ZooReaderWriter;
import org.apache.accumulo.server.fs.VolumeManager;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * This test is not thread-safe.
 */
public class InitializeTest {
  private Configuration conf;
  private VolumeManager fs;
  private SiteConfiguration sconf;
  private ZooReaderWriter zooOrig;
  private ZooReaderWriter zoo;

  @SuppressWarnings("deprecation")
  @Before
  public void setUp() {
    conf = createMock(Configuration.class);
    fs = createMock(VolumeManager.class);
    sconf = createMock(SiteConfiguration.class);
    expect(sconf.get(Property.INSTANCE_VOLUMES)).andReturn("").anyTimes();
    expect(sconf.get(Property.INSTANCE_DFS_DIR)).andReturn("/bar").anyTimes();
    expect(sconf.get(Property.INSTANCE_ZK_HOST)).andReturn("zk1").anyTimes();
    zoo = createMock(ZooReaderWriter.class);
    zooOrig = Initialize.getZooReaderWriter();
    Initialize.setZooReaderWriter(zoo);
  }

  @After
  public void tearDown() {
    Initialize.setZooReaderWriter(zooOrig);
  }

  @SuppressWarnings("deprecation")
  @Test
  public void testIsInitialized_HasInstanceId() throws Exception {
    expect(sconf.get(Property.INSTANCE_DFS_URI)).andReturn("hdfs://foo");
    expect(fs.exists(anyObject(Path.class))).andReturn(true);
    replay(fs, sconf);
    assertTrue(Initialize.isInitialized(fs, sconf, conf));
  }

  @SuppressWarnings("deprecation")
  @Test
  public void testIsInitialized_HasDataVersion() throws Exception {
    expect(sconf.get(Property.INSTANCE_DFS_URI)).andReturn("hdfs://foo");
    expect(fs.exists(anyObject(Path.class))).andReturn(false);
    expect(fs.exists(anyObject(Path.class))).andReturn(true);
    replay(fs, sconf);
    assertTrue(Initialize.isInitialized(fs, sconf, conf));
  }

  @SuppressWarnings("deprecation")
  @Test
  public void testCheckInit_NoZK() throws Exception {
    expect(sconf.get(Property.INSTANCE_DFS_URI)).andReturn("hdfs://foo");
    expectLastCall().anyTimes();
    replay(sconf);
    expect(zoo.exists("/")).andReturn(false);
    replay(zoo);

    assertFalse(Initialize.checkInit(fs, sconf, conf));
  }

  @SuppressWarnings("deprecation")
  @Test
  public void testCheckInit_AlreadyInit() throws Exception {
    expect(sconf.get(Property.INSTANCE_DFS_URI)).andReturn("hdfs://foo").anyTimes();
    expect(sconf.get(Property.INSTANCE_SECRET))
        .andReturn(Property.INSTANCE_SECRET.getDefaultValue());
    replay(sconf);
    expect(zoo.exists("/")).andReturn(true);
    replay(zoo);
    expect(fs.exists(anyObject(Path.class))).andReturn(true);
    replay(fs);

    assertFalse(Initialize.checkInit(fs, sconf, conf));
  }

  @SuppressWarnings("deprecation")
  @Test(expected = IOException.class)
  public void testCheckInit_FSException() throws Exception {
    expect(sconf.get(Property.INSTANCE_DFS_URI)).andReturn("hdfs://foo");
    expectLastCall().anyTimes();
    expect(sconf.get(Property.INSTANCE_SECRET))
        .andReturn(Property.INSTANCE_SECRET.getDefaultValue());
    replay(sconf);
    expect(zoo.exists("/")).andReturn(true);
    replay(zoo);
    expect(fs.exists(anyObject(Path.class))).andThrow(new IOException());
    replay(fs);

    Initialize.checkInit(fs, sconf, conf);
  }

  @SuppressWarnings("deprecation")
  @Test
  public void testCheckInit_OK() throws Exception {
    expect(sconf.get(Property.INSTANCE_DFS_URI)).andReturn("hdfs://foo").anyTimes();
    expect(sconf.get(Property.INSTANCE_SECRET))
        .andReturn(Property.INSTANCE_SECRET.getDefaultValue()).anyTimes();
    replay(sconf);
    expect(zoo.exists("/")).andReturn(true);
    replay(zoo);
    expect(fs.exists(anyObject(Path.class))).andReturn(false);
    expect(fs.exists(anyObject(Path.class))).andReturn(false);
    replay(fs);

    assertTrue(Initialize.checkInit(fs, sconf, conf));
  }
}

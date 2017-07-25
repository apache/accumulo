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
package org.apache.accumulo.shell;

import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.expect;
import static org.powermock.api.easymock.PowerMock.createMock;
import static org.powermock.api.easymock.PowerMock.expectLastCall;
import static org.powermock.api.easymock.PowerMock.expectNew;
import static org.powermock.api.easymock.PowerMock.mockStatic;
import static org.powermock.api.easymock.PowerMock.replay;
import static org.powermock.api.easymock.PowerMock.verify;

import java.io.FileDescriptor;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import org.apache.accumulo.core.client.ClientConfiguration;
import org.apache.accumulo.core.client.ClientConfiguration.ClientProperty;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.ConfigSanityCheck;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.conf.SiteConfiguration;
import org.apache.accumulo.core.zookeeper.ZooUtil;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Level;
import org.easymock.EasyMock;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import jline.console.ConsoleReader;

@RunWith(PowerMockRunner.class)
@PowerMockIgnore("javax.security.*")
@PrepareForTest({Shell.class, ZooUtil.class, ConfigSanityCheck.class})
public class ShellSetInstanceTest {
  public static class TestOutputStream extends OutputStream {
    StringBuilder sb = new StringBuilder();

    @Override
    public void write(int b) throws IOException {
      sb.append((char) (0xff & b));
    }

    public String get() {
      return sb.toString();
    }

    public void clear() {
      sb.setLength(0);
    }
  }

  /**
   * Skip all tests if we have a known bad version of java; See ACCUMULO-3031
   */
  @BeforeClass
  public static void checkJavaVersion() {
    String javaVer = System.getProperty("java.version", "");
    if (javaVer.matches("^1[.]7[.]0_(\\d+)$")) {
      int v = Integer.parseInt(javaVer.substring(6));
      Assume.assumeTrue("Skipping test due to incompatible Java version; See ACCUMULO-3031", v <= 60 || v >= 72);
    }
  }

  @BeforeClass
  public static void setupClass() {
    // This is necessary because PowerMock messes with Hadoop's ability to
    // determine the current user (see security.UserGroupInformation).
    System.setProperty("HADOOP_USER_NAME", "test");
  }

  @AfterClass
  public static void teardownClass() {
    System.clearProperty("HADOOP_USER_NAME");
  }

  private TestOutputStream output;
  private Shell shell;

  @Before
  public void setup() throws IOException {
    Shell.log.setLevel(Level.OFF);
    output = new TestOutputStream();
    shell = new Shell(new ConsoleReader(new FileInputStream(FileDescriptor.in), output));
    shell.setLogErrorsToConsole();
  }

  @After
  public void tearDown() {
    shell.shutdown();
    SiteConfiguration.clearInstance();
  }

  @Deprecated
  @Test
  public void testSetInstance_Fake() throws Exception {
    ShellOptionsJC opts = createMock(ShellOptionsJC.class);
    expect(opts.isFake()).andReturn(true);
    replay(opts);

    shell.setInstance(opts);
    Assert.assertTrue(shell.getInstance() instanceof org.apache.accumulo.core.client.mock.MockInstance);
  }

  @Test
  public void testSetInstance_HdfsZooInstance_Explicit() throws Exception {
    testSetInstance_HdfsZooInstance(true, false, false);
  }

  @Test
  public void testSetInstance_HdfsZooInstance_InstanceGiven() throws Exception {
    testSetInstance_HdfsZooInstance(false, true, false);
  }

  @Test
  public void testSetInstance_HdfsZooInstance_HostsGiven() throws Exception {
    testSetInstance_HdfsZooInstance(false, false, true);
  }

  @Test
  public void testSetInstance_HdfsZooInstance_Implicit() throws Exception {
    testSetInstance_HdfsZooInstance(false, false, false);
  }

  private void testSetInstance_HdfsZooInstance(boolean explicitHdfs, boolean onlyInstance, boolean onlyHosts) throws Exception {
    ClientConfiguration clientConf = createMock(ClientConfiguration.class);
    ShellOptionsJC opts = createMock(ShellOptionsJC.class);
    expect(opts.isFake()).andReturn(false);
    expect(opts.getClientConfiguration()).andReturn(clientConf);
    expect(opts.isHdfsZooInstance()).andReturn(explicitHdfs);
    if (!explicitHdfs) {
      expect(opts.getZooKeeperInstance()).andReturn(Collections.<String> emptyList());
      if (onlyInstance) {
        expect(opts.getZooKeeperInstanceName()).andReturn("instance");
        expect(clientConf.withInstance("instance")).andReturn(clientConf);
      } else {
        expect(opts.getZooKeeperInstanceName()).andReturn(null);
      }
      if (onlyHosts) {
        expect(opts.getZooKeeperHosts()).andReturn("host3,host4");
        expect(clientConf.withZkHosts("host3,host4")).andReturn(clientConf);
      } else {
        expect(opts.getZooKeeperHosts()).andReturn(null);
      }
    }
    replay(opts);

    if (!onlyInstance) {
      expect(clientConf.get(ClientProperty.INSTANCE_NAME)).andReturn(null);
    }

    mockStatic(ConfigSanityCheck.class);
    ConfigSanityCheck.validate(EasyMock.<AccumuloConfiguration> anyObject());
    expectLastCall().atLeastOnce();
    replay(ConfigSanityCheck.class);

    if (!onlyHosts) {
      expect(clientConf.containsKey(ClientProperty.INSTANCE_ZK_HOST.getKey())).andReturn(true).atLeastOnce();
      expect(clientConf.get(ClientProperty.INSTANCE_ZK_HOST)).andReturn("host1,host2").atLeastOnce();
      expect(clientConf.withZkHosts("host1,host2")).andReturn(clientConf);
    }
    if (!onlyInstance) {
      expect(clientConf.containsKey(Property.INSTANCE_VOLUMES.getKey())).andReturn(false).atLeastOnce();
      @SuppressWarnings("deprecation")
      String INSTANCE_DFS_DIR_KEY = Property.INSTANCE_DFS_DIR.getKey();
      @SuppressWarnings("deprecation")
      String INSTANCE_DFS_URI_KEY = Property.INSTANCE_DFS_URI.getKey();
      expect(clientConf.containsKey(INSTANCE_DFS_DIR_KEY)).andReturn(true).atLeastOnce();
      expect(clientConf.containsKey(INSTANCE_DFS_URI_KEY)).andReturn(true).atLeastOnce();
      expect(clientConf.getString(INSTANCE_DFS_URI_KEY)).andReturn("hdfs://nn1").atLeastOnce();
      expect(clientConf.getString(INSTANCE_DFS_DIR_KEY)).andReturn("/dfs").atLeastOnce();
    }

    UUID randomUUID = null;
    if (!onlyInstance) {
      mockStatic(ZooUtil.class);
      randomUUID = UUID.randomUUID();
      expect(ZooUtil.getInstanceIDFromHdfs(anyObject(Path.class), anyObject(AccumuloConfiguration.class))).andReturn(randomUUID.toString());
      replay(ZooUtil.class);
      expect(clientConf.withInstance(randomUUID)).andReturn(clientConf);
    }
    replay(clientConf);

    ZooKeeperInstance theInstance = createMock(ZooKeeperInstance.class);

    expectNew(ZooKeeperInstance.class, clientConf).andReturn(theInstance);
    replay(theInstance, ZooKeeperInstance.class);

    shell.setInstance(opts);
    verify(theInstance, ZooKeeperInstance.class);
  }

  @Test
  public void testSetInstance_ZKInstance_DashZ() throws Exception {
    testSetInstance_ZKInstance(true);
  }

  @Test
  public void testSetInstance_ZKInstance_DashZIandZH() throws Exception {
    testSetInstance_ZKInstance(false);
  }

  private void testSetInstance_ZKInstance(boolean dashZ) throws Exception {
    ClientConfiguration clientConf = createMock(ClientConfiguration.class);
    ShellOptionsJC opts = createMock(ShellOptionsJC.class);
    expect(opts.isFake()).andReturn(false);
    expect(opts.getClientConfiguration()).andReturn(clientConf);
    expect(opts.isHdfsZooInstance()).andReturn(false);
    expect(clientConf.getKeys()).andReturn(Arrays.asList(ClientProperty.INSTANCE_NAME.getKey(), ClientProperty.INSTANCE_ZK_HOST.getKey()).iterator());
    expect(clientConf.getString(Property.GENERAL_SECURITY_CREDENTIAL_PROVIDER_PATHS.getKey())).andReturn(null);
    if (dashZ) {
      expect(clientConf.withInstance("foo")).andReturn(clientConf);
      expect(clientConf.getString(ClientProperty.INSTANCE_NAME.getKey())).andReturn("foo");
      expect(clientConf.withZkHosts("host1,host2")).andReturn(clientConf);
      expect(clientConf.getString(ClientProperty.INSTANCE_ZK_HOST.getKey())).andReturn("host1,host2");
      List<String> zl = new java.util.ArrayList<>();
      zl.add("foo");
      zl.add("host1,host2");
      expect(opts.getZooKeeperInstance()).andReturn(zl);
      expectLastCall().anyTimes();
    } else {
      expect(clientConf.withInstance("bar")).andReturn(clientConf);
      expect(clientConf.getString(ClientProperty.INSTANCE_NAME.getKey())).andReturn("bar");
      expect(clientConf.withZkHosts("host3,host4")).andReturn(clientConf);
      expect(clientConf.getString(ClientProperty.INSTANCE_ZK_HOST.getKey())).andReturn("host3,host4");
      expect(opts.getZooKeeperInstance()).andReturn(Collections.<String> emptyList());
      expect(opts.getZooKeeperInstanceName()).andReturn("bar");
      expect(opts.getZooKeeperHosts()).andReturn("host3,host4");
    }
    replay(clientConf);
    replay(opts);

    ZooKeeperInstance theInstance = createMock(ZooKeeperInstance.class);
    expectNew(ZooKeeperInstance.class, clientConf).andReturn(theInstance);
    replay(theInstance, ZooKeeperInstance.class);

    shell.setInstance(opts);
    verify(theInstance, ZooKeeperInstance.class);
  }
}

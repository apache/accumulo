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
package org.apache.accumulo.core.util.shell;

import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.expect;
import static org.powermock.api.easymock.PowerMock.*;

import java.io.FileDescriptor;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Collections;
import java.util.UUID;
import java.util.List;

import jline.console.ConsoleReader;

import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.core.zookeeper.ZooUtil;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Level;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

@RunWith(PowerMockRunner.class)
@PrepareForTest({Shell.class, AccumuloConfiguration.class, ZooUtil.class})
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

  @Test
  public void testSetInstance_Fake() throws Exception {
    ShellOptionsJC opts = createMock(ShellOptionsJC.class);
    expect(opts.isFake()).andReturn(true);
    replay(opts);
    MockInstance theInstance = createMock(MockInstance.class);
    expectNew(MockInstance.class, "fake").andReturn(theInstance);
    replay(theInstance, MockInstance.class);

    shell.setInstance(opts);
    verify(theInstance, MockInstance.class);
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
  private void testSetInstance_HdfsZooInstance(boolean explicitHdfs, boolean onlyInstance, boolean onlyHosts)
    throws Exception {
    ShellOptionsJC opts = createMock(ShellOptionsJC.class);
    expect(opts.isFake()).andReturn(false);
    expect(opts.isHdfsZooInstance()).andReturn(explicitHdfs);
    if (!explicitHdfs) {
      expect(opts.getZooKeeperInstance())
        .andReturn(Collections.<String>emptyList());
      if (onlyInstance) {
        expect(opts.getZooKeeperInstanceName()).andReturn("instance");
      } else {
        expect(opts.getZooKeeperInstanceName()).andReturn(null);
      }
      if (onlyHosts) {
        expect(opts.getZooKeeperHosts()).andReturn("host1,host2");
      } else {
        expect(opts.getZooKeeperHosts()).andReturn(null);
      }
    }
    replay(opts);

    AccumuloConfiguration conf = createMock(AccumuloConfiguration.class);
    mockStatic(AccumuloConfiguration.class);
    expect(AccumuloConfiguration.getSiteConfiguration()).andReturn(conf);
    replay(AccumuloConfiguration.class);

    if (!onlyHosts) {
      expect(conf.get(Property.INSTANCE_ZK_HOST)).andReturn("host1,host2");
    }
    if (!onlyInstance) {
      expect(conf.get(Property.INSTANCE_DFS_DIR)).andReturn("/dfs");
    }
    replay(conf);
    UUID randomUUID = null;
    if (!onlyInstance) {
      mockStatic(ZooUtil.class);
      randomUUID = UUID.randomUUID();
      expect(ZooUtil.getInstanceIDFromHdfs(anyObject(Path.class)))
        .andReturn(randomUUID.toString());
      replay(ZooUtil.class);
    }

    ZooKeeperInstance theInstance = createMock(ZooKeeperInstance.class);
    if (!onlyInstance) {
      expectNew(ZooKeeperInstance.class, randomUUID, "host1,host2")
        .andReturn(theInstance);
    } else {
      expectNew(ZooKeeperInstance.class, "instance", "host1,host2")
        .andReturn(theInstance);
    }
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
    ShellOptionsJC opts = createMock(ShellOptionsJC.class);
    expect(opts.isFake()).andReturn(false);
    expect(opts.isHdfsZooInstance()).andReturn(false);
    if (dashZ) {
      List<String> zl = new java.util.ArrayList<String>();
      zl.add("instance"); zl.add("host1,host2");
      expect(opts.getZooKeeperInstance()).andReturn(zl);
      expectLastCall().anyTimes();
    } else {
      expect(opts.getZooKeeperInstance()).andReturn(Collections.<String>emptyList());
      expect(opts.getZooKeeperInstanceName()).andReturn("instance");
      expect(opts.getZooKeeperHosts()).andReturn("host1,host2");
    }
    replay(opts);

    ZooKeeperInstance theInstance = createMock(ZooKeeperInstance.class);
    expectNew(ZooKeeperInstance.class, "instance", "host1,host2")
      .andReturn(theInstance);
    replay(theInstance, ZooKeeperInstance.class);

    shell.setInstance(opts);
    verify(theInstance, ZooKeeperInstance.class);
  }
}

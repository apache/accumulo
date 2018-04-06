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

import static org.easymock.EasyMock.expect;
import static org.powermock.api.easymock.PowerMock.createMock;
import static org.powermock.api.easymock.PowerMock.expectLastCall;
import static org.powermock.api.easymock.PowerMock.expectNew;
import static org.powermock.api.easymock.PowerMock.replay;
import static org.powermock.api.easymock.PowerMock.verify;

import java.io.FileDescriptor;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.conf.ClientProperty;
import org.apache.accumulo.core.conf.ConfigSanityCheck;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.conf.SiteConfiguration;
import org.apache.accumulo.core.zookeeper.ZooUtil;
import org.apache.log4j.Level;
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
      Assume.assumeTrue("Skipping test due to incompatible Java version; See ACCUMULO-3031",
          v <= 60 || v >= 72);
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
    Assert.assertTrue(
        shell.getInstance() instanceof org.apache.accumulo.core.client.mock.MockInstance);
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
    Properties props = createMock(Properties.class);
    ShellOptionsJC opts = createMock(ShellOptionsJC.class);
    expect(opts.isFake()).andReturn(false);
    expect(opts.getClientProperties()).andReturn(new Properties());
    expect(props.getProperty(Property.GENERAL_SECURITY_CREDENTIAL_PROVIDER_PATHS.getKey()))
        .andReturn(null);
    if (dashZ) {
      expect(props.getProperty(ClientProperty.INSTANCE_NAME.getKey())).andReturn("foo");
      expect(props.getProperty(ClientProperty.INSTANCE_ZOOKEEPERS.getKey()))
          .andReturn("host1,host2");
      List<String> zl = new java.util.ArrayList<>();
      zl.add("bar");
      zl.add("host3,host4");
      expect(opts.getZooKeeperInstance()).andReturn(zl);
      expectLastCall().anyTimes();
    } else {
      expect(props.getProperty(ClientProperty.INSTANCE_NAME.getKey())).andReturn("bar");
      expect(props.getProperty(ClientProperty.INSTANCE_ZOOKEEPERS.getKey()))
          .andReturn("host3,host4");
      expect(opts.getZooKeeperInstance()).andReturn(Collections.emptyList());
      expect(opts.getZooKeeperInstanceName()).andReturn("bar");
      expect(opts.getZooKeeperHosts()).andReturn("host3,host4");
    }
    replay(props);
    replay(opts);

    ZooKeeperInstance theInstance = createMock(ZooKeeperInstance.class);
    expectNew(ZooKeeperInstance.class, new Class<?>[] {String.class, String.class}, "bar",
        "host3,host4").andReturn(theInstance);
    replay(theInstance, ZooKeeperInstance.class);

    shell.setInstance(opts);
    verify(theInstance, ZooKeeperInstance.class);
  }
}

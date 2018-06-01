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
package org.apache.accumulo.core.client.mapreduce.lib.impl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.Properties;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.ConnectionInfo;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.security.tokens.AuthenticationToken;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.conf.ClientProperty;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;

public class ConfiguratorBaseTest {

  private enum PrivateTestingEnum {
    SOMETHING, SOMETHING_ELSE
  }

  @Test
  public void testEnumToConfKey() {
    assertEquals(this.getClass().getSimpleName() + ".PrivateTestingEnum.Something",
        ConfiguratorBase.enumToConfKey(this.getClass(), PrivateTestingEnum.SOMETHING));
    assertEquals(this.getClass().getSimpleName() + ".PrivateTestingEnum.SomethingElse",
        ConfiguratorBase.enumToConfKey(this.getClass(), PrivateTestingEnum.SOMETHING_ELSE));
  }

  @Test
  public void testSetConnectorInfoClassOfQConfigurationStringAuthenticationToken() {
    Configuration conf = new Configuration();
    assertFalse(ConfiguratorBase.isConnectorInfoSet(this.getClass(), conf));
    ConfiguratorBase.setConnectorInfo(this.getClass(), conf, "testUser",
        new PasswordToken("testPassword"));
    assertTrue(ConfiguratorBase.isConnectorInfoSet(this.getClass(), conf));
    assertEquals("testUser", ConfiguratorBase.getPrincipal(this.getClass(), conf));
    AuthenticationToken token = ConfiguratorBase.getAuthenticationToken(this.getClass(), conf);
    assertNotNull(token);
    assertEquals(PasswordToken.class, token.getClass());
    assertEquals(new PasswordToken("testPassword"), token);
  }

  @Test
  public void testSetConnectorInfoClassOfQConfigurationStringString()
      throws AccumuloSecurityException {
    Configuration conf = new Configuration();
    assertFalse(ConfiguratorBase.isConnectorInfoSet(this.getClass(), conf));
    ConfiguratorBase.setConnectorInfo(this.getClass(), conf, "testUser", new PasswordToken("testPass"));
    assertTrue(ConfiguratorBase.isConnectorInfoSet(this.getClass(), conf));
    assertEquals("testUser", ConfiguratorBase.getPrincipal(this.getClass(), conf));
    assertEquals( "testPass", new String(((PasswordToken)ConfiguratorBase.getConnectionInfo(this.getClass(), conf).getAuthenticationToken()).getPassword()));
  }

  @Test
  public void testSetConnectionInfo() {
    Configuration conf = new Configuration();
    ConnectionInfo info = Connector.builder().forInstance("myinstance", "myzookeepers")
        .usingPassword("user", "pass").info();
    ConfiguratorBase.setConnectionInfo(this.getClass(), conf, info);
    ConnectionInfo info2 = ConfiguratorBase.getConnectionInfo(this.getClass(), conf);
    Assert.assertEquals("myinstance", info2.getInstanceName());
    Assert.assertEquals("myzookeepers", info2.getZooKeepers());
    Assert.assertEquals("user", info2.getPrincipal());
    Assert.assertTrue(info2.getAuthenticationToken() instanceof PasswordToken);
  }

  @SuppressWarnings("deprecation")
  @Test
  public void testSetZooKeeperInstance() {
    Configuration conf = new Configuration();
    ConfiguratorBase.setZooKeeperInstance(this.getClass(), conf,
        org.apache.accumulo.core.client.ClientConfiguration.create()
            .withInstance("testInstanceName").withZkHosts("testZooKeepers").withSsl(true)
            .withZkTimeout(1234));

    org.apache.accumulo.core.client.ClientConfiguration clientConf = ConfiguratorBase
        .getClientConfiguration(this.getClass(), conf);
    assertEquals("testInstanceName", clientConf
        .get(org.apache.accumulo.core.client.ClientConfiguration.ClientProperty.INSTANCE_NAME));

    Properties props = ConfiguratorBase.getConnectionInfo(this.getClass(), conf).getProperties();
    assertEquals("testInstanceName", props.getProperty(ClientProperty.INSTANCE_NAME.getKey()));
    assertEquals("testZooKeepers", props.getProperty(ClientProperty.INSTANCE_ZOOKEEPERS.getKey()));
    assertEquals("true", props.getProperty(ClientProperty.SSL_ENABLED.getKey()));
    assertEquals("1234",
        props.getProperty(ClientProperty.INSTANCE_ZOOKEEPERS_TIMEOUT_SEC.getKey()));
  }

  @Test
  public void testSetLogLevel() {
    Configuration conf = new Configuration();
    Level currentLevel = Logger.getLogger(this.getClass()).getLevel();

    ConfiguratorBase.setLogLevel(this.getClass(), conf, Level.DEBUG);
    Logger.getLogger(this.getClass()).setLevel(currentLevel);
    assertEquals(Level.DEBUG, ConfiguratorBase.getLogLevel(this.getClass(), conf));

    ConfiguratorBase.setLogLevel(this.getClass(), conf, Level.INFO);
    Logger.getLogger(this.getClass()).setLevel(currentLevel);
    assertEquals(Level.INFO, ConfiguratorBase.getLogLevel(this.getClass(), conf));

    ConfiguratorBase.setLogLevel(this.getClass(), conf, Level.FATAL);
    Logger.getLogger(this.getClass()).setLevel(currentLevel);
    assertEquals(Level.FATAL, ConfiguratorBase.getLogLevel(this.getClass(), conf));
  }

  @Test
  public void testSetVisibiltyCacheSize() {
    Configuration conf = new Configuration();
    assertEquals(Constants.DEFAULT_VISIBILITY_CACHE_SIZE,
        ConfiguratorBase.getVisibilityCacheSize(conf));
    ConfiguratorBase.setVisibilityCacheSize(conf, 2000);
    assertEquals(2000, ConfiguratorBase.getVisibilityCacheSize(conf));
  }
}

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
package org.apache.accumulo.hadoopImpl.mapreduce.lib;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.security.tokens.AuthenticationToken;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.clientImpl.ClientInfo;
import org.apache.hadoop.conf.Configuration;
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
  public void testSetConnectorInfoClassOfQConfigurationStringString() {
    Configuration conf = new Configuration();
    assertFalse(ConfiguratorBase.isConnectorInfoSet(this.getClass(), conf));
    ConfiguratorBase.setConnectorInfo(this.getClass(), conf, "testUser",
        new PasswordToken("testPass"));
    assertTrue(ConfiguratorBase.isConnectorInfoSet(this.getClass(), conf));
    assertEquals("testUser", ConfiguratorBase.getPrincipal(this.getClass(), conf));
    assertEquals("testPass", new String(((PasswordToken) ConfiguratorBase
        .getClientInfo(this.getClass(), conf).getAuthenticationToken()).getPassword()));
  }

  @Test
  public void testSetClientInfo() {
    Configuration conf = new Configuration();
    ClientInfo info = ClientInfo.from(
        Accumulo.newClientProperties().to("myinstance", "myzookeepers").as("user", "pass").build());
    ConfiguratorBase.setClientInfo(this.getClass(), conf, info);
    ClientInfo info2 = ConfiguratorBase.getClientInfo(this.getClass(), conf);
    assertEquals("myinstance", info2.getInstanceName());
    assertEquals("myzookeepers", info2.getZooKeepers());
    assertEquals("user", info2.getPrincipal());
    assertTrue(info2.getAuthenticationToken() instanceof PasswordToken);
  }

  @Test
  public void testSetVisibilityCacheSize() {
    Configuration conf = new Configuration();
    assertEquals(Constants.DEFAULT_VISIBILITY_CACHE_SIZE,
        ConfiguratorBase.getVisibilityCacheSize(conf));
    ConfiguratorBase.setVisibilityCacheSize(conf, 2000);
    assertEquals(2000, ConfiguratorBase.getVisibilityCacheSize(conf));
  }
}

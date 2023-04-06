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
package org.apache.accumulo.hadoopImpl.mapreduce.lib;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Properties;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.clientImpl.ClientInfo;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Test;

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
  public void testSetClientProperties() {
    Configuration conf = new Configuration();
    Properties props =
        Accumulo.newClientProperties().to("myinstance", "myzookeepers").as("user", "pass").build();
    assertFalse(ConfiguratorBase.isClientConfigured(this.getClass(), conf));
    ConfiguratorBase.setClientProperties(this.getClass(), conf, props, null);
    assertTrue(ConfiguratorBase.isClientConfigured(this.getClass(), conf));
    Properties props2 = ConfiguratorBase.getClientProperties(this.getClass(), conf);
    ClientInfo info2 = ClientInfo.from(props2);
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

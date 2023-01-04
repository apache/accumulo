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
package org.apache.accumulo.core.conf;

import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Map;

import org.apache.accumulo.core.conf.ConfigCheckUtil.ConfigCheckException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class ConfigCheckUtilTest {
  private Map<String,String> m;

  @BeforeEach
  public void setUp() {
    m = new java.util.HashMap<>();
  }

  @Test
  public void testPass() {
    m.put(Property.MANAGER_CLIENTPORT.getKey(), "9999");
    m.put(Property.MANAGER_TABLET_BALANCER.getKey(),
        "org.apache.accumulo.server.manager.balancer.TableLoadBalancer");
    m.put(Property.MANAGER_BULK_RETRIES.getKey(), "3");
    ConfigCheckUtil.validate(m.entrySet(), "test");
  }

  @Test
  public void testPass_Empty() {
    ConfigCheckUtil.validate(m.entrySet(), "test");
  }

  @Test
  public void testPass_UnrecognizedValidProperty() {
    m.put(Property.MANAGER_CLIENTPORT.getKey(), "9999");
    m.put(Property.MANAGER_PREFIX.getKey() + "something", "abcdefg");
    ConfigCheckUtil.validate(m.entrySet(), "test");
  }

  @Test
  public void testPass_UnrecognizedProperty() {
    m.put(Property.MANAGER_CLIENTPORT.getKey(), "9999");
    m.put("invalid.prefix.value", "abcdefg");
    ConfigCheckUtil.validate(m.entrySet(), "test");
  }

  @Test
  public void testFail_Prefix() {
    m.put(Property.MANAGER_CLIENTPORT.getKey(), "9999");
    m.put(Property.MANAGER_PREFIX.getKey(), "oops");
    assertThrows(ConfigCheckException.class, () -> ConfigCheckUtil.validate(m.entrySet(), "test"));
  }

  @Test
  public void testFail_InstanceZkTimeoutOutOfRange() {
    m.put(Property.INSTANCE_ZK_TIMEOUT.getKey(), "10ms");
    assertThrows(ConfigCheckException.class, () -> ConfigCheckUtil.validate(m.entrySet(), "test"));
  }

  @Test
  public void testFail_badCryptoFactory() {
    m.put(Property.INSTANCE_CRYPTO_FACTORY.getKey(), "DoesNotExistCryptoFactory");
    assertThrows(ConfigCheckException.class, () -> ConfigCheckUtil.validate(m.entrySet(), "test"));
  }

  @Test
  public void testPass_defaultCryptoFactory() {
    m.put(Property.INSTANCE_CRYPTO_FACTORY.getKey(),
        Property.INSTANCE_CRYPTO_FACTORY.getDefaultValue());
    ConfigCheckUtil.validate(m.entrySet(), "test");
  }
}

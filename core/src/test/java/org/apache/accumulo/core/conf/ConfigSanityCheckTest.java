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
package org.apache.accumulo.core.conf;

import java.util.Map;

import org.apache.accumulo.core.conf.ConfigSanityCheck.SanityCheckException;
import org.junit.Before;
import org.junit.Test;

public class ConfigSanityCheckTest {
  private Map<String,String> m;

  @Before
  public void setUp() {
    m = new java.util.HashMap<>();
  }

  @Test
  public void testPass() {
    m.put(Property.MANAGER_CLIENTPORT.getKey(), "9999");
    m.put(Property.MANAGER_TABLET_BALANCER.getKey(),
        "org.apache.accumulo.server.manager.balancer.TableLoadBalancer");
    m.put(Property.MANAGER_BULK_RETRIES.getKey(), "3");
    ConfigSanityCheck.validate(m.entrySet());
  }

  @Test
  public void testPass_Empty() {
    ConfigSanityCheck.validate(m.entrySet());
  }

  @Test
  public void testPass_UnrecognizedValidProperty() {
    m.put(Property.MANAGER_CLIENTPORT.getKey(), "9999");
    m.put(Property.MANAGER_PREFIX.getKey() + "something", "abcdefg");
    ConfigSanityCheck.validate(m.entrySet());
  }

  @Test
  public void testPass_UnrecognizedProperty() {
    m.put(Property.MANAGER_CLIENTPORT.getKey(), "9999");
    m.put("invalid.prefix.value", "abcdefg");
    ConfigSanityCheck.validate(m.entrySet());
  }

  @Test(expected = SanityCheckException.class)
  public void testFail_Prefix() {
    m.put(Property.MANAGER_CLIENTPORT.getKey(), "9999");
    m.put(Property.MANAGER_PREFIX.getKey(), "oops");
    ConfigSanityCheck.validate(m.entrySet());
  }

  @Test(expected = SanityCheckException.class)
  public void testFail_InstanceZkTimeoutOutOfRange() {
    m.put(Property.INSTANCE_ZK_TIMEOUT.getKey(), "10ms");
    ConfigSanityCheck.validate(m.entrySet());
  }

  @Test(expected = SanityCheckException.class)
  public void testFail_badCryptoService() {
    m.put(Property.TABLE_CRYPTO_ENCRYPT_SERVICE.getKey(), "DoesNotExistCryptoService");
    ConfigSanityCheck.validate(m.entrySet());
  }

  @Test
  public void testPass_defaultCryptoService() {
    m.put(Property.TABLE_CRYPTO_ENCRYPT_SERVICE.getKey(),
        Property.TABLE_CRYPTO_ENCRYPT_SERVICE.getDefaultValue());
    ConfigSanityCheck.validate(m.entrySet());
  }
}

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

import static org.junit.jupiter.api.Assertions.assertFalse;
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
    m.put(Property.MANAGER_BULK_TIMEOUT.getKey(), "5m");
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
    // Manger is now a closed prefix, this will pass but will now log a warnning instead of throwing
    ConfigCheckUtil.validate(m.entrySet(), "test");
  }

  @Test
  public void testWarn_ClosedPrefixUnknownKey_doesNotThrow() {
    // Reproduces the exact scenario from #6216: a removed/stale tserver. property should not be
    // silently accepted. It should not throw (kept non-fatal to preserve compatibility), but the
    // key is no longer considered "valid" under Property.isValidPropertyKey.
    m.put(Property.TSERV_CLIENTPORT.getKey(), "9800-9899");
    m.put(Property.TSERV_PREFIX.getKey() + "port.search.removed.property", "true");
    assertFalse(Property
        .isValidPropertyKey(Property.TSERV_PREFIX.getKey() + "port.search.removed.property"));
    ConfigCheckUtil.validate(m.entrySet(), "test"); // logs a warning, does not throw
  }

  @Test
  public void testPass_NestedOpenPrefixUnderClosedPrefix() {
    // tserver.scan.executors. is a nested, open PREFIX even though tserver. itself is closed.
    m.put(Property.TSERV_SCAN_EXECUTORS_PREFIX.getKey() + "myExecutor.threads", "4");
    ConfigCheckUtil.validate(m.entrySet(), "test");
  }

  @Test
  public void testFail_ClosedPrefixAlone() {
    // setting the bare closed prefix itself is still an incomplete key, same as PREFIX today.
    m.put(Property.TSERV_PREFIX.getKey(), "oops");
    assertThrows(ConfigCheckException.class, () -> ConfigCheckUtil.validate(m.entrySet(), "test"));
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

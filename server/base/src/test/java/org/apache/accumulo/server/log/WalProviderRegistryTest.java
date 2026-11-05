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
package org.apache.accumulo.server.log;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

class WalProviderRegistryTest {

  @Test
  void providerIdsMustBeAlphanumeric() {
    assertDoesNotThrow(() -> WalProviderRegistry.validateProviderId("Provider123", "provider"));
    assertThrows(IllegalArgumentException.class,
        () -> WalProviderRegistry.validateProviderId("provider-id", "provider"));
    assertThrows(IllegalArgumentException.class,
        () -> WalProviderRegistry.validateProviderId("provider_id", "provider"));
    assertThrows(IllegalArgumentException.class,
        () -> WalProviderRegistry.validateProviderId("provider.id", "provider"));
    assertThrows(IllegalArgumentException.class,
        () -> WalProviderRegistry.validateProviderId("-", "provider"));
  }

  @Test
  void parsesProviderClassesAndOptions() {
    var config = WalProviderRegistry.parseProviderConfig(
        "{\"org.example.Provider\":{},\"org.example.OtherProvider\":{\"endpoint\":\"test\",\"retries\":\"3\"}}");

    assertEquals(Map.of("org.example.Provider", Map.of(), "org.example.OtherProvider",
        Map.of("endpoint", "test", "retries", "3")), config);
  }

  @Test
  void rejectsInvalidProviderConfiguration() {
    List.of("[]", "\"provider\"", "{\"\":{}}", "{\"org.example.Provider\":\"\"}",
        "{\"org.example.Provider\":{\"count\":3}}", "{\"org.example.Provider\":{}} trailing",
        "{\"org.example.Provider\":{},\"org.example.Provider\":{}}")
        .forEach(json -> assertThrows(IllegalArgumentException.class,
            () -> WalProviderRegistry.parseProviderConfig(json), json));
  }
}

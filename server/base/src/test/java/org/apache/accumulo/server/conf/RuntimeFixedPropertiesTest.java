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
package org.apache.accumulo.server.conf;

import static org.apache.accumulo.core.conf.Property.GC_PORT;
import static org.apache.accumulo.core.conf.Property.TSERV_NATIVEMAP_ENABLED;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashMap;
import java.util.Map;

import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.conf.SiteConfiguration;
import org.junit.jupiter.api.Test;

public class RuntimeFixedPropertiesTest {

  @Test
  public void defaultTest() {
    var siteConfig = SiteConfiguration.fromEnv().build();
    Map<String,String> storedProps = new HashMap<>();

    RuntimeFixedProperties fixed = new RuntimeFixedProperties(storedProps, siteConfig);

    assertEquals("true", fixed.get(Property.TSERV_NATIVEMAP_ENABLED));
  }

  @Test
  public void storedOverrideTest() {
    var siteConfig = SiteConfiguration.fromEnv().build();
    Map<String,String> storedProps = Map.of("tserver.memory.maps.native.enabled", "false");

    RuntimeFixedProperties fixed = new RuntimeFixedProperties(storedProps, siteConfig);

    assertEquals("false", fixed.get(Property.TSERV_NATIVEMAP_ENABLED));
  }

  @Test
  public void allTest() {
    var siteConfig = SiteConfiguration.fromEnv().build();
    Map<String,String> storedProps = new HashMap<>();

    RuntimeFixedProperties fixed = new RuntimeFixedProperties(storedProps, siteConfig);

    assertEquals(Property.fixedProperties.size(), fixed.getAll().size());
  }

  @Test
  public void changedTest() {
    var siteConfig = SiteConfiguration.fromEnv().build();
    Map<String,String> storedProps = Map.of(TSERV_NATIVEMAP_ENABLED.getKey(), "false");

    RuntimeFixedProperties fixed = new RuntimeFixedProperties(storedProps, siteConfig);

    // prop removed - changed
    assertTrue(fixed.hasChanged(Map.of()));

    // prop modified - changed
    assertTrue(fixed.hasChanged(Map.of(TSERV_NATIVEMAP_ENABLED.getKey(), "true")));

    // prop added - changed
    assertTrue(fixed
        .hasChanged(Map.of(TSERV_NATIVEMAP_ENABLED.getKey(), "false", GC_PORT.getKey(), "1234")));

    // same - no change
    assertFalse(fixed.hasChanged(storedProps));

  }
}

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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class LayeredConfigurationTest {

  private AccumuloConfiguration config;
  private ConfigurationCopy copyConfig1;
  private ConfigurationCopy copyConfig2;

  @BeforeEach
  public void setup() {
    copyConfig1 = new ConfigurationCopy(Map.of(Property.COMPACTOR_CLIENTPORT.getKey(), "1234",
        Property.TABLE_DURABILITY.getKey(), "flush"));
    copyConfig2 = new ConfigurationCopy(Map.of(Property.COMPACTOR_CLIENTPORT.getKey(), "1235",
        Property.SPLIT_MAXOPEN.getKey(), "200"));
    config = new LayeredConfiguration(
        new LayeredConfiguration(DefaultConfiguration.getInstance(), copyConfig1), copyConfig2);
    assertNotEquals("1234", DefaultConfiguration.getInstance().get(Property.COMPACTOR_CLIENTPORT));
    assertNotEquals("1235", DefaultConfiguration.getInstance().get(Property.COMPACTOR_CLIENTPORT));
    assertNotEquals("flush", DefaultConfiguration.getInstance().get(Property.TABLE_DURABILITY));
    assertNotEquals("200", DefaultConfiguration.getInstance().get(Property.SPLIT_MAXOPEN));
  }

  @Test
  public void testUpdateCount() {
    assertEquals(0, config.getUpdateCount());
    copyConfig1.set(Property.TABLE_DURABILITY.getKey(), "sync");
    assertEquals(1, config.getUpdateCount());
    copyConfig2.set(Property.TABLE_DURABILITY.getKey(), "flush");
    assertEquals(2, config.getUpdateCount());
  }

  @Test
  public void testGet() {
    assertEquals("1235", config.get(Property.COMPACTOR_CLIENTPORT));
    assertEquals("200", config.get(Property.SPLIT_MAXOPEN));
    assertEquals("flush", config.get(Property.TABLE_DURABILITY));
    assertEquals(DefaultConfiguration.getInstance().get(Property.GC_CYCLE_DELAY),
        config.get(Property.GC_CYCLE_DELAY));
  }

  @Test
  public void testIsPropSet() {
    assertTrue(config.isPropertySet(Property.COMPACTOR_CLIENTPORT));
    assertTrue(config.isPropertySet(Property.SPLIT_MAXOPEN));
    assertTrue(config.isPropertySet(Property.TABLE_DURABILITY));
    assertFalse(config.isPropertySet(Property.GC_CYCLE_DELAY));
  }

  @Test
  public void testGetProps() {
    var props = new HashMap<String,String>();
    config.getProperties(props, p -> true);

    var props2 = new HashMap<String,String>();
    DefaultConfiguration.getInstance().forEach(e -> props2.put(e.getKey(), e.getValue()));
    copyConfig1.forEach(e -> props2.put(e.getKey(), e.getValue()));
    copyConfig2.forEach(e -> props2.put(e.getKey(), e.getValue()));

    assertEquals(props2, props);
  }
}

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
package org.apache.accumulo.server.conf.codec;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class VersionedPropertiesTest {

  private static final Logger log = LoggerFactory.getLogger(VersionedPropertiesTest.class);

  @Test
  public void initProperties() {
    Map<String,String> initProps = new HashMap<>();
    initProps.put("key1", "value1");
    initProps.put("key2", "value2");
    initProps.put("key3", "value3");
    VersionedProperties vProps = new VersionedProperties(initProps);

    Map<String,String> propMap = vProps.asMap();

    assertEquals(initProps.size(), propMap.size());

    assertEquals("value1", propMap.get("key1"));
    assertEquals("value2", propMap.get("key2"));
    assertEquals("value3", propMap.get("key3"));

    // invalid key
    assertNull(propMap.get("key4"));

  }

  @Test
  public void emptyProps() {
    VersionedProperties vProps = new VersionedProperties();

    assertNotNull(vProps);
    assertEquals(0, vProps.asMap().size());
    assertNull(vProps.asMap().get("key1"));
    assertEquals(Collections.emptyMap(), vProps.asMap());
  }

  @Test
  public void nullProps() {
    VersionedProperties vProps = new VersionedProperties(2, Instant.now(), null);
    assertNotNull(vProps);
  }

  @Test
  public void initialProps() {

    Map<String,String> aMap = new HashMap<>();
    aMap.put("k1", "v1");
    aMap.put("k2", "v2");

    VersionedProperties vProps = new VersionedProperties(aMap);

    Map<String,String> rMap = vProps.asMap();
    assertEquals(aMap.size(), rMap.size());

    assertThrows(UnsupportedOperationException.class, () -> rMap.put("k3", "v3"));

  }

  @Test
  public void updateSingleProp() {

    VersionedProperties vProps = new VersionedProperties();
    vProps = vProps.addOrUpdate("k1", "v1");

    assertEquals("v1", vProps.asMap().get("k1"));
    assertEquals(1, vProps.asMap().size());

    vProps = vProps.addOrUpdate("k1", "v1-2");

    assertEquals("v1-2", vProps.asMap().get("k1"));
  }

  @Test
  public void updateProps() {

    Map<String,String> aMap = new HashMap<>();
    aMap.put("k1", "v1");
    aMap.put("k2", "v2");

    VersionedProperties vProps = new VersionedProperties(aMap);

    assertEquals("v1", vProps.asMap().get("k1"));
    assertEquals(aMap.size(), vProps.asMap().size());

    Map<String,String> bMap = new HashMap<>();
    bMap.put("k1", "v1-1");
    bMap.put("k3", "v3");

    VersionedProperties updated = vProps.addOrUpdate(bMap);

    assertEquals(2, vProps.asMap().size());
    assertEquals(3, updated.asMap().size());

    assertEquals("v1-1", updated.asMap().get("k1"));

  }

  @Test
  public void removeProps() {

    Map<String,String> aMap = new HashMap<>();
    aMap.put("k1", "v1");
    aMap.put("k2", "v2");

    VersionedProperties vProps = new VersionedProperties(aMap);

    assertEquals("v1", vProps.asMap().get("k1"));
    assertEquals(aMap.size(), vProps.asMap().size());

    // remove 1 existing and 1 not present
    VersionedProperties vProps2 = vProps.remove(Arrays.asList("k1", "k3"));

    assertEquals(1, vProps2.asMap().size());
    assertNull(vProps2.asMap().get("k1"));
    assertEquals("v2", vProps2.asMap().get("k2"));
  }

  @Test
  public void getInitialDataVersion() {
    VersionedProperties vProps = new VersionedProperties();
    assertEquals(0, vProps.getDataVersion());
    assertTrue(vProps.getTimestamp().compareTo(Instant.now()) <= 0,
        "timestamp should be now or earlier");
  }

  @Test
  public void prettyTest() {
    Map<String,String> aMap = new HashMap<>();
    aMap.put("k1", "v1");
    aMap.put("k2", "v2");

    VersionedProperties vProps = new VersionedProperties(aMap);
    assertFalse(vProps.toString().contains("\n"));
    assertTrue(vProps.print(true).contains("\n"));
  }

  /**
   * validate that the VersionProperty data version increases past MAX_INT (does not go negative on
   * integer overflow)
   */
  @Test
  void dataVersionOverflowTest() {
    long ver = Integer.MAX_VALUE;
    VersionedProperties intmax = new VersionedProperties(ver, Instant.now(), Map.of());
    VersionedProperties intmax_plus_1 = new VersionedProperties(ver + 1, Instant.now(), Map.of());
    VersionedProperties intmax_plus_10 = new VersionedProperties(ver + 10, Instant.now(), Map.of());

    assertTrue(intmax.getDataVersion() < intmax_plus_1.getDataVersion());
    assertTrue(intmax_plus_1.getDataVersion() < intmax_plus_10.getDataVersion());
  }

  @Test
  public void isoTimestamp() {
    VersionedProperties vProps = new VersionedProperties();
    log.trace("timestamp: {}", vProps.getTimestampISO());
    assertTrue(vProps.getTimestampISO().endsWith("Z"));
  }
}

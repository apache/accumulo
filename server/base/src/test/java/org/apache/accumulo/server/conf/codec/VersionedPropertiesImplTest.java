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
package org.apache.accumulo.server.conf.codec;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.junit.Test;

public class VersionedPropertiesImplTest {

  @Test
  public void noProperties() {
    VersionedProperties vProps = new VersionedPropertiesImpl();

    assertNull(vProps.getProperty("key1"));
    assertEquals(Collections.emptyMap(), vProps.getAllProperties());
  }

  @Test
  public void addProperty() {
    VersionedProperties vProps = new VersionedPropertiesImpl();
    vProps.addProperty("key1", "value1");
    assertEquals("value1", vProps.getProperty("key1"));
    assertEquals(1, vProps.getAllProperties().size());
    assertEquals("value1", vProps.getAllProperties().get("key1"));
  }

  @Test
  public void addProperties() {
    VersionedProperties vProps = new VersionedPropertiesImpl();
    vProps.addProperty("key1", "value1");
    assertEquals("value1", vProps.getProperty("key1"));
    assertEquals(1, vProps.getAllProperties().size());
    assertEquals("value1", vProps.getAllProperties().get("key1"));

    Map<String,String> moreProps = new HashMap<>();
    moreProps.put("key1", "newValue");
    moreProps.put("key2", "value2");
    moreProps.put("key3", "value3");
    vProps.addProperties(moreProps);

    assertEquals("newValue", vProps.getProperty("key1"));
    assertEquals("value2", vProps.getProperty("key2"));
    assertEquals("value3", vProps.getProperty("key3"));
    assertEquals(3, vProps.getAllProperties().size());

  }

  @Test
  public void initProperties() {
    Map<String,String> initProps = new HashMap<>();
    initProps.put("key1", "value1");
    initProps.put("key2", "value2");
    initProps.put("key3", "value3");
    VersionedProperties vProps = new VersionedPropertiesImpl(initProps);

    vProps.addProperty("key4", "value4");

    assertEquals("value1", vProps.getProperty("key1"));
    assertEquals("value2", vProps.getProperty("key2"));
    assertEquals("value3", vProps.getProperty("key3"));
    assertEquals("value4", vProps.getProperty("key4"));

    assertEquals(4, vProps.getAllProperties().size());

  }

  @Test
  public void removeProperty() {
    VersionedProperties vProps = new VersionedPropertiesImpl();
    vProps.addProperty("key1", "value1");
    vProps.addProperty("key2", "value2");

    assertEquals("value1", vProps.getProperty("key1"));
    assertEquals("value2", vProps.getProperty("key2"));
    assertEquals(2, vProps.getAllProperties().size());

    vProps.removeProperty("key2");

    assertEquals("value1", vProps.getProperty("key1"));
    assertNull(vProps.getProperty("key2"));
    assertEquals(1, vProps.getAllProperties().size());

    vProps.addProperty("key2", "value2");
    vProps.addProperty("key3", "value3");
    vProps.addProperty("key4", "value4");

    assertEquals("value1", vProps.getProperty("key1"));
    assertEquals("value2", vProps.getProperty("key2"));
    assertEquals("value3", vProps.getProperty("key3"));
    assertEquals("value4", vProps.getProperty("key4"));
    assertEquals(4, vProps.getAllProperties().size());

    Set<String> keys = new HashSet<>();
    keys.add("key1");
    keys.add("key2");
    keys.add("key3");

    int count = vProps.removeProperties(keys);
    assertEquals("Expected 3 keys to be removed", 3, count);
    assertEquals("value4", vProps.getProperty("key4"));
    assertEquals(1, vProps.getAllProperties().size());

  }

  @Test
  public void getInitialDataVersion() {
    VersionedProperties vProps = new VersionedPropertiesImpl();
    assertEquals(0, vProps.getDataVersion());

    // the initial version for write should be 0
    assertEquals("Initial expected version should be 0", 0, vProps.getNextVersion());
    assertTrue("timestamp should be now or earlier",
        vProps.getTimestamp().compareTo(Instant.now()) <= 0);
  }

  @Test
  public void prettyTest() {
    VersionedProperties vProps = new VersionedPropertiesImpl();
    assertFalse(vProps.toString().contains("\n"));
    assertTrue(vProps.print(true).contains("\n"));
  }
}

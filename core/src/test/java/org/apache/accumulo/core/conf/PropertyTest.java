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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.util.HashSet;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.function.Predicate;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.junit.Test;

/**
 * Test the Property class
 */
public class PropertyTest {
  @Test
  public void testProperties() {
    HashSet<String> validPrefixes = new HashSet<>();
    for (Property prop : Property.values()) {
      if (prop.getType().equals(PropertyType.PREFIX)) {
        validPrefixes.add(prop.getKey());
      }
    }

    HashSet<String> propertyNames = new HashSet<>();
    for (Property prop : Property.values()) {
      // make sure properties default values match their type
      if (prop.getType() == PropertyType.PREFIX) {
        assertNull("PREFIX property " + prop.name() + " has unexpected non-null default value.",
            prop.getDefaultValue());
      } else {
        assertTrue(
            "Property " + prop + " has invalid default value " + prop.getDefaultValue()
                + " for type " + prop.getType(),
            prop.getType().isValidFormat(prop.getDefaultValue()));
      }

      // make sure property has a description
      assertFalse("Description not set for " + prop,
          prop.getDescription() == null || prop.getDescription().isEmpty());

      // make sure property starts with valid prefix
      boolean containsValidPrefix = false;
      for (String pre : validPrefixes) {
        if (prop.getKey().startsWith(pre)) {
          containsValidPrefix = true;
          break;
        }
      }
      assertTrue("Invalid prefix on prop " + prop, containsValidPrefix);

      // make sure properties aren't duplicate
      assertFalse("Duplicate property name " + prop.getKey(),
          propertyNames.contains(prop.getKey()));
      propertyNames.add(prop.getKey());

    }
  }

  @Test
  public void testPorts() {
    HashSet<Integer> usedPorts = new HashSet<>();
    for (Property prop : Property.values()) {
      if (prop.getType().equals(PropertyType.PORT)) {
        int port = Integer.parseInt(prop.getDefaultValue());
        assertFalse("Port already in use: " + port, usedPorts.contains(port));
        usedPorts.add(port);
        assertTrue("Port out of range of valid ports: " + port, port > 1023 && port < 65536);
      }
    }
  }

  // This test verifies all "sensitive" properties are properly marked as sensitive
  @Test
  public void testSensitiveKeys() {
    // add trace token, because it's a sensitive property not in the default configuration
    ConfigurationCopy conf = new ConfigurationCopy(DefaultConfiguration.getInstance());
    conf.set("trace.token.property.blah", "something");

    // ignores duplicates because ConfigurationCopy already de-duplicates
    Collector<Entry<String,String>,?,TreeMap<String,String>> treeMapCollector =
        Collectors.toMap(Entry::getKey, Entry::getValue, (a, b) -> a, TreeMap::new);

    Predicate<Entry<String,String>> sensitiveNames =
        e -> e.getKey().equals(Property.INSTANCE_SECRET.getKey())
            || e.getKey().toLowerCase().contains("password")
            || e.getKey().toLowerCase().endsWith("secret")
            || e.getKey().startsWith(Property.TRACE_TOKEN_PROPERTY_PREFIX.getKey());

    Predicate<Entry<String,String>> isMarkedSensitive = e -> Property.isSensitive(e.getKey());

    TreeMap<String,String> expected = StreamSupport.stream(conf.spliterator(), false)
        .filter(sensitiveNames).collect(treeMapCollector);
    TreeMap<String,String> actual = StreamSupport.stream(conf.spliterator(), false)
        .filter(isMarkedSensitive).collect(treeMapCollector);

    // make sure trace token property wasn't excluded from both
    assertEquals("something", expected.get("trace.token.property.blah"));
    assertEquals(expected, actual);
  }

  @Test
  public void validatePropertyKeys() {
    for (Property prop : Property.values()) {
      if (prop.getType().equals(PropertyType.PREFIX)) {
        assertTrue(prop.getKey().endsWith("."));
        assertNull(prop.getDefaultValue());
      }
    }
  }

  @SuppressWarnings("deprecation")
  private Property getDeprecatedProperty() {
    return Property.INSTANCE_DFS_DIR;
  }

  @Test
  public void testAnnotations() {
    assertTrue(Property.GENERAL_VOLUME_CHOOSER.isExperimental());
    assertFalse(Property.TABLE_SAMPLER.isExperimental());

    assertTrue(Property.INSTANCE_SECRET.isSensitive());
    assertFalse(Property.INSTANCE_VOLUMES.isSensitive());

    assertTrue(getDeprecatedProperty().isDeprecated());
    assertFalse(Property.INSTANCE_VOLUMES_REPLACEMENTS.isDeprecated());

  }

  @Test
  public void testGetPropertyByKey() {
    for (Property prop : Property.values()) {
      assertSame(prop, Property.getPropertyByKey(prop.getKey()));
    }
  }

  @Test
  public void testIsValidPropertyKey() {
    for (Property prop : Property.values()) {
      assertTrue(Property.isValidPropertyKey(prop.getKey()));
      if (prop.getType().equals(PropertyType.PREFIX)) {
        assertTrue(Property.isValidPropertyKey(prop.getKey() + "foo9"));
      }
    }

    assertFalse(Property.isValidPropertyKey("abc.def"));
  }

  @Test
  public void testIsValidTablePropertyKey() {
    for (Property prop : Property.values()) {
      if (prop.getKey().startsWith("table.") && !prop.getKey().equals("table.")) {
        assertTrue(Property.isValidTablePropertyKey(prop.getKey()));

        if (prop.getType().equals(PropertyType.PREFIX)) {
          assertTrue(Property.isValidTablePropertyKey(prop.getKey() + "foo9"));
        } else {
          assertFalse(Property.isValidTablePropertyKey(prop.getKey() + "foo9"));
        }
      } else {
        assertFalse(Property.isValidTablePropertyKey(prop.getKey()));
      }

    }

    assertFalse(Property.isValidTablePropertyKey("abc.def"));
  }
}

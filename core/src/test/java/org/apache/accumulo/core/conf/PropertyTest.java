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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashSet;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.function.Predicate;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test the Property class
 */
public class PropertyTest {

  private static final Logger LOG = LoggerFactory.getLogger(PropertyTest.class);

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
        assertNull(prop.getDefaultValue(),
            "PREFIX property " + prop.name() + " has unexpected non-null default value.");
      } else {
        assertTrue(Property.isValidProperty(prop.getKey(), prop.getDefaultValue()),
            "Property " + prop + " has invalid default value " + prop.getDefaultValue()
                + " for type " + prop.getType());
      }

      // make sure property has a description
      assertFalse(prop.getDescription() == null || prop.getDescription().isEmpty(),
          "Description not set for " + prop);

      // make sure property description ends with a period
      assertTrue(prop.getDescription().endsWith("."),
          "Property: " + prop.getKey() + " description does not end with period.");

      // make sure property starts with valid prefix
      boolean containsValidPrefix = false;
      for (String pre : validPrefixes) {
        if (prop.getKey().startsWith(pre)) {
          containsValidPrefix = true;
          break;
        }
      }
      assertTrue(containsValidPrefix, "Invalid prefix on prop " + prop);

      // make sure properties aren't duplicate
      assertFalse(propertyNames.contains(prop.getKey()),
          "Duplicate property name " + prop.getKey());
      propertyNames.add(prop.getKey());

    }
  }

  @Test
  public void testPorts() {
    HashSet<Integer> usedPorts = new HashSet<>();
    for (Property prop : Property.values()) {
      if (prop.getType().equals(PropertyType.PORT)) {
        int port = Integer.parseInt(prop.getDefaultValue());
        assertTrue(Property.isValidProperty(prop.getKey(), Integer.toString(port)));
        assertFalse(usedPorts.contains(port), "Port already in use: " + port);
        usedPorts.add(port);
        assertTrue(port > 1023 && port < 65536, "Port out of range of valid ports: " + port);
      }
    }
  }

  @Test
  public void testPropertyValidation() {

    for (Property property : Property.values()) {
      PropertyType propertyType = property.getType();
      String invalidValue, validValue = property.getDefaultValue();
      LOG.debug("Testing property: {} with type: {}", property.getKey(), propertyType);

      switch (propertyType) {
        case URI:
        case PATH:
        case PREFIX:
        case STRING:
          // Skipping these values as they have default type of null
          LOG.debug("Skipping property {} due to property type: \"{}\"", property.getKey(),
              propertyType);
          continue;
        case TIMEDURATION:
          invalidValue = "1h30min";
          break;
        case BYTES:
          invalidValue = "1M500k";
          break;
        case MEMORY:
          invalidValue = "1.5G";
          break;
        case HOSTLIST:
          invalidValue = ":1000";
          break;
        case PORT:
          invalidValue = "65539";
          break;
        case COUNT:
          invalidValue = "-1";
          break;
        case FRACTION:
          invalidValue = "10Percent";
          break;
        case ABSOLUTEPATH:
          invalidValue = "~/foo";
          break;
        case CLASSNAME:
          LOG.debug("CLASSNAME properties currently fail this test");
          LOG.debug("Regex used for CLASSNAME property types may need to be modified");
          continue;
        case CLASSNAMELIST:
          invalidValue = "String,Object;Thing";
          break;
        case DURABILITY:
          invalidValue = "rinse";
          break;
        case GC_POST_ACTION:
          invalidValue = "expand";
          break;
        case BOOLEAN:
          invalidValue = "fooFalse";
          break;
        default:
          LOG.debug("Property type: {} has no defined test case", propertyType);
          invalidValue = "foo";
      }
      assertFalse(Property.isValidProperty(property.getKey(), invalidValue));
      assertTrue(Property.isValidProperty(property.getKey(), validValue));
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

    @SuppressWarnings("deprecation")
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

  @Test
  public void testAnnotations() {
    assertTrue(Property.GENERAL_VOLUME_CHOOSER.isExperimental());
    assertFalse(Property.TABLE_SAMPLER.isExperimental());

    assertTrue(Property.INSTANCE_SECRET.isSensitive());
    assertFalse(Property.INSTANCE_VOLUMES.isSensitive());

    @SuppressWarnings("deprecation")
    Property deprecatedProp = Property.GENERAL_CLASSPATHS;
    assertTrue(deprecatedProp.isDeprecated());
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
        assertTrue(Property.isValidTablePropertyKey(prop.getKey()), prop.getKey());

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

  @Test
  public void testFixedPropertiesNonNull() {
    Property.fixedProperties.forEach(p -> {
      assertNotNull(p.getDefaultValue());
      assertFalse(p.getDefaultValue().isBlank());
    });
  }
}

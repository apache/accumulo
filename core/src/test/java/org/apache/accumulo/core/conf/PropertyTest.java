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

import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
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
      if (prop.getType() == PropertyType.PREFIX) {
        assertNull(prop.getDefaultValue(),
            "PREFIX property " + prop.name() + " has unexpected non-null default value.");
      } else {
        // default values shouldn't be null, but they can be an empty string
        assertNotNull(prop.getDefaultValue());
        // default values shouldn't start or end with whitespace
        assertEquals(prop.getDefaultValue().strip(), prop.getDefaultValue(),
            "Property " + prop.name() + " starts or ends with whitespace");
        // default values shouldn't contain newline characters or tabs
        assertFalse(prop.getDefaultValue().contains("\t"),
            "Property " + prop.name() + " contains a tab character");
        assertFalse(prop.getDefaultValue().contains("\n"),
            "Property " + prop.name() + " contains a newline (\\n) character");
        assertFalse(prop.getDefaultValue().contains("\r"),
            "Property " + prop.name() + " contains a return (\\r) character");

        assertTrue(Property.isValidProperty(prop.getKey(), prop.getDefaultValue()),
            "Property " + prop.name() + " has invalid default value " + prop.getDefaultValue()
                + " for type " + prop.getType());
      }

      // make sure property has a description
      assertFalse(prop.getDescription() == null || prop.getDescription().isEmpty(),
          "Description not set for " + prop);

      // make sure property description doesn't have extra whitespace
      assertEquals(prop.getDescription().strip(), prop.getDescription(),
          "Property: %s has extraneous whitespace".formatted(prop.name()));

      // make sure property description ends with a period
      assertTrue(prop.getDescription().endsWith(".") || prop.getDescription().endsWith("\n```"),
          "Property: " + prop.getKey()
              + " description does not end with period or example block. Description = "
              + prop.getDescription());

      if (EnumSet
          .of(PropertyType.JSON, PropertyType.FATE_META_CONFIG, PropertyType.FATE_USER_CONFIG)
          .contains(prop.getType())) {
        assertFalse(prop.getDefaultValue().contains("'"),
            "json prop %s contains single quotes".formatted(prop.name()));
      }

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
  public void testJson() {
    // using "real" example
    String json1 =
        "[{'name':'small','type':'internal','maxSize':'32M','numThreads':2},{'name':'huge','type':'internal','numThreads':2}]"
            .replaceAll("'", "\"");
    // use synthetic, but valid json
    String json2 =
        "[{'foo':'bar','type':'test','fooBar':'32'},{'foo':'bar','type':'test','fooBar':32}]"
            .replaceAll("'", "\"");
    String json3 = "{'foo':'bar','type':'test','fooBar':'32'}".replaceAll("'", "\"");

    List<String> valids = List.of(json1, json2, json3);

    List<String> invalids = List.of("notJson", "also not json", "{\"x}", "{\"y\"", "{name:value}",
        "{ \"foo\" : \"bar\", \"foo\" : \"baz\" }", "{\"y\":123}extra");

    for (Property prop : Property.values()) {
      if (prop.getType().equals(PropertyType.JSON)) {
        valids.forEach(j -> assertTrue(Property.isValidProperty(prop.getKey(), j)));
        valids.forEach(j -> assertTrue(prop.getType().isValidFormat(j)));

        invalids.forEach(j -> assertFalse(Property.isValidProperty(prop.getKey(), j)));
        invalids.forEach(j -> assertFalse(prop.getType().isValidFormat(j)));
      }
    }
  }

  @Test
  @SuppressWarnings("deprecation")
  public void testPropertyValidation() {

    for (Property property : Property.values()) {
      if (property == Property.MANAGER_FATE_THREADPOOL_SIZE) {
        // deprecated and unused property, no need to test
        continue;
      }
      PropertyType propertyType = property.getType();
      String invalidValue;
      String validValue = property.getDefaultValue();
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
        case JSON:
          invalidValue = "not json";
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
    // add instance.crypto.opts, because it's a sensitive property not in the default configuration
    ConfigurationCopy conf = new ConfigurationCopy(DefaultConfiguration.getInstance());
    conf.set("instance.crypto.opts.sensitive.blah", "something");

    // ignores duplicates because ConfigurationCopy already de-duplicates
    Collector<Entry<String,String>,?,TreeMap<String,String>> treeMapCollector =
        Collectors.toMap(Entry::getKey, Entry::getValue, (a, b) -> a, TreeMap::new);

    Predicate<Entry<String,String>> sensitiveNames =
        e -> e.getKey().equals(Property.INSTANCE_SECRET.getKey())
            || e.getKey().toLowerCase().contains("password")
            || e.getKey().toLowerCase().endsWith("secret")
            || e.getKey().startsWith(Property.INSTANCE_CRYPTO_SENSITIVE_PREFIX.getKey());

    Predicate<Entry<String,String>> isMarkedSensitive = e -> Property.isSensitive(e.getKey());

    TreeMap<String,String> expected = StreamSupport.stream(conf.spliterator(), false)
        .filter(sensitiveNames).collect(treeMapCollector);
    TreeMap<String,String> actual = StreamSupport.stream(conf.spliterator(), false)
        .filter(isMarkedSensitive).collect(treeMapCollector);

    // make sure instance.crypto.opts property wasn't excluded from both
    assertEquals("something", expected.get("instance.crypto.opts.sensitive.blah"));
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
    assertTrue(Property.INSTANCE_CRYPTO_FACTORY.isExperimental());
    assertFalse(Property.TABLE_SAMPLER.isExperimental());

    assertTrue(Property.INSTANCE_SECRET.isSensitive());
    assertFalse(Property.INSTANCE_VOLUMES.isSensitive());

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
    Property.FIXED_PROPERTIES.forEach(p -> {
      assertNotNull(p.getDefaultValue());
      assertFalse(p.getDefaultValue().isBlank());
    });
  }
}

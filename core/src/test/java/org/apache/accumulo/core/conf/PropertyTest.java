/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.accumulo.core.conf;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.TreeSet;

import org.junit.Test;

/**
 * Test the Property class
 */
public class PropertyTest {
  @Test
  public void testProperties() {
    HashSet<String> validPrefixes = new HashSet<String>();
    for (Property prop : Property.values())
      if (prop.getType().equals(PropertyType.PREFIX))
        validPrefixes.add(prop.getKey());

    HashSet<String> propertyNames = new HashSet<String>();
    for (Property prop : Property.values()) {
      // make sure properties default values match their type
      assertTrue("Property " + prop + " has invalid default value " + prop.getDefaultValue() + " for type " + prop.getType(),
          prop.getType().isValidFormat(prop.getDefaultValue()));

      // make sure property has a description
      assertFalse("Description not set for " + prop, prop.getDescription() == null || prop.getDescription().isEmpty());

      // make sure property starts with valid prefix
      boolean containsValidPrefix = false;
      for (String pre : validPrefixes)
        if (prop.getKey().startsWith(pre)) {
          containsValidPrefix = true;
          break;
        }
      assertTrue("Invalid prefix on prop " + prop, containsValidPrefix);

      // make sure properties aren't duplicate
      assertFalse("Duplicate property name " + prop.getKey(), propertyNames.contains(prop.getKey()));
      propertyNames.add(prop.getKey());

    }
  }

  @Test
  public void testPorts() {
    HashSet<Integer> usedPorts = new HashSet<Integer>();
    for (Property prop : Property.values())
      if (prop.getType().equals(PropertyType.PORT)) {
        int port = Integer.parseInt(prop.getDefaultValue());
        assertFalse("Port already in use: " + port, usedPorts.contains(port));
        usedPorts.add(port);
        assertTrue("Port out of range of valid ports: " + port, port > 1023 && port < 65536);
      }
  }

  @Test
  public void testRawDefaultValues() {
    AccumuloConfiguration conf = AccumuloConfiguration.getDefaultConfiguration();
    assertEquals("${java.io.tmpdir}" + File.separator + "accumulo-vfs-cache-${user.name}", Property.VFS_CLASSLOADER_CACHE_DIR.getRawDefaultValue());
    assertEquals(new File(System.getProperty("java.io.tmpdir"), "accumulo-vfs-cache-" + System.getProperty("user.name")).getAbsolutePath(),
        conf.get(Property.VFS_CLASSLOADER_CACHE_DIR));
  }

  @Test
  public void testGetDefaultValue_AbsolutePath() {
    // should not expand because default is ""
    assertEquals("", Property.GENERAL_MAVEN_PROJECT_BASEDIR.getDefaultValue());
  }

  @Test
  public void testSensitiveKeys() {
    final TreeMap<String,String> extras = new TreeMap<String,String>();
    extras.put("trace.token.property.blah", "something");

    AccumuloConfiguration conf = new DefaultConfiguration() {
      @Override
      public Iterator<Entry<String,String>> iterator() {
        final Iterator<Entry<String,String>> parent = super.iterator();
        final Iterator<Entry<String,String>> mine = extras.entrySet().iterator();

        return new Iterator<Entry<String,String>>() {

          @Override
          public boolean hasNext() {
            return parent.hasNext() || mine.hasNext();
          }

          @Override
          public Entry<String,String> next() {
            return parent.hasNext() ? parent.next() : mine.next();
          }

          @Override
          public void remove() {
            throw new UnsupportedOperationException();
          }
        };
      }
    };
    TreeSet<String> expected = new TreeSet<String>();
    for (Entry<String,String> entry : conf) {
      String key = entry.getKey();
      if (key.equals(Property.INSTANCE_SECRET.getKey()) || key.toLowerCase().contains("password") || key.toLowerCase().endsWith("secret")
          || key.startsWith(Property.TRACE_TOKEN_PROPERTY_PREFIX.getKey()))
        expected.add(key);
    }
    TreeSet<String> actual = new TreeSet<String>();
    for (Entry<String,String> entry : conf) {
      String key = entry.getKey();
      if (Property.isSensitive(key))
        actual.add(key);
    }
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
}

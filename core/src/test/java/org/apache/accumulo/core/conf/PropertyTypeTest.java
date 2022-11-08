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
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Method;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.accumulo.core.WithTestNames;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.google.common.base.Joiner;

public class PropertyTypeTest extends WithTestNames {

  private PropertyType type;

  @BeforeEach
  public void getPropertyTypeForTest() {
    if (testName().startsWith("testType")) {
      String tn = testName().substring("testType".length());
      try {
        type = PropertyType.valueOf(tn);
      } catch (IllegalArgumentException e) {
        throw new AssertionError("Unexpected test method for non-existent "
            + PropertyType.class.getSimpleName() + "." + tn);
      }
    }
  }

  @Test
  public void testGetFormatDescription() {
    assertEquals(
        "An arbitrary string of characters whose format is unspecified"
            + " and interpreted based on the context of the property to which it applies.",
        PropertyType.STRING.getFormatDescription());
  }

  @Test
  public void testToString() {
    assertEquals("string", PropertyType.STRING.toString());
  }

  /**
   * This test checks the remainder of the methods in this class to ensure each property type has a
   * corresponding test
   */
  @Test
  public void testFullCoverage() {

    String typePrefix = "testType";
    Set<String> typesTested = Stream.of(this.getClass().getMethods()).map(Method::getName)
        .filter(m -> m.startsWith(typePrefix)).map(m -> m.substring(typePrefix.length()))
        .collect(Collectors.toSet());

    Set<String> types =
        Stream.of(PropertyType.values()).map(Enum<PropertyType>::name).collect(Collectors.toSet());

    assertEquals(types, typesTested, "Expected to see a test method for each property type");
  }

  private void valid(final String... args) {
    for (String s : args) {
      assertTrue(type.isValidFormat(s),
          s + " should be valid for " + PropertyType.class.getSimpleName() + "." + type.name());
    }
  }

  private void invalid(final String... args) {
    for (String s : args) {
      assertFalse(type.isValidFormat(s),
          s + " should be invalid for " + PropertyType.class.getSimpleName() + "." + type.name());
    }
  }

  @Test
  public void testTypeABSOLUTEPATH() {
    valid(null, "/foo", "/foo/c", "/", System.getProperty("user.dir"));
    // in Hadoop 2.x, Path only normalizes Windows paths properly when run on a Windows system
    // this makes the following checks fail
    if (System.getProperty("os.name").toLowerCase().contains("windows")) {
      valid("d:\\foo12", "c:\\foo\\g", "c:\\foo\\c", "c:\\");
    }
    invalid("foo12", "foo/g", "foo\\c");
  }

  @Test
  public void testTypeBOOLEAN() {
    valid(null, "True", "true", "False", "false", "tRUE", "fAlSe");
    invalid("foobar", "", "F", "T", "1", "0", "f", "t");
  }

  @Test
  public void testTypeCLASSNAME() {
    valid(null, "", String.class.getName(), String.class.getName() + "$1",
        String.class.getName() + "$TestClass");
    invalid("abc-def", "-", "!@#$%");
  }

  @Test
  public void testTypeCLASSNAMELIST() {
    testTypeCLASSNAME(); // test single class name
    valid(null, Joiner.on(",").join(String.class.getName(), String.class.getName() + "$1",
        String.class.getName() + "$TestClass"));
  }

  @Test
  public void testTypeCOUNT() {
    valid(null, "0", "1024", Long.toString(Integer.MAX_VALUE));
    invalid(Long.toString(Integer.MAX_VALUE + 1L), "-65535", "-1");
  }

  @Test
  public void testTypeDURABILITY() {
    valid(null, "none", "log", "flush", "sync");
    invalid("", "other");
  }

  @Test
  public void testTypeGC_POST_ACTION() {
    valid(null, "none", "flush", "compact");
    invalid("", "other");
  }

  @Test
  public void testTypeFRACTION() {
    valid(null, "1", "0", "1.0", "25%", "2.5%", "10.2E-3", "10.2E-3%", ".3");
    invalid("", "other", "20%%", "-0.3", "3.6a", "%25", "3%a");
  }

  @Test
  public void testTypeHOSTLIST() {
    valid(null, "localhost", "server1,server2,server3", "server1:1111,server2:3333",
        "localhost:1111", "server2:1111", "www.server", "www.server:1111", "www.server.com",
        "www.server.com:111");
    invalid(":111", "local host");
  }

  @Test
  public void testTypeBYTES() {
    valid(null, "1024", "20B", "100K", "1500M", "2G");
    invalid("1M500K", "1M 2K", "1MB", "1.5G", "1,024K", "", "a", "10%");
  }

  @Test
  public void testTypeMEMORY() {
    valid(null, "1024", "20B", "100K", "1500M", "2G", "10%");
    invalid("1M500K", "1M 2K", "1MB", "1.5G", "1,024K", "", "a");
  }

  @Test
  public void testTypePATH() {
    valid(null, "", "/absolute/path", "relative/path", "/with/trailing/slash/",
        "with/trailing/slash/");
  }

  @Test
  public void testTypePORT() {
    valid(null, "0", "1024", "30000", "65535");
    invalid("65536", "-65535", "-1", "1023");
  }

  @Test
  public void testTypePREFIX() {
    invalid(null, "", "whatever");
  }

  @Test
  public void testTypeSTRING() {
    valid(null, "", "whatever");
  }

  @Test
  public void testTypeTIMEDURATION() {
    valid(null, "600", "30s", "45m", "30000ms", "3d", "1h");
    invalid("1w", "1h30m", "1s 200ms", "ms", "", "a");
  }

  @Test
  public void testTypeURI() {
    valid(null, "", "hdfs://hostname", "file:///path/", "hdfs://example.com:port/path");
  }

}

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
import static org.junit.Assert.assertTrue;

import org.junit.Test;

public class PropertyTypeTest {
  @Test
  public void testToString() {
    assertEquals("string", PropertyType.STRING.toString());
  }

  @Test
  public void testGetFormatDescription() {
    assertEquals("An arbitrary string of characters whose format is unspecified and interpreted based on the context of the property to which it applies.",
        PropertyType.STRING.getFormatDescription());
  }

  private void typeCheckValidFormat(PropertyType type, String... args) {
    for (String s : args)
      assertTrue(s + " should be valid", type.isValidFormat(s));
  }

  private void typeCheckInvalidFormat(PropertyType type, String... args) {
    for (String s : args)
      assertFalse(s + " should be invalid", type.isValidFormat(s));
  }

  @Test
  public void testTypeFormats() {
    typeCheckValidFormat(PropertyType.TIMEDURATION, "600", "30s", "45m", "30000ms", "3d", "1h");
    typeCheckInvalidFormat(PropertyType.TIMEDURATION, "1w", "1h30m", "1s 200ms", "ms", "", "a");

    typeCheckValidFormat(PropertyType.MEMORY, "1024", "20B", "100K", "1500M", "2G");
    typeCheckInvalidFormat(PropertyType.MEMORY, "1M500K", "1M 2K", "1MB", "1.5G", "1,024K", "", "a");

    typeCheckValidFormat(PropertyType.HOSTLIST, "localhost", "server1,server2,server3", "server1:1111,server2:3333", "localhost:1111", "server2:1111",
        "www.server", "www.server:1111", "www.server.com", "www.server.com:111");
    typeCheckInvalidFormat(PropertyType.HOSTLIST, ":111", "local host");

    typeCheckValidFormat(PropertyType.ABSOLUTEPATH, "/foo", "/foo/c", "/");
    // in hadoop 2.0 Path only normalizes Windows paths properly when run on a Windows system
    // this makes the following checks fail
    if (System.getProperty("os.name").toLowerCase().contains("windows"))
      typeCheckValidFormat(PropertyType.ABSOLUTEPATH, "d:\\foo12", "c:\\foo\\g", "c:\\foo\\c", "c:\\");
    typeCheckValidFormat(PropertyType.ABSOLUTEPATH, System.getProperty("user.dir"));
    typeCheckInvalidFormat(PropertyType.ABSOLUTEPATH, "foo12", "foo/g", "foo\\c");
  }

  @Test
  public void testIsValidFormat_RegexAbsent() {
    // assertTrue(PropertyType.PREFIX.isValidFormat("whatever")); currently forbidden
    assertTrue(PropertyType.PREFIX.isValidFormat(null));
  }
}

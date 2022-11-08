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
package org.apache.accumulo.start.classloader.vfs;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.regex.Pattern;

import org.junit.jupiter.api.Test;

public class AccumuloClasspathTest {

  private static void assertPattern(String output, Pattern pattern, boolean shouldMatch) {
    if (shouldMatch) {
      assertTrue(pattern.matcher(output).matches(),
          "Pattern " + pattern + " did not match output: " + output);
    } else {
      assertFalse(pattern.matcher(output).matches(),
          "Pattern " + pattern + " should not match output: " + output);
    }
  }

  @Test
  public void basic() {
    var pattern = Pattern.compile("(?s).*\\s+.*\\n$");
    assertPattern(getClassPath(true), pattern, true);
    assertTrue(getClassPath(true).contains("app"));
    assertTrue(getClassPath(true).contains("Level"));

    assertTrue(getClassPath(true).length() > getClassPath(false).length());

    assertPattern(getClassPath(false), pattern, false);
    assertFalse(getClassPath(false).contains("app"));
    assertFalse(getClassPath(false).contains("Level"));
  }

  @SuppressWarnings("deprecation")
  private String getClassPath(boolean b) {
    return org.apache.accumulo.start.classloader.vfs.AccumuloVFSClassLoader.getClassPath(b);
  }
}

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
package org.apache.accumulo.start.classloader.vfs;

import static org.apache.accumulo.start.classloader.vfs.AccumuloVFSClassLoader.getClassPath;

import org.junit.Assert;
import org.junit.Test;

public class AccumuloClasspathTest {

  private static void assertPattern(String output, String pattern, boolean shouldMatch) {
    if (shouldMatch) {
      Assert.assertTrue("Pattern " + pattern + " did not match output: " + output, output.matches(pattern));
    } else {
      Assert.assertFalse("Pattern " + pattern + " should not match output: " + output, output.matches(pattern));
    }
  }

  @Test
  public void basic() {
    assertPattern(getClassPath(true), "(?s).*\\s+.*\\n$", true);
    Assert.assertTrue(getClassPath(true).contains("Java System Classloader"));
    Assert.assertTrue(getClassPath(true).contains("Level"));

    Assert.assertTrue(getClassPath(true).length() > getClassPath(false).length());

    assertPattern(getClassPath(false), "(?s).*\\s+.*\\n$", false);
    Assert.assertFalse(getClassPath(false).contains("Java System Classloader"));
    Assert.assertFalse(getClassPath(false).contains("Level"));
  }
}

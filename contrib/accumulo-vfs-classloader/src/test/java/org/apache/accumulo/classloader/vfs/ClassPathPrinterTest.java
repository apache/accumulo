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
package org.apache.accumulo.classloader.vfs;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.net.MalformedURLException;

import org.apache.accumulo.classloader.ClassPathPrinter;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

@SuppressFBWarnings(value = "PATH_TRAVERSAL_IN", justification = "paths not set by user input")
public class ClassPathPrinterTest {

  @Rule
  public TemporaryFolder folder1 =
      new TemporaryFolder(new File(System.getProperty("user.dir") + "/target"));

  private final ClassLoader parent = ClassPathPrinterTest.class.getClassLoader();

  private static void assertPattern(String output, String pattern, boolean shouldMatch) {
    if (shouldMatch) {
      assertTrue("Pattern " + pattern + " did not match output: " + output,
          output.matches(pattern));
    } else {
      assertFalse("Pattern " + pattern + " should not match output: " + output,
          output.matches(pattern));
    }
  }

  @Test
  public void testPrintClassPath() throws Exception {
    File conf = folder1.newFile("accumulo.properties");

    ReloadingVFSClassLoader cl = new ReloadingVFSClassLoader(parent) {
      @Override
      protected String getClassPath() {
        try {
          return conf.toURI().toURL().toString();
        } catch (MalformedURLException e) {
          throw new RuntimeException("URL problem", e);
        }
      }
    };
    assertPattern(ClassPathPrinter.getClassPath(cl, true), "(?s).*\\s+.*\\n$", true);
    assertTrue(ClassPathPrinter.getClassPath(cl, true)
        .contains("Level 3: ReloadingVFSClassLoader Classloader"));
    assertTrue(ClassPathPrinter.getClassPath(cl, true).length()
        > ClassPathPrinter.getClassPath(cl, false).length());
    assertPattern(ClassPathPrinter.getClassPath(cl, false), "(?s).*\\s+.*\\n$", false);
    assertFalse(ClassPathPrinter.getClassPath(cl, false)
        .contains("Level 3: ReloadingVFSClassLoader Classloader"));
  }
}

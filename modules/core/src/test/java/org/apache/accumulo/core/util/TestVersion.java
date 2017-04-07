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
package org.apache.accumulo.core.util;

import junit.framework.TestCase;

public class TestVersion extends TestCase {
  Version make(String version) {
    return new Version(version);
  }

  public void testOne() {
    Version v;

    v = make("abc-1.2.3-ugly");
    assertTrue(v != null);
    assertTrue(v.getPackage().equals("abc"));
    assertTrue(v.getMajorVersion() == 1);
    assertTrue(v.getMinorVersion() == 2);
    assertTrue(v.getReleaseVersion() == 3);
    assertTrue(v.getEtcetera().equals("ugly"));

    v = make("3.2.1");
    assertTrue(v.getPackage() == null);
    assertTrue(v.getMajorVersion() == 3);
    assertTrue(v.getMinorVersion() == 2);
    assertTrue(v.getReleaseVersion() == 1);
    assertTrue(v.getEtcetera() == null);

    v = make("55");
    assertTrue(v.getPackage() == null);
    assertTrue(v.getMajorVersion() == 55);
    assertTrue(v.getMinorVersion() == 0);
    assertTrue(v.getReleaseVersion() == 0);
    assertTrue(v.getEtcetera() == null);

    v = make("7.1-beta");
    assertTrue(v.getPackage() == null);
    assertTrue(v.getMajorVersion() == 7);
    assertTrue(v.getMinorVersion() == 1);
    assertTrue(v.getReleaseVersion() == 0);
    assertTrue(v.getEtcetera().equals("beta"));

    try {
      make("beta");
      fail("Should have thrown an error");
    } catch (IllegalArgumentException t) {}
  }

}

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
import static org.junit.Assert.assertTrue;

import org.junit.Test;

public class AccumuloConfigurationTest {

  @Test
  public void testGetMemoryInBytes() throws Exception {
    assertEquals(42l, AccumuloConfiguration.getMemoryInBytes("42"));
    assertEquals(42l, AccumuloConfiguration.getMemoryInBytes("42b"));
    assertEquals(42l, AccumuloConfiguration.getMemoryInBytes("42B"));
    assertEquals(42l * 1024l, AccumuloConfiguration.getMemoryInBytes("42K"));
    assertEquals(42l * 1024l, AccumuloConfiguration.getMemoryInBytes("42k"));
    assertEquals(42l * 1024l * 1024l, AccumuloConfiguration.getMemoryInBytes("42M"));
    assertEquals(42l * 1024l * 1024l, AccumuloConfiguration.getMemoryInBytes("42m"));
    assertEquals(42l * 1024l * 1024l * 1024l, AccumuloConfiguration.getMemoryInBytes("42G"));
    assertEquals(42l * 1024l * 1024l * 1024l, AccumuloConfiguration.getMemoryInBytes("42g"));

  }

  @Test
  public void testGetTimeInMillis() {
    assertEquals(42L * 24 * 60 * 60 * 1000, AccumuloConfiguration.getTimeInMillis("42d"));
    assertEquals(42L * 60 * 60 * 1000, AccumuloConfiguration.getTimeInMillis("42h"));
    assertEquals(42L * 60 * 1000, AccumuloConfiguration.getTimeInMillis("42m"));
    assertEquals(42L * 1000, AccumuloConfiguration.getTimeInMillis("42s"));
    assertEquals(42L * 1000, AccumuloConfiguration.getTimeInMillis("42"));
    assertEquals(42L, AccumuloConfiguration.getTimeInMillis("42ms"));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testGetMemoryInBytesFailureCases1() throws Exception {
    AccumuloConfiguration.getMemoryInBytes("42x");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testGetMemoryInBytesFailureCases2() throws Exception {
    AccumuloConfiguration.getMemoryInBytes("FooBar");
  }

  @Test
  public void testGetPropertyByString() {
    AccumuloConfiguration c = AccumuloConfiguration.getDefaultConfiguration();
    boolean found = false;
    for (Property p : Property.values()) {
      if (p.getType() != PropertyType.PREFIX) {
        found = true;
        // ensure checking by property and by key works the same
        assertEquals(c.get(p), c.get(p.getKey()));
        // ensure that getting by key returns the expected value
        assertEquals(p.getDefaultValue(), c.get(p.getKey()));
      }
    }
    assertTrue("test was a dud, and did nothing", found);
  }
}

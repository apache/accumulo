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

import java.util.Map;

import org.junit.Before;
import org.junit.Test;

public class DefaultConfigurationTest {
  private DefaultConfiguration c;

  @Before
  public void setUp() {
    c = DefaultConfiguration.getInstance();
  }

  @Test
  public void testGet() {
    assertEquals(Property.MASTER_CLIENTPORT.getDefaultValue(), c.get(Property.MASTER_CLIENTPORT));
  }

  @Test
  public void testGetProperties() {
    Map<String,String> p = new java.util.HashMap<>();
    c.getProperties(p, x -> true);
    assertEquals(Property.MASTER_CLIENTPORT.getDefaultValue(), p.get(Property.MASTER_CLIENTPORT.getKey()));
    assertFalse(p.containsKey(Property.MASTER_PREFIX.getKey()));
    assertTrue(p.containsKey(Property.TSERV_DEFAULT_BLOCKSIZE.getKey()));
  }

  @Test
  public void testSanityCheck() {
    ConfigSanityCheck.validate(c);
  }

  @Test
  public void testDefaultBlockSizeIsAllowed() {
    long bsi = c.getAsBytes(Property.TABLE_FILE_COMPRESSED_BLOCK_SIZE_INDEX);
    assert (bsi > 0);
    assert (bsi < Integer.MAX_VALUE);
  }
}

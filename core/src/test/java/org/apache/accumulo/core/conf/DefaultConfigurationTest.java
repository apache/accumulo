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

import java.util.Map;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class DefaultConfigurationTest {
  private DefaultConfiguration c;

  @BeforeEach
  public void setUp() {
    c = DefaultConfiguration.getInstance();
  }

  @Test
  public void testGet() {
    assertEquals(Property.MANAGER_CLIENTPORT.getDefaultValue(), c.get(Property.MANAGER_CLIENTPORT));
  }

  @Test
  public void testGetProperties() {
    Map<String,String> p = new java.util.HashMap<>();
    c.getProperties(p, x -> true);
    assertEquals(Property.MANAGER_CLIENTPORT.getDefaultValue(),
        p.get(Property.MANAGER_CLIENTPORT.getKey()));
    assertFalse(p.containsKey(Property.MANAGER_PREFIX.getKey()));
    assertTrue(p.containsKey(Property.TSERV_DEFAULT_BLOCKSIZE.getKey()));
  }

  @Test
  public void testSanityCheck() {
    ConfigCheckUtil.validate(c, "test");
  }
}

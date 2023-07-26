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
package org.apache.accumulo.core.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.Map;

import org.apache.accumulo.core.conf.ConfigurationCopy;
import org.junit.jupiter.api.Test;

public class ConfigurationImplTest {
  @Test
  public void testCustomProps() {
    ConfigurationCopy conf = new ConfigurationCopy();
    conf.set("table.custom.p1", "v1");
    conf.set("table.custom.p2", "v2");
    conf.set("general.custom.p3", "v3");
    conf.set("general.custom.p4", "v4");

    var confImp = new ConfigurationImpl(conf);

    assertEquals(Map.of("p3", "v3", "p4", "v4"), confImp.getCustom());
    assertEquals(Map.of("p1", "v1", "p2", "v2"), confImp.getTableCustom());

    assertEquals("v3", confImp.getCustom("p3"));
    assertEquals("v1", confImp.getTableCustom("p1"));

    assertNull(confImp.getCustom("p1"));
    assertNull(confImp.getTableCustom("p3"));

    // ensure changes to custom props are seen
    conf.set("general.custom.p4", "v5");
    conf.set("table.custom.p2", "v6");
    conf.set("table.custom.p5", "v7");

    assertEquals(Map.of("p3", "v3", "p4", "v5"), confImp.getCustom());
    assertEquals(Map.of("p1", "v1", "p2", "v6", "p5", "v7"), confImp.getTableCustom());

    assertEquals("v3", confImp.getCustom("p3"));
    assertEquals("v5", confImp.getCustom("p4"));
    assertEquals("v1", confImp.getTableCustom("p1"));
    assertEquals("v6", confImp.getTableCustom("p2"));

    assertNull(confImp.getCustom("p5"));
    assertNull(confImp.getTableCustom("p4"));
  }
}

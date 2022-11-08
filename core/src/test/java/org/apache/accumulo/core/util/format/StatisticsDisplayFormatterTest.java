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
package org.apache.accumulo.core.util.format;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Map;
import java.util.TreeMap;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class StatisticsDisplayFormatterTest {
  StatisticsDisplayFormatter formatter;

  Map<Key,Value> data;

  @BeforeEach
  public void setUp() {
    data = new TreeMap<>();
    formatter = new StatisticsDisplayFormatter();
  }

  @Test
  public void testInitialize() {
    data.put(new Key(), new Value());
    formatter.initialize(data.entrySet(), new FormatterConfig());

    assertTrue(formatter.hasNext());
  }

  @Test
  public void testAggregate() {
    data.put(new Key("", "", "", 1), new Value());
    data.put(new Key("", "", "", 2), new Value());
    formatter.initialize(data.entrySet(), new FormatterConfig());

    String[] output = formatter.next().split("\n");
    assertTrue(output[2].endsWith(": 1"));
    assertTrue(output[5].endsWith(": 1"));
    assertTrue(output[8].endsWith(": 1"));
    assertEquals("2 entries matched.", output[9]);

    assertFalse(formatter.hasNext());
  }
}

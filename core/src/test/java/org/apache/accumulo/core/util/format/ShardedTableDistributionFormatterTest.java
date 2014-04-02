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
package org.apache.accumulo.core.util.format;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.TreeMap;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.junit.Before;
import org.junit.Test;

public class ShardedTableDistributionFormatterTest {
  ShardedTableDistributionFormatter formatter;

  Map<Key,Value> data;

  @Before
  public void setUp() {
    data = new TreeMap<Key,Value>();
    formatter = new ShardedTableDistributionFormatter();
  }

  @Test
  public void testInitialize() {
    data.put(new Key(), new Value());
    data.put(new Key("r", "~tab"), new Value());
    formatter.initialize(data.entrySet(), false);

    assertTrue(formatter.hasNext());
    formatter.next();
    assertFalse(formatter.hasNext());
  }

  @Test
  public void testAggregate() {
    data.put(new Key("t", "~tab", "loc"), new Value("srv1".getBytes(StandardCharsets.UTF_8)));
    data.put(new Key("t;19700101", "~tab", "loc", 0), new Value("srv1".getBytes(StandardCharsets.UTF_8)));
    data.put(new Key("t;19700101", "~tab", "loc", 1), new Value("srv2".getBytes(StandardCharsets.UTF_8)));

    formatter.initialize(data.entrySet(), false);

    String[] result = formatter.next().split("\n");
    assertTrue(result[2].endsWith("\t1"));
    assertTrue(result[3].endsWith("\t2"));
    assertFalse(formatter.hasNext());
  }
}

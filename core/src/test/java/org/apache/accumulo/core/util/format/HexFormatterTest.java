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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Map;
import java.util.TreeMap;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.Text;
import org.junit.Before;
import org.junit.Test;

public class HexFormatterTest {
  HexFormatter formatter;

  Map<Key,Value> data;

  @Before
  public void setUp() {
    data = new TreeMap<Key,Value>();
    formatter = new HexFormatter();
  }

  @Test
  public void testInitialize() {
    data.put(new Key(), new Value());
    formatter.initialize(data.entrySet(), false);

    assertTrue(formatter.hasNext());
    assertEquals("  " + "  " + " [" + "] ", formatter.next());
  }

  @Test
  public void testInterpretRow() {
    assertEquals(new Text(), formatter.interpretRow(new Text()));
    assertEquals(new Text("\0"), formatter.interpretRow(new Text("0")));
  }

  @Test
  public void testRoundTripRows() {
    Text bytes = new Text(new byte[] {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15});
    data.put(new Key(bytes), new Value());

    formatter.initialize(data.entrySet(), false);

    String row = formatter.next().split(" ")[0];
    assertEquals("0001-0203-0405-0607-0809-0a0b-0c0d-0e0f", row);
    assertEquals(bytes, formatter.interpretRow(new Text(row)));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInterpretBadRow0() {
    formatter.interpretRow(new Text("!"));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInterpretBadRow1() {
    formatter.interpretRow(new Text("z"));
  }

  @Test
  public void testTimestamps() {
    long now = System.currentTimeMillis();
    data.put(new Key("", "", "", now), new Value());
    formatter.initialize(data.entrySet(), true);
    String entry = formatter.next().split("\\s+")[2];
    assertEquals(now, Long.valueOf(entry).longValue());
  }
}

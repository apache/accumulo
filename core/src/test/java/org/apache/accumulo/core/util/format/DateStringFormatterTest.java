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
import java.util.TimeZone;
import java.util.TreeMap;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.junit.Before;
import org.junit.Test;

public class DateStringFormatterTest {
  DateStringFormatter formatter;

  Map<Key,Value> data;

  @Before
  public void setUp() {
    formatter = new DateStringFormatter();
    data = new TreeMap<Key,Value>();
    data.put(new Key("", "", "", 0), new Value());
  }

  @Test
  public void testTimestamps() {
    formatter.initialize(data.entrySet(), true);
    formatter.setTimeZone(TimeZone.getTimeZone("UTC"));

    assertTrue(formatter.hasNext());
    assertTrue(formatter.next().endsWith("1970/01/01 00:00:00.000"));
  }

  @Test
  public void testNoTimestamps() {
    data.put(new Key("", "", "", 1), new Value());

    assertEquals(2, data.size());

    formatter.initialize(data.entrySet(), false);

    assertEquals(formatter.next(), formatter.next());
  }

}

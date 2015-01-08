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
package org.apache.accumulo.core.iterators.system;

import java.util.HashSet;
import java.util.TreeMap;

import junit.framework.TestCase;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.SortedMapIterator;

public class TimeSettingIteratorTest extends TestCase {

  public void test1() throws Exception {
    TreeMap<Key,Value> tm1 = new TreeMap<Key,Value>();

    tm1.put(new Key("r0", "cf1", "cq1", 9l), new Value("v0".getBytes()));
    tm1.put(new Key("r1", "cf1", "cq1", Long.MAX_VALUE), new Value("v1".getBytes()));
    tm1.put(new Key("r1", "cf1", "cq1", 90l), new Value("v2".getBytes()));
    tm1.put(new Key("r1", "cf1", "cq1", 0l), new Value("v3".getBytes()));
    tm1.put(new Key("r2", "cf1", "cq1", 6l), new Value("v4".getBytes()));

    TimeSettingIterator tsi = new TimeSettingIterator(new SortedMapIterator(tm1), 50);

    tsi.seek(new Range(new Key("r1", "cf1", "cq1", 50l), true, new Key("r1", "cf1", "cq1", 50l), true), new HashSet<ByteSequence>(), false);

    assertTrue(tsi.hasTop());
    assertEquals(new Key("r1", "cf1", "cq1", 50l), tsi.getTopKey());
    assertEquals("v1", tsi.getTopValue().toString());
    tsi.next();

    assertTrue(tsi.hasTop());
    assertEquals(new Key("r1", "cf1", "cq1", 50l), tsi.getTopKey());
    assertEquals("v2", tsi.getTopValue().toString());
    tsi.next();

    assertTrue(tsi.hasTop());
    assertEquals(new Key("r1", "cf1", "cq1", 50l), tsi.getTopKey());
    assertEquals("v3", tsi.getTopValue().toString());
    tsi.next();

    assertFalse(tsi.hasTop());

    tsi.seek(new Range(new Key("r1", "cf1", "cq1", 50l), false, null, true), new HashSet<ByteSequence>(), false);

    assertTrue(tsi.hasTop());
    assertEquals(new Key("r2", "cf1", "cq1", 50l), tsi.getTopKey());
    assertEquals("v4", tsi.getTopValue().toString());
    tsi.next();

    assertFalse(tsi.hasTop());

    tsi.seek(new Range(null, true, new Key("r1", "cf1", "cq1", 50l), false), new HashSet<ByteSequence>(), false);

    assertTrue(tsi.hasTop());
    assertEquals(new Key("r0", "cf1", "cq1", 50l), tsi.getTopKey());
    assertEquals("v0", tsi.getTopValue().toString());
    tsi.next();

    assertFalse(tsi.hasTop());

    tsi.seek(new Range(new Key("r1", "cf1", "cq1", 51l), true, new Key("r1", "cf1", "cq1", 50l), false), new HashSet<ByteSequence>(), false);
    assertFalse(tsi.hasTop());
  }

}

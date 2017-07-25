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

import java.io.IOException;
import java.util.HashSet;
import java.util.TreeMap;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.SortedMapIterator;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import junit.framework.TestCase;

public class VisibilityFilterTest extends TestCase {

  public void testBadVisibility() throws IOException {
    TreeMap<Key,Value> tm = new TreeMap<>();

    tm.put(new Key("r1", "cf1", "cq1", "A&"), new Value(new byte[0]));
    SortedKeyValueIterator<Key,Value> filter = VisibilityFilter.wrap(new SortedMapIterator(tm), new Authorizations("A"), "".getBytes());

    // suppress logging
    Level prevLevel = Logger.getLogger(VisibilityFilter.class).getLevel();
    Logger.getLogger(VisibilityFilter.class).setLevel(Level.FATAL);

    filter.seek(new Range(), new HashSet<ByteSequence>(), false);
    assertFalse(filter.hasTop());

    Logger.getLogger(VisibilityFilter.class).setLevel(prevLevel);
  }

  public void testEmptyAuths() throws IOException {
    TreeMap<Key,Value> tm = new TreeMap<>();

    tm.put(new Key("r1", "cf1", "cq1", ""), new Value(new byte[0]));
    tm.put(new Key("r1", "cf1", "cq2", "C"), new Value(new byte[0]));
    tm.put(new Key("r1", "cf1", "cq3", ""), new Value(new byte[0]));
    SortedKeyValueIterator<Key,Value> filter = VisibilityFilter.wrap(new SortedMapIterator(tm), Authorizations.EMPTY, "".getBytes());

    filter.seek(new Range(), new HashSet<ByteSequence>(), false);
    assertTrue(filter.hasTop());
    assertEquals(new Key("r1", "cf1", "cq1", ""), filter.getTopKey());
    filter.next();
    assertTrue(filter.hasTop());
    assertEquals(new Key("r1", "cf1", "cq3", ""), filter.getTopKey());
    filter.next();
    assertFalse(filter.hasTop());
  }
}

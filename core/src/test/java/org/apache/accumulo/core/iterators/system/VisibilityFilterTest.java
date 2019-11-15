/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.accumulo.core.iterators.system;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.HashSet;
import java.util.TreeMap;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.SortedMapIterator;
import org.apache.accumulo.core.iteratorsImpl.system.VisibilityFilter;
import org.apache.accumulo.core.security.Authorizations;
import org.junit.Test;

public class VisibilityFilterTest {

  @Test
  public void testBadVisibility() throws IOException {
    TreeMap<Key,Value> tm = new TreeMap<>();

    tm.put(new Key("r1", "cf1", "cq1", "A&"), new Value(new byte[0]));
    SortedKeyValueIterator<Key,Value> filter =
        VisibilityFilter.wrap(new SortedMapIterator(tm), new Authorizations("A"), "".getBytes());

    filter.seek(new Range(), new HashSet<>(), false);
    assertFalse(filter.hasTop());

  }

  @Test
  public void testEmptyAuths() throws IOException {
    TreeMap<Key,Value> tm = new TreeMap<>();

    tm.put(new Key("r1", "cf1", "cq1", ""), new Value(new byte[0]));
    tm.put(new Key("r1", "cf1", "cq2", "C"), new Value(new byte[0]));
    tm.put(new Key("r1", "cf1", "cq3", ""), new Value(new byte[0]));
    SortedKeyValueIterator<Key,Value> filter =
        VisibilityFilter.wrap(new SortedMapIterator(tm), Authorizations.EMPTY, "".getBytes());

    filter.seek(new Range(), new HashSet<>(), false);
    assertTrue(filter.hasTop());
    assertEquals(new Key("r1", "cf1", "cq1", ""), filter.getTopKey());
    filter.next();
    assertTrue(filter.hasTop());
    assertEquals(new Key("r1", "cf1", "cq3", ""), filter.getTopKey());
    filter.next();
    assertFalse(filter.hasTop());
  }
}

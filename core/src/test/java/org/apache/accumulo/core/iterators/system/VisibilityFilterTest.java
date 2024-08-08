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
package org.apache.accumulo.core.iterators.system;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iteratorsImpl.system.SortedMapIterator;
import org.apache.accumulo.core.iteratorsImpl.system.VisibilityFilter;
import org.apache.accumulo.core.security.Authorizations;
import org.junit.jupiter.api.Test;

public class VisibilityFilterTest {

  @Test
  public void testBadVisibility() throws IOException {
    TreeMap<Key,Value> tm = new TreeMap<>();

    tm.put(new Key("r1", "cf1", "cq1", "A&"), new Value());
    SortedKeyValueIterator<Key,Value> filter = VisibilityFilter.wrap(new SortedMapIterator(tm),
        new Authorizations("A"), "".getBytes(UTF_8));

    filter.seek(new Range(), new HashSet<>(), false);
    assertFalse(filter.hasTop());

  }

  @Test
  public void testEmptyAuths() throws IOException {
    TreeMap<Key,Value> tm = new TreeMap<>();

    tm.put(new Key("r1", "cf1", "cq1", ""), new Value());
    tm.put(new Key("r1", "cf1", "cq2", "C"), new Value());
    tm.put(new Key("r1", "cf1", "cq3", ""), new Value());
    tm.put(new Key("r2", "cf1", "cq2", "C"), new Value());
    SortedKeyValueIterator<Key,Value> filter =
        VisibilityFilter.wrap(new SortedMapIterator(tm), Authorizations.EMPTY, "".getBytes(UTF_8));

    TreeSet<Key> expected = new TreeSet<>();
    expected.add(new Key("r1", "cf1", "cq1", ""));
    expected.add(new Key("r1", "cf1", "cq3", ""));

    verify(expected, filter);
  }

  @Test
  public void testAuths() throws IOException {
    TreeMap<Key,Value> tm = new TreeMap<>();

    // want to have repeated col vis in order to exercise cache in test
    tm.put(new Key("r1", "cf1", "cq1", "A&B"), new Value());
    tm.put(new Key("r1", "cf1", "cq2", "A|C"), new Value());
    tm.put(new Key("r1", "cf1", "cq3", "A&B"), new Value());
    tm.put(new Key("r2", "cf1", "cq2", "A|C"), new Value());
    tm.put(new Key("r2", "cf1", "cq3", ""), new Value());
    tm.put(new Key("r2", "cf1", "cq2", "C|D"), new Value());
    tm.put(new Key("r3", "cf1", "cq2", "C|(A&D)"), new Value());
    tm.put(new Key("r4", "cf1", "cq2", "C|D"), new Value());
    tm.put(new Key("r5", "cf1", "cq2", "A&B"), new Value());
    tm.put(new Key("r5", "cf1", "cq3", ""), new Value());
    tm.put(new Key("r6", "cf1", "cq2", "C|(A&D)"), new Value());

    SortedKeyValueIterator<Key,Value> filter = VisibilityFilter.wrap(new SortedMapIterator(tm),
        new Authorizations("A", "B"), "".getBytes(UTF_8));

    TreeSet<Key> expected = new TreeSet<>();
    expected.add(new Key("r1", "cf1", "cq1", "A&B"));
    expected.add(new Key("r1", "cf1", "cq2", "A|C"));
    expected.add(new Key("r1", "cf1", "cq3", "A&B"));
    expected.add(new Key("r2", "cf1", "cq2", "A|C"));
    expected.add(new Key("r5", "cf1", "cq2", "A&B"));
    expected.add(new Key("r2", "cf1", "cq3", ""));
    expected.add(new Key("r5", "cf1", "cq3", ""));

    verify(expected, filter);
  }

  private static void verify(TreeSet<Key> expected, SortedKeyValueIterator<Key,Value> iter)
      throws IOException {
    for (var filter : List.of(iter, iter.deepCopy(null))) {
      filter.seek(new Range(), Set.of(), false);
      var eiter = expected.iterator();
      while (eiter.hasNext() && filter.hasTop()) {
        Key ekey = eiter.next();
        assertEquals(ekey, filter.getTopKey());
        filter.next();
      }

      assertFalse(filter.hasTop());
      assertFalse(eiter.hasNext());
    }
  }

  @Test
  public void testDefaultVisibility() throws IOException {
    // Test non empty default visibility
    var defaultVis = "A&B&C".getBytes(UTF_8);

    TreeMap<Key,Value> tm = new TreeMap<>();

    tm.put(new Key("r1", "cf1", "cq1", "A&B"), new Value());
    tm.put(new Key("r1", "cf1", "cq2", ""), new Value());
    tm.put(new Key("r1", "cf1", "cq3", "A&B"), new Value());
    tm.put(new Key("r1", "cf1", "cq4", ""), new Value());
    // add something that has the same col vis as the defaultVis
    tm.put(new Key("r1", "cf1", "cq5", "A&B&C"), new Value());
    tm.put(new Key("r1", "cf1", "cq6", ""), new Value());
    tm.put(new Key("r1", "cf1", "cq7", "A&B&C"), new Value());

    // with the set of auths [A,B] the default visibility is not visible
    SortedKeyValueIterator<Key,Value> filter =
        VisibilityFilter.wrap(new SortedMapIterator(tm), new Authorizations("A", "B"), defaultVis);

    TreeSet<Key> expected = new TreeSet<>();
    expected.add(new Key("r1", "cf1", "cq1", "A&B"));
    expected.add(new Key("r1", "cf1", "cq3", "A&B"));

    verify(expected, filter);

    // with the set of auths [A.B.C] should be able to see all data
    filter = VisibilityFilter.wrap(new SortedMapIterator(tm), new Authorizations("A", "B", "C"),
        defaultVis);
    filter.seek(new Range(), Set.of(), false);

    verify(new TreeSet<>(tm.keySet()), filter);
  }

}

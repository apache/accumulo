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
package org.apache.accumulo.core.client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.TreeMap;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.junit.jupiter.api.Test;

public class RowIteratorTest {

  Iterator<Entry<Key,Value>> makeIterator(final String... args) {
    final Map<Key,Value> result = new TreeMap<>();
    for (String s : args) {
      final String[] parts = s.split("[ \t]");
      final Key key = new Key(parts[0], parts[1], parts[2]);
      final Value value = new Value(parts[3]);
      result.put(key, value);
    }
    return result.entrySet().iterator();
  }

  List<List<Entry<Key,Value>>> getRows(final Iterator<Entry<Key,Value>> iter) {
    final List<List<Entry<Key,Value>>> result = new ArrayList<>();
    final RowIterator riter = new RowIterator(iter);
    while (riter.hasNext()) {
      final Iterator<Entry<Key,Value>> row = riter.next();
      final List<Entry<Key,Value>> rlist = new ArrayList<>();
      while (row.hasNext()) {
        rlist.add(row.next());
      }
      result.add(rlist);
    }
    return result;
  }

  @Test
  public void testRowIterator() {
    List<List<Entry<Key,Value>>> rows = getRows(makeIterator());
    assertEquals(0, rows.size());
    rows = getRows(makeIterator("a b c d"));
    assertEquals(1, rows.size());
    assertEquals(1, rows.get(0).size());
    rows = getRows(makeIterator("a cf cq1 v", "a cf cq2 v", "a cf cq3 v", "b cf cq1 x"));
    assertEquals(2, rows.size());
    assertEquals(3, rows.get(0).size());
    assertEquals(1, rows.get(1).size());

    RowIterator i = new RowIterator(makeIterator());
    assertThrows(NoSuchElementException.class, i::next);

    i = new RowIterator(makeIterator("a b c d", "a 1 2 3"));
    assertTrue(i.hasNext());
    Iterator<Entry<Key,Value>> row = i.next();
    assertTrue(row.hasNext());
    row.next();
    assertTrue(row.hasNext());
    row.next();
    assertFalse(row.hasNext());
    assertThrows(NoSuchElementException.class, row::next);
    assertEquals(0, i.getKVCount());
    assertFalse(i.hasNext());
    assertEquals(2, i.getKVCount());
    assertThrows(NoSuchElementException.class, i::next);
  }

  @Test
  public void testUnreadRow() {
    final RowIterator i = new RowIterator(makeIterator("a b c d", "a 1 2 3", "b 1 2 3"));
    assertTrue(i.hasNext());
    Iterator<Entry<Key,Value>> firstRow = i.next();
    assertEquals(0, i.getKVCount());
    assertTrue(i.hasNext());
    assertEquals(2, i.getKVCount());
    Iterator<Entry<Key,Value>> nextRow = i.next();
    assertEquals(2, i.getKVCount());
    assertFalse(i.hasNext());
    assertEquals(3, i.getKVCount());
    assertThrows(IllegalStateException.class, firstRow::hasNext);
    assertThrows(IllegalStateException.class, nextRow::next);
  }
}

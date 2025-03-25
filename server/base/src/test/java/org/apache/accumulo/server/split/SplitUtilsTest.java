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
package org.apache.accumulo.server.split;

import static java.util.stream.Collectors.toCollection;
import static java.util.stream.Collectors.toList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iteratorsImpl.system.SortedMapIterator;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.Test;

import com.google.common.collect.Iterators;

public class SplitUtilsTest {

  private static Key newKey(int i) {
    return new Key(String.format("%04d", i));
  }

  private static SortedSet<Text> newRowsSet(int... rows) {
    return newRowsSet(IntStream.of(rows));
  }

  private static SortedSet<Text> newRowsSet(Function<Integer,Integer> prevFunc, int... rows) {
    return newRowsSet(IntStream.of(rows), prevFunc);
  }

  private static SortedSet<Text> newRowsSet(IntStream intStream) {
    return newRowsSet(intStream, i -> i - 1);
  }

  private static SortedSet<Text> newRowsSet(IntStream intStream,
      Function<Integer,Integer> prevFunc) {
    var iter = intStream.iterator();

    TreeSet<Text> rows = new TreeSet<>();

    while (iter.hasNext()) {
      var i = iter.next();
      var s = String.format("%04d", i);

      if (prevFunc.apply(i) != null) {
        String prev = String.format("%04d", prevFunc.apply(i));

        int common = 0;
        for (; common < s.length(); common++) {
          if (prev.charAt(common) != s.charAt(common)) {
            break;
          }
        }

        if (common + 1 < s.length()) {
          s = s.substring(0, common + 1);
        }
      }

      rows.add(new Text(s));
    }

    return rows;
  }

  private static SortedSet<Text> newRowSet(String... rows) {
    return Stream.of(rows).map(Text::new).collect(toCollection(TreeSet::new));
  }

  private static Iterable<Key> newIndexIterable(IntStream intStream, Integer endRow,
      Integer prevEndRow) {
    return newIndexIterable(intStream.mapToObj(i -> String.format("%04d", i)),
        endRow == null ? null : String.format("%04d", endRow),
        prevEndRow == null ? null : String.format("%04d", prevEndRow));
  }

  private static Iterable<Key> newIndexIterable(Stream<String> stream, String endRow,
      String prevEndRow) {
    TreeMap<Key,Value> data = new TreeMap<>();

    stream.forEach(row -> data.put(new Key(row), new Value("")));
    Text er = endRow == null ? null : new Text(endRow);
    Text pr = prevEndRow == null ? null : new Text(prevEndRow);

    return () -> {
      var accumuloIter = new SortedMapIterator(data);
      try {
        accumuloIter.seek(new Range(), Set.of(), false);
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
      return new SplitUtils.IndexIterator(accumuloIter, er, pr);
    };
  }

  public static SortedSet<Text> findSplits(Iterable<Key> tabletIndexIterator, int desiredSplits) {
    return SplitUtils.findSplits(tabletIndexIterator, desiredSplits, sc -> true);
  }

  @Test
  public void testFindSplits() {
    List<Key> keys = IntStream.range(1, 101).mapToObj(SplitUtilsTest::newKey).collect(toList());
    assertEquals(newRowsSet(50), findSplits(keys, 1));
    assertEquals(newRowsSet(33, 67), findSplits(keys, 2));
    assertEquals(newRowsSet(25, 50, 75), findSplits(keys, 3));
    assertEquals(newRowsSet(20, 40, 60, 80), findSplits(keys, 4));
    assertEquals(newRowsSet(17, 33, 50, 67, 83), findSplits(keys, 5));
    assertEquals(newRowsSet(14, 29, 43, 57, 71, 86), findSplits(keys, 6));
    assertEquals(newRowsSet(13, 25, 38, 50, 63, 75, 88), findSplits(keys, 7));
    assertEquals(newRowsSet(IntStream.range(1, 10).map(i -> i * 10)), findSplits(keys, 9));
    assertEquals(newRowsSet(IntStream.range(1, 20).map(i -> i * 5)), findSplits(keys, 19));
    assertEquals(newRowsSet(IntStream.range(1, 50).map(i -> i * 2)), findSplits(keys, 49));
    assertEquals(newRowsSet(IntStream.range(1, 100)), findSplits(keys, 99));
    assertEquals(newRowsSet(IntStream.range(1, 101)), findSplits(keys, 100));
    assertEquals(newRowsSet(IntStream.range(1, 101)), findSplits(keys, 1000));
  }

  @Test
  public void testFindSplitsFewRows() {
    List<Key> keys = IntStream.range(1, 101).map(i -> i / 10 * 10).mapToObj(SplitUtilsTest::newKey)
        .collect(toList());

    assertEquals(newRowsSet(50), findSplits(keys, 1));
    assertEquals(newRowsSet(i -> i - 10, 30, 60), findSplits(keys, 2));
    assertEquals(newRowsSet(i -> i - 10, 20, 50, 70), findSplits(keys, 3));
    assertEquals(newRowsSet(IntStream.range(1, 10).map(i -> i * 10), i -> i - 10),
        findSplits(keys, 9));
    assertEquals(newRowsSet(IntStream.range(0, 11).map(i -> i * 10), i -> i == 0 ? null : i - 10),
        findSplits(keys, 19));
    assertEquals(newRowsSet(IntStream.range(0, 11).map(i -> i * 10), i -> i == 0 ? null : i - 10),
        findSplits(keys, 100));
  }

  @Test
  public void testIndexIterator() {
    Iterable<Key> keys = newIndexIterable(IntStream.range(1, 101), null, null);
    assertEquals(newRowsSet(50), findSplits(keys, 1));
    assertEquals(newRowsSet(25, 50, 75), findSplits(keys, 3));
    assertEquals(newRowsSet(14, 29, 43, 57, 71, 86), findSplits(keys, 6));
    assertEquals(newRowsSet(IntStream.range(1, 101)), findSplits(keys, 200));

    keys = newIndexIterable(IntStream.range(1, 101), null, 50);
    assertEquals(newRowsSet(75), findSplits(keys, 1));
    assertEquals(newRowsSet(67, 83), findSplits(keys, 2));
    assertEquals(newRowsSet(60, 70, 80, 90), findSplits(keys, 4));
    assertEquals(newRowsSet(IntStream.range(51, 101)), findSplits(keys, 60));

    keys = newIndexIterable(IntStream.range(1, 101), 50, null);
    assertEquals(newRowsSet(25), findSplits(keys, 1));
    assertEquals(newRowsSet(17, 33), findSplits(keys, 2));
    assertEquals(newRowsSet(10, 20, 30, 40), findSplits(keys, 4));
    assertEquals(newRowsSet(IntStream.range(1, 51)), findSplits(keys, 60));

    keys = newIndexIterable(IntStream.range(1, 101), 75, 25);
    assertEquals(newRowsSet(50), findSplits(keys, 1));
    assertEquals(newRowsSet(17 + 25, 33 + 25), findSplits(keys, 2));
    assertEquals(newRowsSet(35, 45, 55, 65), findSplits(keys, 4));
    assertEquals(newRowsSet(IntStream.range(26, 76)), findSplits(keys, 60));
  }

  @Test
  public void testIndexIteratorNoDataInRange() {
    // Simulate the situation where the index has no data in a tablets range because the tablets row
    // range falls between two index keys.
    Iterable<Key> keys = newIndexIterable(IntStream.range(1, 101).map(i -> i * 1000), 250, 150);
    assertFalse(keys.iterator().hasNext());
    assertEquals(Set.of(), findSplits(keys, 1));
    assertEquals(Set.of(), findSplits(keys, 2));
  }

  @Test
  public void testNoSuchElement() {
    var keys = newIndexIterable(IntStream.range(1, 101), 75, 25);
    var iterator = keys.iterator();
    // call next() w/o calling hasNext(), should work because there is data.
    iterator.next();
    assertEquals(50 - 1, Iterators.size(iterator));
    assertThrows(NoSuchElementException.class, iterator::next);

    // try an iterator w/ null rows
    keys = newIndexIterable(IntStream.range(1, 101), null, null);
    iterator = keys.iterator();
    iterator.next();
    assertEquals(100 - 1, Iterators.size(iterator));
    assertThrows(NoSuchElementException.class, iterator::next);
  }

  @Test
  public void testVariableLength() {
    var keys = newIndexIterable(
        Stream.of("aa11", "aa112", "b", "bg45", "ct", "cz7882", "mn", "mn009", "mnrtssd", "mnz076"),
        null, null);
    assertEquals(newRowSet("c"), findSplits(keys, 1));
    assertEquals(newRowSet("b", "m"), findSplits(keys, 2));
    assertEquals(newRowSet("b", "c", "mn0"), findSplits(keys, 3));
    assertEquals(newRowSet("aa112", "bg", "cz", "mn0"), findSplits(keys, 4));
    assertEquals(newRowSet("aa11", "aa112", "b", "bg", "c", "cz", "m", "mn0", "mnr", "mnz"),
        findSplits(keys, 10));
  }

  @Test
  public void testSplitFilter() {
    List<Key> keys = IntStream.range(1, 101).mapToObj(SplitUtilsTest::newKey).collect(toList());
    Predicate<ByteSequence> splitFilter = splitCandidate -> {
      int i = Integer.parseInt(splitCandidate.toString());
      return i % 3 != 0;
    };

    assertEquals(newRowsSet(50), SplitUtils.findSplits(keys, 1, splitFilter));
    assertEquals(newRowsSet(34, 67), SplitUtils.findSplits(keys, 2, splitFilter));
    assertEquals(newRowsSet(25, 50, 76), SplitUtils.findSplits(keys, 3, splitFilter));
    assertEquals(newRowsSet(20, 40, 61, 80), SplitUtils.findSplits(keys, 4, splitFilter));
  }
}

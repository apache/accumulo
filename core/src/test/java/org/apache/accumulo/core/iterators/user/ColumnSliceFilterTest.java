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
package org.apache.accumulo.core.iterators.user;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.DefaultIteratorEnvironment;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iteratorsImpl.system.SortedMapIterator;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class ColumnSliceFilterTest {

  private static final Collection<ByteSequence> EMPTY_COL_FAMS = new ArrayList<>();

  private static final SortedMap<Key,Value> TEST_DATA = new TreeMap<>();
  private static final Key KEY_1 = newKeyValue(TEST_DATA, "boo1", "yup", "20080201", "dog");
  private static final Key KEY_2 = newKeyValue(TEST_DATA, "boo1", "yap", "20080202", "cat");
  private static final Key KEY_3 = newKeyValue(TEST_DATA, "boo2", "yap", "20080203", "hamster");
  private static final Key KEY_4 = newKeyValue(TEST_DATA, "boo2", "yop", "20080204", "lion");
  private static final Key KEY_5 = newKeyValue(TEST_DATA, "boo2", "yup", "20080206", "tiger");
  private static final Key KEY_6 = newKeyValue(TEST_DATA, "boo2", "yip", "20080203", "tiger");

  private IteratorEnvironment iteratorEnvironment;

  private ColumnSliceFilter columnSliceFilter = new ColumnSliceFilter();
  private IteratorSetting is;

  private static Key newKeyValue(SortedMap<Key,Value> tm, String row, String cf, String cq,
      String val) {
    Key k = newKey(row, cf, cq);
    tm.put(k, new Value(val));
    return k;
  }

  private static Key newKey(String row, String cf, String cq) {
    return new Key(new Text(row), new Text(cf), new Text(cq));
  }

  @BeforeEach
  public void setUp() {
    columnSliceFilter.describeOptions();
    iteratorEnvironment = new DefaultIteratorEnvironment();
    is = new IteratorSetting(1, ColumnSliceFilter.class);
  }

  @Test
  public void testBasic() throws IOException {
    ColumnSliceFilter.setSlice(is, "20080202", "20080204");

    assertTrue(columnSliceFilter.validateOptions(is.getOptions()));
    columnSliceFilter.init(new SortedMapIterator(TEST_DATA), is.getOptions(), iteratorEnvironment);
    columnSliceFilter.seek(new Range(), EMPTY_COL_FAMS, true);

    assertTrue(columnSliceFilter.hasTop());
    assertEquals(columnSliceFilter.getTopKey(), KEY_2);
    columnSliceFilter.next();
    assertTrue(columnSliceFilter.hasTop());
    assertEquals(columnSliceFilter.getTopKey(), KEY_3);
    columnSliceFilter.next();
    assertTrue(columnSliceFilter.hasTop());
    assertEquals(columnSliceFilter.getTopKey(), KEY_6);
    columnSliceFilter.next();
    assertFalse(columnSliceFilter.hasTop());
  }

  @Test
  public void testBothInclusive() throws IOException {
    ColumnSliceFilter.setSlice(is, "20080202", true, "20080204", true);

    columnSliceFilter.validateOptions(is.getOptions());
    columnSliceFilter.init(new SortedMapIterator(TEST_DATA), is.getOptions(), iteratorEnvironment);
    columnSliceFilter.seek(new Range(), EMPTY_COL_FAMS, false);

    assertTrue(columnSliceFilter.hasTop());
    assertEquals(columnSliceFilter.getTopKey(), KEY_2);
    columnSliceFilter.next();
    assertTrue(columnSliceFilter.hasTop());
    assertEquals(columnSliceFilter.getTopKey(), KEY_3);
    columnSliceFilter.next();
    assertTrue(columnSliceFilter.hasTop());
    assertEquals(columnSliceFilter.getTopKey(), KEY_6);
    columnSliceFilter.next();
    assertTrue(columnSliceFilter.hasTop());
    assertEquals(columnSliceFilter.getTopKey(), KEY_4);
    columnSliceFilter.next();
    assertFalse(columnSliceFilter.hasTop());
  }

  @Test
  public void testBothExclusive() throws IOException {
    ColumnSliceFilter.setSlice(is, "20080202", false, "20080204", false);

    columnSliceFilter.validateOptions(is.getOptions());
    columnSliceFilter.init(new SortedMapIterator(TEST_DATA), is.getOptions(), iteratorEnvironment);
    columnSliceFilter.seek(new Range(), EMPTY_COL_FAMS, false);

    assertTrue(columnSliceFilter.hasTop());
    assertEquals(columnSliceFilter.getTopKey(), KEY_3);
    columnSliceFilter.next();
    assertTrue(columnSliceFilter.hasTop());
    assertEquals(columnSliceFilter.getTopKey(), KEY_6);
    columnSliceFilter.next();
    assertFalse(columnSliceFilter.hasTop());
  }

  @Test
  public void testStartExclusiveEndInclusive() throws IOException {
    ColumnSliceFilter.setSlice(is, "20080202", false, "20080204", true);

    columnSliceFilter.validateOptions(is.getOptions());
    columnSliceFilter.init(new SortedMapIterator(TEST_DATA), is.getOptions(), iteratorEnvironment);
    columnSliceFilter.seek(new Range(), EMPTY_COL_FAMS, false);

    assertTrue(columnSliceFilter.hasTop());
    assertEquals(columnSliceFilter.getTopKey(), KEY_3);
    columnSliceFilter.next();
    assertTrue(columnSliceFilter.hasTop());
    assertEquals(columnSliceFilter.getTopKey(), KEY_6);
    columnSliceFilter.next();
    assertTrue(columnSliceFilter.hasTop());
    assertEquals(columnSliceFilter.getTopKey(), KEY_4);
    columnSliceFilter.next();
    assertFalse(columnSliceFilter.hasTop());
  }

  @Test
  public void testNullStart() throws IOException {
    ColumnSliceFilter.setSlice(is, null, "20080204");

    columnSliceFilter.validateOptions(is.getOptions());
    columnSliceFilter.init(new SortedMapIterator(TEST_DATA), is.getOptions(), iteratorEnvironment);
    columnSliceFilter.seek(new Range(), EMPTY_COL_FAMS, false);

    assertTrue(columnSliceFilter.hasTop());
    assertEquals(columnSliceFilter.getTopKey(), KEY_2);
    columnSliceFilter.next();
    assertTrue(columnSliceFilter.hasTop());
    assertEquals(columnSliceFilter.getTopKey(), KEY_1);
    columnSliceFilter.next();
    assertTrue(columnSliceFilter.hasTop());
    assertEquals(columnSliceFilter.getTopKey(), KEY_3);
    columnSliceFilter.next();
    assertTrue(columnSliceFilter.hasTop());
    assertEquals(columnSliceFilter.getTopKey(), KEY_6);
    columnSliceFilter.next();
    assertFalse(columnSliceFilter.hasTop());
  }

  @Test
  public void testNullEnd() throws IOException {
    ColumnSliceFilter.setSlice(is, "20080202", null);

    columnSliceFilter.validateOptions(is.getOptions());
    columnSliceFilter.init(new SortedMapIterator(TEST_DATA), is.getOptions(), iteratorEnvironment);
    columnSliceFilter.seek(new Range(), EMPTY_COL_FAMS, false);

    assertTrue(columnSliceFilter.hasTop());
    assertEquals(columnSliceFilter.getTopKey(), KEY_2);
    columnSliceFilter.next();
    assertTrue(columnSliceFilter.hasTop());
    assertEquals(columnSliceFilter.getTopKey(), KEY_3);
    columnSliceFilter.next();
    assertTrue(columnSliceFilter.hasTop());
    assertEquals(columnSliceFilter.getTopKey(), KEY_6);
    columnSliceFilter.next();
    assertTrue(columnSliceFilter.hasTop());
    assertEquals(columnSliceFilter.getTopKey(), KEY_4);
    columnSliceFilter.next();
    assertTrue(columnSliceFilter.hasTop());
    assertEquals(columnSliceFilter.getTopKey(), KEY_5);
    columnSliceFilter.next();
    assertFalse(columnSliceFilter.hasTop());
  }

  @Test
  public void testBothNull() throws IOException {
    ColumnSliceFilter.setSlice(is, null, null);

    columnSliceFilter.validateOptions(is.getOptions());
    columnSliceFilter.init(new SortedMapIterator(TEST_DATA), is.getOptions(), iteratorEnvironment);
    columnSliceFilter.seek(new Range(), EMPTY_COL_FAMS, false);

    assertTrue(columnSliceFilter.hasTop());
    assertEquals(columnSliceFilter.getTopKey(), KEY_2);
    columnSliceFilter.next();
    assertTrue(columnSliceFilter.hasTop());
    assertEquals(columnSliceFilter.getTopKey(), KEY_1);
    columnSliceFilter.next();
    assertTrue(columnSliceFilter.hasTop());
    assertEquals(columnSliceFilter.getTopKey(), KEY_3);
    columnSliceFilter.next();
    assertTrue(columnSliceFilter.hasTop());
    assertEquals(columnSliceFilter.getTopKey(), KEY_6);
    columnSliceFilter.next();
    assertTrue(columnSliceFilter.hasTop());
    assertEquals(columnSliceFilter.getTopKey(), KEY_4);
    columnSliceFilter.next();
    assertTrue(columnSliceFilter.hasTop());
    assertEquals(columnSliceFilter.getTopKey(), KEY_5);
    columnSliceFilter.next();
    assertFalse(columnSliceFilter.hasTop());
  }

  @Test
  public void testStartAfterEnd() {
    assertThrows(IllegalArgumentException.class,
        () -> ColumnSliceFilter.setSlice(is, "20080204", "20080202"));
  }

  @Test
  public void testStartEqualToEndStartInclusiveEndExclusive() {
    assertThrows(IllegalArgumentException.class,
        () -> ColumnSliceFilter.setSlice(is, "20080202", "20080202"));
  }

  @Test
  public void testStartEqualToEndStartExclusiveEndInclusive() {
    assertThrows(IllegalArgumentException.class,
        () -> ColumnSliceFilter.setSlice(is, "20080202", false, "20080202", true));
  }

  @Test
  public void testStartEqualToEndBothInclusive() throws IOException {
    ColumnSliceFilter.setSlice(is, "20080202", true, "20080202", true);

    columnSliceFilter.validateOptions(is.getOptions());
    columnSliceFilter.init(new SortedMapIterator(TEST_DATA), is.getOptions(), iteratorEnvironment);
    columnSliceFilter.seek(new Range(), EMPTY_COL_FAMS, false);

    assertTrue(columnSliceFilter.hasTop());
    assertEquals(columnSliceFilter.getTopKey(), KEY_2);
    columnSliceFilter.next();
    assertFalse(columnSliceFilter.hasTop());
  }
}

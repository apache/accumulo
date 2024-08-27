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
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map.Entry;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iteratorsImpl.system.SortedMapIterator;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class GrepIteratorTest {
  private static final Collection<ByteSequence> EMPTY_COL_FAMS = new ArrayList<>();
  SortedMap<Key,Value> input;
  SortedMap<Key,Value> output;

  @BeforeEach
  public void init() {
    input = new TreeMap<>();
    output = new TreeMap<>();

    input.put(new Key("abcdef", "xyz", "xyz", 0), new Value("xyz"));
    output.put(new Key("abcdef", "xyz", "xyz", 0), new Value("xyz"));

    input.put(new Key("abcdef", "cv", "cv", "colvis", 0), new Value("cv"));
    output.put(new Key("abcdef", "cv", "cv", "colvis", 0), new Value("cv"));

    input.put(new Key("bdf", "ace", "xyz", 0), new Value("xyz"));
    input.put(new Key("bdf", "abcdef", "xyz", 0), new Value("xyz"));
    output.put(new Key("bdf", "abcdef", "xyz", 0), new Value("xyz"));
    input.put(new Key("bdf", "xyz", "xyz", 0), new Value("xyz"));

    input.put(new Key("ceg", "xyz", "abcdef", 0), new Value("xyz"));
    output.put(new Key("ceg", "xyz", "abcdef", 0), new Value("xyz"));
    input.put(new Key("ceg", "xyz", "xyz", 0), new Value("xyz"));

    input.put(new Key("dfh", "xyz", "xyz", 0), new Value("abcdef"));
    output.put(new Key("dfh", "xyz", "xyz", 0), new Value("abcdef"));
    input.put(new Key("dfh", "xyz", "xyz", 1), new Value("xyz"));

    input.put(new Key("dfh", "xyz", "xyz", "abcdef", 0), new Value("xyz"));
    output.put(new Key("dfh", "xyz", "xyz", "abcdef", 0), new Value("xyz"));

    Key k = new Key("dfh", "xyz", "xyz", 1);
    k.setDeleted(true);
    input.put(k, new Value("xyz"));
    output.put(k, new Value("xyz"));
  }

  public static void checkEntries(SortedKeyValueIterator<Key,Value> skvi, SortedMap<Key,Value> map)
      throws IOException {
    for (Entry<Key,Value> e : map.entrySet()) {
      assertTrue(skvi.hasTop());
      assertEquals(e.getKey(), skvi.getTopKey());
      assertEquals(e.getValue(), skvi.getTopValue());
      skvi.next();
    }

    assertFalse(skvi.hasTop());
  }

  @Test
  public void test() throws IOException {
    GrepIterator gi = new GrepIterator();
    IteratorSetting is = new IteratorSetting(1, GrepIterator.class);
    GrepIterator.setTerm(is, "ab");
    GrepIterator.matchColumnVisibility(is, true);
    gi.init(new SortedMapIterator(input), is.getOptions(), null);
    gi.seek(new Range(), EMPTY_COL_FAMS, false);
    checkEntries(gi, output);
    GrepIterator.setTerm(is, "cde");
    gi.init(new SortedMapIterator(input), is.getOptions(), null);
    gi.deepCopy(null);
    gi.seek(new Range(), EMPTY_COL_FAMS, false);
    checkEntries(gi, output);
    GrepIterator.setTerm(is, "def");
    gi.init(new SortedMapIterator(input), is.getOptions(), null);
    gi.seek(new Range(), EMPTY_COL_FAMS, false);

    checkEntries(gi, output);

    input = new TreeMap<>();
    output = new TreeMap<>();

    input.put(new Key("abcdef", "cv", "cv", "colvis", 0), new Value("cv"));
    input.put(new Key("abcdef", "cv", "cv", "nomatch", 0), new Value("cv"));
    output.put(new Key("abcdef", "cv", "cv", "colvis", 0), new Value("cv"));

    GrepIterator.setTerm(is, "colvis");
    gi.init(new SortedMapIterator(input), is.getOptions(), null);
    gi.seek(new Range(), EMPTY_COL_FAMS, false);

    checkEntries(gi, output);
  }

  @Test
  public void testMatchRow() throws Exception {
    GrepIterator gi = new GrepIterator();
    IteratorSetting is = new IteratorSetting(1, GrepIterator.class);

    GrepIterator.setTerm(is, "abcdef");

    GrepIterator.matchRow(is, true);
    GrepIterator.matchColumnFamily(is, false);
    GrepIterator.matchColumnQualifier(is, false);
    GrepIterator.matchColumnVisibility(is, false);
    GrepIterator.matchValue(is, false);

    gi.init(new SortedMapIterator(input), is.getOptions(), null);
    gi.seek(new Range(), EMPTY_COL_FAMS, false);

    SortedMap<Key,Value> expectedOutput = new TreeMap<>();
    input.forEach((k, v) -> {
      if (k.getRowData().toString().contains("abcdef") || k.isDeleted()) {
        expectedOutput.put(k, v);
      }
    });

    assertFalse(expectedOutput.isEmpty());
    checkEntries(gi, expectedOutput);
  }

  @Test
  public void testMatchFamily() throws Exception {
    GrepIterator gi = new GrepIterator();
    IteratorSetting is = new IteratorSetting(1, GrepIterator.class);

    GrepIterator.setTerm(is, "abcdef");

    GrepIterator.matchRow(is, false);
    GrepIterator.matchColumnFamily(is, true);
    GrepIterator.matchColumnQualifier(is, false);
    GrepIterator.matchColumnVisibility(is, false);
    GrepIterator.matchValue(is, false);

    gi.init(new SortedMapIterator(input), is.getOptions(), null);
    gi.seek(new Range(), EMPTY_COL_FAMS, false);

    SortedMap<Key,Value> expectedOutput = new TreeMap<>();
    input.forEach((k, v) -> {
      if (k.getColumnFamilyData().toString().contains("abcdef") || k.isDeleted()) {
        expectedOutput.put(k, v);
      }
    });

    assertFalse(expectedOutput.isEmpty());
    checkEntries(gi, expectedOutput);
  }

  @Test
  public void testMatchQualifier() throws Exception {
    GrepIterator gi = new GrepIterator();
    IteratorSetting is = new IteratorSetting(1, GrepIterator.class);

    GrepIterator.setTerm(is, "abcdef");

    GrepIterator.matchRow(is, false);
    GrepIterator.matchColumnFamily(is, false);
    GrepIterator.matchColumnQualifier(is, true);
    GrepIterator.matchColumnVisibility(is, false);
    GrepIterator.matchValue(is, false);

    gi.init(new SortedMapIterator(input), is.getOptions(), null);
    gi.seek(new Range(), EMPTY_COL_FAMS, false);

    SortedMap<Key,Value> expectedOutput = new TreeMap<>();
    input.forEach((k, v) -> {
      if (k.getColumnQualifierData().toString().contains("abcdef") || k.isDeleted()) {
        expectedOutput.put(k, v);
      }
    });

    assertFalse(expectedOutput.isEmpty());
    checkEntries(gi, expectedOutput);
  }

  @Test
  public void testMatchVisibility() throws Exception {
    GrepIterator gi = new GrepIterator();
    IteratorSetting is = new IteratorSetting(1, GrepIterator.class);

    GrepIterator.setTerm(is, "abcdef");

    GrepIterator.matchRow(is, false);
    GrepIterator.matchColumnFamily(is, false);
    GrepIterator.matchColumnQualifier(is, false);
    GrepIterator.matchColumnVisibility(is, true);
    GrepIterator.matchValue(is, false);

    gi.init(new SortedMapIterator(input), is.getOptions(), null);
    gi.seek(new Range(), EMPTY_COL_FAMS, false);

    SortedMap<Key,Value> expectedOutput = new TreeMap<>();
    input.forEach((k, v) -> {
      if (k.getColumnVisibilityData().toString().contains("abcdef") || k.isDeleted()) {
        expectedOutput.put(k, v);
      }
    });

    assertFalse(expectedOutput.isEmpty());
    checkEntries(gi, expectedOutput);
  }

  @Test
  public void testMatchValue() throws Exception {
    GrepIterator gi = new GrepIterator();
    IteratorSetting is = new IteratorSetting(1, GrepIterator.class);

    GrepIterator.setTerm(is, "abcdef");

    GrepIterator.matchRow(is, false);
    GrepIterator.matchColumnFamily(is, false);
    GrepIterator.matchColumnQualifier(is, false);
    GrepIterator.matchColumnVisibility(is, false);
    GrepIterator.matchValue(is, true);

    gi.init(new SortedMapIterator(input), is.getOptions(), null);
    gi.seek(new Range(), EMPTY_COL_FAMS, false);

    SortedMap<Key,Value> expectedOutput = new TreeMap<>();
    input.forEach((k, v) -> {
      if (v.toString().contains("abcdef") || k.isDeleted()) {
        expectedOutput.put(k, v);
      }
    });

    assertFalse(expectedOutput.isEmpty());
    checkEntries(gi, expectedOutput);
  }
}

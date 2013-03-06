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
package org.apache.accumulo.core.iterators.user;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

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
import org.apache.accumulo.core.iterators.SortedMapIterator;
import org.junit.Before;
import org.junit.Test;

public class GrepIteratorTest {
  private static final Collection<ByteSequence> EMPTY_COL_FAMS = new ArrayList<ByteSequence>();
  SortedMap<Key,Value> input;
  SortedMap<Key,Value> output;

  @Before
  public void init() {
    input = new TreeMap<Key,Value>();
    output = new TreeMap<Key,Value>();
    input.put(new Key("abcdef", "xyz", "xyz", 0), new Value("xyz".getBytes()));
    output.put(new Key("abcdef", "xyz", "xyz", 0), new Value("xyz".getBytes()));

    input.put(new Key("bdf", "ace", "xyz", 0), new Value("xyz".getBytes()));
    input.put(new Key("bdf", "abcdef", "xyz", 0), new Value("xyz".getBytes()));
    output.put(new Key("bdf", "abcdef", "xyz", 0), new Value("xyz".getBytes()));
    input.put(new Key("bdf", "xyz", "xyz", 0), new Value("xyz".getBytes()));

    input.put(new Key("ceg", "xyz", "abcdef", 0), new Value("xyz".getBytes()));
    output.put(new Key("ceg", "xyz", "abcdef", 0), new Value("xyz".getBytes()));
    input.put(new Key("ceg", "xyz", "xyz", 0), new Value("xyz".getBytes()));

    input.put(new Key("dfh", "xyz", "xyz", 0), new Value("abcdef".getBytes()));
    output.put(new Key("dfh", "xyz", "xyz", 0), new Value("abcdef".getBytes()));
    input.put(new Key("dfh", "xyz", "xyz", 1), new Value("xyz".getBytes()));

    Key k = new Key("dfh", "xyz", "xyz", 1);
    k.setDeleted(true);
    input.put(k, new Value("xyz".getBytes()));
    output.put(k, new Value("xyz".getBytes()));
  }

  public static void checkEntries(SortedKeyValueIterator<Key,Value> skvi, SortedMap<Key,Value> map) throws IOException {
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
  }
}

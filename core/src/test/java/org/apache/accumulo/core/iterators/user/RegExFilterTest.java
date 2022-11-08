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

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.TreeMap;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.DefaultIteratorEnvironment;
import org.apache.accumulo.core.iteratorsImpl.system.SortedMapIterator;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.Test;

public class RegExFilterTest {

  private static final Collection<ByteSequence> EMPTY_COL_FAMS = new ArrayList<>();

  private Key newKeyValue(TreeMap<Key,Value> tm, String row, String cf, String cq, String val) {
    Key k = newKey(row, cf, cq);
    tm.put(k, new Value(val));
    return k;
  }

  private Key newKey(String row, String cf, String cq) {
    return new Key(new Text(row), new Text(cf), new Text(cq));
  }

  @Test
  public void test1() throws IOException {
    TreeMap<Key,Value> tm = new TreeMap<>();

    Key k1 = newKeyValue(tm, "boo1", "yup", "20080201", "dog");
    Key k2 = newKeyValue(tm, "boo1", "yap", "20080202", "cat");
    Key k3 = newKeyValue(tm, "boo2", "yip", "20080203", "hamster");

    RegExFilter rei = new RegExFilter();
    rei.describeOptions();

    IteratorSetting is = new IteratorSetting(1, RegExFilter.class);
    RegExFilter.setRegexs(is, ".*2", null, null, null, false);

    assertTrue(rei.validateOptions(is.getOptions()));
    rei.init(new SortedMapIterator(tm), is.getOptions(), new DefaultIteratorEnvironment());
    rei.seek(new Range(), EMPTY_COL_FAMS, false);

    assertTrue(rei.hasTop());
    assertEquals(rei.getTopKey(), k3);
    rei.next();
    assertFalse(rei.hasTop());

    // -----------------------------------------------------
    // Test substring regex
    is.clearOptions();

    RegExFilter.setRegexs(is, null, null, null, "amst", false, true); // Should only match hamster

    rei.validateOptions(is.getOptions());
    rei.init(new SortedMapIterator(tm), is.getOptions(), new DefaultIteratorEnvironment());
    rei.seek(new Range(), EMPTY_COL_FAMS, false);

    assertTrue(rei.hasTop());
    assertEquals(rei.getTopKey(), k3);
    rei.next();
    assertFalse(rei.hasTop());

    // -----------------------------------------------------
    is.clearOptions();

    RegExFilter.setRegexs(is, null, "ya.*", null, null, false);
    assertTrue(rei.validateOptions(is.getOptions()));
    rei.init(new SortedMapIterator(tm), is.getOptions(), new DefaultIteratorEnvironment());
    rei.seek(new Range(), EMPTY_COL_FAMS, false);

    assertTrue(rei.hasTop());
    assertEquals(rei.getTopKey(), k2);
    rei.next();
    assertFalse(rei.hasTop());

    // -----------------------------------------------------
    is.clearOptions();

    RegExFilter.setRegexs(is, null, null, ".*01", null, false);
    assertTrue(rei.validateOptions(is.getOptions()));
    rei.init(new SortedMapIterator(tm), is.getOptions(), new DefaultIteratorEnvironment());
    rei.seek(new Range(), EMPTY_COL_FAMS, false);

    assertTrue(rei.hasTop());
    assertEquals(rei.getTopKey(), k1);
    rei.next();
    assertFalse(rei.hasTop());

    // -----------------------------------------------------
    is.clearOptions();

    RegExFilter.setRegexs(is, null, null, null, ".*at", false);
    assertTrue(rei.validateOptions(is.getOptions()));
    rei.init(new SortedMapIterator(tm), is.getOptions(), new DefaultIteratorEnvironment());
    rei.seek(new Range(), EMPTY_COL_FAMS, false);

    assertTrue(rei.hasTop());
    assertEquals(rei.getTopKey(), k2);
    rei.next();
    assertFalse(rei.hasTop());

    // -----------------------------------------------------
    is.clearOptions();

    RegExFilter.setRegexs(is, null, null, null, ".*ap", false);
    rei.init(new SortedMapIterator(tm), is.getOptions(), new DefaultIteratorEnvironment());
    rei.seek(new Range(), EMPTY_COL_FAMS, false);

    assertFalse(rei.hasTop());

    // -----------------------------------------------------
    is.clearOptions();

    RegExFilter.setRegexs(is, null, "ya.*", null, ".*at", false);
    rei.init(new SortedMapIterator(tm), is.getOptions(), new DefaultIteratorEnvironment());
    rei.seek(new Range(), EMPTY_COL_FAMS, false);

    assertTrue(rei.hasTop());
    assertEquals(rei.getTopKey(), k2);
    rei.next();
    assertFalse(rei.hasTop());

    // -----------------------------------------------------
    is.clearOptions();

    RegExFilter.setRegexs(is, null, "ya.*", null, ".*ap", false);
    rei.init(new SortedMapIterator(tm), is.getOptions(), new DefaultIteratorEnvironment());
    rei.seek(new Range(), EMPTY_COL_FAMS, false);

    assertFalse(rei.hasTop());

    // -----------------------------------------------------
    is.clearOptions();

    RegExFilter.setRegexs(is, "boo1", null, null, null, false);
    rei.init(new SortedMapIterator(tm), is.getOptions(), new DefaultIteratorEnvironment());
    rei.seek(new Range(), EMPTY_COL_FAMS, false);

    assertTrue(rei.hasTop());
    assertEquals(rei.getTopKey(), k2);
    rei.next();
    assertTrue(rei.hasTop());
    assertEquals(rei.getTopKey(), k1);
    rei.next();
    assertFalse(rei.hasTop());

    // -----------------------------------------------------
    is.clearOptions();

    rei.init(new SortedMapIterator(tm), is.getOptions(), new DefaultIteratorEnvironment());
    rei.seek(new Range(), EMPTY_COL_FAMS, false);

    assertTrue(rei.hasTop());
    assertEquals(rei.getTopKey(), k2);
    rei.next();
    assertTrue(rei.hasTop());
    assertEquals(rei.getTopKey(), k1);
    rei.next();
    assertTrue(rei.hasTop());
    assertEquals(rei.getTopKey(), k3);
    rei.next();
    assertFalse(rei.hasTop());

    // -----------------------------------------------------
    is.clearOptions();

    RegExFilter.setRegexs(is, "hamster", null, "hamster", "hamster", true);
    rei.init(new SortedMapIterator(tm), is.getOptions(), new DefaultIteratorEnvironment());
    rei.seek(new Range(), EMPTY_COL_FAMS, false);

    assertTrue(rei.hasTop());
    assertEquals(rei.getTopKey(), k3);
    rei.next();
    assertFalse(rei.hasTop());

    // -----------------------------------------------------
    is.clearOptions();

    RegExFilter.setRegexs(is, null, "ya.*", "hamster", null, true);
    rei.init(new SortedMapIterator(tm), is.getOptions(), new DefaultIteratorEnvironment());
    rei.seek(new Range(), EMPTY_COL_FAMS, false);

    assertTrue(rei.hasTop());
    assertEquals(rei.getTopKey(), k2);
    rei.next();
    assertFalse(rei.hasTop());

    is.clearOptions();

    RegExFilter.setRegexs(is, null, "ya.*", "hamster", null, true);
    rei.init(new SortedMapIterator(tm), is.getOptions(), new DefaultIteratorEnvironment());
    rei.seek(new Range(), EMPTY_COL_FAMS, false);
    rei.deepCopy(new DefaultIteratorEnvironment());

    // -----------------------------------------------------
    String multiByteText = new String("\u6d67\u6F68\u7067");
    String multiByteRegex = new String(".*\u6F68.*");

    Key k4 = new Key("boo4".getBytes(), "hoo".getBytes(), "20080203".getBytes(), "".getBytes(), 1L);
    Value inVal = new Value(multiByteText);
    tm.put(k4, inVal);

    is.clearOptions();

    RegExFilter.setRegexs(is, null, null, null, multiByteRegex, true);
    rei.init(new SortedMapIterator(tm), is.getOptions(), new DefaultIteratorEnvironment());
    rei.seek(new Range(), EMPTY_COL_FAMS, false);

    assertTrue(rei.hasTop());
    Value outValue = rei.getTopValue();
    String outVal = new String(outValue.get(), UTF_8);
    assertEquals(outVal, multiByteText);

  }

  @Test
  public void testNullByteInKey() throws IOException {
    TreeMap<Key,Value> tm = new TreeMap<>();

    String s1 = "first", s2 = "second";
    byte[] b1 = s1.getBytes(), b2 = s2.getBytes(), ball;
    ball = new byte[b1.length + b2.length + 1];
    System.arraycopy(b1, 0, ball, 0, b1.length);
    ball[b1.length] = (byte) 0;
    System.arraycopy(b2, 0, ball, b1.length + 1, b2.length);

    Key key = new Key(ball, new byte[0], new byte[0], new byte[0], 90, false);
    Value val = new Value();
    tm.put(key, val);

    IteratorSetting is = new IteratorSetting(5, RegExFilter.class);
    RegExFilter.setRegexs(is, s2, null, null, null, true, true);

    RegExFilter filter = new RegExFilter();
    filter.init(new SortedMapIterator(tm), is.getOptions(), null);
    filter.seek(new Range(), EMPTY_COL_FAMS, false);

    assertTrue(filter.hasTop(), "iterator couldn't find a match when it should have");
  }
}

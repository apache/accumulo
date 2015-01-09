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
package org.apache.accumulo.core.data;

import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.CoreMatchers.hasItems;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.hadoop.io.Text;
import org.junit.Before;
import org.junit.Test;

public class KeyExtentTest {
  KeyExtent nke(String t, String er, String per) {
    return new KeyExtent(new Text(t), er == null ? null : new Text(er), per == null ? null : new Text(per));
  }

  KeyExtent ke;
  TreeSet<KeyExtent> set0;

  @Before
  public void setup() {
    set0 = new TreeSet<KeyExtent>();
  }

  @Test
  public void testDecodingMetadataRow() {
    Text flattenedExtent = new Text("foo;bar");

    ke = new KeyExtent(flattenedExtent, (Text) null);

    assertEquals(new Text("bar"), ke.getEndRow());
    assertEquals(new Text("foo"), ke.getTableId());
    assertNull(ke.getPrevEndRow());

    flattenedExtent = new Text("foo<");

    ke = new KeyExtent(flattenedExtent, (Text) null);

    assertNull(ke.getEndRow());
    assertEquals(new Text("foo"), ke.getTableId());
    assertNull(ke.getPrevEndRow());

    flattenedExtent = new Text("foo;bar;");

    ke = new KeyExtent(flattenedExtent, (Text) null);

    assertEquals(new Text("bar;"), ke.getEndRow());
    assertEquals(new Text("foo"), ke.getTableId());
    assertNull(ke.getPrevEndRow());

  }

  @Test
  public void testFindContainingExtents() {
    assertNull(KeyExtent.findContainingExtent(nke("t", null, null), set0));
    assertNull(KeyExtent.findContainingExtent(nke("t", "1", "0"), set0));
    assertNull(KeyExtent.findContainingExtent(nke("t", "1", null), set0));
    assertNull(KeyExtent.findContainingExtent(nke("t", null, "0"), set0));

    TreeSet<KeyExtent> set1 = new TreeSet<KeyExtent>();

    set1.add(nke("t", null, null));

    assertEquals(nke("t", null, null), KeyExtent.findContainingExtent(nke("t", null, null), set1));
    assertEquals(nke("t", null, null), KeyExtent.findContainingExtent(nke("t", "1", "0"), set1));
    assertEquals(nke("t", null, null), KeyExtent.findContainingExtent(nke("t", "1", null), set1));
    assertEquals(nke("t", null, null), KeyExtent.findContainingExtent(nke("t", null, "0"), set1));

    TreeSet<KeyExtent> set2 = new TreeSet<KeyExtent>();

    set2.add(nke("t", "g", null));
    set2.add(nke("t", null, "g"));

    assertNull(KeyExtent.findContainingExtent(nke("t", null, null), set2));
    assertEquals(nke("t", "g", null), KeyExtent.findContainingExtent(nke("t", "c", "a"), set2));
    assertEquals(nke("t", "g", null), KeyExtent.findContainingExtent(nke("t", "c", null), set2));

    assertEquals(nke("t", "g", null), KeyExtent.findContainingExtent(nke("t", "g", "a"), set2));
    assertEquals(nke("t", "g", null), KeyExtent.findContainingExtent(nke("t", "g", null), set2));

    assertNull(KeyExtent.findContainingExtent(nke("t", "h", "a"), set2));
    assertNull(KeyExtent.findContainingExtent(nke("t", "h", null), set2));

    assertNull(KeyExtent.findContainingExtent(nke("t", "z", "f"), set2));
    assertNull(KeyExtent.findContainingExtent(nke("t", null, "f"), set2));

    assertEquals(nke("t", null, "g"), KeyExtent.findContainingExtent(nke("t", "z", "g"), set2));
    assertEquals(nke("t", null, "g"), KeyExtent.findContainingExtent(nke("t", null, "g"), set2));

    assertEquals(nke("t", null, "g"), KeyExtent.findContainingExtent(nke("t", "z", "h"), set2));
    assertEquals(nke("t", null, "g"), KeyExtent.findContainingExtent(nke("t", null, "h"), set2));

    TreeSet<KeyExtent> set3 = new TreeSet<KeyExtent>();

    set3.add(nke("t", "g", null));
    set3.add(nke("t", "s", "g"));
    set3.add(nke("t", null, "s"));

    assertNull(KeyExtent.findContainingExtent(nke("t", null, null), set3));

    assertEquals(nke("t", "g", null), KeyExtent.findContainingExtent(nke("t", "g", null), set3));
    assertEquals(nke("t", "s", "g"), KeyExtent.findContainingExtent(nke("t", "s", "g"), set3));
    assertEquals(nke("t", null, "s"), KeyExtent.findContainingExtent(nke("t", null, "s"), set3));

    assertNull(KeyExtent.findContainingExtent(nke("t", "t", "g"), set3));
    assertNull(KeyExtent.findContainingExtent(nke("t", "t", "f"), set3));
    assertNull(KeyExtent.findContainingExtent(nke("t", "s", "f"), set3));

    assertEquals(nke("t", "s", "g"), KeyExtent.findContainingExtent(nke("t", "r", "h"), set3));
    assertEquals(nke("t", "s", "g"), KeyExtent.findContainingExtent(nke("t", "s", "h"), set3));
    assertEquals(nke("t", "s", "g"), KeyExtent.findContainingExtent(nke("t", "r", "g"), set3));

    assertEquals(nke("t", null, "s"), KeyExtent.findContainingExtent(nke("t", null, "t"), set3));
    assertNull(KeyExtent.findContainingExtent(nke("t", null, "r"), set3));

    assertEquals(nke("t", "g", null), KeyExtent.findContainingExtent(nke("t", "f", null), set3));
    assertNull(KeyExtent.findContainingExtent(nke("t", "h", null), set3));

    TreeSet<KeyExtent> set4 = new TreeSet<KeyExtent>();

    set4.add(nke("t1", "d", null));
    set4.add(nke("t1", "q", "d"));
    set4.add(nke("t1", null, "q"));
    set4.add(nke("t2", "g", null));
    set4.add(nke("t2", "s", "g"));
    set4.add(nke("t2", null, "s"));

    assertNull(KeyExtent.findContainingExtent(nke("t", null, null), set4));
    assertNull(KeyExtent.findContainingExtent(nke("z", null, null), set4));
    assertNull(KeyExtent.findContainingExtent(nke("t11", null, null), set4));
    assertNull(KeyExtent.findContainingExtent(nke("t1", null, null), set4));
    assertNull(KeyExtent.findContainingExtent(nke("t2", null, null), set4));

    assertNull(KeyExtent.findContainingExtent(nke("t", "g", null), set4));
    assertNull(KeyExtent.findContainingExtent(nke("z", "g", null), set4));
    assertNull(KeyExtent.findContainingExtent(nke("t11", "g", null), set4));
    assertNull(KeyExtent.findContainingExtent(nke("t1", "g", null), set4));

    assertEquals(nke("t2", "g", null), KeyExtent.findContainingExtent(nke("t2", "g", null), set4));
    assertEquals(nke("t2", "s", "g"), KeyExtent.findContainingExtent(nke("t2", "s", "g"), set4));
    assertEquals(nke("t2", null, "s"), KeyExtent.findContainingExtent(nke("t2", null, "s"), set4));

    assertEquals(nke("t1", "d", null), KeyExtent.findContainingExtent(nke("t1", "d", null), set4));
    assertEquals(nke("t1", "q", "d"), KeyExtent.findContainingExtent(nke("t1", "q", "d"), set4));
    assertEquals(nke("t1", null, "q"), KeyExtent.findContainingExtent(nke("t1", null, "q"), set4));

  }

  private static boolean overlaps(KeyExtent extent, SortedMap<KeyExtent,Object> extents) {
    return !KeyExtent.findOverlapping(extent, extents).isEmpty();
  }

  @Test
  public void testOverlaps() {
    SortedMap<KeyExtent,Object> set0 = new TreeMap<KeyExtent,Object>();
    set0.put(nke("a", null, null), null);

    // Nothing overlaps with the empty set
    assertFalse(overlaps(nke("t", null, null), null));
    assertFalse(overlaps(nke("t", null, null), set0));

    SortedMap<KeyExtent,Object> set1 = new TreeMap<KeyExtent,Object>();

    // Everything overlaps with the infinite range
    set1.put(nke("t", null, null), null);
    assertTrue(overlaps(nke("t", null, null), set1));
    assertTrue(overlaps(nke("t", "b", "a"), set1));
    assertTrue(overlaps(nke("t", null, "a"), set1));

    set1.put(nke("t", "b", "a"), null);
    assertTrue(overlaps(nke("t", null, null), set1));
    assertTrue(overlaps(nke("t", "b", "a"), set1));
    assertTrue(overlaps(nke("t", null, "a"), set1));

    // simple overlaps
    SortedMap<KeyExtent,Object> set2 = new TreeMap<KeyExtent,Object>();
    set2.put(nke("a", null, null), null);
    set2.put(nke("t", "m", "j"), null);
    set2.put(nke("z", null, null), null);
    assertTrue(overlaps(nke("t", null, null), set2));
    assertTrue(overlaps(nke("t", "m", "j"), set2));
    assertTrue(overlaps(nke("t", "z", "a"), set2));
    assertFalse(overlaps(nke("t", "j", "a"), set2));
    assertFalse(overlaps(nke("t", "z", "m"), set2));

    // non-overlaps
    assertFalse(overlaps(nke("t", "b", "a"), set2));
    assertFalse(overlaps(nke("t", "z", "y"), set2));
    assertFalse(overlaps(nke("t", "b", null), set2));
    assertFalse(overlaps(nke("t", null, "y"), set2));
    assertFalse(overlaps(nke("t", "j", null), set2));
    assertFalse(overlaps(nke("t", null, "m"), set2));

    // infinite overlaps
    SortedMap<KeyExtent,Object> set3 = new TreeMap<KeyExtent,Object>();
    set3.put(nke("t", "j", null), null);
    set3.put(nke("t", null, "m"), null);
    assertTrue(overlaps(nke("t", "k", "a"), set3));
    assertTrue(overlaps(nke("t", "k", null), set3));
    assertTrue(overlaps(nke("t", "z", "k"), set3));
    assertTrue(overlaps(nke("t", null, "k"), set3));
    assertTrue(overlaps(nke("t", null, null), set3));

    // falls between
    assertFalse(overlaps(nke("t", "l", "k"), set3));

    SortedMap<KeyExtent,Object> set4 = new TreeMap<KeyExtent,Object>();
    set4.put(nke("t", null, null), null);
    assertTrue(overlaps(nke("t", "k", "a"), set4));
    assertTrue(overlaps(nke("t", "k", null), set4));
    assertTrue(overlaps(nke("t", "z", "k"), set4));
    assertTrue(overlaps(nke("t", null, "k"), set4));
    assertTrue(overlaps(nke("t", null, null), set4));
    assertTrue(overlaps(nke("t", null, null), set4));

    for (String er : new String[] {"z", "y", "r", null}) {
      for (String per : new String[] {"a", "b", "d", null}) {
        assertTrue(nke("t", "y", "b").overlaps(nke("t", er, per)));
        assertTrue(nke("t", "y", null).overlaps(nke("t", er, per)));
        assertTrue(nke("t", null, "b").overlaps(nke("t", er, per)));
        assertTrue(nke("t", null, null).overlaps(nke("t", er, per)));
      }
    }

    assertFalse(nke("t", "y", "b").overlaps(nke("t", "z", "y")));
    assertFalse(nke("t", "y", "b").overlaps(nke("t", null, "y")));
    assertFalse(nke("t", "y", null).overlaps(nke("t", "z", "y")));
    assertFalse(nke("t", "y", null).overlaps(nke("t", null, "y")));
    assertFalse(nke("t", "y", "b").overlaps(nke("t", "b", "a")));
    assertFalse(nke("t", "y", "b").overlaps(nke("t", "b", null)));
    assertFalse(nke("t", null, "b").overlaps(nke("t", "b", "a")));
    assertFalse(nke("t", null, "b").overlaps(nke("t", "b", null)));
  }

  @Test
  public void testWriteReadFields() throws Exception {
    ke = nke("t", "e", "b");
    assertEquals(ke, writeAndReadFields(ke));

    ke = nke("t", "e", null);
    assertEquals(ke, writeAndReadFields(ke));

    ke = nke("t", null, "b");
    assertEquals(ke, writeAndReadFields(ke));

    ke = nke("t", null, null);
    assertEquals(ke, writeAndReadFields(ke));
  }

  private KeyExtent writeAndReadFields(KeyExtent in) throws IOException {
    KeyExtent out = new KeyExtent();

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    in.write(new DataOutputStream(baos));

    ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
    out.readFields(new DataInputStream(bais));

    return out;
  }

  @SuppressWarnings("deprecation")
  @Test
  public void testKeyExtentsForSimpleRange() {
    Collection<KeyExtent> results;

    results = KeyExtent.getKeyExtentsForRange(null, null, null);
    assertTrue("Non-empty set returned from no extents", results.isEmpty());

    results = KeyExtent.getKeyExtentsForRange(null, null, Collections.<KeyExtent> emptySet());
    assertTrue("Non-empty set returned from no extents", results.isEmpty());

    KeyExtent t = nke("t", null, null);
    results = KeyExtent.getKeyExtentsForRange(null, null, Collections.<KeyExtent> singleton(t));
    assertEquals("Single tablet should always be returned", 1, results.size());
    assertEquals(t, results.iterator().next());
  }

  @SuppressWarnings("deprecation")
  @Test
  public void testKeyExtentsForRange() {
    KeyExtent b = nke("t", "b", null);
    KeyExtent e = nke("t", "e", "b");
    KeyExtent h = nke("t", "h", "e");
    KeyExtent m = nke("t", "m", "h");
    KeyExtent z = nke("t", null, "m");

    set0.addAll(Arrays.asList(b, e, h, m, z));

    Collection<KeyExtent> results;

    results = KeyExtent.getKeyExtentsForRange(null, null, set0);
    assertThat("infinite range should return full set", results.size(), is(5));
    assertThat("infinite range should return full set", results, hasItems(b, e, h, m, z));

    results = KeyExtent.getKeyExtentsForRange(new Text("a"), new Text("z"), set0);
    assertThat("full overlap should return full set", results.size(), is(5));
    assertThat("full overlap should return full set", results, hasItems(b, e, h, m, z));

    results = KeyExtent.getKeyExtentsForRange(null, new Text("f"), set0);
    assertThat("end row should return head set", results.size(), is(3));
    assertThat("end row should return head set", results, hasItems(b, e, h));

    results = KeyExtent.getKeyExtentsForRange(new Text("f"), null, set0);
    assertThat("start row should return tail set", results.size(), is(3));
    assertThat("start row should return tail set", results, hasItems(h, m, z));

    results = KeyExtent.getKeyExtentsForRange(new Text("f"), new Text("g"), set0);
    assertThat("slice should return correct subset", results.size(), is(1));
    assertThat("slice should return correct subset", results, hasItem(h));
  }

  @Test
  public void testDecodeEncode() {
    assertNull(KeyExtent.decodePrevEndRow(KeyExtent.encodePrevEndRow(null)));

    Text x = new Text();
    assertEquals(x, KeyExtent.decodePrevEndRow(KeyExtent.encodePrevEndRow(x)));
  }
}

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
package org.apache.accumulo.core.data;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.Test;

public class KeyExtentTest {
  KeyExtent nke(String t, String er, String per) {
    return new KeyExtent(TableId.of(t), er == null ? null : new Text(er),
        per == null ? null : new Text(per));
  }

  KeyExtent ke;

  @Test
  public void testDecodingMetadataRow() {
    Text flattenedExtent = new Text("foo;bar");

    ke = KeyExtent.fromMetaRow(flattenedExtent);

    assertEquals(new Text("bar"), ke.endRow());
    assertEquals("foo", ke.tableId().canonical());
    assertNull(ke.prevEndRow());

    flattenedExtent = new Text("foo<");

    ke = KeyExtent.fromMetaRow(flattenedExtent);

    assertNull(ke.endRow());
    assertEquals("foo", ke.tableId().canonical());
    assertNull(ke.prevEndRow());

    flattenedExtent = new Text("foo;bar;");

    ke = KeyExtent.fromMetaRow(flattenedExtent);

    assertEquals(new Text("bar;"), ke.endRow());
    assertEquals("foo", ke.tableId().canonical());
    assertNull(ke.prevEndRow());

  }

  private static boolean overlaps(KeyExtent extent, SortedMap<KeyExtent,Object> extents) {
    return !KeyExtent.findOverlapping(extent, extents).isEmpty();
  }

  @Test
  public void testOverlaps() {
    SortedMap<KeyExtent,Object> set0 = new TreeMap<>();
    set0.put(nke("a", null, null), null);

    // Nothing overlaps with the empty set
    assertFalse(overlaps(nke("t", null, null), null));
    assertFalse(overlaps(nke("t", null, null), set0));

    SortedMap<KeyExtent,Object> set1 = new TreeMap<>();

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
    SortedMap<KeyExtent,Object> set2 = new TreeMap<>();
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
    SortedMap<KeyExtent,Object> set3 = new TreeMap<>();
    set3.put(nke("t", "j", null), null);
    set3.put(nke("t", null, "m"), null);
    assertTrue(overlaps(nke("t", "k", "a"), set3));
    assertTrue(overlaps(nke("t", "k", null), set3));
    assertTrue(overlaps(nke("t", "z", "k"), set3));
    assertTrue(overlaps(nke("t", null, "k"), set3));
    assertTrue(overlaps(nke("t", null, null), set3));

    // falls between
    assertFalse(overlaps(nke("t", "l", "k"), set3));

    SortedMap<KeyExtent,Object> set4 = new TreeMap<>();
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

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    in.writeTo(new DataOutputStream(baos));

    ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
    return KeyExtent.readFrom(new DataInputStream(bais));
  }

  @Test
  public void testClip() {
    // Test extent will null end/prevEnd and infinite range matches original
    ke = new KeyExtent(TableId.of("1"), null, null);
    Range range = new Range();
    KeyExtent expected = ke;
    KeyExtent clipped = ke.clip(range);
    assertEquals(expected, clipped);

    // Test extent with null end/prevEnd with a range, should match the range
    expected = new KeyExtent(TableId.of("1"), new Text("r_0001"), new Text("r_0000"));
    ke = new KeyExtent(TableId.of("1"), null, null);
    range = new Range("r_0000", false, "r_0001", true);
    clipped = ke.clip(range);
    assertEquals(expected, clipped);

    // Test resulting extent is narrowed, the overlapping rows are 4 - 7
    expected = new KeyExtent(TableId.of("1"), new Text("r_0007"), new Text("r_0003"));
    ke = new KeyExtent(TableId.of("1"), new Text("r_0007"), new Text("r_0000"));
    range = new Range("r_0003", false, "r_0009", true);
    clipped = ke.clip(range);
    assertEquals(expected, clipped);

    // Test disjoint
    ke = new KeyExtent(TableId.of("1"), new Text("r_0007"), new Text("r_0005"));
    range = new Range("r_0002", false, "r_0004", true);
    clipped = ke.clip(range, true);
    assertNull(clipped);
    final Range r = range;
    assertThrows(IllegalArgumentException.class, () -> ke.clip(r));

    // Invalid row ranges
    assertThrows(IllegalArgumentException.class,
        () -> ke.clip(new Range("r_0002", true, "r_0004", true)));
    assertThrows(IllegalArgumentException.class, () -> ke
        .clip(new Range(new Key("r_0002", "cf_0002"), false, new Key("r_0003", "cf_0003"), true)));
    // Test empty row to make sure isExclusiveKey() checks length
    assertThrows(IllegalArgumentException.class,
        () -> ke.clip(new Range(new Key("r_0004").followingKey(PartialKey.ROW), true,
            new Key("").followingKey(PartialKey.ROW), false)));
  }

}

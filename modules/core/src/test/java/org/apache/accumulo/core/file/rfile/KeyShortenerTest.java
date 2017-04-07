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

package org.apache.accumulo.core.file.rfile;

import org.apache.accumulo.core.data.Key;
import org.junit.Assert;
import org.junit.Test;

import com.google.common.primitives.Bytes;

public class KeyShortenerTest {

  private static final byte[] E = new byte[0];
  private static final byte[] FF = new byte[] {(byte) 0xff};

  private void assertBetween(Key p, Key s, Key c) {
    Assert.assertTrue(p.compareTo(s) < 0);
    Assert.assertTrue(s.compareTo(c) < 0);
  }

  private void testKeys(Key prev, Key current, Key expected) {
    Key sk = KeyShortener.shorten(prev, current);
    assertBetween(prev, sk, current);
  }

  /**
   * append 0xff to end of string
   */
  private byte[] apendFF(String s) {
    return Bytes.concat(s.getBytes(), FF);
  }

  /**
   * append 0x00 to end of string
   */
  private byte[] append00(String s) {
    return Bytes.concat(s.getBytes(), new byte[] {(byte) 0x00});
  }

  private byte[] toBytes(Object o) {
    if (o instanceof String) {
      return ((String) o).getBytes();
    } else if (o instanceof byte[]) {
      return (byte[]) o;
    }

    throw new IllegalArgumentException();
  }

  private Key newKey(Object row, Object fam, Object qual, long ts) {
    return new Key(toBytes(row), toBytes(fam), toBytes(qual), E, ts);
  }

  @Test
  public void testOneCharacterDifference() {
    // row has char that differs by one byte
    testKeys(new Key("r321hahahaha", "f89222", "q90232e"), new Key("r321hbhahaha", "f89222", "q90232e"), newKey(apendFF("r321ha"), E, E, 0));

    // family has char that differs by one byte
    testKeys(new Key("r321hahahaha", "f89222", "q90232e"), new Key("r321hahahaha", "f89322", "q90232e"), newKey("r321hahahaha", apendFF("f892"), E, 0));

    // qualifier has char that differs by one byte
    testKeys(new Key("r321hahahaha", "f89222", "q90232e"), new Key("r321hahahaha", "f89222", "q91232e"), newKey("r321hahahaha", "f89222", apendFF("q90"), 0));
  }

  @Test
  public void testMultiCharacterDifference() {
    // row has char that differs by two bytes
    testKeys(new Key("r321hahahaha", "f89222", "q90232e"), new Key("r321hchahaha", "f89222", "q90232e"), newKey("r321hb", E, E, 0));

    // family has char that differs by two bytes
    testKeys(new Key("r321hahahaha", "f89222", "q90232e"), new Key("r321hahahaha", "f89422", "q90232e"), newKey("r321hahahaha", "f893", E, 0));

    // qualifier has char that differs by two bytes
    testKeys(new Key("r321hahahaha", "f89222", "q90232e"), new Key("r321hahahaha", "f89222", "q92232e"), newKey("r321hahahaha", "f89222", "q91", 0));
  }

  @Test
  public void testOneCharacterDifferenceAndFF() {
    byte[] ff1 = Bytes.concat(apendFF("mop"), "b".getBytes());
    byte[] ff2 = Bytes.concat(apendFF("mop"), FF, "b".getBytes());

    byte[] eff1 = Bytes.concat(apendFF("mop"), FF, FF);
    byte[] eff2 = Bytes.concat(apendFF("mop"), FF, FF, FF);

    testKeys(newKey(ff1, "f89222", "q90232e", 34), new Key("mor56", "f89222", "q90232e"), newKey(eff1, E, E, 0));
    testKeys(newKey("r1", ff1, "q90232e", 34), new Key("r1", "mor56", "q90232e"), newKey("r1", eff1, E, 0));
    testKeys(newKey("r1", "f1", ff1, 34), new Key("r1", "f1", "mor56"), newKey("r1", "f1", eff1, 0));

    testKeys(newKey(ff2, "f89222", "q90232e", 34), new Key("mor56", "f89222", "q90232e"), newKey(eff2, E, E, 0));
    testKeys(newKey("r1", ff2, "q90232e", 34), new Key("r1", "mor56", "q90232e"), newKey("r1", eff2, E, 0));
    testKeys(newKey("r1", "f1", ff2, 34), new Key("r1", "f1", "mor56"), newKey("r1", "f1", eff2, 0));

  }

  @Test
  public void testOneCharacterDifferenceAtEnd() {
    testKeys(new Key("r321hahahaha", "f89222", "q90232e"), new Key("r321hahahahb", "f89222", "q90232e"), newKey(append00("r321hahahaha"), E, E, 0));
    testKeys(new Key("r321hahahaha", "f89222", "q90232e"), new Key("r321hahahaha", "f89223", "q90232e"), newKey("r321hahahaha", append00("f89222"), E, 0));
    testKeys(new Key("r321hahahaha", "f89222", "q90232e"), new Key("r321hahahaha", "f89222", "q90232f"),
        newKey("r321hahahaha", "f89222", append00("q90232e"), 0));
  }

  @Test
  public void testSamePrefix() {
    testKeys(new Key("r3boot4", "f89222", "q90232e"), new Key("r3boot452", "f89222", "q90232e"), newKey(append00("r3boot4"), E, E, 0));
    testKeys(new Key("r3boot4", "f892", "q90232e"), new Key("r3boot4", "f89222", "q90232e"), newKey("r3boot4", append00("f892"), E, 0));
    testKeys(new Key("r3boot4", "f89222", "q902"), new Key("r3boot4", "f89222", "q90232e"), newKey("r3boot4", "f89222", append00("q902"), 0));
  }

  @Test
  public void testSamePrefixAnd00() {
    Key prev = new Key("r3boot4", "f89222", "q90232e");
    Assert.assertEquals(prev, KeyShortener.shorten(prev, newKey(append00("r3boot4"), "f89222", "q90232e", 8)));
    prev = new Key("r3boot4", "f892", "q90232e");
    Assert.assertEquals(prev, KeyShortener.shorten(prev, newKey("r3boot4", append00("f892"), "q90232e", 8)));
    prev = new Key("r3boot4", "f89222", "q902");
    Assert.assertEquals(prev, KeyShortener.shorten(prev, newKey("r3boot4", "f89222", append00("q902"), 8)));
  }

  @Test
  public void testSanityCheck1() {
    // prev and shortened equal
    Key prev = new Key("r001", "f002", "q006");
    Assert.assertEquals(prev, KeyShortener.sanityCheck(prev, new Key("r002", "f002", "q006"), new Key("r001", "f002", "q006")));
    // prev > shortened equal
    Assert.assertEquals(prev, KeyShortener.sanityCheck(prev, new Key("r003", "f002", "q006"), new Key("r001", "f002", "q006")));
    // current and shortened equal
    Assert.assertEquals(prev, KeyShortener.sanityCheck(prev, new Key("r003", "f002", "q006"), new Key("r003", "f002", "q006")));
    // shortened > current
    Assert.assertEquals(prev, KeyShortener.sanityCheck(prev, new Key("r003", "f002", "q006"), new Key("r004", "f002", "q006")));
  }
}

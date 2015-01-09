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

import static org.junit.Assert.assertEquals;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.accumulo.core.data.ArrayByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.util.MutableByteSequence;
import org.apache.accumulo.core.util.UnsynchronizedBuffer;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class RelativeKeyTest {

  @Test
  public void testBasicRelativeKey() {
    assertEquals(1, UnsynchronizedBuffer.nextArraySize(0));
    assertEquals(1, UnsynchronizedBuffer.nextArraySize(1));
    assertEquals(2, UnsynchronizedBuffer.nextArraySize(2));
    assertEquals(4, UnsynchronizedBuffer.nextArraySize(3));
    assertEquals(4, UnsynchronizedBuffer.nextArraySize(4));
    assertEquals(8, UnsynchronizedBuffer.nextArraySize(5));
    assertEquals(8, UnsynchronizedBuffer.nextArraySize(8));
    assertEquals(16, UnsynchronizedBuffer.nextArraySize(9));

    assertEquals(1 << 16, UnsynchronizedBuffer.nextArraySize((1 << 16) - 1));
    assertEquals(1 << 16, UnsynchronizedBuffer.nextArraySize(1 << 16));
    assertEquals(1 << 17, UnsynchronizedBuffer.nextArraySize((1 << 16) + 1));

    assertEquals(1 << 30, UnsynchronizedBuffer.nextArraySize((1 << 30) - 1));

    assertEquals(1 << 30, UnsynchronizedBuffer.nextArraySize(1 << 30));

    assertEquals(Integer.MAX_VALUE, UnsynchronizedBuffer.nextArraySize(Integer.MAX_VALUE - 1));
    assertEquals(Integer.MAX_VALUE, UnsynchronizedBuffer.nextArraySize(Integer.MAX_VALUE));
  }

  @Test
  public void testCommonPrefix() {
    // exact matches
    ArrayByteSequence exact = new ArrayByteSequence("abc");
    assertEquals(-1, RelativeKey.getCommonPrefix(exact, exact));
    assertEquals(-1, commonPrefixHelper("", ""));
    assertEquals(-1, commonPrefixHelper("a", "a"));
    assertEquals(-1, commonPrefixHelper("aa", "aa"));
    assertEquals(-1, commonPrefixHelper("aaa", "aaa"));
    assertEquals(-1, commonPrefixHelper("abab", "abab"));
    assertEquals(-1, commonPrefixHelper(new String("aaa"), new ArrayByteSequence("aaa").toString()));
    assertEquals(-1, commonPrefixHelper("abababababab".substring(3, 6), "ccababababcc".substring(3, 6)));

    // no common prefix
    assertEquals(0, commonPrefixHelper("", "a"));
    assertEquals(0, commonPrefixHelper("a", ""));
    assertEquals(0, commonPrefixHelper("a", "b"));
    assertEquals(0, commonPrefixHelper("aaaa", "bbbb"));

    // some common prefix
    assertEquals(1, commonPrefixHelper("a", "ab"));
    assertEquals(1, commonPrefixHelper("ab", "ac"));
    assertEquals(1, commonPrefixHelper("ab", "ac"));
    assertEquals(2, commonPrefixHelper("aa", "aaaa"));
    assertEquals(4, commonPrefixHelper("aaaaa", "aaaab"));
  }

  private int commonPrefixHelper(String a, String b) {
    return RelativeKey.getCommonPrefix(new ArrayByteSequence(a), new ArrayByteSequence(b));
  }

  @Test
  public void testReadWritePrefix() throws IOException {
    Key prevKey = new Key("row1", "columnfamily1", "columnqualifier1", "columnvisibility1", 1000);
    Key newKey = new Key("row2", "columnfamily2", "columnqualifier2", "columnvisibility2", 3000);
    RelativeKey expected = new RelativeKey(prevKey, newKey);

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream out = new DataOutputStream(baos);
    expected.write(out);

    RelativeKey actual = new RelativeKey();
    actual.setPrevKey(prevKey);
    actual.readFields(new DataInputStream(new ByteArrayInputStream(baos.toByteArray())));

    assertEquals(expected.getKey(), actual.getKey());
  }

  private static ArrayList<Key> expectedKeys;
  private static ArrayList<Value> expectedValues;
  private static ArrayList<Integer> expectedPositions;
  private static ByteArrayOutputStream baos;

  @BeforeClass
  public static void initSource() throws IOException {
    int initialListSize = 10000;

    baos = new ByteArrayOutputStream();
    DataOutputStream out = new DataOutputStream(baos);

    expectedKeys = new ArrayList<Key>(initialListSize);
    expectedValues = new ArrayList<Value>(initialListSize);
    expectedPositions = new ArrayList<Integer>(initialListSize);

    Key prev = null;
    int val = 0;
    for (int row = 0; row < 4; row++) {
      String rowS = RFileTest.nf("r_", row);
      for (int cf = 0; cf < 4; cf++) {
        String cfS = RFileTest.nf("cf_", cf);
        for (int cq = 0; cq < 4; cq++) {
          String cqS = RFileTest.nf("cq_", cq);
          for (int cv = 'A'; cv < 'A' + 4; cv++) {
            String cvS = "" + (char) cv;
            for (int ts = 4; ts > 0; ts--) {
              Key k = RFileTest.nk(rowS, cfS, cqS, cvS, ts);
              k.setDeleted(true);
              Value v = RFileTest.nv("" + val);
              expectedPositions.add(out.size());
              new RelativeKey(prev, k).write(out);
              prev = k;
              v.write(out);
              expectedKeys.add(k);
              expectedValues.add(v);

              k = RFileTest.nk(rowS, cfS, cqS, cvS, ts);
              v = RFileTest.nv("" + val);
              expectedPositions.add(out.size());
              new RelativeKey(prev, k).write(out);
              prev = k;
              v.write(out);
              expectedKeys.add(k);
              expectedValues.add(v);

              val++;
            }
          }
        }
      }
    }
  }

  private DataInputStream in;

  @Before
  public void setupDataInputStream() {
    in = new DataInputStream(new ByteArrayInputStream(baos.toByteArray()));
    in.mark(0);
  }

  @Test
  public void testSeekBeforeEverything() throws IOException {
    Key seekKey = new Key();
    Key prevKey = new Key();
    Key currKey = null;
    MutableByteSequence value = new MutableByteSequence(new byte[64], 0, 0);

    RelativeKey.SkippR skippr = RelativeKey.fastSkip(in, seekKey, value, prevKey, currKey);
    assertEquals(1, skippr.skipped);
    assertEquals(new Key(), skippr.prevKey);
    assertEquals(expectedKeys.get(0), skippr.rk.getKey());
    assertEquals(expectedValues.get(0).toString(), value.toString());

    // ensure we can advance after fastskip
    skippr.rk.readFields(in);
    assertEquals(expectedKeys.get(1), skippr.rk.getKey());

    in.reset();

    seekKey = new Key("a", "b", "c", "d", 1);
    seekKey.setDeleted(true);
    skippr = RelativeKey.fastSkip(in, seekKey, value, prevKey, currKey);
    assertEquals(1, skippr.skipped);
    assertEquals(new Key(), skippr.prevKey);
    assertEquals(expectedKeys.get(0), skippr.rk.getKey());
    assertEquals(expectedValues.get(0).toString(), value.toString());

    skippr.rk.readFields(in);
    assertEquals(expectedKeys.get(1), skippr.rk.getKey());
  }

  @Test(expected = EOFException.class)
  public void testSeekAfterEverything() throws IOException {
    Key seekKey = new Key("s", "t", "u", "v", 1);
    Key prevKey = new Key();
    Key currKey = null;
    MutableByteSequence value = new MutableByteSequence(new byte[64], 0, 0);

    RelativeKey.fastSkip(in, seekKey, value, prevKey, currKey);
  }

  @Test
  public void testSeekMiddle() throws IOException {
    int seekIndex = expectedKeys.size() / 2;
    Key seekKey = expectedKeys.get(seekIndex);
    Key prevKey = new Key();
    Key currKey = null;
    MutableByteSequence value = new MutableByteSequence(new byte[64], 0, 0);

    RelativeKey.SkippR skippr = RelativeKey.fastSkip(in, seekKey, value, prevKey, currKey);

    assertEquals(seekIndex + 1, skippr.skipped);
    assertEquals(expectedKeys.get(seekIndex - 1), skippr.prevKey);
    assertEquals(expectedKeys.get(seekIndex), skippr.rk.getKey());
    assertEquals(expectedValues.get(seekIndex).toString(), value.toString());

    skippr.rk.readFields(in);
    assertEquals(expectedValues.get(seekIndex + 1).toString(), value.toString());

    // try fast skipping to a key that does not exist
    in.reset();
    Key fKey = expectedKeys.get(seekIndex).followingKey(PartialKey.ROW_COLFAM_COLQUAL);
    int i;
    for (i = seekIndex; expectedKeys.get(i).compareTo(fKey) < 0; i++) {}

    skippr = RelativeKey.fastSkip(in, expectedKeys.get(i), value, prevKey, currKey);
    assertEquals(i + 1, skippr.skipped);
    assertEquals(expectedKeys.get(i - 1), skippr.prevKey);
    assertEquals(expectedKeys.get(i), skippr.rk.getKey());
    assertEquals(expectedValues.get(i).toString(), value.toString());

    // try fast skipping to our current location
    skippr = RelativeKey.fastSkip(in, expectedKeys.get(i), value, expectedKeys.get(i - 1), expectedKeys.get(i));
    assertEquals(0, skippr.skipped);
    assertEquals(expectedKeys.get(i - 1), skippr.prevKey);
    assertEquals(expectedKeys.get(i), skippr.rk.getKey());
    assertEquals(expectedValues.get(i).toString(), value.toString());

    // try fast skipping 1 column family ahead from our current location, testing fastskip from middle of block as opposed to stating at beginning of block
    fKey = expectedKeys.get(i).followingKey(PartialKey.ROW_COLFAM);
    int j;
    for (j = i; expectedKeys.get(j).compareTo(fKey) < 0; j++) {}
    skippr = RelativeKey.fastSkip(in, fKey, value, expectedKeys.get(i - 1), expectedKeys.get(i));
    assertEquals(j - i, skippr.skipped);
    assertEquals(expectedKeys.get(j - 1), skippr.prevKey);
    assertEquals(expectedKeys.get(j), skippr.rk.getKey());
    assertEquals(expectedValues.get(j).toString(), value.toString());

  }
}

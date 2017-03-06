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
package org.apache.accumulo.test.functional;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.Random;
import java.util.TreeMap;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.util.Pair;
import org.apache.accumulo.test.categories.SunnyDayTests;
import org.apache.accumulo.tserver.NativeMap;
import org.apache.hadoop.io.Text;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(SunnyDayTests.class)
public class NativeMapIT {

  private Key newKey(int r) {
    return new Key(new Text(String.format("r%09d", r)));
  }

  private Key newKey(int r, int cf, int cq, int cv, int ts, boolean deleted) {
    Key k = new Key(new Text(String.format("r%09d", r)), new Text(String.format("cf%09d", cf)), new Text(String.format("cq%09d", cq)), new Text(String.format(
        "cv%09d", cv)), ts);

    k.setDeleted(deleted);

    return k;
  }

  private Value newValue(int v) {
    return new Value(String.format("r%09d", v).getBytes(UTF_8));
  }

  public static File nativeMapLocation() {
    File projectDir = new File(System.getProperty("user.dir")).getParentFile();
    File nativeMapDir = new File(projectDir, "server/native/target/accumulo-native-" + Constants.VERSION + "/accumulo-native-" + Constants.VERSION);
    return nativeMapDir;
  }

  @BeforeClass
  public static void setUp() {
    System.setProperty("accumulo.native.lib.path", nativeMapLocation().getAbsolutePath());
    Assert.assertTrue(NativeMap.isLoaded());
  }

  private void verifyIterator(int start, int end, int valueOffset, Iterator<Entry<Key,Value>> iter) {
    for (int i = start; i <= end; i++) {
      assertTrue(iter.hasNext());
      Entry<Key,Value> entry = iter.next();
      assertEquals(newKey(i), entry.getKey());
      assertEquals(newValue(i + valueOffset), entry.getValue());
    }

    assertFalse(iter.hasNext());
  }

  private void insertAndVerify(NativeMap nm, int start, int end, int valueOffset) {
    for (int i = start; i <= end; i++) {
      nm.put(newKey(i), newValue(i + valueOffset));
    }

    for (int i = start; i <= end; i++) {
      Value v = nm.get(newKey(i));
      assertNotNull(v);
      assertEquals(newValue(i + valueOffset), v);

      Iterator<Entry<Key,Value>> iter2 = nm.iterator(newKey(i));
      assertTrue(iter2.hasNext());
      Entry<Key,Value> entry = iter2.next();
      assertEquals(newKey(i), entry.getKey());
      assertEquals(newValue(i + valueOffset), entry.getValue());
    }

    assertNull(nm.get(newKey(start - 1)));

    assertNull(nm.get(newKey(end + 1)));

    Iterator<Entry<Key,Value>> iter = nm.iterator();
    verifyIterator(start, end, valueOffset, iter);

    for (int i = start; i <= end; i++) {
      iter = nm.iterator(newKey(i));
      verifyIterator(i, end, valueOffset, iter);

      // lookup nonexistant key that falls after existing key
      iter = nm.iterator(newKey(i, 1, 1, 1, 1, false));
      verifyIterator(i + 1, end, valueOffset, iter);
    }

    assertEquals(end - start + 1, nm.size());
  }

  private void insertAndVerifyExhaustive(NativeMap nm, int num, int run) {
    for (int i = 0; i < num; i++) {
      for (int j = 0; j < num; j++) {
        for (int k = 0; k < num; k++) {
          for (int l = 0; l < num; l++) {
            for (int ts = 0; ts < num; ts++) {
              Key key = newKey(i, j, k, l, ts, true);
              Value value = new Value((i + "_" + j + "_" + k + "_" + l + "_" + ts + "_" + true + "_" + run).getBytes(UTF_8));

              nm.put(key, value);

              key = newKey(i, j, k, l, ts, false);
              value = new Value((i + "_" + j + "_" + k + "_" + l + "_" + ts + "_" + false + "_" + run).getBytes(UTF_8));

              nm.put(key, value);
            }
          }
        }
      }
    }

    Iterator<Entry<Key,Value>> iter = nm.iterator();

    for (int i = 0; i < num; i++) {
      for (int j = 0; j < num; j++) {
        for (int k = 0; k < num; k++) {
          for (int l = 0; l < num; l++) {
            for (int ts = num - 1; ts >= 0; ts--) {
              Key key = newKey(i, j, k, l, ts, true);
              Value value = new Value((i + "_" + j + "_" + k + "_" + l + "_" + ts + "_" + true + "_" + run).getBytes(UTF_8));

              assertTrue(iter.hasNext());
              Entry<Key,Value> entry = iter.next();
              assertEquals(key, entry.getKey());
              assertEquals(value, entry.getValue());

              key = newKey(i, j, k, l, ts, false);
              value = new Value((i + "_" + j + "_" + k + "_" + l + "_" + ts + "_" + false + "_" + run).getBytes(UTF_8));

              assertTrue(iter.hasNext());
              entry = iter.next();
              assertEquals(key, entry.getKey());
              assertEquals(value, entry.getValue());
            }
          }
        }
      }
    }

    assertFalse(iter.hasNext());

    for (int i = 0; i < num; i++) {
      for (int j = 0; j < num; j++) {
        for (int k = 0; k < num; k++) {
          for (int l = 0; l < num; l++) {
            for (int ts = 0; ts < num; ts++) {
              Key key = newKey(i, j, k, l, ts, true);
              Value value = new Value((i + "_" + j + "_" + k + "_" + l + "_" + ts + "_" + true + "_" + run).getBytes(UTF_8));

              assertEquals(value, nm.get(key));

              Iterator<Entry<Key,Value>> iter2 = nm.iterator(key);
              assertTrue(iter2.hasNext());
              Entry<Key,Value> entry = iter2.next();
              assertEquals(key, entry.getKey());
              assertEquals(value, entry.getValue());

              key = newKey(i, j, k, l, ts, false);
              value = new Value((i + "_" + j + "_" + k + "_" + l + "_" + ts + "_" + false + "_" + run).getBytes(UTF_8));

              assertEquals(value, nm.get(key));

              Iterator<Entry<Key,Value>> iter3 = nm.iterator(key);
              assertTrue(iter3.hasNext());
              Entry<Key,Value> entry2 = iter3.next();
              assertEquals(key, entry2.getKey());
              assertEquals(value, entry2.getValue());
            }
          }
        }
      }
    }

    assertEquals(num * num * num * num * num * 2, nm.size());
  }

  @Test
  public void test1() {
    NativeMap nm = new NativeMap();
    Iterator<Entry<Key,Value>> iter = nm.iterator();
    assertFalse(iter.hasNext());
    nm.delete();
  }

  @Test
  public void test2() {
    NativeMap nm = new NativeMap();

    insertAndVerify(nm, 1, 10, 0);
    insertAndVerify(nm, 1, 10, 1);
    insertAndVerify(nm, 1, 10, 2);

    nm.delete();
  }

  @Test
  public void test4() {
    NativeMap nm = new NativeMap();

    insertAndVerifyExhaustive(nm, 3, 0);
    insertAndVerifyExhaustive(nm, 3, 1);

    nm.delete();
  }

  @Test
  public void test5() {
    NativeMap nm = new NativeMap();

    insertAndVerify(nm, 1, 10, 0);

    Iterator<Entry<Key,Value>> iter = nm.iterator();
    iter.next();

    nm.delete();

    try {
      nm.put(newKey(1), newValue(1));
      assertTrue(false);
    } catch (IllegalStateException e) {

    }

    try {
      nm.get(newKey(1));
      assertTrue(false);
    } catch (IllegalStateException e) {

    }

    try {
      nm.iterator();
      assertTrue(false);
    } catch (IllegalStateException e) {

    }

    try {
      nm.iterator(newKey(1));
      assertTrue(false);
    } catch (IllegalStateException e) {

    }

    try {
      nm.size();
      assertTrue(false);
    } catch (IllegalStateException e) {

    }

    try {
      iter.next();
      assertTrue(false);
    } catch (IllegalStateException e) {

    }

  }

  @Test
  public void test7() {
    NativeMap nm = new NativeMap();

    insertAndVerify(nm, 1, 10, 0);

    nm.delete();

    try {
      nm.delete();
      assertTrue(false);
    } catch (IllegalStateException e) {

    }
  }

  @Test
  public void test8() {
    // test verifies that native map sorts keys sharing some common prefix properly

    NativeMap nm = new NativeMap();

    TreeMap<Key,Value> tm = new TreeMap<>();

    tm.put(new Key(new Text("fo")), new Value(new byte[] {'0'}));
    tm.put(new Key(new Text("foo")), new Value(new byte[] {'1'}));
    tm.put(new Key(new Text("foo1")), new Value(new byte[] {'2'}));
    tm.put(new Key(new Text("foo2")), new Value(new byte[] {'3'}));

    for (Entry<Key,Value> entry : tm.entrySet()) {
      nm.put(entry.getKey(), entry.getValue());
    }

    Iterator<Entry<Key,Value>> iter = nm.iterator();

    for (Entry<Key,Value> entry : tm.entrySet()) {
      assertTrue(iter.hasNext());
      Entry<Key,Value> entry2 = iter.next();

      assertEquals(entry.getKey(), entry2.getKey());
      assertEquals(entry.getValue(), entry2.getValue());
    }

    assertFalse(iter.hasNext());

    nm.delete();
  }

  @Test
  public void test9() {
    NativeMap nm = new NativeMap();

    Iterator<Entry<Key,Value>> iter = nm.iterator();

    try {
      iter.next();
      assertTrue(false);
    } catch (NoSuchElementException e) {

    }

    insertAndVerify(nm, 1, 1, 0);

    iter = nm.iterator();
    iter.next();

    try {
      iter.next();
      assertTrue(false);
    } catch (NoSuchElementException e) {

    }

    nm.delete();
  }

  @Test
  public void test10() {
    int start = 1;
    int end = 10000;

    NativeMap nm = new NativeMap();
    for (int i = start; i <= end; i++) {
      nm.put(newKey(i), newValue(i));
    }

    long mem1 = nm.getMemoryUsed();

    for (int i = start; i <= end; i++) {
      nm.put(newKey(i), newValue(i));
    }

    long mem2 = nm.getMemoryUsed();

    if (mem1 != mem2) {
      throw new RuntimeException("Memory changed after inserting duplicate data " + mem1 + " " + mem2);
    }

    for (int i = start; i <= end; i++) {
      nm.put(newKey(i), newValue(i));
    }

    long mem3 = nm.getMemoryUsed();

    if (mem1 != mem3) {
      throw new RuntimeException("Memory changed after inserting duplicate data " + mem1 + " " + mem3);
    }

    byte bigrow[] = new byte[1000000];
    byte bigvalue[] = new byte[bigrow.length];

    for (int i = 0; i < bigrow.length; i++) {
      bigrow[i] = (byte) (0xff & (i % 256));
      bigvalue[i] = bigrow[i];
    }

    nm.put(new Key(new Text(bigrow)), new Value(bigvalue));

    long mem4 = nm.getMemoryUsed();

    Value val = nm.get(new Key(new Text(bigrow)));
    if (val == null || !val.equals(new Value(bigvalue))) {
      throw new RuntimeException("Did not get expected big value");
    }

    nm.put(new Key(new Text(bigrow)), new Value(bigvalue));

    long mem5 = nm.getMemoryUsed();

    if (mem4 != mem5) {
      throw new RuntimeException("Memory changed after inserting duplicate data " + mem4 + " " + mem5);
    }

    val = nm.get(new Key(new Text(bigrow)));
    if (val == null || !val.equals(new Value(bigvalue))) {
      throw new RuntimeException("Did not get expected big value");
    }

    nm.delete();
  }

  // random length random field
  private static byte[] getRandomBytes(Random r, int maxLen) {
    int len = r.nextInt(maxLen);

    byte f[] = new byte[len];
    r.nextBytes(f);

    return f;
  }

  @Test
  public void test11() {
    NativeMap nm = new NativeMap();

    // insert things with varying field sizes and value sizes

    // generate random data
    Random r = new Random(75);

    ArrayList<Pair<Key,Value>> testData = new ArrayList<>();

    for (int i = 0; i < 100000; i++) {

      Key k = new Key(getRandomBytes(r, 97), getRandomBytes(r, 13), getRandomBytes(r, 31), getRandomBytes(r, 11), (r.nextLong() & 0x7fffffffffffffffl), false,
          false);
      Value v = new Value(getRandomBytes(r, 511));

      testData.add(new Pair<>(k, v));
    }

    // insert unsorted data
    for (Pair<Key,Value> pair : testData) {
      nm.put(pair.getFirst(), pair.getSecond());
    }

    for (int i = 0; i < 2; i++) {

      // sort data
      Collections.sort(testData, new Comparator<Pair<Key,Value>>() {
        @Override
        public int compare(Pair<Key,Value> o1, Pair<Key,Value> o2) {
          return o1.getFirst().compareTo(o2.getFirst());
        }
      });

      // verify
      Iterator<Entry<Key,Value>> iter1 = nm.iterator();
      Iterator<Pair<Key,Value>> iter2 = testData.iterator();

      while (iter1.hasNext() && iter2.hasNext()) {
        Entry<Key,Value> e = iter1.next();
        Pair<Key,Value> p = iter2.next();

        if (!e.getKey().equals(p.getFirst()))
          throw new RuntimeException("Keys not equal");

        if (!e.getValue().equals(p.getSecond()))
          throw new RuntimeException("Values not equal");
      }

      if (iter1.hasNext())
        throw new RuntimeException("Not all of native map consumed");

      if (iter2.hasNext())
        throw new RuntimeException("Not all of test data consumed");

      System.out.println("test 11 nm mem " + nm.getMemoryUsed());

      // insert data again w/ different value
      Collections.shuffle(testData, r);
      // insert unsorted data
      for (Pair<Key,Value> pair : testData) {
        pair.getSecond().set(getRandomBytes(r, 511));
        nm.put(pair.getFirst(), pair.getSecond());
      }
    }

    nm.delete();
  }

  @Test
  public void testBinary() {
    NativeMap nm = new NativeMap();

    byte emptyBytes[] = new byte[0];

    for (int i = 0; i < 256; i++) {
      for (int j = 0; j < 256; j++) {
        byte row[] = new byte[] {'r', (byte) (0xff & i), (byte) (0xff & j)};
        byte data[] = new byte[] {'v', (byte) (0xff & i), (byte) (0xff & j)};

        Key k = new Key(row, emptyBytes, emptyBytes, emptyBytes, 1);
        Value v = new Value(data);

        nm.put(k, v);
      }
    }

    Iterator<Entry<Key,Value>> iter = nm.iterator();
    for (int i = 0; i < 256; i++) {
      for (int j = 0; j < 256; j++) {
        byte row[] = new byte[] {'r', (byte) (0xff & i), (byte) (0xff & j)};
        byte data[] = new byte[] {'v', (byte) (0xff & i), (byte) (0xff & j)};

        Key k = new Key(row, emptyBytes, emptyBytes, emptyBytes, 1);
        Value v = new Value(data);

        assertTrue(iter.hasNext());
        Entry<Key,Value> entry = iter.next();

        assertEquals(k, entry.getKey());
        assertEquals(v, entry.getValue());

      }
    }

    assertFalse(iter.hasNext());

    for (int i = 0; i < 256; i++) {
      for (int j = 0; j < 256; j++) {
        byte row[] = new byte[] {'r', (byte) (0xff & i), (byte) (0xff & j)};
        byte data[] = new byte[] {'v', (byte) (0xff & i), (byte) (0xff & j)};

        Key k = new Key(row, emptyBytes, emptyBytes, emptyBytes, 1);
        Value v = new Value(data);

        Value v2 = nm.get(k);

        assertEquals(v, v2);
      }
    }

    nm.delete();
  }

  @Test
  public void testEmpty() {
    NativeMap nm = new NativeMap();

    assertTrue(nm.size() == 0);
    assertTrue(nm.getMemoryUsed() == 0);

    nm.delete();
  }

  @Test
  public void testConcurrentIter() throws IOException {
    NativeMap nm = new NativeMap();

    nm.put(newKey(0), newValue(0));
    nm.put(newKey(1), newValue(1));
    nm.put(newKey(3), newValue(3));

    SortedKeyValueIterator<Key,Value> iter = nm.skvIterator();

    // modify map after iter created
    nm.put(newKey(2), newValue(2));

    assertTrue(iter.hasTop());
    assertEquals(iter.getTopKey(), newKey(0));
    iter.next();

    assertTrue(iter.hasTop());
    assertEquals(iter.getTopKey(), newKey(1));
    iter.next();

    assertTrue(iter.hasTop());
    assertEquals(iter.getTopKey(), newKey(2));
    iter.next();

    assertTrue(iter.hasTop());
    assertEquals(iter.getTopKey(), newKey(3));
    iter.next();

    assertFalse(iter.hasTop());

    nm.delete();
  }

}

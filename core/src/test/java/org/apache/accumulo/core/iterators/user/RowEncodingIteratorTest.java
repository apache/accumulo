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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.IteratorUtil;
import org.apache.accumulo.core.iteratorsImpl.system.SortedMapIterator;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.Test;

public class RowEncodingIteratorTest {

  private static final class DummyIteratorEnv implements IteratorEnvironment {
    @Override
    public IteratorUtil.IteratorScope getIteratorScope() {
      return IteratorUtil.IteratorScope.scan;
    }

    @Override
    public boolean isFullMajorCompaction() {
      return false;
    }
  }

  private static final class RowEncodingIteratorImpl extends RowEncodingIterator {

    public static SortedMap<Key,Value> decodeRow(Value rowValue) throws IOException {
      DataInputStream dis = new DataInputStream(new ByteArrayInputStream(rowValue.get()));
      int numKeys = dis.readInt();
      List<Key> decodedKeys = new ArrayList<>();
      List<Value> decodedValues = new ArrayList<>();
      SortedMap<Key,Value> out = new TreeMap<>();
      for (int i = 0; i < numKeys; i++) {
        Key k = new Key();
        k.readFields(dis);
        decodedKeys.add(k);
      }
      int numValues = dis.readInt();
      for (int i = 0; i < numValues; i++) {
        Value v = new Value();
        v.readFields(dis);
        decodedValues.add(v);
      }
      if (decodedKeys.size() != decodedValues.size()) {
        throw new IOException("Number of keys doesn't match number of values");
      }
      for (int i = 0; i < decodedKeys.size(); i++) {
        out.put(decodedKeys.get(i), decodedValues.get(i));
      }
      return out;
    }

    @Override
    public SortedMap<Key,Value> rowDecoder(Key rowKey, Value rowValue) throws IOException {
      return decodeRow(rowValue);
    }

    @Override
    public Value rowEncoder(List<Key> keys, List<Value> values) throws IOException {
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      DataOutputStream dos = new DataOutputStream(baos);
      dos.writeInt(keys.size());
      for (Key key : keys) {
        key.write(dos);
      }
      dos.writeInt(values.size());
      for (Value v : values) {
        v.write(dos);
      }
      dos.flush();
      return new Value(baos.toByteArray());
    }
  }

  private void pkv(SortedMap<Key,Value> map, String row, String cf, String cq, String cv, long ts,
      byte[] val) {
    map.put(new Key(new Text(row), new Text(cf), new Text(cq), new Text(cv), ts),
        new Value(val, true));
  }

  @Test
  public void testEncodeAll() throws IOException {
    byte[] kbVal = new byte[1024];
    // This code is shamelessly borrowed from the WholeRowIteratorTest.
    SortedMap<Key,Value> map1 = new TreeMap<>();
    pkv(map1, "row1", "cf1", "cq1", "cv1", 5, kbVal);
    pkv(map1, "row1", "cf1", "cq2", "cv1", 6, kbVal);

    SortedMap<Key,Value> map2 = new TreeMap<>();
    pkv(map2, "row2", "cf1", "cq1", "cv1", 5, kbVal);
    pkv(map2, "row2", "cf1", "cq2", "cv1", 6, kbVal);

    SortedMap<Key,Value> map3 = new TreeMap<>();
    pkv(map3, "row3", "cf1", "cq1", "cv1", 5, kbVal);
    pkv(map3, "row3", "cf1", "cq2", "cv1", 6, kbVal);

    SortedMap<Key,Value> map = new TreeMap<>();
    map.putAll(map1);
    map.putAll(map2);
    map.putAll(map3);
    SortedMapIterator src = new SortedMapIterator(map);
    Range range = new Range(new Text("row1"), true, new Text("row2"), true);
    RowEncodingIteratorImpl iter = new RowEncodingIteratorImpl();
    Map<String,String> bigBufferOpts = new HashMap<>();
    bigBufferOpts.put(RowEncodingIterator.MAX_BUFFER_SIZE_OPT, "3K");
    iter.init(src, bigBufferOpts, new DummyIteratorEnv());
    iter.seek(range, new ArrayList<>(), false);

    assertTrue(iter.hasTop());
    assertEquals(map1, RowEncodingIteratorImpl.decodeRow(iter.getTopValue()));

    // simulate something continuing using the last key from the iterator
    // this is what client and server code will do
    range = new Range(iter.getTopKey(), false, range.getEndKey(), range.isEndKeyInclusive());
    iter.seek(range, new ArrayList<>(), false);

    assertTrue(iter.hasTop());
    assertEquals(map2, RowEncodingIteratorImpl.decodeRow(iter.getTopValue()));

    iter.next();

    assertFalse(iter.hasTop());
  }

  @Test
  public void testEncodeSome() throws IOException {
    byte[] kbVal = new byte[1024];
    // This code is shamelessly borrowed from the WholeRowIteratorTest.
    SortedMap<Key,Value> map1 = new TreeMap<>();
    pkv(map1, "row1", "cf1", "cq1", "cv1", 5, kbVal);
    pkv(map1, "row1", "cf1", "cq2", "cv1", 6, kbVal);

    SortedMap<Key,Value> map = new TreeMap<>();
    map.putAll(map1);
    SortedMapIterator src = new SortedMapIterator(map);
    Range range = new Range(new Text("row1"), true, new Text("row2"), true);
    RowEncodingIteratorImpl iter = new RowEncodingIteratorImpl();
    Map<String,String> bigBufferOpts = new HashMap<>();
    bigBufferOpts.put(RowEncodingIterator.MAX_BUFFER_SIZE_OPT, "1K");
    iter.init(src, bigBufferOpts, new DummyIteratorEnv());
    assertThrows(IllegalArgumentException.class, () -> iter.seek(range, new ArrayList<>(), false));
    // IllegalArgumentException should be thrown as we can't fit the whole row into its buffer
  }
}

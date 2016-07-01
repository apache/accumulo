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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;

/**
 *
 * The WholeRowIterator is designed to provide row-isolation so that queries see mutations as atomic. It does so by encapsulating an entire row of key/value
 * pairs into a single key/value pair, which is returned through the client as an atomic operation.
 *
 * <p>
 * This iterator extends the {@link RowEncodingIterator}, providing implementations for rowEncoder and rowDecoder which serializes all column and value
 * information from a given row into a single ByteStream in a value.
 *
 * <p>
 * As with the RowEncodingIterator, when seeking in the WholeRowIterator using a range that starts at a non-inclusive first key in a row, this iterator will
 * skip to the next row.
 *
 * <p>
 * To regain the original key/value pairs of the row, call the decodeRow function on the key/value pair that this iterator returned.
 *
 * @see RowFilter
 */
public class WholeRowIterator extends RowEncodingIterator {
  public WholeRowIterator() {}

  WholeRowIterator(SortedKeyValueIterator<Key,Value> source) {
    this.sourceIter = source;
  }

  @Override
  public SortedMap<Key,Value> rowDecoder(Key rowKey, Value rowValue) throws IOException {
    return decodeRow(rowKey, rowValue);
  }

  @Override
  public Value rowEncoder(List<Key> keys, List<Value> values) throws IOException {
    return encodeRow(keys, values);
  }

  /**
   * Returns the byte array containing the field of row key from the given DataInputStream din. Assumes that din first has the length of the field, followed by
   * the field itself.
   */
  private static byte[] readField(DataInputStream din) throws IOException {
    int len = din.readInt();
    byte[] b = new byte[len];
    int readLen = din.read(b);
    // Check if expected length is not same as read length.
    // We ignore the zero length case because DataInputStream.read can return -1
    // if zero length was expected and end of stream has been reached.
    if (len > 0 && len != readLen) {
      throw new IOException(String.format("Expected to read %d bytes but read %d", len, readLen));
    }
    return b;
  }

  // decode a bunch of key value pairs that have been encoded into a single value
  public static final SortedMap<Key,Value> decodeRow(Key rowKey, Value rowValue) throws IOException {
    SortedMap<Key,Value> map = new TreeMap<>();
    ByteArrayInputStream in = new ByteArrayInputStream(rowValue.get());
    DataInputStream din = new DataInputStream(in);
    int numKeys = din.readInt();
    for (int i = 0; i < numKeys; i++) {
      byte[] cf = readField(din); // read the col fam
      byte[] cq = readField(din); // read the col qual
      byte[] cv = readField(din); // read the col visibility
      long timestamp = din.readLong(); // read the timestamp
      byte[] valBytes = readField(din); // read the value
      map.put(new Key(rowKey.getRowData().toArray(), cf, cq, cv, timestamp, false, false), new Value(valBytes, false));
    }
    return map;
  }

  // take a stream of keys and values and output a value that encodes everything but their row
  // keys and values must be paired one for one
  public static final Value encodeRow(List<Key> keys, List<Value> values) throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    DataOutputStream dout = new DataOutputStream(out);
    dout.writeInt(keys.size());
    for (int i = 0; i < keys.size(); i++) {
      Key k = keys.get(i);
      Value v = values.get(i);
      // write the colfam
      {
        ByteSequence bs = k.getColumnFamilyData();
        dout.writeInt(bs.length());
        dout.write(bs.getBackingArray(), bs.offset(), bs.length());
      }
      // write the colqual
      {
        ByteSequence bs = k.getColumnQualifierData();
        dout.writeInt(bs.length());
        dout.write(bs.getBackingArray(), bs.offset(), bs.length());
      }
      // write the column visibility
      {
        ByteSequence bs = k.getColumnVisibilityData();
        dout.writeInt(bs.length());
        dout.write(bs.getBackingArray(), bs.offset(), bs.length());
      }
      // write the timestamp
      dout.writeLong(k.getTimestamp());
      // write the value
      byte[] valBytes = v.get();
      dout.writeInt(valBytes.length);
      dout.write(valBytes);
    }

    return new Value(out.toByteArray());
  }
}

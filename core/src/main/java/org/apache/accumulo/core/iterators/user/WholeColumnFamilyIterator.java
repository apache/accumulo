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
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.OptionDescriber;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.hadoop.io.Text;

/**
 *
 * The WholeColumnFamilyIterator is designed to provide row/cf-isolation so that queries see mutations as atomic. It does so by grouping row/Column family (as
 * key) and rest of data as Value into a single key/value pair, which is returned through the client as an atomic operation.
 *
 * To regain the original key/value pairs of the row, call the decodeRow function on the key/value pair that this iterator returned.
 *
 * @since 1.6.0
 */
public class WholeColumnFamilyIterator implements SortedKeyValueIterator<Key,Value>, OptionDescriber {

  private SortedKeyValueIterator<Key,Value> sourceIter;
  private Key topKey = null;
  private Value topValue = null;

  public WholeColumnFamilyIterator() {

  }

  WholeColumnFamilyIterator(SortedKeyValueIterator<Key,Value> source) {
    this.sourceIter = source;
  }

  /**
   * Decode whole row/cf out of value. decode key value pairs that have been encoded into a single // value
   *
   * @param rowKey
   *          the row key to decode
   * @param rowValue
   *          the value to decode
   * @return the sorted map. After decoding the flattened data map
   * @throws IOException
   *           Signals that an I/O exception has occurred.
   */
  public static final SortedMap<Key,Value> decodeColumnFamily(Key rowKey, Value rowValue) throws IOException {
    SortedMap<Key,Value> map = new TreeMap<Key,Value>();
    ByteArrayInputStream in = new ByteArrayInputStream(rowValue.get());
    DataInputStream din = new DataInputStream(in);
    int numKeys = din.readInt();
    for (int i = 0; i < numKeys; i++) {
      byte[] cq;
      byte[] cv;
      byte[] valBytes;
      // read the col qual
      {
        int len = din.readInt();
        cq = new byte[len];
        din.read(cq);
      }
      // read the col visibility
      {
        int len = din.readInt();
        cv = new byte[len];
        din.read(cv);
      }
      // read the timestamp
      long timestamp = din.readLong();
      // read the value
      {
        int len = din.readInt();
        valBytes = new byte[len];
        din.read(valBytes);
      }
      map.put(new Key(rowKey.getRowData().toArray(), rowKey.getColumnFamilyData().toArray(), cq, cv, timestamp, false, false), new Value(valBytes, false));
    }
    return map;
  }

  /**
   * Encode row/cf. Take a stream of keys and values and output a value that encodes everything but their row and column families keys and values must be paired
   * one for one
   *
   * @param keys
   *          the row keys to encode into value
   * @param values
   *          the value to encode
   * @return the value. After encoding keys/values
   * @throws IOException
   *           Signals that an I/O exception has occurred.
   */
  public static final Value encodeColumnFamily(List<Key> keys, List<Value> values) throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    DataOutputStream dout = new DataOutputStream(out);
    dout.writeInt(keys.size());
    for (int i = 0; i < keys.size(); i++) {
      Key k = keys.get(i);
      Value v = values.get(i);
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

  List<Key> keys = new ArrayList<Key>();
  List<Value> values = new ArrayList<Value>();

  private void prepKeys() throws IOException {
    if (topKey != null)
      return;
    Text currentRow;
    Text currentCf;

    do {
      if (sourceIter.hasTop() == false)
        return;
      currentRow = new Text(sourceIter.getTopKey().getRow());
      currentCf = new Text(sourceIter.getTopKey().getColumnFamily());

      keys.clear();
      values.clear();
      while (sourceIter.hasTop() && sourceIter.getTopKey().getRow().equals(currentRow) && sourceIter.getTopKey().getColumnFamily().equals(currentCf)) {
        keys.add(new Key(sourceIter.getTopKey()));
        values.add(new Value(sourceIter.getTopValue()));
        sourceIter.next();
      }
    } while (!filter(currentRow, keys, values));

    topKey = new Key(currentRow, currentCf);
    topValue = encodeColumnFamily(keys, values);

  }

  /**
   *
   * @param currentRow
   *          All keys & cf have this in their row portion (do not modify!).
   * @param keys
   *          One key for each key & cf group in the row, ordered as they are given by the source iterator (do not modify!).
   * @param values
   *          One value for each key in keys, ordered to correspond to the ordering in keys (do not modify!).
   * @return true if we want to keep the row, false if we want to skip it
   */
  protected boolean filter(Text currentRow, List<Key> keys, List<Value> values) {
    return true;
  }

  @Override
  public SortedKeyValueIterator<Key,Value> deepCopy(IteratorEnvironment env) {
    if (sourceIter != null)
      return new WholeColumnFamilyIterator(sourceIter.deepCopy(env));
    return new WholeColumnFamilyIterator();
  }

  @Override
  public Key getTopKey() {
    return topKey;
  }

  @Override
  public Value getTopValue() {
    return topValue;
  }

  @Override
  public boolean hasTop() {
    return topKey != null || sourceIter.hasTop();
  }

  @Override
  public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options, IteratorEnvironment env) throws IOException {
    sourceIter = source;
  }

  @Override
  public void next() throws IOException {
    topKey = null;
    topValue = null;
    prepKeys();
  }

  @Override
  public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {
    topKey = null;
    topValue = null;

    Key sk = range.getStartKey();

    if (sk != null && sk.getColumnQualifierData().length() == 0 && sk.getColumnVisibilityData().length() == 0 && sk.getTimestamp() == Long.MAX_VALUE
        && !range.isStartKeyInclusive()) {
      // assuming that we are seeking using a key previously returned by
      // this iterator
      // therefore go to the next row/cf
      Key followingRowKey = sk.followingKey(PartialKey.ROW_COLFAM);
      if (range.getEndKey() != null && followingRowKey.compareTo(range.getEndKey()) > 0)
        return;

      range = new Range(sk.followingKey(PartialKey.ROW_COLFAM), true, range.getEndKey(), range.isEndKeyInclusive());
    }

    sourceIter.seek(range, columnFamilies, inclusive);
    prepKeys();
  }

  @Override
  public IteratorOptions describeOptions() {
    return new IteratorOptions("wholecolumnfamilyiterator", "WholeColumnFamilyIterator. Group equal row & column family into single row entry.", null, null);
  }

  @Override
  public boolean validateOptions(Map<String,String> options) {
    return true;
  }

}

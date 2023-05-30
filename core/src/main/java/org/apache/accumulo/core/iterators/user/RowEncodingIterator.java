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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;

import org.apache.accumulo.core.conf.ConfigurationTypeHelper;
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
 * The RowEncodingIterator is designed to provide row-isolation so that queries see mutations as
 * atomic. It does so by encapsulating an entire row of key/value pairs into a single key/value
 * pair, which is returned through the client as an atomic operation. This is an abstract class,
 * allowing the user to implement rowEncoder and rowDecoder such that the columns and values of a
 * given row may be encoded in a format best suited to the client.
 *
 * <p>
 * For an example implementation, see {@link WholeRowIterator}.
 *
 * <p>
 * One caveat is that when seeking in the WholeRowIterator using a range that starts at a
 * non-inclusive first key in a row, (e.g. seek(new Range(new Key(new Text("row")),false,...),...))
 * this iterator will skip to the next row. This is done in order to prevent repeated scanning of
 * the same row when system automatically creates ranges of that form, which happens in the case of
 * the client calling continueScan, or in the case of the tablet server continuing a scan after
 * swapping out sources.
 *
 * <p>
 * To regain the original key/value pairs of the row, call the rowDecoder function on the key/value
 * pair that this iterator returned.
 *
 * @see RowFilter
 */
public abstract class RowEncodingIterator
    implements SortedKeyValueIterator<Key,Value>, OptionDescriber {

  public static final String MAX_BUFFER_SIZE_OPT = "maxBufferSize";
  private static final long DEFAULT_MAX_BUFFER_SIZE = Long.MAX_VALUE;

  protected SortedKeyValueIterator<Key,Value> sourceIter;
  private Key topKey = null;
  private Value topValue = null;
  private long maxBufferSize = DEFAULT_MAX_BUFFER_SIZE;

  // decode a bunch of key value pairs that have been encoded into a single value
  /**
   * Given a value generated by the rowEncoder implementation, recreate the original Key, Value
   * pairs.
   */
  public abstract SortedMap<Key,Value> rowDecoder(Key rowKey, Value rowValue) throws IOException;

  /**
   * Take a stream of keys and values. Return values in the same order encoded such that all
   * portions of the key (except for the row value) and the original value are encoded in some way.
   */
  public abstract Value rowEncoder(List<Key> keys, List<Value> values) throws IOException;

  @Override
  public SortedKeyValueIterator<Key,Value> deepCopy(IteratorEnvironment env) {
    RowEncodingIterator newInstance;
    try {
      newInstance = this.getClass().getDeclaredConstructor().newInstance();
    } catch (ReflectiveOperationException e) {
      throw new IllegalStateException(e);
    }
    newInstance.sourceIter = sourceIter.deepCopy(env);
    newInstance.maxBufferSize = maxBufferSize;
    return newInstance;
  }

  List<Key> keys = new ArrayList<>();
  List<Value> values = new ArrayList<>();

  private void prepKeys() throws IOException {
    long kvBufSize = 0;
    if (topKey != null) {
      return;
    }
    Text currentRow;
    do {
      if (!sourceIter.hasTop()) {
        return;
      }
      currentRow = new Text(sourceIter.getTopKey().getRow());
      keys.clear();
      values.clear();
      while (sourceIter.hasTop() && sourceIter.getTopKey().getRow().equals(currentRow)) {
        Key sourceTopKey = sourceIter.getTopKey();
        Value sourceTopValue = sourceIter.getTopValue();
        keys.add(new Key(sourceTopKey));
        values.add(new Value(sourceTopValue));
        kvBufSize += sourceTopKey.getSize() + sourceTopValue.getSize() + 128;
        if (kvBufSize > maxBufferSize) {
          throw new IllegalArgumentException(
              "Exceeded buffer size of " + maxBufferSize + " for row: " + sourceTopKey.getRow());
        }
        sourceIter.next();
      }
    } while (!filter(currentRow, keys, values));

    topKey = new Key(currentRow);
    topValue = rowEncoder(keys, values);
  }

  /**
   *
   * @param currentRow All keys have this in their row portion (do not modify!).
   * @param keys One key for each key in the row, ordered as they are given by the source iterator
   *        (do not modify!).
   * @param values One value for each key in keys, ordered to correspond to the ordering in keys (do
   *        not modify!).
   * @return true if we want to keep the row, false if we want to skip it
   */
  protected boolean filter(Text currentRow, List<Key> keys, List<Value> values) {
    return true;
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
    return topKey != null;
  }

  @Override
  public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options,
      IteratorEnvironment env) throws IOException {
    sourceIter = source;
    if (options.containsKey(MAX_BUFFER_SIZE_OPT)) {
      maxBufferSize =
          ConfigurationTypeHelper.getFixedMemoryAsBytes(options.get(MAX_BUFFER_SIZE_OPT));
    }
  }

  @Override
  public IteratorOptions describeOptions() {
    String desc = "This iterator encapsulates an entire row of Key/Value pairs"
        + " into a single Key/Value pair.";
    String bufferDesc = "Maximum buffer size (in accumulo memory spec) to use"
        + " for buffering keys before throwing a BufferOverflowException.";
    HashMap<String,String> namedOptions = new HashMap<>();
    namedOptions.put(MAX_BUFFER_SIZE_OPT, bufferDesc);
    return new IteratorOptions(getClass().getSimpleName(), desc, namedOptions, null);
  }

  @Override
  public boolean validateOptions(Map<String,String> options) {
    String maxBufferSizeStr = options.get(MAX_BUFFER_SIZE_OPT);
    try {
      ConfigurationTypeHelper.getFixedMemoryAsBytes(maxBufferSizeStr);
    } catch (Exception e) {
      throw new IllegalArgumentException(
          "Failed to parse opt " + MAX_BUFFER_SIZE_OPT + " " + maxBufferSizeStr, e);
    }
    return true;
  }

  @Override
  public void next() throws IOException {
    topKey = null;
    topValue = null;
    prepKeys();
  }

  @Override
  public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive)
      throws IOException {
    topKey = null;
    topValue = null;

    Key sk = range.getStartKey();

    if (sk != null && sk.getColumnFamilyData().length() == 0
        && sk.getColumnQualifierData().length() == 0 && sk.getColumnVisibilityData().length() == 0
        && sk.getTimestamp() == Long.MAX_VALUE && !range.isStartKeyInclusive()) {
      // assuming that we are seeking using a key previously returned by this iterator
      // therefore go to the next row
      Key followingRowKey = sk.followingKey(PartialKey.ROW);
      if (range.getEndKey() != null && followingRowKey.compareTo(range.getEndKey()) > 0) {
        return;
      }

      range = new Range(sk.followingKey(PartialKey.ROW), true, range.getEndKey(),
          range.isEndKeyInclusive());
    }

    sourceIter.seek(range, columnFamilies, inclusive);
    prepKeys();
  }

}

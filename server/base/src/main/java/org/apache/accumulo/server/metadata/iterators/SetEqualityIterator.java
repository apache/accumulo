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
package org.apache.accumulo.server.metadata.iterators;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.function.Function;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.clientImpl.lexicoder.ByteUtils;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Condition;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.server.metadata.ConditionalTabletMutatorImpl;
import org.apache.hadoop.io.Text;

import com.google.common.base.Preconditions;

/**
 * This iterator exists to enable checking for set equality in a conditional mutation. It allows
 * comparing a set in a client process to a set encoded in column qualifiers within a tablet.
 */
public class SetEqualityIterator implements SortedKeyValueIterator<Key,Value> {

  // ELASTICITY_TODO unit test this iterator

  private SortedKeyValueIterator<Key,Value> source;

  private Key startKey = null;
  private Value topValue = null;

  private boolean includeValue = false;
  private static final String ENCODE_VALUE_OPTION = "ENCODE_VALUE";

  @Override
  public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive)
      throws IOException {

    Text tabletRow = LocationExistsIterator.getTabletRow(range);
    Text family = range.getStartKey().getColumnFamily();

    Preconditions.checkArgument(
        family.getLength() > 0 && range.getStartKey().getColumnQualifier().getLength() == 0);

    startKey = new Key(tabletRow, family);
    Key endKey = new Key(tabletRow, family).followingKey(PartialKey.ROW_COLFAM);

    Range r = new Range(startKey, true, endKey, false);

    source.seek(r, Set.of(), false);

    try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos)) {

      int count = 0;

      while (source.hasTop()) {
        if (includeValue) {
          byte[] ba = source.getTopKey().getColumnQualifierData().toArray();
          byte[] valueData = source.getTopValue().get();
          byte[] encodedData = encodeKeyValue(ba, valueData);
          dos.writeInt(encodedData.length);
          dos.write(encodedData, 0, encodedData.length);
        } else {
          // only encode the key/qualifier
          byte[] ba = source.getTopKey().getColumnQualifierData().toArray();
          dos.writeInt(ba.length);
          dos.write(ba, 0, ba.length);
        }
        source.next();
        count++;
      }
      // The lenght is written last so that buffering can be avoided in this iterator.
      dos.writeInt(count);
      topValue = new Value(baos.toByteArray());
    }
  }

  @Override
  public Key getTopKey() {
    if (startKey == null) {
      throw new IllegalStateException("never been seeked");
    }
    if (topValue == null) {
      throw new NoSuchElementException();
    }

    return startKey;
  }

  @Override
  public Value getTopValue() {
    if (startKey == null) {
      throw new IllegalStateException("never been seeked");
    }
    if (topValue == null) {
      throw new NoSuchElementException();
    }
    return topValue;
  }

  @Override
  public SortedKeyValueIterator<Key,Value> deepCopy(IteratorEnvironment env) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean isRunningLowOnMemory() {
    return source.isRunningLowOnMemory();
  }

  @Override
  public boolean hasTop() {
    if (startKey == null) {
      throw new IllegalStateException("never been seeked");
    }
    return topValue != null;
  }

  @Override
  public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options,
      IteratorEnvironment env) throws IOException {
    this.source = source;
    String includeValueOption = options.get("includeValue");
    this.includeValue = includeValueOption != null && Boolean.parseBoolean(includeValueOption);

  }

  @Override
  public void next() throws IOException {
    if (startKey == null) {
      throw new IllegalStateException("never been seeked");
    }
    topValue = null;
  }

  /**
   * If two sets are equals and they are encoded with this method then the resulting byte arrays
   * should be equal.
   */
  private static <T> byte[] encode(Set<T> set, Function<T,byte[]> encoder) {
    try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos)) {
      set.stream().map(encoder).sorted(Arrays::compare).forEach(ba -> {
        try {
          dos.writeInt(ba.length);
          dos.write(ba, 0, ba.length);
        } catch (IOException ioe) {
          throw new UncheckedIOException(ioe);
        }

      });

      dos.writeInt(set.size());

      dos.close();

      return baos.toByteArray();
    } catch (IOException ioe) {
      throw new UncheckedIOException(ioe);
    }
  }

  private static final Text EMPTY = new Text();

  public static <T> Condition createCondition(Collection<T> set, Function<T,byte[]> encoder,
      Text family) {
    Preconditions.checkArgument(set instanceof Set);
    IteratorSetting is = new IteratorSetting(ConditionalTabletMutatorImpl.INITIAL_ITERATOR_PRIO,
        SetEqualityIterator.class);
    return new Condition(family, EMPTY).setValue(encode((Set<T>) set, encoder)).setIterators(is);
  }

  public static <K,T> Condition createCondition(Map<K,T> map,
      Function<Map.Entry<K,T>,Map.Entry<byte[],byte[]>> entryEncoder, Text family) {
    IteratorSetting is = new IteratorSetting(ConditionalTabletMutatorImpl.INITIAL_ITERATOR_PRIO,
        SetEqualityIterator.class);
    is.addOption(ENCODE_VALUE_OPTION, "true");
    Function<Map.Entry<K,T>,byte[]> encoder = entry -> {
      Map.Entry<byte[],byte[]> byteEntry = entryEncoder.apply(entry);

      return encodeKeyValue(byteEntry.getKey(), byteEntry.getValue());
    };
    return new Condition(family, EMPTY).setValue(encode(map.entrySet(), encoder)).setIterators(is);
  }

  private static byte[] encodeKeyValue(byte[] key, byte[] value) {
    return ByteUtils.concat(ByteUtils.escape(key), ByteUtils.escape(value));
  }

}

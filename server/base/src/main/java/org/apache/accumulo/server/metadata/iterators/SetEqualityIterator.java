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

import static java.nio.charset.StandardCharsets.UTF_8;

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
 * comparing a set in a client process to a set encoded in column qualifiers within a tablet. If the
 * "concat.value" options is supplied and is true, then the bytes from the Value will be added with
 * a null byte separator.
 */
public class SetEqualityIterator implements SortedKeyValueIterator<Key,Value> {

  public static final String CONCAT_VALUE = "concat.value";
  public static final String VALUE_SEPARATOR = "\u0000";
  private static final byte[] VALUE_SEPARATOR_BYTES = VALUE_SEPARATOR.getBytes(UTF_8);
  private static final int VALUE_SEPARATOR_BYTES_LENGTH = VALUE_SEPARATOR_BYTES.length;

  private SortedKeyValueIterator<Key,Value> source;

  private Key startKey = null;
  private Value topValue = null;
  private boolean concat = false;

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
        byte[] bytesToWrite = null;
        byte[] ba = source.getTopKey().getColumnQualifierData().toArray();
        if (concat) {
          byte[] val = source.getTopValue().get();
          bytesToWrite = new byte[ba.length + VALUE_SEPARATOR_BYTES_LENGTH + val.length];
          System.arraycopy(ba, 0, bytesToWrite, 0, ba.length);
          System.arraycopy(VALUE_SEPARATOR_BYTES, 0, bytesToWrite, ba.length,
              VALUE_SEPARATOR_BYTES_LENGTH);
          System.arraycopy(val, 0, bytesToWrite, ba.length + VALUE_SEPARATOR_BYTES_LENGTH,
              val.length);
        } else {
          bytesToWrite = ba;
        }
        dos.writeInt(bytesToWrite.length);
        dos.write(bytesToWrite, 0, bytesToWrite.length);
        source.next();
        count++;
      }

      // The length is written last so that buffering can be avoided in this iterator.
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
    this.concat = Boolean.parseBoolean(options.getOrDefault(CONCAT_VALUE, Boolean.toString(false)));
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
      Text family, boolean concatValue) {
    Preconditions.checkArgument(set instanceof Set);
    IteratorSetting is = new IteratorSetting(ConditionalTabletMutatorImpl.INITIAL_ITERATOR_PRIO,
        SetEqualityIterator.class);
    is.addOption(SetEqualityIterator.CONCAT_VALUE, Boolean.toString(concatValue));
    return new Condition(family, EMPTY).setValue(encode((Set<T>) set, encoder)).setIterators(is);
  }

}

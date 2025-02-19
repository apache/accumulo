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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.data.Condition;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.util.Pair;
import org.apache.accumulo.server.metadata.ConditionalTabletMutatorImpl;
import org.apache.hadoop.io.Text;

import com.google.common.base.Preconditions;

/**
 * This iterator exists to enable checking for set equality in a conditional mutation. The
 * createCondition methods allow the client to create conditions for specific column families in a
 * tablets metadata. The conditions will check for equality based on the value in the column
 * qualifier or values in the column qualifier and Value.
 *
 * <h2>Options</h2>
 * <ul>
 * <li><b>concat.value:</b> This option must be supplied. If true, then the bytes from the Value
 * will be concatenated with a null byte separator.</li>
 * </ul>
 */
public class SetEncodingIterator extends ColumnFamilyTransformationIterator {

  public static final String CONCAT_VALUE = "concat.value";
  private static final String VALUE_SEPARATOR = "\u0000";
  private static final byte[] VALUE_SEPARATOR_BYTES = VALUE_SEPARATOR.getBytes(UTF_8);
  private static final int VALUE_SEPARATOR_BYTES_LENGTH = VALUE_SEPARATOR_BYTES.length;

  private boolean concat = false;

  @Override
  protected Value transform(SortedKeyValueIterator<Key,Value> source) throws IOException {
    try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos)) {

      int count = 0;

      while (source.hasTop()) {
        final byte[] bytesToWrite;
        byte[] ba = source.getTopKey().getColumnQualifierData().toArray();
        if (concat) {
          byte[] val = source.getTopValue().get();
          bytesToWrite = encodeKeyValue(ba, val);
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

      return new Value(baos.toByteArray());
    }
  }

  @Override
  public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options,
      IteratorEnvironment env) throws IOException {
    super.init(source, options, env);
    String concat = options.get(CONCAT_VALUE);
    if (concat == null || !(concat.equalsIgnoreCase("true") || concat.equalsIgnoreCase("false"))) {
      throw new IllegalArgumentException(
          CONCAT_VALUE + " option must be supplied with a value of 'true' or 'false'");
    }
    this.concat = Boolean.parseBoolean(concat);
  }

  /**
   * If two sets are equals and they are encoded with this method then the resulting byte arrays
   * should be equal.
   */
  private static <T> byte[] encode(Set<T> set, Function<T,byte[]> encoder) {
    try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos)) {

      if (set.size() == 1) {
        // no need to sort or use a stream when the set is size 1.
        var ba = encoder.apply(set.iterator().next());
        dos.writeInt(ba.length);
        dos.write(ba, 0, ba.length);
      } else if (set.size() > 1) {
        List<byte[]> copy = new ArrayList<>(set.size());
        for (var entry : set) {
          copy.add(encoder.apply(entry));
        }
        copy.sort(Arrays::compare);
        for (var ba : copy) {
          dos.writeInt(ba.length);
          dos.write(ba, 0, ba.length);
        }
      }

      dos.writeInt(set.size());

      dos.close();

      return baos.toByteArray();
    } catch (IOException ioe) {
      throw new UncheckedIOException(ioe);
    }
  }

  private static byte[] encodeKeyValue(byte[] key, byte[] val) {
    var bytesToWrite = new byte[key.length + VALUE_SEPARATOR_BYTES_LENGTH + val.length];
    System.arraycopy(key, 0, bytesToWrite, 0, key.length);
    System.arraycopy(VALUE_SEPARATOR_BYTES, 0, bytesToWrite, key.length,
        VALUE_SEPARATOR_BYTES_LENGTH);
    System.arraycopy(val, 0, bytesToWrite, key.length + VALUE_SEPARATOR_BYTES_LENGTH, val.length);
    return bytesToWrite;
  }

  /*
   * Create a condition that will check the column qualifier values of the rows in the tablets
   * metadata with the matching family against a set of values produced by the encoder function.
   */
  public static <T> Condition createCondition(Collection<T> set, Function<T,byte[]> encoder,
      Text family) {
    Preconditions.checkArgument(set instanceof Set);
    IteratorSetting is = new IteratorSetting(ConditionalTabletMutatorImpl.INITIAL_ITERATOR_PRIO,
        SetEncodingIterator.class);
    is.addOption(SetEncodingIterator.CONCAT_VALUE, Boolean.toString(false));
    return new Condition(family, EMPTY).setValue(encode((Set<T>) set, encoder)).setIterators(is);
  }

  /*
   * Create a condition that will check the column qualifier and Value values of the rows in the
   * tablets metadata with the matching family against a set of values produced by the encoder
   * function.
   */
  public static <T> Condition createConditionWithVal(Collection<T> set,
      Function<T,Pair<byte[],byte[]>> encoder, Text family) {
    Preconditions.checkArgument(set instanceof Set);
    IteratorSetting is = new IteratorSetting(ConditionalTabletMutatorImpl.INITIAL_ITERATOR_PRIO,
        SetEncodingIterator.class);
    is.addOption(SetEncodingIterator.CONCAT_VALUE, Boolean.toString(true));
    return new Condition(family, EMPTY).setValue(encode((Set<T>) set, s -> {
      Pair<byte[],byte[]> kv = encoder.apply(s);
      return encodeKeyValue(kv.getFirst(), kv.getSecond());
    })).setIterators(is);
  }

}

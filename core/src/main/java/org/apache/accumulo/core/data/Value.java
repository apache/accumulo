/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.accumulo.core.data;

import static com.google.common.base.Charsets.UTF_8;
import static org.apache.accumulo.core.util.ByteBufferUtil.toBytes;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

import com.google.common.base.Preconditions;

/**
 * A byte sequence that is usable as a key or value. Based on {@link org.apache.hadoop.io.BytesWritable} only this class is NOT resizable and DOES NOT
 * distinguish between the size of the sequence and the current capacity as {@link org.apache.hadoop.io.BytesWritable} does. Hence its comparatively
 * 'immutable'.
 */
public class Value implements WritableComparable<Object> {
  private static final byte[] EMPTY = new byte[0];
  protected byte[] value;

  /**
   * Create a zero-size sequence.
   */
  public Value() {
    this(EMPTY, false);
  }

  /**
   * Create a Value using the byte array as the initial value.
   *
   * @param bytes
   *          May not be null
   */
  public Value(byte[] bytes) {
    this(bytes, false);
  }

  /**
   * Create a Value using a copy of the ByteBuffer's content.
   *
   * @param bytes
   *          May not be null
   */
  public Value(ByteBuffer bytes) {
    /* TODO ACCUMULO-2509 right now this uses the entire backing array, which must be accessible. */
    this(toBytes(bytes), false);
  }

  /**
   * @param bytes
   *          may not be null
   * @deprecated A copy of the bytes in the buffer is always made. Use {@link #Value(ByteBuffer)} instead.
   */
  @Deprecated
  public Value(ByteBuffer bytes, boolean copy) {
    /* TODO ACCUMULO-2509 right now this uses the entire backing array, which must be accessible. */
    this(toBytes(bytes), false);
  }

  /**
   * Create a Value based on the given bytes.
   *
   * @param bytes
   *          may not be null
   * @param copy
   *          signal if Value must make its own copy of bytes, or if it can use the array directly.
   */
  public Value(byte[] bytes, boolean copy) {
    Preconditions.checkNotNull(bytes);
    if (!copy) {
      this.value = bytes;
    } else {
      this.value = new byte[bytes.length];
      System.arraycopy(bytes, 0, this.value, 0, bytes.length);
    }

  }

  /**
   * Set the new Value to a copy of the contents of the passed <code>ibw</code>.
   *
   * @param ibw
   *          may not be null.
   */
  public Value(final Value ibw) {
    this(ibw.get(), 0, ibw.getSize());
  }

  /**
   * Set the value to a copy of the given byte range
   *
   * @param newData
   *          source of copy, may not be null
   * @param offset
   *          the offset in newData to start at
   * @param length
   *          the number of bytes to copy
   */
  public Value(final byte[] newData, final int offset, final int length) {
    Preconditions.checkNotNull(newData);
    this.value = new byte[length];
    System.arraycopy(newData, offset, this.value, 0, length);
  }

  /**
   * @return the underlying byte array directly.
   */
  public byte[] get() {
    assert (null != value);
    return this.value;
  }

  /**
   * @param b
   *          Use passed bytes as backing array for this instance, may not be null.
   */
  public void set(final byte[] b) {
    Preconditions.checkNotNull(b);
    this.value = b;
  }

  /**
   *
   * @param b
   *          copy the given byte array, may not be null.
   */
  public void copy(byte[] b) {
    Preconditions.checkNotNull(b);
    this.value = new byte[b.length];
    System.arraycopy(b, 0, this.value, 0, b.length);
  }

  /**
   * @return the current size of the underlying buffer.
   */
  public int getSize() {
    assert (null != value);
    return this.value.length;
  }

  @Override
  public void readFields(final DataInput in) throws IOException {
    this.value = new byte[in.readInt()];
    in.readFully(this.value, 0, this.value.length);
  }

  @Override
  public void write(final DataOutput out) throws IOException {
    out.writeInt(this.value.length);
    out.write(this.value, 0, this.value.length);
  }

  // Below methods copied from BytesWritable

  @Override
  public int hashCode() {
    return WritableComparator.hashBytes(value, this.value.length);
  }

  /**
   * Define the sort order of the BytesWritable.
   *
   * @param right_obj
   *          The other bytes writable
   * @return Positive if left is bigger than right, 0 if they are equal, and negative if left is smaller than right.
   */
  @Override
  public int compareTo(Object right_obj) {
    return compareTo(((Value) right_obj).get());
  }

  /**
   * Compares the bytes in this object to the specified byte array
   *
   * @return Positive if left is bigger than right, 0 if they are equal, and negative if left is smaller than right.
   */
  public int compareTo(final byte[] that) {
    int diff = this.value.length - that.length;
    return (diff != 0) ? diff : WritableComparator.compareBytes(this.value, 0, this.value.length, that, 0, that.length);
  }

  @Override
  public boolean equals(Object right_obj) {
    if (right_obj instanceof byte[]) {
      return compareTo((byte[]) right_obj) == 0;
    }
    if (right_obj instanceof Value) {
      return compareTo(right_obj) == 0;
    }
    return false;
  }

  @Override
  public String toString() {
    return new String(get(), UTF_8);
  }

  /**
   * A Comparator optimized for Value.
   */
  public static class Comparator extends WritableComparator {
    private BytesWritable.Comparator comparator = new BytesWritable.Comparator();

    /** constructor */
    public Comparator() {
      super(Value.class);
    }

    @Override
    public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
      return comparator.compare(b1, s1, l1, b2, s2, l2);
    }
  }

  static { // register this comparator
    WritableComparator.define(Value.class, new Comparator());
  }

  public static byte[][] toArray(final List<byte[]> array) {
    // List#toArray doesn't work on lists of byte [].
    byte[][] results = new byte[array.size()][];
    for (int i = 0; i < array.size(); i++) {
      results[i] = array.get(i);
    }
    return results;
  }

}

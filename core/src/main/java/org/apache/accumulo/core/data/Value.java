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
package org.apache.accumulo.core.data;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;
import static org.apache.accumulo.core.util.ByteBufferUtil.toBytes;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * A byte sequence that is usable as a key or value. Based on
 * {@link org.apache.hadoop.io.BytesWritable} only this class is NOT resizable and DOES NOT
 * distinguish between the size of the sequence and the current capacity as
 * {@link org.apache.hadoop.io.BytesWritable} does. Hence it is comparatively 'immutable'.
 */
public class Value implements WritableComparable<Object> {
  private static final byte[] EMPTY = new byte[0];
  protected byte[] value;

  /**
   * Creates a zero-size sequence.
   */
  public Value() {
    this(EMPTY, false);
  }

  /**
   * Creates a value using the UTF-8 encoding of the CharSequence
   *
   * @param cs may not be null
   *
   * @since 1.8.0
   */
  public Value(CharSequence cs) {
    this(cs.toString().getBytes(UTF_8));
  }

  /**
   * Creates a Value using the bytes of the Text. Makes a copy, does not use the byte array from the
   * Text.
   *
   * @param text may not be null
   *
   * @since 1.8.0
   */
  public Value(Text text) {
    this(text.getBytes(), 0, text.getLength());
  }

  /**
   * Creates a Value using a byte array as the initial value. The given byte array is used directly
   * as the backing array, so later changes made to the array reflect into the new Value.
   *
   * @param bytes May not be null
   */
  public Value(byte[] bytes) {
    this(bytes, false);
  }

  /**
   * Creates a Value using the bytes in a buffer as the initial value. Makes a defensive copy.
   *
   * @param bytes May not be null
   */
  public Value(ByteBuffer bytes) {
    /* TODO ACCUMULO-2509 right now this uses the entire backing array, which must be accessible. */
    this(toBytes(bytes), false);
  }

  /**
   * Creates a Value using a byte array as the initial value.
   *
   * @param bytes may not be null
   * @param copy false to use the given byte array directly as the backing array, true to force a
   *        copy
   */
  public Value(byte[] bytes, boolean copy) {
    requireNonNull(bytes);
    if (copy) {
      this.value = new byte[bytes.length];
      System.arraycopy(bytes, 0, this.value, 0, bytes.length);
    } else {
      this.value = bytes;
    }

  }

  /**
   * Creates a new Value based on another.
   *
   * @param ibw may not be null.
   */
  public Value(final Value ibw) {
    this(ibw.get(), 0, ibw.getSize());
  }

  /**
   * Creates a Value based on a range in a byte array. A copy of the bytes is always made.
   *
   * @param newData source of copy, may not be null
   * @param offset the offset in newData to start with for value bytes
   * @param length the number of bytes in the value
   * @throws IndexOutOfBoundsException if offset or length are invalid
   */
  public Value(final byte[] newData, final int offset, final int length) {
    requireNonNull(newData);
    this.value = new byte[length];
    System.arraycopy(newData, offset, this.value, 0, length);
  }

  /**
   * Gets the byte data of this value.
   *
   * @return the underlying byte array directly.
   */
  public byte[] get() {
    assert (value != null);
    return this.value;
  }

  /**
   * Sets the byte data of this value. The given byte array is used directly as the backing array,
   * so later changes made to the array reflect into this Value.
   *
   * @param b may not be null
   */
  public void set(final byte[] b) {
    requireNonNull(b);
    this.value = b;
  }

  /**
   * Sets the byte data of this value. The given byte array is copied.
   *
   * @param b may not be null
   */
  public void copy(byte[] b) {
    requireNonNull(b);
    this.value = new byte[b.length];
    System.arraycopy(b, 0, this.value, 0, b.length);
  }

  /**
   * Gets the size of this value.
   *
   * @return size in bytes
   */
  public int getSize() {
    assert (value != null);
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
   * @param right_obj The other bytes writable
   * @return Positive if left is bigger than right, 0 if they are equal, and negative if left is
   *         smaller than right.
   */
  @Override
  public int compareTo(Object right_obj) {
    return compareTo(((Value) right_obj).get());
  }

  /**
   * Compares the bytes in this object to the specified byte array
   *
   * @return Positive if left is bigger than right, 0 if they are equal, and negative if left is
   *         smaller than right.
   */
  public int compareTo(final byte[] that) {
    int diff = this.value.length - that.length;
    return (diff != 0) ? diff
        : WritableComparator.compareBytes(this.value, 0, this.value.length, that, 0, that.length);
  }

  @Override
  public boolean equals(Object right_obj) {
    if (right_obj instanceof Value) {
      return compareTo(right_obj) == 0;
    }
    return false;
  }

  /**
   * Compares the bytes in this object to the specified byte array
   *
   * @return true if the contents of this Value is equivalent to the supplied byte array
   * @since 2.0.0
   */
  public boolean contentEquals(byte[] right_obj) {
    return compareTo(right_obj) == 0;
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

}

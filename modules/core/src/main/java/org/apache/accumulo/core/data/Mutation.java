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
package org.apache.accumulo.core.data;

import static java.util.Objects.requireNonNull;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.accumulo.core.data.thrift.TMutation;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.accumulo.core.util.ByteBufferUtil;
import org.apache.accumulo.core.util.UnsynchronizedBuffer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

/**
 * Mutation represents an action that manipulates a row in a table. A mutation holds a list of column/value pairs that represent an atomic set of modifications
 * to make to a row.
 *
 * <p>
 * Convenience methods which takes columns and value as CharSequence (String implements CharSequence) are provided. CharSequence is converted to UTF-8 by
 * constructing a new Text object.
 *
 * <p>
 * When always passing in the same data as a CharSequence/String, it's probably more efficient to call the Text put methods. This way the data is only encoded
 * once and only one Text object is created.
 *
 * <p>
 * All of the put methods append data to the mutation; they do not overwrite anything that was previously put. The mutation holds a list of all columns/values
 * that were put into it.
 *
 * <p>
 * The putDelete() methods do not remove something that was previously added to the mutation; rather, they indicate that Accumulo should insert a delete marker
 * for that row column. A delete marker effectively hides entries for that row column with a timestamp earlier than the marker's. (The hidden data is eventually
 * removed during Accumulo garbage collection.)
 */
public class Mutation implements Writable {

  /**
   * Internally, this class keeps most mutation data in a byte buffer. If a cell value put into a mutation exceeds this size, then it is stored in a separate
   * buffer, and a reference to it is inserted into the main buffer.
   */
  static final int VALUE_SIZE_COPY_CUTOFF = 1 << 15;

  /**
   * Formats available for serializing Mutations. The formats are described in a <a href="doc-files/mutation-serialization.html">separate document</a>.
   */
  public static enum SERIALIZED_FORMAT {
    VERSION1, VERSION2
  }

  private boolean useOldDeserialize = false;
  private byte[] row;
  private byte[] data;
  private int entries;
  private List<byte[]> values;

  private UnsynchronizedBuffer.Writer buffer;

  private List<ColumnUpdate> updates;

  private static final Set<String> EMPTY = Collections.emptySet();
  private Set<String> replicationSources = EMPTY;

  private static final byte[] EMPTY_BYTES = new byte[0];

  private void serialize() {
    if (buffer != null) {
      data = buffer.toArray();
      buffer = null;
    }
  }

  /**
   * This is so hashCode and equals can be called without changing this object.
   *
   * It will return a copy of the current data buffer if serialized has not been called previously. Otherwise, this.data will be returned since the buffer is
   * null and will not change.
   */
  private ByteBuffer serializedSnapshot() {
    if (buffer != null) {
      return this.buffer.toByteBuffer();
    } else {
      return ByteBuffer.wrap(this.data);
    }
  }

  /**
   * Creates a new mutation. A defensive copy is made.
   *
   * @param row
   *          row ID
   * @since 1.5.0
   */
  public Mutation(byte[] row) {
    this(row, 0, row.length);
  }

  /**
   * Creates a new mutation. A defensive copy is made.
   *
   * @param row
   *          row ID
   * @param initialBufferSize
   *          the initial size, in bytes, of the internal buffer for serializing
   * @since 1.7.0
   */
  public Mutation(byte[] row, int initialBufferSize) {
    this(row, 0, row.length, initialBufferSize);
  }

  /**
   * Creates a new mutation. A defensive copy is made.
   *
   * @param row
   *          byte array containing row ID
   * @param start
   *          starting index of row ID in byte array
   * @param length
   *          length of row ID in byte array
   * @throws IndexOutOfBoundsException
   *           if start or length is invalid
   * @since 1.5.0
   */
  public Mutation(byte[] row, int start, int length) {
    this(row, start, length, 64);
  }

  /**
   * Creates a new mutation. A defensive copy is made.
   *
   * @param row
   *          byte array containing row ID
   * @param start
   *          starting index of row ID in byte array
   * @param length
   *          length of row ID in byte array
   * @param initialBufferSize
   *          the initial size, in bytes, of the internal buffer for serializing
   * @throws IndexOutOfBoundsException
   *           if start or length is invalid
   * @since 1.7.0
   */
  public Mutation(byte[] row, int start, int length, int initialBufferSize) {
    this.row = new byte[length];
    System.arraycopy(row, start, this.row, 0, length);
    buffer = new UnsynchronizedBuffer.Writer(initialBufferSize);
  }

  /**
   * Creates a new mutation. A defensive copy is made.
   *
   * @param row
   *          row ID
   */
  public Mutation(Text row) {
    this(row.getBytes(), 0, row.getLength());
  }

  /**
   * Creates a new mutation. A defensive copy is made.
   *
   * @param row
   *          row ID
   * @param initialBufferSize
   *          the initial size, in bytes, of the internal buffer for serializing
   * @since 1.7.0
   */
  public Mutation(Text row, int initialBufferSize) {
    this(row.getBytes(), 0, row.getLength(), initialBufferSize);
  }

  /**
   * Creates a new mutation.
   *
   * @param row
   *          row ID
   */
  public Mutation(CharSequence row) {
    this(new Text(row.toString()));
  }

  /**
   * Creates a new mutation.
   *
   * @param row
   *          row ID
   * @param initialBufferSize
   *          the initial size, in bytes, of the internal buffer for serializing
   * @since 1.7.0
   */
  public Mutation(CharSequence row, int initialBufferSize) {
    this(new Text(row.toString()), initialBufferSize);
  }

  /**
   * Creates a new mutation.
   */
  public Mutation() {}

  /**
   * Creates a new mutation from a Thrift mutation.
   *
   * @param tmutation
   *          Thrift mutation
   */
  public Mutation(TMutation tmutation) {
    this.row = ByteBufferUtil.toBytes(tmutation.row);
    this.data = ByteBufferUtil.toBytes(tmutation.data);
    this.entries = tmutation.entries;
    this.values = ByteBufferUtil.toBytesList(tmutation.values);

    if (tmutation.isSetSources()) {
      this.replicationSources = new HashSet<>(tmutation.sources);
    }

    if (this.row == null) {
      throw new IllegalArgumentException("null row");
    }
    if (this.data == null) {
      throw new IllegalArgumentException("null serialized data");
    }
  }

  /**
   * Creates a new mutation by copying another.
   *
   * @param m
   *          mutation to copy
   */
  public Mutation(Mutation m) {
    m.serialize();
    this.row = m.row;
    this.data = m.data;
    this.entries = m.entries;
    this.values = m.values;
    this.replicationSources = m.replicationSources;
  }

  /**
   * Gets the row ID for this mutation. Not a defensive copy.
   *
   * @return row ID
   */
  public byte[] getRow() {
    return row;
  }

  private void put(byte b[]) {
    put(b, b.length);
  }

  private void put(byte b[], int length) {
    buffer.writeVLong(length);
    buffer.add(b, 0, length);
  }

  private void put(boolean b) {
    buffer.add(b);
  }

  private void put(int i) {
    buffer.writeVLong(i);
  }

  private void put(long l) {
    buffer.writeVLong(l);
  }

  private void put(byte[] cf, byte[] cq, byte[] cv, boolean hasts, long ts, boolean deleted, byte[] val) {
    put(cf, cf.length, cq, cq.length, cv, hasts, ts, deleted, val, val.length);
  }

  /*
   * When dealing with Text object the length must be gotten from the object, not from the byte array.
   */
  private void put(Text cf, Text cq, byte[] cv, boolean hasts, long ts, boolean deleted, byte[] val) {
    put(cf.getBytes(), cf.getLength(), cq.getBytes(), cq.getLength(), cv, hasts, ts, deleted, val, val.length);
  }

  private void put(byte[] cf, int cfLength, byte[] cq, int cqLength, byte[] cv, boolean hasts, long ts, boolean deleted, byte[] val, int valLength) {
    if (buffer == null) {
      throw new IllegalStateException("Can not add to mutation after serializing it");
    }
    put(cf, cfLength);
    put(cq, cqLength);
    put(cv);
    put(hasts);
    if (hasts) {
      put(ts);
    }
    put(deleted);

    if (valLength < VALUE_SIZE_COPY_CUTOFF) {
      put(val, valLength);
    } else {
      if (values == null) {
        values = new ArrayList<>();
      }
      byte copy[] = new byte[valLength];
      System.arraycopy(val, 0, copy, 0, valLength);
      values.add(copy);
      put(-1 * values.size());
    }

    entries++;
  }

  private void put(CharSequence cf, CharSequence cq, byte[] cv, boolean hasts, long ts, boolean deleted, byte[] val) {
    put(new Text(cf.toString()), new Text(cq.toString()), cv, hasts, ts, deleted, val);
  }

  private void put(Text cf, Text cq, byte[] cv, boolean hasts, long ts, boolean deleted, Text val) {
    put(cf.getBytes(), cf.getLength(), cq.getBytes(), cq.getLength(), cv, hasts, ts, deleted, val.getBytes(), val.getLength());
  }

  private void put(CharSequence cf, CharSequence cq, byte[] cv, boolean hasts, long ts, boolean deleted, CharSequence val) {
    put(new Text(cf.toString()), new Text(cq.toString()), cv, hasts, ts, deleted, new Text(val.toString()));
  }

  /**
   * Puts a modification in this mutation. Column visibility is empty; timestamp is not set. All parameters are defensively copied.
   *
   * @param columnFamily
   *          column family
   * @param columnQualifier
   *          column qualifier
   * @param value
   *          cell value
   */
  public void put(Text columnFamily, Text columnQualifier, Value value) {
    put(columnFamily, columnQualifier, EMPTY_BYTES, false, 0l, false, value.get());
  }

  /**
   * Puts a modification in this mutation. Timestamp is not set. All parameters are defensively copied.
   *
   * @param columnFamily
   *          column family
   * @param columnQualifier
   *          column qualifier
   * @param columnVisibility
   *          column visibility
   * @param value
   *          cell value
   */
  public void put(Text columnFamily, Text columnQualifier, ColumnVisibility columnVisibility, Value value) {
    put(columnFamily, columnQualifier, columnVisibility.getExpression(), false, 0l, false, value.get());
  }

  /**
   * Puts a modification in this mutation. Column visibility is empty. All appropriate parameters are defensively copied.
   *
   * @param columnFamily
   *          column family
   * @param columnQualifier
   *          column qualifier
   * @param timestamp
   *          timestamp
   * @param value
   *          cell value
   */
  public void put(Text columnFamily, Text columnQualifier, long timestamp, Value value) {
    put(columnFamily, columnQualifier, EMPTY_BYTES, true, timestamp, false, value.get());
  }

  /**
   * Puts a modification in this mutation. All appropriate parameters are defensively copied.
   *
   * @param columnFamily
   *          column family
   * @param columnQualifier
   *          column qualifier
   * @param columnVisibility
   *          column visibility
   * @param timestamp
   *          timestamp
   * @param value
   *          cell value
   */
  public void put(Text columnFamily, Text columnQualifier, ColumnVisibility columnVisibility, long timestamp, Value value) {
    put(columnFamily, columnQualifier, columnVisibility.getExpression(), true, timestamp, false, value.get());
  }

  /**
   * Puts a deletion in this mutation. Matches empty column visibility; timestamp is not set. All parameters are defensively copied.
   *
   * @param columnFamily
   *          column family
   * @param columnQualifier
   *          column qualifier
   */
  public void putDelete(Text columnFamily, Text columnQualifier) {
    put(columnFamily, columnQualifier, EMPTY_BYTES, false, 0l, true, EMPTY_BYTES);
  }

  /**
   * Puts a deletion in this mutation. Timestamp is not set. All parameters are defensively copied.
   *
   * @param columnFamily
   *          column family
   * @param columnQualifier
   *          column qualifier
   * @param columnVisibility
   *          column visibility
   */
  public void putDelete(Text columnFamily, Text columnQualifier, ColumnVisibility columnVisibility) {
    put(columnFamily, columnQualifier, columnVisibility.getExpression(), false, 0l, true, EMPTY_BYTES);
  }

  /**
   * Puts a deletion in this mutation. Matches empty column visibility. All appropriate parameters are defensively copied.
   *
   * @param columnFamily
   *          column family
   * @param columnQualifier
   *          column qualifier
   * @param timestamp
   *          timestamp
   */
  public void putDelete(Text columnFamily, Text columnQualifier, long timestamp) {
    put(columnFamily, columnQualifier, EMPTY_BYTES, true, timestamp, true, EMPTY_BYTES);
  }

  /**
   * Puts a deletion in this mutation. All appropriate parameters are defensively copied.
   *
   * @param columnFamily
   *          column family
   * @param columnQualifier
   *          column qualifier
   * @param columnVisibility
   *          column visibility
   * @param timestamp
   *          timestamp
   */
  public void putDelete(Text columnFamily, Text columnQualifier, ColumnVisibility columnVisibility, long timestamp) {
    put(columnFamily, columnQualifier, columnVisibility.getExpression(), true, timestamp, true, EMPTY_BYTES);
  }

  /**
   * Puts a modification in this mutation. Column visibility is empty; timestamp is not set. All parameters are defensively copied.
   *
   * @param columnFamily
   *          column family
   * @param columnQualifier
   *          column qualifier
   */
  public void put(CharSequence columnFamily, CharSequence columnQualifier, Value value) {
    put(columnFamily, columnQualifier, EMPTY_BYTES, false, 0l, false, value.get());
  }

  /**
   * Puts a modification in this mutation. Timestamp is not set. All parameters are defensively copied.
   *
   * @param columnFamily
   *          column family
   * @param columnQualifier
   *          column qualifier
   * @param columnVisibility
   *          column visibility
   * @param value
   *          cell value
   */
  public void put(CharSequence columnFamily, CharSequence columnQualifier, ColumnVisibility columnVisibility, Value value) {
    put(columnFamily, columnQualifier, columnVisibility.getExpression(), false, 0l, false, value.get());
  }

  /**
   * Puts a modification in this mutation. Column visibility is empty. All appropriate parameters are defensively copied.
   *
   * @param columnFamily
   *          column family
   * @param columnQualifier
   *          column qualifier
   * @param timestamp
   *          timestamp
   * @param value
   *          cell value
   */
  public void put(CharSequence columnFamily, CharSequence columnQualifier, long timestamp, Value value) {
    put(columnFamily, columnQualifier, EMPTY_BYTES, true, timestamp, false, value.get());
  }

  /**
   * Puts a modification in this mutation. All appropriate parameters are defensively copied.
   *
   * @param columnFamily
   *          column family
   * @param columnQualifier
   *          column qualifier
   * @param columnVisibility
   *          column visibility
   * @param timestamp
   *          timestamp
   * @param value
   *          cell value
   */
  public void put(CharSequence columnFamily, CharSequence columnQualifier, ColumnVisibility columnVisibility, long timestamp, Value value) {
    put(columnFamily, columnQualifier, columnVisibility.getExpression(), true, timestamp, false, value.get());
  }

  /**
   * Puts a deletion in this mutation. Matches empty column visibility; timestamp is not set. All parameters are defensively copied.
   *
   * @param columnFamily
   *          column family
   * @param columnQualifier
   *          column qualifier
   */
  public void putDelete(CharSequence columnFamily, CharSequence columnQualifier) {
    put(columnFamily, columnQualifier, EMPTY_BYTES, false, 0l, true, EMPTY_BYTES);
  }

  /**
   * Puts a deletion in this mutation. Timestamp is not set. All appropriate parameters are defensively copied.
   *
   * @param columnFamily
   *          column family
   * @param columnQualifier
   *          column qualifier
   * @param columnVisibility
   *          column visibility
   */
  public void putDelete(CharSequence columnFamily, CharSequence columnQualifier, ColumnVisibility columnVisibility) {
    put(columnFamily, columnQualifier, columnVisibility.getExpression(), false, 0l, true, EMPTY_BYTES);
  }

  /**
   * Puts a deletion in this mutation. Matches empty column visibility. All appropriate parameters are defensively copied.
   *
   * @param columnFamily
   *          column family
   * @param columnQualifier
   *          column qualifier
   * @param timestamp
   *          timestamp
   */
  public void putDelete(CharSequence columnFamily, CharSequence columnQualifier, long timestamp) {
    put(columnFamily, columnQualifier, EMPTY_BYTES, true, timestamp, true, EMPTY_BYTES);
  }

  /**
   * Puts a deletion in this mutation. All appropriate parameters are defensively copied.
   *
   * @param columnFamily
   *          column family
   * @param columnQualifier
   *          column qualifier
   * @param columnVisibility
   *          column visibility
   * @param timestamp
   *          timestamp
   */
  public void putDelete(CharSequence columnFamily, CharSequence columnQualifier, ColumnVisibility columnVisibility, long timestamp) {
    put(columnFamily, columnQualifier, columnVisibility.getExpression(), true, timestamp, true, EMPTY_BYTES);
  }

  /**
   * Puts a modification in this mutation. Column visibility is empty; timestamp is not set. All parameters are defensively copied.
   *
   * @param columnFamily
   *          column family
   * @param columnQualifier
   *          column qualifier
   * @param value
   *          cell value
   */
  public void put(CharSequence columnFamily, CharSequence columnQualifier, CharSequence value) {
    put(columnFamily, columnQualifier, EMPTY_BYTES, false, 0l, false, value);
  }

  /**
   * Puts a modification in this mutation. Timestamp is not set. All parameters are defensively copied.
   *
   * @param columnFamily
   *          column family
   * @param columnQualifier
   *          column qualifier
   * @param columnVisibility
   *          column visibility
   * @param value
   *          cell value
   */
  public void put(CharSequence columnFamily, CharSequence columnQualifier, ColumnVisibility columnVisibility, CharSequence value) {
    put(columnFamily, columnQualifier, columnVisibility.getExpression(), false, 0l, false, value);
  }

  /**
   * Puts a modification in this mutation. Column visibility is empty. All appropriate parameters are defensively copied.
   *
   * @param columnFamily
   *          column family
   * @param columnQualifier
   *          column qualifier
   * @param timestamp
   *          timestamp
   * @param value
   *          cell value
   */
  public void put(CharSequence columnFamily, CharSequence columnQualifier, long timestamp, CharSequence value) {
    put(columnFamily, columnQualifier, EMPTY_BYTES, true, timestamp, false, value);
  }

  /**
   * Puts a modification in this mutation. All appropriate parameters are defensively copied.
   *
   * @param columnFamily
   *          column family
   * @param columnQualifier
   *          column qualifier
   * @param columnVisibility
   *          column visibility
   * @param timestamp
   *          timestamp
   * @param value
   *          cell value
   */
  public void put(CharSequence columnFamily, CharSequence columnQualifier, ColumnVisibility columnVisibility, long timestamp, CharSequence value) {
    put(columnFamily, columnQualifier, columnVisibility.getExpression(), true, timestamp, false, value);
  }

  /**
   * Puts a modification in this mutation. Column visibility is empty; timestamp is not set. All parameters are defensively copied.
   *
   * @param columnFamily
   *          column family
   * @param columnQualifier
   *          column qualifier
   * @param value
   *          cell value
   * @since 1.5.0
   */
  public void put(byte[] columnFamily, byte[] columnQualifier, byte[] value) {
    put(columnFamily, columnQualifier, EMPTY_BYTES, false, 0l, false, value);
  }

  /**
   * Puts a modification in this mutation. Timestamp is not set. All parameters are defensively copied.
   *
   * @param columnFamily
   *          column family
   * @param columnQualifier
   *          column qualifier
   * @param columnVisibility
   *          column visibility
   * @param value
   *          cell value
   * @since 1.5.0
   */
  public void put(byte[] columnFamily, byte[] columnQualifier, ColumnVisibility columnVisibility, byte[] value) {
    put(columnFamily, columnQualifier, columnVisibility.getExpression(), false, 0l, false, value);
  }

  /**
   * Puts a modification in this mutation. Column visibility is empty. All appropriate parameters are defensively copied.
   *
   * @param columnFamily
   *          column family
   * @param columnQualifier
   *          column qualifier
   * @param timestamp
   *          timestamp
   * @param value
   *          cell value
   * @since 1.5.0
   */
  public void put(byte[] columnFamily, byte[] columnQualifier, long timestamp, byte[] value) {
    put(columnFamily, columnQualifier, EMPTY_BYTES, true, timestamp, false, value);
  }

  /**
   * Puts a modification in this mutation. All appropriate parameters are defensively copied.
   *
   * @param columnFamily
   *          column family
   * @param columnQualifier
   *          column qualifier
   * @param columnVisibility
   *          column visibility
   * @param timestamp
   *          timestamp
   * @param value
   *          cell value
   * @since 1.5.0
   */
  public void put(byte[] columnFamily, byte[] columnQualifier, ColumnVisibility columnVisibility, long timestamp, byte[] value) {
    put(columnFamily, columnQualifier, columnVisibility.getExpression(), true, timestamp, false, value);
  }

  /**
   * Puts a deletion in this mutation. Matches empty column visibility; timestamp is not set. All parameters are defensively copied.
   *
   * @param columnFamily
   *          column family
   * @param columnQualifier
   *          column qualifier
   * @since 1.5.0
   */
  public void putDelete(byte[] columnFamily, byte[] columnQualifier) {
    put(columnFamily, columnQualifier, EMPTY_BYTES, false, 0l, true, EMPTY_BYTES);
  }

  /**
   * Puts a deletion in this mutation. Timestamp is not set. All parameters are defensively copied.
   *
   * @param columnFamily
   *          column family
   * @param columnQualifier
   *          column qualifier
   * @param columnVisibility
   *          column visibility
   * @since 1.5.0
   */
  public void putDelete(byte[] columnFamily, byte[] columnQualifier, ColumnVisibility columnVisibility) {
    put(columnFamily, columnQualifier, columnVisibility.getExpression(), false, 0l, true, EMPTY_BYTES);
  }

  /**
   * Puts a deletion in this mutation. Matches empty column visibility. All appropriate parameters are defensively copied.
   *
   * @param columnFamily
   *          column family
   * @param columnQualifier
   *          column qualifier
   * @param timestamp
   *          timestamp
   * @since 1.5.0
   */
  public void putDelete(byte[] columnFamily, byte[] columnQualifier, long timestamp) {
    put(columnFamily, columnQualifier, EMPTY_BYTES, true, timestamp, true, EMPTY_BYTES);
  }

  /**
   * Puts a deletion in this mutation. All appropriate parameters are defensively copied.
   *
   * @param columnFamily
   *          column family
   * @param columnQualifier
   *          column qualifier
   * @param columnVisibility
   *          column visibility
   * @param timestamp
   *          timestamp
   * @since 1.5.0
   */
  public void putDelete(byte[] columnFamily, byte[] columnQualifier, ColumnVisibility columnVisibility, long timestamp) {
    put(columnFamily, columnQualifier, columnVisibility.getExpression(), true, timestamp, true, EMPTY_BYTES);
  }

  private byte[] oldReadBytes(UnsynchronizedBuffer.Reader in) {
    int len = in.readInt();
    if (len == 0)
      return EMPTY_BYTES;

    byte bytes[] = new byte[len];
    in.readBytes(bytes);
    return bytes;
  }

  private byte[] readBytes(UnsynchronizedBuffer.Reader in) {
    int len = (int) in.readVLong();
    if (len == 0)
      return EMPTY_BYTES;

    byte bytes[] = new byte[len];
    in.readBytes(bytes);
    return bytes;
  }

  /**
   * Gets the modifications and deletions in this mutation. After calling this method, further modifications to this mutation are ignored. Changes made to the
   * returned updates do not affect this mutation.
   *
   * @return list of modifications and deletions
   */
  public List<ColumnUpdate> getUpdates() {
    serialize();

    UnsynchronizedBuffer.Reader in = new UnsynchronizedBuffer.Reader(data);

    if (updates == null) {
      if (entries == 1) {
        updates = Collections.singletonList(deserializeColumnUpdate(in));
      } else {
        ColumnUpdate[] tmpUpdates = new ColumnUpdate[entries];

        for (int i = 0; i < entries; i++)
          tmpUpdates[i] = deserializeColumnUpdate(in);

        updates = Arrays.asList(tmpUpdates);
      }
    }

    return updates;
  }

  protected ColumnUpdate newColumnUpdate(byte[] cf, byte[] cq, byte[] cv, boolean hasts, long ts, boolean deleted, byte[] val) {
    return new ColumnUpdate(cf, cq, cv, hasts, ts, deleted, val);
  }

  private ColumnUpdate deserializeColumnUpdate(UnsynchronizedBuffer.Reader in) {
    byte[] cf = readBytes(in);
    byte[] cq = readBytes(in);
    byte[] cv = readBytes(in);
    boolean hasts = in.readBoolean();
    long ts = 0;
    if (hasts)
      ts = in.readVLong();
    boolean deleted = in.readBoolean();

    byte[] val;
    int valLen = (int) in.readVLong();

    if (valLen < 0) {
      val = values.get((-1 * valLen) - 1);
    } else if (valLen == 0) {
      val = EMPTY_BYTES;
    } else {
      val = new byte[valLen];
      in.readBytes(val);
    }

    return newColumnUpdate(cf, cq, cv, hasts, ts, deleted, val);
  }

  private int cachedValLens = -1;

  /**
   * Gets the byte length of all large values stored in this mutation.
   *
   * @return length of all large values
   * @see #VALUE_SIZE_COPY_CUTOFF
   */
  long getValueLengths() {
    if (values == null)
      return 0;

    if (cachedValLens == -1) {
      int tmpCVL = 0;
      for (byte[] val : values)
        tmpCVL += val.length;

      cachedValLens = tmpCVL;
    }

    return cachedValLens;

  }

  /**
   * Gets the total number of bytes in this mutation.
   *
   * @return length of mutation in bytes
   */
  public long numBytes() {
    serialize();
    return row.length + data.length + getValueLengths();
  }

  /**
   * Gets an estimate of the amount of memory used by this mutation. The estimate includes data sizes and object overhead.
   *
   * @return memory usage estimate
   */
  public long estimatedMemoryUsed() {
    return numBytes() + 238;
  }

  /**
   * Gets the number of modifications / deletions in this mutation.
   *
   * @return the number of modifications / deletions
   */
  public int size() {
    return entries;
  }

  /**
   * Add a new element to the set of peers which this Mutation originated from
   *
   * @param peer
   *          the peer to add
   * @since 1.7.0
   */
  public void addReplicationSource(String peer) {
    if (null == replicationSources || replicationSources == EMPTY) {
      replicationSources = new HashSet<>();
    }

    replicationSources.add(peer);
  }

  /**
   * Set the replication peers which this Mutation originated from
   *
   * @param sources
   *          Set of peer names which have processed this update
   * @since 1.7.0
   */
  public void setReplicationSources(Set<String> sources) {
    requireNonNull(sources);
    this.replicationSources = sources;
  }

  /**
   * Return the replication sources for this Mutation
   *
   * @return An unmodifiable view of the replication sources
   */
  public Set<String> getReplicationSources() {
    if (null == replicationSources) {
      return EMPTY;
    }
    return Collections.unmodifiableSet(replicationSources);
  }

  @Override
  public void readFields(DataInput in) throws IOException {

    // Clear out cached column updates and value lengths so
    // that we recalculate them based on the (potentially) new
    // data we are about to read in.
    updates = null;
    cachedValLens = -1;
    buffer = null;
    useOldDeserialize = false;

    byte first = in.readByte();
    if ((first & 0x80) != 0x80) {
      oldReadFields(first, in);
      useOldDeserialize = true;
      return;
    }

    int len = WritableUtils.readVInt(in);
    row = new byte[len];
    in.readFully(row);
    len = WritableUtils.readVInt(in);
    data = new byte[len];
    in.readFully(data);
    entries = WritableUtils.readVInt(in);

    boolean valuesPresent = (first & 0x01) == 0x01;
    if (!valuesPresent) {
      values = null;
    } else {
      values = new ArrayList<>();
      int numValues = WritableUtils.readVInt(in);
      for (int i = 0; i < numValues; i++) {
        len = WritableUtils.readVInt(in);
        byte val[] = new byte[len];
        in.readFully(val);
        values.add(val);
      }
    }

    if (0x02 == (first & 0x02)) {
      int numMutations = WritableUtils.readVInt(in);
      this.replicationSources = new HashSet<>();
      for (int i = 0; i < numMutations; i++) {
        replicationSources.add(WritableUtils.readString(in));
      }
    }
  }

  protected void droppingOldTimestamp(long ts) {}

  private void oldReadFields(byte first, DataInput in) throws IOException {

    byte b = in.readByte();
    byte c = in.readByte();
    byte d = in.readByte();

    int len = (((first & 0xff) << 24) | ((b & 0xff) << 16) | ((c & 0xff) << 8) | (d & 0xff));
    row = new byte[len];
    in.readFully(row);
    len = in.readInt();
    byte[] localData = new byte[len];
    in.readFully(localData);
    int localEntries = in.readInt();

    List<byte[]> localValues;
    boolean valuesPresent = in.readBoolean();
    if (!valuesPresent) {
      localValues = null;
    } else {
      localValues = new ArrayList<>();
      int numValues = in.readInt();
      for (int i = 0; i < numValues; i++) {
        len = in.readInt();
        byte val[] = new byte[len];
        in.readFully(val);
        localValues.add(val);
      }
    }

    // convert data to new format
    UnsynchronizedBuffer.Reader din = new UnsynchronizedBuffer.Reader(localData);
    buffer = new UnsynchronizedBuffer.Writer();
    for (int i = 0; i < localEntries; i++) {
      byte[] cf = oldReadBytes(din);
      byte[] cq = oldReadBytes(din);
      byte[] cv = oldReadBytes(din);
      boolean hasts = din.readBoolean();
      long ts = din.readLong();
      boolean deleted = din.readBoolean();

      byte[] val;
      int valLen = din.readInt();

      if (valLen < 0) {
        val = localValues.get((-1 * valLen) - 1);
      } else if (valLen == 0) {
        val = EMPTY_BYTES;
      } else {
        val = new byte[valLen];
        din.readBytes(val);
      }

      put(cf, cq, cv, hasts, ts, deleted, val);
      if (!hasts)
        droppingOldTimestamp(ts);
    }

    serialize();

  }

  @Override
  public void write(DataOutput out) throws IOException {
    serialize();
    byte hasValues = (values == null) ? 0 : (byte) 1;
    if (!replicationSources.isEmpty()) {
      // Use 2nd least-significant bit for whether or not we have replication sources
      hasValues = (byte) (0x02 | hasValues);
    }
    out.write((byte) (0x80 | hasValues));

    WritableUtils.writeVInt(out, row.length);
    out.write(row);

    WritableUtils.writeVInt(out, data.length);
    out.write(data);
    WritableUtils.writeVInt(out, entries);

    if (0x01 == (0x01 & hasValues)) {
      WritableUtils.writeVInt(out, values.size());
      for (int i = 0; i < values.size(); i++) {
        byte val[] = values.get(i);
        WritableUtils.writeVInt(out, val.length);
        out.write(val);
      }
    }
    if (0x02 == (0x02 & hasValues)) {
      WritableUtils.writeVInt(out, replicationSources.size());
      for (String source : replicationSources) {
        WritableUtils.writeString(out, source);
      }
    }
  }

  @Override
  public boolean equals(Object o) {
    if (o == this) {
      return true;
    }
    if (o != null && o.getClass().equals(this.getClass())) {
      return equalMutation((Mutation) o);
    }
    return false;
  }

  @Override
  public int hashCode() {
    return serializedSnapshot().hashCode();
  }

  /**
   * Checks if this mutation equals another. Two mutations are equal if they target the same row and have the same modifications and deletions, in order. This
   * method may be removed in a future API revision in favor of {@link #equals(Object)}. See ACCUMULO-1627 for more information.
   *
   * @param m
   *          mutation to compare
   * @return true if this mutation equals the other, false otherwise
   */
  public boolean equals(Mutation m) {
    return this.equals((Object) m);
  }

  private boolean equalMutation(Mutation m) {
    ByteBuffer myData = serializedSnapshot();
    ByteBuffer otherData = m.serializedSnapshot();
    if (Arrays.equals(row, m.row) && entries == m.entries && myData.equals(otherData)) {
      // If two mutations don't have the same
      if (!replicationSources.equals(m.replicationSources)) {
        return false;
      }
      if (values == null && m.values == null)
        return true;

      if (values != null && m.values != null && values.size() == m.values.size()) {
        for (int i = 0; i < values.size(); i++) {
          if (!Arrays.equals(values.get(i), m.values.get(i)))
            return false;
        }

        return true;
      }

    }

    return false;
  }

  /**
   * Creates a {@link org.apache.accumulo.core.data.thrift.TMutation} object containing this Mutation's data.
   *
   * Note that this method will move the Mutation into a "serialized" state that will prevent users from adding more data via Mutation#put().
   *
   * @return a thrift form of this Mutation
   */
  public TMutation toThrift() {
    return toThrift(true);
  }

  private TMutation toThrift(boolean serialize) {
    if (serialize) {
      this.serialize();
    }
    ByteBuffer data = serializedSnapshot();
    TMutation tmutation = new TMutation(ByteBuffer.wrap(row), data, ByteBufferUtil.toByteBuffers(values), entries);
    if (!this.replicationSources.isEmpty()) {
      tmutation.setSources(new ArrayList<>(replicationSources));
    }
    return tmutation;
  }

  /**
   * Gets the serialization format used to (de)serialize this mutation.
   *
   * @return serialization format
   */
  protected SERIALIZED_FORMAT getSerializedFormat() {
    return this.useOldDeserialize ? SERIALIZED_FORMAT.VERSION1 : SERIALIZED_FORMAT.VERSION2;
  }

}

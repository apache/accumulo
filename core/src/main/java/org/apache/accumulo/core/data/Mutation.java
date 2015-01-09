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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.accumulo.core.data.thrift.TMutation;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.accumulo.core.util.ByteBufferUtil;
import org.apache.accumulo.core.util.UnsynchronizedBuffer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

/**
 * <p>
 * Mutation represents an action that manipulates a row in a table. A mutation holds a list of column/value pairs that represent an atomic set of modifications
 * to make to a row.
 *
 * <p>
 * Convenience methods which takes columns and value as CharSequence (String implements CharSequence) are provided. CharSequence is converted to UTF-8 by
 * constructing a new Text object.
 *
 * <p>
 * When always passing in the same data as a CharSequence/String, its probably more efficient to call the Text put methods. This way the data is only encoded
 * once and only one Text object is created.
 *
 * <p>
 * All of the put methods append data to the mutation, they do not overwrite anything that was previously put. The mutation holds a list of all column/values
 * that were put into it. The putDelete() methods do not remove something that was previously added to the mutation, rather they indicate that Accumulo should
 * insert a delete marker for that row column.
 *
 */

public class Mutation implements Writable {

  static final int VALUE_SIZE_COPY_CUTOFF = 1 << 15;

  public static enum SERIALIZED_FORMAT {
    VERSION1, VERSION2
  };

  private boolean useOldDeserialize = false;
  private byte[] row;
  private byte[] data;
  private int entries;
  private List<byte[]> values;

  private UnsynchronizedBuffer.Writer buffer;

  private List<ColumnUpdate> updates;

  private static final byte[] EMPTY_BYTES = new byte[0];

  private void serialize() {
    if (buffer != null) {
      data = buffer.toArray();
      buffer = null;
    }
  }

  /**
   * @since 1.5.0
   */
  public Mutation(byte[] row) {
    this(row, 0, row.length);
  }

  /**
   * @since 1.5.0
   */
  public Mutation(byte[] row, int start, int length) {
    this.row = new byte[length];
    System.arraycopy(row, start, this.row, 0, length);
    buffer = new UnsynchronizedBuffer.Writer();
  }

  public Mutation(Text row) {
    this(row.getBytes(), 0, row.getLength());
  }

  public Mutation(CharSequence row) {
    this(new Text(row.toString()));
  }

  public Mutation() {}

  public Mutation(TMutation tmutation) {
    this.row = ByteBufferUtil.toBytes(tmutation.row);
    this.data = ByteBufferUtil.toBytes(tmutation.data);
    this.entries = tmutation.entries;
    this.values = ByteBufferUtil.toBytesList(tmutation.values);

    if (this.row == null) {
      throw new IllegalArgumentException("null row");
    }
    if (this.data == null) {
      throw new IllegalArgumentException("null serialized data");
    }
  }

  public Mutation(Mutation m) {
    m.serialize();
    this.row = m.row;
    this.data = m.data;
    this.entries = m.entries;
    this.values = m.values;
  }

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
        values = new ArrayList<byte[]>();
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

  public void put(Text columnFamily, Text columnQualifier, Value value) {
    put(columnFamily, columnQualifier, EMPTY_BYTES, false, 0l, false, value.get());
  }

  public void put(Text columnFamily, Text columnQualifier, ColumnVisibility columnVisibility, Value value) {
    put(columnFamily, columnQualifier, columnVisibility.getExpression(), false, 0l, false, value.get());
  }

  public void put(Text columnFamily, Text columnQualifier, long timestamp, Value value) {
    put(columnFamily, columnQualifier, EMPTY_BYTES, true, timestamp, false, value.get());
  }

  public void put(Text columnFamily, Text columnQualifier, ColumnVisibility columnVisibility, long timestamp, Value value) {
    put(columnFamily, columnQualifier, columnVisibility.getExpression(), true, timestamp, false, value.get());
  }

  public void putDelete(Text columnFamily, Text columnQualifier) {
    put(columnFamily, columnQualifier, EMPTY_BYTES, false, 0l, true, EMPTY_BYTES);
  }

  public void putDelete(Text columnFamily, Text columnQualifier, ColumnVisibility columnVisibility) {
    put(columnFamily, columnQualifier, columnVisibility.getExpression(), false, 0l, true, EMPTY_BYTES);
  }

  public void putDelete(Text columnFamily, Text columnQualifier, long timestamp) {
    put(columnFamily, columnQualifier, EMPTY_BYTES, true, timestamp, true, EMPTY_BYTES);
  }

  public void putDelete(Text columnFamily, Text columnQualifier, ColumnVisibility columnVisibility, long timestamp) {
    put(columnFamily, columnQualifier, columnVisibility.getExpression(), true, timestamp, true, EMPTY_BYTES);
  }

  public void put(CharSequence columnFamily, CharSequence columnQualifier, Value value) {
    put(columnFamily, columnQualifier, EMPTY_BYTES, false, 0l, false, value.get());
  }

  public void put(CharSequence columnFamily, CharSequence columnQualifier, ColumnVisibility columnVisibility, Value value) {
    put(columnFamily, columnQualifier, columnVisibility.getExpression(), false, 0l, false, value.get());
  }

  public void put(CharSequence columnFamily, CharSequence columnQualifier, long timestamp, Value value) {
    put(columnFamily, columnQualifier, EMPTY_BYTES, true, timestamp, false, value.get());
  }

  public void put(CharSequence columnFamily, CharSequence columnQualifier, ColumnVisibility columnVisibility, long timestamp, Value value) {
    put(columnFamily, columnQualifier, columnVisibility.getExpression(), true, timestamp, false, value.get());
  }

  public void putDelete(CharSequence columnFamily, CharSequence columnQualifier) {
    put(columnFamily, columnQualifier, EMPTY_BYTES, false, 0l, true, EMPTY_BYTES);
  }

  public void putDelete(CharSequence columnFamily, CharSequence columnQualifier, ColumnVisibility columnVisibility) {
    put(columnFamily, columnQualifier, columnVisibility.getExpression(), false, 0l, true, EMPTY_BYTES);
  }

  public void putDelete(CharSequence columnFamily, CharSequence columnQualifier, long timestamp) {
    put(columnFamily, columnQualifier, EMPTY_BYTES, true, timestamp, true, EMPTY_BYTES);
  }

  public void putDelete(CharSequence columnFamily, CharSequence columnQualifier, ColumnVisibility columnVisibility, long timestamp) {
    put(columnFamily, columnQualifier, columnVisibility.getExpression(), true, timestamp, true, EMPTY_BYTES);
  }

  public void put(CharSequence columnFamily, CharSequence columnQualifier, CharSequence value) {
    put(columnFamily, columnQualifier, EMPTY_BYTES, false, 0l, false, value);
  }

  public void put(CharSequence columnFamily, CharSequence columnQualifier, ColumnVisibility columnVisibility, CharSequence value) {
    put(columnFamily, columnQualifier, columnVisibility.getExpression(), false, 0l, false, value);
  }

  public void put(CharSequence columnFamily, CharSequence columnQualifier, long timestamp, CharSequence value) {
    put(columnFamily, columnQualifier, EMPTY_BYTES, true, timestamp, false, value);
  }

  public void put(CharSequence columnFamily, CharSequence columnQualifier, ColumnVisibility columnVisibility, long timestamp, CharSequence value) {
    put(columnFamily, columnQualifier, columnVisibility.getExpression(), true, timestamp, false, value);
  }

  /**
   * @since 1.5.0
   */
  public void put(byte[] columnFamily, byte[] columnQualifier, byte[] value) {
    put(columnFamily, columnQualifier, EMPTY_BYTES, false, 0l, false, value);
  }

  /**
   * @since 1.5.0
   */
  public void put(byte[] columnFamily, byte[] columnQualifier, ColumnVisibility columnVisibility, byte[] value) {
    put(columnFamily, columnQualifier, columnVisibility.getExpression(), false, 0l, false, value);
  }

  /**
   * @since 1.5.0
   */
  public void put(byte[] columnFamily, byte[] columnQualifier, long timestamp, byte[] value) {
    put(columnFamily, columnQualifier, EMPTY_BYTES, true, timestamp, false, value);
  }

  /**
   * @since 1.5.0
   */
  public void put(byte[] columnFamily, byte[] columnQualifier, ColumnVisibility columnVisibility, long timestamp, byte[] value) {
    put(columnFamily, columnQualifier, columnVisibility.getExpression(), true, timestamp, false, value);
  }

  /**
   * @since 1.5.0
   */
  public void putDelete(byte[] columnFamily, byte[] columnQualifier) {
    put(columnFamily, columnQualifier, EMPTY_BYTES, false, 0l, true, EMPTY_BYTES);
  }

  /**
   * @since 1.5.0
   */
  public void putDelete(byte[] columnFamily, byte[] columnQualifier, ColumnVisibility columnVisibility) {
    put(columnFamily, columnQualifier, columnVisibility.getExpression(), false, 0l, true, EMPTY_BYTES);
  }

  /**
   * @since 1.5.0
   */
  public void putDelete(byte[] columnFamily, byte[] columnQualifier, long timestamp) {
    put(columnFamily, columnQualifier, EMPTY_BYTES, true, timestamp, true, EMPTY_BYTES);
  }

  /**
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

  public long numBytes() {
    serialize();
    return row.length + data.length + getValueLengths();
  }

  public long estimatedMemoryUsed() {
    return numBytes() + 238;
  }

  /**
   * @return the number of column value pairs added to the mutation
   */
  public int size() {
    return entries;
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
      values = new ArrayList<byte[]>();
      int numValues = WritableUtils.readVInt(in);
      for (int i = 0; i < numValues; i++) {
        len = WritableUtils.readVInt(in);
        byte val[] = new byte[len];
        in.readFully(val);
        values.add(val);
      }
    }
  }

  protected void droppingOldTimestamp(long ts) {}

  private void oldReadFields(byte first, DataInput in) throws IOException {

    byte b = (byte) in.readByte();
    byte c = (byte) in.readByte();
    byte d = (byte) in.readByte();

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
      localValues = new ArrayList<byte[]>();
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
    out.write((byte) (0x80 | hasValues));

    WritableUtils.writeVInt(out, row.length);
    out.write(row);

    WritableUtils.writeVInt(out, data.length);
    out.write(data);
    WritableUtils.writeVInt(out, entries);

    if (hasValues > 0) {
      WritableUtils.writeVInt(out, values.size());
      for (int i = 0; i < values.size(); i++) {
        byte val[] = values.get(i);
        WritableUtils.writeVInt(out, val.length);
        out.write(val);
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
    return toThrift().hashCode();
  }

  /**
   * Checks if this mutation equals another. This method may be removed in a future API revision in favor of {@link #equals(Object)}. See ACCUMULO-1627 for more
   * information.
   *
   * @param m
   *          mutation
   * @return true if the given mutation equals this one, false otehrwise
   */
  public boolean equals(Mutation m) {
    return this.equals((Object) m);
  }

  private boolean equalMutation(Mutation m) {
    serialize();
    m.serialize();
    if (Arrays.equals(row, m.row) && entries == m.entries && Arrays.equals(data, m.data)) {
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

  public TMutation toThrift() {
    serialize();
    return new TMutation(java.nio.ByteBuffer.wrap(row), java.nio.ByteBuffer.wrap(data), ByteBufferUtil.toByteBuffers(values), entries);
  }

  protected SERIALIZED_FORMAT getSerializedFormat() {
    return this.useOldDeserialize ? SERIALIZED_FORMAT.VERSION1 : SERIALIZED_FORMAT.VERSION2;
  }

}

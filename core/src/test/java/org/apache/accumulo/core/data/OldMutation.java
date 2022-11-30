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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.accumulo.core.dataImpl.thrift.TMutation;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.accumulo.core.util.ByteBufferUtil;
import org.apache.accumulo.core.util.TextUtil;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

/**
 * Will read/write old mutations.
 */
public class OldMutation implements Writable {

  static final int VALUE_SIZE_COPY_CUTOFF = 1 << 15;

  private byte[] row;
  private byte[] data;
  private int entries;
  private List<byte[]> values;

  // created this little class instead of using ByteArrayOutput stream and DataOutputStream
  // because both are synchronized... lots of small syncs slow things down
  private static class ByteBuffer {

    int offset;
    byte[] data = new byte[64];

    private void reserve(int l) {
      if (offset + l > data.length) {
        int newSize = data.length * 2;
        while (newSize <= offset + l) {
          newSize = newSize * 2;
        }

        byte[] newData = new byte[newSize];
        System.arraycopy(data, 0, newData, 0, offset);
        data = newData;
      }

    }

    void add(byte[] b) {
      reserve(b.length);
      System.arraycopy(b, 0, data, offset, b.length);
      offset += b.length;
    }

    public void add(byte[] bytes, int off, int length) {
      reserve(length);
      System.arraycopy(bytes, off, data, offset, length);
      offset += length;
    }

    void add(boolean b) {
      reserve(1);
      if (b) {
        data[offset++] = 1;
      } else {
        data[offset++] = 0;
      }
    }

    void add(long v) {
      reserve(8);
      data[offset++] = (byte) (v >>> 56);
      data[offset++] = (byte) (v >>> 48);
      data[offset++] = (byte) (v >>> 40);
      data[offset++] = (byte) (v >>> 32);
      data[offset++] = (byte) (v >>> 24);
      data[offset++] = (byte) (v >>> 16);
      data[offset++] = (byte) (v >>> 8);
      data[offset++] = (byte) (v >>> 0);
    }

    void add(int i) {
      reserve(4);
      data[offset++] = (byte) (i >>> 24);
      data[offset++] = (byte) (i >>> 16);
      data[offset++] = (byte) (i >>> 8);
      data[offset++] = (byte) (i >>> 0);
    }

    public byte[] toArray() {
      byte[] ret = new byte[offset];
      System.arraycopy(data, 0, ret, 0, offset);
      return ret;
    }

  }

  private static class SimpleReader {
    int offset;
    byte[] data;

    SimpleReader(byte[] b) {
      this.data = b;
    }

    int readInt() {
      return (data[offset++] << 24) + ((data[offset++] & 255) << 16) + ((data[offset++] & 255) << 8)
          + ((data[offset++] & 255) << 0);

    }

    long readLong() {
      return (((long) data[offset++] << 56) + ((long) (data[offset++] & 255) << 48)
          + ((long) (data[offset++] & 255) << 40) + ((long) (data[offset++] & 255) << 32)
          + ((long) (data[offset++] & 255) << 24) + ((data[offset++] & 255) << 16)
          + ((data[offset++] & 255) << 8) + ((data[offset++] & 255) << 0));
    }

    void readBytes(byte[] b) {
      System.arraycopy(data, offset, b, 0, b.length);
      offset += b.length;
    }

    boolean readBoolean() {
      return (data[offset++] == 1);
    }

  }

  private ByteBuffer buffer;

  private List<ColumnUpdate> updates;

  private static final byte[] EMPTY_BYTES = new byte[0];

  private void serialize() {
    if (buffer != null) {
      data = buffer.toArray();
      buffer = null;
    }
  }

  public OldMutation(Text row) {
    this.row = new byte[row.getLength()];
    System.arraycopy(row.getBytes(), 0, this.row, 0, row.getLength());
    buffer = new ByteBuffer();
  }

  public OldMutation(CharSequence row) {
    this(new Text(row.toString()));
  }

  public OldMutation() {}

  public OldMutation(TMutation tmutation) {
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

  public byte[] getRow() {
    return row;
  }

  private void put(byte[] b) {
    buffer.add(b.length);
    buffer.add(b);
  }

  private void put(Text t) {
    buffer.add(t.getLength());
    buffer.add(t.getBytes(), 0, t.getLength());
  }

  private void put(boolean b) {
    buffer.add(b);
  }

  private void put(int i) {
    buffer.add(i);
  }

  private void put(long l) {
    buffer.add(l);
  }

  private void put(Text cf, Text cq, byte[] cv, boolean hasts, long ts, boolean deleted,
      byte[] val) {

    if (buffer == null) {
      throw new IllegalStateException("Can not add to mutation after serializing it");
    }

    put(cf);
    put(cq);
    put(cv);
    put(hasts);
    put(ts);
    put(deleted);

    if (val.length < VALUE_SIZE_COPY_CUTOFF) {
      put(val);
    } else {
      if (values == null) {
        values = new ArrayList<>();
      }
      byte[] copy = new byte[val.length];
      System.arraycopy(val, 0, copy, 0, val.length);
      values.add(copy);
      put(-1 * values.size());
    }

    entries++;
  }

  private void put(CharSequence cf, CharSequence cq, byte[] cv, boolean hasts, long ts,
      boolean deleted, byte[] val) {
    put(new Text(cf.toString()), new Text(cq.toString()), cv, hasts, ts, deleted, val);
  }

  private void put(CharSequence cf, CharSequence cq, byte[] cv, boolean hasts, long ts,
      boolean deleted, CharSequence val) {
    put(cf, cq, cv, hasts, ts, deleted, TextUtil.getBytes(new Text(val.toString())));
  }

  public void put(Text columnFamily, Text columnQualifier, Value value) {
    put(columnFamily, columnQualifier, EMPTY_BYTES, false, 0L, false, value.get());
  }

  public void put(Text columnFamily, Text columnQualifier, ColumnVisibility columnVisibility,
      Value value) {
    put(columnFamily, columnQualifier, columnVisibility.getExpression(), false, 0L, false,
        value.get());
  }

  public void put(Text columnFamily, Text columnQualifier, long timestamp, Value value) {
    put(columnFamily, columnQualifier, EMPTY_BYTES, true, timestamp, false, value.get());
  }

  public void put(Text columnFamily, Text columnQualifier, ColumnVisibility columnVisibility,
      long timestamp, Value value) {
    put(columnFamily, columnQualifier, columnVisibility.getExpression(), true, timestamp, false,
        value.get());
  }

  public void putDelete(Text columnFamily, Text columnQualifier) {
    put(columnFamily, columnQualifier, EMPTY_BYTES, false, 0L, true, EMPTY_BYTES);
  }

  public void putDelete(Text columnFamily, Text columnQualifier,
      ColumnVisibility columnVisibility) {
    put(columnFamily, columnQualifier, columnVisibility.getExpression(), false, 0L, true,
        EMPTY_BYTES);
  }

  public void putDelete(Text columnFamily, Text columnQualifier, long timestamp) {
    put(columnFamily, columnQualifier, EMPTY_BYTES, true, timestamp, true, EMPTY_BYTES);
  }

  public void putDelete(Text columnFamily, Text columnQualifier, ColumnVisibility columnVisibility,
      long timestamp) {
    put(columnFamily, columnQualifier, columnVisibility.getExpression(), true, timestamp, true,
        EMPTY_BYTES);
  }

  public void put(CharSequence columnFamily, CharSequence columnQualifier, Value value) {
    put(columnFamily, columnQualifier, EMPTY_BYTES, false, 0L, false, value.get());
  }

  public void put(CharSequence columnFamily, CharSequence columnQualifier,
      ColumnVisibility columnVisibility, Value value) {
    put(columnFamily, columnQualifier, columnVisibility.getExpression(), false, 0L, false,
        value.get());
  }

  public void put(CharSequence columnFamily, CharSequence columnQualifier, long timestamp,
      Value value) {
    put(columnFamily, columnQualifier, EMPTY_BYTES, true, timestamp, false, value.get());
  }

  public void put(CharSequence columnFamily, CharSequence columnQualifier,
      ColumnVisibility columnVisibility, long timestamp, Value value) {
    put(columnFamily, columnQualifier, columnVisibility.getExpression(), true, timestamp, false,
        value.get());
  }

  public void putDelete(CharSequence columnFamily, CharSequence columnQualifier) {
    put(columnFamily, columnQualifier, EMPTY_BYTES, false, 0L, true, EMPTY_BYTES);
  }

  public void putDelete(CharSequence columnFamily, CharSequence columnQualifier,
      ColumnVisibility columnVisibility) {
    put(columnFamily, columnQualifier, columnVisibility.getExpression(), false, 0L, true,
        EMPTY_BYTES);
  }

  public void putDelete(CharSequence columnFamily, CharSequence columnQualifier, long timestamp) {
    put(columnFamily, columnQualifier, EMPTY_BYTES, true, timestamp, true, EMPTY_BYTES);
  }

  public void putDelete(CharSequence columnFamily, CharSequence columnQualifier,
      ColumnVisibility columnVisibility, long timestamp) {
    put(columnFamily, columnQualifier, columnVisibility.getExpression(), true, timestamp, true,
        EMPTY_BYTES);
  }

  public void put(CharSequence columnFamily, CharSequence columnQualifier, CharSequence value) {
    put(columnFamily, columnQualifier, EMPTY_BYTES, false, 0L, false, value);
  }

  public void put(CharSequence columnFamily, CharSequence columnQualifier,
      ColumnVisibility columnVisibility, CharSequence value) {
    put(columnFamily, columnQualifier, columnVisibility.getExpression(), false, 0L, false, value);
  }

  public void put(CharSequence columnFamily, CharSequence columnQualifier, long timestamp,
      CharSequence value) {
    put(columnFamily, columnQualifier, EMPTY_BYTES, true, timestamp, false, value);
  }

  public void put(CharSequence columnFamily, CharSequence columnQualifier,
      ColumnVisibility columnVisibility, long timestamp, CharSequence value) {
    put(columnFamily, columnQualifier, columnVisibility.getExpression(), true, timestamp, false,
        value);
  }

  private byte[] readBytes(SimpleReader in) {
    int len = in.readInt();
    if (len == 0) {
      return EMPTY_BYTES;
    }

    byte[] bytes = new byte[len];
    in.readBytes(bytes);
    return bytes;
  }

  public List<ColumnUpdate> getUpdates() {
    serialize();

    SimpleReader in = new SimpleReader(data);

    if (updates == null) {
      if (entries == 1) {
        updates = Collections.singletonList(deserializeColumnUpdate(in));
      } else {
        ColumnUpdate[] tmpUpdates = new ColumnUpdate[entries];

        for (int i = 0; i < entries; i++) {
          tmpUpdates[i] = deserializeColumnUpdate(in);
        }

        updates = Arrays.asList(tmpUpdates);
      }
    }

    return updates;
  }

  private ColumnUpdate deserializeColumnUpdate(SimpleReader in) {
    byte[] cf = readBytes(in);
    byte[] cq = readBytes(in);
    byte[] cv = readBytes(in);
    boolean hasts = in.readBoolean();
    long ts = in.readLong();
    boolean deleted = in.readBoolean();

    byte[] val;
    int valLen = in.readInt();

    if (valLen < 0) {
      val = values.get((-1 * valLen) - 1);
    } else if (valLen == 0) {
      val = EMPTY_BYTES;
    } else {
      val = new byte[valLen];
      in.readBytes(val);
    }

    return new ColumnUpdate(cf, cq, cv, hasts, ts, deleted, val);
  }

  private int cachedValLens = -1;

  long getValueLengths() {
    if (values == null) {
      return 0;
    }

    if (cachedValLens == -1) {
      int tmpCVL = 0;
      for (byte[] val : values) {
        tmpCVL += val.length;
      }

      cachedValLens = tmpCVL;
    }

    return cachedValLens;

  }

  public long numBytes() {
    serialize();
    return row.length + data.length + getValueLengths();
  }

  public long estimatedMemoryUsed() {
    return numBytes() + 230;
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

    int len = in.readInt();
    row = new byte[len];
    in.readFully(row);
    len = in.readInt();
    data = new byte[len];
    in.readFully(data);
    entries = in.readInt();

    boolean valuesPresent = in.readBoolean();
    if (valuesPresent) {
      values = new ArrayList<>();
      int numValues = in.readInt();
      for (int i = 0; i < numValues; i++) {
        len = in.readInt();
        byte[] val = new byte[len];
        in.readFully(val);
        values.add(val);
      }
    } else {
      values = null;
    }
  }

  @Override
  public void write(DataOutput out) throws IOException {
    serialize();
    out.writeInt(row.length);
    out.write(row);
    out.writeInt(data.length);
    out.write(data);
    out.writeInt(entries);

    if (values == null) {
      out.writeBoolean(false);
    } else {
      out.writeBoolean(true);
      out.writeInt(values.size());
      for (byte[] val : values) {
        out.writeInt(val.length);
        out.write(val);
      }
    }

  }

  @Override
  public boolean equals(Object o) {
    if (o instanceof OldMutation) {
      return equals((OldMutation) o);
    }
    return false;
  }

  @Override
  public int hashCode() {
    return toThrift().hashCode();
  }

  public boolean equals(OldMutation m) {
    serialize();
    if (!Arrays.equals(row, m.getRow())) {
      return false;
    }
    List<ColumnUpdate> oldcus = this.getUpdates();
    List<ColumnUpdate> newcus = m.getUpdates();
    if (oldcus.size() != newcus.size()) {
      return false;
    }
    for (int i = 0; i < newcus.size(); i++) {
      ColumnUpdate oldcu = oldcus.get(i);
      ColumnUpdate newcu = newcus.get(i);
      if (!oldcu.equals(newcu)) {
        return false;
      }
    }
    return false;
  }

  public TMutation toThrift() {
    serialize();
    return new TMutation(java.nio.ByteBuffer.wrap(row), java.nio.ByteBuffer.wrap(data),
        ByteBufferUtil.toByteBuffers(values), entries);
  }

}

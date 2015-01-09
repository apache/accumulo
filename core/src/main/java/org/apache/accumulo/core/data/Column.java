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

import static com.google.common.base.Charsets.UTF_8;
import static org.apache.accumulo.core.util.ByteBufferUtil.toBytes;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.accumulo.core.data.thrift.TColumn;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class Column implements WritableComparable<Column> {

  static private int compareBytes(byte[] a, byte[] b) {
    if (a == null && b == null)
      return 0;
    if (a == null)
      return -1;
    if (b == null)
      return 1;
    return WritableComparator.compareBytes(a, 0, a.length, b, 0, b.length);
  }

  public int compareTo(Column that) {
    int result;
    result = compareBytes(this.columnFamily, that.columnFamily);
    if (result != 0)
      return result;
    result = compareBytes(this.columnQualifier, that.columnQualifier);
    if (result != 0)
      return result;
    return compareBytes(this.columnVisibility, that.columnVisibility);
  }

  public void readFields(DataInput in) throws IOException {
    if (in.readBoolean()) {
      int len = in.readInt();
      columnFamily = new byte[len];
      in.readFully(columnFamily);
    } else {
      columnFamily = null;
    }

    if (in.readBoolean()) {
      int len = in.readInt();
      columnQualifier = new byte[len];
      in.readFully(columnQualifier);
    } else {
      columnQualifier = null;
    }

    if (in.readBoolean()) {
      int len = in.readInt();
      columnVisibility = new byte[len];
      in.readFully(columnVisibility);
    } else {
      columnVisibility = null;
    }
  }

  @Override
  public void write(DataOutput out) throws IOException {
    if (columnFamily == null) {
      out.writeBoolean(false);
    } else {
      out.writeBoolean(true);
      out.writeInt(columnFamily.length);
      out.write(columnFamily);
    }

    if (columnQualifier == null) {
      out.writeBoolean(false);
    } else {
      out.writeBoolean(true);
      out.writeInt(columnQualifier.length);
      out.write(columnQualifier);
    }

    if (columnVisibility == null) {
      out.writeBoolean(false);
    } else {
      out.writeBoolean(true);
      out.writeInt(columnVisibility.length);
      out.write(columnVisibility);
    }
  }

  public byte[] columnFamily;
  public byte[] columnQualifier;
  public byte[] columnVisibility;

  public Column() {}

  public Column(byte[] columnFamily, byte[] columnQualifier, byte[] columnVisibility) {
    this();
    this.columnFamily = columnFamily;
    this.columnQualifier = columnQualifier;
    this.columnVisibility = columnVisibility;
  }

  public Column(TColumn tcol) {
    this(toBytes(tcol.columnFamily), toBytes(tcol.columnQualifier), toBytes(tcol.columnVisibility));
  }

  @Override
  public boolean equals(Object that) {
    if (that == null)
      return false;
    if (that instanceof Column)
      return this.equals((Column) that);
    return false;
  }

  public boolean equals(Column that) {
    return this.compareTo(that) == 0;
  }

  private static int hash(byte[] b) {
    if (b == null)
      return 0;

    return WritableComparator.hashBytes(b, b.length);
  }

  @Override
  public int hashCode() {
    return hash(columnFamily) + hash(columnQualifier) + hash(columnVisibility);
  }

  public byte[] getColumnFamily() {
    return columnFamily;
  }

  public byte[] getColumnQualifier() {
    return columnQualifier;
  }

  public byte[] getColumnVisibility() {
    return columnVisibility;
  }

  public String toString() {
    return new String(columnFamily == null ? new byte[0] : columnFamily, UTF_8) + ":"
        + new String(columnQualifier == null ? new byte[0] : columnQualifier, UTF_8) + ":"
        + new String(columnVisibility == null ? new byte[0] : columnVisibility, UTF_8);
  }

  public TColumn toThrift() {
    return new TColumn(columnFamily == null ? null : ByteBuffer.wrap(columnFamily), columnQualifier == null ? null : ByteBuffer.wrap(columnQualifier),
        columnVisibility == null ? null : ByteBuffer.wrap(columnVisibility));
  }

}

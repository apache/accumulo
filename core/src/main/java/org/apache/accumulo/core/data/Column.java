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
import static org.apache.accumulo.core.util.ByteBufferUtil.toBytes;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Comparator;

import org.apache.accumulo.core.dataImpl.thrift.TColumn;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * A column, specified by family, qualifier, and visibility.
 */
public class Column implements WritableComparable<Column> {

  private static final Comparator<byte[]> BYTE_COMPARATOR = Comparator
      .nullsFirst((a, b) -> WritableComparator.compareBytes(a, 0, a.length, b, 0, b.length));

  private static final Comparator<Column> COMPARATOR =
      Comparator.comparing(Column::getColumnFamily, BYTE_COMPARATOR)
          .thenComparing(Column::getColumnQualifier, BYTE_COMPARATOR)
          .thenComparing(Column::getColumnVisibility, BYTE_COMPARATOR);

  /**
   * Compares this column to another. Column families are compared first, then qualifiers, then
   * visibilities.
   *
   * @param that column to compare
   * @return comparison result
   */
  @Override
  public int compareTo(Column that) {
    return COMPARATOR.compare(this, that);
  }

  @Override
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

  /**
   * Creates a new blank column.
   */
  public Column() {}

  /**
   * Creates a new column.
   *
   * @param columnFamily family
   * @param columnQualifier qualifier
   * @param columnVisibility visibility
   */
  public Column(byte[] columnFamily, byte[] columnQualifier, byte[] columnVisibility) {
    this();
    this.columnFamily = columnFamily;
    this.columnQualifier = columnQualifier;
    this.columnVisibility = columnVisibility;
  }

  /**
   * Creates a new column.
   *
   * @param tcol Thrift column
   */
  public Column(TColumn tcol) {
    this(toBytes(tcol.columnFamily), toBytes(tcol.columnQualifier), toBytes(tcol.columnVisibility));
  }

  @Override
  public boolean equals(Object that) {
    if (that == null) {
      return false;
    }
    if (that instanceof Column) {
      return this.equals((Column) that);
    }
    return false;
  }

  /**
   * Checks if this column equals another.
   *
   * @param that column to compare
   * @return true if this column equals that, false otherwise
   */
  public boolean equals(Column that) {
    return this.compareTo(that) == 0;
  }

  private static int hash(byte[] b) {
    if (b == null) {
      return 0;
    }

    return WritableComparator.hashBytes(b, b.length);
  }

  @Override
  public int hashCode() {
    return hash(columnFamily) + hash(columnQualifier) + hash(columnVisibility);
  }

  /**
   * Gets the column family. Not a defensive copy.
   *
   * @return family
   */
  public byte[] getColumnFamily() {
    return columnFamily;
  }

  /**
   * Gets the column qualifier. Not a defensive copy.
   *
   * @return qualifier
   */
  public byte[] getColumnQualifier() {
    return columnQualifier;
  }

  /**
   * Gets the column visibility. Not a defensive copy.
   *
   * @return visibility
   */
  public byte[] getColumnVisibility() {
    return columnVisibility;
  }

  /**
   * Gets a string representation of this column. The family, qualifier, and visibility are
   * interpreted as strings using the UTF-8 encoding; nulls are interpreted as empty strings.
   *
   * @return string form of column
   */
  @Override
  public String toString() {
    return new String(columnFamily == null ? new byte[0] : columnFamily, UTF_8) + ":"
        + new String(columnQualifier == null ? new byte[0] : columnQualifier, UTF_8) + ":"
        + new String(columnVisibility == null ? new byte[0] : columnVisibility, UTF_8);
  }

  /**
   * Converts this column to Thrift.
   *
   * @return Thrift column
   */
  public TColumn toThrift() {
    return new TColumn(columnFamily == null ? null : ByteBuffer.wrap(columnFamily),
        columnQualifier == null ? null : ByteBuffer.wrap(columnQualifier),
        columnVisibility == null ? null : ByteBuffer.wrap(columnVisibility));
  }

}

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

import static org.apache.accumulo.core.util.ByteBufferUtil.toBytes;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.dataImpl.thrift.TKey;
import org.apache.accumulo.core.dataImpl.thrift.TKeyValue;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableUtils;

/**
 * This is the Key used to store and access individual values in Accumulo. A Key is a tuple composed
 * of a row, column family, column qualifier, column visibility, timestamp, and delete marker.
 * <p>
 * Keys are comparable and therefore have a sorted order defined by {@link #compareTo(Key)}.
 */
public class Key implements WritableComparable<Key>, Cloneable {

  protected byte[] row;
  protected byte[] colFamily;
  protected byte[] colQualifier;
  protected byte[] colVisibility;
  protected long timestamp;
  protected boolean deleted;

  /**
   * Create a {@link Key} builder.
   *
   * @since 2.0
   * @param copyBytes if the bytes of the {@link Key} components should be copied
   * @return the builder at the {@link KeyBuilder.RowStep}
   */
  public static KeyBuilder.RowStep builder(boolean copyBytes) {
    return new KeyBuilder.KeyBuilderImpl(copyBytes);
  }

  /**
   * Create a {@link Key} builder. Using the builder makes it easy to mix types, like {@code String}
   * and {@code byte[]}, for different fields. Copy bytes defaults to true.
   *
   * @since 2.0
   * @return the builder at the {@link KeyBuilder.RowStep}
   * @see #builder(boolean)
   */
  public static KeyBuilder.RowStep builder() {
    return new KeyBuilder.KeyBuilderImpl(true);
  }

  @Override
  public boolean equals(Object o) {
    if (o instanceof Key) {
      return this.equals((Key) o, PartialKey.ROW_COLFAM_COLQUAL_COLVIS_TIME_DEL);
    }
    return false;
  }

  private static final byte[] EMPTY_BYTES = new byte[0];

  static byte[] copyIfNeeded(byte[] ba, int off, int len, boolean copyData) {
    if (len == 0) {
      return EMPTY_BYTES;
    }

    if (!copyData && ba.length == len && off == 0) {
      return ba;
    }

    byte[] copy = new byte[len];
    System.arraycopy(ba, off, copy, 0, len);
    return copy;
  }

  private final void init(byte[] r, int rOff, int rLen, byte[] cf, int cfOff, int cfLen, byte[] cq,
      int cqOff, int cqLen, byte[] cv, int cvOff, int cvLen, long ts, boolean del, boolean copy) {
    row = copyIfNeeded(r, rOff, rLen, copy);
    colFamily = copyIfNeeded(cf, cfOff, cfLen, copy);
    colQualifier = copyIfNeeded(cq, cqOff, cqLen, copy);
    colVisibility = copyIfNeeded(cv, cvOff, cvLen, copy);
    timestamp = ts;
    deleted = del;
  }

  /**
   * Creates a key with empty row, empty column family, empty column qualifier, empty column
   * visibility, timestamp {@link Long#MAX_VALUE}, and delete marker false.
   */
  public Key() {
    row = EMPTY_BYTES;
    colFamily = EMPTY_BYTES;
    colQualifier = EMPTY_BYTES;
    colVisibility = EMPTY_BYTES;
    timestamp = Long.MAX_VALUE;
    deleted = false;
  }

  /**
   * Creates a key with the specified row, empty column family, empty column qualifier, empty column
   * visibility, timestamp {@link Long#MAX_VALUE}, and delete marker false. This constructor creates
   * a copy of row.
   * <p>
   * To avoid copying, use
   * {@link Key#Key(byte[] row, byte[] cf, byte[] cq, byte[] cv, long ts, boolean deleted, boolean copy)}
   * instead.
   *
   * @param row row ID
   * @see #builder()
   */
  public Key(Text row) {
    init(row.getBytes(), 0, row.getLength(), EMPTY_BYTES, 0, 0, EMPTY_BYTES, 0, 0, EMPTY_BYTES, 0,
        0, Long.MAX_VALUE, false, true);
  }

  /**
   * Creates a key with the specified row, empty column family, empty column qualifier, empty column
   * visibility, timestamp {@link Long#MAX_VALUE}, and delete marker false. This constructor creates
   * a copy of row.
   * <p>
   * To avoid copying, use
   * {@link Key#Key(byte[] row, byte[] cf, byte[] cq, byte[] cv, long ts, boolean deleted, boolean copy)}
   * instead.
   *
   * @param row row ID
   * @since 1.8.0
   * @see #builder()
   */
  public Key(byte[] row) {
    init(row, 0, row.length, EMPTY_BYTES, 0, 0, EMPTY_BYTES, 0, 0, EMPTY_BYTES, 0, 0,
        Long.MAX_VALUE, false, true);
  }

  /**
   * Creates a key with the specified row, empty column family, empty column qualifier, empty column
   * visibility, the specified timestamp, and delete marker false. This constructor creates a copy
   * of row.
   * <p>
   * To avoid copying, use
   * {@link Key#Key(byte[] row, byte[] cf, byte[] cq, byte[] cv, long ts, boolean deleted, boolean copy)}
   * instead.
   *
   * @param row row ID
   * @param ts timestamp
   * @see #builder()
   */
  public Key(Text row, long ts) {
    this(row);
    timestamp = ts;
  }

  /**
   * Creates a key with the specified row, empty column family, empty column qualifier, empty column
   * visibility, the specified timestamp, and delete marker false. This constructor creates a copy
   * of row.
   * <p>
   * To avoid copying, use
   * {@link Key#Key(byte[] row, byte[] cf, byte[] cq, byte[] cv, long ts, boolean deleted, boolean copy)}
   * instead.
   *
   * @param row row ID
   * @param ts timestamp
   * @since 1.8.0
   * @see #builder()
   */
  public Key(byte[] row, long ts) {
    this(row);
    timestamp = ts;
  }

  /**
   * Creates a key. The delete marker defaults to false. This constructor creates a copy of each
   * specified array.
   * <p>
   * To avoid copying, use
   * {@link Key#Key(byte[] row, byte[] cf, byte[] cq, byte[] cv, long ts, boolean deleted, boolean copy)}
   * instead.
   *
   * @param row bytes containing row ID
   * @param rOff offset into row where key's row ID begins (inclusive)
   * @param rLen length of row ID in row
   * @param cf bytes containing column family
   * @param cfOff offset into cf where key's column family begins (inclusive)
   * @param cfLen length of column family in cf
   * @param cq bytes containing column qualifier
   * @param cqOff offset into cq where key's column qualifier begins (inclusive)
   * @param cqLen length of column qualifier in cq
   * @param cv bytes containing column visibility
   * @param cvOff offset into cv where key's column visibility begins (inclusive)
   * @param cvLen length of column visibility in cv
   * @param ts timestamp
   * @see #builder()
   */
  public Key(byte[] row, int rOff, int rLen, byte[] cf, int cfOff, int cfLen, byte[] cq, int cqOff,
      int cqLen, byte[] cv, int cvOff, int cvLen, long ts) {
    init(row, rOff, rLen, cf, cfOff, cfLen, cq, cqOff, cqLen, cv, cvOff, cvLen, ts, false, true);
  }

  /**
   * Creates a key.
   *
   * @param row bytes containing row ID
   * @param rOff offset into row where key's row ID begins (inclusive)
   * @param rLen length of row ID in row
   * @param cf bytes containing column family
   * @param cfOff offset into cf where key's column family begins (inclusive)
   * @param cfLen length of column family in cf
   * @param cq bytes containing column qualifier
   * @param cqOff offset into cq where key's column qualifier begins (inclusive)
   * @param cqLen length of column qualifier in cq
   * @param cv bytes containing column visibility
   * @param cvOff offset into cv where key's column visibility begins (inclusive)
   * @param cvLen length of column visibility in cv
   * @param ts timestamp
   * @param deleted delete marker
   * @param copy if true, forces copy of byte array values into key
   * @see #builder()
   */
  Key(byte[] row, int rOff, int rLen, byte[] cf, int cfOff, int cfLen, byte[] cq, int cqOff,
      int cqLen, byte[] cv, int cvOff, int cvLen, long ts, boolean deleted, boolean copy) {
    init(row, rOff, rLen, cf, cfOff, cfLen, cq, cqOff, cqLen, cv, cvOff, cvLen, ts, deleted, copy);
  }

  /**
   * Creates a key. The delete marker defaults to false. This constructor creates a copy of each
   * specified array.
   * <p>
   * To avoid copying, use
   * {@link Key#Key(byte[] row, byte[] cf, byte[] cq, byte[] cv, long ts, boolean deleted, boolean copy)}
   * instead.
   *
   * @param row row ID
   * @param colFamily column family
   * @param colQualifier column qualifier
   * @param colVisibility column visibility
   * @param timestamp timestamp
   * @see #builder()
   */
  public Key(byte[] row, byte[] colFamily, byte[] colQualifier, byte[] colVisibility,
      long timestamp) {
    this(row, colFamily, colQualifier, colVisibility, timestamp, false, true);
  }

  /**
   * Creates a key. This constructor creates a copy of each specified arrays.
   * <p>
   * To avoid copying, use
   * {@link Key#Key(byte[] row, byte[] cf, byte[] cq, byte[] cv, long ts, boolean deleted, boolean copy)}
   * instead.
   *
   * @param row row ID
   * @param cf column family
   * @param cq column qualifier
   * @param cv column visibility
   * @param ts timestamp
   * @param deleted delete marker
   * @see #builder()
   */
  public Key(byte[] row, byte[] cf, byte[] cq, byte[] cv, long ts, boolean deleted) {
    this(row, cf, cq, cv, ts, deleted, true);
  }

  /**
   * Creates a key.
   *
   * @param row row ID
   * @param cf column family
   * @param cq column qualifier
   * @param cv column visibility
   * @param ts timestamp
   * @param deleted delete marker
   * @param copy if true, forces copy of byte array values into key
   * @see #builder()
   */
  public Key(byte[] row, byte[] cf, byte[] cq, byte[] cv, long ts, boolean deleted, boolean copy) {
    init(row, 0, row.length, cf, 0, cf.length, cq, 0, cq.length, cv, 0, cv.length, ts, deleted,
        copy);
  }

  /**
   * Creates a key with the specified row, the specified column family, empty column qualifier,
   * empty column visibility, timestamp {@link Long#MAX_VALUE}, and delete marker false. This
   * constructor creates a copy of row.
   * <p>
   * To avoid copying, use
   * {@link Key#Key(byte[] row, byte[] cf, byte[] cq, byte[] cv, long ts, boolean deleted, boolean copy)}
   * instead.
   *
   * @see #builder()
   */
  public Key(Text row, Text cf) {
    init(row.getBytes(), 0, row.getLength(), cf.getBytes(), 0, cf.getLength(), EMPTY_BYTES, 0, 0,
        EMPTY_BYTES, 0, 0, Long.MAX_VALUE, false, true);
  }

  /**
   * Creates a key with the specified row, the specified column family, empty column qualifier,
   * empty column visibility, timestamp {@link Long#MAX_VALUE}, and delete marker false. This
   * constructor creates a copy of each specified array.
   * <p>
   * To avoid copying, use
   * {@link Key#Key(byte[] row, byte[] cf, byte[] cq, byte[] cv, long ts, boolean deleted, boolean copy)}
   * instead.
   *
   * @since 1.8.0
   * @see #builder()
   */
  public Key(byte[] row, byte[] cf) {
    init(row, 0, row.length, cf, 0, cf.length, EMPTY_BYTES, 0, 0, EMPTY_BYTES, 0, 0, Long.MAX_VALUE,
        false, true);
  }

  /**
   * Creates a key with the specified row, the specified column family, the specified column
   * qualifier, empty column visibility, timestamp {@link Long#MAX_VALUE}, and delete marker false.
   * This constructor creates a copy of row.
   * <p>
   * To avoid copying, use
   * {@link Key#Key(byte[] row, byte[] cf, byte[] cq, byte[] cv, long ts, boolean deleted, boolean copy)}
   * instead.
   *
   * @see #builder()
   */
  public Key(Text row, Text cf, Text cq) {
    init(row.getBytes(), 0, row.getLength(), cf.getBytes(), 0, cf.getLength(), cq.getBytes(), 0,
        cq.getLength(), EMPTY_BYTES, 0, 0, Long.MAX_VALUE, false, true);
  }

  /**
   * Creates a key with the specified row, the specified column family, the specified column
   * qualifier, empty column visibility, timestamp {@link Long#MAX_VALUE}, and delete marker false.
   * This constructor creates a copy of each specified array.
   * <p>
   * To avoid copying, use
   * {@link Key#Key(byte[] row, byte[] cf, byte[] cq, byte[] cv, long ts, boolean deleted, boolean copy)}
   * instead.
   *
   * @since 1.8.0
   * @see #builder()
   */
  public Key(byte[] row, byte[] cf, byte[] cq) {
    init(row, 0, row.length, cf, 0, cf.length, cq, 0, cq.length, EMPTY_BYTES, 0, 0, Long.MAX_VALUE,
        false, true);
  }

  /**
   * Creates a key with the specified row, the specified column family, the specified column
   * qualifier, the specified column visibility, timestamp {@link Long#MAX_VALUE}, and delete marker
   * false. This constructor creates a copy of row.
   * <p>
   * To avoid copying, use
   * {@link Key#Key(byte[] row, byte[] cf, byte[] cq, byte[] cv, long ts, boolean deleted, boolean copy)}
   * instead.
   *
   * @see #builder()
   */
  public Key(Text row, Text cf, Text cq, Text cv) {
    init(row.getBytes(), 0, row.getLength(), cf.getBytes(), 0, cf.getLength(), cq.getBytes(), 0,
        cq.getLength(), cv.getBytes(), 0, cv.getLength(), Long.MAX_VALUE, false, true);
  }

  /**
   * Creates a key with the specified row, the specified column family, the specified column
   * qualifier, the specified column visibility, timestamp {@link Long#MAX_VALUE}, and delete marker
   * false. This constructor creates a copy of each specified array.
   * <p>
   * To avoid copying, use
   * {@link Key#Key(byte[] row, byte[] cf, byte[] cq, byte[] cv, long ts, boolean deleted, boolean copy)}
   * instead.
   *
   * @since 1.8.0
   * @see #builder()
   */
  public Key(byte[] row, byte[] cf, byte[] cq, byte[] cv) {
    init(row, 0, row.length, cf, 0, cf.length, cq, 0, cq.length, cv, 0, cv.length, Long.MAX_VALUE,
        false, true);
  }

  /**
   * Creates a key with the specified row, the specified column family, the specified column
   * qualifier, empty column visibility, the specified timestamp, and delete marker false. This
   * constructor creates a copy of row.
   * <p>
   * To avoid copying, use
   * {@link Key#Key(byte[] row, byte[] cf, byte[] cq, byte[] cv, long ts, boolean deleted, boolean copy)}
   * instead.
   *
   * @see #builder()
   */
  public Key(Text row, Text cf, Text cq, long ts) {
    init(row.getBytes(), 0, row.getLength(), cf.getBytes(), 0, cf.getLength(), cq.getBytes(), 0,
        cq.getLength(), EMPTY_BYTES, 0, 0, ts, false, true);
  }

  /**
   * Creates a key with the specified row, the specified column family, the specified column
   * qualifier, empty column visibility, the specified timestamp, and delete marker false. This
   * constructor creates a copy of each specified array.
   * <p>
   * To avoid copying, use
   * {@link Key#Key(byte[] row, byte[] cf, byte[] cq, byte[] cv, long ts, boolean deleted, boolean copy)}
   * instead.
   *
   * @since 1.8.0
   * @see #builder()
   */
  public Key(byte[] row, byte[] cf, byte[] cq, long ts) {
    init(row, 0, row.length, cf, 0, cf.length, cq, 0, cq.length, EMPTY_BYTES, 0, 0, ts, false,
        true);
  }

  /**
   * Creates a key with the specified row, the specified column family, the specified column
   * qualifier, the specified column visibility, the specified timestamp, and delete marker false.
   * This constructor creates a copy of row.
   * <p>
   * To avoid copying, use
   * {@link Key#Key(byte[] row, byte[] cf, byte[] cq, byte[] cv, long ts, boolean deleted, boolean copy)}
   * instead.
   *
   * @see #builder()
   */
  public Key(Text row, Text cf, Text cq, Text cv, long ts) {
    init(row.getBytes(), 0, row.getLength(), cf.getBytes(), 0, cf.getLength(), cq.getBytes(), 0,
        cq.getLength(), cv.getBytes(), 0, cv.getLength(), ts, false, true);
  }

  /**
   * Creates a key with the specified row, the specified column family, the specified column
   * qualifier, the specified column visibility, the specified timestamp, and delete marker false.
   * This constructor creates a copy of row.
   * <p>
   * To avoid copying, use
   * {@link Key#Key(byte[] row, byte[] cf, byte[] cq, byte[] cv, long ts, boolean deleted, boolean copy)}
   * instead.
   *
   * @see #builder()
   */
  public Key(Text row, Text cf, Text cq, ColumnVisibility cv, long ts) {
    byte[] expr = cv.getExpression();
    init(row.getBytes(), 0, row.getLength(), cf.getBytes(), 0, cf.getLength(), cq.getBytes(), 0,
        cq.getLength(), expr, 0, expr.length, ts, false, true);
  }

  /**
   * Creates a key with the specified row, the specified column family, the specified column
   * qualifier, the specified column visibility, the specified timestamp, and delete marker false.
   * This constructor creates a copy of each specified array.
   * <p>
   * To avoid copying, use
   * {@link Key#Key(byte[] row, byte[] cf, byte[] cq, byte[] cv, long ts, boolean deleted, boolean copy)}
   * instead.
   *
   * @since 1.8.0
   * @see #builder()
   */
  public Key(byte[] row, byte[] cf, byte[] cq, ColumnVisibility cv, long ts) {
    byte[] expr = cv.getExpression();
    init(row, 0, row.length, cf, 0, cf.length, cq, 0, cq.length, expr, 0, expr.length, ts, false,
        true);
  }

  /**
   * Converts CharSequence to Text and creates a Key using {@link #Key(Text)}.
   *
   * @see #builder()
   */
  public Key(CharSequence row) {
    this(new Text(row.toString()));
  }

  /**
   * Converts CharSequence to Text and creates a Key using {@link #Key(Text,Text)}.
   *
   * @see #builder()
   */
  public Key(CharSequence row, CharSequence cf) {
    this(new Text(row.toString()), new Text(cf.toString()));
  }

  /**
   * Converts CharSequence to Text and creates a Key using {@link #Key(Text,Text,Text)}.
   *
   * @see #builder()
   */
  public Key(CharSequence row, CharSequence cf, CharSequence cq) {
    this(new Text(row.toString()), new Text(cf.toString()), new Text(cq.toString()));
  }

  /**
   * Converts CharSequence to Text and creates a Key using {@link #Key(Text,Text,Text,Text)}.
   *
   * @see #builder()
   */
  public Key(CharSequence row, CharSequence cf, CharSequence cq, CharSequence cv) {
    this(new Text(row.toString()), new Text(cf.toString()), new Text(cq.toString()),
        new Text(cv.toString()));
  }

  /**
   * Converts CharSequence to Text and creates a Key using {@link #Key(Text,Text,Text,long)}.
   *
   * @see #builder()
   */
  public Key(CharSequence row, CharSequence cf, CharSequence cq, long ts) {
    this(new Text(row.toString()), new Text(cf.toString()), new Text(cq.toString()), ts);
  }

  /**
   * Converts CharSequence to Text and creates a Key using {@link #Key(Text,Text,Text,Text,long)}.
   *
   * @see #builder()
   */
  public Key(CharSequence row, CharSequence cf, CharSequence cq, CharSequence cv, long ts) {
    this(new Text(row.toString()), new Text(cf.toString()), new Text(cq.toString()),
        new Text(cv.toString()), ts);
  }

  /**
   * Converts CharSequence to Text and creates a Key using
   * {@link #Key(Text,Text,Text,ColumnVisibility,long)}.
   *
   * @see #builder()
   */
  public Key(CharSequence row, CharSequence cf, CharSequence cq, ColumnVisibility cv, long ts) {
    this(new Text(row.toString()), new Text(cf.toString()), new Text(cq.toString()),
        new Text(cv.getExpression()), ts);
  }

  private byte[] followingArray(byte[] ba) {
    byte[] fba = new byte[ba.length + 1];
    System.arraycopy(ba, 0, fba, 0, ba.length);
    fba[ba.length] = (byte) 0x00;
    return fba;
  }

  /**
   * Returns a key that will sort immediately after this key.
   *
   * @param part PartialKey except {@link PartialKey#ROW_COLFAM_COLQUAL_COLVIS_TIME_DEL}
   */
  public Key followingKey(PartialKey part) {
    Key returnKey = new Key();
    switch (part) {
      case ROW:
        returnKey.row = followingArray(row);
        break;
      case ROW_COLFAM:
        returnKey.row = row;
        returnKey.colFamily = followingArray(colFamily);
        break;
      case ROW_COLFAM_COLQUAL:
        returnKey.row = row;
        returnKey.colFamily = colFamily;
        returnKey.colQualifier = followingArray(colQualifier);
        break;
      case ROW_COLFAM_COLQUAL_COLVIS:
        // This isn't useful for inserting into accumulo, but may be useful for lookups.
        returnKey.row = row;
        returnKey.colFamily = colFamily;
        returnKey.colQualifier = colQualifier;
        returnKey.colVisibility = followingArray(colVisibility);
        break;
      case ROW_COLFAM_COLQUAL_COLVIS_TIME:
        returnKey.row = row;
        returnKey.colFamily = colFamily;
        returnKey.colQualifier = colQualifier;
        returnKey.colVisibility = colVisibility;
        returnKey.setTimestamp(timestamp - 1);
        returnKey.deleted = false;
        break;
      default:
        throw new IllegalArgumentException("Partial key specification " + part + " disallowed");
    }
    return returnKey;
  }

  /**
   * Creates a key with the same row, column family, column qualifier, column visibility, timestamp,
   * and delete marker as the given key.
   */
  public Key(Key other) {
    set(other);
  }

  /**
   * Creates a key from Thrift.
   *
   * @param tkey Thrift key
   */
  public Key(TKey tkey) {
    this.row = toBytes(tkey.row);
    this.colFamily = toBytes(tkey.colFamily);
    this.colQualifier = toBytes(tkey.colQualifier);
    this.colVisibility = toBytes(tkey.colVisibility);
    this.timestamp = tkey.timestamp;
    this.deleted = false;

    if (row == null) {
      throw new IllegalArgumentException("null row");
    }
    if (colFamily == null) {
      throw new IllegalArgumentException("null column family");
    }
    if (colQualifier == null) {
      throw new IllegalArgumentException("null column qualifier");
    }
    if (colVisibility == null) {
      throw new IllegalArgumentException("null column visibility");
    }
  }

  /**
   * Writes the row ID into the given <code>Text</code>. This method gives users control over
   * allocation of Text objects by copying into the passed in text.
   *
   * @param r <code>Text</code> object to copy into
   * @return the <code>Text</code> that was passed in
   */
  public Text getRow(Text r) {
    r.set(row, 0, row.length);
    return r;
  }

  /**
   * Returns the row ID as a byte sequence. This method returns a pointer to the key's internal data
   * and does not copy it.
   *
   * @return ByteSequence that points to the internal key row ID data
   */
  public ByteSequence getRowData() {
    return new ArrayByteSequence(row);
  }

  /**
   * Gets the row ID as a <code>Text</code> object.
   *
   * @return Text containing the row ID
   */
  public Text getRow() {
    return getRow(new Text());
  }

  /**
   * Compares this key's row ID with another.
   *
   * @param r row ID to compare
   * @return same as {@link #getRow()}.compareTo(r)
   */
  public int compareRow(Text r) {
    return WritableComparator.compareBytes(row, 0, row.length, r.getBytes(), 0, r.getLength());
  }

  /**
   * Returns the column family as a byte sequence. This method returns a pointer to the key's
   * internal data and does not copy it.
   *
   * @return ByteSequence that points to the internal key column family data
   */
  public ByteSequence getColumnFamilyData() {
    return new ArrayByteSequence(colFamily);
  }

  /**
   * Writes the column family into the given <code>Text</code>. This method gives users control over
   * allocation of Text objects by copying into the passed in text.
   *
   * @param cf <code>Text</code> object to copy into
   * @return the <code>Text</code> that was passed in
   */
  public Text getColumnFamily(Text cf) {
    cf.set(colFamily, 0, colFamily.length);
    return cf;
  }

  /**
   * Gets the column family as a <code>Text</code> object.
   *
   * @return Text containing the column family
   */
  public Text getColumnFamily() {
    return getColumnFamily(new Text());
  }

  /**
   * Compares this key's column family with another.
   *
   * @param cf column family to compare
   * @return same as {@link #getColumnFamily()}.compareTo(cf)
   */

  public int compareColumnFamily(Text cf) {
    return WritableComparator.compareBytes(colFamily, 0, colFamily.length, cf.getBytes(), 0,
        cf.getLength());
  }

  /**
   * Returns the column qualifier as a byte sequence. This method returns a pointer to the key's
   * internal data and does not copy it.
   *
   * @return ByteSequence that points to the internal key column qualifier data
   */
  public ByteSequence getColumnQualifierData() {
    return new ArrayByteSequence(colQualifier);
  }

  /**
   * Writes the column qualifier into the given <code>Text</code>. This method gives users control
   * over allocation of Text objects by copying into the passed in text.
   *
   * @param cq <code>Text</code> object to copy into
   * @return the <code>Text</code> that was passed in
   */
  public Text getColumnQualifier(Text cq) {
    cq.set(colQualifier, 0, colQualifier.length);
    return cq;
  }

  /**
   * Gets the column qualifier as a <code>Text</code> object.
   *
   * @return Text containing the column qualifier
   */
  public Text getColumnQualifier() {
    return getColumnQualifier(new Text());
  }

  /**
   * Compares this key's column qualifier with another.
   *
   * @param cq column qualifier to compare
   * @return same as {@link #getColumnQualifier()}.compareTo(cq)
   */
  public int compareColumnQualifier(Text cq) {
    return WritableComparator.compareBytes(colQualifier, 0, colQualifier.length, cq.getBytes(), 0,
        cq.getLength());
  }

  /**
   * Sets the timestamp.
   *
   * @param ts timestamp
   */
  public void setTimestamp(long ts) {
    this.timestamp = ts;
  }

  /**
   * Gets the timestamp.
   *
   * @return timestamp
   */
  public long getTimestamp() {
    return timestamp;
  }

  /**
   * Determines if this key is deleted (i.e., has a delete marker = true).
   *
   * @return true if key is deleted, false if not
   */
  public boolean isDeleted() {
    return deleted;
  }

  /**
   * Sets the delete marker on this key.
   *
   * @param deleted delete marker (true to delete)
   */
  public void setDeleted(boolean deleted) {
    this.deleted = deleted;
  }

  /**
   * Returns the column visibility as a byte sequence. This method returns a pointer to the key's
   * internal data and does not copy it.
   *
   * @return ByteSequence that points to the internal key column visibility data
   */
  public ByteSequence getColumnVisibilityData() {
    return new ArrayByteSequence(colVisibility);
  }

  /**
   * Gets the column visibility as a <code>Text</code> object.
   *
   * @return Text containing the column visibility
   */
  public final Text getColumnVisibility() {
    return getColumnVisibility(new Text());
  }

  /**
   * Writes the column visibility into the given <code>Text</code>. This method gives users control
   * over allocation of Text objects by copying into the passed in text.
   *
   * @param cv <code>Text</code> object to copy into
   * @return the <code>Text</code> that was passed in
   */
  public final Text getColumnVisibility(Text cv) {
    cv.set(colVisibility, 0, colVisibility.length);
    return cv;
  }

  /**
   * Gets the column visibility. <b>WARNING:</b> using this method may inhibit performance since a
   * new ColumnVisibility object is created on every call.
   *
   * @return ColumnVisibility representing the column visibility
   * @since 1.5.0
   */
  public final ColumnVisibility getColumnVisibilityParsed() {
    return new ColumnVisibility(colVisibility);
  }

  /**
   * Sets this key's row, column family, column qualifier, column visibility, timestamp, and delete
   * marker to be the same as another key's. This method does not copy data from the other key, but
   * only references to it.
   *
   * @param k key to set from
   */
  public void set(Key k) {
    row = k.row;
    colFamily = k.colFamily;
    colQualifier = k.colQualifier;
    colVisibility = k.colVisibility;
    timestamp = k.timestamp;
    deleted = k.deleted;
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    // this method is a little screwy so it will be compatible with older
    // code that serialized data

    int colFamilyOffset = WritableUtils.readVInt(in);
    int colQualifierOffset = WritableUtils.readVInt(in);
    int colVisibilityOffset = WritableUtils.readVInt(in);
    int totalLen = WritableUtils.readVInt(in);

    row = new byte[colFamilyOffset];
    colFamily = new byte[colQualifierOffset - colFamilyOffset];
    colQualifier = new byte[colVisibilityOffset - colQualifierOffset];
    colVisibility = new byte[totalLen - colVisibilityOffset];

    in.readFully(row);
    in.readFully(colFamily);
    in.readFully(colQualifier);
    in.readFully(colVisibility);

    timestamp = WritableUtils.readVLong(in);
    deleted = in.readBoolean();
  }

  @Override
  public void write(DataOutput out) throws IOException {

    int colFamilyOffset = row.length;
    int colQualifierOffset = colFamilyOffset + colFamily.length;
    int colVisibilityOffset = colQualifierOffset + colQualifier.length;
    int totalLen = colVisibilityOffset + colVisibility.length;

    WritableUtils.writeVInt(out, colFamilyOffset);
    WritableUtils.writeVInt(out, colQualifierOffset);
    WritableUtils.writeVInt(out, colVisibilityOffset);

    WritableUtils.writeVInt(out, totalLen);

    out.write(row);
    out.write(colFamily);
    out.write(colQualifier);
    out.write(colVisibility);

    WritableUtils.writeVLong(out, timestamp);
    out.writeBoolean(deleted);
  }

  /**
   * Compares part of a key. For example, compares just the row and column family, and if those are
   * equal then return true.
   *
   * @param other key to compare to
   * @param part part of key to compare
   * @return true if specified parts of keys match, false otherwise
   */
  public boolean equals(Key other, PartialKey part) {
    switch (part) {
      case ROW:
        return isEqual(row, other.row);
      case ROW_COLFAM:
        return isEqual(row, other.row) && isEqual(colFamily, other.colFamily);
      case ROW_COLFAM_COLQUAL:
        return isEqual(row, other.row) && isEqual(colFamily, other.colFamily)
            && isEqual(colQualifier, other.colQualifier);
      case ROW_COLFAM_COLQUAL_COLVIS:
        return isEqual(row, other.row) && isEqual(colFamily, other.colFamily)
            && isEqual(colQualifier, other.colQualifier)
            && isEqual(colVisibility, other.colVisibility);
      case ROW_COLFAM_COLQUAL_COLVIS_TIME:
        return isEqual(row, other.row) && isEqual(colFamily, other.colFamily)
            && isEqual(colQualifier, other.colQualifier)
            && isEqual(colVisibility, other.colVisibility) && timestamp == other.timestamp;
      case ROW_COLFAM_COLQUAL_COLVIS_TIME_DEL:
        return isEqual(row, other.row) && isEqual(colFamily, other.colFamily)
            && isEqual(colQualifier, other.colQualifier)
            && isEqual(colVisibility, other.colVisibility) && timestamp == other.timestamp
            && deleted == other.deleted;
      default:
        throw new IllegalArgumentException("Unrecognized partial key specification " + part);
    }
  }

  /**
   * Compares elements of a key given by a {@link PartialKey}. The corresponding elements (row,
   * column family, column qualifier, column visibility, timestamp, and delete marker) are compared
   * in order until unequal elements are found. The row, column family, column qualifier, and column
   * visibility are compared lexicographically and sorted ascending. The timestamps are compared
   * numerically and sorted descending so that the most recent data comes first. Lastly, a delete
   * marker of true sorts before a delete marker of false. The result of the first unequal
   * comparison is returned.
   *
   * For example, for {@link PartialKey#ROW_COLFAM}, this method compares just the row and column
   * family. If the row IDs are not equal, return the result of the row comparison; otherwise,
   * returns the result of the column family comparison.
   *
   * @param other key to compare to
   * @param part part of key to compare
   * @return comparison result
   * @see #compareTo(Key)
   */
  public int compareTo(Key other, PartialKey part) {
    // check for matching row
    int result =
        WritableComparator.compareBytes(row, 0, row.length, other.row, 0, other.row.length);
    if (result != 0 || part.equals(PartialKey.ROW)) {
      return result;
    }

    // check for matching column family
    result = WritableComparator.compareBytes(colFamily, 0, colFamily.length, other.colFamily, 0,
        other.colFamily.length);
    if (result != 0 || part.equals(PartialKey.ROW_COLFAM)) {
      return result;
    }

    // check for matching column qualifier
    result = WritableComparator.compareBytes(colQualifier, 0, colQualifier.length,
        other.colQualifier, 0, other.colQualifier.length);
    if (result != 0 || part.equals(PartialKey.ROW_COLFAM_COLQUAL)) {
      return result;
    }

    // check for matching column visibility
    result = WritableComparator.compareBytes(colVisibility, 0, colVisibility.length,
        other.colVisibility, 0, other.colVisibility.length);
    if (result != 0 || part.equals(PartialKey.ROW_COLFAM_COLQUAL_COLVIS)) {
      return result;
    }

    // check for matching timestamp
    result = Long.compare(other.timestamp, timestamp);

    if (result != 0 || part.equals(PartialKey.ROW_COLFAM_COLQUAL_COLVIS_TIME)) {
      return result;
    }

    // check for matching deleted flag
    if (deleted) {
      result = other.deleted ? 0 : -1;
    } else {
      result = other.deleted ? 1 : 0;
    }

    return result;
  }

  @Override
  public int compareTo(Key other) {
    return compareTo(other, PartialKey.ROW_COLFAM_COLQUAL_COLVIS_TIME_DEL);
  }

  @Override
  public int hashCode() {
    return WritableComparator.hashBytes(row, row.length)
        + WritableComparator.hashBytes(colFamily, colFamily.length)
        + WritableComparator.hashBytes(colQualifier, colQualifier.length)
        + WritableComparator.hashBytes(colVisibility, colVisibility.length)
        + (int) (timestamp ^ (timestamp >>> 32));
  }

  /**
   * Returns an ASCII printable string form of the given byte array, treating the bytes as ASCII
   * characters. See {@link #appendPrintableString(byte[], int, int, int, StringBuilder)} for
   * caveats.
   *
   * @param ba byte array
   * @param offset offset to start with in byte array (inclusive)
   * @param len number of bytes to print
   * @param maxLen maximum number of bytes to convert to printable form
   * @return printable string
   * @see #appendPrintableString(byte[], int, int, int, StringBuilder)
   */
  public static String toPrintableString(byte[] ba, int offset, int len, int maxLen) {
    return appendPrintableString(ba, offset, len, maxLen, new StringBuilder()).toString();
  }

  /**
   * Appends ASCII printable characters to a string, based on the given byte array, treating the
   * bytes as ASCII characters. If a byte can be converted to a ASCII printable character it is
   * appended as is; otherwise, it is appended as a character code, e.g., %05; for byte value 5. If
   * len &gt; maxlen, the string includes a "TRUNCATED" note at the end.
   *
   * @param ba byte array
   * @param offset offset to start with in byte array (inclusive)
   * @param len number of bytes to print
   * @param maxLen maximum number of bytes to convert to printable form
   * @param sb <code>StringBuilder</code> to append to
   * @return given <code>StringBuilder</code>
   */
  public static StringBuilder appendPrintableString(byte[] ba, int offset, int len, int maxLen,
      StringBuilder sb) {
    int plen = Math.min(len, maxLen);

    for (int i = 0; i < plen; i++) {
      int c = 0xff & ba[offset + i];
      if (c >= 32 && c <= 126) {
        sb.append((char) c);
      } else {
        sb.append("%" + String.format("%02x;", c));
      }
    }

    if (len > maxLen) {
      sb.append("... TRUNCATED");
    }

    return sb;
  }

  private StringBuilder rowColumnStringBuilder() {
    return rowColumnStringBuilder(Constants.MAX_DATA_TO_PRINT);
  }

  private StringBuilder rowColumnStringBuilder(int maxComponentLength) {
    StringBuilder sb = new StringBuilder();
    appendPrintableString(row, 0, row.length, maxComponentLength, sb);
    sb.append(" ");
    appendPrintableString(colFamily, 0, colFamily.length, maxComponentLength, sb);
    sb.append(":");
    appendPrintableString(colQualifier, 0, colQualifier.length, maxComponentLength, sb);
    sb.append(" [");
    appendPrintableString(colVisibility, 0, colVisibility.length, maxComponentLength, sb);
    sb.append("]");
    return sb;
  }

  @Override
  public String toString() {
    StringBuilder sb = rowColumnStringBuilder();
    sb.append(" ");
    sb.append(timestamp);
    sb.append(" ");
    sb.append(deleted);
    return sb.toString();
  }

  /**
   * Stringify this {@link Key}, avoiding truncation of each component, only limiting each component
   * to a length of {@link Integer#MAX_VALUE}
   *
   * @since 1.7.0
   */
  public String toStringNoTruncate() {
    StringBuilder sb = rowColumnStringBuilder(Integer.MAX_VALUE);
    sb.append(" ");
    sb.append(timestamp);
    sb.append(" ");
    sb.append(deleted);
    return sb.toString();
  }

  /**
   * Converts this key to a string, not including timestamp or delete marker.
   *
   * @return string form of key
   */
  public String toStringNoTime() {
    return rowColumnStringBuilder().toString();
  }

  /**
   * Returns the sums of the lengths of the row, column family, column qualifier, and column
   * visibility.
   *
   * @return sum of key field lengths
   */
  public int getLength() {
    return row.length + colFamily.length + colQualifier.length + colVisibility.length;
  }

  /**
   * Same as {@link #getLength()}.
   *
   * @return sum of key field lengths
   */
  public int getSize() {
    return getLength();
  }

  private static boolean isEqual(byte[] a1, byte[] a2) {
    if (a1 == a2) {
      return true;
    }

    int last = a1.length;

    if (last != a2.length) {
      return false;
    }

    if (last == 0) {
      return true;
    }

    // since sorted data is usually compared in accumulo,
    // the prefixes will normally be the same... so compare
    // the last two characters first.. the most likely place
    // to have disorder is at end of the strings when the
    // data is sorted... if those are the same compare the rest
    // of the data forward... comparing backwards is slower
    // (compiler and cpu optimized for reading data forward)..
    // do not want slower comparisons when data is equal...
    // sorting brings equals data together

    last--;

    if (a1[last] == a2[last]) {
      for (int i = 0; i < last; i++) {
        if (a1[i] != a2[i]) {
          return false;
        }
      }
    } else {
      return false;
    }

    return true;

  }

  /**
   * Compresses a list of key/value pairs before sending them via thrift.
   *
   * @param param list of key/value pairs
   * @return list of Thrift key/value pairs
   */
  public static List<TKeyValue> compress(List<? extends KeyValue> param) {

    List<TKeyValue> tkvl = Arrays.asList(new TKeyValue[param.size()]);

    if (!param.isEmpty()) {
      tkvl.set(0, new TKeyValue(param.get(0).getKey().toThrift(),
          ByteBuffer.wrap(param.get(0).getValue().get())));
    }

    for (int i = param.size() - 1; i > 0; i--) {
      Key prevKey = param.get(i - 1).getKey();
      KeyValue kv = param.get(i);
      Key key = kv.getKey();

      TKey newKey = null;

      if (isEqual(prevKey.row, key.row)) {
        newKey = key.toThrift();
        newKey.row = null;
      }

      if (isEqual(prevKey.colFamily, key.colFamily)) {
        if (newKey == null) {
          newKey = key.toThrift();
        }
        newKey.colFamily = null;
      }

      if (isEqual(prevKey.colQualifier, key.colQualifier)) {
        if (newKey == null) {
          newKey = key.toThrift();
        }
        newKey.colQualifier = null;
      }

      if (isEqual(prevKey.colVisibility, key.colVisibility)) {
        if (newKey == null) {
          newKey = key.toThrift();
        }
        newKey.colVisibility = null;
      }

      if (newKey == null) {
        newKey = key.toThrift();
      }

      tkvl.set(i, new TKeyValue(newKey, ByteBuffer.wrap(kv.getValue().get())));
    }

    return tkvl;
  }

  /**
   * Decompresses a list of key/value pairs received from thrift. Decompression occurs in place, in
   * the list.
   *
   * @param param list of Thrift key/value pairs
   */
  public static void decompress(List<TKeyValue> param) {
    for (int i = 1; i < param.size(); i++) {
      TKey prevKey = param.get(i - 1).key;
      TKey key = param.get(i).key;

      if (key.row == null) {
        key.row = prevKey.row;
      }
      if (key.colFamily == null) {
        key.colFamily = prevKey.colFamily;
      }
      if (key.colQualifier == null) {
        key.colQualifier = prevKey.colQualifier;
      }
      if (key.colVisibility == null) {
        key.colVisibility = prevKey.colVisibility;
      }
    }
  }

  /**
   * Gets the row ID as a byte array.
   *
   * @return row ID
   */
  byte[] getRowBytes() {
    return row;
  }

  /**
   * Gets the column family as a byte array.
   *
   * @return column family
   */
  byte[] getColFamily() {
    return colFamily;
  }

  /**
   * Gets the column qualifier as a byte array.
   *
   * @return column qualifier
   */
  byte[] getColQualifier() {
    return colQualifier;
  }

  /**
   * Gets the column visibility as a byte array.
   *
   * @return column visibility
   */
  byte[] getColVisibility() {
    return colVisibility;
  }

  /**
   * Converts this key to Thrift.
   *
   * @return Thrift key
   */
  public TKey toThrift() {
    return new TKey(ByteBuffer.wrap(row), ByteBuffer.wrap(colFamily), ByteBuffer.wrap(colQualifier),
        ByteBuffer.wrap(colVisibility), timestamp);
  }

  @Override
  public Object clone() throws CloneNotSupportedException {
    Key r = (Key) super.clone();
    r.row = Arrays.copyOf(row, row.length);
    r.colFamily = Arrays.copyOf(colFamily, colFamily.length);
    r.colQualifier = Arrays.copyOf(colQualifier, colQualifier.length);
    r.colVisibility = Arrays.copyOf(colVisibility, colVisibility.length);
    return r;
  }
}

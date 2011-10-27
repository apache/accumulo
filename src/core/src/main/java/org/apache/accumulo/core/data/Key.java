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

/**
 * This class is used to group together all the info that goes into a key in 
 * the mapfiles
 * 
 */

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.data.thrift.TKey;
import org.apache.accumulo.core.data.thrift.TKeyValue;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableUtils;

import static org.apache.accumulo.core.util.ByteBufferUtil.toBytes;

public class Key implements WritableComparable<Key>, Cloneable {
  
  protected byte[] row;
  protected byte[] colFamily;
  protected byte[] colQualifier;
  protected byte[] colVisibility;
  protected long timestamp;
  protected boolean deleted;
  
  @Override
  public boolean equals(Object o) {
    if (o instanceof Key)
      return this.equals((Key) o, PartialKey.ROW_COLFAM_COLQUAL_COLVIS_TIME_DEL);
    return false;
  }
  
  private static final byte EMPTY_BYTES[] = new byte[0];
  
  private byte[] copyIfNeeded(byte ba[], int off, int len, boolean copyData) {
    if (len == 0)
      return EMPTY_BYTES;
    
    if (!copyData && ba.length == len && off == 0)
      return ba;
    
    byte[] copy = new byte[len];
    System.arraycopy(ba, off, copy, 0, len);
    return copy;
  }
  
  private final void init(byte r[], int rOff, int rLen, byte cf[], int cfOff, int cfLen, byte cq[], int cqOff, int cqLen, byte cv[], int cvOff, int cvLen,
      long ts, boolean del, boolean copy) {
    row = copyIfNeeded(r, rOff, rLen, copy);
    colFamily = copyIfNeeded(cf, cfOff, cfLen, copy);
    colQualifier = copyIfNeeded(cq, cqOff, cqLen, copy);
    colVisibility = copyIfNeeded(cv, cvOff, cvLen, copy);
    timestamp = ts;
    deleted = del;
  }
  
  public Key() {
    row = EMPTY_BYTES;
    colFamily = EMPTY_BYTES;
    colQualifier = EMPTY_BYTES;
    colVisibility = EMPTY_BYTES;
    timestamp = Long.MAX_VALUE;
    deleted = false;
  }
  
  public Key(Text row) {
    init(row.getBytes(), 0, row.getLength(), EMPTY_BYTES, 0, 0, EMPTY_BYTES, 0, 0, EMPTY_BYTES, 0, 0, Long.MAX_VALUE, false, true);
  }
  
  public Key(Text row, long ts) {
    this(row);
    timestamp = ts;
  }
  
  public Key(byte row[], int rOff, int rLen, byte cf[], int cfOff, int cfLen, byte cq[], int cqOff, int cqLen, byte cv[], int cvOff, int cvLen, long ts) {
    init(row, rOff, rLen, cf, cfOff, cfLen, cq, cqOff, cqLen, cv, cvOff, cvLen, ts, false, true);
  }
  
  public Key(byte[] row, byte[] colFamily, byte[] colQualifier, byte[] colVisibility, long timestamp) {
    this(row, colFamily, colQualifier, colVisibility, timestamp, false, true);
  }
  
  public Key(byte[] row, byte[] cf, byte[] cq, byte[] cv, long ts, boolean deleted) {
    this(row, cf, cq, cv, ts, deleted, true);
  }
  
  public Key(byte[] row, byte[] cf, byte[] cq, byte[] cv, long ts, boolean deleted, boolean copy) {
    init(row, 0, row.length, cf, 0, cf.length, cq, 0, cq.length, cv, 0, cv.length, ts, deleted, copy);
  }
  
  public Key(Text row, Text cf) {
    init(row.getBytes(), 0, row.getLength(), cf.getBytes(), 0, cf.getLength(), EMPTY_BYTES, 0, 0, EMPTY_BYTES, 0, 0, Long.MAX_VALUE, false, true);
  }
  
  public Key(Text row, Text cf, Text cq) {
    init(row.getBytes(), 0, row.getLength(), cf.getBytes(), 0, cf.getLength(), cq.getBytes(), 0, cq.getLength(), EMPTY_BYTES, 0, 0, Long.MAX_VALUE, false, true);
  }
  
  public Key(Text row, Text cf, Text cq, Text cv) {
    init(row.getBytes(), 0, row.getLength(), cf.getBytes(), 0, cf.getLength(), cq.getBytes(), 0, cq.getLength(), cv.getBytes(), 0, cv.getLength(),
        Long.MAX_VALUE, false, true);
  }
  
  public Key(Text row, Text cf, Text cq, long ts) {
    init(row.getBytes(), 0, row.getLength(), cf.getBytes(), 0, cf.getLength(), cq.getBytes(), 0, cq.getLength(), EMPTY_BYTES, 0, 0, ts, false, true);
  }
  
  public Key(Text row, Text cf, Text cq, Text cv, long ts) {
    init(row.getBytes(), 0, row.getLength(), cf.getBytes(), 0, cf.getLength(), cq.getBytes(), 0, cq.getLength(), cv.getBytes(), 0, cv.getLength(), ts, false,
        true);
  }
  
  public Key(Text row, Text cf, Text cq, ColumnVisibility cv, long ts) {
    byte[] expr = cv.getExpression();
    init(row.getBytes(), 0, row.getLength(), cf.getBytes(), 0, cf.getLength(), cq.getBytes(), 0, cq.getLength(), expr, 0, expr.length, ts, false, true);
  }
  
  public Key(CharSequence row) {
    this(new Text(row.toString()));
  }
  
  public Key(CharSequence row, CharSequence cf) {
    this(new Text(row.toString()), new Text(cf.toString()));
  }
  
  public Key(CharSequence row, CharSequence cf, CharSequence cq) {
    this(new Text(row.toString()), new Text(cf.toString()), new Text(cq.toString()));
  }
  
  public Key(CharSequence row, CharSequence cf, CharSequence cq, CharSequence cv) {
    this(new Text(row.toString()), new Text(cf.toString()), new Text(cq.toString()), new Text(cv.toString()));
  }
  
  public Key(CharSequence row, CharSequence cf, CharSequence cq, long ts) {
    this(new Text(row.toString()), new Text(cf.toString()), new Text(cq.toString()), ts);
  }
  
  public Key(CharSequence row, CharSequence cf, CharSequence cq, CharSequence cv, long ts) {
    this(new Text(row.toString()), new Text(cf.toString()), new Text(cq.toString()), new Text(cv.toString()), ts);
  }
  
  public Key(CharSequence row, CharSequence cf, CharSequence cq, ColumnVisibility cv, long ts) {
    this(new Text(row.toString()), new Text(cf.toString()), new Text(cq.toString()), new Text(cv.getExpression()), ts);
  }
  
  private byte[] followingArray(byte ba[]) {
    byte[] fba = new byte[ba.length + 1];
    System.arraycopy(ba, 0, fba, 0, ba.length);
    fba[ba.length] = (byte) 0x00;
    return fba;
  }
  
  /**
   * Returns a key that will sort immediately after this key.
   * 
   * @param part
   *          PartialKey except {@link PartialKey#ROW_COLFAM_COLQUAL_COLVIS_TIME_DEL}
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
  
  public Key(Key other) {
    set(other);
  }
  
  public Key(TKey tkey) {
    this.row = toBytes(tkey.row);
    this.colFamily = toBytes(tkey.colFamily);
    this.colQualifier = toBytes(tkey.colQualifier);
    this.colVisibility = toBytes(tkey.colVisibility);
    this.timestamp = tkey.timestamp;
    this.deleted = false;
  }
  
  public Text getRow(Text r) {
    r.set(row, 0, row.length);
    return r;
  }
  
  public ByteSequence getRowData() {
    return new ArrayByteSequence(row);
  }
  
  public Text getRow() {
    return getRow(new Text());
  }
  
  public int compareRow(Text r) {
    return WritableComparator.compareBytes(row, 0, row.length, r.getBytes(), 0, r.getLength());
  }
  
  public ByteSequence getColumnFamilyData() {
    return new ArrayByteSequence(colFamily);
  }
  
  public Text getColumnFamily(Text cf) {
    cf.set(colFamily, 0, colFamily.length);
    return cf;
  }
  
  public Text getColumnFamily() {
    return getColumnFamily(new Text());
  }
  
  public int compareColumnFamily(Text cf) {
    return WritableComparator.compareBytes(colFamily, 0, colFamily.length, cf.getBytes(), 0, cf.getLength());
  }
  
  public ByteSequence getColumnQualifierData() {
    return new ArrayByteSequence(colQualifier);
  }
  
  public Text getColumnQualifier(Text cq) {
    cq.set(colQualifier, 0, colQualifier.length);
    return cq;
  }
  
  public Text getColumnQualifier() {
    return getColumnQualifier(new Text());
  }
  
  public int compareColumnQualifier(Text cq) {
    return WritableComparator.compareBytes(colQualifier, 0, colQualifier.length, cq.getBytes(), 0, cq.getLength());
  }
  
  public void setTimestamp(long ts) {
    this.timestamp = ts;
  }
  
  public long getTimestamp() {
    return timestamp;
  }
  
  public boolean isDeleted() {
    return deleted;
  }
  
  public void setDeleted(boolean deleted) {
    this.deleted = deleted;
  }
  
  public ByteSequence getColumnVisibilityData() {
    return new ArrayByteSequence(colVisibility);
  }
  
  public final Text getColumnVisibility() {
    return getColumnVisibility(new Text());
  }
  
  public final Text getColumnVisibility(Text cv) {
    cv.set(colVisibility, 0, colVisibility.length);
    return cv;
  }
  
  public void set(Key k) {
    row = k.row;
    colFamily = k.colFamily;
    colQualifier = k.colQualifier;
    colVisibility = k.colVisibility;
    timestamp = k.timestamp;
    deleted = k.deleted;
    
  }
  
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
  
  public boolean equals(Key other, PartialKey part) {
    switch (part) {
      case ROW:
        return isEqual(row, other.row);
      case ROW_COLFAM:
        return isEqual(row, other.row) && isEqual(colFamily, other.colFamily);
      case ROW_COLFAM_COLQUAL:
        return isEqual(row, other.row) && isEqual(colFamily, other.colFamily) && isEqual(colQualifier, other.colQualifier);
      case ROW_COLFAM_COLQUAL_COLVIS:
        return isEqual(row, other.row) && isEqual(colFamily, other.colFamily) && isEqual(colQualifier, other.colQualifier)
            && isEqual(colVisibility, other.colVisibility);
      case ROW_COLFAM_COLQUAL_COLVIS_TIME:
        return isEqual(row, other.row) && isEqual(colFamily, other.colFamily) && isEqual(colQualifier, other.colQualifier)
            && isEqual(colVisibility, other.colVisibility) && timestamp == other.timestamp;
      case ROW_COLFAM_COLQUAL_COLVIS_TIME_DEL:
        return isEqual(row, other.row) && isEqual(colFamily, other.colFamily) && isEqual(colQualifier, other.colQualifier)
            && isEqual(colVisibility, other.colVisibility) && timestamp == other.timestamp && deleted == other.deleted;
      default:
        throw new IllegalArgumentException("Unrecognized partial key specification " + part);
    }
  }
  
  public int compareTo(Key other, PartialKey part) {
    // check for matching row
    int result = WritableComparator.compareBytes(row, 0, row.length, other.row, 0, other.row.length);
    if (result != 0 || part.equals(PartialKey.ROW))
      return result;
    
    // check for matching column family
    result = WritableComparator.compareBytes(colFamily, 0, colFamily.length, other.colFamily, 0, other.colFamily.length);
    if (result != 0 || part.equals(PartialKey.ROW_COLFAM))
      return result;
    
    // check for matching column qualifier
    result = WritableComparator.compareBytes(colQualifier, 0, colQualifier.length, other.colQualifier, 0, other.colQualifier.length);
    if (result != 0 || part.equals(PartialKey.ROW_COLFAM_COLQUAL))
      return result;
    
    // check for matching column visibility
    result = WritableComparator.compareBytes(colVisibility, 0, colVisibility.length, other.colVisibility, 0, other.colVisibility.length);
    if (result != 0 || part.equals(PartialKey.ROW_COLFAM_COLQUAL_COLVIS))
      return result;
    
    // check for matching timestamp
    if (timestamp < other.timestamp)
      result = 1;
    else if (timestamp > other.timestamp)
      result = -1;
    else
      result = 0;
    
    if (result != 0 || part.equals(PartialKey.ROW_COLFAM_COLQUAL_COLVIS_TIME))
      return result;
    
    // check for matching deleted flag
    if (deleted)
      result = other.deleted ? 0 : -1;
    else
      result = other.deleted ? 1 : 0;
    
    return result;
  }
  
  /**
   * determines the order of keys in the MapFiles we must then just make sure that *'s are not ever stored
   */
  
  public int compareTo(Key other) {
    return compareTo(other, PartialKey.ROW_COLFAM_COLQUAL_COLVIS_TIME_DEL);
  }
  
  @Override
  public int hashCode() {
    return WritableComparator.hashBytes(row, row.length) + WritableComparator.hashBytes(colFamily, colFamily.length)
        + WritableComparator.hashBytes(colQualifier, colQualifier.length) + WritableComparator.hashBytes(colVisibility, colVisibility.length);
  }
  
  public static String toPrintableString(byte ba[], int offset, int len, int maxLen) {
    StringBuilder sb = new StringBuilder();
    
    int plen = Math.min(len, maxLen);
    
    for (int i = 0; i < plen; i++) {
      int c = 0xff & ba[offset + i];
      if (c >= 32 && c <= 126)
        sb.append((char) c);
      else
        sb.append("%" + String.format("%02x;", c));
    }
    
    if (len > maxLen) {
      sb.append("... TRUNCATED");
    }
    
    return sb.toString();
  }
  
  public String toString() {
    String labelString = new ColumnVisibility(colVisibility).toString();
    
    String s = toPrintableString(row, 0, row.length, Constants.MAX_DATA_TO_PRINT) + " "
        + toPrintableString(colFamily, 0, colFamily.length, Constants.MAX_DATA_TO_PRINT) + ":"
        + toPrintableString(colQualifier, 0, colQualifier.length, Constants.MAX_DATA_TO_PRINT) + " " + labelString + " " + Long.toString(timestamp) + " "
        + deleted;
    return s;
  }
  
  public String toStringNoTime() {
    
    String labelString = new ColumnVisibility(colVisibility).toString();
    
    String s = new String(row, 0, row.length) + " " + new String(colFamily, 0, colFamily.length) + ":" + new String(colQualifier, 0, colQualifier.length) + " "
        + labelString;
    return s;
  }
  
  public int getLength() {
    return row.length + colFamily.length + colQualifier.length + colVisibility.length;
  }
  
  public int getSize() {
    return getLength();
  }
  
  private static boolean isEqual(byte a1[], byte a2[]) {
    if (a1 == a2)
      return true;
    
    int last = a1.length;
    
    if (last != a2.length)
      return false;
    
    if (last == 0)
      return true;
    
    // since sorted data is usually compared in accumulo,
    // the prefixes will normally be the same... so compare
    // the last two charachters first.. the most likely place
    // to have disorder is at end of the strings when the
    // data is sorted... if those are the same compare the rest
    // of the data forward... comparing backwards is slower
    // (compiler and cpu optimized for reading data forward)..
    // do not want slower comparisons when data is equal...
    // sorting brings equals data together
    
    last--;
    
    if (a1[last] == a2[last]) {
      for (int i = 0; i < last; i++)
        if (a1[i] != a2[i])
          return false;
    } else {
      return false;
    }
    
    return true;
    
  }
  
  /**
   * Use this to compress a list of keys before sending them via thrift.
   * 
   * @param param
   */
  public static List<TKeyValue> compress(List<? extends KeyValue> param) {
    
    List<TKeyValue> tkvl = Arrays.asList(new TKeyValue[param.size()]);
    
    if (param.size() > 0)
      tkvl.set(0, new TKeyValue(param.get(0).key.toThrift(), ByteBuffer.wrap(param.get(0).value)));
    
    for (int i = param.size() - 1; i > 0; i--) {
      Key prevKey = param.get(i - 1).key;
      KeyValue kv = param.get(i);
      Key key = kv.key;
      
      TKey newKey = null;
      
      if (isEqual(prevKey.row, key.row)) {
        newKey = key.toThrift();
        newKey.row = null;
      }
      
      if (isEqual(prevKey.colFamily, key.colFamily)) {
        if (newKey == null)
          newKey = key.toThrift();
        newKey.colFamily = null;
      }
      
      if (isEqual(prevKey.colQualifier, key.colQualifier)) {
        if (newKey == null)
          newKey = key.toThrift();
        newKey.colQualifier = null;
      }
      
      if (isEqual(prevKey.colVisibility, key.colVisibility)) {
        if (newKey == null)
          newKey = key.toThrift();
        newKey.colVisibility = null;
      }
      
      if (newKey == null)
        newKey = key.toThrift();
      
      tkvl.set(i, new TKeyValue(newKey, ByteBuffer.wrap(kv.value)));
    }
    
    return tkvl;
  }
  
  /**
   * Use this to decompress a list of keys received from thrift.
   * 
   * @param param
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
  
  byte[] getRowBytes() {
    return row;
  }
  
  byte[] getColFamily() {
    return colFamily;
  }
  
  byte[] getColQualifier() {
    return colQualifier;
  }
  
  byte[] getColVisibility() {
    return colVisibility;
  }
  
  public TKey toThrift() {
    return new TKey(ByteBuffer.wrap(row), ByteBuffer.wrap(colFamily), ByteBuffer.wrap(colQualifier), ByteBuffer.wrap(colVisibility), timestamp);
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

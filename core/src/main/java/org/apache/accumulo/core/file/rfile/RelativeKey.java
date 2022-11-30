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
package org.apache.accumulo.core.file.rfile;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.function.Supplier;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.util.MutableByteSequence;
import org.apache.accumulo.core.util.UnsynchronizedBuffer;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

public class RelativeKey implements Writable {

  private static final byte BIT = 0x01;

  private Key key;
  private Key prevKey;

  private byte fieldsSame;
  private byte fieldsPrefixed;

  // Exact match compression options (first byte) and flag for further
  private static final byte ROW_SAME = BIT << 0;
  private static final byte CF_SAME = BIT << 1;
  private static final byte CQ_SAME = BIT << 2;
  private static final byte CV_SAME = BIT << 3;
  private static final byte TS_SAME = BIT << 4;
  private static final byte DELETED = BIT << 5;
  // private static final byte UNUSED_1_6 = BIT << 6;
  private static final byte PREFIX_COMPRESSION_ENABLED = (byte) (BIT << 7);

  // Prefix compression (second byte)
  private static final byte ROW_COMMON_PREFIX = BIT << 0;
  private static final byte CF_COMMON_PREFIX = BIT << 1;
  private static final byte CQ_COMMON_PREFIX = BIT << 2;
  private static final byte CV_COMMON_PREFIX = BIT << 3;
  private static final byte TS_DIFF = BIT << 4;

  // private static final byte UNUSED_2_5 = BIT << 5;
  // private static final byte UNUSED_2_6 = BIT << 6;
  // private static final byte UNUSED_2_7 = (byte) (BIT << 7);

  // Values for prefix compression
  int rowCommonPrefixLen;
  int cfCommonPrefixLen;
  int cqCommonPrefixLen;
  int cvCommonPrefixLen;
  long tsDiff;

  /**
   * This constructor is used when one needs to read from an input stream
   */
  public RelativeKey() {

  }

  /**
   * This constructor is used when constructing a key for writing to an output stream
   */
  public RelativeKey(Key prevKey, Key key) {

    this.key = key;

    fieldsSame = 0;
    fieldsPrefixed = 0;

    if (prevKey != null) {

      ByteSequence prevKeyScratch = prevKey.getRowData();
      ByteSequence keyScratch = key.getRowData();
      rowCommonPrefixLen =
          getCommonPrefixLen(prevKeyScratch, keyScratch, ROW_SAME, ROW_COMMON_PREFIX);

      prevKeyScratch = prevKey.getColumnFamilyData();
      keyScratch = key.getColumnFamilyData();
      cfCommonPrefixLen = getCommonPrefixLen(prevKeyScratch, keyScratch, CF_SAME, CF_COMMON_PREFIX);

      prevKeyScratch = prevKey.getColumnQualifierData();
      keyScratch = key.getColumnQualifierData();
      cqCommonPrefixLen = getCommonPrefixLen(prevKeyScratch, keyScratch, CQ_SAME, CQ_COMMON_PREFIX);

      prevKeyScratch = prevKey.getColumnVisibilityData();
      keyScratch = key.getColumnVisibilityData();
      cvCommonPrefixLen = getCommonPrefixLen(prevKeyScratch, keyScratch, CV_SAME, CV_COMMON_PREFIX);

      tsDiff = key.getTimestamp() - prevKey.getTimestamp();
      if (tsDiff == 0) {
        fieldsSame |= TS_SAME;
      } else {
        fieldsPrefixed |= TS_DIFF;
      }

      fieldsSame |= fieldsPrefixed == 0 ? 0 : PREFIX_COMPRESSION_ENABLED;
    }

    // stored deleted information in bit vector instead of its own byte
    if (key.isDeleted()) {
      fieldsSame |= DELETED;
    }
  }

  private int getCommonPrefixLen(ByteSequence prevKeyScratch, ByteSequence keyScratch,
      byte fieldBit, byte commonPrefix) {
    int commonPrefixLen = getCommonPrefix(prevKeyScratch, keyScratch);
    if (commonPrefixLen == -1) {
      fieldsSame |= fieldBit;
    } else if (commonPrefixLen > 1) {
      fieldsPrefixed |= commonPrefix;
    }
    return commonPrefixLen;
  }

  /**
   *
   * @return -1 (exact match) or the number of bytes in common
   */
  static int getCommonPrefix(ByteSequence prev, ByteSequence cur) {
    if (prev == cur) {
      return -1; // infinite... exact match
    }

    int prevLen = prev.length();
    int curLen = cur.length();
    int maxChecks = Math.min(prevLen, curLen);
    int common = 0;
    while (common < maxChecks) {
      int a = prev.byteAt(common) & 0xff;
      int b = cur.byteAt(common) & 0xff;
      if (a != b) {
        return common;
      }
      common++;
    }
    // no differences found
    // either exact or matches the part checked, so if they are the same length, they are an exact
    // match,
    // and if not, then they have a common prefix over all the checks we've done
    return prevLen == curLen ? -1 : maxChecks;
  }

  public void setPrevKey(Key pk) {
    this.prevKey = pk;
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    fieldsSame = in.readByte();
    if ((fieldsSame & PREFIX_COMPRESSION_ENABLED) == PREFIX_COMPRESSION_ENABLED) {
      fieldsPrefixed = in.readByte();
    } else {
      fieldsPrefixed = 0;
    }

    final byte[] row, cf, cq, cv;
    final long ts;

    row = getData(in, ROW_SAME, ROW_COMMON_PREFIX, () -> prevKey.getRowData());
    cf = getData(in, CF_SAME, CF_COMMON_PREFIX, () -> prevKey.getColumnFamilyData());
    cq = getData(in, CQ_SAME, CQ_COMMON_PREFIX, () -> prevKey.getColumnQualifierData());
    cv = getData(in, CV_SAME, CV_COMMON_PREFIX, () -> prevKey.getColumnVisibilityData());

    if ((fieldsSame & TS_SAME) == TS_SAME) {
      ts = prevKey.getTimestamp();
    } else if ((fieldsPrefixed & TS_DIFF) == TS_DIFF) {
      ts = WritableUtils.readVLong(in) + prevKey.getTimestamp();
    } else {
      ts = WritableUtils.readVLong(in);
    }

    this.key = new Key(row, cf, cq, cv, ts, (fieldsSame & DELETED) == DELETED, false);
    this.prevKey = this.key;
  }

  private byte[] getData(DataInput in, byte fieldBit, byte commonPrefix,
      Supplier<ByteSequence> data) throws IOException {
    if ((fieldsSame & fieldBit) == fieldBit) {
      return data.get().toArray();
    } else if ((fieldsPrefixed & commonPrefix) == commonPrefix) {
      return readPrefix(in, data.get());
    } else {
      return read(in);
    }
  }

  public static class SkippR {
    RelativeKey rk;
    int skipped;
    Key prevKey;

    SkippR(RelativeKey rk, int skipped, Key prevKey) {
      this.rk = rk;
      this.skipped = skipped;
      this.prevKey = prevKey;
    }
  }

  public static SkippR fastSkip(DataInput in, Key seekKey, MutableByteSequence value, Key prevKey,
      Key currKey, int entriesLeft) throws IOException {
    // this method mostly avoids object allocation and only does compares when the row changes

    MutableByteSequence row, cf, cq, cv;
    MutableByteSequence prow, pcf, pcq, pcv;

    ByteSequence stopRow = seekKey.getRowData();
    ByteSequence stopCF = seekKey.getColumnFamilyData();
    ByteSequence stopCQ = seekKey.getColumnQualifierData();

    long ts = -1;
    long pts = -1;
    boolean pdel = false;

    int rowCmp = -1, cfCmp = -1, cqCmp = -1;

    if (currKey != null) {

      prow = new MutableByteSequence(currKey.getRowData());
      pcf = new MutableByteSequence(currKey.getColumnFamilyData());
      pcq = new MutableByteSequence(currKey.getColumnQualifierData());
      pcv = new MutableByteSequence(currKey.getColumnVisibilityData());
      pts = currKey.getTimestamp();

      row = new MutableByteSequence(currKey.getRowData());
      cf = new MutableByteSequence(currKey.getColumnFamilyData());
      cq = new MutableByteSequence(currKey.getColumnQualifierData());
      cv = new MutableByteSequence(currKey.getColumnVisibilityData());
      ts = currKey.getTimestamp();

      rowCmp = row.compareTo(stopRow);
      cfCmp = cf.compareTo(stopCF);
      cqCmp = cq.compareTo(stopCQ);

      if (rowCmp >= 0) {
        if (rowCmp > 0) {
          RelativeKey rk = new RelativeKey();
          rk.key = rk.prevKey = new Key(currKey);
          return new SkippR(rk, 0, prevKey);
        }

        if (cfCmp >= 0) {
          if (cfCmp > 0) {
            RelativeKey rk = new RelativeKey();
            rk.key = rk.prevKey = new Key(currKey);
            return new SkippR(rk, 0, prevKey);
          }

          if (cqCmp >= 0) {
            RelativeKey rk = new RelativeKey();
            rk.key = rk.prevKey = new Key(currKey);
            return new SkippR(rk, 0, prevKey);
          }
        }
      }

    } else {
      row = new MutableByteSequence(new byte[64], 0, 0);
      cf = new MutableByteSequence(new byte[64], 0, 0);
      cq = new MutableByteSequence(new byte[64], 0, 0);
      cv = new MutableByteSequence(new byte[64], 0, 0);

      prow = new MutableByteSequence(new byte[64], 0, 0);
      pcf = new MutableByteSequence(new byte[64], 0, 0);
      pcq = new MutableByteSequence(new byte[64], 0, 0);
      pcv = new MutableByteSequence(new byte[64], 0, 0);
    }

    byte fieldsSame = -1;
    byte fieldsPrefixed = 0;
    int count = 0;
    Key newPrevKey = null;

    while (count < entriesLeft) {

      pdel = (fieldsSame & DELETED) == DELETED;

      fieldsSame = in.readByte();
      if ((fieldsSame & PREFIX_COMPRESSION_ENABLED) == PREFIX_COMPRESSION_ENABLED) {
        fieldsPrefixed = in.readByte();
      } else {
        fieldsPrefixed = 0;
      }

      boolean changed = false;

      if ((fieldsSame & ROW_SAME) != ROW_SAME) {

        MutableByteSequence tmp = prow;
        prow = row;
        row = tmp;

        if ((fieldsPrefixed & ROW_COMMON_PREFIX) == ROW_COMMON_PREFIX) {
          readPrefix(in, row, prow);
        } else {
          read(in, row);
        }

        // read a new row, so need to compare...
        rowCmp = row.compareTo(stopRow);
        changed = true;
      } // else the row is the same as the last, so no need to compare

      if ((fieldsSame & CF_SAME) != CF_SAME) {

        MutableByteSequence tmp = pcf;
        pcf = cf;
        cf = tmp;

        if ((fieldsPrefixed & CF_COMMON_PREFIX) == CF_COMMON_PREFIX) {
          readPrefix(in, cf, pcf);
        } else {
          read(in, cf);
        }

        cfCmp = cf.compareTo(stopCF);
        changed = true;
      }

      if ((fieldsSame & CQ_SAME) != CQ_SAME) {

        MutableByteSequence tmp = pcq;
        pcq = cq;
        cq = tmp;

        if ((fieldsPrefixed & CQ_COMMON_PREFIX) == CQ_COMMON_PREFIX) {
          readPrefix(in, cq, pcq);
        } else {
          read(in, cq);
        }

        cqCmp = cq.compareTo(stopCQ);
        changed = true;
      }

      if ((fieldsSame & CV_SAME) != CV_SAME) {

        MutableByteSequence tmp = pcv;
        pcv = cv;
        cv = tmp;

        if ((fieldsPrefixed & CV_COMMON_PREFIX) == CV_COMMON_PREFIX) {
          readPrefix(in, cv, pcv);
        } else {
          read(in, cv);
        }
      }

      if ((fieldsSame & TS_SAME) != TS_SAME) {
        pts = ts;

        if ((fieldsPrefixed & TS_DIFF) == TS_DIFF) {
          ts = WritableUtils.readVLong(in) + pts;
        } else {
          ts = WritableUtils.readVLong(in);
        }
      }

      readValue(in, value);

      count++;

      if (changed && rowCmp >= 0) {
        if (rowCmp > 0) {
          break;
        }

        if (cfCmp >= 0) {
          if (cfCmp > 0) {
            break;
          }

          if (cqCmp >= 0) {
            break;
          }
        }
      }

    }

    if (count > 1) {
      MutableByteSequence trow, tcf, tcq, tcv;
      long tts;

      // when the current keys field is same as the last, then
      // set the prev keys field the same as the current key
      trow = (fieldsSame & ROW_SAME) == ROW_SAME ? row : prow;
      tcf = (fieldsSame & CF_SAME) == CF_SAME ? cf : pcf;
      tcq = (fieldsSame & CQ_SAME) == CQ_SAME ? cq : pcq;
      tcv = (fieldsSame & CV_SAME) == CV_SAME ? cv : pcv;
      tts = (fieldsSame & TS_SAME) == TS_SAME ? ts : pts;

      newPrevKey = new Key(trow.getBackingArray(), trow.offset(), trow.length(),
          tcf.getBackingArray(), tcf.offset(), tcf.length(), tcq.getBackingArray(), tcq.offset(),
          tcq.length(), tcv.getBackingArray(), tcv.offset(), tcv.length(), tts);
      newPrevKey.setDeleted(pdel);
    } else if (count == 1) {
      if (currKey != null) {
        newPrevKey = currKey;
      } else {
        newPrevKey = prevKey;
      }
    } else {
      throw new IllegalStateException();
    }

    RelativeKey result = new RelativeKey();
    result.key = new Key(row.getBackingArray(), row.offset(), row.length(), cf.getBackingArray(),
        cf.offset(), cf.length(), cq.getBackingArray(), cq.offset(), cq.length(),
        cv.getBackingArray(), cv.offset(), cv.length(), ts);
    result.key.setDeleted((fieldsSame & DELETED) != 0);
    result.prevKey = result.key;

    return new SkippR(result, count, newPrevKey);
  }

  private static void read(DataInput in, MutableByteSequence mbseq) throws IOException {
    int len = WritableUtils.readVInt(in);
    read(in, mbseq, len);
  }

  private static void readValue(DataInput in, MutableByteSequence mbseq) throws IOException {
    int len = in.readInt();
    read(in, mbseq, len);
  }

  private static void read(DataInput in, MutableByteSequence mbseqDestination, int len)
      throws IOException {
    if (mbseqDestination.getBackingArray().length < len) {
      mbseqDestination.setArray(new byte[UnsynchronizedBuffer.nextArraySize(len)], 0, 0);
    }

    in.readFully(mbseqDestination.getBackingArray(), 0, len);
    mbseqDestination.setLength(len);
  }

  private static byte[] readPrefix(DataInput in, ByteSequence prefixSource) throws IOException {
    int prefixLen = WritableUtils.readVInt(in);
    int remainingLen = WritableUtils.readVInt(in);
    byte[] data = new byte[prefixLen + remainingLen];
    if (prefixSource.isBackedByArray()) {
      System.arraycopy(prefixSource.getBackingArray(), prefixSource.offset(), data, 0, prefixLen);
    } else {
      byte[] prefixArray = prefixSource.toArray();
      System.arraycopy(prefixArray, 0, data, 0, prefixLen);
    }
    // read remaining
    in.readFully(data, prefixLen, remainingLen);
    return data;
  }

  private static void readPrefix(DataInput in, MutableByteSequence dest, ByteSequence prefixSource)
      throws IOException {
    int prefixLen = WritableUtils.readVInt(in);
    int remainingLen = WritableUtils.readVInt(in);
    int len = prefixLen + remainingLen;
    if (dest.getBackingArray().length < len) {
      dest.setArray(new byte[UnsynchronizedBuffer.nextArraySize(len)], 0, 0);
    }
    if (prefixSource.isBackedByArray()) {
      System.arraycopy(prefixSource.getBackingArray(), prefixSource.offset(),
          dest.getBackingArray(), 0, prefixLen);
    } else {
      byte[] prefixArray = prefixSource.toArray();
      System.arraycopy(prefixArray, 0, dest.getBackingArray(), 0, prefixLen);
    }
    // read remaining
    in.readFully(dest.getBackingArray(), prefixLen, remainingLen);
    dest.setLength(len);
  }

  private static byte[] read(DataInput in) throws IOException {
    int len = WritableUtils.readVInt(in);
    byte[] data = new byte[len];
    in.readFully(data);
    return data;
  }

  public Key getKey() {
    return key;
  }

  private static void write(DataOutput out, ByteSequence bs) throws IOException {
    WritableUtils.writeVInt(out, bs.length());
    out.write(bs.getBackingArray(), bs.offset(), bs.length());
  }

  private static void writePrefix(DataOutput out, ByteSequence bs, int commonPrefixLength)
      throws IOException {
    WritableUtils.writeVInt(out, commonPrefixLength);
    WritableUtils.writeVInt(out, bs.length() - commonPrefixLength);
    out.write(bs.getBackingArray(), bs.offset() + commonPrefixLength,
        bs.length() - commonPrefixLength);
  }

  @Override
  public void write(DataOutput out) throws IOException {

    out.writeByte(fieldsSame);

    if ((fieldsSame & PREFIX_COMPRESSION_ENABLED) == PREFIX_COMPRESSION_ENABLED) {
      out.write(fieldsPrefixed);
    }

    if ((fieldsSame & ROW_SAME) == ROW_SAME) {
      // same, write nothing
    } else if ((fieldsPrefixed & ROW_COMMON_PREFIX) == ROW_COMMON_PREFIX) {
      // similar, write what's common
      writePrefix(out, key.getRowData(), rowCommonPrefixLen);
    } else {
      // write it all
      write(out, key.getRowData());
    }

    if ((fieldsSame & CF_SAME) == CF_SAME) {
      // same, write nothing
    } else if ((fieldsPrefixed & CF_COMMON_PREFIX) == CF_COMMON_PREFIX) {
      // similar, write what's common
      writePrefix(out, key.getColumnFamilyData(), cfCommonPrefixLen);
    } else {
      // write it all
      write(out, key.getColumnFamilyData());
    }

    if ((fieldsSame & CQ_SAME) == CQ_SAME) {
      // same, write nothing
    } else if ((fieldsPrefixed & CQ_COMMON_PREFIX) == CQ_COMMON_PREFIX) {
      // similar, write what's common
      writePrefix(out, key.getColumnQualifierData(), cqCommonPrefixLen);
    } else {
      // write it all
      write(out, key.getColumnQualifierData());
    }

    if ((fieldsSame & CV_SAME) == CV_SAME) {
      // same, write nothing
    } else if ((fieldsPrefixed & CV_COMMON_PREFIX) == CV_COMMON_PREFIX) {
      // similar, write what's common
      writePrefix(out, key.getColumnVisibilityData(), cvCommonPrefixLen);
    } else {
      // write it all
      write(out, key.getColumnVisibilityData());
    }

    if ((fieldsSame & TS_SAME) == TS_SAME) {
      // same, write nothing
    } else if ((fieldsPrefixed & TS_DIFF) == TS_DIFF) {
      // similar, write what's common
      WritableUtils.writeVLong(out, tsDiff);
    } else {
      // write it all
      WritableUtils.writeVLong(out, key.getTimestamp());
    }
  }

}

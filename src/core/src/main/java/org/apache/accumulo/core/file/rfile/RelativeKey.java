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
package org.apache.accumulo.core.file.rfile;

import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.Set;
import java.util.zip.GZIPOutputStream;

import org.apache.accumulo.core.data.ArrayByteSequence;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

public class RelativeKey implements WritableComparable<RelativeKey> {
  
  private Key key;
  
  private byte fieldsSame;
  
  private Key prevKey;
  
  private static final byte ROW_SAME = 0x01;
  private static final byte CF_SAME = 0x02;
  private static final byte CQ_SAME = 0x04;
  private static final byte CV_SAME = 0x08;
  private static final byte TS_SAME = 0x10;
  private static final byte DELETED = 0x20;
  
  private static HashMap<Text,Integer> colFams = new HashMap<Text,Integer>();
  
  private static long bytesWritten = 0;
  
  public static void printStats() throws Exception {
    System.out.println("colFams.size() : " + colFams.size());
    Set<Entry<Text,Integer>> es = colFams.entrySet();
    
    int sum = 0;
    
    for (Entry<Text,Integer> entry : es) {
      sum += entry.getKey().getLength();
    }
    
    System.out.println("Total Column name bytes : " + sum);
    
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(new GZIPOutputStream(baos));
    for (Entry<Text,Integer> entry : es) {
      entry.getKey().write(dos);
      dos.writeInt(entry.getValue());
    }
    
    dos.close();
    
    System.out.println("Compressed column map size : " + baos.toByteArray().length);
    System.out.printf("Bytes written : %,d\n", bytesWritten);
    
  }
  
  public RelativeKey() {
    
  }
  
  public RelativeKey(Key prevKey, Key key) {
    
    this.key = key;
    
    fieldsSame = 0;
    
    if (prevKey != null) {
      if (prevKey.getRowData().equals(key.getRowData()))
        fieldsSame |= ROW_SAME;
      
      if (prevKey.getColumnFamilyData().equals(key.getColumnFamilyData()))
        fieldsSame |= CF_SAME;
      
      if (prevKey.getColumnQualifierData().equals(key.getColumnQualifierData()))
        fieldsSame |= CQ_SAME;
      
      if (prevKey.getColumnVisibilityData().equals(key.getColumnVisibilityData()))
        fieldsSame |= CV_SAME;
      
      if (prevKey.getTimestamp() == key.getTimestamp())
        fieldsSame |= TS_SAME;
      
    }
    
    // stored deleted information in bit vector instead of its own byte
    if (key.isDeleted())
      fieldsSame |= DELETED;
  }
  
  @Override
  public void readFields(DataInput in) throws IOException {
    fieldsSame = in.readByte();
    
    byte[] row, cf, cq, cv;
    long ts;
    
    if ((fieldsSame & ROW_SAME) == 0) {
      row = read(in);
    } else {
      row = prevKey.getRowData().toArray();
    }
    
    if ((fieldsSame & CF_SAME) == 0) {
      cf = read(in);
    } else {
      cf = prevKey.getColumnFamilyData().toArray();
    }
    
    if ((fieldsSame & CQ_SAME) == 0) {
      cq = read(in);
    } else {
      cq = prevKey.getColumnQualifierData().toArray();
    }
    
    if ((fieldsSame & CV_SAME) == 0) {
      cv = read(in);
    } else {
      cv = prevKey.getColumnVisibilityData().toArray();
    }
    
    if ((fieldsSame & TS_SAME) == 0) {
      ts = WritableUtils.readVLong(in);
    } else {
      ts = prevKey.getTimestamp();
    }
    
    this.key = new Key(row, cf, cq, cv, ts, (fieldsSame & DELETED) != 0, false);
    
    this.prevKey = this.key;
  }
  
  static class MByteSequence extends ArrayByteSequence {
    MByteSequence(byte[] data, int offset, int length) {
      super(data, offset, length);
    }
    
    MByteSequence(ByteSequence bs) {
      super(new byte[Math.max(64, bs.length())]);
      System.arraycopy(bs.getBackingArray(), bs.offset(), data, 0, bs.length());
      this.length = bs.length();
      this.offset = 0;
    }
    
    void setArray(byte[] data) {
      this.data = data;
      this.offset = 0;
      this.length = 0;
    }
    
    void setLength(int len) {
      this.length = len;
    }
  }
  
  int fastSkip(DataInput in, Key seekKey, MByteSequence value, Key pkey, Key currKey) throws IOException {
    // this method assumes that fast skip is being called on a compressed block where the last key
    // in the compressed block is >= seekKey... therefore this method should go passed the end of the
    // compressed block... if it does, there is probably an error in the callers logic
    
    // this method mostly avoids object allocation and only does compares when the row changes
    
    MByteSequence row, cf, cq, cv;
    MByteSequence prow, pcf, pcq, pcv;
    
    ByteSequence stopRow = seekKey.getRowData();
    ByteSequence stopCF = seekKey.getColumnFamilyData();
    ByteSequence stopCQ = seekKey.getColumnQualifierData();
    
    long ts = -1;
    long pts = -1;
    boolean pdel = false;
    
    int rowCmp = -1, cfCmp = -1, cqCmp = -1;
    
    if (currKey != null) {
      
      prow = new MByteSequence(pkey.getRowData());
      pcf = new MByteSequence(pkey.getColumnFamilyData());
      pcq = new MByteSequence(pkey.getColumnQualifierData());
      pcv = new MByteSequence(pkey.getColumnVisibilityData());
      pts = pkey.getTimestamp();
      
      row = new MByteSequence(currKey.getRowData());
      cf = new MByteSequence(currKey.getColumnFamilyData());
      cq = new MByteSequence(currKey.getColumnQualifierData());
      cv = new MByteSequence(currKey.getColumnVisibilityData());
      ts = currKey.getTimestamp();
      
      rowCmp = row.compareTo(stopRow);
      cfCmp = cf.compareTo(stopCF);
      cqCmp = cq.compareTo(stopCQ);
      
      if (rowCmp >= 0) {
        if (rowCmp > 0)
          return 0;
        
        if (cfCmp >= 0) {
          if (cfCmp > 0)
            return 0;
          
          if (cqCmp >= 0)
            return 0;
        }
      }
      
    } else {
      row = new MByteSequence(new byte[64], 0, 0);
      cf = new MByteSequence(new byte[64], 0, 0);
      cq = new MByteSequence(new byte[64], 0, 0);
      cv = new MByteSequence(new byte[64], 0, 0);
      
      prow = new MByteSequence(new byte[64], 0, 0);
      pcf = new MByteSequence(new byte[64], 0, 0);
      pcq = new MByteSequence(new byte[64], 0, 0);
      pcv = new MByteSequence(new byte[64], 0, 0);
    }
    
    byte fieldsSame = -1;
    int count = 0;
    
    while (true) {
      
      pdel = (fieldsSame & DELETED) != 0;
      
      fieldsSame = in.readByte();
      
      boolean changed = false;
      
      if ((fieldsSame & ROW_SAME) == 0) {
        
        MByteSequence tmp = prow;
        prow = row;
        row = tmp;
        
        read(in, row);
        
        // read a new row, so need to compare...
        rowCmp = row.compareTo(stopRow);
        changed = true;
      }// else the row is the same as the last, so no need to compare
      
      if ((fieldsSame & CF_SAME) == 0) {
        
        MByteSequence tmp = pcf;
        pcf = cf;
        cf = tmp;
        
        read(in, cf);
        
        cfCmp = cf.compareTo(stopCF);
        changed = true;
      }
      
      if ((fieldsSame & CQ_SAME) == 0) {
        
        MByteSequence tmp = pcq;
        pcq = cq;
        cq = tmp;
        
        read(in, cq);
        
        cqCmp = cq.compareTo(stopCQ);
        changed = true;
      }
      
      if ((fieldsSame & CV_SAME) == 0) {
        
        MByteSequence tmp = pcv;
        pcv = cv;
        cv = tmp;
        
        read(in, cv);
      }
      
      if ((fieldsSame & TS_SAME) == 0) {
        pts = ts;
        ts = WritableUtils.readVLong(in);
      }
      
      readValue(in, value);
      
      count++;
      
      if (changed && rowCmp >= 0) {
        if (rowCmp > 0)
          break;
        
        if (cfCmp >= 0) {
          if (cfCmp > 0)
            break;
          
          if (cqCmp >= 0)
            break;
        }
      }
      
    }
    
    if (count > 1) {
      MByteSequence trow, tcf, tcq, tcv;
      long tts;
      
      // when the current keys field is same as the last, then
      // set the prev keys field the same as the current key
      trow = (fieldsSame & ROW_SAME) == 0 ? prow : row;
      tcf = (fieldsSame & CF_SAME) == 0 ? pcf : cf;
      tcq = (fieldsSame & CQ_SAME) == 0 ? pcq : cq;
      tcv = (fieldsSame & CV_SAME) == 0 ? pcv : cv;
      tts = (fieldsSame & TS_SAME) == 0 ? pts : ts;
      
      Key tmp = new Key(trow.getBackingArray(), trow.offset(), trow.length(), tcf.getBackingArray(), tcf.offset(), tcf.length(), tcq.getBackingArray(),
          tcq.offset(), tcq.length(), tcv.getBackingArray(), tcv.offset(), tcv.length(), tts);
      tmp.setDeleted(pdel);
      pkey.set(tmp);
    }
    
    this.key = new Key(row.getBackingArray(), row.offset(), row.length(), cf.getBackingArray(), cf.offset(), cf.length(), cq.getBackingArray(), cq.offset(),
        cq.length(), cv.getBackingArray(), cv.offset(), cv.length(), ts);
    this.key.setDeleted((fieldsSame & DELETED) != 0);
    
    this.prevKey = this.key;
    
    return count;
  }
  
  private void read(DataInput in, MByteSequence mbseq) throws IOException {
    int len = WritableUtils.readVInt(in);
    read(in, mbseq, len);
  }
  
  private void readValue(DataInput in, MByteSequence mbseq) throws IOException {
    int len = in.readInt();
    read(in, mbseq, len);
  }
  
  private void read(DataInput in, MByteSequence mbseq, int len) throws IOException {
    if (mbseq.getBackingArray().length < len) {
      int newLen = mbseq.getBackingArray().length;
      
      while (newLen < len) {
        newLen = newLen * 2;
      }
      
      mbseq.setArray(new byte[newLen]);
    }
    
    in.readFully(mbseq.getBackingArray(), 0, len);
    mbseq.setLength(len);
  }
  
  private byte[] read(DataInput in) throws IOException {
    int len = WritableUtils.readVInt(in);
    byte[] data = new byte[len];
    in.readFully(data);
    return data;
  }
  
  public Key getKey() {
    return key;
  }
  
  private void write(DataOutput out, ByteSequence bs) throws IOException {
    WritableUtils.writeVInt(out, bs.length());
    out.write(bs.getBackingArray(), bs.offset(), bs.length());
  }
  
  @Override
  public void write(DataOutput out) throws IOException {
    
    out.writeByte(fieldsSame);
    
    // System.out.printf("wrote fs %x\n", fieldsSame);
    
    bytesWritten += 1;
    
    if ((fieldsSame & ROW_SAME) == 0) {
      write(out, key.getRowData());
    }
    
    if ((fieldsSame & CF_SAME) == 0) {
      write(out, key.getColumnFamilyData());
    }
    
    if ((fieldsSame & CQ_SAME) == 0) {
      
      write(out, key.getColumnQualifierData());
      
      /*
       * Integer id = colFams.get(key.getColumnQualifier()); if(id == null){ id = nextId++; colFams.put(key.getColumnQualifier(), id); }
       * 
       * WritableUtils.writeVInt(out, id); bytesWritten += 1;
       */
      
    }
    
    if ((fieldsSame & CV_SAME) == 0) {
      write(out, key.getColumnVisibilityData());
    }
    
    if ((fieldsSame & TS_SAME) == 0) {
      WritableUtils.writeVLong(out, key.getTimestamp());
    }
  }
  
  @Override
  public int compareTo(RelativeKey o) {
    throw new UnsupportedOperationException();
  }
  
}

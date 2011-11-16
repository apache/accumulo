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
package org.apache.accumulo.core.iterators;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Map;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.WritableUtils;

/*
 * A TypedValueCombiner that translates each Value to a Long before reducing, then encodes the reduced Long back to a Value.
 * 
 * Subclasses must implement a typedReduce method:
 *   public Long typedReduce(Key key, Iterator<Long> iter);
 * 
 * This typedReduce method will be passed the most recent Key and an iterator over the Values (translated to Longs) for all non-deleted versions of that Key.
 * 
 * A required option for this Combiner is "type" which indicates which type of Encoder to use to encode and decode Longs into Values.  Supported types are
 * VARNUM, LONG, and STRING which indicate the VarNumEncoder, LongEncoder, and StringEncoder respectively.
 */
public abstract class LongCombiner extends TypedValueCombiner<Long> {
  public static final String TYPE = "type";
  
  public static enum Type {
    VARNUM, LONG, STRING
  }
  
  @Override
  public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options, IteratorEnvironment env) throws IOException {
    super.init(source, options, env);
    if (options.get(TYPE) == null)
      throw new IllegalArgumentException("no type specified");
    switch (Type.valueOf(options.get(TYPE))) {
      case VARNUM:
        encoder = new VarNumEncoder();
        return;
      case LONG:
        encoder = new LongEncoder();
        return;
      case STRING:
        encoder = new StringEncoder();
        return;
      default:
        throw new IllegalArgumentException();
    }
  }
  
  @Override
  public IteratorOptions describeOptions() {
    IteratorOptions io = super.describeOptions();
    io.setName("typedcombiner");
    io.setDescription("TypedValueCombiner can interpret Values as a variety of number encodings (VLong, Long, or String) before combining");
    io.addNamedOption(TYPE, "<VARNUM|LONG|STRING>");
    return io;
  }
  
  @Override
  public boolean validateOptions(Map<String,String> options) {
    if (options.get(TYPE) == null)
      return false;
    try {
      Type.valueOf(options.get(TYPE));
    } catch (Exception e) {
      return false;
    }
    return true;
  }
  
  /*
   * An Encoder that uses a variable-length encoding for Longs. It uses WritableUtils.writeVLong and WritableUtils.readVLong for encoding and decoding.
   */
  public static class VarNumEncoder implements Encoder<Long> {
    @Override
    public byte[] encode(Long v) {
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      DataOutputStream dos = new DataOutputStream(baos);
      
      try {
        WritableUtils.writeVLong(dos, v);
      } catch (IOException e) {
        throw new NumberFormatException(e.getMessage());
      }
      
      return baos.toByteArray();
    }
    
    @Override
    public Long decode(byte[] b) {
      DataInputStream dis = new DataInputStream(new ByteArrayInputStream(b));
      try {
        return WritableUtils.readVLong(dis);
      } catch (IOException e) {
        throw new NumberFormatException(e.getMessage());
      }
    }
  }
  
  /*
   * An Encoder that uses an 8-byte encoding for Longs.
   */
  public static class LongEncoder implements Encoder<Long> {
    @Override
    public byte[] encode(Long l) {
      byte[] b = new byte[8];
      b[0] = (byte) (l >>> 56);
      b[1] = (byte) (l >>> 48);
      b[2] = (byte) (l >>> 40);
      b[3] = (byte) (l >>> 32);
      b[4] = (byte) (l >>> 24);
      b[5] = (byte) (l >>> 16);
      b[6] = (byte) (l >>> 8);
      b[7] = (byte) (l >>> 0);
      return b;
    }
    
    @Override
    public Long decode(byte[] b) {
      return decode(b, 0);
    }
    
    public static long decode(byte[] b, int offset) {
      if (b.length < offset + 8)
        throw new NumberFormatException("trying to convert to long, but byte array isn't long enough, wanted " + (offset + 8) + " found " + b.length);
      return (((long) b[offset + 0] << 56) + ((long) (b[offset + 1] & 255) << 48) + ((long) (b[offset + 2] & 255) << 40) + ((long) (b[offset + 3] & 255) << 32)
          + ((long) (b[offset + 4] & 255) << 24) + ((b[offset + 5] & 255) << 16) + ((b[offset + 6] & 255) << 8) + ((b[offset + 7] & 255) << 0));
    }
  }
  
  /*
   * An Encoder that uses a String representation of Longs. It uses Long.toString and Long.parseLong for encoding and decoding.
   */
  public static class StringEncoder implements Encoder<Long> {
    @Override
    public byte[] encode(Long v) {
      return Long.toString(v).getBytes();
    }
    
    @Override
    public Long decode(byte[] b) {
      return Long.parseLong(new String(b));
    }
  }
  
  public static long safeAdd(long a, long b) {
    long aSign = Long.signum(a);
    long bSign = Long.signum(b);
    if ((aSign != 0) && (bSign != 0) && (aSign == bSign)) {
      if (aSign > 0) {
        if (Long.MAX_VALUE - a < b)
          return Long.MAX_VALUE;
      } else {
        if (Long.MIN_VALUE - a > b)
          return Long.MIN_VALUE;
      }
    }
    return a + b;
  }
}

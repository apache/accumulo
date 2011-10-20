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
package org.apache.accumulo.core.iterators.user;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.LongCombiner;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.TypedValueCombiner;
import org.apache.accumulo.core.iterators.LongCombiner.Type;
import org.apache.hadoop.io.WritableUtils;

public class SummingArrayCombiner extends TypedValueCombiner<List<Long>> {
  @Override
  public List<Long> typedReduce(Key key, Iterator<List<Long>> iter) {
    List<Long> sum = new ArrayList<Long>();
    while (iter.hasNext()) {
      sum = arrayAdd(sum, iter.next());
    }
    return sum;
  }
  
  public static List<Long> arrayAdd(List<Long> la, List<Long> lb) {
    if (la.size() > lb.size()) {
      for (int i = 0; i < lb.size(); i++) {
        la.set(i, LongCombiner.safeAdd(la.get(i), lb.get(i)));
      }
      return la;
    } else {
      for (int i = 0; i < la.size(); i++) {
        lb.set(i, LongCombiner.safeAdd(lb.get(i), la.get(i)));
      }
      return lb;
    }
  }
  
  @Override
  public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options, IteratorEnvironment env) throws IOException {
    super.init(source, options, env);
    if (options.get(LongCombiner.TYPE) == null) throw new IOException("no type specified");
    switch (Type.valueOf(options.get(LongCombiner.TYPE))) {
      case VARNUM:
        encoder = new VarNumArrayEncoder();
        return;
      case LONG:
        encoder = new LongArrayEncoder();
        return;
      case STRING:
        encoder = new StringArrayEncoder();
        return;
      default:
        throw new UnsupportedOperationException();
    }
  }
  
  @Override
  public IteratorOptions describeOptions() {
    IteratorOptions io = super.describeOptions();
    io.setName("typedcombiner");
    io.setDescription("TypedValueCombiner can interpret Values as a variety of number encodings (VLong, Long, or String) before combining");
    io.addNamedOption(LongCombiner.TYPE, "<VARNUM|LONG|STRING>");
    return io;
  }
  
  @Override
  public boolean validateOptions(Map<String,String> options) {
    if (options.get(LongCombiner.TYPE) == null) return false;
    try {
      Type.valueOf(options.get(LongCombiner.TYPE));
    } catch (Exception e) {
      return false;
    }
    return true;
  }
  
  public abstract static class DOSArrayEncoder<V> implements Encoder<List<V>> {
    public abstract void write(DataOutputStream dos, V v) throws IOException;
    
    public abstract V read(DataInputStream dis) throws IOException;
    
    @Override
    public byte[] encode(List<V> vl) {
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      DataOutputStream dos = new DataOutputStream(baos);
      try {
        WritableUtils.writeVInt(dos, vl.size());
        for (V v : vl) {
          write(dos, v);
        }
      } catch (IOException e) {
        throw new NumberFormatException(e.getMessage());
      }
      return baos.toByteArray();
    }
    
    @Override
    public List<V> decode(byte[] b) {
      DataInputStream dis = new DataInputStream(new ByteArrayInputStream(b));
      try {
        int len = WritableUtils.readVInt(dis);
        List<V> vl = new ArrayList<V>(len);
        for (int i = 0; i < len; i++) {
          vl.add(read(dis));
        }
        return vl;
      } catch (IOException e) {
        throw new NumberFormatException(e.getMessage());
      }
    }
  }
  
  public static class VarNumArrayEncoder extends DOSArrayEncoder<Long> {
    @Override
    public void write(DataOutputStream dos, Long v) throws IOException {
      WritableUtils.writeVLong(dos, v);
    }
    
    @Override
    public Long read(DataInputStream dis) throws IOException {
      return WritableUtils.readVLong(dis);
    }
  }
  
  public static class LongArrayEncoder extends DOSArrayEncoder<Long> {
    @Override
    public void write(DataOutputStream dos, Long v) throws IOException {
      dos.writeLong(v);
    }
    
    @Override
    public Long read(DataInputStream dis) throws IOException {
      return dis.readLong();
    }
  }
  
  public static class StringArrayEncoder implements Encoder<List<Long>> {
    @Override
    public byte[] encode(List<Long> la) {
      if (la.size() == 0) return new byte[] {};
      StringBuilder sb = new StringBuilder(Long.toString(la.get(0)));
      for (int i = 1; i < la.size(); i++) {
        sb.append(",");
        sb.append(Long.toString(la.get(i)));
      }
      return sb.toString().getBytes();
    }
    
    @Override
    public List<Long> decode(byte[] b) {
      String[] longstrs = new String(b).split(",");
      List<Long> la = new ArrayList<Long>(longstrs.length);
      for (String s : longstrs) {
        if (s.length() == 0) la.add(0l);
        else la.add(Long.parseLong(s));
      }
      return la;
    }
  }
}

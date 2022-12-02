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
package org.apache.accumulo.core.iterators.user;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.lexicoder.AbstractEncoder;
import org.apache.accumulo.core.client.lexicoder.AbstractLexicoder;
import org.apache.accumulo.core.client.lexicoder.Encoder;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.LongCombiner;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.TypedValueCombiner;
import org.apache.accumulo.core.iterators.ValueFormatException;
import org.apache.hadoop.io.WritableUtils;

/**
 * A Combiner that interprets Values as arrays of Longs and returns an array of element-wise sums.
 */
public class SummingArrayCombiner extends TypedValueCombiner<List<Long>> {
  public static final Encoder<List<Long>> FIXED_LONG_ARRAY_ENCODER = new FixedLongArrayEncoder();
  public static final Encoder<List<Long>> VAR_LONG_ARRAY_ENCODER = new VarLongArrayEncoder();
  public static final Encoder<List<Long>> STRING_ARRAY_ENCODER = new StringArrayEncoder();

  private static final String TYPE = "type";
  private static final String CLASS_PREFIX = "class:";

  public enum Type {
    /**
     * indicates a variable-length encoding of a list of Longs using
     * {@link SummingArrayCombiner.VarLongArrayEncoder}
     */
    VARLEN,
    /**
     * indicates a fixed-length (8 bytes for each Long) encoding of a list of Longs using
     * {@link SummingArrayCombiner.FixedLongArrayEncoder}
     */
    FIXEDLEN,
    /**
     * indicates a string (comma-separated) representation of a list of Longs using
     * {@link SummingArrayCombiner.StringArrayEncoder}
     */
    STRING
  }

  @Override
  public List<Long> typedReduce(Key key, Iterator<List<Long>> iter) {
    List<Long> sum = new ArrayList<>();
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
  public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options,
      IteratorEnvironment env) throws IOException {
    super.init(source, options, env);
    setEncoder(options);
  }

  private void setEncoder(Map<String,String> options) {
    String type = options.get(TYPE);
    if (type == null) {
      throw new IllegalArgumentException("no type specified");
    }
    if (type.startsWith(CLASS_PREFIX)) {
      setEncoder(type.substring(CLASS_PREFIX.length()));
      testEncoder(Arrays.asList(0L, 1L));
    } else {
      switch (Type.valueOf(options.get(TYPE))) {
        case VARLEN:
          setEncoder(VAR_LONG_ARRAY_ENCODER);
          return;
        case FIXEDLEN:
          setEncoder(FIXED_LONG_ARRAY_ENCODER);
          return;
        case STRING:
          setEncoder(STRING_ARRAY_ENCODER);
          return;
        default:
          throw new IllegalArgumentException();
      }
    }
  }

  @Override
  public IteratorOptions describeOptions() {
    IteratorOptions io = super.describeOptions();
    io.setName("sumarray");
    io.setDescription("SummingArrayCombiner can interpret Values as arrays of"
        + " Longs using a variety of encodings (arrays of variable length longs or"
        + " fixed length longs, or comma-separated strings) before summing element-wise.");
    io.addNamedOption(TYPE, "<VARLEN|FIXEDLEN|STRING|fullClassName>");
    return io;
  }

  @Override
  public boolean validateOptions(Map<String,String> options) {
    if (!super.validateOptions(options)) {
      return false;
    }
    try {
      setEncoder(options);
    } catch (Exception e) {
      throw new IllegalArgumentException("bad encoder option", e);
    }
    return true;
  }

  public abstract static class DOSArrayEncoder<V> extends AbstractLexicoder<List<V>> {
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
      // This concrete implementation is provided for binary compatibility, since the corresponding
      // superclass method has type-erased return type Object. See ACCUMULO-3789 and #1285.
      return super.decode(b);
    }

    @Override
    protected List<V> decodeUnchecked(byte[] b, int offset, int origLen) {
      DataInputStream dis = new DataInputStream(new ByteArrayInputStream(b, offset, origLen));
      try {
        int len = WritableUtils.readVInt(dis);
        List<V> vl = new ArrayList<>(len);
        for (int i = 0; i < len; i++) {
          vl.add(read(dis));
        }
        return vl;
      } catch (IOException e) {
        throw new ValueFormatException(e);
      }
    }
  }

  public static class VarLongArrayEncoder extends DOSArrayEncoder<Long> {
    @Override
    public void write(DataOutputStream dos, Long v) throws IOException {
      WritableUtils.writeVLong(dos, v);
    }

    @Override
    public Long read(DataInputStream dis) throws IOException {
      return WritableUtils.readVLong(dis);
    }
  }

  public static class FixedLongArrayEncoder extends DOSArrayEncoder<Long> {
    @Override
    public void write(DataOutputStream dos, Long v) throws IOException {
      dos.writeLong(v);
    }

    @Override
    public Long read(DataInputStream dis) throws IOException {
      return dis.readLong();
    }
  }

  public static class StringArrayEncoder extends AbstractEncoder<List<Long>> {
    @Override
    public byte[] encode(List<Long> la) {
      if (la.isEmpty()) {
        return new byte[] {};
      }
      StringBuilder sb = new StringBuilder(Long.toString(la.get(0)));
      for (int i = 1; i < la.size(); i++) {
        sb.append(",");
        sb.append(la.get(i));
      }
      return sb.toString().getBytes(UTF_8);
    }

    @Override
    public List<Long> decode(byte[] b) {
      // This concrete implementation is provided for binary compatibility, since the corresponding
      // superclass method has type-erased return type Object. See ACCUMULO-3789 and #1285.
      return super.decode(b);
    }

    @Override
    protected List<Long> decodeUnchecked(byte[] b, int offset, int len) {
      String[] longstrs = new String(b, offset, len, UTF_8).split(",");
      List<Long> la = new ArrayList<>(longstrs.length);
      for (String s : longstrs) {
        if (s.isEmpty()) {
          la.add(0L);
        } else {
          try {
            la.add(Long.parseLong(s));
          } catch (NumberFormatException nfe) {
            throw new ValueFormatException(nfe);
          }
        }
      }
      return la;
    }
  }

  /**
   * A convenience method for setting the encoding type.
   *
   * @param is IteratorSetting object to configure.
   * @param type SummingArrayCombiner.Type specifying the encoding type.
   */
  public static void setEncodingType(IteratorSetting is, Type type) {
    is.addOption(TYPE, type.toString());
  }

  /**
   * A convenience method for setting the encoding type.
   *
   * @param is IteratorSetting object to configure.
   * @param encoderClass {@code Class<? extends Encoder<List<Long>>>} specifying the encoding type.
   */
  public static void setEncodingType(IteratorSetting is,
      Class<? extends Encoder<List<Long>>> encoderClass) {
    is.addOption(TYPE, CLASS_PREFIX + encoderClass.getName());
  }

  /**
   * A convenience method for setting the encoding type.
   *
   * @param is IteratorSetting object to configure.
   * @param encoderClassName name of a class specifying the encoding type.
   */
  public static void setEncodingType(IteratorSetting is, String encoderClassName) {
    is.addOption(TYPE, CLASS_PREFIX + encoderClassName);
  }
}

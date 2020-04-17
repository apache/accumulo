/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.accumulo.core.clientImpl;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.admin.CompactionConfig;
import org.apache.accumulo.core.client.admin.CompactionConfigurerConfig;
import org.apache.accumulo.core.client.admin.CompactionSelectorConfig;
import org.apache.accumulo.core.client.admin.CompactionStrategyConfig;
import org.apache.hadoop.io.Text;

import com.google.common.base.Preconditions;

public class UserCompactionUtils {

  private static final int MAGIC = 0x02040810;
  private static final int SELECTOR_MAGIC = 0xae9270bf;
  private static final int CONFIGURER_MAGIC = 0xf93e570a;

  public static CompactionConfigurerConfig DEFAULT_CCC = new CompactionConfigurerConfig("");
  public static CompactionSelectorConfig DEFAULT_CSC = new CompactionSelectorConfig("");

  public static void encode(DataOutput dout, Map<String,String> options) {
    try {
      dout.writeInt(options.size());

      for (Entry<String,String> entry : options.entrySet()) {
        dout.writeUTF(entry.getKey());
        dout.writeUTF(entry.getValue());
      }
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  public static void encode(DataOutput dout, int magic, int version, String className,
      Map<String,String> options) {

    try {
      dout.writeInt(magic);
      dout.writeByte(version);

      dout.writeUTF(className);
      encode(dout, options);

    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  public static interface Encoder<T> {
    public void encode(DataOutput dout, T p);
  }

  public static <T> byte[] encode(T csc, Encoder<T> encoder) {

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(baos);

    try {
      encoder.encode(dos, csc);
      dos.close();
      return baos.toByteArray();
    } catch (IOException ioe) {
      throw new UncheckedIOException(ioe);
    }
  }

  public static class PluginConfigData {
    String className;
    Map<String,String> opts;
  }

  public static Map<String,String> decodeMap(DataInput din) {
    try {
      int numEntries = din.readInt();

      var opts = new HashMap<String,String>();

      for (int i = 0; i < numEntries; i++) {
        String k = din.readUTF();
        String v = din.readUTF();
        opts.put(k, v);
      }

      return opts;
    } catch (IOException ioe) {
      throw new UncheckedIOException(ioe);
    }
  }

  public static PluginConfigData decode(DataInput din, int magic, int version) {

    try {
      if (din.readInt() != magic) {
        throw new IllegalArgumentException("Unexpected MAGIC ");
      }

      if (din.readByte() != version) {
        throw new IllegalArgumentException("Unexpected version");
      }

      var pcd = new PluginConfigData();

      pcd.className = din.readUTF();
      int numEntries = din.readInt();

      pcd.opts = new HashMap<>();

      for (int i = 0; i < numEntries; i++) {
        String k = din.readUTF();
        String v = din.readUTF();
        pcd.opts.put(k, v);
      }

      return pcd;
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  public static interface Decoder<T> {
    T decode(DataInput di);
  }

  public static <T> T decode(byte[] encodedCsc, Decoder<T> decoder) {
    ByteArrayInputStream bais = new ByteArrayInputStream(encodedCsc);
    DataInputStream dis = new DataInputStream(bais);
    return decoder.decode(dis);
  }

  public static void encode(DataOutput dout, CompactionSelectorConfig csc) {
    encode(dout, SELECTOR_MAGIC, 1, csc.getClassName(), csc.getOptions());
  }

  public static byte[] encode(CompactionSelectorConfig csc) {
    return encode(csc, UserCompactionUtils::encode);
  }

  public static CompactionSelectorConfig decodeSelector(DataInput di) {
    var pcd = decode(di, SELECTOR_MAGIC, 1);
    return new CompactionSelectorConfig(pcd.className).setOptions(pcd.opts);
  }

  public static CompactionSelectorConfig decodeSelector(byte[] bytes) {
    return decode(bytes, UserCompactionUtils::decodeSelector);
  }

  public static void encode(DataOutput dout, CompactionConfigurerConfig ccc) {
    encode(dout, CONFIGURER_MAGIC, 1, ccc.getClassName(), ccc.getOptions());
  }

  public static byte[] encode(CompactionConfigurerConfig ccc) {
    return encode(ccc, UserCompactionUtils::encode);
  }

  public static CompactionConfigurerConfig decodeConfigurer(DataInput di) {
    var pcd = decode(di, CONFIGURER_MAGIC, 1);
    return new CompactionConfigurerConfig(pcd.className).setOptions(pcd.opts);
  }

  public static CompactionConfigurerConfig decodeConfigurer(byte[] bytes) {
    return decode(bytes, UserCompactionUtils::decodeConfigurer);
  }

  public static byte[] encode(Map<String,String> options) {
    return encode(options, UserCompactionUtils::encode);
  }

  public static Map<String,String> decodeMap(byte[] bytes) {
    return decode(bytes, UserCompactionUtils::decodeMap);
  }

  public static void encode(DataOutput dout, CompactionConfig cc) {
    try {
      dout.writeInt(MAGIC);

      dout.writeBoolean(cc.getStartRow() != null);
      if (cc.getStartRow() != null) {
        cc.getStartRow().write(dout);
      }

      dout.writeBoolean(cc.getEndRow() != null);
      if (cc.getEndRow() != null) {
        cc.getEndRow().write(dout);
      }

      dout.writeInt(cc.getIterators().size());
      for (IteratorSetting is : cc.getIterators()) {
        is.write(dout);
      }

      CompactionStrategyConfigUtil.encode(dout, cc.getCompactionStrategy());

      encode(dout, cc.getConfigurer());
      encode(dout, cc.getSelector());
      encode(dout, cc.getExecutionHints());

    } catch (IOException ioe) {
      throw new UncheckedIOException(ioe);
    }

  }

  public static byte[] encode(CompactionConfig cc) {
    return encode(cc, UserCompactionUtils::encode);
  }

  public static CompactionConfig decodeCompactionConfig(DataInput din) {
    try {
      Preconditions.checkArgument(MAGIC == din.readInt());

      CompactionConfig cc = new CompactionConfig();

      if (din.readBoolean()) {
        Text startRow = new Text();
        startRow.readFields(din);
        cc.setStartRow(startRow);
      }

      if (din.readBoolean()) {
        Text endRow = new Text();
        endRow.readFields(din);
        cc.setStartRow(endRow);
      }

      int num = din.readInt();
      var iterators = new ArrayList<IteratorSetting>(num);

      for (int i = 0; i < num; i++) {
        iterators.add(new IteratorSetting(din));
      }

      cc.setIterators(iterators);

      var compactionStrategy = CompactionStrategyConfigUtil.decode(din);
      if (!isDefault(compactionStrategy)) {
        cc.setCompactionStrategy(compactionStrategy);
      }

      var configurer = decodeConfigurer(din);
      if (!isDefault(configurer)) {
        cc.setConfigurer(configurer);
      }

      var selector = decodeSelector(din);
      if (!isDefault(selector)) {
        cc.setSelector(selector);
      }

      var hints = decodeMap(din);
      cc.setExecutionHints(hints);

      return cc;
    } catch (IOException ioe) {
      throw new UncheckedIOException(ioe);
    }
  }

  public static boolean isDefault(CompactionConfigurerConfig configurer) {
    return configurer.equals(DEFAULT_CCC);
  }

  public static CompactionConfig decodeCompactionConfig(byte[] bytes) {
    return decode(bytes, UserCompactionUtils::decodeCompactionConfig);
  }

  public static boolean isDefault(CompactionStrategyConfig compactionStrategy) {
    return compactionStrategy.equals(CompactionStrategyConfigUtil.DEFAULT_STRATEGY);

  }

  public static boolean isDefault(CompactionSelectorConfig selector) {
    return selector.equals(DEFAULT_CSC);
  }
}

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

package org.apache.accumulo.core.client.impl;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.accumulo.core.client.admin.CompactionStrategyConfig;

public class CompactionStrategyConfigUtil {

  public static final CompactionStrategyConfig DEFAULT_STRATEGY = new CompactionStrategyConfig(
      "org.apache.accumulo.tserver.compaction.EverythingCompactionStrategy") {
    @Override
    public CompactionStrategyConfig setOptions(Map<String,String> opts) {
      throw new UnsupportedOperationException();
    }
  };

  private static final int MAGIC = 0xcc5e6024;

  public static void encode(DataOutput dout, CompactionStrategyConfig csc) throws IOException {

    dout.writeInt(MAGIC);
    dout.writeByte(1);

    dout.writeUTF(csc.getClassName());
    dout.writeInt(csc.getOptions().size());

    for (Entry<String,String> entry : csc.getOptions().entrySet()) {
      dout.writeUTF(entry.getKey());
      dout.writeUTF(entry.getValue());
    }

  }

  public static byte[] encode(CompactionStrategyConfig csc) {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(baos);

    try {
      encode(dos, csc);
      dos.close();

      return baos.toByteArray();
    } catch (IOException ioe) {
      throw new RuntimeException(ioe);
    }
  }

  public static CompactionStrategyConfig decode(DataInput din) throws IOException {
    if (din.readInt() != MAGIC) {
      throw new IllegalArgumentException("Unexpected MAGIC ");
    }

    if (din.readByte() != 1) {
      throw new IllegalArgumentException("Unexpected version");
    }

    String classname = din.readUTF();
    int numEntries = din.readInt();

    HashMap<String,String> opts = new HashMap<>();

    for (int i = 0; i < numEntries; i++) {
      String k = din.readUTF();
      String v = din.readUTF();
      opts.put(k, v);
    }

    return new CompactionStrategyConfig(classname).setOptions(opts);
  }

  public static CompactionStrategyConfig decode(byte[] encodedCsc) {
    ByteArrayInputStream bais = new ByteArrayInputStream(encodedCsc);
    DataInputStream dis = new DataInputStream(bais);

    try {
      return decode(dis);
    } catch (IOException ioe) {
      throw new RuntimeException(ioe);
    }
  }
}

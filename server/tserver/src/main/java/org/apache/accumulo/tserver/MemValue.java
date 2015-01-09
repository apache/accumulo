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
package org.apache.accumulo.tserver;

import java.io.DataOutput;
import java.io.IOException;

import org.apache.accumulo.core.data.Value;

/**
 *
 */
public class MemValue extends Value {
  int kvCount;
  boolean merged = false;

  /**
   * @param value
   *          Value
   * @param kv
   *          kv count
   */
  public MemValue(byte[] value, int kv) {
    super(value);
    this.kvCount = kv;
  }

  public MemValue() {
    super();
    this.kvCount = Integer.MAX_VALUE;
  }

  public MemValue(Value value, int kv) {
    super(value);
    this.kvCount = kv;
  }

  // Override
  @Override
  public void write(final DataOutput out) throws IOException {
    if (!merged) {
      byte[] combinedBytes = new byte[getSize() + 4];
      System.arraycopy(value, 0, combinedBytes, 4, getSize());
      combinedBytes[0] = (byte) (kvCount >>> 24);
      combinedBytes[1] = (byte) (kvCount >>> 16);
      combinedBytes[2] = (byte) (kvCount >>> 8);
      combinedBytes[3] = (byte) (kvCount);
      value = combinedBytes;
      merged = true;
    }
    super.write(out);
  }

  @Override
  public void set(final byte[] b) {
    super.set(b);
    merged = false;
  }

  @Override
  public void copy(byte[] b) {
    super.copy(b);
    merged = false;
  }

  /**
   * Takes a Value and will take out the embedded kvCount, and then return that value while replacing the Value with the original unembedded version
   *
   * @return The kvCount embedded in v.
   */
  public static int splitKVCount(Value v) {
    if (v instanceof MemValue)
      return ((MemValue) v).kvCount;

    byte[] originalBytes = new byte[v.getSize() - 4];
    byte[] combined = v.get();
    System.arraycopy(combined, 4, originalBytes, 0, originalBytes.length);
    v.set(originalBytes);
    return (combined[0] << 24) + ((combined[1] & 0xFF) << 16) + ((combined[2] & 0xFF) << 8) + (combined[3] & 0xFF);
  }
}

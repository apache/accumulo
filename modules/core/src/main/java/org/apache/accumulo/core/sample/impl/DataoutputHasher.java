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

package org.apache.accumulo.core.sample.impl;

import java.io.DataOutput;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

import com.google.common.hash.Hasher;

public class DataoutputHasher implements DataOutput {

  private Hasher hasher;

  public DataoutputHasher(Hasher hasher) {
    this.hasher = hasher;
  }

  @Override
  public void write(int b) throws IOException {
    hasher.putByte((byte) (0xff & b));
  }

  @Override
  public void write(byte[] b) throws IOException {
    hasher.putBytes(b);
  }

  @Override
  public void write(byte[] b, int off, int len) throws IOException {
    hasher.putBytes(b, off, len);
  }

  @Override
  public void writeBoolean(boolean v) throws IOException {
    hasher.putBoolean(v);
  }

  @Override
  public void writeByte(int v) throws IOException {
    hasher.putByte((byte) (0xff & v));

  }

  @Override
  public void writeShort(int v) throws IOException {
    hasher.putShort((short) (0xffff & v));
  }

  @Override
  public void writeChar(int v) throws IOException {
    hasher.putChar((char) v);
  }

  @Override
  public void writeInt(int v) throws IOException {
    hasher.putInt(v);
  }

  @Override
  public void writeLong(long v) throws IOException {
    hasher.putLong(v);
  }

  @Override
  public void writeFloat(float v) throws IOException {
    hasher.putDouble(v);
  }

  @Override
  public void writeDouble(double v) throws IOException {
    hasher.putDouble(v);
  }

  @Override
  public void writeBytes(String s) throws IOException {
    for (int i = 0; i < s.length(); i++) {
      hasher.putByte((byte) (0xff & s.charAt(i)));
    }
  }

  @Override
  public void writeChars(String s) throws IOException {
    hasher.putString(s);

  }

  @Override
  public void writeUTF(String s) throws IOException {
    hasher.putInt(s.length());
    hasher.putBytes(s.getBytes(StandardCharsets.UTF_8));
  }
}

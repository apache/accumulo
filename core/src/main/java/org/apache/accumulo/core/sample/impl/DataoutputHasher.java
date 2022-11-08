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
package org.apache.accumulo.core.sample.impl;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.DataOutput;

import com.google.common.hash.Hasher;

public class DataoutputHasher implements DataOutput {

  private Hasher hasher;

  public DataoutputHasher(Hasher hasher) {
    this.hasher = hasher;
  }

  @Override
  public void write(int b) {
    hasher.putByte((byte) (0xff & b));
  }

  @Override
  public void write(byte[] b) {
    hasher.putBytes(b);
  }

  @Override
  public void write(byte[] b, int off, int len) {
    hasher.putBytes(b, off, len);
  }

  @Override
  public void writeBoolean(boolean v) {
    hasher.putBoolean(v);
  }

  @Override
  public void writeByte(int v) {
    hasher.putByte((byte) (0xff & v));

  }

  @Override
  public void writeShort(int v) {
    hasher.putShort((short) (0xffff & v));
  }

  @Override
  public void writeChar(int v) {
    hasher.putChar((char) v);
  }

  @Override
  public void writeInt(int v) {
    hasher.putInt(v);
  }

  @Override
  public void writeLong(long v) {
    hasher.putLong(v);
  }

  @Override
  public void writeFloat(float v) {
    hasher.putDouble(v);
  }

  @Override
  public void writeDouble(double v) {
    hasher.putDouble(v);
  }

  @Override
  public void writeBytes(String s) {
    for (int i = 0; i < s.length(); i++) {
      hasher.putByte((byte) (0xff & s.charAt(i)));
    }
  }

  @Override
  public void writeChars(String s) {
    hasher.putString(s, UTF_8);

  }

  @Override
  public void writeUTF(String s) {
    hasher.putInt(s.length());
    hasher.putBytes(s.getBytes(UTF_8));
  }
}

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
package org.apache.accumulo.core.data;

import static com.google.common.base.Charsets.UTF_8;

import java.io.Serializable;
import java.nio.ByteBuffer;

public class ArrayByteSequence extends ByteSequence implements Serializable {

  private static final long serialVersionUID = 1L;

  protected byte data[];
  protected int offset;
  protected int length;

  public ArrayByteSequence(byte data[]) {
    this.data = data;
    this.offset = 0;
    this.length = data.length;
  }

  public ArrayByteSequence(byte data[], int offset, int length) {

    if (offset < 0 || offset > data.length || length < 0 || (offset + length) > data.length) {
      throw new IllegalArgumentException(" Bad offset and/or length data.length = " + data.length + " offset = " + offset + " length = " + length);
    }

    this.data = data;
    this.offset = offset;
    this.length = length;

  }

  public ArrayByteSequence(String s) {
    this(s.getBytes(UTF_8));
  }

  public ArrayByteSequence(ByteBuffer buffer) {
    this.length = buffer.remaining();

    if (buffer.hasArray()) {
      this.data = buffer.array();
      this.offset = buffer.position();
    } else {
      this.data = new byte[length];
      this.offset = 0;
      buffer.get(data);
    }
  }

  @Override
  public byte byteAt(int i) {

    if (i < 0) {
      throw new IllegalArgumentException("i < 0, " + i);
    }

    if (i >= length) {
      throw new IllegalArgumentException("i >= length, " + i + " >= " + length);
    }

    return data[offset + i];
  }

  @Override
  public byte[] getBackingArray() {
    return data;
  }

  @Override
  public boolean isBackedByArray() {
    return true;
  }

  @Override
  public int length() {
    return length;
  }

  @Override
  public int offset() {
    return offset;
  }

  @Override
  public ByteSequence subSequence(int start, int end) {

    if (start > end || start < 0 || end > length) {
      throw new IllegalArgumentException("Bad start and/end start = " + start + " end=" + end + " offset=" + offset + " length=" + length);
    }

    return new ArrayByteSequence(data, offset + start, end - start);
  }

  @Override
  public byte[] toArray() {
    if (offset == 0 && length == data.length)
      return data;

    byte[] copy = new byte[length];
    System.arraycopy(data, offset, copy, 0, length);
    return copy;
  }

  public String toString() {
    return new String(data, offset, length, UTF_8);
  }
}

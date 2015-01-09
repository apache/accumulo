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
package org.apache.accumulo.core.util;

import static com.google.common.base.Charsets.UTF_8;

import org.apache.accumulo.core.data.ByteSequence;

public class ByteArrayBackedCharSequence implements CharSequence {

  private byte[] data;
  private int offset;
  private int len;

  public ByteArrayBackedCharSequence(byte[] data, int offset, int len) {
    this.data = data;
    this.offset = offset;
    this.len = len;
  }

  public ByteArrayBackedCharSequence(byte[] data) {
    this(data, 0, data.length);
  }

  public ByteArrayBackedCharSequence() {
    this(null, 0, 0);
  }

  public void set(byte[] data, int offset, int len) {
    this.data = data;
    this.offset = offset;
    this.len = len;
  }

  @Override
  public char charAt(int index) {
    return (char) (0xff & data[offset + index]);
  }

  @Override
  public int length() {
    return len;
  }

  @Override
  public CharSequence subSequence(int start, int end) {
    return new ByteArrayBackedCharSequence(data, offset + start, end - start);
  }

  public String toString() {
    return new String(data, offset, len, UTF_8);
  }

  public void set(ByteSequence bs) {
    set(bs.getBackingArray(), bs.offset(), bs.length());
  }
}

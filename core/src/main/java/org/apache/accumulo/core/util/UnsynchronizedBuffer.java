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

import java.nio.ByteBuffer;

import org.apache.hadoop.io.WritableUtils;

/**
 *
 */
public class UnsynchronizedBuffer {
  // created this little class instead of using ByteArrayOutput stream and DataOutputStream
  // because both are synchronized... lots of small syncs slow things down
  public static class Writer {

    int offset = 0;
    byte data[];

    public Writer() {
      data = new byte[64];
    }

    public Writer(int initialCapacity) {
      data = new byte[initialCapacity];
    }

    private void reserve(int l) {
      if (offset + l > data.length) {
        int newSize = UnsynchronizedBuffer.nextArraySize(offset + l);

        byte[] newData = new byte[newSize];
        System.arraycopy(data, 0, newData, 0, offset);
        data = newData;
      }

    }

    public void add(byte[] bytes, int off, int length) {
      reserve(length);
      System.arraycopy(bytes, off, data, offset, length);
      offset += length;
    }

    public void add(boolean b) {
      reserve(1);
      if (b)
        data[offset++] = 1;
      else
        data[offset++] = 0;
    }

    public byte[] toArray() {
      byte ret[] = new byte[offset];
      System.arraycopy(data, 0, ret, 0, offset);
      return ret;
    }

    public ByteBuffer toByteBuffer() {
      return ByteBuffer.wrap(data, 0, offset);
    }

    public void writeVInt(int i) {
      writeVLong(i);
    }

    public void writeVLong(long i) {
      reserve(9);
      if (i >= -112 && i <= 127) {
        data[offset++] = (byte) i;
        return;
      }

      int len = -112;
      if (i < 0) {
        i ^= -1L; // take one's complement'
        len = -120;
      }

      long tmp = i;
      while (tmp != 0) {
        tmp = tmp >> 8;
        len--;
      }

      data[offset++] = (byte) len;

      len = (len < -120) ? -(len + 120) : -(len + 112);

      for (int idx = len; idx != 0; idx--) {
        int shiftbits = (idx - 1) * 8;
        long mask = 0xFFL << shiftbits;
        data[offset++] = (byte) ((i & mask) >> shiftbits);
      }
    }
  }

  public static class Reader {
    int offset;
    byte data[];

    public Reader(byte b[]) {
      this.data = b;
    }

    public Reader(ByteBuffer buffer) {
      if (buffer.hasArray()) {
        offset = buffer.arrayOffset();
        data = buffer.array();
      } else {
        data = new byte[buffer.remaining()];
        buffer.get(data);
      }
    }

    public int readInt() {
      return (data[offset++] << 24) + ((data[offset++] & 255) << 16) + ((data[offset++] & 255) << 8) + ((data[offset++] & 255) << 0);
    }

    public long readLong() {
      return (((long) data[offset++] << 56) + ((long) (data[offset++] & 255) << 48) + ((long) (data[offset++] & 255) << 40)
          + ((long) (data[offset++] & 255) << 32) + ((long) (data[offset++] & 255) << 24) + ((data[offset++] & 255) << 16) + ((data[offset++] & 255) << 8) + ((data[offset++] & 255) << 0));
    }

    public void readBytes(byte b[]) {
      System.arraycopy(data, offset, b, 0, b.length);
      offset += b.length;
    }

    public boolean readBoolean() {
      return (data[offset++] == 1);
    }

    public int readVInt() {
      return (int) readVLong();
    }

    public long readVLong() {
      byte firstByte = data[offset++];
      int len = WritableUtils.decodeVIntSize(firstByte);
      if (len == 1) {
        return firstByte;
      }
      long i = 0;
      for (int idx = 0; idx < len - 1; idx++) {
        byte b = data[offset++];
        i = i << 8;
        i = i | (b & 0xFF);
      }
      return (WritableUtils.isNegativeVInt(firstByte) ? (i ^ -1L) : i);
    }
  }

  /**
   * Determines what next array size should be by rounding up to next power of two.
   *
   */
  public static int nextArraySize(int i) {
    if (i < 0)
      throw new IllegalArgumentException();

    if (i > (1 << 30))
      return Integer.MAX_VALUE; // this is the next power of 2 minus one... a special case

    if (i == 0) {
      return 1;
    }

    // round up to next power of two
    int ret = i;
    ret--;
    ret |= ret >> 1;
    ret |= ret >> 2;
    ret |= ret >> 4;
    ret |= ret >> 8;
    ret |= ret >> 16;
    ret++;

    return ret;
  }
}

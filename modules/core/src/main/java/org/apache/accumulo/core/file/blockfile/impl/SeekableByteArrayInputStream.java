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

package org.apache.accumulo.core.file.blockfile.impl;

import static java.util.Objects.requireNonNull;

import java.io.IOException;
import java.io.InputStream;

/**
 * This class is like byte array input stream with two differences. It supports seeking and avoids synchronization.
 */
public class SeekableByteArrayInputStream extends InputStream {

  // making this volatile for the following case
  // * thread 1 creates and initalizes byte array
  // * thread 2 reads from bye array
  // Findbugs complains about this because thread2 may not see any changes to the byte array after thread 1 set the voltile,
  // however the expectation is that the byte array is static. In the case of it being static, volatile ensures that
  // thread 2 sees all of thread 1 changes before setting the volatile.
  private volatile byte buffer[];
  private int cur;
  private int max;

  @Override
  public int read() {
    if (cur < max) {
      return buffer[cur++] & 0xff;
    } else {
      return -1;
    }
  }

  @Override
  public int read(byte b[], int offset, int length) {
    if (b == null) {
      throw new NullPointerException();
    }

    if (length < 0 || offset < 0 || length > b.length - offset) {
      throw new IndexOutOfBoundsException();
    }

    if (length == 0) {
      return 0;
    }

    int avail = max - cur;

    if (avail <= 0) {
      return -1;
    }

    if (length > avail) {
      length = avail;
    }

    System.arraycopy(buffer, cur, b, offset, length);
    cur += length;
    return length;
  }

  @Override
  public long skip(long requestedSkip) {
    long actualSkip = max - cur;
    if (requestedSkip < actualSkip)
      if (requestedSkip < 0)
        actualSkip = 0;
      else
        actualSkip = requestedSkip;

    cur += actualSkip;
    return actualSkip;
  }

  @Override
  public int available() {
    return max - cur;
  }

  @Override
  public boolean markSupported() {
    return false;
  }

  @Override
  public void mark(int readAheadLimit) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void reset() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void close() throws IOException {}

  public SeekableByteArrayInputStream(byte[] buf) {
    requireNonNull(buf, "bug argument was null");
    this.buffer = buf;
    this.cur = 0;
    this.max = buf.length;
  }

  public SeekableByteArrayInputStream(byte[] buf, int maxOffset) {
    requireNonNull(buf, "bug argument was null");
    this.buffer = buf;
    this.cur = 0;
    this.max = maxOffset;
  }

  public void seek(int position) {
    if (position < 0 || position >= max)
      throw new IllegalArgumentException("position = " + position + " maxOffset = " + max);
    this.cur = position;
  }

  public int getPosition() {
    return this.cur;
  }

  byte[] getBuffer() {
    return buffer;
  }
}

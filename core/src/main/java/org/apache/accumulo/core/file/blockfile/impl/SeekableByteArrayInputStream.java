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
package org.apache.accumulo.core.file.blockfile.impl;

import static java.util.Objects.requireNonNull;

import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import java.util.function.IntBinaryOperator;

/**
 * This class is like byte array input stream with two differences. It supports seeking and avoids
 * synchronization.
 */
public class SeekableByteArrayInputStream extends InputStream {

  private final byte[] buffer;
  private final AtomicInteger cur = new AtomicInteger(0);
  private final int max;

  @Override
  public int read() {
    // advance the pointer by 1 if we haven't reached the end
    final int currentValue = cur.getAndAccumulate(1, (v, x) -> v < max ? v + x : v);
    if (currentValue < max) {
      return buffer[currentValue] & 0xff;
    } else {
      return -1;
    }
  }

  @Override
  public int read(byte[] b, int offset, int length) {
    if (b == null) {
      throw new NullPointerException();
    }

    if (length < 0 || offset < 0 || length > b.length - offset) {
      throw new IndexOutOfBoundsException();
    }

    if (length == 0) {
      return 0;
    }

    // compute how much to read, based on what's left available
    IntBinaryOperator add = (cur1, length1) -> {
      final int available = max - cur1;
      if (available <= 0) {
        return cur1;
      } else if (length1 > available) {
        length1 = available;
      }
      return cur1 + length1;
    };

    final int currentValue = cur.getAndAccumulate(length, add);

    final int avail = max - currentValue;

    if (avail <= 0) {
      return -1;
    }

    if (length > avail) {
      length = avail;
    }

    System.arraycopy(buffer, currentValue, b, offset, length);
    return length;
  }

  @Override
  public long skip(long requestedSkip) {

    // actual skip is at least 0, but no more than what's available
    BiFunction<Integer,Integer,Integer> skipValue =
        (current, skip) -> Math.max(0, Math.min(max - current, skip));

    // compute how much to advance, based on actual amount skipped
    IntBinaryOperator add = (cur1, skip) -> cur1 + skipValue.apply(cur1, skip);

    // advance the pointer and return the actual amount skipped
    int currentValue = cur.getAndAccumulate((int) requestedSkip, add);

    return skipValue.apply(currentValue, (int) requestedSkip);
  }

  @Override
  public int available() {
    return max - cur.get();
  }

  @Override
  public boolean markSupported() {
    return false;
  }

  @Override
  public synchronized void mark(int readAheadLimit) {
    throw new UnsupportedOperationException();
  }

  @Override
  public synchronized void reset() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void close() throws IOException {}

  public SeekableByteArrayInputStream(byte[] buf) {
    requireNonNull(buf, "bug argument was null");
    this.buffer = buf;
    this.max = buf.length;
  }

  public SeekableByteArrayInputStream(byte[] buf, int maxOffset) {
    requireNonNull(buf, "bug argument was null");
    this.buffer = buf;
    this.max = maxOffset;
  }

  public void seek(int position) {
    if (position < 0 || position >= max) {
      throw new IllegalArgumentException("position = " + position + " maxOffset = " + max);
    }
    this.cur.set(position);
  }

  public int getPosition() {
    return this.cur.get();
  }

  byte[] getBuffer() {
    return buffer;
  }
}

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

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * This class is like byte array input stream with two differences. It supports seeking and avoids
 * synchronization.
 */
public class SeekableByteArrayInputStream extends InputStream {

  // making this volatile for the following case
  // * thread 1 creates and initializes byte array
  // * thread 2 reads from bye array
  // spotbugs complains about this because thread2 may not see any changes to the byte array after
  // thread 1 set the volatile,
  // however the expectation is that the byte array is static. In the case of it being static,
  // volatile ensures that
  // thread 2 sees all of thread 1 changes before setting the volatile.
  @SuppressFBWarnings(value = "VO_VOLATILE_REFERENCE_TO_ARRAY",
      justification = "see explanation above")
  private final byte[] buffer;
  private final AtomicInteger cur = new AtomicInteger(0);
  private final int max;

  @Override
  public int read() {
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

    BiFunction<Integer,Integer,Integer> skipValue = (current, skip) -> {
      int actual = max - current;
      if (skip < actual) {
        actual = Math.max(skip, 0);
      }
      return actual;
    };

    IntBinaryOperator add = (cur1, skip) -> {
      int actual = skipValue.apply(cur1, skip);
      return cur1 + actual;
    };

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

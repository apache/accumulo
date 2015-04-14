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
package org.apache.accumulo.core.security.crypto;

import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;

/**
 * Reader corresponding to BlockedOutputStream. Expects all data to be in the form of size (int) data (size bytes) junk (however many bytes it takes to complete
 * a block)
 */
public class BlockedInputStream extends InputStream {
  byte[] array;
  // ReadPos is where to start reading
  // WritePos is the last position written to
  int readPos, writePos;
  DataInputStream in;
  int blockSize;
  boolean finished = false;

  public BlockedInputStream(InputStream in, int blockSize, int maxSize) {
    if (blockSize == 0)
      throw new RuntimeException("Invalid block size");
    if (in instanceof DataInputStream)
      this.in = (DataInputStream) in;
    else
      this.in = new DataInputStream(in);

    array = new byte[maxSize];
    readPos = 0;
    writePos = -1;

    this.blockSize = blockSize;
  }

  @Override
  public int read() throws IOException {
    if (remaining() > 0)
      return (array[readAndIncrement(1)] & 0xFF);
    return -1;
  }

  private int readAndIncrement(int toAdd) {
    int toRet = readPos;
    readPos += toAdd;
    if (readPos == array.length)
      readPos = 0;
    else if (readPos > array.length)
      throw new RuntimeException("Unexpected state, this should only ever increase or cycle on the boundry!");
    return toRet;
  }

  @Override
  public int read(byte b[], int off, int len) throws IOException {
    int toCopy = Math.min(len, remaining());
    if (toCopy > 0) {
      System.arraycopy(array, readPos, b, off, toCopy);
      readAndIncrement(toCopy);
    }
    return toCopy;
  }

  private int remaining() throws IOException {
    if (finished)
      return -1;
    if (available() == 0)
      refill();

    return available();
  }

  // Amount available to read
  @Override
  public int available() {
    int toRet = writePos + 1 - readPos;
    if (toRet < 0)
      toRet += array.length;
    return Math.min(array.length - readPos, toRet);
  }

  private boolean refill() throws IOException {
    if (finished)
      return false;
    int size;
    try {
      size = in.readInt();
    } catch (EOFException eof) {
      finished = true;
      return false;
    }

    // Shortcut for if we're reading garbage data
    if (size < 0 || size > array.length) {
      finished = true;
      return false;
    } else if (size == 0)
      throw new RuntimeException("Empty block written, this shouldn't happen with this BlockedOutputStream.");

    // We have already checked, not concerned with looping the buffer here
    int bufferAvailable = array.length - readPos;
    if (size > bufferAvailable) {
      in.readFully(array, writePos + 1, bufferAvailable);
      in.readFully(array, 0, size - bufferAvailable);
    } else {
      in.readFully(array, writePos + 1, size);
    }
    writePos += size;
    if (writePos >= array.length - 1)
      writePos -= array.length;

    // Skip the cruft
    int remainder = blockSize - ((size + 4) % blockSize);
    if (remainder != blockSize) {
      // If remainder isn't spilling the rest of the block, we know it's incomplete.
      if (in.available() < remainder) {
        undoWrite(size);
        return false;
      }
      in.skip(remainder);
    }

    return true;
  }

  private void undoWrite(int size) {
    writePos = writePos - size;
    if (writePos < -1)
      writePos += array.length;
  }

  @Override
  public long skip(long n) throws IOException {
    throw new UnsupportedOperationException();
    // available(n);
    // bb.position(bb.position()+(int)n);
  }

  @Override
  public void close() throws IOException {
    array = null;
    in.close();
  }

  @Override
  public synchronized void mark(int readlimit) {
    throw new UnsupportedOperationException();
  }

  @Override
  public synchronized void reset() throws IOException {
    in.reset();
    readPos = 0;
    writePos = -1;
  }

  @Override
  public boolean markSupported() {
    return false;
  }
}

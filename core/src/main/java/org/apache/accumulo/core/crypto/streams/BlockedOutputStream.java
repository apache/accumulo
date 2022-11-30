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
package org.apache.accumulo.core.crypto.streams;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;

/**
 * Buffers all input in a growing buffer until flush() is called. Then entire buffer is written,
 * with size information, and padding to force the underlying crypto output stream to also fully
 * flush
 */
public class BlockedOutputStream extends OutputStream {
  int blockSize;
  DataOutputStream out;
  ByteBuffer bb;

  public BlockedOutputStream(OutputStream out, int blockSize, int bufferSize) {
    if (bufferSize <= 0) {
      throw new IllegalArgumentException("bufferSize must be greater than 0.");
    }
    if (out instanceof DataOutputStream) {
      this.out = (DataOutputStream) out;
    } else {
      this.out = new DataOutputStream(out);
    }
    this.blockSize = blockSize;
    int remainder = bufferSize % blockSize;
    if (remainder != 0) {
      remainder = blockSize - remainder;
    }
    // some buffer space + bytes to make the buffer evened up with the cipher block size - 4 bytes
    // for the size int
    bb = ByteBuffer.allocate(bufferSize + remainder - 4);
  }

  @Override
  public synchronized void flush() throws IOException {
    if (!bb.hasArray()) {
      throw new RuntimeException("BlockedOutputStream has no backing array.");
    }
    int size = bb.position();
    if (size == 0) {
      return;
    }
    out.writeInt(size);

    int remainder = ((size + 4) % blockSize);
    if (remainder != 0) {
      remainder = blockSize - remainder;
    }

    // This is garbage
    bb.position(size + remainder);
    out.write(bb.array(), 0, size + remainder);

    out.flush();
    bb.rewind();
  }

  @Override
  public void write(int b) throws IOException {
    // Checking before provides same functionality but causes the case of previous flush() failing
    // to now throw a buffer out of bounds error
    if (bb.remaining() == 0) {
      flush();
    }
    bb.put((byte) b);
  }

  @Override
  public void write(byte[] b, int off, int len) throws IOException {
    // Can't recurse here in case the len is large and the blocksize is small (and the stack is
    // small)
    // So we'll just fill up the buffer over and over
    while (len >= bb.remaining()) {
      int remaining = bb.remaining();
      bb.put(b, off, remaining);
      // This is guaranteed to have the buffer filled, so we'll just flush it. No check needed
      flush();
      off += remaining;
      len -= remaining;
    }
    // And then write the remainder (and this is guaranteed to not fill the buffer, so we won't
    // flush afterward
    bb.put(b, off, len);
  }

  @Override
  public void write(byte[] b) throws IOException {
    write(b, 0, b.length);
  }

  @Override
  public void close() throws IOException {
    flush();
    out.close();
  }
}

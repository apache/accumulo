/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.accumulo.core.file.streams;

import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.fs.Seekable;

/**
 * BoundedRangeFIleInputStream abstracts a contiguous region of a Hadoop FSDataInputStream as a regular input stream. One can create multiple
 * BoundedRangeFileInputStream on top of the same FSDataInputStream and they would not interfere with each other.
 */
public class BoundedRangeFileInputStream extends InputStream {

  private volatile boolean closed = false;
  private final InputStream in;
  private long pos;
  private long end;
  private long mark;
  private final byte[] oneByte = new byte[1];

  /**
   * Constructor
   *
   * @param in
   *          The FSDataInputStream we connect to.
   * @param offset
   *          Beginning offset of the region.
   * @param length
   *          Length of the region.
   *
   *          The actual length of the region may be smaller if (off_begin + length) goes beyond the end of FS input stream.
   */
  public <StreamType extends InputStream & Seekable> BoundedRangeFileInputStream(StreamType in, long offset, long length) {
    if (offset < 0 || length < 0) {
      throw new IndexOutOfBoundsException("Invalid offset/length: " + offset + "/" + length);
    }

    this.in = in;
    this.pos = offset;
    this.end = offset + length;
    this.mark = -1;
  }

  @Override
  public int available() throws IOException {
    return (int) (end - pos);
  }

  @Override
  public int read() throws IOException {
    int ret = read(oneByte);
    if (ret == 1)
      return oneByte[0] & 0xff;
    return -1;
  }

  @Override
  public int read(byte[] b) throws IOException {
    return read(b, 0, b.length);
  }

  @Override
  public int read(final byte[] b, final int off, int len) throws IOException {
    if ((off | len | (off + len) | (b.length - (off + len))) < 0) {
      throw new IndexOutOfBoundsException();
    }

    final int n = (int) Math.min(Integer.MAX_VALUE, Math.min(len, (end - pos)));
    if (n == 0)
      return -1;
    Integer ret = 0;
    synchronized (in) {
      // ensuring we are not closed which would be followed by someone else reusing the decompressor
      if (closed) {
        throw new IOException("Stream closed");
      }
      ((Seekable) in).seek(pos);
      ret = in.read(b, off, n);
    }
    if (ret < 0) {
      end = pos;
      return -1;
    }
    pos += ret;
    return ret;
  }

  @Override
  /*
   * We may skip beyond the end of the file.
   */
  public long skip(long n) throws IOException {
    long len = Math.min(n, end - pos);
    pos += len;
    return len;
  }

  @Override
  public void mark(int readlimit) {
    mark = pos;
  }

  @Override
  public void reset() throws IOException {
    if (mark < 0)
      throw new IOException("Resetting to invalid mark");
    pos = mark;
  }

  @Override
  public boolean markSupported() {
    return true;
  }

  @Override
  public void close() {
    // Synchronize on the FSDataInputStream to ensure we are blocked if in the read method:
    // Once this close completes, the underlying decompression stream may be returned to
    // the pool and subsequently used. Turns out this is a problem if currently using it to read.
    if (!closed) {
      synchronized (in) {
        // Invalidate the state of the stream.
        closed = true;
      }
    }
  }
}

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
package org.apache.accumulo.core.file.rfile.bcfile;

import java.io.FilterInputStream;
import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.InputStream;
import org.apache.accumulo.core.util.RateLimiter;
import org.apache.hadoop.fs.Seekable;

public class RateLimited {
  /**
   * To avoid too much lock contention, we only request permits from a RateLimiter once we notice that at least this many bytes have been read/written since our
   * last request.
   */
  protected static class BlockedRateLimiter {
    private static final long PERMIT_REQUEST_BLOCK = 65536;

    private final RateLimiter rateLimiter;
    private long permitsUsed = 0;
    private long permitsAcquired = 0;

    BlockedRateLimiter(RateLimiter rateLimiter) {
      this.rateLimiter = rateLimiter;
    }

    public synchronized void acquire(long permitsNeeded) {
      permitsUsed += permitsNeeded;
      // Skip the rate limiter so long as you're not more than PERMIT_REQUST_BLOCK bytes beyond
      // the number of permits you've already acquired from the rate limiter. We'll make up the difference
      // in finish(), at close() time.
      while (rateLimiter != null && permitsUsed - permitsAcquired > PERMIT_REQUEST_BLOCK) {
        long requestCount = Math.max(PERMIT_REQUEST_BLOCK, permitsUsed - permitsAcquired);
        requestCount = Math.min(requestCount, Integer.MAX_VALUE);
        rateLimiter.acquire((int) requestCount);
        permitsAcquired += requestCount;
      }
    }

    public synchronized long getPermitsUsed() {
      return permitsUsed;
    }

    public void finish() {
      rateLimiter.acquire((int) (permitsUsed - permitsAcquired));
    }
  }

  public static class OutputStream extends FilterOutputStream {
    private final BlockedRateLimiter writeLimiter;

    OutputStream(java.io.OutputStream wrappedStream, RateLimiter writeLimiter) {
      super(wrappedStream);
      this.writeLimiter = new BlockedRateLimiter(writeLimiter);
    }

    public long getPos() {
      return writeLimiter.getPermitsUsed();
    }

    @Override
    public void write(int i) throws IOException {
      writeLimiter.acquire(1);
      out.write(i);
    }

    @Override
    public void write(byte[] buffer, int offset, int length) throws IOException {
      writeLimiter.acquire(length);
      out.write(buffer, offset, length);
    }

    @Override
    public void close() throws IOException {
      try {
        out.close();
      } finally {
        writeLimiter.finish();
      }
    }
  }

  public static class DataOutputStream extends java.io.DataOutputStream {
    DataOutputStream(java.io.OutputStream wrappedStream, RateLimiter writeLimiter) {
      super(new RateLimited.OutputStream(wrappedStream, writeLimiter));
    }

    public long getPos() {
      return ((RateLimited.OutputStream) out).getPos();
    }
  }

  public static class SeekableInputStream extends FilterInputStream implements Seekable {
    private final BlockedRateLimiter readLimiter;

    public <StreamType extends InputStream & Seekable> SeekableInputStream(StreamType stream, RateLimiter readLimiter) {
      super(stream);
      this.readLimiter = new BlockedRateLimiter(readLimiter);
    }

    @Override
    public void seek(long pos) throws IOException {
      ((Seekable) in).seek(pos);
    }

    @Override
    public long getPos() throws IOException {
      return ((Seekable) in).getPos();
    }

    @Override
    public boolean seekToNewSource(long targetPos) throws IOException {
      return ((Seekable) in).seekToNewSource(targetPos);
    }

    @Override
    public int read() throws IOException {
      int retVal = in.read();
      if (retVal >= 0) {
        readLimiter.acquire(1);
      }
      return retVal;
    }

    @Override
    public int read(byte[] buffer, int offset, int length) throws IOException {
      int count = in.read(buffer, offset, length);
      if (count > 0) {
        readLimiter.acquire(count);
      }
      return count;
    }

    @Override
    public void close() throws IOException {
      try {
        super.close();
      } finally {
        readLimiter.finish();
      }
    }
  }

  public static class DataInputStream extends java.io.DataInputStream implements Seekable {
    public <StreamType extends InputStream & Seekable> DataInputStream(StreamType wrappedStream, RateLimiter readLimiter) {
      super(new RateLimited.SeekableInputStream(wrappedStream, readLimiter));
    }

    @Override
    public void seek(long pos) throws IOException {
      ((Seekable) in).seek(pos);
    }

    @Override
    public long getPos() throws IOException {
      return ((Seekable) in).getPos();
    }

    @Override
    public boolean seekToNewSource(long targetPos) throws IOException {
      return ((Seekable) in).seekToNewSource(targetPos);
    }
  }
}

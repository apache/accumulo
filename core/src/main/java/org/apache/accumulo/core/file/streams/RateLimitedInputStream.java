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
package org.apache.accumulo.core.file.streams;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;

import org.apache.accumulo.core.util.ratelimit.NullRateLimiter;
import org.apache.accumulo.core.util.ratelimit.RateLimiter;
import org.apache.hadoop.fs.Seekable;

/**
 * A decorator for an {@code InputStream} which limits the rate at which reads are performed.
 */
public class RateLimitedInputStream extends FilterInputStream implements Seekable {
  private final RateLimiter rateLimiter;

  public <StreamType extends InputStream & Seekable> RateLimitedInputStream(StreamType stream,
      RateLimiter rateLimiter) {
    super(stream);
    this.rateLimiter = rateLimiter == null ? NullRateLimiter.INSTANCE : rateLimiter;
  }

  @Override
  public int read() throws IOException {
    int val = in.read();
    if (val >= 0) {
      rateLimiter.acquire(1);
    }
    return val;
  }

  @Override
  public int read(byte[] buffer, int offset, int length) throws IOException {
    int count = in.read(buffer, offset, length);
    if (count > 0) {
      rateLimiter.acquire(count);
    }
    return count;
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

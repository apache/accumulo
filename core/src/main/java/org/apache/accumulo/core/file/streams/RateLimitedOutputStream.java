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

import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.accumulo.core.util.ratelimit.NullRateLimiter;
import org.apache.accumulo.core.util.ratelimit.RateLimiter;
import org.apache.hadoop.fs.FSDataOutputStream;

/**
 * A decorator for {@code OutputStream} which limits the rate at which data may be written.
 * Underlying OutputStream is a FSDataOutputStream.
 */
public class RateLimitedOutputStream extends DataOutputStream {
  private final RateLimiter writeLimiter;

  public RateLimitedOutputStream(FSDataOutputStream fsDataOutputStream, RateLimiter writeLimiter) {
    super(fsDataOutputStream);
    this.writeLimiter = writeLimiter == null ? NullRateLimiter.INSTANCE : writeLimiter;
  }

  @Override
  public synchronized void write(int i) throws IOException {
    writeLimiter.acquire(1);
    out.write(i);
  }

  @Override
  public synchronized void write(byte[] buffer, int offset, int length) throws IOException {
    writeLimiter.acquire(length);
    out.write(buffer, offset, length);
  }

  @Override
  public void close() throws IOException {
    out.close();
  }

  public long position() {
    return ((FSDataOutputStream) out).getPos();
  }
}

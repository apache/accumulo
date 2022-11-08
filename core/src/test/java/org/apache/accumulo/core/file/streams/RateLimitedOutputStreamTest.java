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

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.OutputStream;
import java.security.SecureRandom;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.accumulo.core.util.ratelimit.RateLimiter;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.easymock.EasyMock;
import org.junit.jupiter.api.Test;

import com.google.common.io.CountingOutputStream;

public class RateLimitedOutputStreamTest {

  private static final SecureRandom random = new SecureRandom();

  @Test
  public void permitsAreProperlyAcquired() throws Exception {
    // Create variables for tracking behaviors of mock object
    AtomicLong rateLimiterPermitsAcquired = new AtomicLong();
    // Construct mock object
    RateLimiter rateLimiter = EasyMock.niceMock(RateLimiter.class);
    // Stub Mock Method
    rateLimiter.acquire(EasyMock.anyLong());
    EasyMock.expectLastCall()
        .andAnswer(() -> rateLimiterPermitsAcquired.addAndGet(EasyMock.getCurrentArgument(0)))
        .anyTimes();
    EasyMock.replay(rateLimiter);

    long bytesWritten = 0;
    try (RateLimitedOutputStream os =
        new RateLimitedOutputStream(new NullOutputStream(), rateLimiter)) {
      for (int i = 0; i < 100; ++i) {
        byte[] bytes = new byte[Math.abs(random.nextInt() % 65536)];
        os.write(bytes);
        bytesWritten += bytes.length;
      }
      assertEquals(bytesWritten, os.position());
    }
    assertEquals(bytesWritten, rateLimiterPermitsAcquired.get());
  }

  public static class NullOutputStream extends FSDataOutputStream {
    public NullOutputStream() {
      super(new CountingOutputStream(OutputStream.nullOutputStream()), null);
    }
  }

}

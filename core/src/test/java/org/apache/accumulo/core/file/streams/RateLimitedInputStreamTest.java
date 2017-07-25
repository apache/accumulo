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
package org.apache.accumulo.core.file.streams;

import java.io.IOException;
import java.io.InputStream;
import java.util.Random;

import org.apache.hadoop.fs.Seekable;
import org.junit.Assert;
import org.junit.Test;

public class RateLimitedInputStreamTest {

  @Test
  public void permitsAreProperlyAcquired() throws Exception {
    Random randGen = new Random();
    MockRateLimiter rateLimiter = new MockRateLimiter();
    long bytesRetrieved = 0;
    try (InputStream is = new RateLimitedInputStream(new RandomInputStream(), rateLimiter)) {
      for (int i = 0; i < 100; ++i) {
        int count = Math.abs(randGen.nextInt()) % 65536;
        int countRead = is.read(new byte[count]);
        Assert.assertEquals(count, countRead);
        bytesRetrieved += count;
      }
    }
    Assert.assertEquals(bytesRetrieved, rateLimiter.getPermitsAcquired());
  }

  private static class RandomInputStream extends InputStream implements Seekable {
    private final Random r = new Random();

    @Override
    public int read() throws IOException {
      return r.nextInt() & 0xff;
    }

    @Override
    public void seek(long pos) throws IOException {
      throw new UnsupportedOperationException("Not supported yet."); // To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public long getPos() throws IOException {
      throw new UnsupportedOperationException("Not supported yet."); // To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public boolean seekToNewSource(long targetPos) throws IOException {
      throw new UnsupportedOperationException("Not supported yet."); // To change body of generated methods, choose Tools | Templates.
    }

  }

}

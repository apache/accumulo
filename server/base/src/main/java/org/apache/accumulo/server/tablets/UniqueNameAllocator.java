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
package org.apache.accumulo.server.tablets;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.security.SecureRandom;
import java.util.Random;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.util.FastFormat;
import org.apache.accumulo.fate.zookeeper.ZooReaderWriter;
import org.apache.accumulo.fate.zookeeper.ZooUtil;
import org.apache.accumulo.server.ServerContext;

/**
 * Allocates unique names for an accumulo instance. The names are unique for the lifetime of the
 * instance.
 *
 * This is useful for filenames because it makes caching easy.
 */
public class UniqueNameAllocator {

  private ServerContext context;
  private long next = 0;
  private long maxAllocated = 0;
  private String nextNamePath;
  private Random rand;

  public UniqueNameAllocator(ServerContext context) {
    this.context = context;
    nextNamePath = Constants.ZROOT + "/" + context.getInstanceID() + Constants.ZNEXT_FILE;
    rand = new SecureRandom();
  }

  public synchronized String getNextName() {

    while (next >= maxAllocated) {
      final int allocate = 100 + rand.nextInt(100);

      try {
        byte[] max = context.getZooReaderWriter().mutate(nextNamePath, null, ZooUtil.PRIVATE,
            new ZooReaderWriter.Mutator() {
              @Override
              public byte[] mutate(byte[] currentValue) {
                long l = Long.parseLong(new String(currentValue, UTF_8), Character.MAX_RADIX);
                l += allocate;
                return Long.toString(l, Character.MAX_RADIX).getBytes(UTF_8);
              }
            });

        maxAllocated = Long.parseLong(new String(max, UTF_8), Character.MAX_RADIX);
        next = maxAllocated - allocate;

      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }

    return new String(FastFormat.toZeroPaddedString(next++, 7, Character.MAX_RADIX, new byte[0]),
        UTF_8);
  }
}

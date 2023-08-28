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
package org.apache.accumulo.server.tablets;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.security.SecureRandom;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.util.FastFormat;
import org.apache.accumulo.server.ServerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Allocates unique names for an accumulo instance. The names are unique for the lifetime of the
 * instance.
 *
 * This is useful for filenames because it makes caching easy.
 */
public class UniqueNameAllocator {

  private static Logger log = LoggerFactory.getLogger(UniqueNameAllocator.class);

  private static final int DEFAULT_BASE_ALLOCATION =
      Integer.parseInt(Property.GENERAL_FILENAME_BASE_ALLOCATION.getDefaultValue());

  private ServerContext context;
  private long next = 0;
  private long maxAllocated = 0;
  private String nextNamePath;
  private static final SecureRandom random = new SecureRandom();

  public UniqueNameAllocator(ServerContext context) {
    this.context = context;
    nextNamePath = Constants.ZROOT + "/" + context.getInstanceID() + Constants.ZNEXT_FILE;
  }

  public synchronized String getNextName() {

    while (next >= maxAllocated) {
      final int allocate = getAllocation();

      try {
        byte[] max = context.getZooReaderWriter().mutateExisting(nextNamePath, currentValue -> {
          long l = Long.parseLong(new String(currentValue, UTF_8), Character.MAX_RADIX);
          return Long.toString(l + allocate, Character.MAX_RADIX).getBytes(UTF_8);
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

  private int getAllocation() {
    int baseAllocation =
        context.getConfiguration().getCount(Property.GENERAL_FILENAME_BASE_ALLOCATION);
    int jitterAllocation =
        context.getConfiguration().getCount(Property.GENERAL_FILENAME_JITTER_ALLOCATION);

    if (baseAllocation <= 0) {
      log.warn("{} was set to {}, must be greater than 0. Using the default {}.",
          Property.GENERAL_FILENAME_BASE_ALLOCATION.getKey(), baseAllocation,
          DEFAULT_BASE_ALLOCATION);
      baseAllocation = DEFAULT_BASE_ALLOCATION;
    }

    int totalAllocation = baseAllocation;
    if (jitterAllocation > 0) {
      totalAllocation += random.nextInt(jitterAllocation);
    }

    log.debug("Allocating {} filenames", totalAllocation);

    return totalAllocation;
  }
}

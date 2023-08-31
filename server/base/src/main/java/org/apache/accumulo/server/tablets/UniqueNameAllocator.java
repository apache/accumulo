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
import org.apache.accumulo.core.conf.DefaultConfiguration;
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

  private static final Logger log = LoggerFactory.getLogger(UniqueNameAllocator.class);
  private static final SecureRandom random = new SecureRandom();
  private static final Property MIN_PROP = Property.GENERAL_FILE_NAME_ALLOCATION_BATCH_SIZE_MIN;
  private static final Property MAX_PROP = Property.GENERAL_FILE_NAME_ALLOCATION_BATCH_SIZE_MAX;
  private static final int DEFAULT_MIN = DefaultConfiguration.getInstance().getCount(MIN_PROP);

  private final ServerContext context;
  private final String nextNamePath;

  private long next = 0;
  private long maxAllocated = 0;

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
        throw new IllegalStateException(e);
      }
    }
    return new String(FastFormat.toZeroPaddedString(next++, 7, Character.MAX_RADIX, new byte[0]),
        UTF_8);
  }

  private int getAllocation() {
    int minAllocation = context.getConfiguration().getCount(MIN_PROP);
    int maxAllocation = context.getConfiguration().getCount(MAX_PROP);

    if (minAllocation <= 0) {
      log.warn("{} was set to {}, but must be greater than 0. Using the default ({}).",
          MIN_PROP.getKey(), minAllocation, DEFAULT_MIN);
      minAllocation = DEFAULT_MIN;
    }

    if (maxAllocation < minAllocation) {
      log.warn("{} was set to {}, must be greater than or equal to {} ({}). Using {}.",
          MAX_PROP.getKey(), maxAllocation, MIN_PROP.getKey(), minAllocation, minAllocation);
      maxAllocation = minAllocation;
    }

    int actualBatchSize = minAllocation + random.nextInt((maxAllocation - minAllocation) + 1);
    log.debug("Allocating {} filenames", actualBatchSize);
    return actualBatchSize;
  }
}

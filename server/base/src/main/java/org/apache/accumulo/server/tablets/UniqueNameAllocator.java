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
import static org.apache.accumulo.core.util.LazySingletons.RANDOM;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.conf.DefaultConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.util.FastFormat;
import org.apache.accumulo.server.ServerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

/**
 * Allocates unique names for an accumulo instance. The names are unique for the lifetime of the
 * instance.
 *
 * This is useful for filenames because it makes caching easy.
 */
public class UniqueNameAllocator {

  private static final Logger log = LoggerFactory.getLogger(UniqueNameAllocator.class);
  private static final Property MIN_PROP = Property.GENERAL_FILE_NAME_ALLOCATION_BATCH_SIZE_MIN;
  private static final Property MAX_PROP = Property.GENERAL_FILE_NAME_ALLOCATION_BATCH_SIZE_MAX;
  private static final int DEFAULT_MIN = DefaultConfiguration.getInstance().getCount(MIN_PROP);

  private final ServerContext context;

  private long next = 0;
  // non inclusive end allocated names
  private long maxAllocated = 0;

  public UniqueNameAllocator(ServerContext context) {
    this.context = context;
  }

  public synchronized String getNextName() {
    return getNextNames(1).next();
  }

  /**
   * @param needed the number of names needed
   * @return a thread safe iterator that can be called up to the number of names requested
   */
  public synchronized Iterator<String> getNextNames(int needed) {
    Preconditions.checkArgument(needed > 0);
    while ((next + needed) > maxAllocated) {
      final int allocate = getAllocation(needed);
      try {
        byte[] max = context.getZooSession().asReaderWriter().mutateExisting(Constants.ZNEXT_FILE,
            currentValue -> {
              long l = Long.parseLong(new String(currentValue, UTF_8), Character.MAX_RADIX);
              return Long.toString(l + allocate, Character.MAX_RADIX).getBytes(UTF_8);
            });

        maxAllocated = Long.parseLong(new String(max, UTF_8), Character.MAX_RADIX);
        next = maxAllocated - allocate;

      } catch (Exception e) {
        throw new IllegalStateException(e);
      }
    }

    // inclusive start of rane
    final long start = next;
    next += needed;
    // non inclusive end of range
    final long end = start + needed;
    Preconditions.checkState(end <= maxAllocated);

    // This iterator is thread safe and avoids creating all of the string objects ahead of time.
    return new Iterator<>() {
      private AtomicLong iterNext = new AtomicLong(start);

      @Override
      public boolean hasNext() {
        return iterNext.get() < end;
      }

      @Override
      public String next() {
        long name = iterNext.getAndIncrement();
        if (name >= end) {
          throw new NoSuchElementException();
        }
        return new String(FastFormat.toZeroPaddedString(name, 7, Character.MAX_RADIX, new byte[0]),
            UTF_8);
      }
    };
  }

  private int getAllocation(int needed) {
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

    int actualBatchSize = minAllocation + RANDOM.get().nextInt((maxAllocation - minAllocation) + 1);
    if (needed >= actualBatchSize) {
      actualBatchSize += needed;
    }
    log.debug("Allocating {} filenames", actualBatchSize);
    return actualBatchSize;
  }
}

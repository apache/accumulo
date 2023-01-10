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
package org.apache.accumulo.test.functional;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.iterators.WrappingIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.util.concurrent.Uninterruptibles;

public class SlowMemoryConsumingIterator extends WrappingIterator {

  public static class SlowMemoryConsumingWaitingIterator extends WrappingIterator {

    @Override
    public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive)
        throws IOException {
      LOG.info("Waiting iterator is waiting");
      try {
        LATCH.await();
      } catch (InterruptedException e) {
        Thread.interrupted();
        throw new IOException("thread interrupted", e);
      }
      LOG.info("Waiting iterator is free");
      super.seek(range, columnFamilies, inclusive);
    }

  }

  private static final Logger LOG = LoggerFactory.getLogger(SlowMemoryConsumingIterator.class);

  private static final CountDownLatch LATCH = new CountDownLatch(1);
  private static final int MEMORY_BUFFER_SIZE = 36 * 1024 * 1024;

  private int getAmountToConsume() {
    System.gc();
    Runtime runtime = Runtime.getRuntime();
    long maxConfiguredMemory = runtime.maxMemory();
    long allocatedMemory = runtime.totalMemory();
    long allocatedFreeMemory = runtime.freeMemory();
    long freeMem = (maxConfiguredMemory - (allocatedMemory - allocatedFreeMemory));
    if (freeMem > Integer.MAX_VALUE) {
      throw new IllegalStateException(
          "Unsupported memory size for tablet server when using this iterator");
    }
    int free = (int) freeMem;
    return (free - MEMORY_BUFFER_SIZE);
  }

  @Override
  public void next() throws IOException {
    LOG.info("next called");
    int amountToConsume = getAmountToConsume();
    LOG.info("allocating memory: " + amountToConsume);
    byte[] buffer = new byte[amountToConsume];
    LOG.info("memory allocated");
    LATCH.countDown();
    Arrays.fill(buffer, (byte) '1');
    try {
      LOG.info("sleeping");
      Uninterruptibles.sleepUninterruptibly(30, TimeUnit.MILLISECONDS);
      super.next();
      LOG.info("exiting next");
    } finally {
      buffer = null;
      LOG.info("next complete");
    }
  }

}

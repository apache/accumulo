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

import static java.util.concurrent.TimeUnit.SECONDS;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.iterators.WrappingIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

public class MemoryConsumingIterator extends WrappingIterator {

  private static final Logger LOG = LoggerFactory.getLogger(MemoryConsumingIterator.class);

  private static final List<byte[]> BUFFERS = new ArrayList<>();

  public static void freeBuffers() {
    BUFFERS.clear();
  }

  @SuppressFBWarnings(value = "DM_GC", justification = "gc is okay for test")
  private int getAmountToConsume() {
    System.gc();
    Runtime runtime = Runtime.getRuntime();
    long maxConfiguredMemory = runtime.maxMemory();
    long usedMemory = runtime.totalMemory() - runtime.freeMemory();
    long freeMemory = maxConfiguredMemory - usedMemory;
    long minFreeMemory = (long) (maxConfiguredMemory * MemoryStarvedScanIT.FREE_MEMORY_THRESHOLD);

    // consume free memory, and exceed the minimum threshold by just a little bit
    // don't exceed typical JDK byte array limit
    long amountToConsume =
        Math.min(Math.max(0, freeMemory + 1 - minFreeMemory), Integer.MAX_VALUE - 8);
    LOG.info("max: {}, used: {}, free: {}, minFree: {}, amountToConsume: {}", maxConfiguredMemory,
        usedMemory, freeMemory, minFreeMemory, amountToConsume);
    return (int) amountToConsume;
  }

  public void consume() throws IOException {
    while (!this.isRunningLowOnMemory()) {
      int amountToConsume = getAmountToConsume();
      if (amountToConsume > 0) {
        LOG.info("allocating memory: " + amountToConsume);
        BUFFERS.add(new byte[amountToConsume]);
        LOG.info("memory allocated");
      } else {
        LOG.info("consumed enough; no more memory allocated");
      }
      LOG.info("Waiting for LowMemoryDetector to recognize low on memory condition.");
      try {
        Thread.sleep(SECONDS.toMillis(5));
      } catch (InterruptedException ex) {
        Thread.currentThread().interrupt();
        throw new IOException("interrupted during sleep", ex);
      }
    }
    LOG.info("Running low on memory == true");
  }

  @Override
  public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive)
      throws IOException {
    LOG.info("seek called");
    consume();
    super.seek(range, columnFamilies, inclusive);
  }

}

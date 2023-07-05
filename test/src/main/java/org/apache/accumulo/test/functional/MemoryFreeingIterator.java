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

import org.apache.accumulo.core.iterators.WrappingIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

public class MemoryFreeingIterator extends WrappingIterator {

  private static final Logger LOG = LoggerFactory.getLogger(MemoryFreeingIterator.class);

  @SuppressFBWarnings(value = "DM_GC", justification = "gc is okay for test")
  public MemoryFreeingIterator() throws InterruptedException {
    LOG.info("Try to free consumed memory - will block until isRunningLowOnMemory returns false.");
    MemoryConsumingIterator.freeBuffers();
    while (this.isRunningLowOnMemory()) {
      System.gc();
      // wait for LowMemoryDetector to recognize the memory is free.
      Thread.sleep(SECONDS.toMillis(1));
    }
    LOG.info("isRunningLowOnMemory returned false - memory available");
  }
}

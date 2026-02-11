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
import java.util.Map;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.WrappingIterator;
import org.apache.accumulo.core.util.UtilWaitThread;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

public class MemoryFreeingIterator extends WrappingIterator {

  private static final Logger LOG = LoggerFactory.getLogger(MemoryFreeingIterator.class);

  @Override
  @SuppressFBWarnings(value = "DM_GC", justification = "gc is okay for test")
  public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options,
      IteratorEnvironment env) throws IOException {
    super.init(source, options, env);
    LOG.info("Try to free consumed memory - will block until isRunningLowOnMemory returns false.");
    MemoryConsumingIterator.freeBuffers();
    System.gc();
    while (this.isRunningLowOnMemory()) {
      LOG.info("Waiting for runningLowOnMemory to return false");
      System.gc();
      // wait for LowMemoryDetector to recognize the memory is free.
      UtilWaitThread.sleep(1000);
    }
    LOG.info("isRunningLowOnMemory returned false - memory available");
  }

}

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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SlowMemoryConsumingIterator extends SlowIterator {

  private static final Logger LOG = LoggerFactory.getLogger(SlowMemoryConsumingIterator.class);
  
  @Override
  public void next() throws IOException {
    
    LOG.info("next called");
    
    System.gc();
    long freeMem = Runtime.getRuntime().freeMemory();
    if (freeMem > Integer.MAX_VALUE) {
      throw new IOException("Unsupported memory size for tablet server when using this iterator");
    }
    int free = (int) freeMem;
    int fiveMegs = 5 * 1024 * 1024;
    int amountToConsume = free - fiveMegs;

    LOG.info("allocating memory: " + amountToConsume);
    byte[] buffer = new byte[amountToConsume];
    Arrays.fill(buffer, (byte) '1');
    try {
      LOG.info("sleeping");
      super.next();
      LOG.info("exiting next");
    } finally {
      buffer = null;
    }
  }

}

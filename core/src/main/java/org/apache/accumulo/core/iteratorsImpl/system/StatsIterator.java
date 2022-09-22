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
package org.apache.accumulo.core.iteratorsImpl.system;

import java.io.IOException;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.ServerWrappingIterator;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;

public class StatsIterator extends ServerWrappingIterator {

  private int numRead = 0;
  private AtomicLong seekCounter;
  private AtomicLong scanCounter;
  private LongAdder serverScanCounter;

  public StatsIterator(SortedKeyValueIterator<Key,Value> source, AtomicLong seekCounter,
      AtomicLong tabletScanCounter, LongAdder serverScanCounter) {
    super(source);
    this.seekCounter = seekCounter;
    this.scanCounter = tabletScanCounter;
    this.serverScanCounter = serverScanCounter;
  }

  @Override
  public void next() throws IOException {
    source.next();
    numRead++;

    if (numRead % 23 == 0) {
      scanCounter.addAndGet(numRead);
      serverScanCounter.add(numRead);
      numRead = 0;
    }
  }

  @Override
  public SortedKeyValueIterator<Key,Value> deepCopy(IteratorEnvironment env) {
    return new StatsIterator(source.deepCopy(env), seekCounter, scanCounter, serverScanCounter);
  }

  @Override
  public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive)
      throws IOException {
    source.seek(range, columnFamilies, inclusive);
    seekCounter.incrementAndGet();
    scanCounter.addAndGet(numRead);
    serverScanCounter.add(numRead);
    numRead = 0;
  }

  public void report() {
    scanCounter.addAndGet(numRead);
    serverScanCounter.add(numRead);
    numRead = 0;
  }
}

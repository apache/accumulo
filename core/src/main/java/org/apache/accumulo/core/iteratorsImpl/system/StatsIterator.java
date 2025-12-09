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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
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
  private final AtomicLong scanSeekCounter;
  private final LongAdder serverSeekCounter;
  private final AtomicLong scanCounter;
  private final LongAdder tabletScanCounter;
  private final LongAdder serverScanCounter;
  private final List<StatsIterator> deepCopies = Collections.synchronizedList(new ArrayList<>());

  public StatsIterator(SortedKeyValueIterator<Key,Value> source, AtomicLong scanSeekCounter,
      LongAdder serverSeekCounter, AtomicLong scanCounter, LongAdder tabletScanCounter,
      LongAdder serverScanCounter) {
    super(source);
    this.scanSeekCounter = scanSeekCounter;
    this.serverSeekCounter = serverSeekCounter;
    this.scanCounter = scanCounter;
    this.tabletScanCounter = tabletScanCounter;
    this.serverScanCounter = serverScanCounter;
  }

  @Override
  public void next() throws IOException {
    source.next();
    numRead++;

    if (numRead % 1009 == 0) {
      // only report on self, do not force deep copies to report
      report(false);
    }
  }

  @Override
  public SortedKeyValueIterator<Key,Value> deepCopy(IteratorEnvironment env) {
    var deepCopy = new StatsIterator(source.deepCopy(env), scanSeekCounter, serverSeekCounter,
        scanCounter, tabletScanCounter, serverScanCounter);
    deepCopies.add(deepCopy);
    return deepCopy;
  }

  @Override
  public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive)
      throws IOException {
    source.seek(range, columnFamilies, inclusive);
    serverSeekCounter.increment();
    scanSeekCounter.incrementAndGet();
    // only report on self, do not force deep copies to report
    report(false);
  }

  public void report(boolean reportDeepCopies) {
    if (numRead > 0) {
      scanCounter.addAndGet(numRead);
      tabletScanCounter.add(numRead);
      serverScanCounter.add(numRead);
      numRead = 0;
    }

    if (reportDeepCopies) {
      // recurse down the fat tree of deep copies forcing them to report
      for (var deepCopy : deepCopies) {
        deepCopy.report(true);
      }
    }
  }
}

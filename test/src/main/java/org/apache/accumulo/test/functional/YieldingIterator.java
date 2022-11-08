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
import java.util.Collection;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.WrappingIterator;
import org.apache.accumulo.core.iterators.YieldCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This iterator which implements yielding will yield after every other next and every other seek
 * call.
 */
public class YieldingIterator extends WrappingIterator {
  private static final Logger log = LoggerFactory.getLogger(YieldingIterator.class);
  private static final AtomicInteger yieldNexts = new AtomicInteger(0);
  private static final AtomicInteger yieldSeeks = new AtomicInteger(0);
  private static final AtomicInteger rebuilds = new AtomicInteger(0);

  private static final AtomicBoolean yieldNextKey = new AtomicBoolean(false);
  private static final AtomicBoolean yieldSeekKey = new AtomicBoolean(false);

  private Optional<YieldCallback<Key>> yield = Optional.empty();

  @Override
  public SortedKeyValueIterator<Key,Value> deepCopy(IteratorEnvironment env) {
    YieldingIterator it = new YieldingIterator();
    it.setSource(getSource().deepCopy(env));
    return it;
  }

  @Override
  public boolean hasTop() {
    return (!(yield.isPresent() && yield.get().hasYielded()) && super.hasTop());
  }

  @Override
  public void next() throws IOException {
    log.info("start YieldingIterator.next: " + getTopValue());
    boolean yielded = false;

    // yield on every other next call.
    yieldNextKey.set(!yieldNextKey.get());
    if (yield.isPresent() && yieldNextKey.get()) {
      yielded = true;
      yieldNexts.incrementAndGet();
      // since we are not actually skipping keys underneath, simply use the key following the top
      // key as the yield key
      yield.get().yield(getTopKey().followingKey(PartialKey.ROW_COLFAM_COLQUAL_COLVIS_TIME));
      log.info("end YieldingIterator.next: yielded at " + getTopKey());
    }

    // if not yielding, then simply pass on the next call
    if (!yielded) {
      super.next();
      log.info("end YieldingIterator.next: "
          + (hasTop() ? getTopKey() + " " + getTopValue() : "no top"));
    }
  }

  /**
   * The top value will encode the current state of the yields, seeks, and rebuilds for use by the
   * YieldScannersIT tests.
   *
   * @return a top value of the form {yieldNexts},{yieldSeeks},{rebuilds}
   */
  @Override
  public Value getTopValue() {
    String value = yieldNexts.get() + "," + yieldSeeks.get() + "," + rebuilds.get();
    return new Value(value);
  }

  @Override
  public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive)
      throws IOException {
    log.info("start YieldingIterator.seek: " + getTopValue() + " with range " + range);
    boolean yielded = false;

    if (range.isStartKeyInclusive()) {
      // must be a new scan so re-initialize the counters
      log.info("resetting counters");
      resetCounters();
    } else {
      rebuilds.incrementAndGet();
    }

    if (range.getStartKey() != null) {
      // yield on every other seek call.
      yieldSeekKey.set(!yieldSeekKey.get());
      if (yield.isPresent() && yieldSeekKey.get()) {
        yielded = true;
        yieldSeeks.incrementAndGet();
        // since we are not actually skipping keys underneath, simply use the key following the
        // range start key
        if (range.isStartKeyInclusive()) {
          yield.get().yield(range.getStartKey());
        } else {
          yield.get()
              .yield(range.getStartKey().followingKey(PartialKey.ROW_COLFAM_COLQUAL_COLVIS_TIME));
        }
        log.info("end YieldingIterator.next: yielded at " + range.getStartKey());
      }
    }

    // if not yielding, then simply pass on the call to the source
    if (!yielded) {
      super.seek(range, columnFamilies, inclusive);
      log.info("end YieldingIterator.seek: "
          + (hasTop() ? getTopKey() + " " + getTopValue() : "no top"));
    }
  }

  @Override
  public void enableYielding(YieldCallback<Key> yield) {
    this.yield = Optional.of(yield);
  }

  protected void resetCounters() {
    yieldNexts.set(0);
    yieldSeeks.set(0);
    rebuilds.set(0);
    yieldNextKey.set(false);
    yieldSeekKey.set(false);
  }
}

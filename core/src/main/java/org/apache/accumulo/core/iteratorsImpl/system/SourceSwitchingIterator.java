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
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.YieldCallback;

/**
 * A SortedKeyValueIterator which presents a view over some section of data, regardless of whether
 * or not it is backed by memory (InMemoryMap) or an RFile (InMemoryMap that was minor compacted to
 * a file). Clients reading from a table that has data in memory should not see interruption in
 * their scan when that data is minor compacted. This iterator is designed to manage this behind the
 * scene.
 */
public class SourceSwitchingIterator implements InterruptibleIterator {

  public interface DataSource {
    boolean isCurrent();

    DataSource getNewDataSource();

    DataSource getDeepCopyDataSource(IteratorEnvironment env);

    SortedKeyValueIterator<Key,Value> iterator() throws IOException;

    void setInterruptFlag(AtomicBoolean flag);

    default void close(boolean sawErrors) {}
  }

  private DataSource source;
  private SortedKeyValueIterator<Key,Value> iter;

  private Optional<YieldCallback<Key>> yield = Optional.empty();

  private Key key;
  private Value val;

  private Range range;
  private boolean inclusive;
  private Collection<ByteSequence> columnFamilies;

  private boolean onlySwitchAfterRow;

  // Synchronization on copies synchronizes operations across all deep copies of this instance.
  //
  // This implementation assumes that there is one thread reading data (a scan) from all deep copies
  // and that another thread may call switch at any point. A single scan may have multiple deep
  // copies of this iterator if other iterators above this one duplicate their source. For example,
  // if an IntersectingIterator over two columns was configured, `copies` would contain two SSIs
  // instead of just one SSI. The two instances in `copies` would both be at the same "level"
  // in the tree of iterators for the scan. If multiple instances of SSI are configure in the
  // iterator
  // tree (e.g. priority 8 and priority 12), each instance would share their own `copies` e.g.
  // SSI@priority8:copies1[...], SSI@priority12:copies2[...]

  private final List<SourceSwitchingIterator> copies;

  private SourceSwitchingIterator(DataSource source, boolean onlySwitchAfterRow,
      List<SourceSwitchingIterator> copies) {
    this.source = source;
    this.onlySwitchAfterRow = onlySwitchAfterRow;
    this.copies = copies;
    copies.add(this);
  }

  public SourceSwitchingIterator(DataSource source, boolean onlySwitchAfterRow) {
    this(source, onlySwitchAfterRow, new ArrayList<>());
  }

  public SourceSwitchingIterator(DataSource source) {
    this(source, false);
  }

  @Override
  public SortedKeyValueIterator<Key,Value> deepCopy(IteratorEnvironment env) {
    synchronized (copies) {
      return new SourceSwitchingIterator(source.getDeepCopyDataSource(env), onlySwitchAfterRow,
          copies);
    }
  }

  @Override
  public Key getTopKey() {
    return key;
  }

  @Override
  public Value getTopValue() {
    return val;
  }

  @Override
  public boolean hasTop() {
    return key != null;
  }

  @Override
  public void enableYielding(YieldCallback<Key> yield) {
    this.yield = Optional.of(yield);

    // if we require row isolation, then we cannot support yielding in the middle.
    if (!onlySwitchAfterRow) {
      if (iter != null) {
        iter.enableYielding(yield);
      }
    }
  }

  @Override
  public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options,
      IteratorEnvironment env) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void next() throws IOException {
    synchronized (copies) {
      readNext(false);
    }
  }

  private void readNext(boolean initialSeek) throws IOException {

    // we need to check here if we were yielded in case the source was switched out and re-seeked by
    // someone else (minor compaction/InMemoryMap)
    boolean yielded = (yield.isPresent() && yield.get().hasYielded());

    // check of initialSeek second is intentional so that it does not short
    // circuit the call to switchSource
    boolean seekNeeded = yielded || (!onlySwitchAfterRow && switchSource()) || initialSeek;

    if (seekNeeded) {
      if (initialSeek) {
        iter.seek(range, columnFamilies, inclusive);
      } else if (yielded) {
        Key yieldPosition = yield.get().getPositionAndReset();
        if (!range.contains(yieldPosition)) {
          throw new IOException("Underlying iterator yielded to a position outside of its range: "
              + yieldPosition + " not in " + range);
        }
        iter.seek(new Range(yieldPosition, false, range.getEndKey(), range.isEndKeyInclusive()),
            columnFamilies, inclusive);
      } else {
        iter.seek(new Range(key, false, range.getEndKey(), range.isEndKeyInclusive()),
            columnFamilies, inclusive);
      }
    } else {
      iter.next();
      if (onlySwitchAfterRow && iter.hasTop() && !source.isCurrent()
          && !key.getRowData().equals(iter.getTopKey().getRowData())) {
        switchSource();
        iter.seek(new Range(key.followingKey(PartialKey.ROW), true, range.getEndKey(),
            range.isEndKeyInclusive()), columnFamilies, inclusive);
      }
    }

    if (iter.hasTop()) {
      if (yield.isPresent() && yield.get().hasYielded()) {
        throw new IOException("Coding error: hasTop returned true but has yielded at "
            + yield.get().getPositionAndReset());
      }

      Key nextKey = iter.getTopKey();
      Value nextVal = iter.getTopValue();

      try {
        key = (Key) nextKey.clone();
      } catch (CloneNotSupportedException e) {
        throw new IOException(e);
      }
      val = nextVal;
    } else {
      key = null;
      val = null;
    }
  }

  private boolean switchSource() throws IOException {
    if (!source.isCurrent()) {
      source = source.getNewDataSource();
      iter = source.iterator();
      if (!onlySwitchAfterRow && yield.isPresent()) {
        iter.enableYielding(yield.get());
      }
      return true;
    }

    return false;
  }

  @Override
  public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive)
      throws IOException {
    synchronized (copies) {
      this.range = range;
      this.inclusive = inclusive;
      this.columnFamilies = columnFamilies;

      if (iter == null) {
        iter = source.iterator();
        if (!onlySwitchAfterRow && yield.isPresent()) {
          iter.enableYielding(yield.get());
        }
      }

      readNext(true);
    }
  }

  private void _switchNow() throws IOException {
    if (onlySwitchAfterRow) {
      throw new IllegalStateException("Can only switch on row boundaries");
    }

    if (switchSource()) {
      if (key != null) {
        iter.seek(new Range(key, true, range.getEndKey(), range.isEndKeyInclusive()),
            columnFamilies, inclusive);
      }
    }
  }

  public void switchNow() throws IOException {
    synchronized (copies) {
      for (SourceSwitchingIterator ssi : copies) {
        ssi._switchNow();
      }
    }
  }

  @Override
  public void setInterruptFlag(AtomicBoolean flag) {
    synchronized (copies) {
      if (copies.size() != 1) {
        throw new IllegalStateException(
            "setInterruptFlag() called after deep copies made " + copies.size());
      }

      if (iter != null) {
        ((InterruptibleIterator) iter).setInterruptFlag(flag);
      }

      source.setInterruptFlag(flag);
    }
  }
}

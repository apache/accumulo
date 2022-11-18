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
import java.util.PriorityQueue;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;

/**
 * Constructs a {@link PriorityQueue} of multiple SortedKeyValueIterators. Provides a simple way to
 * interact with multiple SortedKeyValueIterators in sorted order.
 */
public abstract class HeapIterator implements SortedKeyValueIterator<Key,Value> {
  private PriorityQueue<SortedKeyValueIterator<Key,Value>> heap;
  private SortedKeyValueIterator<Key,Value> topIdx = null;
  private Key nextKey;

  protected HeapIterator() {
    heap = null;
  }

  protected HeapIterator(int maxSize) {
    createHeap(maxSize);
  }

  protected void createHeap(int maxSize) {
    if (heap != null) {
      throw new IllegalStateException("heap already exist");
    }

    heap = new PriorityQueue<>(maxSize == 0 ? 1 : maxSize,
        (si1, si2) -> si1.getTopKey().compareTo(si2.getTopKey()));
  }

  @Override
  public final Key getTopKey() {
    return topIdx.getTopKey();
  }

  @Override
  public final Value getTopValue() {
    return topIdx.getTopValue();
  }

  @Override
  public final boolean hasTop() {
    return topIdx != null;
  }

  @Override
  public final void next() throws IOException {
    if (topIdx == null) {
      throw new IllegalStateException("Called next() when there is no top");
    }

    topIdx.next();
    if (topIdx.hasTop()) {
      if (nextKey == null) {
        // topIdx is the only iterator
        return;
      }

      if (nextKey.compareTo(topIdx.getTopKey()) < 0) {
        // Grab the next top iterator and put the current top iterator back on the heap
        // This updating of references is special-cased to save on percolation on edge cases
        // since the current top is guaranteed to not be the minimum
        SortedKeyValueIterator<Key,Value> nextTopIdx = heap.remove();
        heap.add(topIdx);

        topIdx = nextTopIdx;
        nextKey = heap.peek().getTopKey();
      }
    } else {
      if (nextKey == null) {
        // No iterators left
        topIdx = null;
        return;
      }

      pullReferencesFromHeap();
    }
  }

  private void pullReferencesFromHeap() {
    topIdx = heap.remove();
    if (heap.isEmpty()) {
      nextKey = null;
    } else {
      nextKey = heap.peek().getTopKey();
    }
  }

  protected final void clear() {
    heap.clear();
    topIdx = null;
    nextKey = null;
  }

  protected final void addSource(SortedKeyValueIterator<Key,Value> source) {
    if (source.hasTop()) {
      heap.add(source);
      if (topIdx != null) {
        heap.add(topIdx);
      }

      pullReferencesFromHeap();
    }
  }
}

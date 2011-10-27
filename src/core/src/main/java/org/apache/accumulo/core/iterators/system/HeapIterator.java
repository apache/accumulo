/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.accumulo.core.iterators.system;

import java.io.IOException;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.commons.collections.buffer.PriorityBuffer;

public abstract class HeapIterator implements SortedKeyValueIterator<Key,Value> {
  private PriorityBuffer heap;
  private SortedKeyValueIterator<Key,Value> currentIter;
  
  private static class Index implements Comparable<Index> {
    SortedKeyValueIterator<Key,Value> iter;
    
    public Index(SortedKeyValueIterator<Key,Value> iter) {
      this.iter = iter;
    }
    
    public int compareTo(Index o) {
      return iter.getTopKey().compareTo(o.iter.getTopKey());
    }
  }
  
  protected HeapIterator() {
    heap = null;
  }
  
  protected HeapIterator(int maxSize) {
    createHeap(maxSize);
  }
  
  protected void createHeap(int maxSize) {
    if (heap != null)
      throw new IllegalStateException("heap already exist");
    
    heap = new PriorityBuffer(maxSize == 0 ? 1 : maxSize);
  }
  
  @Override
  final public Key getTopKey() {
    return currentIter.getTopKey();
  }
  
  @Override
  final public Value getTopValue() {
    return currentIter.getTopValue();
  }
  
  @Override
  final public boolean hasTop() {
    return heap.size() > 0;
  }
  
  @Override
  final public void next() throws IOException {
    switch (heap.size()) {
      case 0:
        throw new IllegalStateException("Called next() when there is no top");
      case 1:
        // optimization for case when heap contains one entry,
        // avoids remove and add
        currentIter.next();
        if (!currentIter.hasTop()) {
          heap.remove();
          currentIter = null;
        }
        break;
      default:
        Index idx = (Index) heap.remove();
        idx.iter.next();
        if (idx.iter.hasTop()) {
          heap.add(idx);
        }
        // to get to the default case heap has at least
        // two entries, therefore there must be at least
        // one entry when get() is called below
        currentIter = ((Index) heap.get()).iter;
    }
  }
  
  final protected void clear() {
    heap.clear();
    currentIter = null;
  }
  
  final protected void addSource(SortedKeyValueIterator<Key,Value> source) {
    
    if (source.hasTop())
      heap.add(new Index(source));
    
    if (heap.size() > 0)
      currentIter = ((Index) heap.get()).iter;
    else
      currentIter = null;
  }
  
}

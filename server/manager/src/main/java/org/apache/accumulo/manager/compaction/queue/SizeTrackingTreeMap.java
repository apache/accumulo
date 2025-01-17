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
package org.apache.accumulo.manager.compaction.queue;

import java.util.AbstractMap;
import java.util.Map;
import java.util.TreeMap;

import com.google.common.base.Preconditions;

/**
 * This class wraps a treemap and tracks the data size of everything added and removed from the
 * treemap.
 */
class SizeTrackingTreeMap<K,V> {

  private static class ValueWrapper<V2> {
    final V2 val;
    final long computedSize;

    private ValueWrapper(V2 val, long computedSize) {
      this.val = val;
      this.computedSize = computedSize;
    }
  }

  private final TreeMap<K,ValueWrapper<V>> map = new TreeMap<>();
  private long dataSize = 0;
  private Weigher<V> weigher;

  private Map.Entry<K,V> unwrap(Map.Entry<K,ValueWrapper<V>> wrapperEntry) {
    if (wrapperEntry == null) {
      return null;
    }
    return new AbstractMap.SimpleImmutableEntry<>(wrapperEntry.getKey(),
        wrapperEntry.getValue().val);
  }

  private void incrementDataSize(ValueWrapper<V> val) {
    Preconditions.checkState(dataSize >= 0);
    dataSize += val.computedSize;
  }

  private void decrementDataSize(Map.Entry<K,ValueWrapper<V>> entry) {
    if (entry != null) {
      decrementDataSize(entry.getValue());
    }
  }

  private void decrementDataSize(ValueWrapper<V> val) {
    if (val != null) {
      Preconditions.checkState(dataSize >= val.computedSize);
      dataSize -= val.computedSize;
    }
  }

  interface Weigher<V2> {
    long weigh(V2 val);
  }

  public SizeTrackingTreeMap(Weigher<V> weigher) {
    this.weigher = weigher;
  }

  public boolean isEmpty() {
    return map.isEmpty();
  }

  public long dataSize() {
    return dataSize;
  }

  public int entrySize() {
    return map.size();
  }

  public K lastKey() {
    return map.lastKey();
  }

  public Map.Entry<K,V> firstEntry() {
    return unwrap(map.firstEntry());
  }

  public void remove(K key) {
    var prev = map.remove(key);
    decrementDataSize(prev);
  }

  public Map.Entry<K,V> pollFirstEntry() {
    var first = map.pollFirstEntry();
    decrementDataSize(first);
    return unwrap(first);
  }

  public Map.Entry<K,V> pollLastEntry() {
    var last = map.pollLastEntry();
    decrementDataSize(last);
    return unwrap(last);
  }

  public void put(K key, V val) {
    var wrapped = new ValueWrapper<>(val, weigher.weigh(val));
    var prev = map.put(key, wrapped);
    decrementDataSize(prev);
    incrementDataSize(wrapped);
  }

  public void clear() {
    map.clear();
    dataSize = 0;
  }
}

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
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.KeyExtent;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;

/**
 * An iterator capable of iterating over other iterators in sorted order.
 *
 *
 *
 */

public class MultiIterator extends HeapIterator {

  private List<SortedKeyValueIterator<Key,Value>> iters;
  private Range fence;

  // deep copy with no seek/scan state
  public MultiIterator deepCopy(IteratorEnvironment env) {
    return new MultiIterator(this, env);
  }

  private MultiIterator(MultiIterator other, IteratorEnvironment env) {
    super(other.iters.size());
    this.iters = new ArrayList<SortedKeyValueIterator<Key,Value>>();
    this.fence = other.fence;
    for (SortedKeyValueIterator<Key,Value> iter : other.iters) {
      iters.add(iter.deepCopy(env));
    }
  }

  private void init() {
    for (SortedKeyValueIterator<Key,Value> skvi : iters)
      addSource(skvi);
  }

  private MultiIterator(List<SortedKeyValueIterator<Key,Value>> iters, Range seekFence, boolean init) {
    super(iters.size());

    if (seekFence != null && init) {
      // throw this exception because multi-iterator does not seek on init, therefore the
      // fence would not be enforced in anyway, so do not want to give the impression it
      // will enforce this
      throw new IllegalArgumentException("Initializing not supported when seek fence set");
    }

    this.fence = seekFence;
    this.iters = iters;

    if (init) {
      init();
    }
  }

  public MultiIterator(List<SortedKeyValueIterator<Key,Value>> iters, Range seekFence) {
    this(iters, seekFence, false);
  }

  public MultiIterator(List<SortedKeyValueIterator<Key,Value>> iters2, KeyExtent extent) {
    this(iters2, new Range(extent.getPrevEndRow(), false, extent.getEndRow(), true), false);
  }

  public MultiIterator(List<SortedKeyValueIterator<Key,Value>> readers, boolean init) {
    this(readers, (Range) null, init);
  }

  @Override
  public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {
    clear();

    if (fence != null) {
      range = fence.clip(range, true);
      if (range == null)
        return;
    }

    for (SortedKeyValueIterator<Key,Value> skvi : iters) {
      skvi.seek(range, columnFamilies, inclusive);
      addSource(skvi);
    }
  }

  @Override
  public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options, IteratorEnvironment env) throws IOException {
    throw new UnsupportedOperationException();
  }
}

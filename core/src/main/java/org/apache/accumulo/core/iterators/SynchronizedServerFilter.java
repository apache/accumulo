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
package org.apache.accumulo.core.iterators;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;

/**
 * A SortedKeyValueIterator that filters entries from its source iterator.
 *
 * Subclasses must implement an accept method: public boolean accept(Key k, Value v);
 *
 * Key/Value pairs for which the accept method returns true are said to match the filter. By default, this class iterates over entries that match its filter.
 * This iterator takes an optional "negate" boolean parameter that defaults to false. If negate is set to true, this class instead omits entries that match its
 * filter, thus iterating over entries that do not match its filter.
 */
public abstract class SynchronizedServerFilter implements SortedKeyValueIterator<Key,Value> {

  protected final SortedKeyValueIterator<Key,Value> source;

  public SynchronizedServerFilter(SortedKeyValueIterator<Key,Value> source) {
    this.source = source;
  }

  @Override
  public abstract SortedKeyValueIterator<Key,Value> deepCopy(IteratorEnvironment env);

  @Override
  public synchronized void next() throws IOException {
    source.next();
    findTop();
  }

  @Override
  public synchronized void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {
    source.seek(range, columnFamilies, inclusive);
    findTop();
  }

  @Override
  public synchronized Key getTopKey() {
    return source.getTopKey();
  }

  @Override
  public synchronized Value getTopValue() {
    return source.getTopValue();
  }

  @Override
  public synchronized boolean hasTop() {
    return source.hasTop();
  }

  /**
   * Iterates over the source until an acceptable key/value pair is found.
   */
  private void findTop() throws IOException {
    while (source.hasTop()) {
      Key top = source.getTopKey();
      if (top.isDeleted() || (accept(top, source.getTopValue()))) {
        break;
      }
      source.next();
    }
  }

  /**
   * @return <tt>true</tt> if the key/value pair is accepted by the filter.
   */
  protected abstract boolean accept(Key k, Value v);

  @Override
  public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options, IteratorEnvironment env) throws IOException {
    throw new UnsupportedOperationException();
  }
}

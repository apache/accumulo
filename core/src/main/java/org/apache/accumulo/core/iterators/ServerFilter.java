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
package org.apache.accumulo.core.iterators;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;

/**
 * An optimized version of {@link org.apache.accumulo.core.iterators.Filter}. This class grants
 * protected access to the read only <code>source</code> iterator. For performance reasons, the
 * <code>source</code> iterator is declared final and subclasses directly access it, no longer
 * requiring calls to getSource(). The
 * {@link #init(SortedKeyValueIterator, Map, IteratorEnvironment)} method is not supported since the
 * source can only be assigned in the constructor.
 *
 * @since 2.0
 */
public abstract class ServerFilter extends ServerWrappingIterator {

  public ServerFilter(SortedKeyValueIterator<Key,Value> source) {
    super(source);
  }

  @Override
  public abstract SortedKeyValueIterator<Key,Value> deepCopy(IteratorEnvironment env);

  @Override
  public void next() throws IOException {
    source.next();
    findTop();
  }

  @Override
  public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive)
      throws IOException {
    source.seek(range, columnFamilies, inclusive);
    findTop();
  }

  /**
   * Iterates over the source until an acceptable key/value pair is found.
   */
  private void findTop() throws IOException {
    while (source.hasTop()) {
      Key top = source.getTopKey();
      if (top.isDeleted() || accept(top, source.getTopValue())) {
        break;
      }
      source.next();
    }
  }

  /**
   * @return <code>true</code> if the key/value pair is accepted by the filter.
   */
  public abstract boolean accept(Key k, Value v);

  @Override
  public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options,
      IteratorEnvironment env) throws IOException {
    throw new UnsupportedOperationException();
  }
}

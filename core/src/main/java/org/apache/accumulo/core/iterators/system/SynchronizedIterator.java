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
import java.util.Collection;
import java.util.Map;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

/**
 * Wraps a SortedKeyValueIterator so that all of its methods are synchronized. The intent is that user iterators which are multi-threaded have the possibility
 * to call parent methods concurrently. The SynchronizedIterators aims to reduce the likelihood of unwanted concurrent access.
 */
public class SynchronizedIterator<K extends WritableComparable<?>,V extends Writable> implements SortedKeyValueIterator<K,V> {

  private final SortedKeyValueIterator<K,V> source;

  @Override
  public synchronized void init(SortedKeyValueIterator<K,V> source, Map<String,String> options, IteratorEnvironment env) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public synchronized boolean hasTop() {
    return source.hasTop();
  }

  @Override
  public synchronized void next() throws IOException {
    source.next();
  }

  @Override
  public synchronized void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {
    source.seek(range, columnFamilies, inclusive);
  }

  @Override
  public synchronized K getTopKey() {
    return source.getTopKey();
  }

  @Override
  public synchronized V getTopValue() {
    return source.getTopValue();
  }

  @Override
  public synchronized SortedKeyValueIterator<K,V> deepCopy(IteratorEnvironment env) {
    return new SynchronizedIterator<>(source.deepCopy(env));
  }

  public SynchronizedIterator(SortedKeyValueIterator<K,V> source) {
    this.source = source;
  }
}

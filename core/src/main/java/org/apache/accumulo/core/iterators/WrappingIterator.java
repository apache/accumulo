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
 * A convenience class for implementing iterators that select, but do not modify, entries read from a source iterator. Default implementations exist for all
 * methods, but {@link #deepCopy} will throw an <code>UnsupportedOperationException</code>.
 *
 * This iterator has some checks in place to enforce the iterator contract. Specifically, it verifies that it has a source iterator and that {@link #seek} has
 * been called before any data is read. If either of these conditions does not hold true, an <code>IllegalStateException</code> will be thrown. In particular,
 * this means that <code>getSource().seek</code> and <code>super.seek</code> no longer perform identical actions. Implementors should take note of this and if
 * <code>seek</code> is overridden, ensure that <code>super.seek</code> is called before data is read.
 */
public abstract class WrappingIterator implements SortedKeyValueIterator<Key,Value> {

  private SortedKeyValueIterator<Key,Value> source = null;
  boolean seenSeek = false;

  protected void setSource(SortedKeyValueIterator<Key,Value> source) {
    this.source = source;
  }

  protected SortedKeyValueIterator<Key,Value> getSource() {
    if (source == null)
      throw new IllegalStateException("getting null source");
    return source;
  }

  @Override
  public SortedKeyValueIterator<Key,Value> deepCopy(IteratorEnvironment env) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Key getTopKey() {
    if (source == null)
      throw new IllegalStateException("no source set");
    if (seenSeek == false)
      throw new IllegalStateException("never been seeked");
    return getSource().getTopKey();
  }

  @Override
  public Value getTopValue() {
    if (source == null)
      throw new IllegalStateException("no source set");
    if (seenSeek == false)
      throw new IllegalStateException("never been seeked");
    return getSource().getTopValue();
  }

  @Override
  public boolean hasTop() {
    if (source == null)
      throw new IllegalStateException("no source set");
    if (seenSeek == false)
      throw new IllegalStateException("never been seeked");
    return getSource().hasTop();
  }

  @Override
  public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options, IteratorEnvironment env) throws IOException {
    this.setSource(source);

  }

  @Override
  public void next() throws IOException {
    if (source == null)
      throw new IllegalStateException("no source set");
    if (seenSeek == false)
      throw new IllegalStateException("never been seeked");
    getSource().next();
  }

  @Override
  public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {
    getSource().seek(range, columnFamilies, inclusive);
    seenSeek = true;
  }

}

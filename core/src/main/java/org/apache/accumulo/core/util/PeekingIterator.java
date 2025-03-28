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
package org.apache.accumulo.core.util;

import java.util.Iterator;
import java.util.function.Predicate;

import com.google.common.base.Preconditions;

public class PeekingIterator<E> implements Iterator<E> {

  boolean isInitialized;
  Iterator<E> source;
  E top;

  public PeekingIterator(Iterator<E> source) {
    this.source = source;
    if (source.hasNext()) {
      top = source.next();
    } else {
      top = null;
    }
    isInitialized = true;
  }

  /**
   * Creates an uninitialized instance. This should be used in conjunction with
   * {@link #initialize(Iterator)}.
   */
  public PeekingIterator() {
    isInitialized = false;
  }

  /**
   * Initializes this iterator, to be used with {@link #PeekingIterator()}.
   */
  public PeekingIterator<E> initialize(Iterator<E> source) {
    this.source = source;
    if (source.hasNext()) {
      top = source.next();
    } else {
      top = null;
    }
    isInitialized = true;
    return this;
  }

  public E peek() {
    if (!isInitialized) {
      throw new IllegalStateException("Iterator has not yet been initialized");
    }
    return top;
  }

  @Override
  public E next() {
    if (!isInitialized) {
      throw new IllegalStateException("Iterator has not yet been initialized");
    }
    E lastPeeked = top;
    if (source.hasNext()) {
      top = source.next();
    } else {
      top = null;
    }
    return lastPeeked;
  }

  @Override
  public void remove() {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean hasNext() {
    if (!isInitialized) {
      throw new IllegalStateException("Iterator has not yet been initialized");
    }
    return top != null;
  }

  /**
   * Advances the underlying iterator looking for a match, inspecting up to {@code limit} elements
   * from the iterator. If this method finds a match to the predicate, then it will return true and
   * will be positioned before the matching element (peek() and next() will return the matching
   * element). If this method does not find a match because the underlying iterator ended before
   * {@code limit}, then it will return false and hasNext will also return false. Otherwise, if this
   * method does not find a match, then it will return false and be positioned before the limit
   * element (peek() and next() will return the {@code limit} element).
   *
   * @param predicate condition that we are looking for, parameter could be null, so the Predicate
   *        implementation needs to handle this.
   * @param limit number of times that we should look for a match, parameter must be a positive
   *        int
   * @return true if an element matched the predicate or false otherwise. When true hasNext() will
   *         return true and peek() and next() will return the matching element. When false
   *         hasNext() may return false if the end has been reached, or hasNext() may return true in
   *         which case peek() and next() will return the element {@code limit} positions ahead of
   *         where this iterator was before this method was called.
   */
  public boolean findWithin(Predicate<E> predicate, int limit) {
    Preconditions.checkArgument(limit > 0);
    for (int i = 0; i < limit; i++) {
      if (predicate.test(peek())) {
        return true;
      } else if (i < (limit - 1)) {
        if (hasNext()) {
          next();
        } else {
          return false;
        }
      }
    }
    return false;
  }
}

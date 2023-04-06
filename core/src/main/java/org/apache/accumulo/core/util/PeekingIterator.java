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
}

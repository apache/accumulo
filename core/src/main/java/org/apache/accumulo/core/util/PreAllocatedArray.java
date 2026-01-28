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

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * A {@link List} implementation that represents a type-safe pre-allocated array. This should be
 * used exactly like an array, but helps avoid type-safety issues when mixing arrays with generics.
 * The iterator is unmodifiable.
 */
public final class PreAllocatedArray<T> implements Iterable<T> {

  private final List<T> internal;
  public final int length;

  /**
   * Creates an instance of the given capacity, with all elements initialized to null
   */
  @SuppressWarnings("unchecked")
  public PreAllocatedArray(final int capacity) {
    internal = Arrays.asList((T[]) new Object[capacity]);
    length = capacity;
  }

  /**
   * Set the element at the specified index, and return the old value.
   */
  public T set(final int index, final T element) {
    return internal.set(index, element);
  }

  /**
   * Get the item stored at the specified index.
   */
  public T get(final int index) {
    return internal.get(index);
  }

  @Override
  public Iterator<T> iterator() {
    return internal.iterator();
  }
}

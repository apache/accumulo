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
package org.apache.accumulo.core.data;

import java.io.Serializable;
import java.util.Objects;

/**
 * An abstract identifier class for comparing equality of identifiers of the same type.
 *
 * @since 2.0.0
 */
public abstract class AbstractId<T extends AbstractId<T>> implements Comparable<T>, Serializable {

  private static final long serialVersionUID = 1L;
  private final String canonical;

  protected AbstractId(final String canonical) {
    this.canonical = Objects.requireNonNull(canonical, "canonical cannot be null");
  }

  /**
   * The canonical ID. This is guaranteed to be non-null.
   */
  public final String canonical() {
    return canonical;
  }

  /**
   * AbstractID objects are considered equal if, and only if, they are of the same type and have the
   * same canonical identifier.
   */
  @Override
  public boolean equals(final Object obj) {
    return this == obj || (obj != null && Objects.equals(getClass(), obj.getClass())
        && Objects.equals(canonical(), ((AbstractId<?>) obj).canonical()));
  }

  @Override
  public int hashCode() {
    return canonical().hashCode();
  }

  /**
   * Returns a string of the canonical ID. This is guaranteed to be non-null.
   */
  @Override
  public final String toString() {
    return canonical();
  }

  @Override
  public int compareTo(T other) {
    return canonical().compareTo(Objects.requireNonNull(other, "other cannot be null").canonical());
  }

}

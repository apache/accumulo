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
package org.apache.accumulo.core.client.impl;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;

import java.io.Serializable;
import java.util.Objects;

/**
 * An abstract identifier class for comparing equality of identifiers of the same type.
 */
public abstract class AbstractId implements Comparable<AbstractId>, Serializable {

  private static final long serialVersionUID = -155513612834787244L;
  private final String canonical;
  private Integer hashCode = null;

  protected AbstractId(final String canonical) {
    requireNonNull(canonical, "canonical cannot be null");
    this.canonical = canonical;
  }

  /**
   * The canonical ID
   */
  public final String canonicalID() {
    return canonical;
  }

  public boolean isEmpty() {
    return canonical.isEmpty();
  }

  /**
   * AbstractID objects are considered equal if, and only if, they are of the same type and have the same canonical identifier.
   */
  @Override
  public boolean equals(final Object obj) {
    if (this == obj)
      return true;
    return obj != null && Objects.equals(getClass(), obj.getClass()) && Objects.equals(canonicalID(), ((AbstractId) obj).canonicalID());
  }

  @Override
  public int hashCode() {
    if (hashCode == null) {
      hashCode = Objects.hash(canonicalID());
    }
    return hashCode;
  }

  /**
   * Returns a string of the canonical ID
   */
  @Override
  public String toString() {
    return canonical;
  }

  /**
   * Return a UTF_8 byte[] of the canonical ID.
   */
  public final byte[] getUtf8() {
    return canonical.getBytes(UTF_8);
  }

  @Override
  public int compareTo(AbstractId id) {
    requireNonNull(id, "id cannot be null");
    return this.canonicalID().compareTo(id.canonicalID());
  }

}

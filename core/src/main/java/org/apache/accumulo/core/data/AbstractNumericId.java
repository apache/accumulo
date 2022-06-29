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
 * An abstract numeric class for comparing equality of identifiers of the same type.
 *
 * @since 2.1.0
 */
public class AbstractNumericId<T extends AbstractNumericId<T>>
    implements Comparable<T>, Serializable {
  private final Number canonical;
  private static final long serialVersionUID = 1L;

  public AbstractNumericId(long canonical) {
    this.canonical = canonical;
  }

  public Long canonical() {
    return canonical.longValue();
  }

  @Override
  public boolean equals(final Object obj) {
    return this == obj || (obj != null && Objects.equals(getClass(), obj.getClass())
        && canonical() == ((FateTxId) obj).canonical());
  }

  @Override
  public int hashCode() {
    return canonical.hashCode();
  }

  @Override
  public int compareTo(T other) {
    if (this.equals(other)) {
      return 0;
    }
    if (other != null) {
      return Long.compare(this.canonical(), other.canonical());
    } else {
      throw new IllegalArgumentException("The FateTxId provided was null.");
    }
  }
}

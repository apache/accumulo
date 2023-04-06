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

import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.Map.Entry;
import java.util.Objects;

public class Pair<A,B> {
  A first;
  B second;

  public Pair(A f, B s) {
    this.first = f;
    this.second = s;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(first) + Objects.hashCode(second);
  }

  @Override
  public boolean equals(Object o) {
    if (o instanceof Pair<?,?>) {
      Pair<?,?> other = (Pair<?,?>) o;
      return Objects.equals(first, other.first) && Objects.equals(second, other.second);
    }
    return false;
  }

  public A getFirst() {
    return first;
  }

  public B getSecond() {
    return second;
  }

  @Override
  public String toString() {
    return toString("(", ",", ")");
  }

  public String toString(String prefix, String separator, String suffix) {
    return prefix + first + separator + second + suffix;
  }

  public Entry<A,B> toMapEntry() {
    return new SimpleImmutableEntry<>(getFirst(), getSecond());
  }

  public Pair<B,A> swap() {
    return new Pair<>(getSecond(), getFirst());
  }

  public static <K2,V2,K1 extends K2,V1 extends V2> Pair<K2,V2> fromEntry(Entry<K1,V1> entry) {
    return new Pair<>(entry.getKey(), entry.getValue());
  }

}

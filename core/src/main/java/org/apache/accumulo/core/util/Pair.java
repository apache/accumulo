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
package org.apache.accumulo.core.util;

public class Pair<A,B> {
  A first;
  B second;
  
  public Pair(A f, B s) {
    this.first = f;
    this.second = s;
  }
  
  private int hashCode(Object o) {
    if (o == null)
      return 0;
    return o.hashCode();
  }
  
  @Override
  public int hashCode() {
    return hashCode(first) + hashCode(second);
  }
  
  private boolean equals(Object o1, Object o2) {
    if (o1 == null || o2 == null)
      return o1 == o2;
    
    return o1.equals(o2);
  }
  
  @Override
  public boolean equals(Object o) {
    if (o instanceof Pair<?,?>) {
      Pair<?,?> op = (Pair<?,?>) o;
      return equals(first, op.first) && equals(second, op.second);
    }
    return false;
  }
  
  public A getFirst() {
    return first;
  }
  
  public B getSecond() {
    return second;
  }
  
  public String toString() {
    return "(" + first + "," + second + ")";
  }
}

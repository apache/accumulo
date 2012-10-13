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
package org.apache.accumulo.server.tabletserver;

import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;

public class SimpleLRUCache<T> extends HashSet<T> {
  
  private static final long serialVersionUID = 1L;
  
  final int max;
  List<T> recent = new LinkedList<T>();
  
  public SimpleLRUCache(int max) {
    this.max = max;
  }
  
  @Override
  public boolean add(T e) {
    boolean result = super.add(e);
    if (result) {
      recent.add(e);
      if (recent.size() > max) {
        T oldest = recent.remove(0);
        super.remove(oldest);
      }
    } else {
      recent.remove(e);
      recent.add(e);
    }
    return result;
  }
  
  @Override
  public boolean remove(Object o) {
    if (super.remove(o)) {
      recent.remove(o);
      return true;
    }
    return false;
  }
  
  @Override
  public boolean addAll(Collection<? extends T> c) {
    throw new UnsupportedOperationException();
  }
  
  @Override
  public boolean retainAll(Collection<?> c) {
    throw new UnsupportedOperationException();
  }
  
  @Override
  public boolean removeAll(Collection<?> c) {
    throw new UnsupportedOperationException();
  }
  
  @Override
  public void clear() {
    super.clear();
    recent.clear();
  }
  
}

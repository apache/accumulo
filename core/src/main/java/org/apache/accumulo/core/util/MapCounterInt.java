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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Set;

/**
 * A Map Counter for counting with Integers
 */
public class MapCounterInt<KT> {

  static class MutableInt {
    int i = 0;
  }

  private HashMap<KT,MutableInt> map;

  public MapCounterInt() {
    map = new HashMap<>();
  }

  public int increment(KT key, int i) {
    MutableInt mutableInt = map.computeIfAbsent(key, KT -> new MutableInt());

    mutableInt.i += i;

    if (mutableInt.i == 0) {
      map.remove(key);
    }

    return mutableInt.i;
  }

  public int decrement(KT key, int i) {
    return increment(key, -1 * i);
  }

  public int get(KT key) {
    MutableInt mutableInt = map.get(key);
    if (mutableInt == null) {
      return 0;
    }

    return mutableInt.i;
  }

  public Set<KT> keySet() {
    return map.keySet();
  }

  public Collection<Integer> values() {
    Collection<MutableInt> vals = map.values();
    ArrayList<Integer> ret = new ArrayList<>(vals.size());
    for (MutableInt mutableInt : vals) {
      ret.add(mutableInt.i);
    }

    return ret;
  }

  public int size() {
    return map.size();
  }
}

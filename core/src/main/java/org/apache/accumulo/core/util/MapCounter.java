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

import java.util.HashMap;
import java.util.Set;
import java.util.stream.LongStream;

/**
 * A Map counter for counting with longs or integers. Not thread safe.
 */
public class MapCounter<KT> {

  static class MutableLong {
    long l = 0L;
  }

  private final HashMap<KT,MutableLong> map;

  public MapCounter() {
    map = new HashMap<>();
  }

  public long increment(KT key, long l) {
    MutableLong ml = map.computeIfAbsent(key, KT -> new MutableLong());

    ml.l += l;

    if (ml.l == 0) {
      map.remove(key);
    }

    return ml.l;
  }

  public long decrement(KT key, long l) {
    return increment(key, -1 * l);
  }

  public long get(KT key) {
    MutableLong ml = map.get(key);
    if (ml == null) {
      return 0;
    }

    return ml.l;
  }

  public int getInt(KT key) {
    return Math.toIntExact(get(key));
  }

  public Set<KT> keySet() {
    return map.keySet();
  }

  public LongStream values() {
    return map.values().stream().mapToLong(mutLong -> mutLong.l);
  }

  public long max() {
    return values().max().orElse(0);
  }

  public int size() {
    return map.size();
  }
}

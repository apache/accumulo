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

public class MapCounter<KT> {

  static class MutableLong {
    long l = 0l;
  }

  private HashMap<KT,MutableLong> map;

  public MapCounter() {
    map = new HashMap<>();
  }

  public long increment(KT key, long l) {
    MutableLong ml = map.get(key);
    if (ml == null) {
      ml = new MutableLong();
      map.put(key, ml);
    }

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

  public Set<KT> keySet() {
    return map.keySet();
  }

  public Collection<Long> values() {
    Collection<MutableLong> vals = map.values();
    ArrayList<Long> ret = new ArrayList<>(vals.size());
    for (MutableLong ml : vals) {
      ret.add(ml.l);
    }

    return ret;
  }

  public int size() {
    return map.size();
  }
}

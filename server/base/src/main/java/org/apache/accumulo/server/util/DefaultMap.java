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
package org.apache.accumulo.server.util;

import java.util.HashMap;

/**
 * A HashMap that returns a default value if the key is not stored in the map.
 *
 * A zero-argument constructor of the default object's class is used, otherwise the default object is used.
 */
public class DefaultMap<K,V> extends HashMap<K,V> {
  private static final long serialVersionUID = 1L;
  V dfault;

  public DefaultMap(V dfault) {
    this.dfault = dfault;
  }

  @SuppressWarnings("unchecked")
  @Override
  public V get(Object key) {
    K k = (K) key; // fail early that key is correct type, rather than during put
    V result = super.get(k);
    if (result == null) {
      try {
        super.put(k, result = construct());
      } catch (Exception ex) {
        throw new RuntimeException(ex);
      }
    }
    return result;
  }

  @SuppressWarnings("unchecked")
  private V construct() {
    try {
      return (V) dfault.getClass().newInstance();
    } catch (Exception ex) {
      return dfault;
    }
  }
}

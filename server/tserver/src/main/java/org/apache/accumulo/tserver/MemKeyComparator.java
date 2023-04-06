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
package org.apache.accumulo.tserver;

import java.io.Serializable;
import java.util.Comparator;

import org.apache.accumulo.core.data.Key;

class MemKeyComparator implements Comparator<Key>, Serializable {

  private static final long serialVersionUID = 1L;

  @Override
  public int compare(Key k1, Key k2) {
    int cmp = k1.compareTo(k2);

    if (cmp == 0) {
      if (k1 instanceof MemKey) {
        if (k2 instanceof MemKey) {
          cmp = ((MemKey) k2).getKVCount() - ((MemKey) k1).getKVCount();
        } else {
          cmp = 1;
        }
      } else if (k2 instanceof MemKey) {
        cmp = -1;
      }
    }

    return cmp;
  }
}

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

import java.util.Collections;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.tserver.tablet.Tablet;

import com.google.common.collect.ImmutableSortedMap;

/*
 * The set of online tablets is frequently read by many threads and infrequently updated.  This
 * class exists to create a simple wrapper that keeps an immutable snapshot up to date.  Many
 * threads can access the snapshot without interfering with each other.
 */
public class OnlineTablets {
  private volatile SortedMap<KeyExtent,Tablet> snapshot = Collections.emptySortedMap();
  private final SortedMap<KeyExtent,Tablet> onlineTablets = new TreeMap<>();

  public synchronized void put(KeyExtent ke, Tablet t) {
    onlineTablets.put(ke, t);
    snapshot = ImmutableSortedMap.copyOf(onlineTablets);
  }

  public synchronized void remove(KeyExtent ke) {
    onlineTablets.remove(ke);
    snapshot = ImmutableSortedMap.copyOf(onlineTablets);
  }

  public synchronized void split(KeyExtent oldTablet, Tablet newTablet1, Tablet newTablet2) {
    onlineTablets.remove(oldTablet);
    onlineTablets.put(newTablet1.getExtent(), newTablet1);
    onlineTablets.put(newTablet2.getExtent(), newTablet2);
    snapshot = ImmutableSortedMap.copyOf(onlineTablets);
  }

  SortedMap<KeyExtent,Tablet> snapshot() {
    return snapshot;
  }
}

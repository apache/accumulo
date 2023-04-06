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
package org.apache.accumulo.server.conf.store;

public interface PropChangeListener {

  /**
   * Signal that a ZooKeeper data change event occurred and that the data has changed.
   */
  void zkChangeEvent(final PropStoreKey<?> propStoreKey);

  /**
   * Signal that a cache change event occurred - cache change events occur on eviction or
   * invalidation of the cache entry. The underlying data may or may not have changed.
   */
  void cacheChangeEvent(final PropStoreKey<?> propStoreKey);

  /**
   * Signal that the node had been deleted from ZooKeeper.
   */
  void deleteEvent(final PropStoreKey<?> propStoreKey);

  /**
   * A ZooKeeper connection event (session closed, expired...) and that
   */
  void connectionEvent();
}

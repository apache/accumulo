/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.accumulo.server.conf.store;

import java.util.Collection;
import java.util.Map;

import org.apache.accumulo.server.conf.codec.VersionedProperties;
import org.checkerframework.checker.nullness.qual.Nullable;

public interface PropStore {

  /**
   * Test that a node for properties exists without throwing a KeeperException.
   *
   * @param propCacheKey
   *          the prop cache id
   * @return true if the property node exists, false otherwise.
   * @throws PropStoreException
   *           if the check fails due to interrupt.
   */
  boolean exists(PropCacheKey propCacheKey);

  /**
   * Return the data version of the encoded property node.
   *
   * @param propCacheKey
   *          the prop cache id
   * @return the data version or -1 if the version cannot be determined.
   * @throws PropStoreException
   *           if the call to data store fails.
   */
  int getNodeVersion(PropCacheKey propCacheKey);

  /**
   * Create an initial entry for the PropCacheId. If properties already exist, they are not
   * modified.
   *
   * @param propCacheKey
   *          the prop cache id
   * @param props
   *          a map of property k,v pairs
   * @throws PropStoreException
   *           if the updates fails because of an underlying store exception
   */
  void create(PropCacheKey propCacheKey, Map<String,String> props);

  /**
   *
   * @param propCacheId
   *          the prop cache id
   * @return The versioned properties or null if the properties do not exist for the id.
   * @throws PropStoreException
   *           if the updates fails because of an underlying store exception
   */
  @Nullable
  VersionedProperties get(PropCacheKey propCacheId);

  /**
   * Adds or updates current properties. If the property currently exists it is overwritten,
   * otherwise it is added.
   *
   * @param propCacheKey
   *          the prop cache id
   * @param props
   *          a map of property k,v pairs
   * @throws PropStoreException
   *           if the values cannot be written or if an underlying store exception occurs.
   */
  void putAll(PropCacheKey propCacheKey, Map<String,String> props);

  /**
   * Delete the store node from the underlying store.
   *
   * @param propCacheKey
   *          the prop cache id
   * @throws PropStoreException
   *           if the updates fails because of an underlying store exception
   */
  void delete(PropCacheKey propCacheKey);

  /**
   * Deletes individual properties specified by the set of keys.
   *
   * @param propCacheKey
   *          the prop cache id
   * @param keys
   *          a set of keys.
   * @throws PropStoreException
   *           if the values cannot be deleted or if an underlying store exception occurs.
   */
  void removeProperties(PropCacheKey propCacheKey, Collection<String> keys);

  /**
   * External processes can register for notifications if the properties change. Normally processes
   * can read from the store and always receive the current snapshot of the latest values. However,
   * it the process wants to take an active action on change detections, then they can register and
   * receive notifications.
   * <p>
   * Implementation detail - the notification occurs on a separate thread from the underlying store,
   * but listeners should not perform lengthy operations on the notification to prevent delaying
   * other listeners from receive timely notification of the changes detected.
   *
   * @param propCacheKey
   *          the prop cache id
   * @param listener
   *          a listener
   */
  void registerAsListener(PropCacheKey propCacheKey, PropChangeListener listener);

}

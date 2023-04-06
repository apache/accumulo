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

import java.util.Collection;
import java.util.ConcurrentModificationException;
import java.util.Map;

import org.apache.accumulo.server.conf.codec.VersionedProperties;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

public interface PropStore {

  /**
   * Test that a node for properties exists without throwing a KeeperException.
   *
   * @param propStoreKey the prop cache key
   * @return true if the property node exists, false otherwise.
   * @throws IllegalStateException if the check fails due to interrupt.
   */
  boolean exists(PropStoreKey<?> propStoreKey);

  /**
   * Create an initial entry for the PropCacheId. If properties already exist, they are not
   * modified.
   *
   * @param propStoreKey the prop cache key
   * @param props a map of property k,v pairs
   * @throws IllegalStateException if the updates fails because of an underlying store exception
   */
  void create(PropStoreKey<?> propStoreKey, Map<String,String> props);

  /**
   * @param propCacheId the prop cache key
   * @return The versioned properties.
   * @throws IllegalStateException if the updates fails because of an underlying store exception or
   *         if the properties do not exist for the propCacheId
   */
  @NonNull
  VersionedProperties get(PropStoreKey<?> propCacheId);

  /**
   * Adds or updates current properties. If the property currently exists it is overwritten,
   * otherwise it is added.
   *
   * @param propStoreKey the prop cache key
   * @param props a map of property k,v pairs
   * @throws IllegalStateException if the values cannot be written or if an underlying store
   *         exception occurs.
   */
  void putAll(PropStoreKey<?> propStoreKey, Map<String,String> props);

  /**
   * Replaces all current properties with map provided. If a property is not included in the new
   * map, the property will not be set.
   *
   * @param propStoreKey the prop cache key
   * @param version the version of the properties
   * @param props a map of property k,v pairs
   * @throws IllegalStateException if the values cannot be written or if an underlying store
   *         exception occurs.
   * @throws java.util.ConcurrentModificationException if the properties changed since reading and
   *         can not be modified
   */
  void replaceAll(PropStoreKey<?> propStoreKey, long version, Map<String,String> props)
      throws ConcurrentModificationException;

  /**
   * Delete the store node from the underlying store.
   *
   * @param propStoreKey the prop cache key
   * @throws IllegalStateException if the updates fails because of an underlying store exception
   */
  void delete(PropStoreKey<?> propStoreKey);

  /**
   * Deletes individual properties specified by the set of keys.
   *
   * @param propStoreKey the prop cache key
   * @param keys a set of keys.
   * @throws IllegalStateException if the values cannot be deleted or if an underlying store
   *         exception occurs.
   */
  void removeProperties(PropStoreKey<?> propStoreKey, Collection<String> keys);

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
   * @param propStoreKey the prop cache key
   * @param listener a listener
   */
  void registerAsListener(PropStoreKey<?> propStoreKey, PropChangeListener listener);

  PropCache getCache();

  @Nullable
  VersionedProperties getIfCached(PropStoreKey<?> propStoreKey);

  /**
   * Compare the stored data version with the expected version. Notifies subscribers of the change
   * detection.
   *
   * @param storeKey specifies key for backend store
   * @param expectedVersion the expected data version
   * @return true if the stored version matches the provided expected version.
   */
  boolean validateDataVersion(PropStoreKey<?> storeKey, long expectedVersion);
}

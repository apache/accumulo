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

public interface PropStore {

  /**
   * Create an initial entry for the PropCacheId. If properties already exist, they are not
   * modified.
   *
   * @param PropCacheId
   *          the prop cache id
   * @param props
   *          a map of property k,v pairs
   * @return true if created, false otherwise.
   * @throws PropStoreException
   *           if the updates fails because of an underlying store exception
   */
  boolean create(PropCacheId PropCacheId, Map<String,String> props) throws PropStoreException;

  /**
   *
   * @param propCacheId
   *          the prop cache id
   * @return The versioned properties or null if the properties do not exist for the id.
   * @throws PropStoreException
   *           if the updates fails because of an underlying store exception
   */
  VersionedProperties get(PropCacheId propCacheId) throws PropStoreException;

  /**
   * Adds or updates current properties. If the property currently exists it is overwritten,
   * otherwise it is added.
   *
   * @param propCacheId
   *          the prop cache id
   * @param props
   *          a map of property k,v pairs
   * @return true if successful, false otherwise
   * @throws PropStoreException
   *           if the updates fails because of an underlying store exception
   */
  boolean putAll(PropCacheId propCacheId, Map<String,String> props) throws PropStoreException;

  /**
   * Delete the store node from the underlying store.
   *
   * @param propCacheId
   *          the prop cache id
   * @throws PropStoreException
   *           if the updates fails because of an underlying store exception
   */
  void delete(PropCacheId propCacheId) throws PropStoreException;

  /**
   * Deletes individual properties specified by the set of keys.
   *
   * @param propCacheId
   *          the prop cache id
   * @param keys
   *          a set of keys.
   * @return true if successful, false otherwise
   * @throws PropStoreException
   *           if the updates fails because of an underlying store exception
   */
  boolean removeProperties(PropCacheId propCacheId, Collection<String> keys)
      throws PropStoreException;

  /**
   * Get a fixed set of defined properties (designated in Properties as fixed). Certain properties
   * are stored in for persistence across restarts, they are read during start-up and remain
   * unchanged for the life of the instance. Any updates to the properties will only be reflected
   * with a restart.
   *
   * @return the properties read from the store at start-up
   */
  Map<String,String> readFixed();

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
   * @param propCacheId
   *          the prop cache id
   * @param listener
   *          a listener
   */
  void registerAsListener(PropCacheId propCacheId, PropChangeListener listener);

}

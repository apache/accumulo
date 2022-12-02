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
package org.apache.accumulo.server.conf.util;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.accumulo.server.conf.codec.VersionedProperties;
import org.apache.accumulo.server.conf.store.PropChangeListener;
import org.apache.accumulo.server.conf.store.PropStore;
import org.apache.accumulo.server.conf.store.PropStoreKey;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class provides a secondary, local cache of properties and is intended to be used to optimize
 * constructing a configuration hierarchy from the underlying properties stored in ZooKeeper.
 * <p>
 * Configurations and especially the derivers use an updateCount to detect configuration changes.
 * The updateCount is checked frequently and the configuration hierarchy rebuilt when a change is
 * detected.
 */
public class PropSnapshot implements PropChangeListener {

  private static final Logger log = LoggerFactory.getLogger(PropSnapshot.class);

  private final Lock updateLock = new ReentrantLock();
  private final AtomicBoolean needsUpdate = new AtomicBoolean(true);
  private final AtomicReference<VersionedProperties> vPropRef = new AtomicReference<>();
  private final PropStoreKey<?> propStoreKey;
  private final PropStore propStore;

  public static PropSnapshot create(final PropStoreKey<?> propStoreKey, final PropStore propStore) {
    var ps = new PropSnapshot(propStoreKey, propStore);
    propStore.registerAsListener(propStoreKey, ps);
    return ps;
  }

  private PropSnapshot(final PropStoreKey<?> propStoreKey, final PropStore propStore) {
    this.propStoreKey = propStoreKey;
    this.propStore = propStore;
  }

  /**
   * Get the current snapshot - updating if necessary.
   *
   * @return the current property snapshot.
   */
  public @NonNull VersionedProperties getVersionedProperties() {
    updateSnapshot();
    var answer = vPropRef.get();
    if (answer == null) {
      throw new IllegalStateException("Invalid state for property snapshot, no value has been set");
    }
    return answer;
  }

  /**
   * Signal the current snapshot is invalid and needs to be updated on next access.
   */
  public void requireUpdate() {
    updateLock.lock();
    try {
      needsUpdate.set(true);
    } finally {
      updateLock.unlock();
    }
  }

  /**
   * Update the current snapshot if a refresh is required.
   *
   * @throws IllegalStateException if the properties cannot be retrieved from the underlying store.
   */
  private void updateSnapshot() {
    if (!needsUpdate.get()) {
      return;
    }
    updateLock.lock();
    try {
      // check after locked - another thread could have updated while waiting for lock
      if (!needsUpdate.get()) {
        return;
      }
      var vProps = propStore.get(propStoreKey);
      vPropRef.set(vProps);
      needsUpdate.set(false);
    } finally {
      updateLock.unlock();
    }
  }

  @Override
  public void zkChangeEvent(final PropStoreKey<?> eventPropKey) {
    if (propStoreKey.equals(eventPropKey)) {
      requireUpdate();
    }
  }

  @Override
  public void cacheChangeEvent(final PropStoreKey<?> eventPropKey) {
    if (propStoreKey.equals(eventPropKey)) {
      requireUpdate();
    }
  }

  @Override
  public void deleteEvent(final PropStoreKey<?> eventPropKey) {
    if (propStoreKey.equals(eventPropKey)) {
      requireUpdate();
      log.debug("Received property delete event for {}", propStoreKey);
    }
  }

  @Override
  public void connectionEvent() {
    requireUpdate();
    log.debug("Received connection event - update properties required");
  }

}

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
package org.apache.accumulo.server.conf.store.impl;

import java.util.Set;

import org.apache.accumulo.server.conf.store.PropChangeListener;
import org.apache.accumulo.server.conf.store.PropStoreKey;

/**
 * Provides a simple runnable base task for notifying listeners for PropStore event change
 * notifications.
 */
public abstract class PropStoreEventTask implements Runnable {

  private final PropStoreKey<?> propStoreKey;
  private final Set<PropChangeListener> listeners;

  /**
   * Used when the notification is sent to the listeners without specifying a prop cache key.
   *
   * @param listeners the set of listeners.
   */
  private PropStoreEventTask(final Set<PropChangeListener> listeners) {
    this.propStoreKey = null;
    this.listeners = listeners;
  }

  /**
   * Used when listeners for the specified prop cahe key should receive a notification.
   *
   * @param propStoreKey the prop cache key
   * @param listeners the set of listeners
   */
  private PropStoreEventTask(final PropStoreKey<?> propStoreKey,
      final Set<PropChangeListener> listeners) {
    this.propStoreKey = propStoreKey;
    this.listeners = listeners;
  }

  public static class PropStoreZkChangeEventTask extends PropStoreEventTask {

    PropStoreZkChangeEventTask(final PropStoreKey<?> propStoreKey,
        final Set<PropChangeListener> listeners) {
      super(propStoreKey, listeners);
    }

    @Override
    public void run() {
      super.listeners.forEach(listener -> listener.zkChangeEvent(super.propStoreKey));
    }
  }

  public static class PropStoreCacheChangeEventTask extends PropStoreEventTask {

    PropStoreCacheChangeEventTask(final PropStoreKey<?> propStoreKey,
        final Set<PropChangeListener> listeners) {
      super(propStoreKey, listeners);
    }

    @Override
    public void run() {
      super.listeners.forEach(listener -> listener.cacheChangeEvent(super.propStoreKey));
    }
  }

  public static class PropStoreDeleteEventTask extends PropStoreEventTask {

    PropStoreDeleteEventTask(final PropStoreKey<?> propStoreKey,
        final Set<PropChangeListener> listeners) {
      super(propStoreKey, listeners);
    }

    @Override
    public void run() {
      super.listeners.forEach(listener -> listener.deleteEvent(super.propStoreKey));
    }
  }

  public static class PropStoreConnectionEventTask extends PropStoreEventTask {

    PropStoreConnectionEventTask(final Set<PropChangeListener> listeners) {
      super(listeners);
    }

    @Override
    public void run() {
      super.listeners.forEach(PropChangeListener::connectionEvent);
    }
  }
}

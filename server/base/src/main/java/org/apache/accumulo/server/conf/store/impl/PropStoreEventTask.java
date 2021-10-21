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
package org.apache.accumulo.server.conf.store.impl;

import java.util.Set;

import org.apache.accumulo.server.conf.store.PropCacheId;
import org.apache.accumulo.server.conf.store.PropChangeListener;

/**
 * Provides a simple runnable base task for notifying listeners for PropStore event change
 * notifications.
 */
public abstract class PropStoreEventTask implements Runnable {

  private final PropCacheId propCacheId;
  private final Set<PropChangeListener> listeners;

  private PropStoreEventTask(final PropCacheId propCacheId,
      final Set<PropChangeListener> listeners) {
    this.propCacheId = propCacheId;
    this.listeners = listeners;
  }

  public static class PropStoreZkChangeEventTask extends PropStoreEventTask {

    PropStoreZkChangeEventTask(final PropCacheId propCacheId,
        final Set<PropChangeListener> listeners) {
      super(propCacheId, listeners);
    }

    @Override
    public void run() {
      super.listeners.forEach(listener -> listener.zkChangeEvent(super.propCacheId));
    }
  }

  public static class PropStoreCacheChangeEventTask extends PropStoreEventTask {

    PropStoreCacheChangeEventTask(final PropCacheId propCacheId,
        final Set<PropChangeListener> listeners) {
      super(propCacheId, listeners);
    }

    @Override
    public void run() {
      super.listeners.forEach(listener -> listener.cacheChangeEvent(super.propCacheId));
    }
  }

  public static class PropStoreDeleteEventTask extends PropStoreEventTask {

    PropStoreDeleteEventTask(final PropCacheId propCacheId,
        final Set<PropChangeListener> listeners) {
      super(propCacheId, listeners);
    }

    @Override
    public void run() {
      super.listeners.forEach(listener -> listener.deleteEvent(super.propCacheId));
    }
  }

  public static class PropStoreConnectionEventTask extends PropStoreEventTask {

    PropStoreConnectionEventTask(final PropCacheId propCacheId,
        final Set<PropChangeListener> listeners) {
      super(null, listeners);
    }

    @Override
    public void run() {
      super.listeners.forEach(listener -> listener.connectionEvent());
    }
  }
}

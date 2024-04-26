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

import static java.util.stream.Collectors.collectingAndThen;
import static java.util.stream.Collectors.toSet;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.accumulo.core.util.threads.ThreadPools;
import org.apache.accumulo.server.conf.store.PropChangeListener;
import org.apache.accumulo.server.conf.store.PropStoreKey;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class serves as a translator between ZooKeeper events and converts them to PropStore events.
 * Using this as an intermediary, the external listeners do not need to set / manage external
 * ZooKeeper watchers, they can register for PropStore events if they need to take active action on
 * change detection.
 * <p>
 * Users of the PropStore.get() will get properties that match what is stored in ZooKeeper for each
 * call and do not need to manage any caching. However, the ability to receive active notification
 * without needed to register / manage ZooKeeper watchers external to the PropStore is provided in
 * case other code is relying on active notifications.
 * <p>
 * The notification occurs on a separate thread from the ZooKeeper notification handling, but
 * listeners should not perform lengthy operations on the notification thread so that other listener
 * notifications are not delayed.
 */
public class PropStoreWatcher implements Watcher {

  private static final Logger log = LoggerFactory.getLogger(PropStoreWatcher.class);

  private static final ExecutorService executorService = ThreadPools.getServerThreadPools()
      .getPoolBuilder("zoo_change_update").numCoreThreads(2).build();
  private final ReentrantReadWriteLock listenerLock = new ReentrantReadWriteLock();
  private final ReentrantReadWriteLock.ReadLock listenerReadLock = listenerLock.readLock();
  private final ReentrantReadWriteLock.WriteLock listenerWriteLock = listenerLock.writeLock();

  // access should be guarded by acquiring the listener read or write lock
  private final Map<PropStoreKey<?>,Set<PropChangeListener>> listeners = new HashMap<>();

  private final ReadyMonitor zkReadyMonitor;

  public PropStoreWatcher(final ReadyMonitor zkReadyMonitor) {
    this.zkReadyMonitor = zkReadyMonitor;
  }

  public void registerListener(final PropStoreKey<?> propStoreKey,
      final PropChangeListener listener) {
    listenerWriteLock.lock();
    try {
      Set<PropChangeListener> set = listeners.computeIfAbsent(propStoreKey, s -> new HashSet<>());
      set.add(listener);
    } finally {
      listenerWriteLock.unlock();
    }
  }

  /**
   * Process a ZooKeeper event. This method does not reset the watcher. Subscribers are notified of
   * the change - if they call get to update and respond to the change the watcher will be (re)set
   * then. This helps clean up watchers by not automatically re-adding the watcher on the event but
   * only if being used.
   *
   * @param event ZooKeeper event.
   */
  @Override
  public void process(final WatchedEvent event) {

    String path;
    PropStoreKey<?> propStoreKey;
    switch (event.getType()) {
      case NodeDataChanged:
        path = event.getPath();
        log.trace("handle change event for path: {}", path);
        propStoreKey = PropStoreKey.fromPath(path);
        if (propStoreKey != null) {
          signalZkChangeEvent(propStoreKey);
        }
        break;
      case NodeDeleted:
        path = event.getPath();
        log.trace("handle delete event for path: {}", path);
        propStoreKey = PropStoreKey.fromPath(path);
        if (propStoreKey != null) {
          // notify listeners
          Set<PropChangeListener> snapshot = getListenerSnapshot(propStoreKey);
          if (snapshot != null) {
            executorService
                .execute(new PropStoreEventTask.PropStoreDeleteEventTask(propStoreKey, snapshot));
          }
          listenerCleanup(propStoreKey);
        }
        break;
      case None:
        Event.KeeperState state = event.getState();
        switch (state) {
          // pause - could reconnect
          case ConnectedReadOnly:
          case Disconnected:
            log.debug("ZooKeeper disconnected event received");
            zkReadyMonitor.clearReady();
            executorService.execute(
                new PropStoreEventTask.PropStoreConnectionEventTask(getAllListenersSnapshot()));
            break;

          // okay
          case SyncConnected:
            log.debug("ZooKeeper connected event received");
            zkReadyMonitor.setReady();
            break;

          // terminal - never coming back.
          case Expired:
          case Closed:
            log.info("ZooKeeper connection closed event received");
            zkReadyMonitor.clearReady();
            zkReadyMonitor.setClosed(); // terminal condition
            executorService.execute(
                new PropStoreEventTask.PropStoreConnectionEventTask(getAllListenersSnapshot()));
            break;

          default:
            log.trace("ignoring zooKeeper state: {}", state);
        }
        break;
      default:
        break;
    }

  }

  /**
   * Execute a task to notify registered listeners that the propStoreKey node received an event
   * notification from ZooKeeper and should be updated. The process can be initiated either by a
   * ZooKeeper notification or a change detected in the cache based on a ZooKeeper event.
   *
   * @param propStoreKey the cache id
   */
  public void signalZkChangeEvent(@NonNull final PropStoreKey<?> propStoreKey) {
    log.trace("signal ZooKeeper change event: {}", propStoreKey);
    Set<PropChangeListener> snapshot = getListenerSnapshot(propStoreKey);
    log.trace("Sending change event to: {}", snapshot);
    if (snapshot != null) {
      executorService
          .execute(new PropStoreEventTask.PropStoreZkChangeEventTask(propStoreKey, snapshot));
    }
  }

  /**
   * Execute a task to notify registered listeners that the propStoreKey node change was detected
   * should be updated.
   *
   * @param propStoreKey the cache id
   */
  public void signalCacheChangeEvent(final PropStoreKey<?> propStoreKey) {
    log.trace("cache change event: {}", propStoreKey);
    Set<PropChangeListener> snapshot = getListenerSnapshot(propStoreKey);
    if (snapshot != null) {
      executorService
          .execute(new PropStoreEventTask.PropStoreCacheChangeEventTask(propStoreKey, snapshot));
    }
  }

  /**
   * Clean-up the active listeners set when an entry is removed from the cache, remove it from the
   * active listeners.
   *
   * @param propStoreKey the cache id
   */
  public void listenerCleanup(final PropStoreKey<?> propStoreKey) {
    listenerWriteLock.lock();
    try {
      listeners.remove(propStoreKey);
    } finally {
      listenerWriteLock.unlock();
    }
  }

  /**
   * Get an immutable snapshot of the listeners for a prop cache id. The set is intended for
   * notification of changes for a specific prop cache id.
   *
   * @param propStoreKey the prop cache id
   * @return an immutable copy of listeners.
   */
  private Set<PropChangeListener> getListenerSnapshot(final PropStoreKey<?> propStoreKey) {

    Set<PropChangeListener> snapshot = null;
    listenerReadLock.lock();
    try {
      Set<PropChangeListener> set = listeners.get(propStoreKey);
      if (set != null) {
        snapshot = Set.copyOf(set);
      }

    } finally {
      listenerReadLock.unlock();
    }
    return snapshot;
  }

  /**
   * Get an immutable snapshot of the all listeners registered for event. The set is intended for
   * connection event notifications that are not specific to an individual prop cache id.
   *
   * @return an immutable copy of all registered listeners.
   */
  private Set<PropChangeListener> getAllListenersSnapshot() {

    listenerReadLock.lock();
    try {
      return listeners.keySet().stream().flatMap(key -> listeners.get(key).stream())
          .collect(collectingAndThen(toSet(), Collections::unmodifiableSet));
    } finally {
      listenerReadLock.unlock();
    }
  }
}

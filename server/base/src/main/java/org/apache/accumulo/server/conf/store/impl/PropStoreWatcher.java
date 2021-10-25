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

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

import org.apache.accumulo.core.util.threads.ThreadPools;
import org.apache.accumulo.server.conf.store.PropCacheId;
import org.apache.accumulo.server.conf.store.PropChangeListener;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class serves as a translator between ZooKeeper events and converts them to PropStore events.
 * Using this as an intermediary, the external listeners do not need to set / manage external
 * ZooKeeper watchers, they can register a for PropStore events if the need to take active action on
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

  private final ExecutorService executorService =
      ThreadPools.createFixedThreadPool(1, "zoo_change_update", false);

  private final ReentrantReadWriteLock listenerLock = new ReentrantReadWriteLock();
  private final ReentrantReadWriteLock.ReadLock listenerReadLock = listenerLock.readLock();
  private final ReentrantReadWriteLock.WriteLock listenerWriteLock = listenerLock.writeLock();

  // access should be guarded by acquiring the listener read or write lock
  private final Map<PropCacheId,Set<PropChangeListener>> listeners = new HashMap<>();

  private final ReadyMonitor zkReadyMonitor;

  public PropStoreWatcher(final ReadyMonitor zkReadyMonitor) {
    this.zkReadyMonitor = zkReadyMonitor;
  }

  public void registerListener(final PropCacheId propCacheId, final PropChangeListener listener) {
    listenerWriteLock.lock();
    try {
      Set<PropChangeListener> set = listeners.computeIfAbsent(propCacheId, s -> new HashSet<>());
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
   * @param event
   *          ZooKeeper event.
   */
  @Override
  public void process(final WatchedEvent event) {

    String path;
    switch (event.getType()) {
      case NodeDataChanged:
        path = event.getPath();
        log.trace("handle change event for path: {}", path);
        PropCacheId.fromPath(path).ifPresent(this::signalZkChangeEvent);
        break;
      case NodeDeleted:
        path = event.getPath();
        log.trace("handle delete event for path: {}", path);
        PropCacheId.fromPath(path).ifPresent(cacheId -> {
          // notify listeners
          Set<PropChangeListener> snapshot = getListenerSnapshot(cacheId);
          if (Objects.nonNull(snapshot)) {
            executorService
                .submit(new PropStoreEventTask.PropStoreDeleteEventTask(cacheId, snapshot));
          }

          listenerCleanup(cacheId);

        });

        break;
      case None:
        Event.KeeperState state = event.getState();
        switch (state) {
          // pause - could reconnect
          case ConnectedReadOnly:
          case Disconnected:
            log.info("ZooKeeper disconnected event received");
            zkReadyMonitor.clearReady();
            executorService.submit(new PropStoreEventTask.PropStoreConnectionEventTask(null,
                getAllListenersSnapshot()));
            break;

          // okay
          case SyncConnected:
            log.info("ZooKeeper connected event received");
            zkReadyMonitor.setReady();
            break;

          // terminal - never coming back.
          case Expired:
          case Closed:
            log.info("ZooKeeper connection closed event received");
            zkReadyMonitor.clearReady();
            zkReadyMonitor.setClosed(); // terminal condition
            executorService.submit(new PropStoreEventTask.PropStoreConnectionEventTask(null,
                getAllListenersSnapshot()));
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
   * Submit task to notify registered listeners that the propCacheId node received an event
   * notification from ZooKeeper and should be updated. The process can be initiated either by a
   * ZooKeeper notification or a change detected in the cache based on a ZooKeeper event.
   *
   * @param propCacheId
   *          the cache id
   */
  public void signalZkChangeEvent(final PropCacheId propCacheId) {
    log.debug("signal ZooKeeper change event: {}", propCacheId);
    Set<PropChangeListener> snapshot = getListenerSnapshot(propCacheId);
    if (Objects.nonNull(snapshot)) {
      executorService
          .submit(new PropStoreEventTask.PropStoreZkChangeEventTask(propCacheId, snapshot));
    }
  }

  /**
   * Submit task to notify registered listeners that the propCacheId node change was detected should
   * be updated.
   *
   * @param propCacheId
   *          the cache id
   */
  public void signalCacheChangeEvent(final PropCacheId propCacheId) {
    log.info("cache change event: {}", propCacheId);
    Set<PropChangeListener> snapshot = getListenerSnapshot(propCacheId);
    if (Objects.nonNull(snapshot)) {
      executorService
          .submit(new PropStoreEventTask.PropStoreCacheChangeEventTask(propCacheId, snapshot));
    }
  }

  /**
   * Clean-up the active listeners set when an entry is removed from the cache, remove it from the
   * active listeners.
   *
   * @param propCacheId
   *          the cache id
   */
  public void listenerCleanup(final PropCacheId propCacheId) {
    listenerWriteLock.lock();
    try {
      listeners.remove(propCacheId);
    } finally {
      listenerWriteLock.unlock();
    }
  }

  /**
   * Get an immutable snapshot of the listeners for a prop cache id. The set is intended for
   * notification of changes for a specific prop cache id.
   *
   * @param PropCacheId
   *          the prop cache id
   * @return an immutable copy of listeners.
   */
  private Set<PropChangeListener> getListenerSnapshot(final PropCacheId PropCacheId) {

    Set<PropChangeListener> snapshot = null;
    listenerReadLock.lock();
    try {
      Set<PropChangeListener> set = listeners.get(PropCacheId);
      if (Objects.nonNull(set)) {
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

    Set<PropChangeListener> snapshot;
    listenerReadLock.lock();
    try {

      snapshot = listeners.keySet().stream().flatMap(key -> listeners.get(key).stream())
          .collect(Collectors.toSet());

    } finally {
      listenerReadLock.unlock();
    }
    return Collections.unmodifiableSet(snapshot);
  }
}

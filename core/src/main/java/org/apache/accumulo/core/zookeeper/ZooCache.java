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
package org.apache.accumulo.core.zookeeper;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;
import static org.apache.accumulo.core.util.LazySingletons.RANDOM;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.ConcurrentModificationException;
import java.util.List;
import java.util.NavigableSet;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Consumer;
import java.util.function.Predicate;

import org.apache.accumulo.core.lock.ServiceLock;
import org.apache.accumulo.core.lock.ServiceLockData;
import org.apache.accumulo.core.lock.ServiceLockPaths.ServiceLockPath;
import org.apache.accumulo.core.util.cache.Caches;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Ticker;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

/**
 * A cache for values stored in ZooKeeper. Values are kept up to date as they change.
 */
public class ZooCache {

  public interface ZooCacheWatcher extends Consumer<WatchedEvent> {}

  private static final Logger log = LoggerFactory.getLogger(ZooCache.class);

  protected volatile NavigableSet<String> watchedPaths =
      Collections.unmodifiableNavigableSet(new TreeSet<>());
  // visible for tests
  protected final ZCacheWatcher watcher = new ZCacheWatcher();
  private final List<ZooCacheWatcher> externalWatchers =
      Collections.synchronizedList(new ArrayList<>());

  private static final AtomicLong nextCacheId = new AtomicLong(0);
  private final String cacheId = "ZC" + nextCacheId.incrementAndGet();

  public static final Duration CACHE_DURATION = Duration.ofMinutes(30);

  private final Cache<String,ZcNode> cache;

  private final ConcurrentMap<String,ZcNode> nodeCache;

  private final ZooSession zk;

  private volatile boolean closed = false;

  private final AtomicLong updateCount = new AtomicLong(0);

  class ZCacheWatcher implements Watcher {
    @Override
    public void process(WatchedEvent event) {
      if (log.isTraceEnabled()) {
        log.trace("{}: {}", cacheId, event);
      }

      switch (event.getType()) {
        case NodeChildrenChanged:
          // According to documentation we should not receive this event.
          // According to https://issues.apache.org/jira/browse/ZOOKEEPER-4475 we
          // may receive this event (Fixed in 3.9.0)
          break;
        case ChildWatchRemoved:
        case DataWatchRemoved:
          // We don't need to do anything with the cache on these events.
          break;
        case NodeDataChanged:
          log.trace("{} node data changed; clearing {}", cacheId, event.getPath());
          clear(path -> path.equals(event.getPath()));
          break;
        case NodeCreated:
        case NodeDeleted:
          // With the Watcher being set at a higher level we need to remove
          // the parent of the affected node and all of its children from the cache
          // so that the parent and children node can be re-cached. If we only remove the
          // affected node, then the cached children in the parent could be incorrect.
          int lastSlash = event.getPath().lastIndexOf('/');
          String parent = lastSlash == 0 ? "/" : event.getPath().substring(0, lastSlash);
          log.trace("{} node created or deleted {}; clearing {}", cacheId, event.getPath(), parent);
          clear((path) -> path.startsWith(parent));
          break;
        case None:
          switch (event.getState()) {
            case Closed:
              // Closed is a new event in ZK 3.5 generated client-side when the ZK client is closed
              // These are ignored, because they are generated by SingletonManager closing
              // ZooKeepers for ZooSession, and SingletonManager is already responsible for clearing
              // caches via the registered ZooCacheFactory singleton
              log.trace("{} ZooKeeper connection closed, ignoring; {}", cacheId, event);
              break;
            case Disconnected:
              log.trace("{} ZooKeeper connection disconnected, clearing cache; {}", cacheId, event);
              clear();
              break;
            case SyncConnected:
              log.trace("{} ZooKeeper connection established, ignoring; {}", cacheId, event);
              break;
            case Expired:
              log.trace("{} ZooKeeper connection expired, clearing cache; {}", cacheId, event);
              clear();
              break;
            default:
              log.warn("{} Unhandled state {}", cacheId, event);
              break;
          }
          break;
        default:
          log.warn("{} Unhandled event type {}", cacheId, event);
          break;
      }

      externalWatchers.forEach(ew -> ew.accept(event));
    }
  }

  /**
   * Creates a ZooCache instance that uses the supplied ZooSession for communicating with the
   * instance's ZooKeeper servers. The ZooCache will create persistent watchers at the given
   * pathsToWatch, if any, to be updated when changes are made in ZooKeeper for nodes at or below in
   * the tree. If ZooCacheWatcher's are added via {@code addZooCacheWatcher}, then they will be
   * notified when this object is notified of changes via the PersistentWatcher callback.
   *
   * @param zk ZooSession for this instance
   * @param pathsToWatch Paths in ZooKeeper to watch
   */
  public ZooCache(ZooSession zk, Set<String> pathsToWatch) {
    this(zk, pathsToWatch, Ticker.systemTicker());
  }

  // visible for tests that use a Ticker
  public ZooCache(ZooSession zk, Set<String> pathsToWatch, Ticker ticker) {
    this.zk = requireNonNull(zk);
    this.cache = Caches.getInstance().createNewBuilder(Caches.CacheName.ZOO_CACHE, false)
        .ticker(requireNonNull(ticker)).expireAfterAccess(CACHE_DURATION).build();
    // The concurrent map returned by Caffiene will only allow one thread to run at a time for a
    // given key and ZooCache relies on that. Not all concurrent map implementations have this
    // behavior
    // for their compute functions.
    this.nodeCache = cache.asMap();
    setupWatchers(pathsToWatch);
    log.trace("{} created new cache", cacheId, new Exception());
  }

  public void addZooCacheWatcher(ZooCacheWatcher watcher) {
    externalWatchers.add(requireNonNull(watcher));
  }

  // Called on construction and when ZooKeeper connection changes
  synchronized void setupWatchers(Set<String> pathsToWatch) {

    for (String left : pathsToWatch) {
      for (String right : pathsToWatch) {
        if (!left.equals(right) && left.contains(right)) {
          throw new IllegalArgumentException(
              "Overlapping paths found in paths to watch. left: " + left + ", right: " + right);
        }
      }
    }

    try {
      zk.addPersistentRecursiveWatchers(pathsToWatch, List.of(this.watcher));
      watchedPaths = Collections.unmodifiableNavigableSet(new TreeSet<>(pathsToWatch));
    } catch (KeeperException | InterruptedException e) {
      throw new RuntimeException("Error setting up persistent recursive watcher", e);
    }

  }

  private boolean isWatchedPath(String path) {
    // Check that the path is equal to, or a descendant of, a watched path
    var floor = watchedPaths.floor(path);
    return floor != null
        && (floor.equals("/") || floor.equals(path) || path.startsWith(floor + "/"));
  }

  // Use this instead of Preconditions.checkState(isWatchedPath, String)
  // so that we are not creating String unnecessarily.
  private void ensureWatched(String path) {
    if (!isWatchedPath(path)) {
      throw new IllegalStateException("Supplied path " + path + " is not watched by this ZooCache");
    }
  }

  private abstract static class ZooRunnable<T> {
    /**
     * Runs an operation against ZooKeeper. Retries are performed by the retry method when
     * KeeperExceptions occur.
     *
     * Changes were made in ACCUMULO-4388 so that the run method no longer accepts Zookeeper as an
     * argument, and instead relies on the ZooRunnable implementation to call
     * {@link #getZooKeeper()}. Performing the call to retrieving a ZooKeeper Session after caches
     * are checked has the benefit of limiting ZK connections and blocking as a result of obtaining
     * these sessions.
     *
     * @return T the result of the runnable
     */
    abstract T run() throws KeeperException, InterruptedException;

    /**
     * Retry will attempt to call the run method. Run should make a call to {@link #getZooKeeper()}
     * after checks to cached information are made. This change, per ACCUMULO-4388 ensures that we
     * don't create a ZooKeeper session when information is cached, and access to ZooKeeper is
     * unnecessary.
     *
     * @return result of the runnable access success ( i.e. no exceptions ).
     */
    public T retry() {

      int sleepTime = 100;

      while (true) {

        try {
          return run();
        } catch (KeeperException | ZcException e) {
          KeeperException ke;
          if (e instanceof ZcException) {
            ke = ((ZcException) e).getZKException();
          } else {
            ke = ((KeeperException) e);
          }
          final Code code = ke.code();
          if (code == Code.NONODE) {
            log.error("Looked up non-existent node in cache " + ke.getPath(), e);
          } else if (code == Code.CONNECTIONLOSS || code == Code.OPERATIONTIMEOUT
              || code == Code.SESSIONEXPIRED) {
            log.warn("Saw (possibly) transient exception communicating with ZooKeeper, will retry",
                e);
          } else {
            log.warn("Zookeeper error, will retry", e);
          }
        } catch (InterruptedException | ZcInterruptedException e) {
          log.info("Zookeeper error, will retry", e);
        } catch (ConcurrentModificationException e) {
          log.debug("Zookeeper was modified, will retry");
        }

        try {
          // do not hold lock while sleeping
          Thread.sleep(sleepTime);
        } catch (InterruptedException e) {
          log.debug("Wait in retry() was interrupted.", e);
        }
        LockSupport.parkNanos(sleepTime);
        if (sleepTime < 10_000) {
          sleepTime = (int) (sleepTime + sleepTime * RANDOM.get().nextDouble());
        }
      }
    }

  }

  private static class ZcException extends RuntimeException {
    private static final long serialVersionUID = 1;

    private ZcException(KeeperException e) {
      super(e);
    }

    public KeeperException getZKException() {
      return (KeeperException) getCause();
    }
  }

  private static class ZcInterruptedException extends RuntimeException {
    private static final long serialVersionUID = 1;

    private ZcInterruptedException(InterruptedException e) {
      super(e);
    }
  }

  /**
   * Gets the children of the given node. A watch is established by this call.
   *
   * @param zPath path of node
   * @return children list, or null if node has no children or does not exist
   */
  public List<String> getChildren(final String zPath) {
    Preconditions.checkState(!closed);
    ensureWatched(zPath);
    ZooRunnable<List<String>> zr = new ZooRunnable<>() {

      @Override
      public List<String> run() throws KeeperException, InterruptedException {

        var zcNode = nodeCache.get(zPath);
        if (zcNode != null && zcNode.cachedChildren()) {
          return zcNode.getChildren();
        }

        log.trace("{} {} was not in children cache, looking up in zookeeper", cacheId, zPath);

        zcNode = nodeCache.compute(zPath, (zp, zcn) -> {
          // recheck the children now that lock is held on key
          if (zcn != null && zcn.cachedChildren()) {
            return zcn;
          }
          try {
            // Register a watcher on the node to monitor creation/deletion events for the node. It
            // is possible that an event from this watch could trigger prior to calling getChildren.
            // That is ok because the compute() call on the map has a lock and processing the event
            // will block until compute() returns. After compute() returns the event processing
            // would clear the map entry.
            Stat stat = zk.exists(zPath, null);
            if (stat == null) {
              log.trace("{} getChildren saw that {} does not exists", cacheId, zPath);
              return ZcNode.NON_EXISTENT;
            }
            List<String> children = zk.getChildren(zPath, null);
            log.trace("{} adding {} children of {} to cache", cacheId, children.size(), zPath);
            return new ZcNode(children, zcn);
          } catch (KeeperException.NoNodeException nne) {
            log.trace("{} get children saw race condition for {}, node deleted after exists call",
                cacheId, zPath);
            throw new ConcurrentModificationException(nne);
          } catch (KeeperException e) {
            throw new ZcException(e);
          } catch (InterruptedException e) {
            throw new ZcInterruptedException(e);
          }
        });
        // increment this after compute call completes when the change is visible
        updateCount.incrementAndGet();
        return zcNode.getChildren();
      }
    };

    return zr.retry();
  }

  /**
   * Gets data at the given path. Status information is not returned. A watch is established by this
   * call.
   *
   * @param zPath path to get
   * @return path data, or null if non-existent
   */
  public byte[] get(final String zPath) {
    return get(zPath, null);
  }

  /**
   * Gets data at the given path, filling status information into the given <code>Stat</code>
   * object. A watch is established by this call.
   *
   * @param zPath path to get
   * @param status status object to populate
   * @return path data, or null if non-existent
   */
  public byte[] get(final String zPath, final ZcStat status) {
    Preconditions.checkState(!closed);
    ensureWatched(zPath);
    ZooRunnable<byte[]> zr = new ZooRunnable<>() {

      @Override
      public byte[] run() throws KeeperException, InterruptedException {

        var zcNode = nodeCache.get(zPath);
        if (zcNode != null && zcNode.cachedData()) {
          if (status != null) {
            copyStats(status, zcNode.getStat());
          }
          return zcNode.getData();
        }

        log.trace("{} {} was not in data cache, looking up in zookeeper", cacheId, zPath);

        zcNode = nodeCache.compute(zPath, (zp, zcn) -> {
          // recheck the now that lock is held on key, it may be present now. Could have been
          // computed while waiting for lock.
          if (zcn != null && zcn.cachedData()) {
            return zcn;
          }
          /*
           * The following call to exists() is important, since we are caching that a node does not
           * exist. Once the node comes into existence, it will be added to the cache. But this
           * notification of a node coming into existence will only be given if exists() was
           * previously called. If the call to exists() is bypassed and only getData() is called
           * with a special case that looks for Code.NONODE in the KeeperException, then
           * non-existence can not be cached.
           */
          try {
            Stat stat = zk.exists(zPath, null);
            if (stat == null) {
              if (log.isTraceEnabled()) {
                log.trace("{} zookeeper did not contain {}", cacheId, zPath);
              }
              return ZcNode.NON_EXISTENT;
            } else {
              byte[] data = null;
              ZcStat zstat = null;
              try {
                data = zk.getData(zPath, null, stat);
                zstat = new ZcStat(stat);
              } catch (KeeperException.BadVersionException | KeeperException.NoNodeException e1) {
                throw new ConcurrentModificationException(e1);
              } catch (InterruptedException e) {
                throw new ZcInterruptedException(e);
              }
              if (log.isTraceEnabled()) {
                log.trace("{} zookeeper contained {} {}", cacheId, zPath,
                    (data == null ? null : new String(data, UTF_8)));
              }
              return new ZcNode(data, zstat, zcn);
            }
          } catch (KeeperException ke) {
            throw new ZcException(ke);
          } catch (InterruptedException e) {
            throw new ZcInterruptedException(e);
          }

        });

        // update this after the compute call completes when the change is visible
        updateCount.incrementAndGet();
        if (status != null) {
          copyStats(status, zcNode.getStat());
        }
        return zcNode.getData();
      }
    };

    return zr.retry();
  }

  /**
   * Helper method to copy stats from the cached stat into userStat
   *
   * @param userStat user Stat object
   * @param cachedStat cached statistic, that is or will be cached
   */
  protected void copyStats(ZcStat userStat, ZcStat cachedStat) {
    Preconditions.checkState(!closed);
    if (userStat != null && cachedStat != null) {
      userStat.set(cachedStat);
    }
  }

  /**
   * Clears this cache.
   */
  void clear() {
    Preconditions.checkState(!closed);
    nodeCache.clear();
    updateCount.incrementAndGet();
    log.trace("{} cleared all from cache", cacheId);
  }

  public void close() {
    clear();
    closed = true;
  }

  /**
   * Returns a monotonically increasing count of the number of time the cache was updated. If the
   * count is the same, then it means cache did not change.
   */
  public long getUpdateCount() {
    Preconditions.checkState(!closed);
    return updateCount.get();
  }

  /**
   * Checks if a data value (or lack of one) is cached.
   *
   * @param zPath path of node
   * @return true if data value is cached
   */
  @VisibleForTesting
  public boolean dataCached(String zPath) {
    ensureWatched(zPath);
    var zcn = nodeCache.get(zPath);
    return zcn != null && zcn.cachedData();
  }

  /**
   * Checks if children of a node (or lack of them) are cached.
   *
   * @param zPath path of node
   * @return true if children are cached
   */
  @VisibleForTesting
  public boolean childrenCached(String zPath) {
    ensureWatched(zPath);
    var zcn = nodeCache.get(zPath);
    return zcn != null && zcn.cachedChildren();
  }

  /**
   * Removes all paths in the cache match the predicate.
   */
  public void clear(Predicate<String> pathPredicate) {
    Preconditions.checkState(!closed);
    Predicate<String> pathPredicateWrapper = path -> {
      boolean testResult = pathPredicate.test(path);
      if (testResult) {
        updateCount.incrementAndGet();
        log.trace("{} removing {} from cache", cacheId, path);
      }
      return testResult;
    };
    nodeCache.keySet().removeIf(pathPredicateWrapper);
  }

  /**
   * Clears this cache of all information about nodes rooted at the given path.
   *
   * @param zPath path of top node
   */
  public void clear(String zPath) {
    ensureWatched(zPath);
    clear(path -> path.startsWith(zPath));
  }

  /**
   * Gets the lock data from the node in the cache at the specified path
   */
  public Optional<ServiceLockData> getLockData(ServiceLockPath path) {
    ensureWatched(path.toString());
    List<String> children = ServiceLock.validateAndSort(path, getChildren(path.toString()));
    if (children == null || children.isEmpty()) {
      return Optional.empty();
    }
    String lockNode = children.get(0);

    byte[] lockData = get(path + "/" + lockNode);
    if (log.isTraceEnabled()) {
      log.trace("{} Data from lockNode {} is {}", cacheId, lockNode, new String(lockData, UTF_8));
    }
    if (lockData == null) {
      lockData = new byte[0];
    }
    return ServiceLockData.parse(lockData);
  }

}

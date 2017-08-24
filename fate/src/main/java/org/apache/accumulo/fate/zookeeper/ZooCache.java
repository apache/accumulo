/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.accumulo.fate.zookeeper;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Collections;
import java.util.ConcurrentModificationException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.LockSupport;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;

/**
 * A cache for values stored in ZooKeeper. Values are kept up to date as they change.
 */
public class ZooCache {
  private static final Logger log = LoggerFactory.getLogger(ZooCache.class);

  private final ZCacheWatcher watcher = new ZCacheWatcher();
  private final Watcher externalWatcher;

  private final ReadWriteLock cacheLock = new ReentrantReadWriteLock(false);
  private final Lock cacheWriteLock = cacheLock.writeLock();
  private final Lock cacheReadLock = cacheLock.readLock();

  private final HashMap<String,byte[]> cache;
  private final HashMap<String,Stat> statCache;
  private final HashMap<String,List<String>> childrenCache;

  private final ZooReader zReader;

  /**
   * Returns a ZooKeeper session. Calls should be made within run of ZooRunnable after caches are checked. This will be performed at each retry of the run
   * method. Calls to {@link #getZooKeeper()} should be made, ideally, after cache checks since other threads may have succeeded when updating the cache. Doing
   * this will ensure that we don't pay the cost of retrieving a ZooKeeper session on each retry until we've ensured the caches aren't populated for a given
   * node.
   *
   * @return ZooKeeper session.
   */
  private ZooKeeper getZooKeeper() {
    return zReader.getZooKeeper();
  }

  private class ZCacheWatcher implements Watcher {
    @Override
    public void process(WatchedEvent event) {

      if (log.isTraceEnabled()) {
        log.trace("{}", event);
      }

      switch (event.getType()) {
        case NodeDataChanged:
        case NodeChildrenChanged:
        case NodeCreated:
        case NodeDeleted:
          remove(event.getPath());
          break;
        case None:
          switch (event.getState()) {
            case Disconnected:
              if (log.isTraceEnabled())
                log.trace("Zoo keeper connection disconnected, clearing cache");
              clear();
              break;
            case SyncConnected:
              break;
            case Expired:
              if (log.isTraceEnabled())
                log.trace("Zoo keeper connection expired, clearing cache");
              clear();
              break;
            default:
              log.warn("Unhandled: {}", event);
              break;
          }
          break;
        default:
          log.warn("Unhandled: {}", event);
          break;
      }

      if (externalWatcher != null) {
        externalWatcher.process(event);
      }
    }
  }

  /**
   * Creates a new cache.
   *
   * @param zooKeepers
   *          comma-separated list of ZooKeeper host[:port]s
   * @param sessionTimeout
   *          ZooKeeper session timeout
   */
  public ZooCache(String zooKeepers, int sessionTimeout) {
    this(zooKeepers, sessionTimeout, null);
  }

  /**
   * Creates a new cache. The given watcher is called whenever a watched node changes.
   *
   * @param zooKeepers
   *          comma-separated list of ZooKeeper host[:port]s
   * @param sessionTimeout
   *          ZooKeeper session timeout
   * @param watcher
   *          watcher object
   */
  public ZooCache(String zooKeepers, int sessionTimeout, Watcher watcher) {
    this(new ZooReader(zooKeepers, sessionTimeout), watcher);
  }

  /**
   * Creates a new cache. The given watcher is called whenever a watched node changes.
   *
   * @param reader
   *          ZooKeeper reader
   * @param watcher
   *          watcher object
   */
  public ZooCache(ZooReader reader, Watcher watcher) {
    this.zReader = reader;
    this.cache = new HashMap<>();
    this.statCache = new HashMap<>();
    this.childrenCache = new HashMap<>();
    this.externalWatcher = watcher;
  }

  private abstract class ZooRunnable<T> {
    /**
     * Runs an operation against ZooKeeper. Retries are performed by the retry method when KeeperExceptions occur.
     *
     * Changes were made in ACCUMULO-4388 so that the run method no longer accepts Zookeeper as an argument, and instead relies on the ZooRunnable
     * implementation to call {@link #getZooKeeper()}. Performing the call to retrieving a ZooKeeper Session after caches are checked has the benefit of
     * limiting ZK connections and blocking as a result of obtaining these sessions.
     *
     * @return T the result of the runnable
     */
    abstract T run() throws KeeperException, InterruptedException;

    /**
     * Retry will attempt to call the run method. Run should make a call to {@link #getZooKeeper()} after checks to cached information are made. This change,
     * per ACCUMULO-4388 ensures that we don't create a ZooKeeper session when information is cached, and access to ZooKeeper is unnecessary.
     *
     * @return result of the runnable access success ( i.e. no exceptions ).
     */
    public T retry() {

      int sleepTime = 100;

      while (true) {

        try {
          return run();
        } catch (KeeperException e) {
          final Code code = e.code();
          if (code == Code.NONODE) {
            log.error("Looked up non-existent node in cache " + e.getPath(), e);
          } else if (code == Code.CONNECTIONLOSS || code == Code.OPERATIONTIMEOUT || code == Code.SESSIONEXPIRED) {
            log.warn("Saw (possibly) transient exception communicating with ZooKeeper, will retry", e);
          } else {
            log.warn("Zookeeper error, will retry", e);
          }
        } catch (InterruptedException e) {
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
          sleepTime = (int) (sleepTime + sleepTime * Math.random());
        }
      }
    }

  }

  /**
   * Gets the children of the given node. A watch is established by this call.
   *
   * @param zPath
   *          path of node
   * @return children list, or null if node has no children or does not exist
   */
  public synchronized List<String> getChildren(final String zPath) {

    ZooRunnable<List<String>> zr = new ZooRunnable<List<String>>() {

      @Override
      public List<String> run() throws KeeperException, InterruptedException {
        try {
          cacheReadLock.lock();
          if (childrenCache.containsKey(zPath)) {
            return childrenCache.get(zPath);
          }
        } finally {
          cacheReadLock.unlock();
        }

        cacheWriteLock.lock();
        try {
          if (childrenCache.containsKey(zPath)) {
            return childrenCache.get(zPath);
          }

          final ZooKeeper zooKeeper = getZooKeeper();

          List<String> children = zooKeeper.getChildren(zPath, watcher);
          childrenCache.put(zPath, children);
          return children;
        } catch (KeeperException ke) {
          if (ke.code() != Code.NONODE) {
            throw ke;
          }
        } finally {
          cacheWriteLock.unlock();
        }
        return null;
      }

    };

    List<String> children = zr.retry();

    if (children == null) {
      return null;
    }
    return Collections.unmodifiableList(children);
  }

  /**
   * Gets data at the given path. Status information is not returned. A watch is established by this call.
   *
   * @param zPath
   *          path to get
   * @return path data, or null if non-existent
   */
  public byte[] get(final String zPath) {
    return get(zPath, null);
  }

  /**
   * Gets data at the given path, filling status information into the given <code>Stat</code> object. A watch is established by this call.
   *
   * @param zPath
   *          path to get
   * @param status
   *          status object to populate
   * @return path data, or null if non-existent
   */
  public byte[] get(final String zPath, final Stat status) {
    ZooRunnable<byte[]> zr = new ZooRunnable<byte[]>() {

      @Override
      public byte[] run() throws KeeperException, InterruptedException {
        Stat stat = null;
        cacheReadLock.lock();
        try {
          if (cache.containsKey(zPath)) {
            stat = statCache.get(zPath);
            copyStats(status, stat);
            return cache.get(zPath);
          }
        } finally {
          cacheReadLock.unlock();
        }

        /*
         * The following call to exists() is important, since we are caching that a node does not exist. Once the node comes into existence, it will be added to
         * the cache. But this notification of a node coming into existence will only be given if exists() was previously called. If the call to exists() is
         * bypassed and only getData() is called with a special case that looks for Code.NONODE in the KeeperException, then non-existence can not be cached.
         */
        cacheWriteLock.lock();
        try {
          final ZooKeeper zooKeeper = getZooKeeper();
          stat = zooKeeper.exists(zPath, watcher);
          byte[] data = null;
          if (stat == null) {
            if (log.isTraceEnabled()) {
              log.trace("zookeeper did not contain {}", zPath);
            }
          } else {
            try {
              data = zooKeeper.getData(zPath, watcher, stat);
            } catch (KeeperException.BadVersionException e1) {
              throw new ConcurrentModificationException();
            } catch (KeeperException.NoNodeException e2) {
              throw new ConcurrentModificationException();
            }
            if (log.isTraceEnabled()) {
              log.trace("zookeeper contained {} {}", zPath, (data == null ? null : new String(data, UTF_8)));
            }
          }
          put(zPath, data, stat);
          copyStats(status, stat);
          statCache.put(zPath, stat);
          return data;
        } finally {
          cacheWriteLock.unlock();
        }
      }
    };

    return zr.retry();
  }

  /**
   * Helper method to copy stats from the cached stat into userStat
   *
   * @param userStat
   *          user Stat object
   * @param cachedStat
   *          cached statistic, that is or will be cached
   */
  protected void copyStats(Stat userStat, Stat cachedStat) {
    if (userStat != null && cachedStat != null) {
      try {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);
        cachedStat.write(dos);
        dos.close();

        ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
        DataInputStream dis = new DataInputStream(bais);
        userStat.readFields(dis);

        dis.close();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  private void put(String zPath, byte[] data, Stat stat) {
    cacheWriteLock.lock();
    try {
      cache.put(zPath, data);
      statCache.put(zPath, stat);
    } finally {
      cacheWriteLock.unlock();
    }
  }

  private void remove(String zPath) {
    cacheWriteLock.lock();
    try {
      cache.remove(zPath);
      childrenCache.remove(zPath);
      statCache.remove(zPath);
    } finally {
      cacheWriteLock.unlock();
    }
  }

  /**
   * Clears this cache.
   */
  public synchronized void clear() {
    cacheWriteLock.lock();
    try {
      cache.clear();
      childrenCache.clear();
      statCache.clear();
    } finally {
      cacheWriteLock.unlock();
    }
  }

  /**
   * Checks if a data value (or lack of one) is cached.
   *
   * @param zPath
   *          path of node
   * @return true if data value is cached
   */
  @VisibleForTesting
  synchronized boolean dataCached(String zPath) {
    return cache.containsKey(zPath);
  }

  /**
   * Checks if children of a node (or lack of them) are cached.
   *
   * @param zPath
   *          path of node
   * @return true if children are cached
   */
  @VisibleForTesting
  boolean childrenCached(String zPath) {
    cacheReadLock.lock();
    try {
      return childrenCache.containsKey(zPath);
    } finally {
      cacheReadLock.unlock();
    }
  }

  /**
   * Clears this cache of all information about nodes rooted at the given path.
   *
   * @param zPath
   *          path of top node
   */
  public void clear(String zPath) {
    cacheWriteLock.lock();
    try {
      for (Iterator<String> i = cache.keySet().iterator(); i.hasNext();) {
        String path = i.next();
        if (path.startsWith(zPath))
          i.remove();
      }

      for (Iterator<String> i = childrenCache.keySet().iterator(); i.hasNext();) {
        String path = i.next();
        if (path.startsWith(zPath))
          i.remove();
      }

      for (Iterator<String> i = statCache.keySet().iterator(); i.hasNext();) {
        String path = i.next();
        if (path.startsWith(zPath))
          i.remove();
      }
    } finally {
      cacheWriteLock.unlock();
    }
  }

}

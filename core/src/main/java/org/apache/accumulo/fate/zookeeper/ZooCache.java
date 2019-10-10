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

import java.security.SecureRandom;
import java.util.Collections;
import java.util.ConcurrentModificationException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.LockSupport;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.accumulo.core.Constants;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

/**
 * A cache for values stored in ZooKeeper. Values are kept up to date as they change.
 */
public class ZooCache {
  private static final Logger log = LoggerFactory.getLogger(ZooCache.class);

  public final static Pattern TABLE_SETTING_CONFIG_PATTERN =
      Pattern.compile("(/accumulo/[0-9a-z-]+)(" + Constants.ZTABLES + ")(/.*)("
          + Constants.ZTABLE_CONF + ")/(table.*|tserver.*)");

  public final static Pattern ZNODE_PATTERN = Pattern.compile("(/accumulo/[0-9a-z-]+)/.*");

  private final ZCacheWatcher watcher = new ZCacheWatcher();
  private final Watcher externalWatcher;

  private final ReadWriteLock cacheLock = new ReentrantReadWriteLock(false);
  private final Lock cacheWriteLock = cacheLock.writeLock();
  private final Lock cacheReadLock = cacheLock.readLock();

  private final HashMap<String,byte[]> cache;
  private final HashMap<String,ZcStat> statCache;
  private final HashMap<String,List<String>> childrenCache;

  private final ZooReader zReader;
  private final SecureRandom secureRandom = new SecureRandom();

  private volatile boolean closed = false;
  private static boolean tableConfigWatcherSet = false;
  private static int lastTableConfigVersion = 0;

  public static class ZcStat {
    private long ephemeralOwner;
    private long mzxid;

    public ZcStat() {}

    private ZcStat(Stat stat) {
      this.ephemeralOwner = stat.getEphemeralOwner();
      this.mzxid = stat.getMzxid();
    }

    public long getEphemeralOwner() {
      return ephemeralOwner;
    }

    private void set(ZcStat cachedStat) {
      this.ephemeralOwner = cachedStat.ephemeralOwner;
      this.mzxid = cachedStat.mzxid;
    }

    @VisibleForTesting
    public void setEphemeralOwner(long ephemeralOwner) {
      this.ephemeralOwner = ephemeralOwner;
    }

    public long getMzxid() {
      return mzxid;
    }
  }

  private static class ImmutableCacheCopies {
    final Map<String,byte[]> cache;
    final Map<String,ZcStat> statCache;
    final Map<String,List<String>> childrenCache;
    final long updateCount;

    ImmutableCacheCopies(long updateCount) {
      this.updateCount = updateCount;
      cache = Collections.emptyMap();
      statCache = Collections.emptyMap();
      childrenCache = Collections.emptyMap();
    }

    ImmutableCacheCopies(long updateCount, Map<String,byte[]> cache, Map<String,ZcStat> statCache,
        Map<String,List<String>> childrenCache) {
      this.updateCount = updateCount;
      this.cache = Collections.unmodifiableMap(new HashMap<>(cache));
      this.statCache = Collections.unmodifiableMap(new HashMap<>(statCache));
      this.childrenCache = Collections.unmodifiableMap(new HashMap<>(childrenCache));
    }

    ImmutableCacheCopies(long updateCount, ImmutableCacheCopies prev,
        Map<String,List<String>> childrenCache) {
      this.updateCount = updateCount;
      this.cache = prev.cache;
      this.statCache = prev.statCache;
      this.childrenCache = Collections.unmodifiableMap(new HashMap<>(childrenCache));
    }

    ImmutableCacheCopies(long updateCount, Map<String,byte[]> cache, Map<String,ZcStat> statCache,
        ImmutableCacheCopies prev) {
      this.updateCount = updateCount;
      this.cache = Collections.unmodifiableMap(new HashMap<>(cache));
      this.statCache = Collections.unmodifiableMap(new HashMap<>(statCache));
      this.childrenCache = prev.childrenCache;
    }
  }

  private volatile ImmutableCacheCopies immutableCache = new ImmutableCacheCopies(0);
  private long updateCount = 0;

  /**
   * Returns a ZooKeeper session. Calls should be made within run of ZooRunnable after caches are
   * checked. This will be performed at each retry of the run method. Calls to this method should be
   * made, ideally, after cache checks since other threads may have succeeded when updating the
   * cache. Doing this will ensure that we don't pay the cost of retrieving a ZooKeeper session on
   * each retry until we've ensured the caches aren't populated for a given node.
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
      setWatcherForTableConfigVersion(event);

      switch (event.getType()) {
        case NodeDataChanged:
          if (event.getPath().endsWith(Constants.ZTABLE_CONFIG_VERSION)) {
            processTableConfigurationItem(event);
            break;
          }
        case NodeChildrenChanged:
        case NodeCreated:
        case NodeDeleted:
          remove(event.getPath());
          break;
        case None:
          switch (event.getState()) {
            case Disconnected:
              if (log.isTraceEnabled()) {
                log.trace("Zoo keeper connection disconnected, clearing cache");
              }
              clear();
              break;
            case SyncConnected:
              break;
            case Expired:
              if (log.isTraceEnabled()) {
                log.trace("Zoo keeper connection expired, clearing cache");
              }
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

    /**
     * The processTableConfigurationItem function is only called when there is a NodeDataChanged
     * event on the "table_config_version" znode.
     *
     * It resets the watcher on the "table_config_version" znode and extracts the last modified
     * table config path from the table_config_version znode. If the version of the
     * table_config_version znode is what this process expects to be the next one we simply emulate
     * what we used to do and remove that actual table configuration property's zpath from the
     * ZooCache. If the version is not what we expect to be the next then we remove all the table
     * config properties from the ZooCache as specified in Accumulo Issue #1225. We gain a little
     * efficiency in ZooKeeper by trying to keep track of the version of the table_config_version
     * znode which Zookeeper generates on its own. It would be unwise to try to calculate the
     * version on our own since many processes on different machines will be setting table
     * configuration properties.
     *
     * @param event
     *          Contains the Zpath of the table_config_version znode.
     */
    private synchronized void processTableConfigurationItem(WatchedEvent event) {

      if (event.getPath() == null || event.getPath().isEmpty())
        return;

      try {
        Stat versionStat = getZooKeeper().exists(event.getPath(), watcher);
        if (versionStat == null) {
          return;
        }

        byte[] configToRefresh = getZooKeeper().getData(event.getPath(), true, versionStat);

        if (configToRefresh == null) {
          return;
        }

        if ((versionStat != null) && (versionStat.getVersion() - lastTableConfigVersion == 1)) {
          lastTableConfigVersion = versionStat.getVersion();
          refreshTableConfig(configToRefresh);
          if (log.isTraceEnabled()) {
            log.trace(
                "Successfully refreshed table single table config: " + new String(configToRefresh));
          }
        } else {

          if (log.isTraceEnabled()) {
            log.trace("We have to update all table configs.");
          }

          if (versionStat != null) {
            lastTableConfigVersion = versionStat.getVersion();
            updateAllTableConfigurations();
          }
        }

      } catch (Exception e) {
        log.error("Error getting data from TABLE_CONFIGS zoonode " + e.getMessage());
      }

      return;
    }

    /**
     * Remove all table configuration items for all tables from the cache.
     */
    private synchronized void updateAllTableConfigurations() {

      for (String cachedItem : cache.keySet()) {
        if (cachedItem == null || cachedItem.isEmpty())
          continue;

        Matcher tableConfigMatcher = TABLE_SETTING_CONFIG_PATTERN.matcher(cachedItem);
        if (tableConfigMatcher.matches()) {
          remove(cachedItem);
        }
      }

    }

    /**
     * Create the first watcher for the /accumulo/{InstanceID}/table-config-version znode.
     *
     * It only operates on the table-config-version znode. Watchers need to be set on a node if a
     * watch event is ever to be triggered. Watch events are triggered by a data change, addition or
     * deletion of a znode. Watches, once triggered need to be reset. (The reset of the watcher set
     * here will be done in the ZCacheWatcher.processTableConfigurationItem function.) The change to
     * the table-config-version znode occurs during setTableProperty and removeTableProperty calls
     * in the TablePropUtil class. During setTableProperty and removeTableProperty calls, the data
     * value of the table-config-version znode will also be changed in addition to the actual table
     * configuration property (that will no longer be watched). Everytime the data in the
     * table-config-version znode changes the internal "data version" tracked by Zookeeper will be
     * automatically incremented. Accumulo does not have to compute what that version is. Zookeeper
     * does that by itself. The ZooCache object will track this version and use it to maintain the
     * cache. Many processes will be using it so when the "lastTableConfigVersion member" does not
     * match the actual version of the table-config-version znode we just remove all the table
     * configurations (in the particular ZooCache instance) as was specified in the Accumulo Issue
     * #1225. What I found is that we can usually rely on events coming to the ZCacheWatcher in
     * order and we only need to remove one table configuration from the ZooCache. The one that just
     * got changed, added, or deleted. This should lead to far few calls to getData on Zookeeper
     * during runtime. This is accomplished by setting the data of the table-config-version Znode to
     * the ZooPath of the table configuration that was modified. This zPath with be removed from the
     * ZooCache in the ZCacheWathcer.process(). * It will be restored in the ZooCache the next time
     * ZooCache.get(String) is called for that table configuration path. This was how ZooCached
     * worked when we watched all table configurations and we are just emulating it now using only
     * one watched node for all the configuration items - the "table-config-version node".
     *
     * @param event
     *          Contains a Zpath which the will be used to extract the accumulo instance id which
     *          will be used to create the Zpath to the table_config_version znode so a initial
     *          watcher can be put on it.
     */
    private synchronized void setWatcherForTableConfigVersion(WatchedEvent event) {

      if (event.getPath() == null || event.getPath().isEmpty())
        return;

      if (!tableConfigWatcherSet) {
        Matcher znodeMatcher = ZNODE_PATTERN.matcher(event.getPath());
        if (znodeMatcher.matches()) {
          String pathPrefix = znodeMatcher.group(1);
          try {

            Stat versionStat =
                getZooKeeper().exists(pathPrefix + Constants.ZTABLE_CONFIG_VERSION, watcher);
            if (versionStat != null) {
              lastTableConfigVersion = versionStat.getVersion();
              tableConfigWatcherSet = true;
              if (log.isTraceEnabled())
                log.trace("Successfully set table_config watcher");
            }

          } catch (KeeperException ke) {
            log.error("Could not set watcher on " + pathPrefix + Constants.ZTABLE_CONFIG_VERSION
                + " will retry later " + ke.getMessage());
          } catch (InterruptedException ie) {
            log.error("Could not set watcher on " + pathPrefix + Constants.ZTABLE_CONFIG_VERSION
                + " will retry later " + ie.getMessage());
          }
        }
      }
    }
  }

  /**
   * Removes a ZPath that matches the TABLE_SETTING_CONFIG_PATTERN regex from the ZCache so it can
   * be refreshed by a getData call to ZooKeeper when get(zpath) is called the next time the
   * effected Zpath.
   *
   * @param configToRefresh
   *          the byte array obtained from the getData call to the table_config_version node which
   *          holds the ZPath of the last table configuration znode that was modified.
   */
  private void refreshTableConfig(byte[] configToRefresh) {
    if (configToRefresh == null)
      return;

    String strConfigToRefresh = new String(configToRefresh);
    if (strConfigToRefresh.isEmpty())
      return;

    Matcher tableConfigMatcher = TABLE_SETTING_CONFIG_PATTERN.matcher(strConfigToRefresh);
    if (!tableConfigMatcher.matches())
      return;

    if (log.isTraceEnabled()) {
      log.trace("NodeDataChanged called refreshTableConfig and refreshed table config: "
          + new String(configToRefresh));
    }

    remove(strConfigToRefresh);

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

  public ZooCache(ZooReader reader) {
    this(reader, null);
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
        } catch (KeeperException e) {
          final Code code = e.code();
          if (code == Code.NONODE) {
            log.error("Looked up non-existent node in cache " + e.getPath(), e);
          } else if (code == Code.CONNECTIONLOSS || code == Code.OPERATIONTIMEOUT
              || code == Code.SESSIONEXPIRED) {
            log.warn("Saw (possibly) transient exception communicating with ZooKeeper, will retry",
                e);
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
          sleepTime = (int) (sleepTime + sleepTime * secureRandom.nextDouble());
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
  public List<String> getChildren(final String zPath) {
    Preconditions.checkState(!closed);

    ZooRunnable<List<String>> zr = new ZooRunnable<>() {

      @Override
      public List<String> run() throws KeeperException, InterruptedException {

        // only read volatile once for consistency
        ImmutableCacheCopies lic = immutableCache;
        if (lic.childrenCache.containsKey(zPath)) {
          return lic.childrenCache.get(zPath);
        }

        cacheWriteLock.lock();
        try {
          if (childrenCache.containsKey(zPath)) {
            return childrenCache.get(zPath);
          }

          final ZooKeeper zooKeeper = getZooKeeper();

          List<String> children = zooKeeper.getChildren(zPath, watcher);
          if (children != null) {
            children = List.copyOf(children);
          }
          childrenCache.put(zPath, children);
          immutableCache = new ImmutableCacheCopies(++updateCount, immutableCache, childrenCache);
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

    return zr.retry();
  }

  /**
   * Gets data at the given path. Status information is not returned. A watch is established by this
   * call.
   *
   * @param zPath
   *          path to get
   * @return path data, or null if non-existent
   */
  public byte[] get(final String zPath) {
    return get(zPath, null);
  }

  /**
   * Gets data at the given path, filling status information into the given <code>Stat</code>
   * object. A watch is established by this call.
   *
   * @param zPath
   *          path to get
   * @param status
   *          status object to populate
   * @return path data, or null if non-existent
   */
  public byte[] get(final String zPath, final ZcStat status) {
    Preconditions.checkState(!closed);
    ZooRunnable<byte[]> zr = new ZooRunnable<>() {

      @Override
      public byte[] run() throws KeeperException, InterruptedException {
        ZcStat zstat = null;

        // only read volatile once so following code works with a consistent snapshot
        ImmutableCacheCopies lic = immutableCache;
        byte[] val = lic.cache.get(zPath);
        if (val != null || lic.cache.containsKey(zPath)) {
          if (status != null) {
            zstat = lic.statCache.get(zPath);
            copyStats(status, zstat);
          }
          return val;
        }

        /*
         * The following call to exists() is important, since we are caching that a node does not
         * exist. Once the node comes into existence, it will be added to the cache. But this
         * notification of a node coming into existence will only be given if exists() was
         * previously called. If the call to exists() is bypassed and only getData() is called with
         * a special case that looks for Code.NONODE in the KeeperException, then non-existence can
         * not be cached.
         */

        Matcher configMatcher = TABLE_SETTING_CONFIG_PATTERN.matcher(zPath);
        boolean createWatch = !configMatcher.matches();

        cacheWriteLock.lock();
        try {
          final ZooKeeper zooKeeper = getZooKeeper();
          Stat stat = zooKeeper.exists(zPath, createWatch ? watcher : null);
          byte[] data = null;
          if (stat == null) {
            if (log.isTraceEnabled()) {
              log.trace("zookeeper did not contain {}", zPath);
            }
          } else {
            try {
              data = zooKeeper.getData(zPath, createWatch ? watcher : null, stat);
              zstat = new ZcStat(stat);
            } catch (KeeperException.BadVersionException | KeeperException.NoNodeException e1) {
              throw new ConcurrentModificationException();
            }
            if (log.isTraceEnabled()) {
              log.trace("zookeeper contained {} {}", zPath,
                  (data == null ? null : new String(data, UTF_8)));
            }
          }
          put(zPath, data, zstat);
          copyStats(status, zstat);
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
  protected void copyStats(ZcStat userStat, ZcStat cachedStat) {
    Preconditions.checkState(!closed);
    if (userStat != null && cachedStat != null) {
      userStat.set(cachedStat);
    }
  }

  private void put(String zPath, byte[] data, ZcStat stat) {
    cacheWriteLock.lock();
    try {
      cache.put(zPath, data);
      statCache.put(zPath, stat);

      immutableCache = new ImmutableCacheCopies(++updateCount, cache, statCache, immutableCache);
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

      immutableCache = new ImmutableCacheCopies(++updateCount, cache, statCache, childrenCache);
    } finally {
      cacheWriteLock.unlock();
    }
  }

  /**
   * Clears this cache.
   */
  public void clear() {
    Preconditions.checkState(!closed);
    cacheWriteLock.lock();
    try {
      cache.clear();
      childrenCache.clear();
      statCache.clear();

      immutableCache = new ImmutableCacheCopies(++updateCount);
    } finally {
      cacheWriteLock.unlock();
    }
  }

  public void close() {
    closed = true;
  }

  /**
   * Returns a monotonically increasing count of the number of time the cache was updated. If the
   * count is the same, then it means cache did not change.
   */
  public long getUpdateCount() {
    Preconditions.checkState(!closed);
    return immutableCache.updateCount;
  }

  /**
   * Checks if a data value (or lack of one) is cached.
   *
   * @param zPath
   *          path of node
   * @return true if data value is cached
   */
  @VisibleForTesting
  boolean dataCached(String zPath) {
    cacheReadLock.lock();
    try {
      return immutableCache.cache.containsKey(zPath) && cache.containsKey(zPath);
    } finally {
      cacheReadLock.unlock();
    }
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
      return immutableCache.childrenCache.containsKey(zPath) && childrenCache.containsKey(zPath);
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

    Preconditions.checkState(!closed);
    cacheWriteLock.lock();
    try {
      cache.keySet().removeIf(path -> path.startsWith(zPath));
      childrenCache.keySet().removeIf(path -> path.startsWith(zPath));
      statCache.keySet().removeIf(path -> path.startsWith(zPath));

      immutableCache = new ImmutableCacheCopies(++updateCount, cache, statCache, childrenCache);
    } finally {
      cacheWriteLock.unlock();
    }
  }
}

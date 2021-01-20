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
package org.apache.accumulo.fate.zookeeper;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.UUID;

import org.apache.accumulo.fate.zookeeper.ZooCache.ZcStat;
import org.apache.accumulo.fate.zookeeper.ZooUtil.LockID;
import org.apache.accumulo.fate.zookeeper.ZooUtil.NodeMissingPolicy;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZooLock implements Watcher {
  private static final Logger LOG = LoggerFactory.getLogger(ZooLock.class);

  private static final String ZLOCK_PREFIX = "zlock#";
  private static final String ZLOCK_UUID = UUID.randomUUID().toString();
  private static final String THIS_ZLOCK_PREFIX = ZLOCK_PREFIX + ZLOCK_UUID + "#";

  private static ZooCache LOCK_DATA_ZOO_CACHE;

  public enum LockLossReason {
    LOCK_DELETED, SESSION_EXPIRED
  }

  public interface LockWatcher {
    void lostLock(LockLossReason reason);

    /**
     * lost the ability to monitor the lock node, and its status is unknown
     */
    void unableToMonitorLockNode(Exception e);
  }

  public interface AccumuloLockWatcher extends LockWatcher {
    void acquiredLock();

    void failedToAcquireLock(Exception e);
  }

  private final String path;
  protected final ZooReaderWriter zooKeeper;

  private LockWatcher lockWatcher;
  private String lockNodeName;
  private volatile boolean lockWasAcquired;
  private volatile boolean watchingParent = false;

  private String createdNodeName;
  private String watchingNodeName;

  public ZooLock(ZooReaderWriter zoo, String path) {
    this(new ZooCache(zoo), zoo, path);
  }

  public ZooLock(String zookeepers, int timeInMillis, String secret, String path) {
    this(new ZooCacheFactory().getZooCache(zookeepers, timeInMillis),
        new ZooReaderWriter(zookeepers, timeInMillis, secret), path);
  }

  protected ZooLock(ZooCache zc, ZooReaderWriter zrw, String path) {
    LOCK_DATA_ZOO_CACHE = zc;
    this.path = path;
    zooKeeper = zrw;
    try {
      zooKeeper.getStatus(path, this);
      watchingParent = true;
    } catch (Exception ex) {
      LOG.warn("Error getting setting initial watch on ZooLock", ex);
      throw new RuntimeException(ex);
    }
  }

  private static class LockWatcherWrapper implements AccumuloLockWatcher {

    boolean acquiredLock = false;
    LockWatcher lw;

    public LockWatcherWrapper(LockWatcher lw2) {
      this.lw = lw2;
    }

    @Override
    public void acquiredLock() {
      acquiredLock = true;
    }

    @Override
    public void failedToAcquireLock(Exception e) {}

    @Override
    public void lostLock(LockLossReason reason) {
      lw.lostLock(reason);
    }

    @Override
    public void unableToMonitorLockNode(Exception e) {
      lw.unableToMonitorLockNode(e);
    }

  }

  public synchronized boolean tryLock(LockWatcher lw, byte[] data)
      throws KeeperException, InterruptedException {

    LockWatcherWrapper lww = new LockWatcherWrapper(lw);

    lock(lww, data);

    if (lww.acquiredLock) {
      return true;
    }

    // If we didn't acquire the lock, then delete the path we just created
    if (createdNodeName != null) {
      String pathToDelete = path + "/" + createdNodeName;
      LOG.trace("[{}] Deleting all at path: {}", this.getZLockPrefix(), pathToDelete);
      zooKeeper.recursiveDelete(pathToDelete, NodeMissingPolicy.SKIP);
      createdNodeName = null;
    }

    return false;
  }

  public static void sortChildrenByLockPrefix(List<String> children) {
    Collections.sort(children, new Comparator<String>() {
      @Override
      public int compare(String o1, String o2) {

        // Lock should be of the form:
        // zlock#UUID#sequenceNumber
        // Example:
        // zlock#44755fbe-1c9e-40b3-8458-03abaf950d7e#0000000000

        boolean lValid = true;
        if (o1.length() <= 43) {
          lValid = false;
        }
        boolean rValid = true;
        if (o2.length() <= 43) {
          rValid = false;
        }
        // Make any invalid's sort last (higher)
        if (lValid && rValid) {
          int leftIdx = o1.lastIndexOf('#');
          int rightIdx = o2.lastIndexOf('#');
          return o1.substring(leftIdx).compareTo(o2.substring(rightIdx));
        } else if (!lValid && rValid) {
          return 1;
        } else if (lValid && !rValid) {
          return -1;
        } else {
          return 0;
        }
      }
    });
    if (LOG.isTraceEnabled()) {
      LOG.trace("Children nodes");
      for (String child : children) {
        LOG.trace("- {}", child);
      }
    }
  }

  private synchronized void determineLockOwnership(final String myLock,
      final AccumuloLockWatcher lw) throws KeeperException, InterruptedException {

    if (createdNodeName == null) {
      throw new IllegalStateException(
          "Called determineLockOwnership() when ephemeralNodeName == null");
    }

    List<String> children = new ArrayList<>();
    zooKeeper.getChildren(path).forEach(s -> {
      if (s.startsWith(ZLOCK_PREFIX)) {
        children.add(s);
      }
    });

    if (!children.contains(myLock)) {
      throw new RuntimeException("Lock attempt ephemeral node no longer exist " + myLock);
    }
    sortChildrenByLockPrefix(children);

    if (children.get(0).equals(myLock)) {
      LOG.trace("[{}] First candidate is my lock, acquiring", this.getZLockPrefix());
      if (!watchingParent) {
        throw new IllegalStateException(
            "Can not acquire lock, no longer watching parent : " + path);
      }
      this.lockWatcher = lw;
      this.lockNodeName = myLock;
      createdNodeName = null;
      lockWasAcquired = true;
      lw.acquiredLock();
    } else {
      int idx = children.indexOf(myLock);
      // Get the prefix from the prior ephemeral node
      String prev = children.get(idx - 1);
      int prefixIdx = prev.lastIndexOf('#');
      String prevPrefix = prev.substring(0, prefixIdx);

      // Find the lowest sequential ephemeral node with prevPrefix
      int i = 2;
      String lowestPrevNode = prev;
      while ((idx - i) >= 0) {
        prev = children.get(idx - i);
        i++;
        if (prev.startsWith(prevPrefix)) {
          lowestPrevNode = prev;
        } else {
          break;
        }
      }

      watchingNodeName = path + "/" + lowestPrevNode;
      final String nodeToWatch = watchingNodeName;
      LOG.trace("[{}] Establishing watch on prior node {}", this.getZLockPrefix(), nodeToWatch);
      Watcher priorNodeWatcher = new Watcher() {
        @Override
        public void process(WatchedEvent event) {
          if (LOG.isTraceEnabled()) {
            LOG.trace("[{}] Processing event:", getZLockPrefix());
            LOG.trace("- type  {}", event.getType());
            LOG.trace("- path  {}", event.getPath());
            LOG.trace("- state {}", event.getState());
          }
          boolean renew = true;
          if (event.getType() == EventType.NodeDeleted && event.getPath().equals(nodeToWatch)) {
            LOG.trace("[{}] Detected deletion of prior node {}, attempting to acquire lock",
                getZLockPrefix(), nodeToWatch);
            synchronized (ZooLock.this) {
              try {
                if (createdNodeName != null) {
                  determineLockOwnership(myLock, lw);
                } else if (LOG.isTraceEnabled()) {
                  LOG.trace("[{}] While waiting for another lock {}, {} was deleted",
                      getZLockPrefix(), nodeToWatch, myLock);
                }
              } catch (Exception e) {
                if (lockNodeName == null) {
                  // have not acquired lock yet
                  lw.failedToAcquireLock(e);
                }
              }
            }
            renew = false;
          }

          if (event.getState() == KeeperState.Expired
              || event.getState() == KeeperState.Disconnected) {
            synchronized (ZooLock.this) {
              if (lockNodeName == null) {
                lw.failedToAcquireLock(new Exception("Zookeeper Session expired / disconnected"));
              }
            }
            renew = false;
          }
          if (renew) {
            LOG.trace("[{}] Renewing watch on prior node  {}", getZLockPrefix(), nodeToWatch);
            try {
              Stat restat = zooKeeper.getStatus(nodeToWatch, this);
              if (restat == null) {
                // if stat is null from the zookeeper.exists(path, Watcher) call, then we just
                // created a Watcher on a node that does not exist. Delete the watcher we just
                // created.
                zooKeeper.getZooKeeper().removeWatches(nodeToWatch, this, WatcherType.Any, true);
                determineLockOwnership(myLock, lw);
              }
            } catch (KeeperException | InterruptedException e) {
              lw.failedToAcquireLock(new Exception("Failed to renew watch on other master node"));
            }
          }
        }

      };

      Stat stat = zooKeeper.getStatus(nodeToWatch, priorNodeWatcher);
      if (stat == null) {
        // if stat is null from the zookeeper.exists(path, Watcher) call, then we just
        // created a Watcher on a node that does not exist. Delete the watcher we just created.
        zooKeeper.getZooKeeper().removeWatches(nodeToWatch, priorNodeWatcher, WatcherType.Any,
            true);
        determineLockOwnership(myLock, lw);
      }
    }

  }

  private void lostLock(LockLossReason reason) {
    LockWatcher localLw = lockWatcher;
    lockNodeName = null;
    lockWatcher = null;

    localLw.lostLock(reason);
  }

  /*
   * Exists to be overriden in tests
   */
  protected String getZLockPrefix() {
    return THIS_ZLOCK_PREFIX;
  }

  public synchronized void lock(final AccumuloLockWatcher lw, byte[] data) {

    if (lockWatcher != null || lockNodeName != null || createdNodeName != null) {
      throw new IllegalStateException();
    }

    lockWasAcquired = false;

    try {
      final String lockPathPrefix = path + "/" + getZLockPrefix();
      // Implement recipe at https://zookeeper.apache.org/doc/current/recipes.html#sc_recipes_Locks
      // except that instead of the ephemeral lock node being of the form guid-lock- use lock-guid-.
      // Another deviation from the recipe is that we cleanup any extraneous ephermal nodes that
      // were
      // created.
      final String createPath = zooKeeper.putEphemeralSequential(lockPathPrefix, data);
      LOG.trace("[{}] Ephemeral node {} created", this.getZLockPrefix(), createPath);

      // It's possible that the call above was retried several times and multiple ephemeral nodes
      // were created but the client missed the response for some reason. Find the ephemeral nodes
      // with this ZLOCK_UUID and lowest sequential number.
      List<String> children = new ArrayList<>();
      zooKeeper.getChildren(path).forEach(s -> {
        if (s.startsWith(ZLOCK_PREFIX)) {
          children.add(s);
        }
      });
      String lowestSequentialPath = null;
      sortChildrenByLockPrefix(children);
      for (String child : children) {
        if (child.startsWith(getZLockPrefix())) {
          if (null == lowestSequentialPath) {
            if (createPath.equals(path + "/" + child)) {
              // the path returned from create is the lowest sequential one
              lowestSequentialPath = createPath;
              break;
            }
            lowestSequentialPath = path + "/" + child;
            LOG.trace("[{}] lowest sequential node found: {}", this.getZLockPrefix(),
                lowestSequentialPath);
          } else {
            LOG.info(
                "[{}] Zookeeper client missed server response, multiple ephemeral child nodes created at {}",
                this.getZLockPrefix(), lockPathPrefix);
            LOG.trace("[{}] higher sequential node found: {}, deleting it", this.getZLockPrefix(),
                child);
            zooKeeper.delete(path + "/" + child);
          }
        }
      }
      final String pathForWatcher = lowestSequentialPath;

      // Set a watcher on the lowest sequential node that we created, this handles the case
      // where the node we created is deleted or if this client becomes disconnected.
      LOG.trace("[{}] Setting watcher on {}", this.getZLockPrefix(), pathForWatcher);
      Watcher watcherForNodeWeCreated = new Watcher() {

        private void failedToAcquireLock() {
          LOG.trace("[{}] Lock deleted before acquired, setting createdNodeName {} to null",
              getZLockPrefix(), createdNodeName);
          lw.failedToAcquireLock(new Exception("Lock deleted before acquired"));
          createdNodeName = null;
        }

        @Override
        public void process(WatchedEvent event) {
          synchronized (ZooLock.this) {
            if (lockNodeName != null && event.getType() == EventType.NodeDeleted
                && event.getPath().equals(path + "/" + lockNodeName)) {
              LOG.trace("[{}] {} was deleted", getZLockPrefix(), lockNodeName);
              lostLock(LockLossReason.LOCK_DELETED);
            } else if (createdNodeName != null && event.getType() == EventType.NodeDeleted
                && event.getPath().equals(path + "/" + createdNodeName)) {
              LOG.trace("[{}] {} was deleted", getZLockPrefix(), createdNodeName);
              failedToAcquireLock();
            } else if (event.getState() != KeeperState.Disconnected
                && event.getState() != KeeperState.Expired
                && (lockNodeName != null || createdNodeName != null)) {
              LOG.debug("Unexpected event watching lock node {} {}", event, pathForWatcher);
              try {
                Stat stat2 = zooKeeper.getStatus(pathForWatcher, this);
                if (stat2 == null) {
                  // if stat is null from the zookeeper.exists(path, Watcher) call, then we just
                  // created a Watcher on a node that does not exist. Delete the watcher we just
                  // created.
                  zooKeeper.getZooKeeper().removeWatches(pathForWatcher, this, WatcherType.Any,
                      true);

                  if (lockNodeName != null)
                    lostLock(LockLossReason.LOCK_DELETED);
                  else if (createdNodeName != null)
                    failedToAcquireLock();
                }
              } catch (Exception e) {
                lockWatcher.unableToMonitorLockNode(e);
                LOG.error("Failed to stat lock node " + pathForWatcher, e);
              }
            }

          }
        }
      };

      Stat stat = zooKeeper.getStatus(pathForWatcher, watcherForNodeWeCreated);
      if (stat == null) {
        // if stat is null from the zookeeper.exists(path, Watcher) call, then we just
        // created a Watcher on a node that does not exist. Delete the watcher we just created.
        zooKeeper.getZooKeeper().removeWatches(pathForWatcher, watcherForNodeWeCreated,
            WatcherType.Any, true);
        lw.failedToAcquireLock(new Exception("Lock does not exist after create"));
        return;
      }

      createdNodeName = pathForWatcher.substring(path.length() + 1);

      // We have created a node, do we own the lock?
      determineLockOwnership(createdNodeName, lw);

    } catch (KeeperException | InterruptedException e) {
      lw.failedToAcquireLock(e);
    }
  }

  public synchronized boolean tryToCancelAsyncLockOrUnlock()
      throws InterruptedException, KeeperException {
    boolean del = false;

    if (createdNodeName != null) {
      String pathToDelete = path + "/" + createdNodeName;
      LOG.trace("[{}] Deleting all at path {} due to lock cancellation", this.getZLockPrefix(),
          pathToDelete);
      zooKeeper.recursiveDelete(pathToDelete, NodeMissingPolicy.SKIP);
      del = true;
    }

    if (lockNodeName != null) {
      unlock();
      del = true;
    }

    return del;
  }

  public synchronized void unlock() throws InterruptedException, KeeperException {
    if (lockNodeName == null) {
      throw new IllegalStateException();
    }

    LockWatcher localLw = lockWatcher;
    String localLock = lockNodeName;

    lockNodeName = null;
    lockWatcher = null;

    final String pathToDelete = path + "/" + localLock;
    LOG.trace("[{}] Deleting all at path {} due to unlock", this.getZLockPrefix(), pathToDelete);
    zooKeeper.recursiveDelete(pathToDelete, NodeMissingPolicy.SKIP);

    localLw.lostLock(LockLossReason.LOCK_DELETED);
  }

  /**
   * @return path of node that this lock is watching
   */
  public synchronized String getWatching() {
    return watchingNodeName;
  }

  public synchronized String getLockPath() {
    if (lockNodeName == null) {
      return null;
    }
    return path + "/" + lockNodeName;
  }

  public synchronized String getLockName() {
    return lockNodeName;
  }

  public synchronized LockID getLockID() {
    if (lockNodeName == null) {
      throw new IllegalStateException("Lock not held");
    }
    return new LockID(path, lockNodeName, zooKeeper.getZooKeeper().getSessionId());
  }

  /**
   * indicates if the lock was acquired in the past.... helps discriminate between the case where
   * the lock was never held, or held and lost....
   *
   * @return true if the lock was acquired, otherwise false.
   */
  public synchronized boolean wasLockAcquired() {
    return lockWasAcquired;
  }

  public synchronized boolean isLocked() {
    return lockNodeName != null;
  }

  public synchronized void replaceLockData(byte[] b) throws KeeperException, InterruptedException {
    if (getLockPath() != null)
      zooKeeper.getZooKeeper().setData(getLockPath(), b, -1);
  }

  @Override
  public synchronized void process(WatchedEvent event) {
    LOG.debug("event {} {} {}", event.getPath(), event.getType(), event.getState());

    watchingParent = false;

    if (event.getState() == KeeperState.Expired && lockNodeName != null) {
      lostLock(LockLossReason.SESSION_EXPIRED);
    } else {

      try { // set the watch on the parent node again
        zooKeeper.getStatus(path, this);
        watchingParent = true;
      } catch (KeeperException.ConnectionLossException ex) {
        // we can't look at the lock because we aren't connected, but our session is still good
        LOG.warn("lost connection to zookeeper");
      } catch (Exception ex) {
        if (lockNodeName != null || createdNodeName != null) {
          lockWatcher.unableToMonitorLockNode(ex);
          LOG.error("Error resetting watch on ZooLock {} {}",
              lockNodeName != null ? lockNodeName : createdNodeName, event, ex);
        }
      }

    }

  }

  public static boolean isLockHeld(ZooCache zc, LockID lid) {

    List<String> children = zc.getChildren(lid.path);

    if (children == null || children.isEmpty()) {
      return false;
    }

    children = new ArrayList<>(children);

    sortChildrenByLockPrefix(children);

    String lockNode = children.get(0);
    if (!lid.node.equals(lockNode))
      return false;

    ZcStat stat = new ZcStat();
    return zc.get(lid.path + "/" + lid.node, stat) != null && stat.getEphemeralOwner() == lid.eid;
  }

  public static byte[] getLockData(ZooKeeper zk, String path)
      throws KeeperException, InterruptedException {
    List<String> children = zk.getChildren(path, false);

    if (children == null || children.isEmpty()) {
      return null;
    }

    sortChildrenByLockPrefix(children);

    String lockNode = children.get(0);

    return zk.getData(path + "/" + lockNode, false, null);
  }

  public static byte[] getLockData(org.apache.accumulo.fate.zookeeper.ZooCache zc, String path,
      ZcStat stat) {

    List<String> children = zc.getChildren(path);

    if (children == null || children.isEmpty()) {
      return null;
    }

    children = new ArrayList<>(children);
    sortChildrenByLockPrefix(children);

    String lockNode = children.get(0);

    if (!lockNode.startsWith(ZLOCK_PREFIX)) {
      throw new RuntimeException("Node " + lockNode + " at " + path + " is not a lock node");
    }

    return zc.get(path + "/" + lockNode, stat);
  }

  public static long getSessionId(ZooCache zc, String path) {
    List<String> children = zc.getChildren(path);

    if (children == null || children.isEmpty()) {
      return 0;
    }

    children = new ArrayList<>(children);
    sortChildrenByLockPrefix(children);

    String lockNode = children.get(0);

    ZcStat stat = new ZcStat();
    if (zc.get(path + "/" + lockNode, stat) != null)
      return stat.getEphemeralOwner();
    return 0;
  }

  public long getSessionId() throws KeeperException, InterruptedException {
    return getSessionId(LOCK_DATA_ZOO_CACHE, path);
  }

  public static void deleteLock(ZooReaderWriter zk, String path)
      throws InterruptedException, KeeperException {
    List<String> children;

    children = zk.getChildren(path);

    if (children == null || children.isEmpty()) {
      throw new IllegalStateException("No lock is held at " + path);
    }

    sortChildrenByLockPrefix(children);

    String lockNode = children.get(0);

    if (!lockNode.startsWith(ZLOCK_PREFIX)) {
      throw new RuntimeException("Node " + lockNode + " at " + path + " is not a lock node");
    }

    String pathToDelete = path + "/" + lockNode;
    LOG.trace("Deleting all at path {} due to lock deletion", pathToDelete);
    zk.recursiveDelete(pathToDelete, NodeMissingPolicy.SKIP);

  }

  public static boolean deleteLock(ZooReaderWriter zk, String path, String lockData)
      throws InterruptedException, KeeperException {
    List<String> children;

    children = zk.getChildren(path);

    if (children == null || children.isEmpty()) {
      throw new IllegalStateException("No lock is held at " + path);
    }

    sortChildrenByLockPrefix(children);

    String lockNode = children.get(0);

    if (!lockNode.startsWith(ZLOCK_PREFIX)) {
      throw new RuntimeException("Node " + lockNode + " at " + path + " is not a lock node");
    }

    byte[] data = zk.getData(path + "/" + lockNode);

    if (lockData.equals(new String(data, UTF_8))) {
      String pathToDelete = path + "/" + lockNode;
      LOG.trace("Deleting all at path {} due to lock deletion", pathToDelete);
      zk.recursiveDelete(pathToDelete, NodeMissingPolicy.FAIL);
      return true;
    }

    return false;
  }
}

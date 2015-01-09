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

import static com.google.common.base.Charsets.UTF_8;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.accumulo.fate.zookeeper.ZooUtil.LockID;
import org.apache.accumulo.fate.zookeeper.ZooUtil.NodeMissingPolicy;
import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

public class ZooLock implements Watcher {
  protected static final Logger log = Logger.getLogger(ZooLock.class);

  public static final String LOCK_PREFIX = "zlock-";

  public enum LockLossReason {
    LOCK_DELETED, SESSION_EXPIRED
  }

  public interface LockWatcher {
    void lostLock(LockLossReason reason);

    /**
     * lost the ability to monitor the lock node, and its status is unknown
     */
    void unableToMonitorLockNode(Throwable e);
  }

  public interface AsyncLockWatcher extends LockWatcher {
    void acquiredLock();

    void failedToAcquireLock(Exception e);
  }

  private boolean lockWasAcquired;
  final private String path;
  protected final IZooReaderWriter zooKeeper;
  private String lock;
  private LockWatcher lockWatcher;
  private boolean watchingParent = false;
  private String asyncLock;

  public ZooLock(String zookeepers, int timeInMillis, String scheme, byte[] auth, String path) {
    this(new ZooCacheFactory().getZooCache(zookeepers, timeInMillis), ZooReaderWriter.getInstance(zookeepers, timeInMillis, scheme, auth), path);
  }

  protected ZooLock(ZooCache zc, IZooReaderWriter zrw, String path) {
    getLockDataZooCache = zc;
    this.path = path;
    zooKeeper = zrw;
    try {
      zooKeeper.getStatus(path, this);
      watchingParent = true;
    } catch (Exception ex) {
      log.warn("Error getting setting initial watch on ZooLock", ex);
      throw new RuntimeException(ex);
    }
  }

  private static class TryLockAsyncLockWatcher implements AsyncLockWatcher {

    boolean acquiredLock = false;
    LockWatcher lw;

    public TryLockAsyncLockWatcher(LockWatcher lw2) {
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
    public void unableToMonitorLockNode(Throwable e) {
      lw.unableToMonitorLockNode(e);
    }

  }

  public synchronized boolean tryLock(LockWatcher lw, byte data[]) throws KeeperException, InterruptedException {

    TryLockAsyncLockWatcher tlalw = new TryLockAsyncLockWatcher(lw);

    lockAsync(tlalw, data);

    if (tlalw.acquiredLock) {
      return true;
    }

    if (asyncLock != null) {
      zooKeeper.recursiveDelete(path + "/" + asyncLock, NodeMissingPolicy.SKIP);
      asyncLock = null;
    }

    return false;
  }

  private synchronized void lockAsync(final String myLock, final AsyncLockWatcher lw) throws KeeperException, InterruptedException {

    if (asyncLock == null) {
      throw new IllegalStateException("Called lockAsync() when asyncLock == null");
    }

    List<String> children = zooKeeper.getChildren(path);

    if (!children.contains(myLock)) {
      throw new RuntimeException("Lock attempt ephemeral node no longer exist " + myLock);
    }

    Collections.sort(children);
    if (log.isTraceEnabled()) {
      log.trace("Candidate lock nodes");
      for (String child : children) {
        log.trace("- " + child);
      }
    }

    if (children.get(0).equals(myLock)) {
      log.trace("First candidate is my lock, acquiring");
      if (!watchingParent) {
        throw new IllegalStateException("Can not acquire lock, no longer watching parent : " + path);
      }
      this.lockWatcher = lw;
      this.lock = myLock;
      asyncLock = null;
      lockWasAcquired = true;
      lw.acquiredLock();
      return;
    }
    String prev = null;
    for (String child : children) {
      if (child.equals(myLock)) {
        break;
      }

      prev = child;
    }

    final String lockToWatch = path + "/" + prev;
    log.trace("Establishing watch on " + lockToWatch);
    Stat stat = zooKeeper.getStatus(lockToWatch, new Watcher() {

      @Override
      public void process(WatchedEvent event) {
        if (log.isTraceEnabled()) {
          log.trace("Processing event:");
          log.trace("- type  " + event.getType());
          log.trace("- path  " + event.getPath());
          log.trace("- state " + event.getState());
        }
        boolean renew = true;
        if (event.getType() == EventType.NodeDeleted && event.getPath().equals(lockToWatch)) {
          log.trace("Detected deletion of " + lockToWatch + ", attempting to acquire lock");
          synchronized (ZooLock.this) {
            try {
              if (asyncLock != null) {
                lockAsync(myLock, lw);
              } else if (log.isTraceEnabled()) {
                log.trace("While waiting for another lock " + lockToWatch + " " + myLock + " was deleted");
              }
            } catch (Exception e) {
              if (lock == null) {
                // have not acquired lock yet
                lw.failedToAcquireLock(e);
              }
            }
          }
          renew = false;
        }

        if (event.getState() == KeeperState.Expired || event.getState() == KeeperState.Disconnected) {
          synchronized (ZooLock.this) {
            if (lock == null) {
              lw.failedToAcquireLock(new Exception("Zookeeper Session expired / disconnected"));
            }
          }
          renew = false;
        }
        if (renew) {
          log.trace("Renewing watch on " + lockToWatch);
          try {
            Stat restat = zooKeeper.getStatus(lockToWatch, this);
            if (restat == null) {
              lockAsync(myLock, lw);
            }
          } catch (KeeperException e) {
            lw.failedToAcquireLock(new Exception("Failed to renew watch on other master node"));
          } catch (InterruptedException e) {
            lw.failedToAcquireLock(new Exception("Failed to renew watch on other master node"));
          }
        }
      }

    });

    if (stat == null)
      lockAsync(myLock, lw);
  }

  private void lostLock(LockLossReason reason) {
    LockWatcher localLw = lockWatcher;
    lock = null;
    lockWatcher = null;

    localLw.lostLock(reason);
  }

  public synchronized void lockAsync(final AsyncLockWatcher lw, byte data[]) {

    if (lockWatcher != null || lock != null || asyncLock != null) {
      throw new IllegalStateException();
    }

    lockWasAcquired = false;

    try {
      final String asyncLockPath = zooKeeper.putEphemeralSequential(path + "/" + LOCK_PREFIX, data);
      log.trace("Ephemeral node " + asyncLockPath + " created");
      Stat stat = zooKeeper.getStatus(asyncLockPath, new Watcher() {

        private void failedToAcquireLock() {
          lw.failedToAcquireLock(new Exception("Lock deleted before acquired"));
          asyncLock = null;
        }

        public void process(WatchedEvent event) {
          synchronized (ZooLock.this) {
            if (lock != null && event.getType() == EventType.NodeDeleted && event.getPath().equals(path + "/" + lock)) {
              lostLock(LockLossReason.LOCK_DELETED);
            } else if (asyncLock != null && event.getType() == EventType.NodeDeleted && event.getPath().equals(path + "/" + asyncLock)) {
              failedToAcquireLock();
            } else if (event.getState() != KeeperState.Disconnected && event.getState() != KeeperState.Expired && (lock != null || asyncLock != null)) {
              log.debug("Unexpected event watching lock node " + event + " " + asyncLockPath);
              try {
                Stat stat2 = zooKeeper.getStatus(asyncLockPath, this);
                if (stat2 == null) {
                  if (lock != null)
                    lostLock(LockLossReason.LOCK_DELETED);
                  else if (asyncLock != null)
                    failedToAcquireLock();
                }
              } catch (Throwable e) {
                lockWatcher.unableToMonitorLockNode(e);
                log.error("Failed to stat lock node " + asyncLockPath, e);
              }
            }

          }
        }
      });

      if (stat == null) {
        lw.failedToAcquireLock(new Exception("Lock does not exist after create"));
        return;
      }

      asyncLock = asyncLockPath.substring(path.length() + 1);

      lockAsync(asyncLock, lw);

    } catch (KeeperException e) {
      lw.failedToAcquireLock(e);
    } catch (InterruptedException e) {
      lw.failedToAcquireLock(e);
    }
  }

  public synchronized boolean tryToCancelAsyncLockOrUnlock() throws InterruptedException, KeeperException {
    boolean del = false;

    if (asyncLock != null) {
      zooKeeper.recursiveDelete(path + "/" + asyncLock, NodeMissingPolicy.SKIP);
      del = true;
    }

    if (lock != null) {
      unlock();
      del = true;
    }

    return del;
  }

  public synchronized void unlock() throws InterruptedException, KeeperException {
    if (lock == null) {
      throw new IllegalStateException();
    }

    LockWatcher localLw = lockWatcher;
    String localLock = lock;

    lock = null;
    lockWatcher = null;

    zooKeeper.recursiveDelete(path + "/" + localLock, NodeMissingPolicy.SKIP);

    localLw.lostLock(LockLossReason.LOCK_DELETED);
  }

  public synchronized String getLockPath() {
    if (lock == null) {
      return null;
    }
    return path + "/" + lock;
  }

  public synchronized String getLockName() {
    return lock;
  }

  public synchronized LockID getLockID() {
    if (lock == null) {
      throw new IllegalStateException("Lock not held");
    }
    return new LockID(path, lock, zooKeeper.getZooKeeper().getSessionId());
  }

  /**
   * indicates if the lock was acquired in the past.... helps discriminate between the case where the lock was never held, or held and lost....
   *
   * @return true if the lock was aquired, otherwise false.
   */
  public synchronized boolean wasLockAcquired() {
    return lockWasAcquired;
  }

  public synchronized boolean isLocked() {
    return lock != null;
  }

  public synchronized void replaceLockData(byte[] b) throws KeeperException, InterruptedException {
    if (getLockPath() != null)
      zooKeeper.getZooKeeper().setData(getLockPath(), b, -1);
  }

  @Override
  public synchronized void process(WatchedEvent event) {
    log.debug("event " + event.getPath() + " " + event.getType() + " " + event.getState());

    watchingParent = false;

    if (event.getState() == KeeperState.Expired && lock != null) {
      lostLock(LockLossReason.SESSION_EXPIRED);
    } else {

      try { // set the watch on the parent node again
        zooKeeper.getStatus(path, this);
        watchingParent = true;
      } catch (KeeperException.ConnectionLossException ex) {
        // we can't look at the lock because we aren't connected, but our session is still good
        log.warn("lost connection to zookeeper");
      } catch (Exception ex) {
        if (lock != null || asyncLock != null) {
          lockWatcher.unableToMonitorLockNode(ex);
          log.error("Error resetting watch on ZooLock " + lock == null ? asyncLock : lock + " " + event, ex);
        }
      }

    }

  }

  public static boolean isLockHeld(ZooKeeper zk, LockID lid) throws KeeperException, InterruptedException {

    List<String> children = zk.getChildren(lid.path, false);

    if (children == null || children.size() == 0) {
      return false;
    }

    Collections.sort(children);

    String lockNode = children.get(0);
    if (!lid.node.equals(lockNode))
      return false;

    Stat stat = zk.exists(lid.path + "/" + lid.node, false);
    return stat != null && stat.getEphemeralOwner() == lid.eid;
  }

  public static boolean isLockHeld(ZooCache zc, LockID lid) {

    List<String> children = zc.getChildren(lid.path);

    if (children == null || children.size() == 0) {
      return false;
    }

    children = new ArrayList<String>(children);
    Collections.sort(children);

    String lockNode = children.get(0);
    if (!lid.node.equals(lockNode))
      return false;

    Stat stat = new Stat();
    return zc.get(lid.path + "/" + lid.node, stat) != null && stat.getEphemeralOwner() == lid.eid;
  }

  public static byte[] getLockData(ZooKeeper zk, String path) throws KeeperException, InterruptedException {
    List<String> children = zk.getChildren(path, false);

    if (children == null || children.size() == 0) {
      return null;
    }

    Collections.sort(children);

    String lockNode = children.get(0);

    return zk.getData(path + "/" + lockNode, false, null);
  }

  public static byte[] getLockData(org.apache.accumulo.fate.zookeeper.ZooCache zc, String path, Stat stat) {

    List<String> children = zc.getChildren(path);

    if (children == null || children.size() == 0) {
      return null;
    }

    children = new ArrayList<String>(children);
    Collections.sort(children);

    String lockNode = children.get(0);

    if (!lockNode.startsWith(LOCK_PREFIX)) {
      throw new RuntimeException("Node " + lockNode + " at " + path + " is not a lock node");
    }

    return zc.get(path + "/" + lockNode, stat);
  }

  public static long getSessionId(ZooCache zc, String path) throws KeeperException, InterruptedException {
    List<String> children = zc.getChildren(path);

    if (children == null || children.size() == 0) {
      return 0;
    }

    children = new ArrayList<String>(children);
    Collections.sort(children);

    String lockNode = children.get(0);

    Stat stat = new Stat();
    if (zc.get(path + "/" + lockNode, stat) != null)
      return stat.getEphemeralOwner();
    return 0;
  }

  private static ZooCache getLockDataZooCache;

  public long getSessionId() throws KeeperException, InterruptedException {
    return getSessionId(getLockDataZooCache, path);
  }

  public static void deleteLock(IZooReaderWriter zk, String path) throws InterruptedException, KeeperException {
    List<String> children;

    children = zk.getChildren(path);

    if (children == null || children.size() == 0) {
      throw new IllegalStateException("No lock is held at " + path);
    }

    Collections.sort(children);

    String lockNode = children.get(0);

    if (!lockNode.startsWith(LOCK_PREFIX)) {
      throw new RuntimeException("Node " + lockNode + " at " + path + " is not a lock node");
    }

    zk.recursiveDelete(path + "/" + lockNode, NodeMissingPolicy.SKIP);

  }

  public static boolean deleteLock(IZooReaderWriter zk, String path, String lockData) throws InterruptedException, KeeperException {
    List<String> children;

    children = zk.getChildren(path);

    if (children == null || children.size() == 0) {
      throw new IllegalStateException("No lock is held at " + path);
    }

    Collections.sort(children);

    String lockNode = children.get(0);

    if (!lockNode.startsWith(LOCK_PREFIX)) {
      throw new RuntimeException("Node " + lockNode + " at " + path + " is not a lock node");
    }

    byte[] data = zk.getData(path + "/" + lockNode, null);

    if (lockData.equals(new String(data, UTF_8))) {
      zk.recursiveDelete(path + "/" + lockNode, NodeMissingPolicy.FAIL);
      return true;
    }

    return false;
  }
}

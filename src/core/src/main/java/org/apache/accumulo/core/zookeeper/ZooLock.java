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
package org.apache.accumulo.core.zookeeper;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.data.Stat;

public class ZooLock implements Watcher {
  
  private static final Logger log = Logger.getLogger(ZooLock.class);
  
  public static final String LOCK_PREFIX = "zlock-";
  
  public static class LockID {
    private long eid;
    private String path;
    private String node;
    
    public LockID(String root, String serializedLID) {
      String sa[] = serializedLID.split("\\$");
      int lastSlash = sa[0].lastIndexOf('/');
      
      if (sa.length != 2 || lastSlash < 0) {
        throw new IllegalArgumentException("Malformed serialized lock id " + serializedLID);
      }
      
      if (lastSlash == 0)
        path = root;
      else
        path = root + "/" + sa[0].substring(0, lastSlash);
      node = sa[0].substring(lastSlash + 1);
      eid = Long.parseLong(sa[1]);
    }
    
    private LockID(String path, String node, long eid) {
      this.path = path;
      this.node = node;
      this.eid = eid;
    }
    
    public String serialize(String root) {
      
      return path.substring(root.length()) + "/" + node + "$" + eid;
    }
    
    @Override
    public String toString() {
      return " path = " + path + " node = " + node + " eid = " + eid;
    }
  }
  
  public enum LockLossReason {
    LOCK_DELETED, SESSION_EXPIRED
  }
  
  public interface LockWatcher {
    void lostLock(LockLossReason reason);
  }
  
  public interface AsyncLockWatcher extends LockWatcher {
    void acquiredLock();
    
    void failedToAcquireLock(Exception e);
  }
  
  private boolean lockWasAcquired;
  final private String path;
  final private ZooKeeper zooKeeper;
  private String lock;
  private LockWatcher lockWatcher;
  
  private String asyncLock;
  
  public ZooLock(String path) {
    this.path = path;
    zooKeeper = ZooSession.getSession(this);
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
    
  }
  
  public synchronized boolean tryLock(LockWatcher lw, byte data[]) throws KeeperException, InterruptedException {
    
    TryLockAsyncLockWatcher tlalw = new TryLockAsyncLockWatcher(lw);
    
    lockAsync(tlalw, data);
    
    if (tlalw.acquiredLock) {
      return true;
    }
    
    if (asyncLock != null) {
      zooKeeper.delete(path + "/" + asyncLock, -1);
      asyncLock = null;
    }
    
    return false;
  }
  
  private synchronized void lockAsync(final String myLock, final AsyncLockWatcher lw) throws KeeperException, InterruptedException {
    
    if (asyncLock == null) {
      throw new IllegalStateException("Called lockAsync() when asyncLock == null");
    }
    
    List<String> children = zooKeeper.getChildren(path, false);
    
    if (!children.contains(myLock)) {
      throw new RuntimeException("Lock attempt ephemeral node no longer exist " + myLock);
    }
    
    Collections.sort(children);
    
    if (children.get(0).equals(myLock)) {
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
    
    Stat stat = zooKeeper.exists(path + "/" + prev, new Watcher() {
      
      @Override
      public void process(WatchedEvent event) {
        
        if (event.getType() == EventType.NodeDeleted && event.getPath().equals(lockToWatch)) {
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
        }
        
        if (event.getState() == KeeperState.Expired) {
          synchronized (ZooLock.this) {
            if (lock == null) {
              lw.failedToAcquireLock(new Exception("Zookeeper Session expired"));
            }
          }
        }
      }
      
    });
    
    if (stat == null)
      lockAsync(myLock, lw);
  }
  
  public synchronized void lockAsync(final AsyncLockWatcher lw, byte data[]) {
    
    if (lockWatcher != null || lock != null || asyncLock != null) {
      throw new IllegalStateException();
    }
    
    lockWasAcquired = false;
    
    try {
      String asyncLockPath = zooKeeper.create(path + "/" + LOCK_PREFIX, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
      
      Stat stat = zooKeeper.exists(asyncLockPath, new Watcher() {
        public void process(WatchedEvent event) {
          synchronized (ZooLock.this) {
            if (lock != null && event.getType() == EventType.NodeDeleted && event.getPath().equals(path + "/" + lock)) {
              LockWatcher localLw = lockWatcher;
              lock = null;
              lockWatcher = null;
              
              localLw.lostLock(LockLossReason.LOCK_DELETED);
              
            } else if (asyncLock != null && event.getType() == EventType.NodeDeleted && event.getPath().equals(path + "/" + asyncLock)) {
              lw.failedToAcquireLock(new Exception("Lock deleted before acquired"));
              asyncLock = null;
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
      zooKeeper.delete(path + "/" + asyncLock, -1);
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
    
    zooKeeper.delete(path + "/" + localLock, -1);
    
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
    
    return new LockID(path, lock, zooKeeper.getSessionId());
  }
  
  /**
   * indicates if the lock was acquired in the past.... helps discriminate between the case where the lock was never held, or held and lost....
   * 
   * @return
   */
  public synchronized boolean wasLockAcquired() {
    return lockWasAcquired;
  }
  
  public synchronized boolean isLocked() {
    return lock != null;
  }
  
  @Override
  public synchronized void process(WatchedEvent event) {
    log.debug("event " + event.getPath() + " " + event.getType() + " " + event.getState());
    
    if (event.getState() == KeeperState.Expired && lock != null) {
      LockWatcher localLw = lockWatcher;
      lock = null;
      lockWatcher = null;
      localLw.lostLock(LockLossReason.SESSION_EXPIRED);
    }
  }
  
  public static boolean isLockHeld(ZooKeeper zk, LockID lid) throws KeeperException, InterruptedException {
    
    List<String> children = zk.getChildren(lid.path, false);
    
    if (children.size() == 0) {
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
    
    if (children.size() == 0) {
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
    
    if (children.size() == 0) {
      return null;
    }
    
    Collections.sort(children);
    
    String lockNode = children.get(0);
    
    return zk.getData(path + "/" + lockNode, false, null);
  }
  
  public static byte[] getLockData(ZooCache zc, String path) {
    
    List<String> children = zc.getChildren(path);
    
    if (children.size() == 0) {
      return null;
    }
    
    children = new ArrayList<String>(children);
    Collections.sort(children);
    
    String lockNode = children.get(0);
    
    if (!lockNode.startsWith(LOCK_PREFIX)) {
      throw new RuntimeException("Node " + lockNode + " at " + path + " is not a lock node");
    }
    
    return zc.get(path + "/" + lockNode);
  }
  
  private static ZooCache getLockDataZooCache = new ZooCache();
  
  public static byte[] getLockData(String path) {
    return getLockData(getLockDataZooCache, path);
  }
  
  public static long getSessionId(ZooCache zc, String path) throws KeeperException, InterruptedException {
    List<String> children = zc.getChildren(path);
    
    if (children.size() == 0) {
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
  
  public long getSessionId() throws KeeperException, InterruptedException {
    return getSessionId(getLockDataZooCache, path);
  }
  
  public static void deleteLock(String path) throws InterruptedException, KeeperException {
    List<String> children;
    
    children = ZooSession.getSession().getChildren(path, false);
    
    if (children.size() == 0) {
      throw new IllegalStateException("No lock is held at " + path);
    }
    
    Collections.sort(children);
    
    String lockNode = children.get(0);
    
    if (!lockNode.startsWith(LOCK_PREFIX)) {
      throw new RuntimeException("Node " + lockNode + " at " + path + " is not a lock node");
    }
    
    ZooSession.getSession().delete(path + "/" + lockNode, -1);
    
  }
  
  public static boolean deleteLock(String path, String lockData) throws InterruptedException, KeeperException {
    List<String> children;
    
    children = ZooSession.getSession().getChildren(path, false);
    
    if (children.size() == 0) {
      throw new IllegalStateException("No lock is held at " + path);
    }
    
    Collections.sort(children);
    
    String lockNode = children.get(0);
    
    if (!lockNode.startsWith(LOCK_PREFIX)) {
      throw new RuntimeException("Node " + lockNode + " at " + path + " is not a lock node");
    }
    
    Stat stat = new Stat();
    byte[] data = ZooSession.getSession().getData(path + "/" + lockNode, false, stat);
    
    if (lockData.equals(new String(data))) {
      ZooSession.getSession().delete(path + "/" + lockNode, stat.getVersion());
      return true;
    }
    
    return false;
  }
  
  public static void main(String[] args) throws Exception {
    String node = "/test/lock1";
    ZooLock zl = new ZooLock(node);
    
    zl.lockAsync(new AsyncLockWatcher() {
      
      @Override
      public void acquiredLock() {
        System.out.println("I got the lock");
      }
      
      @Override
      public void lostLock(LockLossReason reason) {
        System.out.println("OMG I lost my lock, reason = " + reason);
        
      }
      
      @Override
      public void failedToAcquireLock(Exception e) {
        System.out.println("Failed to acquire lock  ");
        e.printStackTrace();
      }
      
    }, new byte[0]);
    
    while (true) {
      Thread.sleep(1000);
    }
  }
  
}

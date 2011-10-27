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

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import org.apache.accumulo.core.util.UtilWaitThread;
import org.apache.log4j.Logger;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooKeeper.States;

class ZooSession {
  
  private static final Logger log = Logger.getLogger(ZooSession.class);
  
  private static class ZooSessionInfo {
    public ZooSessionInfo(ZooKeeper zooKeeper, AccumuloWatcher watcher) {
      this.zooKeeper = zooKeeper;
    }
    
    ZooKeeper zooKeeper;
  }
  
  private static Map<String,ZooSessionInfo> sessions = new HashMap<String,ZooSessionInfo>();
  
  private static String sessionKey(String keepers, int timeout, String auth) {
    return keepers + ":" + timeout + ":" + (auth == null ? "" : auth);
  }
  
  private static class AccumuloWatcher implements Watcher {
    
    private HashSet<Watcher> watchers = new HashSet<Watcher>();
    
    public void process(WatchedEvent event) {
      // copy the watchers, in case the callback adds() more Watchers
      // otherwise we get a ConcurrentModificationException
      Collection<Watcher> watcherCopy = new ArrayList<Watcher>(watchers);
      
      for (Watcher watcher : watcherCopy) {
        watcher.process(event);
      }
      
      if (event.getState() == KeeperState.Expired) {
        log.debug("Session expired, state of current session : " + event.getState());
      }
    }
    
  }
  
  public static ZooKeeper connect(String host, int timeout, String auth, Watcher watcher) {
    final int TIME_BETWEEN_CONNECT_CHECKS_MS = 100;
    final int TOTAL_CONNECT_TIME_WAIT_MS = 10 * 1000;
    boolean tryAgain = true;
    int sleepTime = 100;
    ZooKeeper zooKeeper = null;
    
    while (tryAgain) {
      try {
        zooKeeper = new ZooKeeper(host, timeout, watcher);
        // it may take some time to get connected to zookeeper if some of the servers are down
        for (int i = 0; i < TOTAL_CONNECT_TIME_WAIT_MS / TIME_BETWEEN_CONNECT_CHECKS_MS && tryAgain; i++) {
          if (zooKeeper.getState().equals(States.CONNECTED)) {
            if (auth != null)
              zooKeeper.addAuthInfo("digest", auth.getBytes());
            tryAgain = false;
          } else
            UtilWaitThread.sleep(TIME_BETWEEN_CONNECT_CHECKS_MS);
        }
      } catch (UnknownHostException uhe) {
        // do not expect to recover from this
        log.warn(uhe.getClass().getName() + " : " + uhe.getMessage());
        throw new RuntimeException(uhe);
      } catch (IOException e) {
        log.warn("Connection to zooKeeper failed, will try again in " + String.format("%.2f secs", sleepTime / 1000.0), e);
      } finally {
        if (tryAgain && zooKeeper != null)
          try {
            zooKeeper.close();
            zooKeeper = null;
          } catch (InterruptedException e) {
            log.warn("interrupted", e);
          }
      }
      
      if (tryAgain) {
        UtilWaitThread.sleep(sleepTime);
        if (sleepTime < 10000)
          sleepTime = (int) (sleepTime + sleepTime * Math.random());
      }
    }
    
    return zooKeeper;
  }
  
  public static synchronized ZooKeeper getSession(String zooKeepers, int timeout) {
    return getSession(zooKeepers, timeout, null);
  }
  
  public static synchronized ZooKeeper getSession(String zooKeepers, int timeout, String auth) {
    
    String sessionKey = sessionKey(zooKeepers, timeout, auth);
    
    // a read-only session can use a session with authorizations, so cache a copy for it w/out auths
    String readOnlySessionKey = sessionKey(zooKeepers, timeout, null);
    
    ZooSessionInfo zsi = sessions.get(sessionKey);
    if (zsi != null && zsi.zooKeeper.getState() == States.CLOSED) {
      if (auth != null && sessions.get(readOnlySessionKey) == zsi)
        sessions.remove(readOnlySessionKey);
      zsi = null;
      sessions.remove(sessionKey);
    }
    
    if (zsi == null) {
      AccumuloWatcher watcher = new AccumuloWatcher();
      log.debug("Connecting to " + zooKeepers + " with timeout " + timeout + " with auth");
      zsi = new ZooSessionInfo(connect(zooKeepers, timeout, auth, watcher), watcher);
      sessions.put(sessionKey, zsi);
      if (auth != null && !sessions.containsKey(readOnlySessionKey))
        sessions.put(readOnlySessionKey, zsi);
    }
    return zsi.zooKeeper;
  }
}

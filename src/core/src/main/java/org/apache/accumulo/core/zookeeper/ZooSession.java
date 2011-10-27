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

import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.util.UtilWaitThread;
import org.apache.log4j.Logger;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooKeeper.States;

public class ZooSession {
  
  private static final Logger log = Logger.getLogger(ZooSession.class);
  
  private static class ZooSessionInfo {
    public ZooSessionInfo(ZooKeeper zooKeeper, AccumuloWatcher watcher) {
      this.zooKeeper = zooKeeper;
      this.watcher = watcher;
    }
    
    ZooKeeper zooKeeper;
    AccumuloWatcher watcher;
  }
  
  private static Map<String,ZooSessionInfo> sessions = new HashMap<String,ZooSessionInfo>();
  
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
    
    public void add(Watcher w) {
      watchers.add(w);
    }
  }
  
  public static ZooKeeper connect(String host, int timeout, Watcher watcher) {
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
          if (zooKeeper.getState().equals(States.CONNECTED))
            tryAgain = false;
          else
            UtilWaitThread.sleep(TIME_BETWEEN_CONNECT_CHECKS_MS);
        }
      } catch (UnknownHostException uhe) {
        // do not expect to recover from this
        log.warn(uhe.getClass().getName() + " : " + uhe.getMessage());
        throw new RuntimeException(uhe);
      } catch (IOException e) {
        log.warn("Connection to zooKeeper failed, will try again in " + String.format("%.2f secs", sleepTime / 1000.0), e);
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
    
    ZooSessionInfo zsi = sessions.get(zooKeepers + ":" + timeout);
    if (zsi != null && zsi.zooKeeper.getState() == States.CLOSED) {
      zsi = null;
      sessions.remove(zooKeepers + ":" + timeout);
    }
    
    if (zsi == null) {
      AccumuloWatcher watcher = new AccumuloWatcher();
      zsi = new ZooSessionInfo(connect(zooKeepers, timeout, watcher), watcher);
      sessions.put(zooKeepers + ":" + timeout, zsi);
    }
    
    return zsi.zooKeeper;
  }
  
  public static synchronized ZooKeeper getSession() {
    int timeout = (int) AccumuloConfiguration.getSystemConfiguration().getTimeInMillis(Property.INSTANCE_ZK_TIMEOUT);
    return getSession(AccumuloConfiguration.getSystemConfiguration().get(Property.INSTANCE_ZK_HOST), timeout);
  }
  
  public static synchronized ZooKeeper getSession(Watcher w) {
    int timeout = (int) AccumuloConfiguration.getSystemConfiguration().getTimeInMillis(Property.INSTANCE_ZK_TIMEOUT);
    getSession(AccumuloConfiguration.getSystemConfiguration().get(Property.INSTANCE_ZK_HOST), timeout);
    ZooSessionInfo zsi = sessions.get(AccumuloConfiguration.getSystemConfiguration().get(Property.INSTANCE_ZK_HOST) + ":" + timeout);
    zsi.watcher.add(w);
    
    return zsi.zooKeeper;
  }
}

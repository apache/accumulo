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
package org.apache.accumulo.server.master;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.impl.HdfsZooInstance;
import org.apache.accumulo.core.zookeeper.ZooCache;
import org.apache.accumulo.core.zookeeper.ZooUtil;
import org.apache.log4j.Logger;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;

public class TabletServerLoggers implements Watcher {
  
  private static final Logger log = Logger.getLogger(TabletServerLoggers.class);
  
  // Map from zookeeper path to address
  private Map<String,String> names = new HashMap<String,String>();
  
  // Map from address to assignments
  private Map<String,Set<String>> loggers = new HashMap<String,Set<String>>();
  
  private ZooCache cache;
  
  private NewLoggerWatcher watcher;
  
  interface NewLoggerWatcher {
    void newLogger(String address);
  }
  
  public TabletServerLoggers(NewLoggerWatcher watcher) {
    cache = new ZooCache(this);
    this.watcher = watcher;
  }
  
  private String loggerPath() {
    return ZooUtil.getRoot(HdfsZooInstance.getInstance()) + Constants.ZLOGGERS;
  }
  
  synchronized public Map<String,String> getLoggersFromZooKeeper() {
    String path = loggerPath();
    Map<String,String> current = new HashMap<String,String>();
    for (String child : cache.getChildren(path)) {
      byte[] value = cache.get(path + "/" + child);
      if (value != null)
        current.put(child, new String(value));
    }
    return current;
  }
  
  synchronized public void scanZooKeeperForUpdates() {
    Map<String,String> current = getLoggersFromZooKeeper();
    for (Entry<String,String> entry : current.entrySet())
      log.debug("looking at logger " + entry.getKey() + " -> " + entry.getValue());
    Set<String> currentAddresses = new HashSet<String>(current.values());
    Set<String> deleted = new HashSet<String>(names.keySet());
    deleted.removeAll(current.keySet());
    Set<String> new_ = new HashSet<String>(current.keySet());
    new_.removeAll(names.keySet());
    for (String logger : deleted) {
      String address = names.get(logger);
      if (!currentAddresses.contains(address)) {
        log.info("Noticed logger went away: " + address);
        loggers.remove(address);
      }
    }
    for (String logger : new_) {
      String address = current.get(logger);
      log.debug("Adding logger " + address);
      loggers.put(address, new HashSet<String>());
      watcher.newLogger(address);
    }
    names = current;
  }
  
  synchronized public List<String> leastBusyLoggers() {
    ArrayList<String> result = new ArrayList<String>(loggers.keySet());
    Collections.sort(result, new Comparator<String>() {
      @Override
      public int compare(String o1, String o2) {
        int len1 = loggers.get(o1).size();
        int len2 = loggers.get(o2).size();
        return len1 - len2;
      }
    });
    return result;
  }
  
  synchronized void assignLoggersToTabletServer(Collection<String> updates, String tserver) {
    for (Set<String> tservers : loggers.values()) {
      tservers.remove(tserver);
    }
    for (String logger : updates) {
      loggers.get(logger).add(tserver);
    }
  }
  
  @Override
  public void process(WatchedEvent event) {
    try {
      scanZooKeeperForUpdates();
    } catch (Exception ex) {
      log.info("Got exception scanning zookeeper", ex);
    }
  }
}

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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.zookeeper.ZooUtil;
import org.apache.accumulo.server.client.HdfsZooInstance;
import org.apache.accumulo.server.zookeeper.ZooCache;
import org.apache.log4j.Logger;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;

public class TabletServerLoggers implements Watcher {
  
  private static final Logger log = Logger.getLogger(TabletServerLoggers.class);
  
  // Map from zookeeper path to address
  private Map<String,String> names = new HashMap<String,String>();
  
  private ZooCache cache;
  private LoggerWatcher watcher;
  
  interface LoggerWatcher {
    void newLogger(String address);
    
    void deadLogger(String address);
  }
  
  public TabletServerLoggers(LoggerWatcher watcher, AccumuloConfiguration conf) {
    cache = new ZooCache(conf, this);
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
    if (log.isDebugEnabled()) {
      if (current.entrySet().size() < 100) {
        for (Entry<String,String> entry : current.entrySet())
          log.debug("looking at logger " + entry.getKey() + " -> " + entry.getValue());
      } else {
        log.debug("looking at " + current.entrySet().size() + " loggers");
      }
    }
    Set<String> currentAddresses = new HashSet<String>(current.values());
    Set<String> deleted = new HashSet<String>(names.keySet());
    deleted.removeAll(current.keySet());
    Set<String> new_ = new HashSet<String>(current.keySet());
    new_.removeAll(names.keySet());
    for (String logger : deleted) {
      String address = names.get(logger);
      if (!currentAddresses.contains(address)) {
        watcher.deadLogger(address);
      }
    }
    for (String logger : new_) {
      String address = current.get(logger);
      watcher.newLogger(address);
    }
    names = current;
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

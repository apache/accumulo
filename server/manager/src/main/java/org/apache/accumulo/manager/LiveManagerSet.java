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
package org.apache.accumulo.manager;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.fate.zookeeper.ZooCache;
import org.apache.accumulo.core.util.threads.ThreadPools;
import org.apache.accumulo.server.ServerContext;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LiveManagerSet implements Watcher {

  public interface Listener {
    void managersChanged(Collection<String> addresses);
  }

  private static final Logger log = LoggerFactory.getLogger(LiveManagerSet.class);

  private final Listener cback;
  private final ServerContext context;
  private ZooCache zooCache;
  private final String path;

  private List<String> managers = new ArrayList<>();

  public LiveManagerSet(ServerContext context, Listener cback) {
    this.cback = cback;
    this.context = context;
    path = this.context.getZooKeeperRoot() + Constants.ZMANAGERS;
  }

  public synchronized ZooCache getZooCache() {
    if (zooCache == null) {
      zooCache = new ZooCache(context.getZooReader(), this);
    }
    return zooCache;
  }

  public synchronized void startListeningForManagerServerChanges() {
    scanServers();

    ThreadPools.watchCriticalScheduledTask(this.context.getScheduledExecutor()
        .scheduleWithFixedDelay(this::scanServers, 0, 5000, TimeUnit.MILLISECONDS));
  }

  public synchronized void scanServers() {
    try {

      List<String> current = getZooCache().getChildren(path).stream().filter(s -> s.contains(":"))
          .collect(Collectors.toList());
      if (current.equals(managers)) {
        return;
      }
      managers = new ArrayList<>(current);
      this.cback.managersChanged(managers);
    } catch (Exception ex) {
      log.error("{}", ex.getMessage(), ex);
    }
  }

  @Override
  public void process(WatchedEvent event) {
    if (event.getPath() != null) {
      if (event.getPath().endsWith(Constants.ZMANAGERS)
          || event.getPath().contains(Constants.ZMANAGERS)) {
        // something changed, scan the servers
        scanServers();
      } else {
        log.info("event received: {}", event);
      }
    }
  }

}

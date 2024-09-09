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
package org.apache.accumulo.core.clientImpl;

import java.util.Optional;
import java.util.Set;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.clientImpl.ClientTabletCacheImpl.TabletServerLockChecker;
import org.apache.accumulo.core.lock.ServiceLock;
import org.apache.accumulo.core.lock.ServiceLockPaths.ServiceLockPath;

import com.google.common.net.HostAndPort;

public class ZookeeperLockChecker implements TabletServerLockChecker {

  private final ClientContext ctx;
  private final String root;

  ZookeeperLockChecker(ClientContext context) {
    this.ctx = context;
    this.root = context.getZooKeeperRoot() + Constants.ZTSERVERS;
  }

  public boolean doesTabletServerLockExist(String server) {
    // ServiceLockPaths only returns items that have a lock
    Set<ServiceLockPath> tservers = ctx.getServerPaths().getTabletServer(Optional.empty(),
        Optional.of(HostAndPort.fromString(server)));
    if (tservers.isEmpty()) {
      return false;
    }
    return true;
  }

  @Override
  public boolean isLockHeld(String server, String session) {
    // ServiceLockPaths only returns items that have a lock
    Set<ServiceLockPath> tservers = ctx.getServerPaths().getTabletServer(Optional.empty(),
        Optional.of(HostAndPort.fromString(server)));
    if (tservers.isEmpty()) {
      return false;
    }
    for (ServiceLockPath slp : tservers) {
      if (ServiceLock.getSessionId(ctx.getZooCache(), slp) == Long.parseLong(session, 16)) {
        return true;
      }
    }
    return false;
  }

  @Override
  public void invalidateCache(String tserver) {
    ctx.getZooCache().clear(root + "/" + tserver);
  }

}

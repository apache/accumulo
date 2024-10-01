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
package org.apache.accumulo.server.util;

import java.util.Optional;
import java.util.Set;

import org.apache.accumulo.core.fate.zookeeper.ZooCache;
import org.apache.accumulo.core.fate.zookeeper.ZooReaderWriter;
import org.apache.accumulo.core.lock.ServiceLock;
import org.apache.accumulo.core.lock.ServiceLockData;
import org.apache.accumulo.core.lock.ServiceLockData.ThriftService;
import org.apache.accumulo.core.lock.ServiceLockPaths.ServiceLockPath;
import org.apache.accumulo.server.ServerContext;

import com.google.common.base.Preconditions;
import com.google.common.net.HostAndPort;

public class TabletServerLocks {

  public static void execute(final ServerContext context, final String lock, final String delete)
      throws Exception {

    ZooCache cache = context.getZooCache();
    ZooReaderWriter zoo = context.getZooReaderWriter();

    if (delete == null) {
      Set<ServiceLockPath> tabletServers =
          context.getServerPaths().getTabletServer(Optional.empty(), Optional.empty(), false);
      if (tabletServers.isEmpty()) {
        System.err.println("No tservers found in ZK");
      }

      for (ServiceLockPath tabletServer : tabletServers) {
        Optional<ServiceLockData> lockData = ServiceLock.getLockData(cache, tabletServer, null);
        final String holder;
        if (lockData.isPresent()) {
          holder = lockData.orElseThrow().getAddressString(ThriftService.TSERV);
        } else {
          holder = "<none>";
        }

        System.out.printf("%32s %16s%n", tabletServer.getServer(), holder);
      }
    } else {
      if (lock == null) {
        printUsage();
      } else {
        Set<ServiceLockPath> paths = context.getServerPaths().getTabletServer(Optional.empty(),
            Optional.of(HostAndPort.fromString(lock)), true);
        Preconditions.checkArgument(paths.size() == 1,
            lock + " does not match a single ZooKeeper TabletServer lock. matches=" + paths);
        ServiceLockPath path = paths.iterator().next();
        ServiceLock.deleteLock(zoo, path);
        System.out.printf("Deleted %s", path.toString());
      }

    }
  }

  private static void printUsage() {
    System.out.println("Usage : accumulo admin locks -delete <tserver lock>");
  }

}

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

import org.apache.accumulo.core.clientImpl.ClientContext;
import org.apache.accumulo.core.lock.ServiceLockPaths.ServiceLockPath;

public class AccumuloStatus {
  /**
   * Determines if there could be an accumulo instance running via zookeeper lock checking
   *
   * @return true iff all servers show no indication of being registered in zookeeper, otherwise
   *         false
   */
  public static boolean isAccumuloOffline(ClientContext context) {
    Set<ServiceLockPath> tservers =
        context.getServerPaths().getTabletServer(Optional.empty(), Optional.empty(), true);
    if (!tservers.isEmpty()) {
      return false;
    }
    if (context.getServerPaths().getManager(true) != null) {
      return false;
    }
    if (context.getServerPaths().getMonitor(true) != null) {
      return false;
    }
    if (context.getServerPaths().getGarbageCollector(true) != null) {
      return false;
    }
    return true;
  }

}

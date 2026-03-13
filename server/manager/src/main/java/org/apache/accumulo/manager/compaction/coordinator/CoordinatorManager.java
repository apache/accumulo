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
package org.apache.accumulo.manager.compaction.coordinator;

import java.util.HashMap;
import java.util.Map;

import org.apache.accumulo.core.client.admin.servers.ServerId;
import org.apache.accumulo.core.data.ResourceGroupId;
import org.apache.accumulo.core.util.UtilWaitThread;
import org.apache.accumulo.core.util.threads.Threads;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.compaction.CoordinatorLocations;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.net.HostAndPort;

/**
 * Handles assigning compactor resource groups to different manager processes. This is run by the
 * primary manager to distribute compaction coordination.
 */
public class CoordinatorManager {

  private static final Logger log = LoggerFactory.getLogger(CoordinatorManager.class);

  private final ServerContext context;

  public CoordinatorManager(ServerContext context) {
    this.context = context;
  }

  private Thread assignmentThread = null;

  private void managerCoordinators() {
    while (true) {
      try {
        var serverIds = context.instanceOperations().getServers(ServerId.Type.MANAGER);
        if (!serverIds.isEmpty()) {
          var primary = HostAndPort.fromString(serverIds.iterator().next().toHostPortString());
          var compactorGroups =
              CompactionCoordinator.getCompactionServicesConfigurationGroups(context);
          Map<ResourceGroupId,HostAndPort> assignments = new HashMap<>();
          compactorGroups.forEach(rg -> assignments.put(rg, primary));
          CoordinatorLocations.setLocations(context.getZooSession().asReaderWriter(), assignments);
          log.debug("Set locations {}", assignments);
        }
        // TODO better exception handling
      } catch (ReflectiveOperationException | IllegalArgumentException | SecurityException
          | InterruptedException | KeeperException e) {
        log.warn("Failed to manage coordinators", e);
      }

      // TODO not good for test
      UtilWaitThread.sleep(5000);
    }

  }

  public synchronized void start() {
    Preconditions.checkState(assignmentThread == null);

    assignmentThread = Threads.createCriticalThread("Coordinator Manager", () -> {
      try {
        managerCoordinators();
      } catch (Exception e) {
        throw new IllegalStateException(e);
      }
    });
    assignmentThread.start();
  }

  // TODO stop()
}

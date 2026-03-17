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

import static java.util.stream.Collectors.toSet;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.accumulo.core.compaction.thrift.CompactionCoordinatorService;
import org.apache.accumulo.core.data.AbstractId;
import org.apache.accumulo.core.data.ResourceGroupId;
import org.apache.accumulo.core.lock.ServiceLockPaths;
import org.apache.accumulo.core.rpc.ThriftUtil;
import org.apache.accumulo.core.rpc.clients.ThriftClientTypes;
import org.apache.accumulo.core.trace.TraceUtil;
import org.apache.accumulo.core.util.UtilWaitThread;
import org.apache.accumulo.core.util.threads.Threads;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.compaction.CoordinatorLocations;
import org.apache.thrift.TException;
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

        var assistants = context.getServerPaths()
            .getAssistantManagers(ServiceLockPaths.AddressSelector.all(), true);
        if (!assistants.isEmpty()) {
          var compactorGroups =
              CompactionCoordinator.getCompactionServicesConfigurationGroups(context);

          int minGroupsPerAssistant = compactorGroups.size() / assistants.size();
          int maxGroupsPerAssistant =
              minGroupsPerAssistant + Math.min(compactorGroups.size() % assistants.size(), 1);

          Map<ResourceGroupId,HostAndPort> currentLocations =
              context.getCoordinatorLocations(false);

          // group by coordinator
          Map<HostAndPort,Set<ResourceGroupId>> groupsPerCompactor = new HashMap<>();
          currentLocations.forEach((rg, hp) -> {
            groupsPerCompactor.computeIfAbsent(hp, hp2 -> new HashSet<>()).add(rg);
          });

          boolean needsUpdates = !currentLocations.keySet().containsAll(compactorGroups);
          // Look for any compactors that are not correct
          for (var groups : groupsPerCompactor.values()) {
            if (groups.size() < minGroupsPerAssistant || groups.size() > maxGroupsPerAssistant
                || !compactorGroups.containsAll(groups)) {
              needsUpdates = true;
            }
          }

          if (needsUpdates) {
            // TODO this does not try to keep current assignemnts
            // TODO this should ask coordinators what they currently have instead of assuming ZK is
            // correct, on failure ZK could be out of sync... otherwise always set RG on
            // coordinators to ensure they match ZK
            // TODO combine/share code w/ fate for assignments, that is why the todos above are not
            // done, this code is temporary hack and it has problems.
            Map<HostAndPort,Set<ResourceGroupId>> updates = new HashMap<>();
            assistants.forEach(
                slp -> updates.put(HostAndPort.fromString(slp.getServer()), new HashSet<>()));

            var iter = compactorGroups.iterator();
            updates.values().forEach(groups -> {
              while (groups.size() < minGroupsPerAssistant) {
                groups.add(iter.next());
              }
            });

            updates.values().forEach(groups -> {
              while (iter.hasNext() && groups.size() < maxGroupsPerAssistant) {
                groups.add(iter.next());
              }
            });

            updates.forEach(this::setResourceGroups);
            Map<ResourceGroupId,HostAndPort> newLocations = new HashMap<>();
            updates.forEach((hp, groups) -> {
              groups.forEach(group -> newLocations.put(group, hp));
            });

            CoordinatorLocations.setLocations(context.getZooSession().asReaderWriter(),
                newLocations);
            log.debug("Set locations {}", newLocations);
          }
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

  private void setResourceGroups(HostAndPort hp, Set<ResourceGroupId> groups) {
    CompactionCoordinatorService.Client client = null;
    try {
      client = ThriftUtil.getClient(ThriftClientTypes.COORDINATOR, hp, context);
      client.setResourceGroups(TraceUtil.traceInfo(), context.rpcCreds(),
          groups.stream().map(AbstractId::canonical).collect(toSet()));
    } catch (TException e) {
      // TODO
      throw new RuntimeException(e);
    } finally {
      ThriftUtil.returnClient(client, context);
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

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
import static org.apache.accumulo.manager.multi.ManagerAssignment.computeAssignments;
import static org.apache.accumulo.server.compaction.CompactionPluginUtils.getConfiguredCompactionResourceGroups;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.accumulo.core.compaction.thrift.CompactionCoordinatorService;
import org.apache.accumulo.core.data.AbstractId;
import org.apache.accumulo.core.data.ResourceGroupId;
import org.apache.accumulo.core.fate.zookeeper.ZooUtil.NodeExistsPolicy;
import org.apache.accumulo.core.lock.ServiceLockPaths;
import org.apache.accumulo.core.rpc.ThriftUtil;
import org.apache.accumulo.core.rpc.clients.ThriftClientTypes;
import org.apache.accumulo.core.trace.TraceUtil;
import org.apache.accumulo.core.util.LazySingletons;
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
        long updateId = LazySingletons.RANDOM.get().nextLong();
        var current = getCurrentAssignments(updateId);

        log.trace("Current assignments {}", current);

        var configuredGroups = getConfiguredCompactionResourceGroups(context).stream()
            .map(ResourceGroupId::of).collect(toSet());

        log.trace("Configured groups {}", configuredGroups);

        var desired = computeAssignments(current, configuredGroups);

        log.trace("Desired assignments {}", desired);
        if (!current.equals(desired)) {
          setAssignments(updateId, desired);
        }

        updateZookeeper(desired);
      } catch (ReflectiveOperationException | InterruptedException | KeeperException e) {
        // TODO
        throw new RuntimeException(e);
      }

      // TODO not good for tests
      UtilWaitThread.sleep(5000);
    }

  }

  private void updateZookeeper(Map<HostAndPort,Set<ResourceGroupId>> desired)
      throws InterruptedException, KeeperException {
    // transform desired set to look like what is in zookeeper
    HashMap<ResourceGroupId,HostAndPort> transformed = new HashMap<>();
    desired.forEach((hp, rgs) -> {
      rgs.forEach(rg -> transformed.put(rg, hp));
    });

    var assignmentsInZk = context.getCoordinatorLocations(true);
    if (!transformed.equals(assignmentsInZk)) {
      CoordinatorLocations.setLocations(context.getZooSession().asReaderWriter(), transformed,
          NodeExistsPolicy.OVERWRITE);
      log.debug("Set new coordinator locations {}", desired);

    }
  }

  private Map<HostAndPort,Set<ResourceGroupId>> getCurrentAssignments(long updateId) {
    var assistants =
        context.getServerPaths().getAssistantManagers(ServiceLockPaths.AddressSelector.all(), true);

    Map<HostAndPort,Set<ResourceGroupId>> assignments = new HashMap<>();

    for (var assistant : assistants) {
      CompactionCoordinatorService.Client client = null;
      try {
        var hp = HostAndPort.fromString(assistant.getServer());
        client = ThriftUtil.getClient(ThriftClientTypes.COORDINATOR, hp, context);
        var groups = client.getResourceGroups(TraceUtil.traceInfo(), context.rpcCreds(), updateId);
        assignments.put(hp, groups.stream().map(ResourceGroupId::of).collect(toSet()));
      } catch (TException e) {
        // TODO
        throw new RuntimeException(e);
      } finally {
        ThriftUtil.returnClient(client, context);
      }
    }

    return assignments;
  }

  private void setAssignments(long updateId, Map<HostAndPort,Set<ResourceGroupId>> assignments) {
    assignments.forEach((hp, groups) -> {
      setResourceGroups(hp, groups, updateId);
    });
  }

  private void setResourceGroups(HostAndPort hp, Set<ResourceGroupId> groups, long updateId) {
    CompactionCoordinatorService.Client client = null;
    try {
      client = ThriftUtil.getClient(ThriftClientTypes.COORDINATOR, hp, context);
      client.setResourceGroups(TraceUtil.traceInfo(), context.rpcCreds(), updateId,
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

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
import static org.apache.accumulo.core.util.LazySingletons.RANDOM;
import static org.apache.accumulo.manager.multi.ManagerAssignment.computeAssignments;
import static org.apache.accumulo.server.compaction.CompactionPluginUtils.getConfiguredCompactionResourceGroups;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.compaction.thrift.CompactionCoordinatorService;
import org.apache.accumulo.core.data.AbstractId;
import org.apache.accumulo.core.data.ResourceGroupId;
import org.apache.accumulo.core.fate.FateClient;
import org.apache.accumulo.core.fate.FateInstanceType;
import org.apache.accumulo.core.fate.zookeeper.ZooReaderWriter;
import org.apache.accumulo.core.fate.zookeeper.ZooUtil.NodeExistsPolicy;
import org.apache.accumulo.core.lock.ServiceLockPaths;
import org.apache.accumulo.core.rpc.ThriftUtil;
import org.apache.accumulo.core.rpc.clients.ThriftClientTypes;
import org.apache.accumulo.core.trace.TraceUtil;
import org.apache.accumulo.core.util.UtilWaitThread;
import org.apache.accumulo.core.util.threads.ThreadPools;
import org.apache.accumulo.core.util.threads.Threads;
import org.apache.accumulo.manager.tableOps.FateEnv;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.compaction.CoordinatorLocationsFactory;
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

  private final AtomicBoolean stopped = new AtomicBoolean(false);

  public CoordinatorManager(ServerContext context,
      Function<FateInstanceType,FateClient<FateEnv>> fateClients) {
    this.context = context;
    deadCompactionDetector =
        new DeadCompactionDetector(context, context.getScheduledExecutor(), fateClients);
  }

  private Thread assignmentThread = null;

  private final DeadCompactionDetector deadCompactionDetector;

  private void managerCoordinators() {
    final long initialSleepTime = 10;
    final long maxSleepTime = 5000;
    long sleepTime = initialSleepTime;
    while (!stopped.get()) {
      try {
        long updateId = RANDOM.get().nextLong();
        var current = getCurrentAssignments(updateId);

        log.trace("Current assignments {}", current);

        var configuredGroups = getConfiguredCompactionResourceGroups(context).stream()
            .map(ResourceGroupId::of).collect(toSet());

        log.trace("Configured groups {}", configuredGroups);

        var desired = computeAssignments(current, configuredGroups);

        log.trace("Desired assignments {}", desired);
        if (!current.equals(desired)) {
          sleepTime = initialSleepTime;
          setAssignments(updateId, desired);
        } else {
          sleepTime = Math.min(sleepTime + 10, maxSleepTime);
        }

        updateZookeeper(desired);
      } catch (Exception e) {
        log.warn("Failed to set compaction coordinator assignments", e);
        sleepTime = maxSleepTime;
      }

      UtilWaitThread.sleep(sleepTime);
    }

  }

  private void updateZookeeper(Map<HostAndPort,Set<ResourceGroupId>> desired)
      throws InterruptedException, KeeperException {
    // transform desired set to look like what is in zookeeper
    HashMap<ResourceGroupId,HostAndPort> transformed = new HashMap<>();
    desired.forEach((hp, rgs) -> {
      rgs.forEach(rg -> transformed.put(rg, hp));
    });

    var assignmentsInZk = context.getCoordinatorLocations(true).locations();
    if (!transformed.equals(assignmentsInZk)) {
      CoordinatorLocationsFactory.setLocations(context.getZooSession().asReaderWriter(),
          transformed, NodeExistsPolicy.OVERWRITE);
      log.debug("Set new coordinator locations {}", desired);

    }
  }

  private Map<HostAndPort,Set<ResourceGroupId>> getCurrentAssignments(long updateId) {
    var assistants =
        context.getServerPaths().getAssistantManagers(ServiceLockPaths.AddressSelector.all(), true);

    Map<HostAndPort,Set<ResourceGroupId>> assignments = new HashMap<>();

    for (var assistant : assistants) {
      CompactionCoordinatorService.Client client = null;
      var hp = HostAndPort.fromString(assistant.getServer());
      try {
        client = ThriftUtil.getClient(ThriftClientTypes.COORDINATOR, hp, context);
        var groups = client.getResourceGroups(TraceUtil.traceInfo(), context.rpcCreds(), updateId);
        assignments.put(hp, groups.stream().map(ResourceGroupId::of).collect(toSet()));
      } catch (TException e) {
        log.warn("Failed to get current assignments from {}", hp, e);
        return Map.of();
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
      log.warn("Failed to set resource groups for {}", hp, e);
    } finally {
      ThriftUtil.returnClient(client, context);
    }
  }

  public synchronized void start() {
    Preconditions.checkState(assignmentThread == null);
    Preconditions.checkState(!stopped.get());

    startCompactorZKCleaner(context.getScheduledExecutor());

    deadCompactionDetector.start();

    assignmentThread = Threads.createCriticalThread("Coordinator Manager", () -> {
      try {
        managerCoordinators();
      } catch (Exception e) {
        throw new IllegalStateException(e);
      }
    });
    assignmentThread.start();
  }

  public synchronized void stop() {
    stopped.set(true);
    try {
      assignmentThread.join();
    } catch (InterruptedException e) {
      throw new IllegalStateException(e);
    }
  }

  protected void startCompactorZKCleaner(ScheduledThreadPoolExecutor schedExecutor) {
    ScheduledFuture<?> future = schedExecutor
        .scheduleWithFixedDelay(this::cleanUpEmptyCompactorPathInZK, 0, 5, TimeUnit.MINUTES);
    ThreadPools.watchNonCriticalScheduledTask(future);
  }

  private void deleteEmpty(ZooReaderWriter zoorw, String path)
      throws KeeperException, InterruptedException {
    try {
      log.debug("Deleting empty ZK node {}", path);
      zoorw.delete(path);
    } catch (KeeperException.NotEmptyException e) {
      log.debug("Failed to delete {} its not empty, likely an expected race condition.", path);
    }
  }

  private void cleanUpEmptyCompactorPathInZK() {

    final var zoorw = this.context.getZooSession().asReaderWriter();

    try {
      var groups = zoorw.getChildren(Constants.ZCOMPACTORS);

      for (String group : groups) {
        final String qpath = Constants.ZCOMPACTORS + "/" + group;
        final ResourceGroupId cgid = ResourceGroupId.of(group);
        final var compactors = zoorw.getChildren(qpath);

        if (compactors.isEmpty()) {
          deleteEmpty(zoorw, qpath);
        } else {
          for (String compactor : compactors) {
            String cpath = Constants.ZCOMPACTORS + "/" + group + "/" + compactor;
            var lockNodes =
                zoorw.getChildren(Constants.ZCOMPACTORS + "/" + group + "/" + compactor);
            if (lockNodes.isEmpty()) {
              deleteEmpty(zoorw, cpath);
            }
          }
        }
      }
    } catch (KeeperException | RuntimeException e) {
      log.warn("Failed to clean up compactors", e);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException(e);
    }
  }
}

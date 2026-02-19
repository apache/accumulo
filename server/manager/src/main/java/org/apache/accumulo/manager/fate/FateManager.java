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
package org.apache.accumulo.manager.fate;

import static org.apache.accumulo.core.lock.ServiceLockPaths.ResourceGroupPredicate.DEFAULT_RG_ONLY;

import java.time.Duration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import org.apache.accumulo.core.fate.FateId;
import org.apache.accumulo.core.fate.FateInstanceType;
import org.apache.accumulo.core.fate.FatePartition;
import org.apache.accumulo.core.fate.thrift.FateWorkerService;
import org.apache.accumulo.core.fate.user.UserFateStore;
import org.apache.accumulo.core.lock.ServiceLockPaths.AddressSelector;
import org.apache.accumulo.core.metadata.SystemTables;
import org.apache.accumulo.core.rpc.ThriftUtil;
import org.apache.accumulo.core.rpc.clients.ThriftClientTypes;
import org.apache.accumulo.core.trace.TraceUtil;
import org.apache.accumulo.core.util.CountDownTimer;
import org.apache.accumulo.core.util.threads.Threads;
import org.apache.accumulo.manager.tableOps.FateEnv;
import org.apache.accumulo.server.ServerContext;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.Range;
import com.google.common.collect.RangeMap;
import com.google.common.collect.Sets;
import com.google.common.collect.TreeRangeMap;
import com.google.common.net.HostAndPort;

/**
 * Partitions {@link FateInstanceType#USER} fate across manager assistant processes. This is done by assigning ranges of the fate
 * uuid key space to different processes. The partitions are logical and do not correspond to the
 * physical partitioning of the fate table.
 *
 * <p>Does not currently manage {@link FateInstanceType#META}</p>
 */
public class FateManager {

  private static final Logger log = LoggerFactory.getLogger(FateManager.class);

  private final ServerContext context;

  public FateManager(ServerContext context) {
    this.context = context;
  }

  private final AtomicBoolean stop = new AtomicBoolean(false);

  record FateHostPartition(HostAndPort hostPort, FatePartition partition) {
  }

  private final AtomicReference<RangeMap<FateId,FateHostPartition>> stableAssignments =
      new AtomicReference<>(TreeRangeMap.create());

  private final Map<HostAndPort,Set<FatePartition>> pendingNotifications = new HashMap<>();

  private void managerWorkers() throws TException, InterruptedException {
    log.debug("Started Fate Manager");
    long stableCount = 0;
    outer: while (!stop.get()) {

      long sleepTime = Math.min(stableCount * 100, 5_000);
      Thread.sleep(sleepTime);

      // This map will contain all current workers even their partitions are empty
      Map<HostAndPort,CurrentPartitions> currentPartitions = getCurrentAssignments();
      Map<HostAndPort,Set<FatePartition>> currentAssignments = new HashMap<>();
      currentPartitions.forEach((k, v) -> currentAssignments.put(k, v.partitions()));
      Set<FatePartition> desiredParititions = getDesiredPartitions(currentAssignments.size());

      Map<HostAndPort,Set<FatePartition>> desired =
          computeDesiredAssignments(currentAssignments, desiredParititions);

      if (desired.equals(currentAssignments)) {
        RangeMap<FateId,FateHostPartition> rangeMap = TreeRangeMap.create();
        currentAssignments.forEach((hostAndPort, partitions) -> {
          partitions.forEach(partition -> {
            rangeMap.put(Range.closed(partition.start(), partition.end()),
                new FateHostPartition(hostAndPort, partition));
          });
        });
        stableAssignments.set(rangeMap);
        stableCount++;
      } else {
        stableAssignments.set(TreeRangeMap.create());
        stableCount = 0;
      }

      // are there any workers with extra partitions? If so need to unload those first.
      int unloads = 0;
      for (Map.Entry<HostAndPort,Set<FatePartition>> entry : desired.entrySet()) {
        HostAndPort worker = entry.getKey();
        Set<FatePartition> partitions = entry.getValue();
        var curr = currentAssignments.getOrDefault(worker, Set.of());
        if (!Sets.difference(curr, partitions).isEmpty()) {
          // This worker has extra partitions that are not desired
          var intersection = Sets.intersection(curr, partitions);
          if (!setWorkerPartitions(worker, currentPartitions.get(worker).updateId(),
              intersection)) {
            log.debug("Failed to set partitions for {} to {}", worker, intersection);
            // could not set, so start completely over
            continue outer;
          } else {
            log.debug("Set partitions for {} to {} from {}", worker, intersection, curr);
            unloads++;
          }
        }
      }

      if (unloads > 0) {
        // some tablets were unloaded, so start over and get new update ids and the current
        // partitions
        continue outer;
      }

      // Load all partitions on all workers..
      for (Map.Entry<HostAndPort,Set<FatePartition>> entry : desired.entrySet()) {
        HostAndPort worker = entry.getKey();
        Set<FatePartition> partitions = entry.getValue();
        var curr = currentAssignments.getOrDefault(worker, Set.of());
        if (!curr.equals(partitions)) {
          if (!setWorkerPartitions(worker, currentPartitions.get(worker).updateId(), partitions)) {
            log.debug("Failed to set partitions for {} to {}", worker, partitions);
            // could not set, so start completely over
            continue outer;
          } else {
            log.debug("Set partitions for {} to {} from {}", worker, partitions, curr);
          }
        }
      }
    }
  }

  private Thread assignmentThread = null;
  private Thread ntfyThread = null;

  public synchronized void start() {
    Preconditions.checkState(assignmentThread == null);
    Preconditions.checkState(ntfyThread == null);
    Preconditions.checkState(!stop.get());

    assignmentThread = Threads.createCriticalThread("Fate Manager", () -> {
      try {
        managerWorkers();
      } catch (Exception e) {
        throw new IllegalStateException(e);
      }
    });
    assignmentThread.start();

    ntfyThread = Threads.createCriticalThread("Fate Notify", new NotifyTask());
    ntfyThread.start();
  }

  public synchronized void stop(Duration timeout) {
    if (!stop.compareAndSet(false, true)) {
      return;
    }

    var timer = CountDownTimer.startNew(timeout);

    try {
      if (assignmentThread != null) {
        assignmentThread.join();
      }
      if (ntfyThread != null) {
        ntfyThread.join();
      }
    } catch (InterruptedException e) {
      throw new IllegalStateException(e);
    }
    // Try to set every assistant manager to an empty set of partitions.  This will cause them all to stop looking for work.
    Map<HostAndPort,CurrentPartitions> currentAssignments = null;
    try {
      currentAssignments = getCurrentAssignments();
    } catch (TException e) {
      log.warn("Failed to get current assignments", e);
      currentAssignments = Map.of();
    }
    for (var entry : currentAssignments.entrySet()) {
      var hostPort = entry.getKey();
      var currentPartitions = entry.getValue();
      if (!currentPartitions.partitions.isEmpty()) {
        try {
          setWorkerPartitions(hostPort, currentPartitions.updateId(), Set.of());
        } catch (TException e) {
          log.warn("Failed to unassign fate partitions {}", hostPort, e);
        }
      }
    }

    stableAssignments.set(TreeRangeMap.create());

    if(!timer.isExpired()) {
      var store = new UserFateStore<FateEnv>(context, SystemTables.FATE.tableName(), null, null);

      var reserved = store.getActiveReservations(Set.of(FatePartition.all(FateInstanceType.USER)));
      while (!reserved.isEmpty() && !timer.isExpired()) {
        if (log.isTraceEnabled()) {
          reserved.forEach((fateId, reservation) -> {
            log.trace("In stop(), waiting on {} {} ", fateId, reservation);
          });
        }
        try {
          Thread.sleep(Math.min(100, timer.timeLeft(TimeUnit.MILLISECONDS)));
        } catch (InterruptedException e) {
          throw new IllegalStateException(e);
        }
      }
    }
  }

  /**
   * Makes a best effort to notify this fate operation was seeded.
   */
  public void notifySeeded(FateId fateId) {
    var hostPartition = stableAssignments.get().get(fateId);
    if (hostPartition != null) {
      synchronized (pendingNotifications) {
        pendingNotifications.computeIfAbsent(hostPartition.hostPort(), k -> new HashSet<>())
            .add(hostPartition.partition());
        pendingNotifications.notify();
      }
    }
  }

  private class NotifyTask implements Runnable {

    @Override
    public void run() {
      while (!stop.get()) {
        try {
          Map<HostAndPort,Set<FatePartition>> copy;
          synchronized (pendingNotifications) {
            if (pendingNotifications.isEmpty()) {
              pendingNotifications.wait(100);
            }
            copy = Map.copyOf(pendingNotifications);
            pendingNotifications.clear();
          }

          for (var entry : copy.entrySet()) {
            HostAndPort address = entry.getKey();
            Set<FatePartition> partitions = entry.getValue();
            FateWorkerService.Client client =
                ThriftUtil.getClient(ThriftClientTypes.FATE_WORKER, address, context);
            try {
              log.trace("Notifying about seeding {} {}", address, partitions);
              client.seeded(TraceUtil.traceInfo(), context.rpcCreds(),
                  partitions.stream().map(FatePartition::toThrift).toList());
            } finally {
              ThriftUtil.returnClient(client, context);
            }
          }

        } catch (InterruptedException e) {
          throw new IllegalStateException(e);
        } catch (TException e) {
          log.warn("Failed to send notification that fate was seeded", e);
        }
      }
    }
  }

  /**
   * Sets the complete set of partitions a server should work on. It will only succeed if the update
   * id is valid. The update id avoids race conditions w/ previously queued network messages, it's a
   * distributed compare and set mechanism that can detect changes.
   *
   * @param address The server to set partitions on
   * @param updateId What we think the servers current set of fate partitions are.
   * @param desired The new set of fate partitions this server should start working. It should only
   *        work on these and nothing else.
   * @return true if the partitions were set false if they were not set.
   */
  private boolean setWorkerPartitions(HostAndPort address, long updateId,
      Set<FatePartition> desired) throws TException {
    FateWorkerService.Client client =
        ThriftUtil.getClient(ThriftClientTypes.FATE_WORKER, address, context);
    try {
      log.trace("Setting partitions {} {}", address, desired);
      var result =  client.setPartitions(TraceUtil.traceInfo(), context.rpcCreds(), updateId,
          desired.stream().map(FatePartition::toThrift).toList());
      return result;
    } finally {
      ThriftUtil.returnClient(client, context);
    }
  }

  /**
   * Compute the desired distribution of partitions across workers. Favors leaving partitions in
   * place if possible.
   */
  private Map<HostAndPort,Set<FatePartition>> computeDesiredAssignments(
      Map<HostAndPort,Set<FatePartition>> currentAssignments,
      Set<FatePartition> desiredParititions) {

    Preconditions.checkArgument(currentAssignments.size() == desiredParititions.size());
    Map<HostAndPort,Set<FatePartition>> desiredAssignments = new HashMap<>();

    var copy = new HashSet<>(desiredParititions);

    currentAssignments.forEach((hp, partitions) -> {
      if (!partitions.isEmpty()) {
        var firstPart = partitions.iterator().next();
        if (copy.contains(firstPart)) {
          desiredAssignments.put(hp, Set.of(firstPart));
          copy.remove(firstPart);
        }
      }
    });

    var iter = copy.iterator();
    currentAssignments.forEach((hp, partitions) -> {
      if (!desiredAssignments.containsKey(hp)) {
        desiredAssignments.put(hp, Set.of(iter.next()));
      }
    });

    if(log.isTraceEnabled()) {
      log.trace("Logging desired partitions");
      desiredAssignments.forEach((hp, parts) -> {
        log.trace(" desired {} {} {}", hp, parts.size(), parts);
      });
    }

    return desiredAssignments;
  }

  /**
   * Computes a single partition for each worker such that the partition cover all possible UUIDs
   * and evenly divide the UUIDs.
   */
  private Set<FatePartition> getDesiredPartitions(int numWorkers) {
    Preconditions.checkArgument(numWorkers >= 0);

    if (numWorkers == 0) {
      return Set.of();
    }

    // create a single partition per worker that equally divides the space
    HashSet<FatePartition> desired = new HashSet<>();
    // All the shifting is because java does not have unsigned integers. Want to evenly partition
    // [0,2^64) into numWorker ranges, but can not directly do that. Work w/ 60 bit unsigned
    // integers to partition the space and then shift over by 4. Used 60 bits instead of 63 so it
    // nicely aligns w/ hex in the uuid.
    long jump = ((1L << 60)) / numWorkers;
    for (int i = 0; i < numWorkers - 1; i++) {
      long start = (i * jump) << 4;
      long end = ((i + 1) * jump) << 4;

      UUID startUuid = new UUID(start, 0);
      UUID endUuid = new UUID(end, 0);

      desired.add(new FatePartition(FateId.from(FateInstanceType.USER, startUuid),
          FateId.from(FateInstanceType.USER, endUuid)));
    }

    long start = ((numWorkers - 1) * jump) << 4;
    UUID startUuid = new UUID(start, 0);
    // last partition has a special end uuid that is all f nibbles.
    UUID endUuid = new UUID(-1, -1);
    desired.add(new FatePartition(FateId.from(FateInstanceType.USER, startUuid),
        FateId.from(FateInstanceType.USER, endUuid)));

    return desired;
  }

  // The updateId accomplishes two things. First it ensures that setting partition RPC can only
  // execute once on the server side. Second when a new update id is requested it cancels any
  // outstanding RPCs to set partitions that have not executed yet.
  record CurrentPartitions(long updateId, Set<FatePartition> partitions) {
  }

  private Map<HostAndPort,CurrentPartitions> getCurrentAssignments() throws TException {
    var workers =
        context.getServerPaths().getManagerWorker(DEFAULT_RG_ONLY, AddressSelector.all(), true);

    log.trace("getting current assignments from {}", workers);

    Map<HostAndPort,CurrentPartitions> currentAssignments = new HashMap<>();

    for (var worker : workers) {
      var address = HostAndPort.fromString(worker.getServer());

      FateWorkerService.Client client =
          ThriftUtil.getClient(ThriftClientTypes.FATE_WORKER, address, context);
      try {

        var tparitions = client.getPartitions(TraceUtil.traceInfo(), context.rpcCreds());
        var partitions =
            tparitions.partitions.stream().map(FatePartition::from).collect(Collectors.toSet());
        currentAssignments.put(address, new CurrentPartitions(tparitions.updateId, partitions));
      } finally {
        ThriftUtil.returnClient(client, context);
      }
    }

    if(log.isTraceEnabled()){
      log.trace("Logging current assignments");
      currentAssignments.forEach((hostPort, partitions)->{
        log.trace("current assignment {} {}", hostPort, partitions);
      });
    }

    return currentAssignments;
  }
}

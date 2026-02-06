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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import org.apache.accumulo.core.fate.FateId;
import org.apache.accumulo.core.fate.FateInstanceType;
import org.apache.accumulo.core.fate.FatePartition;
import org.apache.accumulo.core.fate.thrift.FateWorkerService;
import org.apache.accumulo.core.lock.ServiceLockPaths.AddressSelector;
import org.apache.accumulo.core.rpc.ThriftUtil;
import org.apache.accumulo.core.rpc.clients.ThriftClientTypes;
import org.apache.accumulo.core.trace.TraceUtil;
import org.apache.accumulo.server.ServerContext;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import com.google.common.net.HostAndPort;

/**
 * Partitions fate across manager assistant processes. This is done by assigning ranges of the fate
 * uuid key space to different processes. The partitions are logical and do not correspond to the
 * physical partitioning of the fate table.
 */
public class FateManager {

  private static final Logger log = LoggerFactory.getLogger(FateManager.class);

  private final ServerContext context;

  public FateManager(ServerContext context) {
    this.context = context;
  }

  // TODO remove, here for testing
  public static final AtomicBoolean stop = new AtomicBoolean(false);

  public void managerWorkers() throws Exception {
    outer: while (!stop.get()) {
      // TODO make configurable
      Thread.sleep(3_000);

      // TODO could support RG... could user ServerId
      // This map will contain all current workers even their partitions are empty
      Map<HostAndPort,Set<FatePartition>> currentAssignments = getCurrentAssignments();
      Set<FatePartition> desiredParititions = getDesiredPartitions(currentAssignments.size());

      Map<HostAndPort,Set<FatePartition>> desired =
          computeDesiredAssignments(currentAssignments, desiredParititions);

      // are there any workers with extra partitions? If so need to unload those first.
      for (Map.Entry<HostAndPort,Set<FatePartition>> entry : desired.entrySet()) {
        HostAndPort worker = entry.getKey();
        Set<FatePartition> partitions = entry.getValue();
        var curr = currentAssignments.getOrDefault(worker, Set.of());
        if (!Sets.difference(curr, partitions).isEmpty()) {
          // This worker has extra partitions that are not desired
          var intersection = Sets.intersection(curr, partitions);
          if (!setWorkerPartitions(worker, curr, intersection)) {
            log.debug("Failed to set partitions for {} to {}", worker, intersection);
            // could not set, so start completely over
            continue outer;
          } else {
            log.debug("Set partitions for {} to {} from {}", worker, intersection, curr);
          }
          currentAssignments.put(worker, intersection);
        }
      }

      // Load all partitions on all workers..
      for (Map.Entry<HostAndPort,Set<FatePartition>> entry : desired.entrySet()) {
        HostAndPort worker = entry.getKey();
        Set<FatePartition> partitions = entry.getValue();
        var curr = currentAssignments.getOrDefault(worker, Set.of());
        if (!curr.equals(partitions)) {
          if (!setWorkerPartitions(worker, curr, partitions)) {
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

  /**
   * Sets the complete set of partitions a server should work on. It will only succeed if the
   * current set we pass in matches the severs actual current set of partitions. Passing the current
   * set avoids some race conditions w/ previously queued network messages, it's a distributed
   * compare and set mechanism that can detect changes.
   *
   * @param address The server to set partitions on
   * @param current What we think the servers current set of fate partitions are.
   * @param desired The new set of fate partitions this server should start working. It should only
   *        work on these and nothing else.
   * @return true if the partitions were set false if they were not set.
   * @throws TException
   */
  private boolean setWorkerPartitions(HostAndPort address, Set<FatePartition> current,
      Set<FatePartition> desired) throws TException {
    // TODO make a compare and set type RPC that uses the current and desired
    FateWorkerService.Client client =
        ThriftUtil.getClient(ThriftClientTypes.FATE_WORKER, address, context);
    try {
      return client.setPartitions(TraceUtil.traceInfo(), context.rpcCreds(),
          current.stream().map(FatePartition::toThrift).toList(),
          desired.stream().map(FatePartition::toThrift).toList());
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

    desiredAssignments.forEach((hp, parts) -> {
      log.debug(" desired " + hp + " " + parts.size() + " " + parts);
    });

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

  private Map<HostAndPort,Set<FatePartition>> getCurrentAssignments() throws TException {
    var workers =
        context.getServerPaths().getManagerWorker(DEFAULT_RG_ONLY, AddressSelector.all(), true);

    log.debug("workers : " + workers);

    Map<HostAndPort,Set<FatePartition>> currentAssignments = new HashMap<>();

    for (var worker : workers) {
      var address = HostAndPort.fromString(worker.getServer());

      FateWorkerService.Client client =
          ThriftUtil.getClient(ThriftClientTypes.FATE_WORKER, address, context);
      try {

        var tparitions = client.getPartitions(TraceUtil.traceInfo(), context.rpcCreds());
        var partitions = tparitions.stream().map(FatePartition::from).collect(Collectors.toSet());
        currentAssignments.put(address, partitions);
      } finally {
        ThriftUtil.returnClient(client, context);
      }
    }

    return currentAssignments;
  }
}

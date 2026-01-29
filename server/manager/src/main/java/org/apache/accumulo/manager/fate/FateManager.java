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
import java.util.stream.Collectors;

import org.apache.accumulo.core.fate.FateId;
import org.apache.accumulo.core.fate.FateInstanceType;
import org.apache.accumulo.core.fate.thrift.FateWorkerService;
import org.apache.accumulo.core.fate.thrift.TFatePartition;
import org.apache.accumulo.core.lock.ServiceLockPaths.AddressSelector;
import org.apache.accumulo.core.rpc.ThriftUtil;
import org.apache.accumulo.core.rpc.clients.ThriftClientTypes;
import org.apache.accumulo.core.trace.TraceUtil;
import org.apache.accumulo.server.ServerContext;
import org.apache.hadoop.util.Sets;
import org.apache.thrift.TException;

import com.google.common.net.HostAndPort;

public class FateManager {

  record FatePartition(FateId start, FateId end) {

    public TFatePartition toThrift() {
      return new TFatePartition(start.canonical(), end.canonical());
    }

    public static FatePartition from(TFatePartition tfp) {
      return new FatePartition(FateId.from(tfp.start), FateId.from(tfp.stop));
    }
  }

  private final ServerContext context;

  public FateManager(ServerContext context) {
    this.context = context;
  }

  public void managerWorkers() throws Exception {
    while (true) {
      // TODO make configurable
      Thread.sleep(10_000);

      // TODO could support RG... could user ServerId
      // This map will contain all current workers even their partitions are empty
      Map<HostAndPort,Set<FatePartition>> currentAssignments = getCurrentAssignments();
      Set<FatePartition> desiredParititions = getDesiredPartitions();

      // TODO handle duplicate current assignments

      System.out.println("current : " + currentAssignments);
      System.out.println("desired : " + desiredParititions);

      Map<HostAndPort,Set<FatePartition>> desired =
          computeDesiredAssignments(currentAssignments, desiredParititions);

      // are there any workers with extra partitions? If so need to unload those first.
      boolean haveExtra = desired.entrySet().stream().anyMatch(e -> {
        HostAndPort worker = e.getKey();
        var curr = currentAssignments.getOrDefault(worker, Set.of());
        var extra = Sets.difference(curr, e.getValue());
        return !extra.isEmpty();
      });

      if (haveExtra) {
        // force unload of extra partitions to make them available for other workers
        for (Map.Entry<HostAndPort,Set<FatePartition>> entry : desired.entrySet()) {
          HostAndPort worker = entry.getKey();
          Set<FatePartition> partitions = entry.getValue();
          var curr = currentAssignments.getOrDefault(worker, Set.of());
          if (!curr.equals(partitions)) {
            var intersection = Sets.intersection(curr, partitions);
            setWorkerPartitions(worker, curr, intersection);
            currentAssignments.put(worker, intersection);
          }
        }
      }

      // Load all partitions on all workers..
      for (Map.Entry<HostAndPort,Set<FatePartition>> entry : desired.entrySet()) {
        HostAndPort worker = entry.getKey();
        Set<FatePartition> partitions = entry.getValue();
        var curr = currentAssignments.getOrDefault(worker, Set.of());
        if (!curr.equals(partitions)) {
          setWorkerPartitions(worker, curr, partitions);
        }
      }
    }
  }

  private void setWorkerPartitions(HostAndPort address, Set<FatePartition> current,
      Set<FatePartition> desired) throws TException {
    // TODO make a compare and set type RPC that uses the current and desired
    FateWorkerService.Client client =
        ThriftUtil.getClient(ThriftClientTypes.FATE_WORKER, address, context);
    try {
      client.setPartitions(TraceUtil.traceInfo(), context.rpcCreds(),
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
    // min number of partitions a single worker must have
    int minPerWorker = desiredParititions.size() / currentAssignments.size();
    // max number of partitions a single worker can have
    int maxPerWorker =
        minPerWorker + Math.min(desiredParititions.size() % currentAssignments.size(), 1);
    // number of workers that can have the max partitions
    int desiredWorkersWithMax =
        desiredParititions.size() - minPerWorker * currentAssignments.size();

    Map<HostAndPort,Set<FatePartition>> desiredAssignments = new HashMap<>();
    Set<FatePartition> availablePartitions = new HashSet<>(desiredParititions);

    // remove everything that is assigned
    currentAssignments.values().forEach(p -> p.forEach(availablePartitions::remove));

    System.out.println("currentAssignments.size():" + currentAssignments.size());
    System.out.println("desiredParititions.size():" + desiredParititions.size());
    System.out.println("minPerWorker:" + minPerWorker);
    System.out.println("maxPerWorker:" + maxPerWorker);
    System.out.println("desiredWorkersWithMax:" + desiredWorkersWithMax);
    System.out.println("availablePartitions:" + availablePartitions);

    // Find workers that currently have too many partitions assigned and place their excess in the
    // available set. Let workers keep what they have when its under the limit.
    int numWorkersWithMax = 0;
    for (var worker : currentAssignments.keySet()) {
      var assignments = new HashSet<FatePartition>();
      var curr = currentAssignments.getOrDefault(worker, Set.of());
      // The number of partitions this worker can have, anything in excess should be added to
      // available
      int canHave = numWorkersWithMax < desiredWorkersWithMax ? maxPerWorker : minPerWorker;

      var iter = curr.iterator();
      for (int i = 0; i < canHave && iter.hasNext(); i++) {
        assignments.add(iter.next());
      }
      iter.forEachRemaining(availablePartitions::add);

      desiredAssignments.put(worker, assignments);
      if (curr.size() >= maxPerWorker) {
        numWorkersWithMax++;
      }
    }

    // Distribute available partitions to workers that do not have the minimum.
    var availIter = availablePartitions.iterator();
    for (var worker : currentAssignments.keySet()) {
      var assignments = desiredAssignments.get(worker);
      while (assignments.size() < minPerWorker) {
        // This should always have next if the creation of available partitions was done correctly.
        assignments.add(availIter.next());
      }
    }

    // Distribute available partitions to workers that do not have the max until no more partitions
    // available.
    for (var worker : currentAssignments.keySet()) {
      var assignments = desiredAssignments.get(worker);
      while (assignments.size() < maxPerWorker && availIter.hasNext()) {
        assignments.add(availIter.next());
      }
      if (!availIter.hasNext()) {
        break;
      }
    }

    desiredAssignments.forEach((hp, parts) -> {
      System.out.println(" desired " + hp + " " + parts.size() + " " + parts);
    });

    return desiredAssignments;
  }

  private Set<FatePartition> getDesiredPartitions() {
    HashSet<FatePartition> desired = new HashSet<>();
    // TODO created based on the number of available servers
    for (long i = 0; i <= 15; i++) {
      UUID start = new UUID((i << 60), -0);
      UUID stop = new UUID((i << 60) | (-1L >>> 4), -1);
      desired.add(new FatePartition(FateId.from(FateInstanceType.USER, start),
          FateId.from(FateInstanceType.USER, stop)));
    }

    return desired;
  }

  private Map<HostAndPort,Set<FatePartition>> getCurrentAssignments() throws TException {
    var workers =
        context.getServerPaths().getManagerWorker(DEFAULT_RG_ONLY, AddressSelector.all(), true);

    System.out.println("workers : " + workers);

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

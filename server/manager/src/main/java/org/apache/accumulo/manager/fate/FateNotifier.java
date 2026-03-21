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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.accumulo.core.fate.FateId;
import org.apache.accumulo.core.fate.FatePartition;
import org.apache.accumulo.core.manager.thrift.FateWorkerService;
import org.apache.accumulo.core.rpc.ThriftUtil;
import org.apache.accumulo.core.rpc.clients.ThriftClientTypes;
import org.apache.accumulo.core.trace.TraceUtil;
import org.apache.accumulo.core.util.threads.Threads;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.manager.FateLocations;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.Range;
import com.google.common.collect.RangeMap;
import com.google.common.collect.TreeRangeMap;
import com.google.common.net.HostAndPort;

/**
 * Responsible for sending notifications that fate operations were seeded between managers. These
 * notifications are best effort, it's ok if they are lost. When lost it means fate operations will
 * not be as responsive, but they will still eventually run. These notifications are important for
 * interactive use of Accumulo where something like create table run in the shell should be
 * responsive. Responsiveness is also important for Accumulo's integration test.
 */
public class FateNotifier {

  private static final Logger log = LoggerFactory.getLogger(FateNotifier.class);

  private final Map<HostAndPort,Set<FatePartition>> pendingNotifications = new HashMap<>();
  private final ServerContext context;
  private final AtomicBoolean stop = new AtomicBoolean();
  private final FateLocations fateLocations;

  private Map<HostAndPort,Set<FatePartition>> lastLocations;
  private RangeMap<FateId,FateHostPartition> hostMapping;

  private Thread ntfyThread;

  public FateNotifier(ServerContext context) {
    this.context = context;
    this.fateLocations = new FateLocations(context);
  }

  public synchronized void start() {
    Preconditions.checkState(ntfyThread == null);
    ntfyThread = Threads.createCriticalThread("Fate Notification Sender", new NotifyTask());
    ntfyThread.start();
  }

  record FateHostPartition(HostAndPort hostPort, FatePartition partition) {
  }

  private synchronized RangeMap<FateId,FateHostPartition> getHostMapping() {

    if (hostMapping == null || lastLocations != fateLocations.getLocations()) {
      lastLocations = fateLocations.getLocations();
      RangeMap<FateId,FateHostPartition> rangeMap = TreeRangeMap.create();
      lastLocations.forEach((hostAndPort, partitions) -> {
        partitions.forEach(partition -> {
          rangeMap.put(Range.closed(partition.start(), partition.end()),
              new FateHostPartition(hostAndPort, partition));
        });
      });
      hostMapping = rangeMap;
    }

    return hostMapping;
  }

  /**
   * Makes a best effort to notify the appropriate manager this fate operation was seeded.
   */
  public void notifySeeded(FateId fateId) {
    var hostPartition = getHostMapping().get(fateId);
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

}

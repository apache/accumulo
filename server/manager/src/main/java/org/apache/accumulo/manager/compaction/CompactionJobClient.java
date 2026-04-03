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
package org.apache.accumulo.manager.compaction;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import org.apache.accumulo.core.compaction.thrift.CompactionCoordinatorService;
import org.apache.accumulo.core.compaction.thrift.TResolvedCompactionJob;
import org.apache.accumulo.core.data.ResourceGroupId;
import org.apache.accumulo.core.metadata.schema.Ample;
import org.apache.accumulo.core.metadata.schema.TabletMetadata;
import org.apache.accumulo.core.rpc.ThriftUtil;
import org.apache.accumulo.core.rpc.clients.ThriftClientTypes;
import org.apache.accumulo.core.spi.compaction.CompactionJob;
import org.apache.accumulo.core.trace.TraceUtil;
import org.apache.accumulo.manager.compaction.queue.ResolvedCompactionJob;
import org.apache.accumulo.server.ServerContext;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.net.HostAndPort;

/**
 * Sends compaction jobs to remote compaction coordinators.
 */
public class CompactionJobClient implements AutoCloseable {

  private static final Logger log = LoggerFactory.getLogger(CompactionJobClient.class);

  private final boolean fullScan;
  private final Ample.DataLevel dataLevel;
  private final ServerContext context;

  record CoordinatorConnection(CompactionCoordinatorService.Client client,
      List<ResolvedCompactionJob> jobBuffer, HostAndPort address) {
  }

  private final Map<ResourceGroupId,HostAndPort> coordinatorLocations;
  private final Map<HostAndPort,CoordinatorConnection> coordinatorConnections = new HashMap<>();
  private final Map<ResourceGroupId,Long> ignoredCounts = new HashMap<>();

  private static final int BUFFER_SIZE = 1000;

  public CompactionJobClient(ServerContext context, Ample.DataLevel dataLevel, boolean fullScan) {
    this.context = context;
    this.dataLevel = dataLevel;
    this.fullScan = fullScan;

    this.coordinatorLocations = context.getCoordinatorLocations(true).locations();

    log.trace("Using coordinator locations {}", coordinatorLocations);

    var uniqueHosts = new HashSet<>(coordinatorLocations.values());
    for (var hostPort : uniqueHosts) {
      CompactionCoordinatorService.Client client = null;
      try {
        client = ThriftUtil.getClient(ThriftClientTypes.COORDINATOR, hostPort, context);
        if (fullScan) {
          client.beginFullJobScan(TraceUtil.traceInfo(), context.rpcCreds(), dataLevel.name());
        }
      } catch (TException | RuntimeException e) {
        log.warn("Failed to connect to coordinator {}", hostPort, e);
        ThriftUtil.returnClient(client, context);
        client = null;
      }

      if (client != null) {
        coordinatorConnections.put(hostPort,
            new CoordinatorConnection(client, new ArrayList<>(BUFFER_SIZE), hostPort));
      }
    }
  }

  public void addJobs(TabletMetadata tabletMetadata, Collection<CompactionJob> jobs) {
    for (var job : jobs) {
      var resolvedJob = new ResolvedCompactionJob(job, tabletMetadata);
      var hostPort = coordinatorLocations.get(resolvedJob.getGroup());
      if (hostPort == null) {
        log.trace("Ignoring job, no coordinator found {}", job.getGroup());
        ignoredCounts.merge(job.getGroup(), 1L, Long::sum);
        continue;
      }
      var coordinator = coordinatorConnections.get(hostPort);
      if (coordinator == null) {
        log.trace("Ignoring job, no connection found {}", job.getGroup());
        ignoredCounts.merge(job.getGroup(), 1L, Long::sum);
        continue;
      }

      coordinator.jobBuffer.add(resolvedJob);
      if (coordinator.jobBuffer.size() >= BUFFER_SIZE) {
        try {
          sendJobs(coordinator);
        } catch (TException e) {
          log.warn("Failed to send compaction jobs to {}", hostPort, e);
          ThriftUtil.returnClient(coordinator.client, context);
          // ignore this coordinator for the rest of the session...
          coordinatorConnections.remove(hostPort);
        }
      }
    }
  }

  private void sendJobs(CoordinatorConnection coordinator) throws TException {
    List<TResolvedCompactionJob> thriftJobs = new ArrayList<>(coordinator.jobBuffer.size());
    for (var job : coordinator.jobBuffer) {
      log.trace("Sending job {} {} {}", coordinator.address, job.getGroup(), job.getExtent());
      thriftJobs.add(job.toThrift());
    }
    coordinator.client.addJobs(TraceUtil.traceInfo(), context.rpcCreds(), thriftJobs);
    coordinator.jobBuffer.clear();
  }

  @Override
  public void close() {
    coordinatorConnections.forEach(((hostAndPort, coordinator) -> {
      try {
        sendJobs(coordinator);
        if (fullScan) {
          coordinator.client.endFullJobScan(TraceUtil.traceInfo(), context.rpcCreds(),
              dataLevel.name());
        }
      } catch (TException e) {
        log.warn("Failed to communicate with coordinator {}", hostAndPort, e);
      } finally {
        ThriftUtil.returnClient(coordinator.client, context);
      }
    }));

    if (!ignoredCounts.isEmpty()) {
      log.debug("Ignored compaction job counts {}", ignoredCounts);
    }
  }
}

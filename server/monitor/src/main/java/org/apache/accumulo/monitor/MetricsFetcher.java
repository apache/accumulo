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
package org.apache.accumulo.monitor;

import static java.util.concurrent.TimeUnit.SECONDS;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.apache.accumulo.core.client.admin.servers.ServerId;
import org.apache.accumulo.core.compaction.thrift.CompactionCoordinatorService;
import org.apache.accumulo.core.compaction.thrift.TExternalCompaction;
import org.apache.accumulo.core.compaction.thrift.TExternalCompactionList;
import org.apache.accumulo.core.lock.ServiceLockData;
import org.apache.accumulo.core.lock.ServiceLockData.ThriftService;
import org.apache.accumulo.core.lock.ServiceLockPaths.ServiceLockPath;
import org.apache.accumulo.core.metrics.thrift.MetricResponse;
import org.apache.accumulo.core.metrics.thrift.MetricService.Client;
import org.apache.accumulo.core.rpc.ThriftUtil;
import org.apache.accumulo.core.rpc.clients.ThriftClientTypes;
import org.apache.accumulo.core.trace.TraceUtil;
import org.apache.accumulo.core.util.UtilWaitThread;
import org.apache.accumulo.core.util.threads.ThreadPools;
import org.apache.accumulo.core.util.threads.ThreadPools.ExecutionError;
import org.apache.accumulo.server.ServerContext;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransportException;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.eclipse.jetty.util.NanoTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.RemovalCause;
import com.github.benmanes.caffeine.cache.RemovalListener;
import com.github.benmanes.caffeine.cache.Scheduler;
import com.google.common.net.HostAndPort;

import io.micrometer.core.instrument.DistributionSummary;

public class MetricsFetcher implements RemovalListener<ServerId,MetricResponse>, Runnable {

  private static final Logger LOG = LoggerFactory.getLogger(MetricsFetcher.class);

  private static class GcServerId extends ServerId {
    private GcServerId(String resourceGroup, String host, int port) {
      // TODO: This is a little wonky, Type.GC does not exist in the public API,
      super(ServerId.Type.MANAGER, resourceGroup, host, port);
    }
  }

  private class MetricFetcher implements Runnable {

    private final ServerContext ctx;
    private final ServerId server;
    private final ResponseSummary summary;

    private MetricFetcher(ServerContext ctx, ServerId server, ResponseSummary summary) {
      this.ctx = ctx;
      this.server = server;
      this.summary = summary;
    }

    @Override
    public void run() {
      try {
        Client metricsClient = ThriftUtil.getClient(ThriftClientTypes.METRICS,
            HostAndPort.fromParts(server.getHost(), server.getPort()), ctx);
        try {
          MetricResponse response = metricsClient.getMetrics(TraceUtil.traceInfo(), ctx.rpcCreds());
          summary.processResponse(server, response);
        } finally {
          ThriftUtil.returnClient(metricsClient, ctx);
        }
      } catch (TException e) {
        LOG.warn("Error trying to get metrics from server: {}", server);
        summary.processError(server);
      }
    }

  }

  private class CompactionListFetcher implements Runnable {

    private final String coordinatorMissingMsg =
        "Error getting the compaction coordinator client. Check that the Manager is running.";

    // Copied from Monitor
    private TExternalCompactionList getExternalCompactions() {
      Set<ServerId> managers = ctx.instanceOperations().getServers(ServerId.Type.MANAGER);
      if (managers.isEmpty()) {
        throw new IllegalStateException(coordinatorMissingMsg);
      }
      ServerId manager = managers.iterator().next();
      HostAndPort hp = HostAndPort.fromParts(manager.getHost(), manager.getPort());
      try {
        CompactionCoordinatorService.Client client =
            ThriftUtil.getClient(ThriftClientTypes.COORDINATOR, hp, ctx);
        try {
          return client.getRunningCompactions(TraceUtil.traceInfo(), ctx.rpcCreds());
        } catch (Exception e) {
          throw new IllegalStateException("Unable to get running compactions from " + hp, e);
        } finally {
          if (client != null) {
            ThriftUtil.returnClient(client, ctx);
          }
        }
      } catch (TTransportException e) {
        LOG.error("Unable to get Compaction coordinator at {}", hp);
        throw new IllegalStateException(coordinatorMissingMsg, e);
      }
    }

    @Override
    public void run() {
      try {
        TExternalCompactionList running = getExternalCompactions();
        // Create an index into the running compaction list that is
        // sorted by duration, meaning earliest start time first. This
        // will allow us to show topN longest running compactions.
        Map<Long,String> timeSortedEcids = new TreeMap<>();
        if (running.getCompactions() != null) {
          running.getCompactions().forEach((ecid, extComp) -> {
            if (extComp.getUpdates() != null && !extComp.getUpdates().isEmpty()) {
              Set<Long> orderedUpdateTimes = new TreeSet<>(extComp.getUpdates().keySet());
              timeSortedEcids.put(orderedUpdateTimes.iterator().next(), ecid);
            }
          });
        }
        runningCompactions.set(running.getCompactions());
        runningCompactionsDurationIndex.set(timeSortedEcids);
      } catch (Exception e) {
        LOG.error("Error gathering running compaction information", e);
      }
    }

  }

  private final String poolName = "MonitorMetricsThreadPool";
  private final ThreadPoolExecutor pool = ThreadPools.getServerThreadPools()
      .getPoolBuilder(poolName).numCoreThreads(10).withTimeOut(30, SECONDS).build();

  private final ServerContext ctx;
  private final Supplier<Long> connectionCount;
  private final AtomicBoolean newConnectionEvent = new AtomicBoolean(false);
  private final Cache<ServerId,MetricResponse> allMetrics;

  private final AtomicReference<ResponseSummary> summaryRef = new AtomicReference<>();

  private final AtomicReference<Map<String,TExternalCompaction>> runningCompactions =
      new AtomicReference<>();
  private final AtomicReference<Map<Long,String>> runningCompactionsDurationIndex =
      new AtomicReference<>();

  public MetricsFetcher(ServerContext ctx, Supplier<Long> connectionCount) {
    this.ctx = ctx;
    this.connectionCount = connectionCount;
    this.allMetrics = Caffeine.newBuilder().executor(pool).scheduler(Scheduler.systemScheduler())
        .expireAfterWrite(Duration.ofMinutes(10)).evictionListener(this::onRemoval).build();
  }

  public void newConnectionEvent() {
    this.newConnectionEvent.compareAndSet(false, true);
  }

  @Override
  public void onRemoval(@Nullable ServerId server, @Nullable MetricResponse response,
      RemovalCause cause) {
    if (server == null) {
      return;
    }
    LOG.info("{} has been evicted", server);
    getSummary().processError(server);
  }

  @Override
  public void run() {

    long refreshTime = 0;

    while (true) {

      // Don't fetch new data if there are no connections
      // On an initial connection, no data may be displayed
      // If a connection has not been made in a while, stale data may be displayed
      // Only refresh every 5s (old monitor logic)
      while (!newConnectionEvent.get() && connectionCount.get() == 0
          && NanoTime.millisElapsed(refreshTime, NanoTime.now()) > 5000) {
        Thread.onSpinWait();
      }
      // reset the connection event flag
      newConnectionEvent.compareAndExchange(true, false);

      LOG.info("Fetching metrics from servers");

      final List<Future<?>> futures = new ArrayList<>();
      final ResponseSummary summary = new ResponseSummary(allMetrics);

      for (ServerId.Type type : ServerId.Type.values()) {
        for (ServerId server : this.ctx.instanceOperations().getServers(type)) {
          futures.add(this.pool.submit(new MetricFetcher(this.ctx, server, summary)));
        }
      }
      ThreadPools.resizePool(pool, () -> Math.max(20, (futures.size() / 20)), poolName);

      // GC is not a public type, add it
      ServiceLockPath zgcPath = this.ctx.getServerPaths().getGarbageCollector(true);
      if (zgcPath != null) {
        Optional<ServiceLockData> sld = this.ctx.getZooCache().getLockData(zgcPath);
        if (sld.isPresent()) {
          String location = sld.orElseThrow().getAddressString(ThriftService.GC);
          String resourceGroup = sld.orElseThrow().getGroup(ThriftService.GC);
          HostAndPort hp = HostAndPort.fromString(location);
          futures.add(this.pool.submit(new MetricFetcher(this.ctx,
              new GcServerId(resourceGroup, hp.getHost(), hp.getPort()), summary)));
        }
      }

      // Fetch external compaction information from the Manager
      futures.add(this.pool.submit(new CompactionListFetcher()));

      while (futures.size() > 0) {
        Iterator<Future<?>> iter = futures.iterator();
        while (iter.hasNext()) {
          Future<?> future = iter.next();
          if (future.isDone()) {
            iter.remove();
            try {
              future.get();
            } catch (InterruptedException | ExecutionException e) {
              // throwing an error so the Monitor will die
              throw new ExecutionError("Error getting metrics from server", e);
            }
          }
        }
        if (futures.size() > 0) {
          UtilWaitThread.sleep(3_000);
        }
      }
      refreshTime = NanoTime.now();
      LOG.info("Finished fetching metrics from servers");
      LOG.info(
          "All: {}, Manager: {}, Garbage Collector: {}, Compactors: {}, Scan Servers: {}, Tablet Servers: {}",
          allMetrics.estimatedSize(), summary.getManager() != null,
          summary.getGarbageCollector() != null,
          summary.getCompactorAllMetricSummary().entrySet().iterator().next().getValue().count(),
          summary.getSServerAllMetricSummary().entrySet().iterator().next().getValue().count(),
          summary.getTServerAllMetricSummary().entrySet().iterator().next().getValue().count());

      ResponseSummary oldSummary = summaryRef.getAndSet(summary);
      oldSummary.clear();
    }

  }

  // Protect against NPE and wait for initial data gathering
  private ResponseSummary getSummary() {
    while (summaryRef.get() == null) {
      Thread.onSpinWait();
    }
    return summaryRef.get();
  }

  public Set<String> getResourceGroups() {
    return getSummary().getResourceGroups();
  }

  public Collection<ServerId> getProblemHosts() {
    return getSummary().getProblemHosts();
  }

  public Collection<MetricResponse> getAll() {
    return allMetrics.asMap().values();
  }

  public MetricResponse getManager() {
    final ServerId s = getSummary().getManager();
    if (s == null) {
      return null;
    }
    return allMetrics.asMap().get(s);
  }

  public MetricResponse getGarbageCollector() {
    final ServerId s = getSummary().getGarbageCollector();
    if (s == null) {
      return null;
    }
    return allMetrics.asMap().get(s);
  }

  public static class InstanceSummary {
    private final String instanceName;
    private final String instanceUUID;
    private final Set<String> zooKeepers;
    private final Set<String> volumes;

    public InstanceSummary(String instanceName, String instanceUUID, Set<String> zooKeepers,
        Set<String> volumes) {
      super();
      this.instanceName = instanceName;
      this.instanceUUID = instanceUUID;
      this.zooKeepers = zooKeepers;
      this.volumes = volumes;
    }

    public String getInstanceName() {
      return instanceName;
    }

    public String getInstanceUUID() {
      return instanceUUID;
    }

    public Set<String> getZooKeepers() {
      return zooKeepers;
    }

    public Set<String> getVolumes() {
      return volumes;
    }
  }

  public InstanceSummary getInstanceSummary() {
    return new InstanceSummary(ctx.getInstanceName(),
        ctx.instanceOperations().getInstanceId().canonical(),
        Set.of(ctx.getZooKeepers().split(",")), ctx.getVolumeManager().getVolumes().stream()
            .map(v -> v.toString()).collect(Collectors.toSet()));
  }

  public Collection<MetricResponse> getCompactors(String resourceGroup) {
    final Set<ServerId> servers = getSummary().getCompactorResourceGroupServers(resourceGroup);
    if (servers == null) {
      throw new IllegalArgumentException("Resource Group " + resourceGroup + " not found.");
    }
    return allMetrics.getAllPresent(servers).values();
  }

  public Map<String,DistributionSummary>
      getCompactorResourceGroupMetricSummary(String resourceGroup) {
    final Map<String,DistributionSummary> metrics =
        getSummary().getCompactorResourceGroupMetricSummary(resourceGroup);
    if (metrics == null) {
      throw new IllegalArgumentException("Resource Group " + resourceGroup + " not found.");
    }
    return metrics;
  }

  public Map<String,DistributionSummary> getCompactorAllMetricSummary() {
    return getSummary().getCompactorAllMetricSummary();
  }

  public Collection<MetricResponse> getScanServers(String resourceGroup) {
    final Set<ServerId> servers = getSummary().getSServerResourceGroupServers(resourceGroup);
    if (servers == null) {
      throw new IllegalArgumentException("Resource Group " + resourceGroup + " not found.");
    }
    return allMetrics.getAllPresent(servers).values();
  }

  public Map<String,DistributionSummary>
      getScanServerResourceGroupMetricSummary(String resourceGroup) {
    final Map<String,DistributionSummary> metrics =
        getSummary().getSServerResourceGroupMetricSummary(resourceGroup);
    if (metrics == null) {
      throw new IllegalArgumentException("Resource Group " + resourceGroup + " not found.");
    }
    return metrics;
  }

  public Map<String,DistributionSummary> getScanServerAllMetricSummary() {
    return getSummary().getSServerAllMetricSummary();
  }

  public Collection<MetricResponse> getTabletServers(String resourceGroup) {
    final Set<ServerId> servers = getSummary().getTServerResourceGroupServers(resourceGroup);
    if (servers == null) {
      throw new IllegalArgumentException("Resource Group " + resourceGroup + " not found.");
    }
    return allMetrics.getAllPresent(servers).values();
  }

  public Map<String,DistributionSummary>
      getTabletServerResourceGroupMetricSummary(String resourceGroup) {
    final Map<String,DistributionSummary> metrics =
        getSummary().getTServerResourceGroupMetricSummary(resourceGroup);
    if (metrics == null) {
      throw new IllegalArgumentException("Resource Group " + resourceGroup + " not found.");
    }
    return metrics;
  }

  public Map<String,DistributionSummary> getTabletServerAllMetricSummary() {
    return getSummary().getTServerAllMetricSummary();
  }

  public Collection<TExternalCompaction> getCompactions(int topN) {
    List<TExternalCompaction> results = new ArrayList<>();
    Map<String,TExternalCompaction> compactions = runningCompactions.get();
    Iterator<String> ecids = runningCompactionsDurationIndex.get().values().iterator();
    for (int i = 0; i < topN && ecids.hasNext(); i++) {
      results.add(compactions.get(ecids.next()));
    }
    return results;
  }
}

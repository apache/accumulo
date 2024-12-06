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
package org.apache.accumulo.monitor.next;

import static java.util.concurrent.TimeUnit.SECONDS;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import jakarta.ws.rs.NotFoundException;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.TabletInformation;
import org.apache.accumulo.core.client.admin.servers.ServerId;
import org.apache.accumulo.core.client.admin.servers.ServerId.Type;
import org.apache.accumulo.core.compaction.thrift.CompactionCoordinatorService;
import org.apache.accumulo.core.compaction.thrift.TExternalCompaction;
import org.apache.accumulo.core.compaction.thrift.TExternalCompactionList;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.metrics.flatbuffers.FMetric;
import org.apache.accumulo.core.metrics.thrift.MetricResponse;
import org.apache.accumulo.core.metrics.thrift.MetricService.Client;
import org.apache.accumulo.core.rpc.ThriftUtil;
import org.apache.accumulo.core.rpc.clients.ThriftClientTypes;
import org.apache.accumulo.core.trace.TraceUtil;
import org.apache.accumulo.core.util.UtilWaitThread;
import org.apache.accumulo.core.util.threads.ThreadPools;
import org.apache.accumulo.monitor.next.SystemInformation.ProcessSummary;
import org.apache.accumulo.monitor.next.SystemInformation.TableSummary;
import org.apache.accumulo.server.ServerContext;
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

import io.micrometer.core.instrument.Meter.Id;
import io.micrometer.core.instrument.cumulative.CumulativeDistributionSummary;

public class InformationFetcher implements RemovalListener<ServerId,MetricResponse>, Runnable {

  private static final Logger LOG = LoggerFactory.getLogger(InformationFetcher.class);

  public static class InstanceSummary {
    private final String instanceName;
    private final String instanceUUID;
    private final Set<String> zooKeepers;
    private final Set<String> volumes;
    private final String version;

    public InstanceSummary(String instanceName, String instanceUUID, Set<String> zooKeepers,
        Set<String> volumes, String version) {
      super();
      this.instanceName = instanceName;
      this.instanceUUID = instanceUUID;
      this.zooKeepers = zooKeepers;
      this.volumes = volumes;
      this.version = version;
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

    public String getVersion() {
      return version;
    }
  }

  private class MetricFetcher implements Runnable {

    private final ServerContext ctx;
    private final ServerId server;
    private final SystemInformation summary;

    private MetricFetcher(ServerContext ctx, ServerId server, SystemInformation summary) {
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
      } catch (Exception e) {
        LOG.warn("Error trying to get metrics from server: {}. Error message: {}", server,
            e.getMessage());
        summary.processError(server);
      }
    }

  }

  private class TableInformationFetcher implements Runnable {
    private final ServerContext ctx;
    private final String table;
    private final SystemInformation summary;

    private TableInformationFetcher(ServerContext ctx, String tableName,
        SystemInformation summary) {
      this.ctx = ctx;
      this.table = tableName;
      this.summary = summary;
    }

    @Override
    public void run() {
      try (Stream<TabletInformation> tablets =
          this.ctx.tableOperations().getTabletInformation(table, new Range())) {
        tablets.forEach(t -> summary.processTabletInformation(table, t));
      } catch (TableNotFoundException e) {
        LOG.warn(
            "TableNotFoundException thrown while trying to gather information for table: " + table,
            e);
      } catch (Exception e) {
        LOG.warn("Interrupted while trying to gather information for table: {}", table);
      }
    }
  }

  private class CompactionListFetcher implements Runnable {

    private final String coordinatorMissingMsg =
        "Error getting the compaction coordinator client. Check that the Manager is running.";

    private final SystemInformation summary;

    public CompactionListFetcher(SystemInformation summary) {
      this.summary = summary;
    }

    // Copied from Monitor
    private Map<String,TExternalCompactionList> getLongRunningCompactions() {
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
          return client.getLongRunningCompactions(TraceUtil.traceInfo(), ctx.rpcCreds());
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
        summary.processExternalCompactionList(getLongRunningCompactions());
      } catch (Exception e) {
        LOG.warn("Error gathering running compaction information. Error message: {}",
            e.getMessage());
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
  private final AtomicReference<SystemInformation> summaryRef = new AtomicReference<>();

  public InformationFetcher(ServerContext ctx, Supplier<Long> connectionCount) {
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
      final SystemInformation summary = new SystemInformation(allMetrics, this.ctx);

      for (ServerId.Type type : ServerId.Type.values()) {
        if (type == Type.MONITOR) {
          continue;
        }
        for (ServerId server : this.ctx.instanceOperations().getServers(type)) {
          futures.add(this.pool.submit(new MetricFetcher(this.ctx, server, summary)));
        }
      }
      ThreadPools.resizePool(pool, () -> Math.max(20, (futures.size() / 20)), poolName);

      // Fetch external compaction information from the Manager
      futures.add(this.pool.submit(new CompactionListFetcher(summary)));

      // Fetch Tablet / Tablet information from the metadata table
      for (String tName : this.ctx.tableOperations().list()) {
        futures.add(this.pool.submit(new TableInformationFetcher(this.ctx, tName, summary)));
      }

      long monitorFetchTimeout =
          ctx.getConfiguration().getTimeInMillis(Property.MONITOR_FETCH_TIMEOUT);
      long allFuturesAdded = NanoTime.now();
      boolean tookToLong = false;
      while (futures.size() > 0) {

        if (NanoTime.millisElapsed(allFuturesAdded, NanoTime.now()) > monitorFetchTimeout) {
          tookToLong = true;
        }

        Iterator<Future<?>> iter = futures.iterator();
        while (iter.hasNext()) {
          Future<?> future = iter.next();
          if (tookToLong && !future.isCancelled()) {
            future.cancel(true);
          } else if (future.isDone()) {
            iter.remove();
            try {
              future.get();
            } catch (CancellationException | InterruptedException | ExecutionException e) {
              LOG.error("Error getting status from future: {}", e.getMessage());
            }
          }
        }
        if (futures.size() > 0) {
          UtilWaitThread.sleep(3_000);
        }
      }

      summary.finish();

      refreshTime = NanoTime.now();
      LOG.info("Finished fetching metrics from servers");
      LOG.info(
          "All: {}, Manager: {}, Garbage Collector: {}, Compactors: {}, Scan Servers: {}, Tablet Servers: {}",
          allMetrics.estimatedSize(), summary.getManager() != null,
          summary.getGarbageCollector() != null,
          summary.getCompactorAllMetricSummary().isEmpty() ? 0
              : summary.getCompactorAllMetricSummary().entrySet().iterator().next().getValue()
                  .count(),
          summary.getSServerAllMetricSummary().isEmpty() ? 0
              : summary.getSServerAllMetricSummary().entrySet().iterator().next().getValue()
                  .count(),
          summary.getTServerAllMetricSummary().isEmpty() ? 0 : summary.getTServerAllMetricSummary()
              .entrySet().iterator().next().getValue().count());

      SystemInformation oldSummary = summaryRef.getAndSet(summary);
      if (oldSummary != null) {
        oldSummary.clear();
      }
    }

  }

  // Protect against NPE and wait for initial data gathering
  private SystemInformation getSummary() {
    while (summaryRef.get() == null) {
      Thread.onSpinWait();
    }
    return summaryRef.get();
  }

  private void validateResourceGroup(String resourceGroup) {
    if (getSummary().getResourceGroups().contains(resourceGroup)) {
      return;
    }
    throw new NotFoundException("Resource Group " + resourceGroup + " not found");
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
      throw new NotFoundException("Manager not found");
    }
    return allMetrics.asMap().get(s);
  }

  public MetricResponse getGarbageCollector() {
    final ServerId s = getSummary().getGarbageCollector();
    if (s == null) {
      throw new NotFoundException("Garbage Collector not found");
    }
    return allMetrics.asMap().get(s);
  }

  public InstanceSummary getInstanceSummary() {
    return new InstanceSummary(ctx.getInstanceName(),
        ctx.instanceOperations().getInstanceId().canonical(),
        Set.of(ctx.getZooKeepers().split(",")), ctx.getVolumeManager().getVolumes().stream()
            .map(v -> v.toString()).collect(Collectors.toSet()),
        Constants.VERSION);
  }

  public Collection<MetricResponse> getCompactors(String resourceGroup) {
    validateResourceGroup(resourceGroup);
    final Set<ServerId> servers = getSummary().getCompactorResourceGroupServers(resourceGroup);
    if (servers == null) {
      return List.of();
    }
    return allMetrics.getAllPresent(servers).values();
  }

  public Map<Id,CumulativeDistributionSummary>
      getCompactorResourceGroupMetricSummary(String resourceGroup) {
    validateResourceGroup(resourceGroup);
    final Map<Id,CumulativeDistributionSummary> metrics =
        getSummary().getCompactorResourceGroupMetricSummary(resourceGroup);
    if (metrics == null) {
      return Map.of();
    }
    return metrics;
  }

  public Map<Id,CumulativeDistributionSummary> getCompactorAllMetricSummary() {
    return getSummary().getCompactorAllMetricSummary();
  }

  public Collection<MetricResponse> getScanServers(String resourceGroup) {
    validateResourceGroup(resourceGroup);
    final Set<ServerId> servers = getSummary().getSServerResourceGroupServers(resourceGroup);
    if (servers == null) {
      return List.of();
    }
    return allMetrics.getAllPresent(servers).values();
  }

  public Map<Id,CumulativeDistributionSummary>
      getScanServerResourceGroupMetricSummary(String resourceGroup) {
    validateResourceGroup(resourceGroup);
    final Map<Id,CumulativeDistributionSummary> metrics =
        getSummary().getSServerResourceGroupMetricSummary(resourceGroup);
    if (metrics == null) {
      return Map.of();
    }
    return metrics;
  }

  public Map<Id,CumulativeDistributionSummary> getScanServerAllMetricSummary() {
    return getSummary().getSServerAllMetricSummary();
  }

  public Collection<MetricResponse> getTabletServers(String resourceGroup) {
    validateResourceGroup(resourceGroup);
    final Set<ServerId> servers = getSummary().getTServerResourceGroupServers(resourceGroup);
    if (servers == null) {
      return List.of();
    }
    return allMetrics.getAllPresent(servers).values();
  }

  public Map<Id,CumulativeDistributionSummary>
      getTabletServerResourceGroupMetricSummary(String resourceGroup) {
    validateResourceGroup(resourceGroup);
    final Map<Id,CumulativeDistributionSummary> metrics =
        getSummary().getTServerResourceGroupMetricSummary(resourceGroup);
    if (metrics == null) {
      return Map.of();
    }
    return metrics;
  }

  public Map<Id,CumulativeDistributionSummary> getTabletServerAllMetricSummary() {
    return getSummary().getTServerAllMetricSummary();
  }

  public Map<String,List<FMetric>> getCompactionMetricSummary() {
    return getSummary().getCompactionMetricSummary();
  }

  public Map<String,List<TExternalCompaction>> getCompactions(int topN) {
    return getSummary().getCompactions(topN);
  }

  public Map<String,TableSummary> getTables() {
    return getSummary().getTables();
  }

  public TableSummary getTable(String tableName) {
    return getSummary().getTables().get(tableName);
  }

  public List<TabletInformation> getTablets(String tableName) {
    return getSummary().getTablets(tableName);
  }

  public Map<String,Map<String,ProcessSummary>> getDeploymentOverview() {
    return getSummary().getDeploymentOverview();
  }

  public Set<String> getSuggestions() {
    return getSummary().getSuggestions();
  }

  public long getTimestamp() {
    return getSummary().getTimestamp();
  }

}

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
import java.util.stream.Stream;

import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.TabletInformation;
import org.apache.accumulo.core.client.admin.servers.ServerId;
import org.apache.accumulo.core.client.admin.servers.ServerId.Type;
import org.apache.accumulo.core.compaction.thrift.CompactionCoordinatorService;
import org.apache.accumulo.core.compaction.thrift.TExternalCompactionList;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.process.thrift.MetricResponse;
import org.apache.accumulo.core.process.thrift.ServerProcessService.Client;
import org.apache.accumulo.core.rpc.ThriftUtil;
import org.apache.accumulo.core.rpc.clients.ThriftClientTypes;
import org.apache.accumulo.core.trace.TraceUtil;
import org.apache.accumulo.core.util.UtilWaitThread;
import org.apache.accumulo.core.util.threads.ThreadPools;
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
        Client metricsClient = ThriftUtil.getClient(ThriftClientTypes.SERVER_PROCESS,
            HostAndPort.fromParts(server.getHost(), server.getPort()), ctx);
        try {
          MetricResponse response = metricsClient.getMetrics(TraceUtil.traceInfo(), ctx.rpcCreds());
          summary.processResponse(server, response);
        } finally {
          ThriftUtil.returnClient(metricsClient, ctx);
        }
      } catch (Exception e) {
        LOG.warn("Error trying to get metrics from server: {}", server, e);
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
        LOG.warn("Interrupted while trying to gather information for table: {}", table, e);
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
        LOG.error("Unable to get Compaction coordinator at {}", hp, e);
        throw new IllegalStateException(coordinatorMissingMsg, e);
      }
    }

    @Override
    public void run() {
      try {
        summary.processExternalCompactionList(getLongRunningCompactions());
      } catch (Exception e) {
        LOG.warn("Error gathering running compaction information.", e);
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

  // Protect against NPE and wait for initial data gathering
  public SystemInformation getSummary() {
    while (summaryRef.get() == null) {
      Thread.onSpinWait();
    }
    return summaryRef.get();
  }

  public Cache<ServerId,MetricResponse> getAllMetrics() {
    return allMetrics;
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

      // Don't fetch new data if there are no connections.
      // On an initial connection, no data may be displayed.
      // If a connection has not been made in a while, stale data may be displayed.
      // Only refresh every 5s (old monitor logic).
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
      while (!futures.isEmpty()) {

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
              LOG.error("Error getting status from future", e);
            }
          }
        }
        if (!futures.isEmpty()) {
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

}

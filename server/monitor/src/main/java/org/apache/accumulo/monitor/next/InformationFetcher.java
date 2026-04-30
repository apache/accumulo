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
import java.util.List;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Supplier;

import jakarta.ws.rs.ServiceUnavailableException;
import jakarta.ws.rs.core.Response;

import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.TabletInformation;
import org.apache.accumulo.core.client.admin.servers.ServerId;
import org.apache.accumulo.core.client.admin.servers.ServerId.Type;
import org.apache.accumulo.core.clientImpl.TabletInformationImpl;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.metadata.RootTable;
import org.apache.accumulo.core.metadata.SystemTables;
import org.apache.accumulo.core.metadata.TServerInstance;
import org.apache.accumulo.core.metadata.TabletState;
import org.apache.accumulo.core.metadata.schema.Ample;
import org.apache.accumulo.core.metadata.schema.MetadataSchema;
import org.apache.accumulo.core.metadata.schema.TabletMetadata;
import org.apache.accumulo.core.process.thrift.MetricResponse;
import org.apache.accumulo.core.process.thrift.ServerProcessService.Client;
import org.apache.accumulo.core.rpc.ThriftUtil;
import org.apache.accumulo.core.rpc.clients.ThriftClientTypes;
import org.apache.accumulo.core.trace.TraceUtil;
import org.apache.accumulo.core.util.UtilWaitThread;
import org.apache.accumulo.core.util.compaction.ExternalCompactionUtil;
import org.apache.accumulo.core.util.threads.ThreadPools;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.compaction.CompactionPluginUtils;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.eclipse.jetty.util.NanoTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.RemovalCause;
import com.github.benmanes.caffeine.cache.RemovalListener;
import com.github.benmanes.caffeine.cache.Scheduler;
import com.google.common.base.Suppliers;
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
          retainedProblemServers.invalidate(server);
          summary.processResponse(server, response);
        } finally {
          ThriftUtil.returnClient(metricsClient, ctx);
        }
      } catch (Exception e) {
        LOG.warn("Error trying to get metrics from server: {}", server, e);
        retainedProblemServers.put(server, Boolean.TRUE);
        summary.processMetricsError(server);
      }
    }

  }

  public static Future<?> fetchTabletMetadata(ServerContext ctx, ExecutorService executor,
      SystemInformation summary) {
    return fetchTabletMetadata(ctx, tabletInformation -> {
      final TableId tableId = tabletInformation.getTabletId().getTable();
      try {
        final String tableName = ctx.getQualifiedTableName(tableId);
        summary.processTabletInformation(tableId, tableName, tabletInformation);
      } catch (TableNotFoundException e) {
        if (SystemTables.containsTableId(tableId)) {
          throw new IllegalStateException(e);
        } else {
          LOG.debug("Table name for table id : {}, assuming table deleted", tableId);
        }
        throw new RuntimeException(e);
      }

    }, executor);
  }

  public static Future<?> fetchTabletMetadata(ServerContext ctx,
      Consumer<TabletInformation> tabletConsumer, ExecutorService executor) {

    Supplier<Duration> currentTime = Suppliers.memoize(() -> {
      try {
        return ctx.instanceOperations().getManagerTime();
      } catch (Exception e) {
        throw new IllegalStateException(e);
      }
    });

    // create an initial background task to read root tablet metadata from zookeeper
    var rootStage = CompletableFuture.supplyAsync(() -> {
      // read live tservers from zookeeper
      Set<TServerInstance> liveTserverSet = TabletMetadata.getLiveTServers(ctx);
      // read root tablet metadata from zookeeper
      var rootTablet = ctx.getAmple().readTablet(RootTable.EXTENT);
      var tabletSate = TabletState.compute(rootTablet, liveTserverSet);
      tabletConsumer
          .accept(new TabletInformationImpl(rootTablet, tabletSate::toString, currentTime));
      if (tabletSate == TabletState.HOSTED) {
        return liveTserverSet;
      } else {
        LOG.info("Not scanning root tablet because its state is {}",
            TabletState.compute(rootTablet, liveTserverSet));
        return null;
      }
    }, executor);

    // runs a follow on task that scans the root tablet and creates a background task to scan each
    // metadata table
    var metaStage = rootStage.thenCompose(liveTserverSet -> {
      if (liveTserverSet == null) {
        // root stage was not successful
        return null;
      }
      // TODO set an aggressive timeout on the metadata scan, if a failure happens after our checks
      // it could cause the scan to hang.
      try (var metaTablets =
          ctx.getAmple().readTablets().forLevel(Ample.DataLevel.METADATA).build()) {
        List<CompletableFuture<?>> futures = new ArrayList<>();
        for (var metaTablet : metaTablets) {
          var tabletState = TabletState.compute(metaTablet, liveTserverSet);
          tabletConsumer
              .accept(new TabletInformationImpl(metaTablet, tabletState::toString, currentTime));
          if (tabletState == TabletState.HOSTED) {
            var range = MetadataSchema.TabletsSection.getRange()
                .clip(metaTablet.getExtent().toDataRange(), true);
            if (range != null) {
              // spawn a task to scan this metadata tablet
              var future = CompletableFuture.runAsync(() -> {
                try (var userTablets =
                    ctx.getAmple().readTablets().scanMetadataTable().overRange(range).build()) {
                  for (var userTablet : userTablets) {
                    tabletConsumer.accept(new TabletInformationImpl(userTablet,
                        () -> TabletState.compute(userTablet, liveTserverSet).toString(),
                        currentTime));
                  }
                }
              });
              futures.add(future);
            }
          } else {
            LOG.info("Not scanning meta tablet {} because its state is {}", metaTablet.getExtent(),
                tabletState);
          }
        }

        // return a completable future that waits for all the scans of metadata tablets
        return CompletableFuture.allOf(futures.toArray(CompletableFuture[]::new));
      }
    });

    return metaStage;
  }

  private class RunningCompactionFetcher implements Runnable {

    private final SystemInformation summary;
    private final ThreadPoolExecutor executor;

    public RunningCompactionFetcher(SystemInformation summary, ThreadPoolExecutor executor) {
      this.summary = summary;
      this.executor = executor;
    }

    @Override
    public void run() {
      try {
        List<ServerId> failures = ExternalCompactionUtil.getCompactionsRunningOnCompactors(ctx,
            executor, (t) -> summary.processExternalCompaction(t));
        summary.getProblemHosts().addAll(failures);
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
  private final Cache<ServerId,Boolean> retainedProblemServers;
  private final AtomicReference<SystemInformation> summaryRef = new AtomicReference<>();

  public InformationFetcher(ServerContext ctx, Supplier<Long> connectionCount) {
    this.ctx = ctx;
    this.connectionCount = connectionCount;
    this.allMetrics = Caffeine.newBuilder().executor(pool).scheduler(Scheduler.systemScheduler())
        .expireAfterWrite(Duration.ofMinutes(10)).evictionListener(this::onRemoval).build();
    this.retainedProblemServers = Caffeine.newBuilder().executor(pool)
        .scheduler(Scheduler.systemScheduler()).expireAfterWrite(Duration.ofMinutes(10)).build();
  }

  public void newConnectionEvent() {
    this.newConnectionEvent.compareAndSet(false, true);
  }

  // Protect against NPE and wait for initial data gathering
  private SystemInformation getSummary() throws InterruptedException {
    while (summaryRef.get() == null) {
      Thread.sleep(100);
    }
    return summaryRef.get();
  }

  /**
   * {@link #getSummary()} but throws a 503 (Service Unavailable) server error to the web client if
   * an {@link InterruptedException} occurs.
   */
  public SystemInformation getSummaryForEndpoint() throws ServiceUnavailableException {
    try {
      return getSummary();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new ServiceUnavailableException(
          Response.status(Response.Status.SERVICE_UNAVAILABLE).build(), e);
    }
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
    try {
      getSummary().processError(server);
      LOG.info("{} has been evicted", server);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      LOG.warn("{} could not be evicted", server, e);
    }
  }

  @Override
  public void run() {

    long lastRunTime = 0;

    while (true) {

      // Don't fetch new data if there are no connections.
      // On an initial connection, no data may be displayed.
      // If a connection has not been made in a while, stale data may be displayed.
      // Only refresh every 5s (old monitor logic).
      while (!newConnectionEvent.get() && connectionCount.get() == 0
          && NanoTime.millisElapsed(lastRunTime, NanoTime.now()) > 5000) {
        try {
          Thread.sleep(100);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new IllegalStateException(
              "Thread " + Thread.currentThread().getName() + " interrupted", e);
        }
      }
      // reset the connection event flag
      newConnectionEvent.compareAndExchange(true, false);

      LOG.info("Fetching information from servers");

      final List<Future<?>> futures = new ArrayList<>();
      final SystemInformation summary = new SystemInformation(allMetrics, this.ctx);
      Set<ServerId> compactors = this.ctx.instanceOperations().getServers(Type.COMPACTOR);
      summary.processExternalCompactionInventory(compactors);

      for (ServerId.Type type : ServerId.Type.values()) {
        if (type == Type.MONITOR) {
          continue;
        }
        for (ServerId server : this.ctx.instanceOperations().getServers(type)) {
          futures.add(this.pool.submit(new MetricFetcher(this.ctx, server, summary)));
        }
      }
      ThreadPools.resizePool(pool, () -> Math.max(20, (futures.size() / 20)), poolName);

      // Fetch external compaction information from the Compactors
      futures.add(this.pool.submit(new RunningCompactionFetcher(summary, pool)));

      // Fetch Tablet / Tablet information from the metadata table

      futures.add(fetchTabletMetadata(ctx, pool, summary));

      futures.add(this.pool.submit(() -> {
        try {
          var groups = CompactionPluginUtils.getConfiguredCompactionResourceGroups(ctx);
          summary.addConfiguredCompactionGroups(groups);
        } catch (ReflectiveOperationException e) {
          throw new IllegalStateException(e);
        }
      }));

      final long monitorFetchTimeout =
          ctx.getConfiguration().getTimeInMillis(Property.MONITOR_FETCH_TIMEOUT);
      final long allFuturesAdded = NanoTime.now();
      boolean tookToLong = false;
      while (!futures.isEmpty()) {

        if (NanoTime.millisElapsed(allFuturesAdded, NanoTime.now()) > monitorFetchTimeout) {
          LOG.warn(
              "Fetching information for Monitor has taken longer {}. Cancelling all"
                  + " remaining tasks and monitor will display old information. Resolve issue"
                  + " causing this or increase property {}.",
              monitorFetchTimeout, Property.MONITOR_FETCH_TIMEOUT.getKey());
          tookToLong = true;
        }

        for (Future<?> future : futures) {
          if (tookToLong && !future.isCancelled()) {
            future.cancel(true);
          } else if (future.isDone()) {
            try {
              future.get();
            } catch (CancellationException | InterruptedException | ExecutionException e) {
              LOG.error("Error getting status from future", e);
            }
          }
        }

        // more efficient to do batch removal from array list than doing it one by one
        futures.removeIf(Future::isDone);

        if (!futures.isEmpty()) {
          UtilWaitThread.sleep(3_000);
        }
      }

      lastRunTime = NanoTime.now();

      if (tookToLong) {
        summary.clear();
      } else {
        retainedProblemServers.asMap().keySet().forEach(summary::retainProblemServer);
        summary.finish();

        LOG.info("Finished fetching metrics from servers");
        LOG.info(
            "All: {}, Managers: {}, Garbage Collector: {}, Compactors: {}, Scan Servers: {}, Tablet Servers: {}",
            allMetrics.estimatedSize(), summary.getManagers().size(),
            summary.getGarbageCollector() != null,
            summary.getCompactorAllMetricSummary().isEmpty() ? 0
                : summary.getCompactorAllMetricSummary().entrySet().iterator().next().getValue()
                    .count(),
            summary.getSServerAllMetricSummary().isEmpty() ? 0
                : summary.getSServerAllMetricSummary().entrySet().iterator().next().getValue()
                    .count(),
            summary.getTServerAllMetricSummary().isEmpty() ? 0 : summary
                .getTServerAllMetricSummary().entrySet().iterator().next().getValue().count());

        SystemInformation oldSummary = summaryRef.getAndSet(summary);
        if (oldSummary != null) {
          oldSummary.clear();
        }
      }
    }

  }

}
